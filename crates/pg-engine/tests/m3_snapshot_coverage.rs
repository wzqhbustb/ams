//! M3 Stage A acceptance: snapshot-registry coverage of the SIX engine
//! call sites (tech-selection §3.2 v1.1 enumeration), leak-freedom on every
//! path, and horizon concurrency (§3.3 v1.3/v1.4).
//!
//! The six registered construction points:
//!   1. `Engine::begin_txn` (guard lives in `TxnHandle`);
//!   2. `Engine::auto_commit` (success AND failure paths unregister);
//!   3. `create_index`'s post-lock re-snapshot (guard dies with the outer
//!      auto-commit closure);
//!   4. pure auto-commit SELECT (lock-free reader, §3.1's hole);
//!   5. `Engine::scan` (lock-free reader);
//!   6. `Engine::index_lookup` (lock-free reader).
//!
//! For the lock-free readers the registry is observed mid-scan by running
//! the reader in a loop on a worker thread while the main thread polls the
//! registry (barrier-style observation without instrumenting the engine).
//! Every concurrency test runs under a watchdog: a regression FAILS, never
//! hangs (Stage T `m2c_stress.rs` convention).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use pg_engine::{Datum, Engine, EngineConfig};
use pg_storage::types::TxnId;
use tempfile::TempDir;

/// Watchdog budget for every join in this file.
const WATCHDOG: Duration = Duration::from_secs(120);
/// How long the main thread polls the registry before declaring a missing
/// registration (mid-execution observation must succeed within this).
const OBSERVE_DEADLINE: Duration = Duration::from_secs(30);

fn open(dir: &std::path::Path) -> Engine {
    Engine::open(dir, EngineConfig::new(dir)).unwrap()
}

fn registry(engine: &Engine) -> BTreeMap<TxnId, usize> {
    engine.txn_manager().snapshot_xmin_registry()
}

/// Sum of registry refcounts == number of live registered snapshots.
fn registered_total(engine: &Engine) -> usize {
    registry(engine).values().sum()
}

/// Registry drained AND the mirror counter agrees (the count-assertion
/// guardrail, checked at quiescent points only).
fn assert_drained(engine: &Engine) {
    assert!(
        registry(engine).is_empty(),
        "registry leaked entries: {:?}",
        registry(engine)
    );
    assert_eq!(
        engine.txn_manager().live_registered_snapshots(),
        0,
        "live-registered counter drifted from the drained registry"
    );
}

/// Poll `cond` at 1ms cadence until it holds or the deadline passes.
fn poll_until(deadline: Duration, mut cond: impl FnMut() -> bool) -> bool {
    let start = Instant::now();
    while start.elapsed() < deadline {
        if cond() {
            return true;
        }
        thread::sleep(Duration::from_millis(1));
    }
    cond()
}

/// Join a worker under a watchdog: a hang is a test failure, not a stuck CI.
fn watch<T: Send + 'static>(handle: JoinHandle<T>, what: &str) -> T {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let _ = tx.send(handle.join());
    });
    match rx.recv_timeout(WATCHDOG) {
        Ok(joined) => joined.unwrap_or_else(|e| panic!("{what}: worker panicked: {e:?}")),
        Err(mpsc::RecvTimeoutError::Timeout) => {
            panic!("{what}: watchdog tripped after {WATCHDOG:?} (deadlock/hang regression)")
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => panic!("{what}: watchdog channel broke"),
    }
}

/// Insert `rows_per_txn` rows as ONE explicit transaction (one fsync) —
/// table setup must not pay a commit fsync per row.
fn load_rows(engine: &Engine, table: &str, n: i32) {
    let txn = engine.begin_txn().unwrap();
    for i in 0..n {
        engine
            .exec(
                Some(&txn),
                &format!("INSERT INTO {table} VALUES ({i}, {i})"),
            )
            .unwrap();
    }
    txn.commit().unwrap();
}

/// 1a. `begin_txn`: the snapshot is registered for the handle's whole
/// lifetime and unregistered by `commit`.
#[test]
fn begin_txn_registers_until_commit() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine.exec(None, "CREATE TABLE t (k INT, v INT)").unwrap();
    assert_drained(&engine);

    let txn = engine.begin_txn().unwrap();
    let xid = txn.xid();
    // Only active transaction ⇒ the snapshot's xmin is the txn's own XID.
    assert_eq!(registry(&engine).get(&xid), Some(&1));
    assert_eq!(engine.oldest_snapshot_xmin(), xid);

    txn.commit().unwrap();
    assert_drained(&engine);
    // Empty registry ⇒ horizon falls back to the XID clock's current value.
    assert!(engine.oldest_snapshot_xmin() > xid);
}

/// 1b. Leak-freedom: `TxnHandle::drop` without commit (best-effort abort)
/// must also unregister.
#[test]
fn txn_handle_drop_without_commit_unregisters() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine.exec(None, "CREATE TABLE t (k INT, v INT)").unwrap();

    let xid = {
        let txn = engine.begin_txn().unwrap();
        let xid = txn.xid();
        assert_eq!(registry(&engine).get(&xid), Some(&1));
        drop(txn); // auto-abort path
        xid
    };
    assert_drained(&engine);
    assert!(engine.oldest_snapshot_xmin() > xid);
}

/// (2) `auto_commit` DML registers its snapshot BEFORE the statement body's
/// lock wait (tech-selection v1.4's corrected path — the one easiest to
/// miss): while the statement is blocked on the table lock, its snapshot's
/// xmin must already pin the horizon.
#[test]
fn auto_commit_dml_registers_before_lock_wait() {
    let tmp = TempDir::new().unwrap();
    let engine = Arc::new(open(tmp.path()));
    engine.exec(None, "CREATE TABLE t (k INT, v INT)").unwrap();

    // Holder A takes the table's RowExclusive lock and stays open.
    let holder = engine.begin_txn().unwrap();
    engine
        .exec(Some(&holder), "INSERT INTO t VALUES (1, 10)")
        .unwrap();
    let holder_xid = holder.xid();
    assert_eq!(registry(&engine).get(&holder_xid), Some(&1));

    // B's auto-commit INSERT: begins (XID into the active set), snapshots
    // (xmin = holder_xid, registered), THEN blocks on RowExclusive.
    let blocked = {
        let engine = Arc::clone(&engine);
        thread::spawn(move || engine.exec(None, "INSERT INTO t VALUES (2, 20)"))
    };

    // While B is stuck behind A's lock, the registry must show B's
    // snapshot as a second reference at holder_xid — proof the snapshot
    // (and its horizon pin) precedes the lock wait.
    assert!(
        poll_until(OBSERVE_DEADLINE, || registry(&engine).get(&holder_xid)
            == Some(&2)),
        "auto-commit DML snapshot did not register before the lock wait: {:?}",
        registry(&engine)
    );
    assert_eq!(engine.oldest_snapshot_xmin(), holder_xid);

    // Let B through; both transactions finish and the registry drains.
    holder.abort().unwrap();
    watch(blocked, "blocked auto-commit INSERT").unwrap();
    assert_drained(&engine);
}

/// 2b. Leak-freedom on the `auto_commit` FAILURE path: the statement
/// errors inside `op`, the abort branch runs, and the snapshot guard must
/// still unregister.
#[test]
fn auto_commit_failure_path_unregisters() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine.exec(None, "CREATE TABLE t (k INT, v INT)").unwrap();
    assert_drained(&engine);

    // Wrong column count: fails inside the auto-commit closure, after the
    // snapshot was taken and registered.
    let result = engine.exec(None, "INSERT INTO t VALUES (1)");
    assert!(result.is_err(), "arity mismatch must fail the statement");
    assert_drained(&engine);
}

/// (4) Pure auto-commit SELECT (the §3.1 lock-free-reader hole): its
/// snapshot is registered for the statement's duration, with
/// xmin = clock.current() (empty active set), and unregistered at the end.
#[test]
fn pure_select_registers_during_statement() {
    let tmp = TempDir::new().unwrap();
    let engine = Arc::new(open(tmp.path()));
    engine.exec(None, "CREATE TABLE t (k INT, v INT)").unwrap();
    load_rows(&engine, "t", 200);
    assert_drained(&engine);
    // Empty registry ⇒ this reads the XID clock's current value, which is
    // exactly the xmin a lock-free reader's snapshot must register.
    let expected_xmin = engine.oldest_snapshot_xmin();

    let stop = Arc::new(AtomicBool::new(false));
    let reader = {
        let engine = Arc::clone(&engine);
        let stop = Arc::clone(&stop);
        thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                engine.exec(None, "SELECT * FROM t").unwrap();
            }
        })
    };

    assert!(
        poll_until(OBSERVE_DEADLINE, || {
            let reg = registry(&engine);
            !reg.is_empty() && reg.keys().all(|&xmin| xmin == expected_xmin)
        }),
        "pure SELECT snapshot never registered at the expected xmin \
         ({expected_xmin:?}): {:?}",
        registry(&engine)
    );

    stop.store(true, Ordering::Relaxed);
    watch(reader, "pure SELECT reader");
    assert_drained(&engine);
}

/// (5) `Engine::scan` (public typed API, lock-free reader): registered for
/// the call frame.
#[test]
fn engine_scan_registers_during_call() {
    let tmp = TempDir::new().unwrap();
    let engine = Arc::new(open(tmp.path()));
    engine.exec(None, "CREATE TABLE t (k INT, v INT)").unwrap();
    load_rows(&engine, "t", 200);
    let expected_xmin = engine.oldest_snapshot_xmin();

    let stop = Arc::new(AtomicBool::new(false));
    let reader = {
        let engine = Arc::clone(&engine);
        let stop = Arc::clone(&stop);
        thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                engine.scan("t", None).unwrap();
            }
        })
    };

    assert!(
        poll_until(OBSERVE_DEADLINE, || {
            let reg = registry(&engine);
            !reg.is_empty() && reg.keys().all(|&xmin| xmin == expected_xmin)
        }),
        "Engine::scan snapshot never registered at the expected xmin: {:?}",
        registry(&engine)
    );

    stop.store(true, Ordering::Relaxed);
    watch(reader, "Engine::scan reader");
    assert_drained(&engine);
}

/// (6) `Engine::index_lookup` (public typed API, lock-free reader):
/// registered for the call frame.
#[test]
fn engine_index_lookup_registers_during_call() {
    let tmp = TempDir::new().unwrap();
    let engine = Arc::new(open(tmp.path()));
    engine.exec(None, "CREATE TABLE t (k INT, v INT)").unwrap();
    load_rows(&engine, "t", 200);
    engine.create_index("t", "k").unwrap();
    assert_drained(&engine);
    let expected_xmin = engine.oldest_snapshot_xmin();

    let stop = Arc::new(AtomicBool::new(false));
    let reader = {
        let engine = Arc::clone(&engine);
        let stop = Arc::clone(&stop);
        thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                engine.index_lookup("t", "k", &Datum::Int4(7)).unwrap();
            }
        })
    };

    assert!(
        poll_until(OBSERVE_DEADLINE, || {
            let reg = registry(&engine);
            !reg.is_empty() && reg.keys().all(|&xmin| xmin == expected_xmin)
        }),
        "Engine::index_lookup snapshot never registered at the expected \
         xmin: {:?}",
        registry(&engine)
    );

    stop.store(true, Ordering::Relaxed);
    watch(reader, "Engine::index_lookup reader");
    assert_drained(&engine);
}

/// (3) `create_index`'s post-lock re-snapshot: during the build BOTH the
/// outer auto-commit snapshot and the re-taken snapshot are registered
/// (same xmin here ⇒ refcount 2), and both die with the auto-commit frame.
#[test]
fn create_index_re_snapshot_registers() {
    let tmp = TempDir::new().unwrap();
    let engine = Arc::new(open(tmp.path()));
    engine.exec(None, "CREATE TABLE t (k INT, v INT)").unwrap();
    load_rows(&engine, "t", 5_000);
    assert_drained(&engine);

    let builder = {
        let engine = Arc::clone(&engine);
        thread::spawn(move || engine.create_index("t", "k"))
    };

    // The build's auto-commit XID is the only active transaction, so both
    // snapshots register the same xmin: a refcount of 2 can only come from
    // the re-snapshot being registered alongside the outer one.
    assert!(
        poll_until(OBSERVE_DEADLINE, || registered_total(&engine) >= 2),
        "create_index re-snapshot never registered alongside the outer \
         auto-commit snapshot: {:?}",
        registry(&engine)
    );

    watch(builder, "create_index builder").unwrap();
    assert_drained(&engine);
}

/// Horizon concurrency (§3.3): mixed churn — concurrent registered
/// snapshots plus auto-commit DML entering the active set — must keep
/// `oldest_snapshot_xmin() ≤ every in-flight snapshot's xmin`, and a
/// 100-thread short-query churn must drain the registry to zero without
/// deadlocking.
#[test]
fn churn_100_threads_registry_drains() {
    let tmp = TempDir::new().unwrap();
    let engine = Arc::new(open(tmp.path()));
    engine.exec(None, "CREATE TABLE t (k INT, v INT)").unwrap();
    load_rows(&engine, "t", 100);

    let stop = Arc::new(AtomicBool::new(false));
    let mut workers = Vec::new();
    for conn in 0..100 {
        let engine = Arc::clone(&engine);
        let stop = Arc::clone(&stop);
        workers.push(thread::spawn(move || {
            let mut i = 0i32;
            while !stop.load(Ordering::Relaxed) {
                if conn % 4 == 0 {
                    // Auto-commit writer: XID + snapshot enter the active
                    // set / registry before any lock wait (v1.4 path).
                    engine
                        .exec(
                            None,
                            &format!("INSERT INTO t VALUES ({}, {})", conn * 1_000_000 + i, i),
                        )
                        .unwrap();
                } else {
                    // Lock-free reader churn.
                    engine.scan("t", None).unwrap();
                }
                i += 1;
            }
        }));
    }

    // Horizon observer: `oldest` is read FIRST and the registry SECOND.
    // Between the two reads a worker may legitimately UNREGISTER an entry
    // (advancing the registry's min — equality is not required), and may
    // also register a NEW entry. The assertion `oldest <= min(registry)`
    // still holds at every instant because the XID clock is strictly
    // monotonic and `begin_txn`/`auto_commit` are the only XID sources:
    // a newly registered snapshot's xmin = min(active set at its begin) can
    // never be below a previously registered one, so min(registry) never
    // moves backward between the two reads. NOTE: this invariant depends on
    // XID-clock monotonicity — it is NOT a spec-level guarantee; if a future
    // code path ever creates snapshots with arbitrary xmins, this observer
    // can false-positive.
    let observer = {
        let engine = Arc::clone(&engine);
        let stop = Arc::clone(&stop);
        thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let oldest = engine.oldest_snapshot_xmin();
                let reg = registry(&engine);
                if let Some(&min) = reg.keys().next() {
                    assert!(
                        oldest <= min,
                        "horizon {oldest:?} skipped past registered xmin {min:?}"
                    );
                }
            }
        })
    };

    // Let the churn run, then stop it.
    thread::sleep(Duration::from_secs(5));
    stop.store(true, Ordering::Relaxed);
    for (i, w) in workers.into_iter().enumerate() {
        watch(w, &format!("churn worker {i}"));
    }
    watch(observer, "horizon observer");
    assert_drained(&engine);
}
