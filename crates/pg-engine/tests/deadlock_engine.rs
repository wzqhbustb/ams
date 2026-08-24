//! M2c Stage R engine-level acceptance: the deadlock detector wired by
//! `Engine::open` (default 100ms tick) breaks real SQL-level deadlocks.
//!
//! Covered:
//!
//! - Row-lock cycle through `exec`: two explicit transactions UPDATE two
//!   rows in opposite order; the younger is interrupted with
//!   `EngineError::Heap(HeapError::DeadlockVictim)`, aborts cleanly (index
//!   undo + `release_all` ran — asserted via final data + index lookups),
//!   and the elder proceeds to commit.
//! - Table-lock cycle through the engine's OWN `LockManager`/`TxnManager`
//!   (the detector wiring under test is the engine's). NOTE: a pure
//!   table-lock cycle is not expressible through `exec` at M2c — the only
//!   modes an explicit transaction can take (`AccessShare` for SELECT,
//!   `RowExclusive` for DML/FOR UPDATE) are mutually compatible under the
//!   §9.2 matrix, and the conflicting modes (`Exclusive`,
//!   `AccessExclusive`) are DDL-only while DDL is rejected inside explicit
//!   transactions — so this test drives the lock manager directly with raw
//!   XIDs from the engine's transaction manager.
//!
//! Acceptance: `cargo test -p pg-engine --test deadlock_engine`

use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use pg_engine::{Datum, Engine, EngineConfig, EngineError, HeapError, Oid, QueryResult};
use pg_txn::{LockError, LockMode};
use tempfile::TempDir;

/// Open an engine (default config: 100ms detector tick) with table
/// `accounts (id INT, v INT)`, rows (1, 10) and (2, 20), and an index on
/// `id` (so the victim's abort exercises the index-undo path).
fn open_with_accounts() -> (TempDir, Arc<Engine>) {
    let tmp = TempDir::new().unwrap();
    let engine = Arc::new(Engine::open(tmp.path(), EngineConfig::new(tmp.path())).unwrap());
    engine
        .exec(None, "CREATE TABLE accounts (id INT, v INT)")
        .unwrap();
    engine
        .exec(None, "INSERT INTO accounts VALUES (1, 10)")
        .unwrap();
    engine
        .exec(None, "INSERT INTO accounts VALUES (2, 20)")
        .unwrap();
    engine.create_index("accounts", "id").unwrap();
    (tmp, engine)
}

/// The committed `v` of row `id`, via a fresh auto-commit SELECT.
fn value_of(engine: &Engine, id: i64) -> i32 {
    match engine
        .exec(None, &format!("SELECT v FROM accounts WHERE id = {id}"))
        .unwrap()
    {
        QueryResult::Rows { rows, .. } => {
            assert_eq!(rows.len(), 1, "exactly one row with id = {id}");
            match &rows[0][0] {
                Some(Datum::Int4(v)) => *v,
                other => panic!("unexpected value: {other:?}"),
            }
        }
        other => panic!("expected Rows, got {other:?}"),
    }
}

/// Row-lock deadlock through SQL: t1 updates row 1 then row 2, t2 updates
/// row 2 then row 1. The detector must pick the YOUNGER transaction (t2)
/// as victim within 200ms; its statement fails with `DeadlockVictim`, its
/// abort unwinds cleanly, and t1's blocked update then succeeds.
#[test]
fn row_lock_deadlock_via_exec_youngest_aborted_elder_commits() {
    let (_tmp, engine) = open_with_accounts();

    let t1 = engine.begin_txn().unwrap();
    let t2 = engine.begin_txn().unwrap();
    let (xid1, xid2) = (t1.xid(), t2.xid());
    assert!(xid2 > xid1, "t2 is the younger transaction");

    engine
        .exec(Some(&t1), "UPDATE accounts SET v = 11 WHERE id = 1")
        .unwrap();
    engine
        .exec(Some(&t2), "UPDATE accounts SET v = 21 WHERE id = 2")
        .unwrap();

    // t1 attacks row 2 (blocks on t2's stamp); on success it commits.
    let (tx1, rx1) = mpsc::channel();
    let engine1 = Arc::clone(&engine);
    thread::spawn(move || {
        let r = engine1.exec(Some(&t1), "UPDATE accounts SET v = 12 WHERE id = 2");
        match &r {
            Ok(_) => t1.commit().unwrap(),
            Err(_) => t1.abort().unwrap(),
        }
        let _ = tx1.send(r);
    });

    // t2 attacks row 1: the cycle closes here. The detector must interrupt
    // t2 (the younger) — run the blocking exec on a worker so a regression
    // fails on a timeout instead of hanging the suite.
    let (tx2, rx2) = mpsc::channel();
    let engine2 = Arc::clone(&engine);
    let closed_at = Instant::now();
    thread::spawn(move || {
        let r = engine2.exec(Some(&t2), "UPDATE accounts SET v = 22 WHERE id = 1");
        let _ = tx2.send((r, t2));
    });
    let (victim_result, t2) = rx2
        .recv_timeout(Duration::from_secs(15))
        .expect("victim's UPDATE was not interrupted within 15s");
    let latency = closed_at.elapsed();
    assert!(
        matches!(
            victim_result,
            Err(EngineError::Heap(HeapError::DeadlockVictim))
        ),
        "younger txn must fail with HeapError::DeadlockVictim, got {victim_result:?}"
    );
    assert!(
        latency <= Duration::from_millis(200),
        "engine-level detection latency {latency:?} exceeds 200ms"
    );
    eprintln!("engine row-lock cycle detected in {latency:?}");

    // PG semantics: the victim's current statement failed; the caller must
    // abort. This runs the index undo for t2's row-2 update, the durable
    // abort, and the 2PL table-lock release.
    t2.abort().unwrap();

    // The elder's blocked update now proceeds (row 2's stamper aborted) and
    // its commit lands.
    let elder_result = rx1
        .recv_timeout(Duration::from_secs(15))
        .expect("elder's UPDATE did not complete after the victim's abort");
    assert!(
        matches!(elder_result, Ok(QueryResult::Affected(1))),
        "elder's UPDATE must succeed after the victim's abort, got {elder_result:?}"
    );

    // Final state: t1's writes committed (v = 11, 12); t2's v = 21 is gone.
    assert_eq!(value_of(&engine, 1), 11);
    assert_eq!(value_of(&engine, 2), 12);
    // The index-undo path ran on the victim's abort: both rows still
    // resolve through the index.
    assert!(engine
        .index_lookup("accounts", "id", &Datum::Int4(1))
        .unwrap()
        .is_some());
    assert!(engine
        .index_lookup("accounts", "id", &Datum::Int4(2))
        .unwrap()
        .is_some());
    // No wait edges, no leaked active XIDs.
    assert!(engine.txn_manager().wait_edges().is_empty());
    assert!(engine.txn_manager().active_xids().is_empty());
    engine.shutdown();
}

/// Table-lock cycle through the engine's wired detector (see the module
/// docs for why this cannot be expressed through `exec`): A holds
/// `Exclusive` on t1 and wants t2, B holds `Exclusive` on t2 and wants t1.
/// The younger (B) is interrupted with `LockError::DeadlockVictim`; after
/// its release, A acquires.
#[test]
fn table_lock_cycle_broken_by_engine_detector() {
    let tmp = TempDir::new().unwrap();
    let engine = Engine::open(tmp.path(), EngineConfig::new(tmp.path())).unwrap();

    let (t1, t2) = (Oid(900_001), Oid(900_002));
    let a = engine.txn_manager().begin_txn();
    let b = engine.txn_manager().begin_txn();
    assert!(b > a, "b is the younger transaction");

    let lm = engine.lock_manager();
    lm.acquire(a, t1, LockMode::Exclusive).unwrap();
    lm.acquire(b, t2, LockMode::Exclusive).unwrap();

    thread::scope(|s| {
        let (tx_a, rx_a) = mpsc::channel();
        s.spawn(move || {
            let _ = tx_a.send(lm.acquire(a, t2, LockMode::Exclusive));
        });
        // A is queued on t2 before B closes the ring.
        let deadline = Instant::now() + Duration::from_secs(10);
        while !lm
            .table_lock_state(t2)
            .is_some_and(|s| s.waiters.iter().any(|(x, _)| *x == a))
        {
            assert!(Instant::now() < deadline, "A never queued on t2");
            thread::sleep(Duration::from_millis(2));
        }

        let (tx_b, rx_b) = mpsc::channel();
        let closed_at = Instant::now();
        s.spawn(move || {
            let _ = tx_b.send(lm.acquire(b, t1, LockMode::Exclusive));
        });
        let victim_result = rx_b
            .recv_timeout(Duration::from_secs(15))
            .expect("B's acquire was not interrupted within 15s");
        let latency = closed_at.elapsed();
        assert_eq!(
            victim_result,
            Err(LockError::DeadlockVictim(b)),
            "the engine's detector must pick the younger transaction"
        );
        assert!(
            latency <= Duration::from_millis(200),
            "table-cycle detection latency {latency:?} exceeds 200ms"
        );
        eprintln!("engine table-lock cycle detected in {latency:?}");

        // Victim abort (2PL release) lets A's acquire through.
        lm.release_all(b);
        assert_eq!(
            rx_a.recv_timeout(Duration::from_secs(15)),
            Ok(Ok(())),
            "A must acquire once the victim released"
        );
        lm.release_all(a);
    });

    // Raw `txn_manager()` begins pair with explicit aborts (see the
    // `commit_txn` doc on table locks; both xids already released above).
    engine.txn_manager().abort_txn(a).unwrap();
    engine.txn_manager().abort_txn(b).unwrap();
    engine.shutdown();
}
