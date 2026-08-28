//! M3 Stage A acceptance: the snapshot registry + vacuum horizon
//! (tech-selection §3.3 v1.3/v1.4).
//!
//! Covers: register/drop/refcount bookkeeping, the empty-registry horizon
//! fallback to the XID clock, the count-assertion guardrail (live
//! registered snapshots == sum of registry refcounts), concurrent
//! register/unregister without drift, and the B1 atomicity stress —
//! a horizon read must never skip past a returned-but-alive snapshot's
//! xmin. All concurrency tests run under watchdogs: a regression FAILS,
//! never hangs (Stage T `m2c_stress.rs` convention).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use pg_storage::error::Result;
use pg_storage::txn_id::TxnIdClock;
use pg_storage::types::{Lsn, TxnId};
use pg_storage::wal::record::WalRecord;
use pg_txn::{CommitWal, InMemoryClogAccessor, Snapshot, TxnManager};

/// Watchdog budget for every concurrency test in this file.
const WATCHDOG: Duration = Duration::from_secs(120);

/// A no-op WAL: append/flush always succeed, so the manager can be driven
/// without touching disk.
#[derive(Debug, Default)]
struct OkWal;

impl CommitWal for OkWal {
    fn append(&self, _record: WalRecord) -> Result<Lsn> {
        Ok(Lsn::FIRST)
    }

    fn flush_to(&self, _lsn: Lsn) -> Result<()> {
        Ok(())
    }
}

fn manager() -> TxnManager {
    TxnManager::new(
        TxnIdClock::new(TxnId::FIRST),
        Arc::new(OkWal),
        Arc::new(InMemoryClogAccessor::new()),
    )
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

/// The count-assertion guardrail (M3 Stage A): live registered snapshots
/// == sum of the registry's refcounts. Only meaningful at a quiescent
/// point (the two counters are not read under one lock).
fn assert_count_consistent(mgr: &TxnManager) {
    let registry = mgr.snapshot_xmin_registry();
    let sum: usize = registry.values().sum();
    assert_eq!(
        mgr.live_registered_snapshots(),
        sum,
        "live registered snapshot count drifted from registry refcount sum"
    );
}

#[test]
fn register_drop_refcount() {
    let mgr = manager();
    assert!(mgr.snapshot_xmin_registry().is_empty());

    // Empty active set: both snapshots collapse to xmin = clock.current(),
    // exercising the same-xmin refcount path.
    let (snap_a, guard_a) = mgr.snapshot(TxnId::INVALID);
    let (snap_b, guard_b) = mgr.snapshot(TxnId::INVALID);
    assert_eq!(snap_a.xmin(), snap_b.xmin());
    assert_eq!(mgr.snapshot_xmin_registry().get(&snap_a.xmin()), Some(&2));
    assert_count_consistent(&mgr);

    drop(guard_a);
    assert_eq!(mgr.snapshot_xmin_registry().get(&snap_a.xmin()), Some(&1));
    assert_count_consistent(&mgr);

    drop(guard_b);
    assert!(
        mgr.snapshot_xmin_registry().is_empty(),
        "zero refcount must remove the key"
    );
    assert_count_consistent(&mgr);
}

#[test]
fn horizon_empty_registry_falls_back_to_min_active_then_clock() {
    let mgr = manager();
    // Nothing at all: horizon = clock (no readers anywhere).
    assert_eq!(mgr.oldest_snapshot_xmin(), TxnId::FIRST);

    // A begun-but-not-yet-snapshotted transaction still holds the horizon
    // back (the begin→snapshot window): registry empty + active non-empty
    // ⇒ horizon = min(active), NOT the clock.
    let t1 = mgr.begin_txn();
    assert_eq!(mgr.oldest_snapshot_xmin(), t1);

    // Once that transaction ends and nothing else is active, the horizon
    // moves up to the clock again.
    mgr.commit_txn(t1).unwrap();
    assert_eq!(mgr.oldest_snapshot_xmin(), TxnId(t1.0 + 1));
}

#[test]
fn horizon_tracks_min_registered_xmin() {
    let mgr = manager();
    let t1 = mgr.begin_txn();
    let t2 = mgr.begin_txn();

    let (snap1, guard1) = mgr.snapshot(t1);
    assert_eq!(snap1.xmin(), t1);
    assert_eq!(mgr.oldest_snapshot_xmin(), t1);

    // A second, younger snapshot does not move the horizon.
    mgr.commit_txn(t1).unwrap();
    let (snap2, _guard2) = mgr.snapshot(t2);
    assert_eq!(snap2.xmin(), t2);
    assert_eq!(
        mgr.oldest_snapshot_xmin(),
        t1,
        "horizon = registry min key, not newest snapshot"
    );

    // Dropping the older guard advances the horizon to the next key.
    drop(guard1);
    assert_eq!(mgr.oldest_snapshot_xmin(), t2);
    assert_count_consistent(&mgr);
}

#[test]
fn everything_is_never_registered() {
    let mgr = manager();
    let everything = Snapshot::everything();
    assert_eq!(everything.xmin(), TxnId(0));
    assert!(
        mgr.snapshot_xmin_registry().is_empty(),
        "Snapshot::everything() must never enter the registry (xmin=0 would \
         pin the horizon at 0 and disable reclamation forever)"
    );
    assert_eq!(mgr.live_registered_snapshots(), 0);
}

#[test]
fn concurrent_register_unregister_no_drift() {
    let mgr = Arc::new(manager());
    let mut workers = Vec::new();
    for _ in 0..16 {
        let mgr = Arc::clone(&mgr);
        workers.push(thread::spawn(move || {
            for _ in 0..200 {
                let (_snap, guard) = mgr.snapshot(TxnId::INVALID);
                // The snapshot is registered for as long as the guard lives.
                debug_assert!(mgr.snapshot_xmin_registry().contains_key(&guard.xmin()));
                drop(guard);
            }
        }));
    }
    for (i, w) in workers.into_iter().enumerate() {
        watch(w, &format!("register/unregister worker {i}"));
    }
    assert!(
        mgr.snapshot_xmin_registry().is_empty(),
        "registry must drain to empty after all guards drop"
    );
    assert_eq!(mgr.live_registered_snapshots(), 0);
    assert_count_consistent(&mgr);
}

/// B1 regression (tech-selection §3.3 v1.3): hammer `snapshot()` from many
/// threads while another thread continuously reads
/// `oldest_snapshot_xmin()` — the horizon must NEVER exceed the xmin of any
/// snapshot that has been returned but not yet dropped. A caller-wrapper
/// (construct-then-register) shape would fail this deterministically
/// through the construction→registration window.
#[test]
fn atomicity_stress_horizon_never_exceeds_live_snapshot() {
    let mgr = Arc::new(manager());
    // Test-side mirror of "returned but not yet dropped" snapshot xmins.
    // Workers insert AFTER `snapshot()` returns (already registered) and
    // remove BEFORE dropping the guard (still registered), so
    // `in mirror ⇒ in registry` holds at every instant.
    let in_flight: Arc<Mutex<BTreeMap<TxnId, usize>>> = Arc::new(Mutex::new(BTreeMap::new()));
    let stop = Arc::new(AtomicBool::new(false));

    let mut workers = Vec::new();
    // Snapshot hammer threads.
    for _ in 0..8 {
        let mgr = Arc::clone(&mgr);
        let in_flight = Arc::clone(&in_flight);
        workers.push(thread::spawn(move || {
            for _ in 0..500 {
                let (snap, guard) = mgr.snapshot(TxnId::INVALID);
                {
                    let mut map = in_flight.lock().unwrap();
                    *map.entry(snap.xmin()).or_insert(0) += 1;
                }
                thread::yield_now();
                {
                    let mut map = in_flight.lock().unwrap();
                    let count = map.get_mut(&snap.xmin()).unwrap();
                    *count -= 1;
                    if *count == 0 {
                        map.remove(&snap.xmin());
                    }
                }
                drop(guard);
            }
        }));
    }
    // XID churn threads: keep the clock and active set moving so xmin
    // values change under the horizon reader (new BEGINs enter the active
    // set mid-run — the §3.3 v1.4 monotonicity scenario). Engine-faithful
    // shape (begin → snapshot → drop → commit): a bare begin/commit churn
    // thread never registers a snapshot, which is a form the engine never
    // produces and could let a sampling instant find every worker guard
    // momentarily absent.
    for _ in 0..2 {
        let mgr = Arc::clone(&mgr);
        workers.push(thread::spawn(move || {
            for _ in 0..500 {
                let xid = mgr.begin_txn();
                let (_snap, guard) = mgr.snapshot(xid);
                thread::yield_now();
                drop(guard);
                mgr.commit_txn(xid).unwrap();
            }
        }));
    }

    // Horizon reader: oldest is read FIRST, then the mirror's minimum —
    // every entry in the mirror was already registered when `oldest` was
    // sampled (see the mirror invariant above), so `oldest <= min` must
    // hold unconditionally.
    let reader = {
        let mgr = Arc::clone(&mgr);
        let in_flight = Arc::clone(&in_flight);
        let stop = Arc::clone(&stop);
        thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let oldest = mgr.oldest_snapshot_xmin();
                let map = in_flight.lock().unwrap();
                if let Some(&min_live) = map.keys().next() {
                    assert!(
                        oldest <= min_live,
                        "horizon {oldest:?} skipped past live snapshot xmin {min_live:?} \
                         (B1 construction→registration window)"
                    );
                }
            }
        })
    };

    for (i, w) in workers.into_iter().enumerate() {
        watch(w, &format!("atomicity stress worker {i}"));
    }
    stop.store(true, Ordering::Relaxed);
    watch(reader, "horizon reader");

    assert!(mgr.snapshot_xmin_registry().is_empty());
    assert_count_consistent(&mgr);
}
