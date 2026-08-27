//! M3 Stage E (tech-selection §6.2) acceptance: the Engine introspection
//! APIs — `active_xids` / `wait_edges` / `table_lock_state` /
//! `oldest_snapshot_xmin` / `clog_hit_rate` / `buffer_pool_hit_rate`.
//!
//! Covered:
//!
//! - a constructed lock-wait fixture (table-lock waiter + a row-lock edge)
//!   produces exactly the expected wait-for graph — the same
//!   `pg_txn::wait_for_edges` composition the deadlock detector consumes;
//! - known hit/miss sequences at the CLOG and buffer pool produce exactly
//!   the expected counter deltas, and the Engine-level rates match the
//!   component counters exactly;
//! - concurrent snapshots pin `oldest_snapshot_xmin` at the oldest live
//!   snapshot's xmin, and the horizon advances as handles commit.
//!
//! Acceptance: `cargo test -p pg-engine --test m3_introspection`

use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use pg_engine::{Engine, EngineConfig};
use pg_storage::types::{Oid, TxnId};
use pg_txn::{ClogAccessor, LockMode};
use tempfile::TempDir;

/// Watchdog budget for the polling loop (a regression FAILS, never hangs —
/// the Stage T convention).
const WATCHDOG: Duration = Duration::from_secs(120);

fn open(dir: &std::path::Path) -> Engine {
    Engine::open(dir, EngineConfig::new(dir)).unwrap()
}

/// Poll `cond` until it holds; panic (fail, not hang) after the deadline.
fn wait_until(cond: impl Fn() -> bool, what: &str) {
    let deadline = Instant::now() + WATCHDOG;
    while !cond() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        thread::sleep(Duration::from_millis(5));
    }
}

/// Lock-wait fixture: `txn_a` holds `RowExclusive` on `t` (an in-transaction
/// INSERT), while a worker's `txn_b` queues for `AccessExclusive`. No wait
/// cycle exists (txn_a waits on nobody), so the deadlock detector never
/// fires. The caller releases the worker by committing `txn_a`, then joins.
#[test]
fn wait_edges_compose_row_and_table_halves() {
    let tmp = TempDir::new().unwrap();
    let engine = Arc::new(open(tmp.path()));
    engine.exec(None, "CREATE TABLE t (id INT)").unwrap();
    let oid = engine.describe_table("t").unwrap().oid;

    let txn_a = engine.begin_txn().unwrap();
    let xid_a = txn_a.xid();
    engine
        .exec(Some(&txn_a), "INSERT INTO t VALUES (1)")
        .unwrap();

    let worker = {
        let engine = Arc::clone(&engine);
        thread::spawn(move || {
            let txn_b = engine.begin_txn().unwrap();
            let xid_b = txn_b.xid();
            // Blocks in the FIFO wait queue until txn_a releases its lock.
            engine
                .lock_manager()
                .acquire(xid_b, oid, LockMode::AccessExclusive)
                .expect("acquire after holder commits");
            txn_b.abort().expect("abort releases the acquired lock");
            xid_b
        })
    };
    // Barrier-style observation (no fixed sleep): the waiter's XID is read
    // back out of the queue once it appears.
    wait_until(
        || {
            engine
                .table_lock_state(oid)
                .is_some_and(|s| !s.waiters.is_empty())
        },
        "txn_b to queue for AccessExclusive",
    );
    let xid_b = engine.table_lock_state(oid).unwrap().waiters[0].0;

    // Engine-level table_lock_state: granted + waiters visible.
    let state = engine.table_lock_state(oid).unwrap();
    assert_eq!(state.granted, vec![(xid_a, LockMode::RowExclusive)]);
    assert_eq!(state.waiters, vec![(xid_b, LockMode::AccessExclusive)]);
    // Unknown table: no state at all.
    assert!(engine.table_lock_state(Oid(999_999)).is_none());

    // Table half of the wait-for graph: the waiter points at the holder.
    assert_eq!(engine.wait_edges(), vec![(xid_b, xid_a)]);

    // Add a row-lock edge from a third active transaction (no cycle, so
    // the deadlock detector stays out of the test).
    let txn_c = engine.begin_txn().unwrap();
    let xid_c = txn_c.xid();
    engine.txn_manager().register_row_wait(xid_c, xid_a);
    assert_eq!(engine.wait_edges(), vec![(xid_b, xid_a), (xid_c, xid_a)]);
    engine.txn_manager().unregister_row_wait(xid_c);
    assert_eq!(engine.wait_edges(), vec![(xid_b, xid_a)]);

    // active_xids sees all three, sorted.
    assert_eq!(engine.active_xids(), vec![xid_a, xid_b, xid_c]);

    // Cleanup: committing the holder releases its locks; the worker's
    // acquire then succeeds and it aborts cleanly.
    txn_c.commit().unwrap();
    txn_a.commit().unwrap();
    let joined = worker.join().expect("worker panicked");
    assert_eq!(joined, xid_b);
    assert_eq!(engine.wait_edges(), Vec::<(TxnId, TxnId)>::new());
    assert!(engine.table_lock_state(oid).is_none());
}

#[test]
fn clog_hit_rate_matches_known_sequence() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());

    // Warm up: make the CLOG page covering small XIDs resident regardless
    // of what recovery touched.
    let _ = engine.clog().get_state(TxnId(1));

    let hits0 = engine.clog().hits();
    let misses0 = engine.clog().misses();
    // XIDs 1..=8 live on CLOG page 0 (128K XIDs per page): every lookup is
    // a hit on the resident page.
    const N: u64 = 8;
    for i in 1..=N {
        let _ = engine.clog().get_state(TxnId(i));
    }
    assert_eq!(engine.clog().hits(), hits0 + N);
    assert_eq!(engine.clog().misses(), misses0);

    // The Engine introspection API is the same number the component
    // reports.
    let hits = engine.clog().hits() as f64;
    let total = hits + engine.clog().misses() as f64;
    assert_eq!(engine.clog_hit_rate(), engine.clog().hit_rate());
    assert_eq!(engine.clog_hit_rate(), hits / total);
}

#[test]
fn buffer_pool_hit_rate_matches_known_sequence() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine.exec(None, "CREATE TABLE t (id INT)").unwrap();
    engine.exec(None, "INSERT INTO t VALUES (1)").unwrap();
    let first_page = engine.describe_table("t").unwrap().first_page;
    let pool = engine.storage().buffer_pool();

    // Warm up: the page is resident regardless of what open/insert pinned.
    drop(pool.pin(first_page).unwrap());

    let hits0 = pool.hits();
    let misses0 = pool.misses();
    // Every pin of a resident page is a hit; nothing is read from disk.
    const N: u64 = 8;
    for _ in 0..N {
        drop(pool.pin(first_page).unwrap());
    }
    assert_eq!(pool.hits(), hits0 + N);
    assert_eq!(pool.misses(), misses0);

    let hits = pool.hits() as f64;
    let total = hits + pool.misses() as f64;
    assert_eq!(engine.buffer_pool_hit_rate(), pool.hit_rate());
    assert_eq!(engine.buffer_pool_hit_rate(), hits / total);
}

#[test]
fn oldest_snapshot_xmin_tracks_concurrent_snapshots() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());

    // No snapshots, no active transactions: horizon is the XID clock's
    // current value.
    let h0 = engine.oldest_snapshot_xmin();
    assert!(h0.0 >= 1);

    let txn_a = engine.begin_txn().unwrap();
    let xid_a = txn_a.xid();
    // txn_a's snapshot registered xmin = xid_a (only active transaction).
    assert_eq!(engine.oldest_snapshot_xmin(), xid_a);

    // A concurrent second transaction's snapshot has the same xmin (xid_a
    // was in its active set), so the horizon stays put.
    let txn_b = engine.begin_txn().unwrap();
    let xid_b = txn_b.xid();
    assert!(xid_b > xid_a);
    assert_eq!(engine.oldest_snapshot_xmin(), xid_a);

    // Committing the oldest transaction does NOT advance the horizon: its
    // xmin survives in txn_b's still-registered snapshot.
    txn_a.commit().unwrap();
    assert_eq!(engine.oldest_snapshot_xmin(), xid_a);

    // Once the last snapshot unregisters and the active set is empty, the
    // horizon falls back to the clock.
    txn_b.commit().unwrap();
    assert!(engine.active_xids().is_empty());
    assert!(engine.oldest_snapshot_xmin().0 > xid_b.0);
}
