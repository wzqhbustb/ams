//! M2b Stage O: 50-thread snapshot-isolation verification.
//!
//! Each thread begins a transaction, takes a snapshot (synchronized via a
//! barrier so all snapshots are taken before any thread inserts), then:
//!
//! 1. SELECT → sees exactly 1 pre-existing committed row (SI: concurrent
//!    transactions' uncommitted writes are invisible)
//! 2. INSERT one row
//! 3. SELECT → sees exactly 2 rows (1 pre-existing + 1 own write via
//!    earlier-command visibility)
//! 4. Commit
//!
//! After all threads commit, a final SELECT sees 51 rows (1 + 50).
//!
//! Acceptance: `cargo test -p pg-engine --test si_isolation_50_txn`

use std::sync::{Arc, Barrier};
use std::thread;

use pg_engine::{Engine, EngineConfig, QueryResult};
use tempfile::TempDir;

fn open(dir: &std::path::Path) -> Engine {
    Engine::open(dir, EngineConfig::new(dir)).unwrap()
}

#[test]
fn si_50_concurrent_transactions() {
    const N_THREADS: usize = 50;

    let tmp = TempDir::new().unwrap();
    let engine = Arc::new(open(tmp.path()));

    // Pre-existing committed row.
    engine
        .exec(None, "CREATE TABLE counter (id INT, val INT)")
        .unwrap();
    engine
        .exec(None, "INSERT INTO counter VALUES (0, 100)")
        .unwrap();

    // Barrier ensures all threads take their snapshot before any thread
    // inserts — so every thread's first SELECT sees exactly 1 row.
    let barrier = Arc::new(Barrier::new(N_THREADS));
    let mut handles = Vec::with_capacity(N_THREADS);

    for i in 0..N_THREADS {
        let engine = Arc::clone(&engine);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            let txn = engine.begin_txn().unwrap();

            // All threads reach the barrier after begin_txn — all snapshots
            // are taken before any thread proceeds to INSERT.
            barrier.wait();

            // SELECT 1: should see exactly 1 pre-existing row (SI).
            let res = engine.exec(Some(&txn), "SELECT * FROM counter").unwrap();
            match &res {
                QueryResult::Rows { rows, .. } => {
                    assert_eq!(
                        rows.len(),
                        1,
                        "thread {i}: expected 1 row before insert, got {}",
                        rows.len()
                    );
                }
                other => panic!("thread {i}: expected Rows, got {other:?}"),
            }

            // INSERT one row as this transaction.
            engine
                .exec(
                    Some(&txn),
                    &format!("INSERT INTO counter VALUES ({i}, {i})"),
                )
                .unwrap();

            // SELECT 2: should see exactly 2 rows (1 pre-existing + 1 own).
            let res = engine.exec(Some(&txn), "SELECT * FROM counter").unwrap();
            match &res {
                QueryResult::Rows { rows, .. } => {
                    assert_eq!(
                        rows.len(),
                        2,
                        "thread {i}: expected 2 rows after own insert, got {}",
                        rows.len()
                    );
                }
                other => panic!("thread {i}: expected Rows, got {other:?}"),
            }

            txn.commit().unwrap();
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // After all 50 threads commit, a fresh snapshot sees all 51 rows.
    let res = engine
        .exec(None, "SELECT * FROM counter ORDER BY id")
        .unwrap();
    match res {
        QueryResult::Rows { rows, .. } => {
            assert_eq!(
                rows.len(),
                N_THREADS + 1,
                "expected {} total rows",
                N_THREADS + 1
            );
        }
        other => panic!("expected Rows, got {other:?}"),
    }

    engine.shutdown();
}
