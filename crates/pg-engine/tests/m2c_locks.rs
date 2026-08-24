//! M2c Stage P (part 2) acceptance: row-lock `t_xmax` protocol, SELECT ...
//! FOR UPDATE, and table-lock wiring at the engine level (tech-selection
//! §9.1/§9.2; coding-plan Stage P).
//!
//! Covered:
//!
//! - 100 threads × read-modify-write increments on the SAME row under
//!   `SELECT ... FOR UPDATE` → exact final value, no lost update
//!   (acceptance gate);
//! - FOR UPDATE blocks a concurrent writer until the locker's commit, and
//!   the locked row stays visible to plain readers (LOCK_ONLY is a lock,
//!   not a delete);
//! - FOR SHARE stamps a shared row lock (Stage S multixact lite) and the
//!   locked row stays visible to all readers;
//! - a concurrent UPDATE of the same row WAITS (does not error) on an
//!   in-progress stamper and proceeds after its abort;
//! - `TupleConcurrentlyUpdated` surfaces as the SQL-level error when the
//!   row was deleted+committed between the victim's snapshot and its
//!   update (SI semantics);
//! - table locks: DROP TABLE (AccessExclusive) blocks on a reader's
//!   AccessShare, then a later SELECT errors `TableNotFound`; CREATE INDEX
//!   (Exclusive) blocks on a writer's RowExclusive, and the build covers
//!   the blocked-behind writer's committed row (F1);
//! - an aborted transaction's lock-only row stamp is re-acquirable;
//! - DROP vs INSERT resolution race fails cleanly with `TableNotFound`
//!   (F3: deterministic via lock-queue polling);
//! - a lock-only stamp surviving a crash neither hides nor wedges the row
//!   after reopen (T1);
//! - cross-page (> PAGE_SIZE/2) updates under contention: no lost update,
//!   no latch deadlock (T3/F2).
//!
//! Acceptance: `cargo test -p pg-engine --test m2c_locks`

use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use pg_engine::{Datum, Engine, EngineConfig, EngineError, HeapError, QueryResult};
use tempfile::TempDir;

fn open(dir: &std::path::Path) -> Engine {
    Engine::open(dir, EngineConfig::new(dir)).unwrap()
}

/// Open an engine with table `counter (id INT, v INT)` preloaded with the
/// single row `(1, 0)`.
fn open_with_counter() -> (TempDir, Arc<Engine>) {
    let tmp = TempDir::new().unwrap();
    let engine = Arc::new(open(tmp.path()));
    engine
        .exec(None, "CREATE TABLE counter (id INT, v INT)")
        .unwrap();
    engine
        .exec(None, "INSERT INTO counter VALUES (1, 0)")
        .unwrap();
    (tmp, engine)
}

/// The current committed value of the counter row.
fn counter_value(engine: &Engine) -> i32 {
    match engine
        .exec(None, "SELECT v FROM counter WHERE id = 1")
        .unwrap()
    {
        QueryResult::Rows { rows, .. } => {
            assert_eq!(rows.len(), 1, "exactly one counter row must be visible");
            match &rows[0][0] {
                Some(Datum::Int4(v)) => *v,
                other => panic!("unexpected counter value: {other:?}"),
            }
        }
        other => panic!("expected Rows, got {other:?}"),
    }
}

/// Is this the §9.1 step-3 "committed concurrent writer" error?
fn is_concurrently_updated(e: &EngineError) -> bool {
    matches!(e, EngineError::Heap(HeapError::TupleConcurrentlyUpdated(_)))
}

/// Poll `pred` until it holds, failing after a deadline (test-orchestration
/// primitive for deterministic lock-state interleavings).
fn wait_until(what: &str, mut pred: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !pred() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        thread::sleep(Duration::from_millis(5));
    }
}

/// Spawn `f` on a worker reporting through a channel, assert it is still
/// blocked after `grace`, run `release`, then require the result within a
/// hard timeout — a regression (e.g. a lost wakeup) must FAIL the test,
/// not hang `cargo test` forever.
fn run_blocked_then<T>(
    what: &str,
    grace: Duration,
    f: impl FnOnce() -> pg_engine::Result<T> + Send + 'static,
    release: impl FnOnce(),
) -> T
where
    T: std::fmt::Debug + Send + 'static,
{
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let _ = tx.send(f());
    });
    match rx.recv_timeout(grace) {
        Err(mpsc::RecvTimeoutError::Timeout) => {} // still blocked, as expected
        other => panic!("{what}: expected to be blocked after {grace:?}, got {other:?}"),
    }
    release();
    match rx.recv_timeout(Duration::from_secs(10)) {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => panic!("{what}: failed after release: {e}"),
        Err(e) => panic!("{what}: still blocked 10s after release: {e}"),
    }
}

/// Stage P acceptance: 100 threads concurrently increment the same counter
/// row via read-modify-write in explicit transactions — final value exact,
/// no lost update.
///
/// The increment protocol per attempt is PG's `SELECT ... FOR UPDATE`
/// pattern: lock the row (blocking until any in-progress stamper ends),
/// read, write, commit. Under SI a transaction whose snapshot predates the
/// previous committer sees the OLD row version; locking that version fails
/// with `TupleConcurrentlyUpdated` (§9.1 step 3) and the attempt retries
/// with a fresh snapshot — PG's Repeatable Read reports the same condition
/// as `could not serialize access due to concurrent update`.
#[test]
fn concurrent_same_row_updates_no_lost_update() {
    const THREADS: usize = 100;
    const INCREMENTS_PER_THREAD: usize = 5;
    const MAX_ATTEMPTS: usize = 10_000;

    let (_tmp, engine) = open_with_counter();

    let mut handles = Vec::with_capacity(THREADS);
    for _ in 0..THREADS {
        let engine = Arc::clone(&engine);
        handles.push(thread::spawn(move || {
            for _ in 0..INCREMENTS_PER_THREAD {
                let mut attempts = 0;
                loop {
                    attempts += 1;
                    assert!(attempts <= MAX_ATTEMPTS, "increment retried too many times");
                    let txn = engine.begin_txn().unwrap();
                    let locked =
                        engine.exec(Some(&txn), "SELECT v FROM counter WHERE id = 1 FOR UPDATE");
                    let v = match locked {
                        Ok(QueryResult::Rows { rows, .. }) => {
                            assert_eq!(
                                rows.len(),
                                1,
                                "FOR UPDATE must see exactly one version of the counter row"
                            );
                            match &rows[0][0] {
                                Some(Datum::Int4(v)) => *v,
                                other => panic!("unexpected value: {other:?}"),
                            }
                        }
                        // Lost the race to a committer between our snapshot
                        // and the lock: retry with a fresh snapshot.
                        Err(e) if is_concurrently_updated(&e) => {
                            txn.abort().unwrap();
                            continue;
                        }
                        other => panic!("FOR UPDATE failed: {other:?}"),
                    };
                    let updated = engine.exec(
                        Some(&txn),
                        &format!("UPDATE counter SET v = {} WHERE id = 1", v + 1),
                    );
                    match updated {
                        Ok(QueryResult::Affected(1)) => {
                            txn.commit().unwrap();
                            break;
                        }
                        // Same lost-race retry as above (the lock was ours,
                        // so this can only come from a delete+commit).
                        Err(e) if is_concurrently_updated(&e) => {
                            txn.abort().unwrap();
                            continue;
                        }
                        other => panic!("UPDATE failed: {other:?}"),
                    }
                }
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(
        counter_value(&engine),
        (THREADS * INCREMENTS_PER_THREAD) as i32,
        "lost update: final counter value mismatch"
    );
    // No wait edges may outlive the run.
    assert!(engine.txn_manager().wait_edges().is_empty());
    engine.shutdown();
}

/// FOR UPDATE blocks a concurrent writer until the locker commits; the
/// locked row stays visible to plain readers throughout (a lock-only stamp
/// is not a delete), and the writer proceeds — it does NOT get
/// `TupleConcurrentlyUpdated`, because the locker never deleted the row.
///
/// The blocked state is asserted POSITIVELY via the wait-for graph (a
/// row-wait edge writer→locker must exist), not just by timing.
#[test]
fn for_update_blocks_writer_until_commit() {
    let (_tmp, engine) = open_with_counter();

    let locker = engine.begin_txn().unwrap();
    let res = engine
        .exec(
            Some(&locker),
            "SELECT v FROM counter WHERE id = 1 FOR UPDATE",
        )
        .unwrap();
    assert!(matches!(res, QueryResult::Rows { .. }));

    // A locked row is still visible to a plain reader (LOCK_ONLY masked).
    assert_eq!(counter_value(&engine), 0);

    let engine2 = Arc::clone(&engine);
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let _ = tx.send(engine2.exec(None, "UPDATE counter SET v = 41 WHERE id = 1"));
    });
    // Positive evidence of the blocked state: the writer's row-wait edge
    // into the locker's XID is registered (§9.1 step 5a).
    wait_until("writer's row-wait edge", || {
        !engine.txn_manager().wait_edges().is_empty()
    });
    let edges = engine.txn_manager().wait_edges();
    assert_eq!(
        edges,
        vec![(edges[0].0, locker.xid())],
        "the single wait edge must point at the locker"
    );
    // And the writer is genuinely parked on it.
    assert!(
        rx.recv_timeout(Duration::from_millis(300)).is_err(),
        "writer must still be blocked while the lock is held"
    );

    locker.commit().unwrap();
    match rx.recv_timeout(Duration::from_secs(10)) {
        Ok(Ok(_)) => {}
        other => panic!("writer did not complete after the locker's commit: {other:?}"),
    }
    // The waiter cleared its own edge on wake.
    assert!(engine.txn_manager().wait_edges().is_empty());

    assert_eq!(counter_value(&engine), 41);
    engine.shutdown();
}

/// `FOR SHARE` (Stage S multixact lite) stamps a shared row lock
/// (`HEAP_XMAX_LOCK_ONLY` + `HEAP_XMAX_IS_SHARE`) and the locked row stays
/// visible to all readers (a shared lock is not a delete).
#[test]
fn for_share_locks_row_and_stays_visible() {
    let (_tmp, engine) = open_with_counter();

    // Auto-commit FOR SHARE: stamps shared lock, commits immediately.
    let res = engine
        .exec(None, "SELECT * FROM counter FOR SHARE")
        .unwrap();
    let QueryResult::Rows { rows, .. } = res else {
        panic!("auto-commit FOR SHARE must return rows, got {res:?}");
    };
    assert_eq!(rows.len(), 1);

    // Row is still visible to a plain reader after the auto-commit lock
    // is released.
    let res = engine.exec(None, "SELECT * FROM counter").unwrap();
    let QueryResult::Rows { rows, .. } = res else {
        panic!("SELECT after FOR SHARE must return rows, got {res:?}");
    };
    assert_eq!(rows.len(), 1);

    // In-txn FOR SHARE: lock persists until commit.
    let txn = engine.begin_txn().unwrap();
    let res = engine
        .exec(Some(&txn), "SELECT * FROM counter FOR SHARE")
        .unwrap();
    let QueryResult::Rows { rows, .. } = res else {
        panic!("in-txn FOR SHARE must return rows, got {res:?}");
    };
    assert_eq!(rows.len(), 1);

    // A concurrent auto-commit reader still sees the row — the shared
    // lock does not hide it (LOCK_ONLY masks xmax to INVALID).
    let res = engine.exec(None, "SELECT * FROM counter").unwrap();
    let QueryResult::Rows { rows, .. } = res else {
        panic!("concurrent reader must see FOR SHARE locked row, got {res:?}");
    };
    assert_eq!(rows.len(), 1);

    txn.commit().unwrap();
    engine.shutdown();
}

/// Concurrent UPDATE of the same row: the second updater WAITS on the
/// in-progress first stamper (§9.1 step 5) instead of erroring, and
/// proceeds once the first transaction aborts — both transactions end
/// cleanly and the ordering is respected.
#[test]
fn concurrent_update_waits_then_proceeds_on_abort() {
    let (_tmp, engine) = open_with_counter();

    let first = engine.begin_txn().unwrap();
    let res = engine
        .exec(Some(&first), "UPDATE counter SET v = 100 WHERE id = 1")
        .unwrap();
    assert!(matches!(res, QueryResult::Affected(1)));

    let engine2 = Arc::clone(&engine);
    run_blocked_then(
        "second UPDATE of an in-progress-updated row",
        Duration::from_millis(300),
        move || {
            let second = engine2.begin_txn().unwrap();
            let r = engine2.exec(Some(&second), "UPDATE counter SET v = 200 WHERE id = 1");
            if r.is_ok() {
                second.commit()?;
            } else {
                second.abort()?;
            }
            r
        },
        move || first.abort().unwrap(),
    );

    // The first transaction aborted: its write is gone, the second's holds.
    assert_eq!(counter_value(&engine), 200);
    assert!(engine.txn_manager().wait_edges().is_empty());
    engine.shutdown();
}

/// SI write-conflict surfacing: the victim scanned the row, then a
/// concurrent transaction deleted and COMMITTED it; the victim's UPDATE
/// must fail with `TupleConcurrentlyUpdated` — distinct from
/// "row does not exist".
#[test]
fn committed_concurrent_delete_surfaces_concurrently_updated() {
    let (_tmp, engine) = open_with_counter();

    let victim = engine.begin_txn().unwrap();
    // The victim's scan (snapshot taken at begin).
    let res = engine
        .exec(Some(&victim), "SELECT v FROM counter WHERE id = 1")
        .unwrap();
    assert!(matches!(res, QueryResult::Rows { .. }));

    // Concurrent delete + commit between the scan and the update.
    engine
        .exec(None, "DELETE FROM counter WHERE id = 1")
        .unwrap();

    let err = engine
        .exec(Some(&victim), "UPDATE counter SET v = 9 WHERE id = 1")
        .unwrap_err();
    assert!(
        is_concurrently_updated(&err),
        "expected TupleConcurrentlyUpdated, got {err:?}"
    );
    victim.abort().unwrap();

    // The delete stands: the row is gone for fresh snapshots.
    match engine
        .exec(None, "SELECT v FROM counter WHERE id = 1")
        .unwrap()
    {
        QueryResult::Rows { rows, .. } => assert!(rows.is_empty()),
        other => panic!("expected Rows, got {other:?}"),
    }
    engine.shutdown();
}

/// Table locks (§9.2): DROP TABLE's AccessExclusive blocks on a reader's
/// AccessShare until the reader commits; afterwards the table is gone and
/// a fresh SELECT errors `TableNotFound` — one coherent outcome.
#[test]
fn drop_table_blocks_on_reader_then_select_fails() {
    let (_tmp, engine) = open_with_counter();

    let reader = engine.begin_txn().unwrap();
    let res = engine.exec(Some(&reader), "SELECT * FROM counter").unwrap();
    assert!(matches!(res, QueryResult::Rows { .. })); // holds AccessShare

    let engine2 = Arc::clone(&engine);
    run_blocked_then(
        "DROP TABLE vs in-flight AccessShare reader",
        Duration::from_millis(300),
        move || engine2.drop_table("counter"),
        move || reader.commit().unwrap(),
    );

    let err = engine.exec(None, "SELECT * FROM counter").unwrap_err();
    assert!(
        matches!(err, EngineError::TableNotFound(_)),
        "SELECT after committed DROP must be TableNotFound, got {err:?}"
    );
    engine.shutdown();
}

/// CREATE INDEX takes Exclusive (§9.2), which blocks behind a writer's
/// RowExclusive until the writer commits (smoke level) — and the index
/// must cover the row that writer committed (F1: the build re-snapshots
/// after the lock wait, so no committed row is silently skipped).
#[test]
fn create_index_exclusive_blocks_on_writer() {
    let (_tmp, engine) = open_with_counter();

    let writer = engine.begin_txn().unwrap();
    engine
        .exec(Some(&writer), "INSERT INTO counter VALUES (2, 2)")
        .unwrap(); // holds RowExclusive

    let engine2 = Arc::clone(&engine);
    run_blocked_then(
        "CREATE INDEX vs in-flight RowExclusive writer",
        Duration::from_millis(300),
        move || engine2.create_index("counter", "id").map(|_| ()),
        move || writer.commit().unwrap(),
    );

    // The index exists and resolves BOTH the preloaded row and the row
    // committed by the writer the build blocked behind.
    let tid = engine
        .index_lookup("counter", "id", &Datum::Int4(1))
        .unwrap();
    assert!(tid.is_some());
    let tid = engine
        .index_lookup("counter", "id", &Datum::Int4(2))
        .unwrap();
    assert!(
        tid.is_some(),
        "row committed by the writer the build blocked behind must be indexed"
    );
    engine.shutdown();
}

/// Lock release on abort: a transaction's lock-only row stamp stops
/// blocking waiters the moment the transaction aborts, and the row stays
/// fully usable (visible, updatable) afterwards.
#[test]
fn aborted_lock_only_stamp_is_reacquirable() {
    let (_tmp, engine) = open_with_counter();

    let locker = engine.begin_txn().unwrap();
    engine
        .exec(
            Some(&locker),
            "SELECT v FROM counter WHERE id = 1 FOR UPDATE",
        )
        .unwrap();

    let engine2 = Arc::clone(&engine);
    run_blocked_then(
        "second FOR UPDATE behind an aborting locker",
        Duration::from_millis(300),
        move || {
            let waiter = engine2.begin_txn().unwrap();
            let r = engine2.exec(
                Some(&waiter),
                "SELECT v FROM counter WHERE id = 1 FOR UPDATE",
            );
            if r.is_ok() {
                waiter.commit()?;
            } else {
                waiter.abort()?;
            }
            r
        },
        move || locker.abort().unwrap(),
    );

    // The aborted lock changed nothing: the row is visible and updatable.
    assert_eq!(counter_value(&engine), 0);
    engine
        .exec(None, "UPDATE counter SET v = 7 WHERE id = 1")
        .unwrap();
    assert_eq!(counter_value(&engine), 7);
    assert!(engine.txn_manager().wait_edges().is_empty());
    engine.shutdown();
}

/// Auto-commit FOR UPDATE is allowed (PG-consistent): the row lock is
/// stamped with the statement's own short-lived transaction and released
/// when it commits, so a later writer is not blocked.
#[test]
fn auto_commit_for_update_releases_at_statement_end() {
    let (_tmp, engine) = open_with_counter();

    let res = engine
        .exec(None, "SELECT v FROM counter WHERE id = 1 FOR UPDATE")
        .unwrap();
    assert!(matches!(res, QueryResult::Rows { .. }));

    // The lock died with the auto-commit statement: an immediate writer
    // proceeds without waiting (no 300ms grace needed — any wait here is
    // a bug).
    engine
        .exec(None, "UPDATE counter SET v = 3 WHERE id = 1")
        .unwrap();
    assert_eq!(counter_value(&engine), 3);
    engine.shutdown();
}

/// F3 regression: a DML statement that resolves the table entry and then
/// queues BEHIND an in-flight DROP must fail with `TableNotFound` once its
/// lock is granted — never write through the stale entry into freed
/// (potentially reallocated) pages. The interleaving is deterministic:
/// the reader's AccessShare makes the drop queue, and the insert queues
/// behind the drop (FIFO), so the insert's lock is granted only after the
/// drop has committed and removed the registry entry.
#[test]
fn drop_table_vs_insert_resolution_race_fails_cleanly() {
    let (_tmp, engine) = open_with_counter();
    let oid = engine.describe_table("counter").unwrap().oid;

    // 1. A reader holds AccessShare, so the DROP's AccessExclusive queues.
    let reader = engine.begin_txn().unwrap();
    engine.exec(Some(&reader), "SELECT * FROM counter").unwrap();

    // 2. Start the DROP; wait until its AccessExclusive is queued.
    let engine2 = Arc::clone(&engine);
    let (drop_tx, drop_rx) = mpsc::channel();
    thread::spawn(move || {
        let _ = drop_tx.send(engine2.drop_table("counter"));
    });
    wait_until("DROP's AccessExclusive queued", || {
        engine
            .lock_manager()
            .table_lock_state(oid)
            .is_some_and(|s| !s.waiters.is_empty())
    });

    // 3. Start the INSERT; it resolves the (still registered) entry, then
    //    its RowExclusive queues behind the DROP. Wait for that queue
    //    position — this is the TOCTOU window made deterministic.
    let engine3 = Arc::clone(&engine);
    let (ins_tx, ins_rx) = mpsc::channel();
    thread::spawn(move || {
        let _ = ins_tx.send(engine3.exec(None, "INSERT INTO counter VALUES (9, 9)"));
    });
    wait_until("INSERT's RowExclusive queued behind the DROP", || {
        engine
            .lock_manager()
            .table_lock_state(oid)
            .is_some_and(|s| s.waiters.len() >= 2)
    });

    // 4. Release the reader: the DROP runs (frees pages, removes the
    //    registry entry, commits), then the INSERT's lock is granted.
    reader.commit().unwrap();
    match drop_rx.recv_timeout(Duration::from_secs(10)) {
        Ok(Ok(())) => {}
        other => panic!("DROP did not complete: {other:?}"),
    }
    match ins_rx.recv_timeout(Duration::from_secs(10)) {
        Ok(Err(EngineError::TableNotFound(_))) => {}
        other => panic!("INSERT after the DROP must fail with TableNotFound, got {other:?}"),
    }
    engine.shutdown();
}

/// T1: a lock-only stamp that reaches disk (FOR UPDATE + checkpoint) and
/// whose owner CRASHES must not haunt the recovery: after reopen, the row
/// is visible (a lock is not a delete), re-lockable, and updatable — the
/// gate's crashed-stamper path (CLOG `InProgress`, not in the active set)
/// treats the stamp as aborted instead of waiting forever.
#[test]
fn lock_only_stamp_survives_crash_without_hiding_row() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine
        .exec(None, "CREATE TABLE counter (id INT, v INT)")
        .unwrap();
    engine
        .exec(None, "INSERT INTO counter VALUES (1, 0)")
        .unwrap();

    let locker = engine.begin_txn().unwrap();
    engine
        .exec(
            Some(&locker),
            "SELECT v FROM counter WHERE id = 1 FOR UPDATE",
        )
        .unwrap();
    // Push the lock-stamped page (and the CLOG) to disk.
    engine.checkpoint().unwrap();
    // Simulate a crash: skip ALL Drop impls — no auto-abort of the locker,
    // no clean shutdown. The lock-only stamp is on disk, its owner never
    // ended.
    std::mem::forget(locker);
    std::mem::forget(engine);

    let engine = open(tmp.path());
    // Visible (LOCK_ONLY masked), updatable (crashed stamp treated as
    // aborted), and re-lockable afterwards.
    assert_eq!(counter_value(&engine), 0);
    engine
        .exec(None, "UPDATE counter SET v = 5 WHERE id = 1")
        .unwrap();
    assert_eq!(counter_value(&engine), 5);
    let txn = engine.begin_txn().unwrap();
    engine
        .exec(Some(&txn), "SELECT v FROM counter WHERE id = 1 FOR UPDATE")
        .unwrap();
    txn.commit().unwrap();
    engine.shutdown();
}

/// T3: the cross-page update path under contention. Rows are padded past
/// PAGE_SIZE/2 so an update NEVER fits beside the old version and always
/// takes the two-latch cross-page path; concurrent increments via FOR
/// UPDATE must serialize with no lost update and no latch deadlock (the
/// two-latch acquisition is PageId-ordered, smaller first). A deadlock
/// regression fails via the supervisor timeout instead of hanging.
#[test]
fn cross_page_update_under_contention() {
    const THREADS: usize = 10;
    const PER_THREAD: usize = 3;

    let tmp = TempDir::new().unwrap();
    let engine = Arc::new(open(tmp.path()));
    engine
        .exec(None, "CREATE TABLE big (id INT, v INT, pad TEXT)")
        .unwrap();
    // > PAGE_SIZE/2 of payload: two versions can never share a page.
    let pad_len = pg_storage::types::PAGE_SIZE / 2 + 256;
    let pad = "x".repeat(pad_len);
    engine
        .exec(None, &format!("INSERT INTO big VALUES (1, 0, '{pad}')"))
        .unwrap();

    let (tx, rx) = mpsc::channel();
    let engine2 = Arc::clone(&engine);
    thread::spawn(move || {
        let mut handles = Vec::new();
        for t in 0..THREADS {
            let engine = Arc::clone(&engine2);
            let pad = "y".repeat(pad_len);
            handles.push(thread::spawn(move || {
                for _ in 0..PER_THREAD {
                    let mut attempts = 0;
                    loop {
                        attempts += 1;
                        assert!(attempts <= 10_000, "cross-page increment retried too often");
                        let txn = engine.begin_txn().unwrap();
                        let locked =
                            engine.exec(Some(&txn), "SELECT v FROM big WHERE id = 1 FOR UPDATE");
                        let v = match locked {
                            Ok(QueryResult::Rows { rows, .. }) => {
                                assert_eq!(rows.len(), 1);
                                match &rows[0][0] {
                                    Some(Datum::Int4(v)) => *v,
                                    other => panic!("unexpected value: {other:?}"),
                                }
                            }
                            Err(e) if is_concurrently_updated(&e) => {
                                txn.abort().unwrap();
                                continue;
                            }
                            other => panic!("FOR UPDATE failed: {other:?}"),
                        };
                        // A fresh pad per write keeps the new version large
                        // (and distinct), forcing the cross-page path.
                        let upd = engine.exec(
                            Some(&txn),
                            &format!(
                                "UPDATE big SET v = {}, pad = '{}{}' WHERE id = 1",
                                v + 1,
                                pad,
                                t
                            ),
                        );
                        match upd {
                            Ok(QueryResult::Affected(1)) => {
                                txn.commit().unwrap();
                                break;
                            }
                            Err(e) if is_concurrently_updated(&e) => {
                                txn.abort().unwrap();
                                continue;
                            }
                            other => panic!("UPDATE failed: {other:?}"),
                        }
                    }
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        let _ = tx.send(());
    });
    rx.recv_timeout(Duration::from_secs(120))
        .expect("cross-page concurrent updates deadlocked or ran too long");

    let v = match engine.exec(None, "SELECT v FROM big WHERE id = 1").unwrap() {
        QueryResult::Rows { rows, .. } => match &rows[0][0] {
            Some(Datum::Int4(v)) => *v,
            other => panic!("unexpected value: {other:?}"),
        },
        other => panic!("expected Rows, got {other:?}"),
    };
    assert_eq!(
        v,
        (THREADS * PER_THREAD) as i32,
        "lost update on cross-page path"
    );
    engine.shutdown();
}

/// H5 (post-Stage-S review): share/share coexistence. Two transactions hold
/// FOR SHARE on the same row at the same time — pre-H5 the second locker
/// blocked on the first's stamp (shared locks conflicted like exclusive
/// ones, producing waits and false deadlocks PG wouldn't have); now both
/// proceed immediately and no wait edge is ever registered.
#[test]
fn for_share_coexists_with_for_share() {
    let (_tmp, engine) = open_with_counter();

    let a = engine.begin_txn().unwrap();
    let b = engine.begin_txn().unwrap();
    let res = engine
        .exec(Some(&a), "SELECT v FROM counter WHERE id = 1 FOR SHARE")
        .unwrap();
    assert!(matches!(res, QueryResult::Rows { .. }));

    // The second FOR SHARE must NOT block: run it on a worker with a
    // watchdog so a regression fails the test instead of hanging it.
    let engine2 = Arc::clone(&engine);
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let r = engine2.exec(Some(&b), "SELECT v FROM counter WHERE id = 1 FOR SHARE");
        let _ = tx.send((r, b));
    });
    let b = match rx.recv_timeout(Duration::from_secs(10)) {
        Ok((Ok(QueryResult::Rows { rows, .. }), b)) => {
            assert_eq!(rows.len(), 1);
            b
        }
        Ok((other, _)) => panic!("second FOR SHARE must coexist with the first, got {other:?}"),
        Err(e) => panic!("second FOR SHARE blocked (share/share regression): {e}"),
    };
    assert!(
        engine.txn_manager().wait_edges().is_empty(),
        "share/share coexistence must never register a wait edge"
    );

    a.commit().unwrap();
    b.commit().unwrap();
    // Both locks died with their transactions: the row is freely writable.
    engine
        .exec(None, "UPDATE counter SET v = 9 WHERE id = 1")
        .unwrap();
    assert_eq!(counter_value(&engine), 9);
    engine.shutdown();
}

/// H5: a writer must wait for ALL registered share holders, not just the
/// one named in `t_xmax`. Two transactions FOR SHARE the row; a concurrent
/// UPDATE stays blocked after the FIRST holder commits and completes only
/// after the second one does.
#[test]
fn writer_waits_for_all_share_holders() {
    let (_tmp, engine) = open_with_counter();

    let a = engine.begin_txn().unwrap();
    let b = engine.begin_txn().unwrap();
    engine
        .exec(Some(&a), "SELECT v FROM counter WHERE id = 1 FOR SHARE")
        .unwrap();
    engine
        .exec(Some(&b), "SELECT v FROM counter WHERE id = 1 FOR SHARE")
        .unwrap();

    let engine2 = Arc::clone(&engine);
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let _ = tx.send(engine2.exec(None, "UPDATE counter SET v = 41 WHERE id = 1"));
    });
    wait_until("writer's row-wait edge", || {
        !engine.txn_manager().wait_edges().is_empty()
    });

    // The FIRST holder commits: the writer must STILL be blocked on the
    // second (pre-H5 the stamp named only one holder, so the writer would
    // have proceeded here).
    a.commit().unwrap();
    assert!(
        rx.recv_timeout(Duration::from_millis(300)).is_err(),
        "writer must wait for ALL share holders, not just the first"
    );

    b.commit().unwrap();
    match rx.recv_timeout(Duration::from_secs(10)) {
        Ok(Ok(_)) => {}
        other => panic!("writer did not complete after the last holder's commit: {other:?}"),
    }
    assert_eq!(counter_value(&engine), 41);
    assert!(engine.txn_manager().wait_edges().is_empty());
    engine.shutdown();
}

/// H5: same-transaction FOR SHARE → FOR UPDATE / UPDATE upgrade. The sole
/// share holder upgrades without waiting; with a second holder present the
/// upgrade waits for it and proceeds after its commit. A FOR SHARE on a row
/// the transaction already holds EXCLUSIVELY does not downgrade the stamp.
#[test]
fn for_share_upgrades_within_same_txn() {
    let (_tmp, engine) = open_with_counter();

    // Sole holder: upgrade to FOR UPDATE, then a real UPDATE — all inside
    // one transaction, no waiting.
    let t = engine.begin_txn().unwrap();
    engine
        .exec(Some(&t), "SELECT v FROM counter WHERE id = 1 FOR SHARE")
        .unwrap();
    engine
        .exec(Some(&t), "SELECT v FROM counter WHERE id = 1 FOR UPDATE")
        .unwrap();
    // FOR SHARE on top of the exclusive lock: no downgrade, no error.
    engine
        .exec(Some(&t), "SELECT v FROM counter WHERE id = 1 FOR SHARE")
        .unwrap();
    let res = engine
        .exec(Some(&t), "UPDATE counter SET v = 5 WHERE id = 1")
        .unwrap();
    assert!(matches!(res, QueryResult::Affected(1)));
    t.commit().unwrap();
    assert_eq!(counter_value(&engine), 5);

    // Two holders: one holder's upgrade waits for the OTHER holder.
    let a = engine.begin_txn().unwrap();
    let b = engine.begin_txn().unwrap();
    engine
        .exec(Some(&a), "SELECT v FROM counter WHERE id = 1 FOR SHARE")
        .unwrap();
    engine
        .exec(Some(&b), "SELECT v FROM counter WHERE id = 1 FOR SHARE")
        .unwrap();

    let engine2 = Arc::clone(&engine);
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let r = engine2.exec(Some(&a), "UPDATE counter SET v = 6 WHERE id = 1");
        if r.is_ok() {
            a.commit().unwrap();
        } else {
            a.abort().unwrap();
        }
        let _ = tx.send(r);
    });
    assert!(
        rx.recv_timeout(Duration::from_millis(300)).is_err(),
        "the upgrade must wait for the other share holder"
    );
    b.commit().unwrap();
    match rx.recv_timeout(Duration::from_secs(10)) {
        Ok(Ok(QueryResult::Affected(1))) => {}
        other => panic!("upgrade did not complete after the other holder's commit: {other:?}"),
    }
    assert_eq!(counter_value(&engine), 6);
    assert!(engine.txn_manager().wait_edges().is_empty());
    engine.shutdown();
}

/// H5: mixed FOR SHARE / FOR UPDATE deadlock detection still works. Two
/// transactions share-lock OPPOSITE rows, then both try to write: each
/// upgrade waits for the other holder, closing a wait-for cycle that the
/// detector breaks by aborting the younger transaction.
#[test]
fn for_share_upgrade_deadlock_detected() {
    let (_tmp, engine) = open_with_counter();
    engine
        .exec(None, "INSERT INTO counter VALUES (2, 2)")
        .unwrap();

    let a = engine.begin_txn().unwrap();
    let b = engine.begin_txn().unwrap();
    assert!(b.xid() > a.xid(), "b is the younger transaction");
    engine
        .exec(Some(&a), "SELECT v FROM counter WHERE id = 1 FOR SHARE")
        .unwrap();
    engine
        .exec(Some(&b), "SELECT v FROM counter WHERE id = 2 FOR SHARE")
        .unwrap();

    // a attacks row 2 (waits on b's share); on success it commits.
    let engine1 = Arc::clone(&engine);
    let (tx_a, rx_a) = mpsc::channel();
    thread::spawn(move || {
        let r = engine1.exec(Some(&a), "UPDATE counter SET v = 12 WHERE id = 2");
        match &r {
            Ok(_) => a.commit().unwrap(),
            Err(_) => a.abort().unwrap(),
        }
        let _ = tx_a.send(r);
    });
    wait_until("a's wait edge into b", || {
        !engine.txn_manager().wait_edges().is_empty()
    });

    // b attacks row 1: the cycle closes here. The detector (100ms tick)
    // must interrupt b — run it on a worker so a regression fails on a
    // timeout instead of hanging the suite.
    let engine2 = Arc::clone(&engine);
    let (tx_b, rx_b) = mpsc::channel();
    thread::spawn(move || {
        let r = engine2.exec(Some(&b), "UPDATE counter SET v = 11 WHERE id = 1");
        let _ = tx_b.send((r, b));
    });
    let (victim_result, b) = rx_b
        .recv_timeout(Duration::from_secs(15))
        .expect("victim's UPDATE was not interrupted within 15s");
    assert!(
        matches!(
            victim_result,
            Err(EngineError::Heap(HeapError::DeadlockVictim))
        ),
        "younger txn must fail with HeapError::DeadlockVictim, got {victim_result:?}"
    );
    b.abort().unwrap();

    // The elder's blocked update now proceeds and its commit lands.
    let elder_result = rx_a
        .recv_timeout(Duration::from_secs(15))
        .expect("elder's UPDATE did not complete after the victim's abort");
    assert!(
        matches!(elder_result, Ok(QueryResult::Affected(1))),
        "elder's UPDATE must succeed after the victim's abort, got {elder_result:?}"
    );
    assert_eq!(counter_value(&engine), 0); // row 1 untouched
    match engine
        .exec(None, "SELECT v FROM counter WHERE id = 2")
        .unwrap()
    {
        QueryResult::Rows { rows, .. } => {
            assert_eq!(rows[0][0], Some(Datum::Int4(12)));
        }
        other => panic!("expected Rows, got {other:?}"),
    }
    assert!(engine.txn_manager().wait_edges().is_empty());
    engine.shutdown();
}
