//! M2b Stage O review acceptance: index maintenance is transactional.
//!
//! The B+Tree holds `(key, tid)` entries with no MVCC metadata, so the
//! engine keeps a per-transaction index undo log: abort reverse-applies it
//! (explicit `TxnHandle::abort`, `TxnHandle::drop` auto-abort, and the
//! auto-commit failure path), commit discards it, and `index_lookup`
//! re-checks every candidate TID against heap visibility (duplicates
//! included — M2b indexes are non-unique).
//!
//! Acceptance: `cargo test -p pg-engine --test m2b_index_txn`

use pg_engine::{Datum, Engine, EngineConfig, QueryResult};
use tempfile::TempDir;

fn open(dir: &std::path::Path) -> Engine {
    Engine::open(dir, EngineConfig::new(dir)).unwrap()
}

/// Engine with table `t (id INT)` and an index on `id`, no rows.
fn setup() -> (TempDir, Engine) {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine.exec(None, "CREATE TABLE t (id INT)").unwrap();
    engine.exec(None, "CREATE INDEX ON t (id)").unwrap();
    (tmp, engine)
}

fn scan_count(engine: &Engine) -> usize {
    engine.scan("t", None).unwrap().len()
}

fn lookup(engine: &Engine, key: i32) -> bool {
    engine
        .index_lookup("t", "id", &Datum::Int4(key))
        .unwrap()
        .is_some()
}

/// The two F7 failure-injection tests below arm the process-global
/// commit-fail hook (`pg_txn::manager::test_hooks`); its ARMED claim panics
/// on concurrent arming, so the pair must be serialized, and disarm must be
/// panic-safe (2026-09-02: the pair raced under parallel test scheduling —
/// latent since the Stage F F7 fix, surfaced by a Stage C workspace run).
struct CommitFailHookGuard(std::sync::MutexGuard<'static, ()>);

static COMMIT_FAIL_HOOK_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

impl CommitFailHookGuard {
    /// Arm the hook under the file-local lock; drop the returned guard to
    /// disarm (Drop also runs on panic, so a failing test cannot wedge its
    /// sibling — the poisoned lock is recovered via `into_inner`).
    fn arm() -> Self {
        let guard = COMMIT_FAIL_HOOK_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        pg_txn::manager::test_hooks::set_commit_txn_force_fail(true);
        Self(guard)
    }
}

impl Drop for CommitFailHookGuard {
    fn drop(&mut self) {
        pg_txn::manager::test_hooks::set_commit_txn_force_fail(false);
    }
}

/// INSERT inside an explicit txn, then ABORT: the index entry must be gone
/// AND the heap scan must be empty.
#[test]
fn insert_abort_removes_index_entry() {
    let (_tmp, engine) = setup();
    let txn = engine.begin_txn().unwrap();
    engine.exec(Some(&txn), "INSERT INTO t VALUES (1)").unwrap();
    txn.abort().unwrap();
    assert!(
        !lookup(&engine, 1),
        "aborted insert left a dangling index entry"
    );
    assert_eq!(scan_count(&engine), 0, "aborted insert visible to scan");
}

/// DELETE inside an explicit txn, then ABORT: the index entry must be
/// restored AND the scan must find the row.
#[test]
fn delete_abort_restores_index_entry() {
    let (_tmp, engine) = setup();
    engine.exec(None, "INSERT INTO t VALUES (1)").unwrap();
    let txn = engine.begin_txn().unwrap();
    engine
        .exec(Some(&txn), "DELETE FROM t WHERE id = 1")
        .unwrap();
    txn.abort().unwrap();
    assert!(
        lookup(&engine, 1),
        "aborted delete lost the live row's index entry"
    );
    assert_eq!(
        scan_count(&engine),
        1,
        "aborted delete hid the row from scan"
    );
}

/// UPDATE (key change) inside an explicit txn, then ABORT: the old key
/// must resolve again, the new key must not, and the scan must show the
/// original row.
#[test]
fn update_abort_restores_old_key_entry() {
    let (_tmp, engine) = setup();
    engine.exec(None, "INSERT INTO t VALUES (1)").unwrap();
    let txn = engine.begin_txn().unwrap();
    engine
        .exec(Some(&txn), "UPDATE t SET id = 2 WHERE id = 1")
        .unwrap();
    txn.abort().unwrap();
    assert!(
        lookup(&engine, 1),
        "aborted update lost the old key's entry"
    );
    assert!(
        !lookup(&engine, 2),
        "aborted update left the new key's entry"
    );
    assert_eq!(scan_count(&engine), 1);
}

/// INSERT + COMMIT: the index entry stays and resolves.
#[test]
fn insert_commit_keeps_index_entry() {
    let (_tmp, engine) = setup();
    let txn = engine.begin_txn().unwrap();
    engine.exec(Some(&txn), "INSERT INTO t VALUES (7)").unwrap();
    txn.commit().unwrap();
    assert!(lookup(&engine, 7));
    assert_eq!(scan_count(&engine), 1);
}

/// Dropping the handle without commit auto-aborts: the index entry must be
/// undone too.
#[test]
fn txn_handle_drop_auto_abort_undoes_index() {
    let (_tmp, engine) = setup();
    {
        let txn = engine.begin_txn().unwrap();
        engine.exec(Some(&txn), "INSERT INTO t VALUES (3)").unwrap();
        // No commit/abort: drop triggers the best-effort auto-abort.
    }
    assert!(
        !lookup(&engine, 3),
        "drop auto-abort left a dangling index entry"
    );
    assert_eq!(scan_count(&engine), 0);
}

/// A failing auto-commit statement (row 2 violates the column type) must
/// undo row 1's index entry as well as its heap insert.
#[test]
fn auto_commit_failure_undoes_index() {
    let (_tmp, engine) = setup();
    let res = engine.exec(None, "INSERT INTO t VALUES (1), ('not-an-int')");
    assert!(res.is_err(), "type-mismatched row must fail the statement");
    assert!(
        !lookup(&engine, 1),
        "failed statement left a dangling index entry"
    );
    assert_eq!(
        scan_count(&engine),
        0,
        "failed statement left a visible row"
    );
}

/// Duplicate keys (non-unique index): two rows share key 5; after one is
/// deleted and committed, the lookup must skip the dead version and return
/// the live one.
#[test]
fn duplicate_keys_lookup_returns_visible_row() {
    let (_tmp, engine) = setup();
    engine.exec(None, "INSERT INTO t VALUES (5), (5)").unwrap();
    // Delete one of the two duplicates (auto-commit).
    let rows = engine.scan("t", None).unwrap();
    assert_eq!(rows.len(), 2);
    engine.delete("t", rows[0].0).unwrap();
    let tid = engine
        .index_lookup("t", "id", &Datum::Int4(5))
        .unwrap()
        .expect("one live duplicate must still resolve");
    assert_eq!(tid, rows[1].0, "lookup must skip the deleted duplicate");
    assert_eq!(scan_count(&engine), 1);
}

/// The same insert-abort scenario driven purely through SQL exec with an
/// explicit transaction.
#[test]
fn sql_exec_insert_abort_index_consistent() {
    let (_tmp, engine) = setup();
    let txn = engine.begin_txn().unwrap();
    engine.exec(Some(&txn), "INSERT INTO t VALUES (9)").unwrap();
    // Sanity: inside the txn the row is visible to its own SELECT.
    match engine.exec(Some(&txn), "SELECT * FROM t").unwrap() {
        QueryResult::Rows { rows, .. } => assert_eq!(rows.len(), 1),
        other => panic!("expected Rows, got {other:?}"),
    }
    txn.abort().unwrap();
    assert!(!lookup(&engine, 9));
    match engine.exec(None, "SELECT * FROM t").unwrap() {
        QueryResult::Rows { rows, .. } => assert!(rows.is_empty()),
        other => panic!("expected Rows, got {other:?}"),
    }
}

// ---------------------------------------------------------------------
// Stage T stress finding: recovery-time loser index compensation
// ---------------------------------------------------------------------

/// Crash with an in-flight DELETE (no commit/abort record) after a
/// checkpoint made its records durable. The heap side stays visible
/// through the CLOG (the xid reads InProgress), but the index delete is
/// physical (xid=0) and replays unconditionally — without compensation the
/// still-visible row loses its index entry. Recovery must re-insert it.
#[test]
fn crash_mid_delete_compensates_index_entry() {
    let tmp = TempDir::new().unwrap();
    {
        let engine = open(tmp.path());
        engine.exec(None, "CREATE TABLE t (id INT)").unwrap();
        engine.exec(None, "CREATE INDEX ON t (id)").unwrap();
        engine.exec(None, "INSERT INTO t VALUES (1)").unwrap();

        let txn = engine.begin_txn().unwrap();
        engine
            .exec(Some(&txn), "DELETE FROM t WHERE id = 1")
            .unwrap();
        // Durability barrier: the in-flight delete's records (heap xmax
        // stamp + index delete) must survive the kill; the checkpoint's
        // ATT snapshot also records the loser as active.
        engine.checkpoint().unwrap();
        // kill -9 with the transaction in flight: forget the handle
        // WITHOUT aborting, then abandon the engine mid-transaction.
        std::mem::forget(txn);
        std::mem::forget(engine);
    }

    let engine = open(tmp.path());
    assert_eq!(
        scan_count(&engine),
        1,
        "the uncommitted delete must stay invisible (row still present)"
    );
    assert!(
        lookup(&engine, 1),
        "recovery must compensate the loser's index delete (P0 forensic: \
         row reachable in the heap but not through the index)"
    );

    // Idempotency: crash again right after recovery (compensation records
    // may be unsynced) and re-verify — a second run must not duplicate the
    // entry or lose it.
    std::mem::forget(engine);
    let engine = open(tmp.path());
    assert_eq!(scan_count(&engine), 1);
    assert!(lookup(&engine, 1), "second recovery must converge");
}

/// Same shape through a non-HOT UPDATE (the indexed column changes): the
/// in-flight update's index maintenance is delete-old-key + insert-new-key;
/// after the crash the OLD key's entry must be restored (the heap keeps
/// the old version visible) and the new key's entry is masked by
/// visibility. This is the exact /tmp/conc_repro_round2 forensic shape.
#[test]
fn crash_mid_update_compensates_index_entry() {
    let tmp = TempDir::new().unwrap();
    {
        let engine = open(tmp.path());
        engine
            .exec(None, "CREATE TABLE t (id INT, name TEXT)")
            .unwrap();
        engine.exec(None, "CREATE INDEX ON t (name)").unwrap();
        engine
            .exec(None, "INSERT INTO t VALUES (1, 'old')")
            .unwrap();

        let txn = engine.begin_txn().unwrap();
        engine
            .exec(Some(&txn), "UPDATE t SET name = 'new' WHERE id = 1")
            .unwrap();
        engine.checkpoint().unwrap();
        std::mem::forget(txn);
        std::mem::forget(engine);
    }

    let engine = open(tmp.path());
    let rows = engine.scan("t", None).unwrap();
    assert_eq!(rows.len(), 1, "the uncommitted update stays invisible");
    assert!(
        engine
            .index_lookup("t", "name", &Datum::Text("old".to_string()))
            .unwrap()
            .is_some(),
        "recovery must restore the old key's index entry"
    );
    assert!(
        engine
            .index_lookup("t", "name", &Datum::Text("new".to_string()))
            .unwrap()
            .is_none(),
        "the loser's new-key entry points at an invisible tuple (masked)"
    );
}

/// Loser-compensation scan boundary (post-Stage-T review, item 3): a loser
/// whose delete record PREDATES the recovery redo start must still be
/// compensated when the record is readable. Construction: the victim table's
/// pages are evicted (flushed) between the in-flight delete and the next
/// checkpoint, so they anchor no DPT entry and the redo start lands AFTER
/// the delete record; the checkpoint's begin (a record boundary in a
/// retained segment) still precedes it. Pre-fix the compensation scan
/// started at the redo start and silently skipped the delete.
#[test]
fn crash_loser_delete_before_redo_start_compensated() {
    let tmp = TempDir::new().unwrap();
    let tmp = tmp.path();
    let mut config = EngineConfig::new(tmp);
    // Small segments (multi-segment WAL) and a tiny pool (evictions).
    config.storage.wal_segment_size = 32 * 1024;
    config.storage.buffer_pool_size = 8 * 8192;

    {
        let engine = Engine::open(tmp, config.clone()).unwrap();
        engine
            .exec(None, "CREATE TABLE t (id INT, name TEXT)")
            .unwrap();
        engine.exec(None, "CREATE INDEX ON t (name)").unwrap();
        engine
            .exec(None, "INSERT INTO t VALUES (1, 'victim')")
            .unwrap();
        // A pad table with enough wide rows to churn the 8-frame pool.
        engine
            .exec(None, "CREATE TABLE pad (id INT, name TEXT)")
            .unwrap();
        let wide = "y".repeat(2000);
        for i in 0..60 {
            engine
                .exec(None, &format!("INSERT INTO pad VALUES ({i}, '{wide}')"))
                .unwrap();
        }
        // Checkpoint A: everything durable; att-A snapshot retained.
        engine.checkpoint().unwrap();

        // The loser: an in-flight DELETE. Its records (heap xmax stamp +
        // index delete) sit AFTER begin_A.
        let txn = engine.begin_txn().unwrap();
        engine
            .exec(Some(&txn), "DELETE FROM t WHERE id = 1")
            .unwrap();

        // Churn the pool with read-only scans of pad: t's dirty pages get
        // evicted and flushed BEFORE checkpoint B's DPT sample, so they
        // anchor no rec_lsn and the redo start lands past the delete.
        for _ in 0..3 {
            assert_eq!(engine.scan("pad", None).unwrap().len(), 60);
        }

        // Checkpoint B: begin_B > delete LSN; begin_A's segment == begin_B's
        // (only the small delete records in between), so it is retained.
        engine.checkpoint().unwrap();

        // kill -9 with the transaction in flight.
        std::mem::forget(txn);
        std::mem::forget(engine);
    }

    let engine = Engine::open(tmp, config).unwrap();
    assert_eq!(
        engine.scan("t", None).unwrap().len(),
        1,
        "the uncommitted delete must stay invisible (row still present)"
    );
    assert!(
        engine
            .index_lookup("t", "name", &Datum::Text("victim".to_string()))
            .unwrap()
            .is_some(),
        "the loser's pre-redo-start index delete must be compensated \
         (scan starts at the oldest retained checkpoint begin)"
    );
}

/// F7 regression: a WAL-fatal commit must NOT leak the XID into the active
/// set — `TxnHandle::commit` falls back to a best-effort abort (index undo
/// replayed first, then the CLOG abort), so the vacuum horizon and every
/// future snapshot's xmin are never pinned by a dead commit.
#[test]
fn commit_failure_falls_back_to_abort_and_reclaims_xid() {
    use pg_am_btree::key::encode_key;
    let (_tmp, engine) = setup();
    let txn = engine.begin_txn().unwrap();
    engine.exec(Some(&txn), "INSERT INTO t VALUES (7)").unwrap();
    let xid = txn.xid();

    // Arm the hook: commit_txn fails at entry, before any WAL work.
    let hook = CommitFailHookGuard::arm();
    let res = txn.commit();
    drop(hook); // disarm before the normal-commit checks below
    assert!(res.is_err(), "the injected commit failure must surface");

    // The fallback abort removed the XID from the active set (pre-fix: it
    // leaked forever — `self.xid` was taken, so Drop never aborted).
    assert!(
        !engine.active_xids().contains(&xid),
        "commit failure leaked the XID into the active set (F7)"
    );
    // It replayed the index undo: the entry is gone even at the raw level.
    let raw = engine
        .btree_index("t", "id")
        .unwrap()
        .lookup_all(&encode_key(&Datum::Int4(7)).unwrap())
        .unwrap();
    assert!(
        raw.is_empty(),
        "fallback abort did not replay the index undo"
    );
    // The row is invisible, and subsequent commits work normally.
    assert_eq!(scan_count(&engine), 0);
    engine.exec(None, "INSERT INTO t VALUES (8)").unwrap();
    assert!(lookup(&engine, 8));
}

/// F7/P2-1 regression, auto-commit path: a WAL-fatal commit inside
/// `auto_commit` must not leak the XID either — the Ok branch mirrors
/// `TxnHandle::commit`'s fallback (undo replay, then abort).
#[test]
fn auto_commit_commit_failure_reclaims_xid() {
    use pg_am_btree::key::encode_key;
    let (_tmp, engine) = setup();
    let hook = CommitFailHookGuard::arm();
    let res = engine.exec(None, "INSERT INTO t VALUES (11)");
    drop(hook); // disarm before the normal-commit checks below
    assert!(res.is_err(), "the injected commit failure must surface");
    assert!(
        engine.active_xids().is_empty(),
        "auto-commit commit failure leaked the XID (F7/P2-1)"
    );
    let raw = engine
        .btree_index("t", "id")
        .unwrap()
        .lookup_all(&encode_key(&Datum::Int4(11)).unwrap())
        .unwrap();
    assert!(raw.is_empty(), "fallback did not replay the index undo");
    assert_eq!(scan_count(&engine), 0);
    engine.exec(None, "INSERT INTO t VALUES (12)").unwrap();
    assert!(lookup(&engine, 12));
}
