//! M2b Stage O acceptance: §7.2 visibility verification through the
//! `Engine::exec` SQL API.
//!
//! The suite exercises the snapshot-isolation + curcid (Halloween) logic:
//!
//! - self INSERT → DELETE → SELECT → empty (earlier-command delete)
//! - self INSERT → SELECT → row visible (earlier-command insert)
//! - self INSERT → UPDATE → SELECT → one row with new values (Halloween)
//! - concurrent uncommitted DELETE → other txn SELECT → sees row (SI)
//! - committed DELETE → other txn SELECT → no row (SI)
//!
//! The §7.2 same-command DELETE ... RETURNING output-channel cases are N/A
//! at SQL level — the M2b subset has no RETURNING — and are covered by the
//! pg-txn visibility unit tests (`pg_txn::visibility`).
//!
//! Acceptance: `cargo test -p pg-engine --test m2b_integration`

use pg_engine::{Engine, EngineConfig, QueryResult};
use tempfile::TempDir;

fn open(dir: &std::path::Path) -> Engine {
    Engine::open(dir, EngineConfig::new(dir)).unwrap()
}

fn assert_affected(res: QueryResult, expected: usize) {
    match res {
        QueryResult::Affected(n) => assert_eq!(n, expected, "expected {expected} affected"),
        other => panic!("expected Affected, got {other:?}"),
    }
}

fn assert_row_count(res: &QueryResult, expected: usize) {
    match res {
        QueryResult::Rows { rows, .. } => {
            assert_eq!(rows.len(), expected, "expected {expected} rows")
        }
        other => panic!("expected Rows, got {other:?}"),
    }
}

fn setup() -> (TempDir, Engine) {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine
        .exec(None, "CREATE TABLE users (id INT, name TEXT)")
        .unwrap();
    engine
        .exec(None, "INSERT INTO users VALUES (1, 'Alice')")
        .unwrap();
    (tmp, engine)
}

/// Case 1: self INSERT → DELETE → SELECT → empty.
#[test]
fn case1_self_insert_delete_select_empty() {
    let (_tmp, engine) = setup();
    let txn = engine.begin_txn().unwrap();
    engine
        .exec(Some(&txn), "INSERT INTO users VALUES (2, 'Bob')")
        .unwrap();
    engine
        .exec(Some(&txn), "DELETE FROM users WHERE id = 2")
        .unwrap();
    let res = engine
        .exec(Some(&txn), "SELECT * FROM users WHERE id = 2")
        .unwrap();
    assert_row_count(&res, 0);
    txn.commit().unwrap();
}

/// Case 2: self INSERT → SELECT → row visible (earlier-command insert).
#[test]
fn case2_self_insert_select_visible() {
    let (_tmp, engine) = setup();
    let txn = engine.begin_txn().unwrap();
    engine
        .exec(Some(&txn), "INSERT INTO users VALUES (2, 'Bob')")
        .unwrap();
    let res = engine
        .exec(Some(&txn), "SELECT * FROM users WHERE id = 2")
        .unwrap();
    assert_row_count(&res, 1);
    txn.commit().unwrap();
}

/// Case 3: self INSERT → DELETE → SELECT → empty (delete path).
#[test]
fn case3_self_insert_then_delete_then_select_empty() {
    let (_tmp, engine) = setup();
    let txn = engine.begin_txn().unwrap();
    engine
        .exec(Some(&txn), "INSERT INTO users VALUES (3, 'Carol')")
        .unwrap();
    engine
        .exec(Some(&txn), "DELETE FROM users WHERE id = 3")
        .unwrap();
    let res = engine.exec(Some(&txn), "SELECT * FROM users").unwrap();
    // Only the pre-existing row (id=1, Alice) should be visible.
    assert_row_count(&res, 1);
    txn.commit().unwrap();
}

/// Case 4: self INSERT → UPDATE → SELECT → one row with new values
/// (Halloween protection: UPDATE does not re-scan its own writes).
#[test]
fn case4_halloween_update_does_not_rescan() {
    let (_tmp, engine) = setup();
    let txn = engine.begin_txn().unwrap();
    engine
        .exec(Some(&txn), "INSERT INTO users VALUES (2, 'Bob')")
        .unwrap();
    engine
        .exec(Some(&txn), "UPDATE users SET name = 'Bobby' WHERE id = 2")
        .unwrap();
    let res = engine
        .exec(Some(&txn), "SELECT * FROM users WHERE id = 2")
        .unwrap();
    match res {
        QueryResult::Rows { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows[0][1],
                Some(pg_engine::Datum::Text("Bobby".to_string()))
            );
        }
        other => panic!("expected Rows, got {other:?}"),
    }
    txn.commit().unwrap();
}

/// Case 5: T1 DELETE (uncommitted) → T2 SELECT → sees row (SI isolation).
#[test]
fn case5_concurrent_uncommitted_delete_other_txn_sees_row() {
    let (_tmp, engine) = setup();
    let t1 = engine.begin_txn().unwrap();
    engine
        .exec(Some(&t1), "DELETE FROM users WHERE id = 1")
        .unwrap();
    // T2 begins AFTER T1's delete is uncommitted — T1 is in T2's xip.
    let t2 = engine.begin_txn().unwrap();
    let res = engine
        .exec(Some(&t2), "SELECT * FROM users WHERE id = 1")
        .unwrap();
    assert_row_count(&res, 1);
    t2.abort().unwrap();
    t1.abort().unwrap();
}

/// Case 6: T1 DELETE → COMMIT → T2 SELECT → no row (SI isolation).
#[test]
fn case6_committed_delete_other_txn_no_row() {
    let (_tmp, engine) = setup();
    let t1 = engine.begin_txn().unwrap();
    engine
        .exec(Some(&t1), "DELETE FROM users WHERE id = 1")
        .unwrap();
    t1.commit().unwrap();
    // T2 begins AFTER T1 commits — T1's delete is visible to T2.
    let t2 = engine.begin_txn().unwrap();
    let res = engine
        .exec(Some(&t2), "SELECT * FROM users WHERE id = 1")
        .unwrap();
    assert_row_count(&res, 0);
    t2.commit().unwrap();
}

/// Auto-commit INSERT/SELECT round-trip through exec.
#[test]
fn exec_auto_commit_roundtrip() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine
        .exec(None, "CREATE TABLE t (id INT, name TEXT)")
        .unwrap();
    assert_affected(
        engine.exec(None, "INSERT INTO t VALUES (1, 'a')").unwrap(),
        1,
    );
    assert_affected(
        engine
            .exec(None, "INSERT INTO t VALUES (2, 'b'), (3, 'c')")
            .unwrap(),
        2,
    );
    let res = engine.exec(None, "SELECT * FROM t ORDER BY id").unwrap();
    assert_row_count(&res, 3);
    // WHERE + ORDER BY DESC + LIMIT
    let res = engine
        .exec(
            None,
            "SELECT * FROM t WHERE id > 1 ORDER BY id DESC LIMIT 1",
        )
        .unwrap();
    assert_row_count(&res, 1);
    match res {
        QueryResult::Rows { rows, .. } => {
            assert_eq!(rows[0][0], Some(pg_engine::Datum::Int4(3)));
        }
        _ => unreachable!(),
    }
}

/// UPDATE / DELETE through exec auto-commit.
#[test]
fn exec_update_delete() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine
        .exec(None, "CREATE TABLE t (id INT, name TEXT)")
        .unwrap();
    engine
        .exec(None, "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .unwrap();
    assert_affected(
        engine
            .exec(None, "UPDATE t SET name = 'x' WHERE id > 1")
            .unwrap(),
        2,
    );
    assert_affected(engine.exec(None, "DELETE FROM t WHERE id < 3").unwrap(), 2);
    let res = engine.exec(None, "SELECT * FROM t").unwrap();
    assert_row_count(&res, 1);
}

/// Explicit transaction: INSERT then COMMIT → SELECT sees row.
#[test]
fn explicit_txn_insert_commit_select() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine.exec(None, "CREATE TABLE t (id INT)").unwrap();
    let txn = engine.begin_txn().unwrap();
    engine.exec(Some(&txn), "INSERT INTO t VALUES (1)").unwrap();
    engine.exec(Some(&txn), "INSERT INTO t VALUES (2)").unwrap();
    txn.commit().unwrap();
    let res = engine.exec(None, "SELECT * FROM t ORDER BY id").unwrap();
    assert_row_count(&res, 2);
}

/// Explicit transaction: INSERT then ABORT → SELECT does not see row.
#[test]
fn explicit_txn_insert_abort_select() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine.exec(None, "CREATE TABLE t (id INT)").unwrap();
    let txn = engine.begin_txn().unwrap();
    engine.exec(Some(&txn), "INSERT INTO t VALUES (1)").unwrap();
    txn.abort().unwrap();
    let res = engine.exec(None, "SELECT * FROM t").unwrap();
    assert_row_count(&res, 0);
}

/// Clean shutdown + reopen: DDL + DML via exec survive a graceful
/// `shutdown()` and engine reopen (not a crash — crash rounds live in
/// `m2b_crash_rounds.rs`).
#[test]
fn exec_clean_shutdown_reopen() {
    let tmp = TempDir::new().unwrap();
    {
        let engine = open(tmp.path());
        engine
            .exec(None, "CREATE TABLE t (id INT, name TEXT)")
            .unwrap();
        engine
            .exec(None, "INSERT INTO t VALUES (1, 'hello')")
            .unwrap();
        engine.checkpoint().unwrap();
        engine
            .exec(None, "INSERT INTO t VALUES (2, 'world')")
            .unwrap();
        engine.shutdown();
    }
    let engine = open(tmp.path());
    let res = engine.exec(None, "SELECT * FROM t ORDER BY id").unwrap();
    assert_row_count(&res, 2);
}

/// Transaction control via exec is rejected: BEGIN/COMMIT/ROLLBACK are
/// programmatic-only (Stage O review; they previously returned Ok silently).
#[test]
fn exec_txn_control_statements_error() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    let err = engine.exec(None, "BEGIN").unwrap_err().to_string();
    assert!(
        err.contains("Engine::begin_txn()"),
        "unexpected BEGIN error: {err}"
    );
    for sql in ["COMMIT", "ROLLBACK"] {
        let err = engine.exec(None, sql).unwrap_err().to_string();
        assert!(
            err.contains("no transaction in progress"),
            "unexpected {sql} error: {err}"
        );
    }
}

/// exec-level negative tests (Stage O review): failures must be loud.
#[test]
fn exec_negative_paths_error() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine
        .exec(None, "CREATE TABLE t (id INT, name TEXT)")
        .unwrap();

    // Malformed SQL.
    assert!(engine.exec(None, "SELEC * FROM t").is_err());
    assert!(engine.exec(None, "SELECT * WHERE id = 1").is_err());
    // Unknown table.
    assert!(engine.exec(None, "SELECT * FROM nope").is_err());
    assert!(engine
        .exec(None, "INSERT INTO nope VALUES (1, 'a')")
        .is_err());
    // Type mismatch on INSERT.
    assert!(engine.exec(None, "INSERT INTO t VALUES ('a', 1)").is_err());
    // DDL inside an explicit transaction (locks in engine.rs exec_txn).
    let txn = engine.begin_txn().unwrap();
    assert!(engine.exec(Some(&txn), "CREATE TABLE u (id INT)").is_err());
    assert!(engine.exec(Some(&txn), "CREATE INDEX ON t (id)").is_err());
    txn.abort().unwrap();
}

/// i64 → i32 truncation is an error, not a silent wrap (Stage O review).
#[test]
fn exec_int4_out_of_range_errors() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine.exec(None, "CREATE TABLE t (id INT)").unwrap();
    engine.exec(None, "INSERT INTO t VALUES (1)").unwrap();

    // 3000000000 > i32::MAX: INSERT must fail, not store a wrapped value.
    let res = engine.exec(None, "INSERT INTO t VALUES (3000000000)");
    assert!(res.is_err(), "out-of-range INT4 literal must fail");
    // 4294967297 wraps to 1 as i32: the WHERE must fail, not match id=1.
    let res = engine.exec(None, "SELECT * FROM t WHERE id = 4294967297");
    assert!(res.is_err(), "out-of-range INT4 filter literal must fail");
    // The table still holds exactly the original row.
    let res = engine.exec(None, "SELECT * FROM t").unwrap();
    assert_row_count(&res, 1);
}

/// One optional trailing semicolon is accepted (Stage O review).
#[test]
fn exec_trailing_semicolon() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine.exec(None, "CREATE TABLE t (id INT);").unwrap();
    engine.exec(None, "INSERT INTO t VALUES (1);").unwrap();
    let res = engine.exec(None, "SELECT * FROM t;").unwrap();
    assert_row_count(&res, 1);
    // A second semicolon is still an error.
    assert!(engine.exec(None, "SELECT * FROM t;;").is_err());
}

/// A TxnHandle is bound to its creating Engine instance (Stage O review).
#[test]
fn exec_txn_handle_from_other_engine_errors() {
    let tmp_a = TempDir::new().unwrap();
    let tmp_b = TempDir::new().unwrap();
    let engine_a = open(tmp_a.path());
    let engine_b = open(tmp_b.path());
    engine_b.exec(None, "CREATE TABLE t (id INT)").unwrap();

    let txn_a = engine_a.begin_txn().unwrap();
    let res = engine_b.exec(Some(&txn_a), "INSERT INTO t VALUES (1)");
    assert!(res.is_err(), "handle from another engine must be rejected");
    txn_a.abort().unwrap();
}

/// Unquoted identifiers fold to lowercase: DDL and DML are uniformly
/// case-insensitive (Stage O review).
#[test]
fn exec_identifier_case_insensitive() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine
        .exec(None, "CREATE TABLE Users (ID INT, Name TEXT)")
        .unwrap();
    engine
        .exec(None, "INSERT INTO users VALUES (1, 'Alice')")
        .unwrap();
    engine
        .exec(None, "INSERT INTO USERS (id, NAME) VALUES (2, 'Bob')")
        .unwrap();
    let res = engine
        .exec(None, "SELECT name FROM Users WHERE ID = 2")
        .unwrap();
    match res {
        QueryResult::Rows { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], Some(pg_engine::Datum::Text("Bob".to_string())));
        }
        other => panic!("expected Rows, got {other:?}"),
    }
}

/// TxnHandle::drop auto-abort, heap/visibility side (Stage O review P2-1;
/// the index-side twin is `txn_handle_drop_auto_abort_undoes_index` in
/// m2b_index_txn.rs): a dropped handle's writes must be invisible AND its
/// XID must leave the active set.
#[test]
fn txn_handle_drop_auto_abort_visibility() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine.exec(None, "CREATE TABLE t (id INT)").unwrap();
    let xid;
    {
        let txn = engine.begin_txn().unwrap();
        xid = txn.xid();
        engine.exec(Some(&txn), "INSERT INTO t VALUES (1)").unwrap();
        assert!(engine.txn_manager().active_xids().contains(&xid));
        // No commit/abort: drop triggers the best-effort auto-abort.
    }
    assert!(
        !engine.txn_manager().active_xids().contains(&xid),
        "dropped handle's XID must leave the active set"
    );
    let res = engine.exec(None, "SELECT * FROM t").unwrap();
    assert_row_count(&res, 0);
}

/// ORDER BY ... DESC returns the full descending order (Stage O review
/// P2-1; exec_auto_commit_roundtrip only checks DESC + LIMIT 1).
#[test]
fn exec_order_by_desc() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine
        .exec(None, "CREATE TABLE t (id INT, name TEXT)")
        .unwrap();
    engine
        .exec(None, "INSERT INTO t VALUES (2, 'b'), (1, 'a'), (3, 'c')")
        .unwrap();
    let res = engine
        .exec(None, "SELECT id FROM t ORDER BY id DESC")
        .unwrap();
    match res {
        QueryResult::Rows { rows, .. } => {
            let ids: Vec<i32> = rows
                .iter()
                .map(|r| match r[0] {
                    Some(pg_engine::Datum::Int4(v)) => v,
                    ref other => panic!("unexpected value {other:?}"),
                })
                .collect();
            assert_eq!(ids, vec![3, 2, 1]);
        }
        other => panic!("expected Rows, got {other:?}"),
    }
}

/// UPDATE / DELETE without WHERE take the filter:None full-table path
/// (Stage O review P2-1).
#[test]
fn exec_update_delete_no_where_full_table() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine
        .exec(None, "CREATE TABLE t (id INT, name TEXT)")
        .unwrap();
    engine
        .exec(None, "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .unwrap();
    // Full-table UPDATE.
    assert_affected(engine.exec(None, "UPDATE t SET name = 'x'").unwrap(), 3);
    let res = engine
        .exec(None, "SELECT * FROM t WHERE name = 'x'")
        .unwrap();
    assert_row_count(&res, 3);
    // Full-table DELETE.
    assert_affected(engine.exec(None, "DELETE FROM t").unwrap(), 3);
    let res = engine.exec(None, "SELECT * FROM t").unwrap();
    assert_row_count(&res, 0);
}
