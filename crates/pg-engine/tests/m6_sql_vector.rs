//! M6 Stage C slice C-4: the SQL minimum — `VECTOR(n)` column type in
//! CREATE TABLE and `[v0, v1, ...]` vector literals in INSERT (selection
//! §11). Phase 4a Stage 0's early ANN-rejection tests also live here.

use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;

use pg_am_heap::Datum;
use pg_engine::{Engine, EngineConfig, QueryResult};
use tempfile::TempDir;

#[test]
fn vector_column_create_insert_readback() {
    let tmp = TempDir::new().unwrap();
    let engine = Engine::open(tmp.path(), EngineConfig::new(tmp.path())).unwrap();

    engine
        .exec(None, "CREATE TABLE t (id INT4, v VECTOR(3))")
        .unwrap();
    let r = engine
        .exec(None, "INSERT INTO t VALUES (1, [1.0, 2.5, -3])")
        .unwrap();
    assert!(matches!(r, QueryResult::Affected(1)));

    let rows = engine.scan("t", None).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].1,
        vec![
            Some(Datum::Int4(1)),
            Some(Datum::Vector(vec![1.0, 2.5, -3.0])),
        ]
    );
    engine.shutdown();
}

#[test]
fn vector_sql_negative_matrix() {
    let tmp = TempDir::new().unwrap();
    let engine = Engine::open(tmp.path(), EngineConfig::new(tmp.path())).unwrap();
    engine
        .exec(None, "CREATE TABLE t (id INT4, v VECTOR(3))")
        .unwrap();

    // dim mismatch
    assert!(engine
        .exec(None, "INSERT INTO t VALUES (1, [1.0, 2.0])")
        .is_err());
    // malformed literal
    assert!(engine
        .exec(None, "INSERT INTO t VALUES (1, [1.0, x])")
        .is_err());
    // dim out of range at DDL
    assert!(engine.exec(None, "CREATE TABLE bad (v VECTOR(0))").is_err());
    assert!(engine
        .exec(None, "CREATE TABLE bad (v VECTOR(2001))")
        .is_err());
    // WHERE on a vector literal is rejected (no vector ordering in M6)
    assert!(engine
        .exec(None, "SELECT id FROM t WHERE v = [1.0, 2.0, 3.0]")
        .is_err());
    // an empty vector is legal syntactically but dim-mismatched
    assert!(engine.exec(None, "INSERT INTO t VALUES (1, [])").is_err());
    engine.shutdown();
}

/// `VECTOR` without a parenthesized dim stays a plain identifier (a column
/// may be named "vector").
#[test]
fn vector_bare_ident_stays_legal() {
    let tmp = TempDir::new().unwrap();
    let engine = Engine::open(tmp.path(), EngineConfig::new(tmp.path())).unwrap();
    engine.exec(None, "CREATE TABLE t (vector INT4)").unwrap();
    engine.exec(None, "INSERT INTO t VALUES (7)").unwrap();
    let rows = engine.scan("t", None).unwrap();
    assert_eq!(rows[0].1, vec![Some(Datum::Int4(7))]);
    engine.shutdown();
}

/// The type maps to ColumnType::Vector at the catalog boundary — proven by
/// a read-back through the codec (a wrong type mapping would fail to
/// decode or return a wrong-typed datum).
#[test]
fn vector_column_type_roundtrip() {
    let tmp = TempDir::new().unwrap();
    let engine = Engine::open(tmp.path(), EngineConfig::new(tmp.path())).unwrap();
    engine.exec(None, "CREATE TABLE t (v VECTOR(8))").unwrap();
    engine
        .exec(None, "INSERT INTO t VALUES ([1, 2, 3, 4, 5, 6, 7, 8])")
        .unwrap();
    let rows = engine.scan("t", None).unwrap();
    assert_eq!(
        rows[0].1,
        vec![Some(Datum::Vector(vec![
            1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0
        ]))]
    );
    engine.shutdown();
}

const ANN_SELECT: &str = "SELECT * FROM t ORDER BY v <-> '[1,2,3]' ASC LIMIT 1";
const ANN_FOR_UPDATE: &str = "SELECT * FROM t ORDER BY v <-> '[1,2,3]' ASC LIMIT 1 FOR UPDATE";
const ANN_INVALID_FILTER_FOR_UPDATE: &str =
    "SELECT * FROM t WHERE missing = 1 ORDER BY v <-> '[1,2,3]' ASC LIMIT 1 FOR UPDATE";

fn assert_ann_stage_b(result: Result<QueryResult, pg_engine::EngineError>) {
    let err = result.unwrap_err().to_string();
    assert!(err.contains("Stage B"), "unexpected ANN rejection: {err}");
}

#[test]
fn ann_rejection_precedes_scan_and_row_locks_in_auto_commit() {
    let tmp = TempDir::new().unwrap();
    let engine = Engine::open(tmp.path(), EngineConfig::new(tmp.path())).unwrap();
    engine
        .exec(None, "CREATE TABLE t (id INT4, v VECTOR(3))")
        .unwrap();
    engine
        .exec(None, "INSERT INTO t VALUES (1, [1,2,3])")
        .unwrap();

    assert_ann_stage_b(engine.exec(None, ANN_SELECT));
    assert_ann_stage_b(engine.exec(None, ANN_FOR_UPDATE));
    // An invalid filter column must still produce the Stage B error, not
    // InvalidPredicate: rejection happens before predicate setup or heap I/O.
    assert_ann_stage_b(engine.exec(None, ANN_INVALID_FILTER_FOR_UPDATE));
    // The failed statements must not leave a row lock behind.
    engine
        .exec(None, "UPDATE t SET v = [4,5,6] WHERE id = 1")
        .unwrap();
    let rows = engine.scan("t", None).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1[1], Some(Datum::Vector(vec![4.0, 5.0, 6.0])));
    engine.shutdown();
}

#[test]
fn ann_rejection_precedes_scan_and_row_locks_in_explicit_txn() {
    let tmp = TempDir::new().unwrap();
    let engine = Arc::new(Engine::open(tmp.path(), EngineConfig::new(tmp.path())).unwrap());
    engine
        .exec(None, "CREATE TABLE t (id INT4, v VECTOR(3))")
        .unwrap();
    engine
        .exec(None, "INSERT INTO t VALUES (1, [1,2,3])")
        .unwrap();
    let txn = engine.begin_txn().unwrap();

    assert_ann_stage_b(engine.exec(Some(&txn), ANN_SELECT));
    assert_ann_stage_b(engine.exec(Some(&txn), ANN_FOR_UPDATE));
    assert_ann_stage_b(engine.exec(Some(&txn), ANN_INVALID_FILTER_FOR_UPDATE));

    // Drop only the table locks. If the failed ANN FOR UPDATE stamped a row
    // lock, the writer below would still block until txn.abort() releases it.
    engine.lock_manager().release_all(txn.xid());
    let (tx, rx) = mpsc::channel();
    let engine2 = Arc::clone(&engine);
    let writer = thread::spawn(move || {
        let result = engine2.exec(None, "UPDATE t SET v = [4,5,6] WHERE id = 1");
        tx.send(result).unwrap();
    });
    match rx.recv_timeout(Duration::from_secs(2)) {
        Ok(result) => {
            assert!(matches!(result, Ok(QueryResult::Affected(1))));
        }
        Err(_) => {
            txn.abort().unwrap();
            writer.join().unwrap();
            panic!("failed ANN FOR UPDATE left a row lock behind");
        }
    }
    txn.abort().unwrap();
    let rows = engine.scan("t", None).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1[1], Some(Datum::Vector(vec![4.0, 5.0, 6.0])));
    engine.shutdown();
}
