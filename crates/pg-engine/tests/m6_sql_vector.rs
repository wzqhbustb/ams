//! M6 Stage C slice C-4: the SQL minimum — `VECTOR(n)` column type in
//! CREATE TABLE and `[v0, v1, ...]` vector literals in INSERT (selection
//! §11). The ANN operator and CREATE INDEX are Phase 4b (not here).

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
