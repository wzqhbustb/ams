//! Phase 4a scalar-type storage and catalog compatibility tests.

use pg_engine::{ColumnDef, ColumnType, Datum, Engine, EngineConfig, Predicate};
use tempfile::TempDir;

fn open(dir: &std::path::Path) -> Engine {
    Engine::open(dir, EngineConfig::new(dir)).unwrap()
}

#[test]
fn float8_columns_roundtrip_through_catalog_and_reopen() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine
        .create_table(
            "measurements",
            &[
                ColumnDef {
                    name: "id".to_string(),
                    col_type: ColumnType::Int4,
                },
                ColumnDef {
                    name: "distance".to_string(),
                    col_type: ColumnType::Float8,
                },
            ],
        )
        .unwrap();
    engine
        .insert(
            "measurements",
            &[Some(Datum::Int4(1)), Some(Datum::Float8(-11.5))],
        )
        .unwrap();
    assert_eq!(
        engine.scan("measurements", None).unwrap()[0].1,
        vec![Some(Datum::Int4(1)), Some(Datum::Float8(-11.5))]
    );
    engine.shutdown();

    let engine = open(tmp.path());
    assert_eq!(
        engine.scan("measurements", None).unwrap()[0].1,
        vec![Some(Datum::Int4(1)), Some(Datum::Float8(-11.5))]
    );
    engine.shutdown();
}

fn ids(result: pg_engine::QueryResult) -> Vec<i32> {
    let pg_engine::QueryResult::Rows { rows, .. } = result else {
        panic!("expected rows")
    };
    rows.into_iter()
        .map(|row| match row[0] {
            Some(Datum::Int4(id)) => id,
            ref other => panic!("unexpected id datum: {other:?}"),
        })
        .collect()
}

#[test]
fn vector_plain_order_by_is_rejected_loudly() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine
        .exec(None, "CREATE TABLE t (id INT4, v VECTOR(2))")
        .unwrap();
    engine
        .exec(None, "INSERT INTO t VALUES (1, [1,0])")
        .unwrap();
    for sql in [
        "SELECT * FROM t ORDER BY v",
        "EXPLAIN SELECT * FROM t ORDER BY v",
    ] {
        let err = engine.exec(None, sql).unwrap_err().to_string();
        assert!(err.contains("ORDER BY VECTOR column"), "{err}");
    }
    engine.shutdown();
}

#[test]
fn float8_nan_sorts_last_asc_and_first_desc() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine
        .create_table(
            "t",
            &[
                ColumnDef {
                    name: "id".to_string(),
                    col_type: ColumnType::Int4,
                },
                ColumnDef {
                    name: "score".to_string(),
                    col_type: ColumnType::Float8,
                },
            ],
        )
        .unwrap();
    for (id, score) in [(1, 1.0), (2, f64::NAN), (3, -0.0), (4, 0.0), (5, -1.0)] {
        engine
            .insert("t", &[Some(Datum::Int4(id)), Some(Datum::Float8(score))])
            .unwrap();
    }

    let asc = ids(engine
        .exec(None, "SELECT * FROM t ORDER BY score ASC")
        .unwrap());
    assert_eq!(asc, vec![5, 3, 4, 1, 2]);
    let desc = ids(engine
        .exec(None, "SELECT * FROM t ORDER BY score DESC")
        .unwrap());
    assert_eq!(desc, vec![2, 1, 3, 4, 5]);
    engine.shutdown();
}

#[test]
fn float8_equality_predicate_matches_nan_zero_and_infinity() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine
        .create_table(
            "t",
            &[
                ColumnDef {
                    name: "id".to_string(),
                    col_type: ColumnType::Int4,
                },
                ColumnDef {
                    name: "score".to_string(),
                    col_type: ColumnType::Float8,
                },
            ],
        )
        .unwrap();
    for (id, score) in [
        (1, f64::NAN),
        (2, 0.0),
        (3, -0.0),
        (4, f64::INFINITY),
        (5, f64::NEG_INFINITY),
    ] {
        engine
            .insert("t", &[Some(Datum::Int4(id)), Some(Datum::Float8(score))])
            .unwrap();
    }

    for (value, expected) in [
        (f64::NAN, vec![1]),
        (0.0, vec![2, 3]),
        (-0.0, vec![2, 3]),
        (f64::INFINITY, vec![4]),
        (f64::NEG_INFINITY, vec![5]),
    ] {
        let rows = engine
            .scan(
                "t",
                Some(Predicate::Eq {
                    col_index: 1,
                    value: Datum::Float8(value),
                }),
            )
            .unwrap();
        let actual: Vec<i32> = rows
            .into_iter()
            .map(|(_, values)| match values[0] {
                Some(Datum::Int4(id)) => id,
                ref other => panic!("unexpected id: {other:?}"),
            })
            .collect();
        assert_eq!(actual, expected, "unexpected equality result for {value}");
    }
    engine.shutdown();
}
