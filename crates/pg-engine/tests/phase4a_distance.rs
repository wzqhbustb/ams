//! Phase 4a Stage B: SQL vector-distance projection golden cases.

use pg_engine::{Datum, Engine, EngineConfig, QueryResult};
use tempfile::TempDir;

fn open(dir: &std::path::Path) -> Engine {
    Engine::open(dir, EngineConfig::new(dir)).unwrap()
}

fn result_rows(result: QueryResult) -> (Vec<String>, Vec<Vec<Option<Datum>>>) {
    let QueryResult::Rows { columns, rows } = result else {
        panic!("expected rows")
    };
    (columns, rows)
}

#[test]
fn l2_projection_returns_euclidean_distance_for_ann_and_seqscan() {
    for with_index in [false, true] {
        let tmp = TempDir::new().unwrap();
        let engine = open(tmp.path());
        engine
            .exec(None, "CREATE TABLE t (id INT4, v VECTOR(2))")
            .unwrap();
        if with_index {
            engine
                .exec(
                    None,
                    "CREATE INDEX ann_idx ON t USING hnsw (v vector_l2_ops)",
                )
                .unwrap();
        }
        engine
            .exec(None, "INSERT INTO t VALUES (1, [0,0]), (2, [3,4])")
            .unwrap();

        let (columns, rows) = result_rows(
            engine
                .exec(
                    None,
                    "SELECT id, v <-> '[0,0]' FROM t ORDER BY v <-> '[0,0]' ASC LIMIT 2",
                )
                .unwrap(),
        );
        assert_eq!(columns, vec!["id", "?column?"]);
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0],
            vec![Some(Datum::Int4(1)), Some(Datum::Float8(0.0))]
        );
        assert_eq!(
            rows[1],
            vec![Some(Datum::Int4(2)), Some(Datum::Float8(5.0))]
        );

        // SELECT and ORDER BY expressions own independent query vectors.
        let (_, rows) = result_rows(
            engine
                .exec(
                    None,
                    "SELECT id, v <-> '[3,4]' FROM t ORDER BY v <-> '[0,0]' ASC LIMIT 1",
                )
                .unwrap(),
        );
        assert_eq!(
            rows[0],
            vec![Some(Datum::Int4(1)), Some(Datum::Float8(5.0))]
        );
        engine.shutdown();
    }
}

#[test]
fn cosine_and_negative_inner_product_projection_match_sql_semantics() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine
        .exec(None, "CREATE TABLE t (id INT4, v VECTOR(2))")
        .unwrap();
    engine
        .exec(
            None,
            "CREATE INDEX ann_idx ON t USING hnsw (v vector_cosine_ops)",
        )
        .unwrap();
    engine
        .exec(None, "INSERT INTO t VALUES (1, [1,0]), (2, [0,1])")
        .unwrap();
    let (_, rows) = result_rows(
        engine
            .exec(
                None,
                "SELECT id, v <=> '[1,0]' FROM t ORDER BY v <=> '[1,0]' ASC LIMIT 2",
            )
            .unwrap(),
    );
    assert_eq!(rows[0][1], Some(Datum::Float8(0.0)));
    assert_eq!(rows[1][1], Some(Datum::Float8(1.0)));
    engine.shutdown();

    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine
        .exec(None, "CREATE TABLE t (id INT4, v VECTOR(2))")
        .unwrap();
    engine
        .exec(
            None,
            "CREATE INDEX ann_idx ON t USING hnsw (v vector_ip_ops)",
        )
        .unwrap();
    engine
        .exec(None, "INSERT INTO t VALUES (1, [1,2]), (2, [0,1])")
        .unwrap();
    let (_, rows) = result_rows(
        engine
            .exec(
                None,
                "SELECT id, v <#> '[3,4]' FROM t ORDER BY v <#> '[3,4]' ASC LIMIT 2",
            )
            .unwrap(),
    );
    assert_eq!(
        rows[0],
        vec![Some(Datum::Int4(1)), Some(Datum::Float8(-11.0))]
    );
    assert_eq!(
        rows[1],
        vec![Some(Datum::Int4(2)), Some(Datum::Float8(-4.0))]
    );
    engine.shutdown();
}

#[test]
fn null_distance_projects_null_and_uses_pg_ordering_defaults() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine
        .exec(None, "CREATE TABLE t (id INT4, v VECTOR(2))")
        .unwrap();
    engine
        .exec(None, "INSERT INTO t VALUES (1, [0,0]), (2, NULL)")
        .unwrap();

    let (_, asc) = result_rows(
        engine
            .exec(
                None,
                "SELECT id, v <-> '[0,0]' FROM t ORDER BY v <-> '[0,0]' ASC LIMIT 2",
            )
            .unwrap(),
    );
    assert_eq!(asc[0][0], Some(Datum::Int4(1)));
    assert_eq!(asc[1], vec![Some(Datum::Int4(2)), None]);

    let (_, desc) = result_rows(
        engine
            .exec(
                None,
                "SELECT id, v <-> '[0,0]' FROM t ORDER BY v <-> '[0,0]' DESC LIMIT 2",
            )
            .unwrap(),
    );
    assert_eq!(desc[0], vec![Some(Datum::Int4(2)), None]);
    assert_eq!(desc[1][0], Some(Datum::Int4(1)));
    engine.shutdown();
}

#[test]
fn ann_appends_visible_null_vectors_after_hnsw_hits() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine
        .exec(None, "CREATE TABLE t (id INT4, v VECTOR(2))")
        .unwrap();
    engine
        .exec(
            None,
            "CREATE INDEX ann_idx ON t USING hnsw (v vector_l2_ops)",
        )
        .unwrap();
    engine
        .exec(
            None,
            "INSERT INTO t VALUES (1, [0,0]), (2, NULL), (3, NULL)",
        )
        .unwrap();

    let (_, rows) = result_rows(
        engine
            .exec(
                None,
                "SELECT id, v <-> '[0,0]' FROM t ORDER BY v <-> '[0,0]' ASC LIMIT 3",
            )
            .unwrap(),
    );
    assert_eq!(rows.len(), 3);
    assert_eq!(
        rows[0],
        vec![Some(Datum::Int4(1)), Some(Datum::Float8(0.0))]
    );
    assert_eq!(rows[1], vec![Some(Datum::Int4(2)), None]);
    assert_eq!(rows[2], vec![Some(Datum::Int4(3)), None]);

    engine
        .exec(None, "CREATE TABLE all_null (id INT4, v VECTOR(2))")
        .unwrap();
    engine
        .exec(
            None,
            "CREATE INDEX all_null_idx ON all_null USING hnsw (v vector_l2_ops)",
        )
        .unwrap();
    engine
        .exec(None, "INSERT INTO all_null VALUES (1, NULL), (2, NULL)")
        .unwrap();
    let (_, rows) = result_rows(
        engine
            .exec(
                None,
                "SELECT id, v <-> '[0,0]' FROM all_null ORDER BY v <-> '[0,0]' ASC LIMIT 2",
            )
            .unwrap(),
    );
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], vec![Some(Datum::Int4(1)), None]);
    assert_eq!(rows[1], vec![Some(Datum::Int4(2)), None]);
    engine.shutdown();
}
