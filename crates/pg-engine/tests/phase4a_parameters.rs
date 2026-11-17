//! Phase 4a Stage C: typed SQL bind parameters and parameter type inference.

use pg_engine::sql::{parse, Expr, Literal, OrderByKind, SelectCols, SelectItem, Statement};
use pg_engine::{
    ColumnDef, ColumnType, Datum, Engine, EngineConfig, ExecutionPath, QueryResult, Value,
};
use tempfile::TempDir;

fn open(dir: &std::path::Path) -> Engine {
    Engine::open(dir, EngineConfig::new(dir)).unwrap()
}

fn create_items(engine: &Engine) {
    engine
        .create_table(
            "items",
            &[
                ColumnDef {
                    name: "id".to_string(),
                    col_type: ColumnType::Int4,
                },
                ColumnDef {
                    name: "big".to_string(),
                    col_type: ColumnType::Int8,
                },
                ColumnDef {
                    name: "score".to_string(),
                    col_type: ColumnType::Float8,
                },
                ColumnDef {
                    name: "name".to_string(),
                    col_type: ColumnType::Text,
                },
                ColumnDef {
                    name: "v".to_string(),
                    col_type: ColumnType::Vector(2),
                },
            ],
        )
        .unwrap();
}

fn rows(result: QueryResult) -> Vec<Vec<Value>> {
    let QueryResult::Rows { rows, .. } = result else {
        panic!("expected rows, got {result:?}")
    };
    rows
}

fn affected(result: QueryResult) -> usize {
    let QueryResult::Affected(count) = result else {
        panic!("expected affected count, got {result:?}")
    };
    count
}

fn ids(result: QueryResult) -> Vec<i32> {
    rows(result)
        .into_iter()
        .map(|row| match row[0] {
            Some(Datum::Int4(id)) => id,
            ref other => panic!("expected INT4 id, got {other:?}"),
        })
        .collect()
}

#[test]
fn parser_accepts_parameters_in_every_literal_position() {
    let Statement::Insert { rows, .. } = parse("INSERT INTO t VALUES ($1, $2), ($2, $3)").unwrap()
    else {
        panic!("expected INSERT")
    };
    assert_eq!(
        rows,
        vec![
            vec![Literal::Param(1), Literal::Param(2)],
            vec![Literal::Param(2), Literal::Param(3)],
        ]
    );

    let Statement::Update { sets, filter, .. } =
        parse("UPDATE t SET name = $1 WHERE id = $2").unwrap()
    else {
        panic!("expected UPDATE")
    };
    assert_eq!(sets[0].1, Literal::Param(1));
    assert_eq!(filter.unwrap().value, Literal::Param(2));

    let Statement::Delete { filter, .. } = parse("DELETE FROM t WHERE id = $1").unwrap() else {
        panic!("expected DELETE")
    };
    assert_eq!(filter.unwrap().value, Literal::Param(1));

    let Statement::Select {
        columns,
        filter,
        order_by,
        ..
    } = parse("SELECT id, v <-> $1 FROM t WHERE id = $2 ORDER BY v <=> $3 ASC LIMIT 1").unwrap()
    else {
        panic!("expected SELECT")
    };
    let SelectCols::Cols(items) = columns else {
        panic!("expected SELECT items")
    };
    let SelectItem::Distance(projection) = &items[1] else {
        panic!("expected SELECT distance")
    };
    assert_eq!(projection.right, Expr::Literal(Literal::Param(1)));
    assert_eq!(filter.unwrap().value, Literal::Param(2));
    let Some(OrderByKind::Expr(order)) = order_by else {
        panic!("expected ORDER BY distance")
    };
    assert_eq!(order.right, Expr::Literal(Literal::Param(3)));
}

#[test]
fn parser_rejects_zero_malformed_suffixed_and_overflowing_parameters() {
    for placeholder in [
        "$0",
        "$",
        "$x",
        "$-1",
        "$1x",
        "$1_0",
        "$1.0",
        "$1$2",
        "$999999999999999999999999999999999999",
    ] {
        let sql = format!("SELECT * FROM t WHERE id = {placeholder}");
        let err = parse(&sql).unwrap_err().to_string();
        assert!(err.contains("parameter"), "{sql}: {err}");
    }
}

#[test]
fn parser_enforces_protocol_parameter_limit_before_type_inference() {
    let Statement::Select {
        filter: Some(filter),
        ..
    } = parse("SELECT * FROM t WHERE id = $65535").unwrap()
    else {
        panic!("expected SELECT with a filter")
    };
    assert_eq!(filter.value, Literal::Param(65_535));

    for index in [65_536, 1_000_000_000, usize::MAX] {
        let sql = format!("SELECT * FROM t WHERE id = ${index}");
        let err = parse(&sql).unwrap_err();
        let message = err.to_string();
        assert!(
            matches!(err, pg_engine::EngineError::InvalidArgument(_)),
            "{sql}: {message}"
        );
        assert!(message.contains("parameter limit"), "{sql}: {message}");
        assert!(message.contains("65535"), "{sql}: {message}");
    }
}

#[test]
fn typed_crud_parameters_support_null_repetition_float8_and_explicit_txns() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    create_items(&engine);

    let insert = "INSERT INTO items (id, big, score, name, v) VALUES \
                  ($1, $2, $3, $4, $5), ($1, $6, $7, $8, $9)";
    let txn = engine.begin_txn().unwrap();
    assert_eq!(
        affected(
            engine
                .exec_with_params(
                    Some(&txn),
                    insert,
                    &[
                        Some(Datum::Int4(1)),
                        Some(Datum::Int8(10)),
                        Some(Datum::Float8(1.25)),
                        Some(Datum::Text("one".to_string())),
                        Some(Datum::Vector(vec![1.0, 0.0])),
                        Some(Datum::Int8(20)),
                        Some(Datum::Float8(-2.5)),
                        None,
                        Some(Datum::Vector(vec![0.0, 1.0])),
                    ],
                )
                .unwrap()
        ),
        2
    );
    txn.commit().unwrap();
    assert_eq!(engine.query_stats().entries().last().unwrap().query, insert);

    let selected = rows(
        engine
            .exec_with_params(
                None,
                "SELECT * FROM items WHERE id = $1 ORDER BY big ASC",
                &[Some(Datum::Int4(1))],
            )
            .unwrap(),
    );
    assert_eq!(selected.len(), 2);
    assert_eq!(selected[0][2], Some(Datum::Float8(1.25)));
    assert_eq!(selected[0][3], Some(Datum::Text("one".to_string())));
    assert_eq!(selected[1][2], Some(Datum::Float8(-2.5)));
    assert_eq!(selected[1][3], None);

    assert_eq!(
        affected(
            engine
                .exec_with_params(
                    None,
                    "UPDATE items SET name = $1, score = $2 WHERE big = $3",
                    &[
                        Some(Datum::Text("updated".to_string())),
                        Some(Datum::Float8(9.5)),
                        Some(Datum::Int8(20)),
                    ],
                )
                .unwrap()
        ),
        1
    );
    let updated = rows(
        engine
            .exec_with_params(
                None,
                "SELECT name, score FROM items WHERE big = $1",
                &[Some(Datum::Int8(20))],
            )
            .unwrap(),
    );
    assert_eq!(
        updated,
        vec![vec![
            Some(Datum::Text("updated".to_string())),
            Some(Datum::Float8(9.5)),
        ]]
    );

    assert_eq!(
        affected(
            engine
                .exec_with_params(
                    None,
                    "DELETE FROM items WHERE id = $1",
                    &[Some(Datum::Int4(1))],
                )
                .unwrap()
        ),
        2
    );
    assert!(rows(
        engine
            .exec_with_params(
                None,
                "SELECT * FROM items WHERE id = $1",
                &[Some(Datum::Int4(1))],
            )
            .unwrap()
    )
    .is_empty());
    engine.shutdown();
}

#[test]
fn bind_rejects_missing_extra_and_wrong_type_parameters_before_writes() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    create_items(&engine);

    let err = engine
        .exec_with_params(
            None,
            "INSERT INTO items (id, big) VALUES ($1, $2)",
            &[Some(Datum::Int4(1))],
        )
        .unwrap_err()
        .to_string();
    assert!(err.contains("missing parameter"), "{err}");

    let err = engine
        .exec_with_params(None, "SELECT * FROM items", &[Some(Datum::Int4(1))])
        .unwrap_err()
        .to_string();
    assert!(err.contains("extra parameter"), "{err}");

    for (sql, value) in [
        ("SELECT * FROM items WHERE id = $1", Some(Datum::Int8(1))),
        (
            "INSERT INTO items (id, v) VALUES ($1, $2)",
            Some(Datum::Text("[1,2]".to_string())),
        ),
        (
            "INSERT INTO items (id, name) VALUES ($1, $2)",
            Some(Datum::Vector(vec![1.0, 2.0])),
        ),
        (
            "INSERT INTO items (id, v) VALUES ($1, $2)",
            Some(Datum::Vector(vec![1.0])),
        ),
        (
            "SELECT * FROM items ORDER BY v <-> $1 ASC LIMIT 1",
            Some(Datum::Text("[1,2]".to_string())),
        ),
    ] {
        let params = if sql.contains("$2") {
            vec![Some(Datum::Int4(1)), value]
        } else {
            vec![value]
        };
        let err = engine
            .exec_with_params(None, sql, &params)
            .unwrap_err()
            .to_string();
        assert!(err.contains("parameter"), "{sql}: {err}");
    }
    assert!(engine.scan("items", None).unwrap().is_empty());
    engine.shutdown();
}

#[test]
fn bind_rejects_non_finite_vector_components_before_any_row_is_written() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    create_items(&engine);

    for (component, vector) in [
        (0, vec![f32::NAN, 0.0]),
        (1, vec![0.0, f32::INFINITY]),
        (1, vec![0.0, f32::NEG_INFINITY]),
    ] {
        let err = engine
            .exec_with_params(
                None,
                "INSERT INTO items (id, v) VALUES ($1, $2), ($3, $4)",
                &[
                    Some(Datum::Int4(1)),
                    Some(Datum::Vector(vec![1.0, 2.0])),
                    Some(Datum::Int4(2)),
                    Some(Datum::Vector(vector)),
                ],
            )
            .unwrap_err();
        match err {
            pg_engine::EngineError::InvalidArgument(message) => {
                assert!(message.contains("parameter $4"), "{message}");
                assert!(
                    message.contains(&format!("component {component}")),
                    "{message}"
                );
                assert!(message.contains("not finite"), "{message}");
            }
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }
    assert!(engine.scan("items", None).unwrap().is_empty());
    engine.shutdown();
}

#[test]
fn sql_vector_literals_reject_non_finite_components_without_writes() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    create_items(&engine);

    let huge = format!("1{}.0", "0".repeat(100));
    for (literal, component) in [(format!("[{huge},0]"), 0), (format!("[0,-{huge}]"), 1)] {
        let sql = format!("INSERT INTO items (id, v) VALUES (1, {literal})");
        let err = engine.exec(None, &sql).unwrap_err();
        match err {
            pg_engine::EngineError::InvalidArgument(message) => {
                assert!(message.contains("vector literal"), "{message}");
                assert!(
                    message.contains(&format!("component {component}")),
                    "{message}"
                );
                assert!(message.contains("not finite"), "{message}");
            }
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }
    assert!(engine.scan("items", None).unwrap().is_empty());
    engine.shutdown();
}

#[test]
fn vector_parameters_preserve_ann_planning_and_distance_projection() {
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
    assert_eq!(
        affected(
            engine
                .exec_with_params(
                    None,
                    "INSERT INTO t VALUES ($1, $2), ($3, $4), ($5, $6)",
                    &[
                        Some(Datum::Int4(1)),
                        Some(Datum::Vector(vec![1.0, 0.0])),
                        Some(Datum::Int4(2)),
                        Some(Datum::Vector(vec![0.0, 1.0])),
                        Some(Datum::Int4(3)),
                        Some(Datum::Vector(vec![-1.0, 0.0])),
                    ],
                )
                .unwrap()
        ),
        3
    );

    let query = "SELECT * FROM t ORDER BY v <-> $1 ASC LIMIT 2";
    assert_eq!(
        ids(engine
            .exec_with_params(None, query, &[Some(Datum::Vector(vec![1.0, 0.0]))])
            .unwrap()),
        vec![1, 2]
    );
    let stat = engine.query_stats().entries().last().unwrap().clone();
    assert_eq!(stat.path, ExecutionPath::IndexLookup);
    assert_eq!(stat.query, query);

    let projected = rows(
        engine
            .exec_with_params(
                None,
                "SELECT id, v <-> $1 FROM t WHERE id = $2",
                &[Some(Datum::Vector(vec![1.0, 0.0])), Some(Datum::Int4(2))],
            )
            .unwrap(),
    );
    assert_eq!(projected[0][0], Some(Datum::Int4(2)));
    assert_eq!(
        projected[0][1],
        Some(Datum::Float8(std::f64::consts::SQRT_2))
    );
    engine.shutdown();
}

#[test]
fn parameter_inference_covers_dml_filters_and_both_distance_positions() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    create_items(&engine);

    let insert = parse(
        "INSERT INTO items (name, id, v, score, big) VALUES \
         ($1, $2, $3, $4, $5), ($6, $2, $7, $8, $9)",
    )
    .unwrap();
    assert_eq!(
        engine.parameter_types(&insert).unwrap(),
        vec![
            Some(ColumnType::Text),
            Some(ColumnType::Int4),
            Some(ColumnType::Vector(2)),
            Some(ColumnType::Float8),
            Some(ColumnType::Int8),
            Some(ColumnType::Text),
            Some(ColumnType::Vector(2)),
            Some(ColumnType::Float8),
            Some(ColumnType::Int8),
        ]
    );

    let update = parse("UPDATE items SET name = $1, score = $2 WHERE big = $3").unwrap();
    assert_eq!(
        engine.parameter_types(&update).unwrap(),
        vec![
            Some(ColumnType::Text),
            Some(ColumnType::Float8),
            Some(ColumnType::Int8),
        ]
    );
    let delete = parse("DELETE FROM items WHERE id = $1").unwrap();
    assert_eq!(
        engine.parameter_types(&delete).unwrap(),
        vec![Some(ColumnType::Int4)]
    );

    let select =
        parse("SELECT id, v <-> $1 FROM items WHERE name = $2 ORDER BY v <=> $1 ASC").unwrap();
    assert_eq!(
        engine.parameter_types(&select).unwrap(),
        vec![Some(ColumnType::Vector(2)), Some(ColumnType::Text)]
    );
    let explain = parse("EXPLAIN SELECT * FROM items WHERE big = $1").unwrap();
    assert_eq!(
        engine.parameter_types(&explain).unwrap(),
        vec![Some(ColumnType::Int8)]
    );
    engine.shutdown();
}

#[test]
fn parameter_inference_preserves_gaps_and_rejects_cross_row_conflicts() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    create_items(&engine);

    let gap = parse("SELECT * FROM items WHERE id = $2").unwrap();
    assert_eq!(
        engine.parameter_types(&gap).unwrap(),
        vec![None, Some(ColumnType::Int4)]
    );
    assert!(engine
        .parameter_types(&parse("SELECT * FROM missing").unwrap())
        .unwrap()
        .is_empty());

    for sql in [
        "INSERT INTO items (id, big) VALUES ($1, $1)",
        "INSERT INTO items (id, big) VALUES ($1, $2), ($2, $1)",
    ] {
        let err = engine
            .parameter_types(&parse(sql).unwrap())
            .unwrap_err()
            .to_string();
        assert!(err.contains("incompatible inferred types"), "{sql}: {err}");
    }
    engine.shutdown();
}
