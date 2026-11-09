//! Phase 4a Stage B: ANN execution, exact SeqScan fallbacks, and EXPLAIN.

use std::collections::HashMap;
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;

use pg_engine::{Datum, Engine, EngineConfig, ExecutionPath, QueryResult, Tid};
use tempfile::TempDir;

fn open(dir: &std::path::Path) -> Engine {
    Engine::open(dir, EngineConfig::new(dir)).unwrap()
}

fn setup(index_opclass: Option<&str>) -> (TempDir, Engine) {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine
        .exec(None, "CREATE TABLE t (id INT4, v VECTOR(2))")
        .unwrap();
    if let Some(opclass) = index_opclass {
        engine
            .exec(
                None,
                &format!("CREATE INDEX ann_idx ON t USING hnsw (v {opclass})"),
            )
            .unwrap();
    }
    engine
        .exec(
            None,
            "INSERT INTO t VALUES (1, [1,0]), (2, [0,1]), (3, [-1,0])",
        )
        .unwrap();
    (tmp, engine)
}

fn ids(result: QueryResult) -> Vec<i32> {
    let QueryResult::Rows { rows, .. } = result else {
        panic!("expected rows")
    };
    rows.into_iter()
        .map(|row| match row[0] {
            Some(Datum::Int4(id)) => id,
            ref other => panic!("expected INT4 id, got {other:?}"),
        })
        .collect()
}

fn plan_text(result: QueryResult) -> String {
    let QueryResult::Rows { rows, .. } = result else {
        panic!("expected EXPLAIN rows")
    };
    match &rows[0][0] {
        Some(Datum::Text(text)) => text.clone(),
        other => panic!("expected plan text, got {other:?}"),
    }
}

#[test]
fn ann_sql_matches_m6_api_rows_and_order_for_every_metric_after_reopen() {
    for (opclass, operator, explain_opclass) in [
        ("vector_l2_ops", "<->", "VectorL2Ops"),
        ("vector_cosine_ops", "<=>", "VectorCosineOps"),
        ("vector_ip_ops", "<#>", "VectorIpOps"),
    ] {
        let (_tmp, engine) = setup(Some(opclass));
        engine.shutdown();
        let engine = open(_tmp.path());
        let query = format!("SELECT * FROM t ORDER BY v {operator} '[1,0]' ASC LIMIT 2");
        let plan = plan_text(engine.exec(None, &format!("EXPLAIN {query}")).unwrap());
        assert!(plan.contains("HnswScan"), "{opclass}: {plan}");
        assert!(plan.contains("ef=64"), "{plan}");
        assert!(plan.contains(explain_opclass), "{plan}");
        assert!(plan.contains("k=2"), "{plan}");
        assert!(plan.contains("candidates=4"), "{plan}");
        assert!(plan.contains("max_retry_rounds=3"), "{plan}");

        let txn = engine.begin_txn().unwrap();
        let actual = ids(engine.exec(Some(&txn), &query).unwrap());
        let hits = engine
            .hnsw_search_visible("t", &[1.0, 0.0], 2, Some(64), &txn.snapshot())
            .unwrap();
        let tid_to_id: HashMap<Tid, i32> = engine
            .scan("t", None)
            .unwrap()
            .into_iter()
            .map(|(tid, values)| match values[0] {
                Some(Datum::Int4(id)) => (tid, id),
                ref other => panic!("unexpected id: {other:?}"),
            })
            .collect();
        let expected: Vec<i32> = hits.into_iter().map(|(tid, _)| tid_to_id[&tid]).collect();
        assert_eq!(actual, expected, "{opclass}");
        assert_eq!(actual, vec![1, 2], "{opclass}");
        assert_eq!(
            engine.query_stats().entries().last().unwrap().path,
            ExecutionPath::IndexLookup
        );
        txn.abort().unwrap();
        engine.shutdown();
    }
}

#[test]
fn ann_fallbacks_execute_exact_distance_order() {
    for (index_opclass, suffix, reason, expected) in [
        (None, "ASC LIMIT 2", "NoIndex", vec![1, 2]),
        (
            Some("vector_cosine_ops"),
            "ASC LIMIT 2",
            "MetricMismatch",
            vec![1, 2],
        ),
        (Some("vector_l2_ops"), "ASC", "MissingLimit", vec![1, 2, 3]),
        (Some("vector_l2_ops"), "DESC LIMIT 3", "Desc", vec![3, 2, 1]),
    ] {
        let (_tmp, engine) = setup(index_opclass);
        let query = format!("SELECT * FROM t ORDER BY v <-> '[1,0]' {suffix}");
        assert_eq!(
            ids(engine.exec(None, &query).unwrap()),
            expected,
            "{reason}"
        );
        assert_eq!(
            engine.query_stats().entries().last().unwrap().path,
            ExecutionPath::SeqScan
        );
        let plan = plan_text(engine.exec(None, &format!("EXPLAIN {query}")).unwrap());
        assert!(plan.contains("SeqScan"), "{plan}");
        assert!(plan.contains(reason), "{plan}");
        engine.shutdown();
    }

    let (_tmp, engine) = setup(Some("vector_l2_ops"));
    let query = "SELECT * FROM t WHERE id > 1 ORDER BY v <-> '[1,0]' ASC LIMIT 2";
    assert_eq!(ids(engine.exec(None, query).unwrap()), vec![2, 3]);
    assert_eq!(
        engine.query_stats().entries().last().unwrap().path,
        ExecutionPath::SeqScan
    );
    let plan = plan_text(engine.exec(None, &format!("EXPLAIN {query}")).unwrap());
    assert!(plan.contains("ExtraWhere"), "{plan}");
    assert!(plan.contains("predicate=Some"), "{plan}");
    assert!(plan.contains("limit=Some(2)"), "{plan}");
    engine.shutdown();
}

#[test]
fn ann_for_update_locks_only_the_limited_row() {
    let (_tmp, engine) = setup(Some("vector_l2_ops"));
    let engine = Arc::new(engine);
    let txn = engine.begin_txn().unwrap();
    let result = engine
        .exec(
            Some(&txn),
            "SELECT * FROM t ORDER BY v <-> '[1,0]' ASC LIMIT 1 FOR UPDATE",
        )
        .unwrap();
    assert_eq!(ids(result), vec![1]);

    let (tx, rx) = mpsc::channel();
    let writer_engine = Arc::clone(&engine);
    let writer = thread::spawn(move || {
        let result = writer_engine.exec(None, "UPDATE t SET v = [9,9] WHERE id = 1");
        tx.send(result).unwrap();
    });
    assert!(matches!(
        rx.recv_timeout(Duration::from_millis(200)),
        Err(mpsc::RecvTimeoutError::Timeout)
    ));
    engine
        .exec(None, "UPDATE t SET v = [0,2] WHERE id = 2")
        .unwrap();

    txn.abort().unwrap();
    assert!(rx.recv_timeout(Duration::from_secs(2)).unwrap().is_ok());
    writer.join().unwrap();

    // Auto-commit FOR UPDATE releases its row lock with the statement.
    let result = engine
        .exec(
            None,
            "SELECT * FROM t ORDER BY v <-> '[1,0]' ASC LIMIT 1 FOR UPDATE",
        )
        .unwrap();
    assert_eq!(ids(result), vec![3]);
    engine
        .exec(None, "UPDATE t SET v = [8,8] WHERE id = 3")
        .unwrap();
    engine.shutdown();
}

#[test]
fn invalid_ann_filter_does_not_stamp_row_locks() {
    let (_tmp, engine) = setup(Some("vector_l2_ops"));
    let engine = Arc::new(engine);
    let txn = engine.begin_txn().unwrap();
    let err = engine
        .exec(
            Some(&txn),
            "SELECT * FROM t WHERE missing = 1 ORDER BY v <-> '[1,0]' ASC LIMIT 1 FOR UPDATE",
        )
        .unwrap_err();
    assert!(matches!(err, pg_engine::EngineError::InvalidPredicate(_)));

    engine.lock_manager().release_all(txn.xid());
    let (tx, rx) = mpsc::channel();
    let writer_engine = Arc::clone(&engine);
    let writer = thread::spawn(move || {
        tx.send(writer_engine.exec(None, "UPDATE t SET v = [4,5] WHERE id = 1"))
            .unwrap();
    });
    assert!(rx.recv_timeout(Duration::from_secs(2)).unwrap().is_ok());
    writer.join().unwrap();
    txn.abort().unwrap();
    engine.shutdown();
}

#[test]
fn ann_rejects_invalid_vectors_and_zero_limit_is_empty() {
    let (_tmp, engine) = setup(Some("vector_l2_ops"));
    for query in [
        "SELECT * FROM t ORDER BY v <-> '[1]' ASC LIMIT 1",
        "SELECT * FROM t ORDER BY v <-> 'not-a-vector' ASC LIMIT 1",
        "SELECT * FROM t ORDER BY v <-> '[1,NaN]' ASC LIMIT 1",
        "SELECT * FROM t ORDER BY v <=> '[0,0]' ASC LIMIT 1",
        "SELECT * FROM t ORDER BY id <-> '[1,0]' ASC LIMIT 1",
    ] {
        assert!(engine.exec(None, query).is_err(), "must reject {query}");
        assert!(
            engine.exec(None, &format!("EXPLAIN {query}")).is_err(),
            "EXPLAIN must reject {query}"
        );
    }
    let result = engine
        .exec(None, "SELECT * FROM t ORDER BY v <-> '[1,0]' ASC LIMIT 0")
        .unwrap();
    assert!(ids(result).is_empty());
    engine.shutdown();
}

#[test]
fn huge_ann_limit_is_bounded_by_the_actual_graph_size() {
    let query = "SELECT * FROM t ORDER BY v <-> '[1,0]' ASC LIMIT 9223372036854775807";

    let tmp = TempDir::new().unwrap();
    let empty = open(tmp.path());
    empty
        .exec(None, "CREATE TABLE t (id INT4, v VECTOR(2))")
        .unwrap();
    empty
        .exec(
            None,
            "CREATE INDEX ann_idx ON t USING hnsw (v vector_l2_ops)",
        )
        .unwrap();
    assert!(ids(empty.exec(None, query).unwrap()).is_empty());
    assert_eq!(
        empty.query_stats().entries().last().unwrap().path,
        ExecutionPath::IndexLookup
    );
    empty.shutdown();

    let (_tmp, engine) = setup(Some("vector_l2_ops"));
    assert_eq!(ids(engine.exec(None, query).unwrap()), vec![1, 2, 3]);
    assert_eq!(
        engine.query_stats().entries().last().unwrap().path,
        ExecutionPath::IndexLookup
    );
    engine.shutdown();
}
