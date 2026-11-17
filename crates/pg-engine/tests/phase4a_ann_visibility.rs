//! Phase 4a Stage B: SQL ANN visibility and SnapshotGuard/vacuum contract.

use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;

use pg_engine::{Datum, Engine, EngineConfig, QueryResult};
use tempfile::TempDir;

const ANN: &str = "SELECT * FROM t ORDER BY v <-> '[1,0]' ASC LIMIT 3";

fn setup() -> (TempDir, Engine) {
    let tmp = TempDir::new().unwrap();
    let engine = Engine::open(tmp.path(), EngineConfig::new(tmp.path())).unwrap();
    engine
        .exec(None, "CREATE TABLE t (id INT4, v VECTOR(2))")
        .unwrap();
    engine
        .exec(
            None,
            "CREATE INDEX ann_idx ON t USING hnsw (v vector_l2_ops)",
        )
        .unwrap();
    (tmp, engine)
}

fn insert(engine: &Engine, id: i32, vector: &str) {
    engine
        .exec(None, &format!("INSERT INTO t VALUES ({id}, {vector})"))
        .unwrap();
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

#[test]
fn sql_ann_hides_in_flight_and_aborted_inserts() {
    let (_tmp, engine) = setup();
    insert(&engine, 1, "[1,0]");
    let writer = engine.begin_txn().unwrap();
    engine
        .exec(Some(&writer), "INSERT INTO t VALUES (2, [0,1])")
        .unwrap();
    assert_eq!(ids(engine.exec(None, ANN).unwrap()), vec![1]);

    writer.abort().unwrap();
    assert_eq!(ids(engine.exec(None, ANN).unwrap()), vec![1]);

    let writer = engine.begin_txn().unwrap();
    engine
        .exec(Some(&writer), "INSERT INTO t VALUES (2, [0,1])")
        .unwrap();
    writer.commit().unwrap();
    assert_eq!(ids(engine.exec(None, ANN).unwrap()), vec![1, 2]);
    engine.shutdown();
}

#[test]
fn sql_ann_old_snapshot_survives_delete_and_vacuum() {
    let (_tmp, engine) = setup();
    insert(&engine, 1, "[1,0]");
    insert(&engine, 2, "[0,1]");
    let reader = engine.begin_txn().unwrap();

    engine.exec(None, "DELETE FROM t WHERE id = 1").unwrap();
    engine.vacuum("t").unwrap();
    assert_eq!(ids(engine.exec(Some(&reader), ANN).unwrap()), vec![1, 2]);
    assert_eq!(ids(engine.exec(None, ANN).unwrap()), vec![2]);

    reader.abort().unwrap();
    engine.vacuum("t").unwrap();
    assert_eq!(ids(engine.exec(None, ANN).unwrap()), vec![2]);
    engine.shutdown();
}

#[test]
fn sql_ann_non_hot_update_serves_old_and_new_snapshots() {
    let (_tmp, engine) = setup();
    // A second index forces the non-vector key update down the non-HOT path.
    engine.create_index("t", "id").unwrap();
    insert(&engine, 1, "[1,0]");
    insert(&engine, 2, "[0,1]");
    let reader = engine.begin_txn().unwrap();

    engine
        .exec(None, "UPDATE t SET id = 3 WHERE id = 1")
        .unwrap();
    assert_eq!(ids(engine.exec(None, ANN).unwrap()), vec![3, 2]);
    assert_eq!(ids(engine.exec(Some(&reader), ANN).unwrap()), vec![1, 2]);

    reader.abort().unwrap();
    engine.vacuum("t").unwrap();
    assert_eq!(ids(engine.exec(None, ANN).unwrap()), vec![3, 2]);
    engine.shutdown();
}

#[test]
fn explain_ann_for_update_does_not_stamp_row_locks() {
    let (_tmp, engine) = setup();
    insert(&engine, 1, "[1,0]");
    let txn = engine.begin_txn().unwrap();
    let result = engine
        .exec(
            Some(&txn),
            "EXPLAIN SELECT * FROM t ORDER BY v <-> '[1,0]' ASC LIMIT 1 FOR UPDATE",
        )
        .unwrap();
    let QueryResult::Rows { rows, .. } = result else {
        panic!("expected EXPLAIN rows")
    };
    assert!(matches!(&rows[0][0], Some(Datum::Text(plan)) if plan.contains("HnswScan")));

    // Releasing only table locks must be enough: EXPLAIN did not stamp xmax.
    engine.lock_manager().release_all(txn.xid());
    engine
        .exec(None, "UPDATE t SET v = [0,1] WHERE id = 1")
        .unwrap();
    txn.abort().unwrap();
    engine.shutdown();
}

#[test]
fn ann_for_update_also_locks_appended_null_rows() {
    let (_tmp, engine) = setup();
    let engine = Arc::new(engine);
    insert(&engine, 1, "[1,0]");
    insert(&engine, 2, "NULL");
    let txn = engine.begin_txn().unwrap();
    let result = engine
        .exec(
            Some(&txn),
            "SELECT id, v <-> '[1,0]' FROM t ORDER BY v <-> '[1,0]' ASC LIMIT 2 FOR UPDATE",
        )
        .unwrap();
    let QueryResult::Rows { rows, .. } = result else {
        panic!("expected rows")
    };
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[1], vec![Some(Datum::Int4(2)), None]);

    let (tx, rx) = mpsc::channel();
    let writer_engine = Arc::clone(&engine);
    let writer = thread::spawn(move || {
        let result = writer_engine.exec(None, "UPDATE t SET v = [0,1] WHERE id = 2");
        tx.send(result).unwrap();
    });
    assert!(
        matches!(
            rx.recv_timeout(Duration::from_millis(200)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ),
        "the appended NULL row must be locked"
    );

    txn.abort().unwrap();
    assert!(rx.recv_timeout(Duration::from_secs(2)).unwrap().is_ok());
    writer.join().unwrap();
    engine.shutdown();
}
