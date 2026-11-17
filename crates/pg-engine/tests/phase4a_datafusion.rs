//! Phase 4a Stage E: optional DataFusion AP integration.

#[cfg(feature = "datafusion")]
use std::sync::Arc;
#[cfg(feature = "datafusion")]
use std::time::{Duration, Instant};

#[cfg(feature = "datafusion")]
use pg_am_heap::access_method::{DeleteContext, RelationDesc};
#[cfg(feature = "datafusion")]
use pg_am_heap::tuple::decode_tuple;
#[cfg(feature = "datafusion")]
use pg_am_heap::{AccessMethod, SlottedPage, ToastPointer};
#[cfg(feature = "datafusion")]
use pg_engine::{
    ColumnDef, ColumnType, Datum, Engine, EngineConfig, EngineError, QueryResult, Value,
};
#[cfg(not(feature = "datafusion"))]
use pg_engine::{Engine, EngineConfig, EngineError};
#[cfg(feature = "datafusion")]
use pg_storage::types::{Tid, PAGE_SIZE};
#[cfg(feature = "datafusion")]
use pg_txn::Snapshot;
#[cfg(feature = "datafusion")]
use tempfile::TempDir;

#[cfg(feature = "datafusion")]
fn open(dir: &std::path::Path) -> Arc<Engine> {
    Arc::new(Engine::open(dir, EngineConfig::new(dir)).unwrap())
}

#[cfg(feature = "datafusion")]
fn column(name: &str, col_type: ColumnType) -> ColumnDef {
    ColumnDef {
        name: name.to_string(),
        col_type,
    }
}

#[cfg(feature = "datafusion")]
fn rows(result: QueryResult) -> Vec<Vec<Value>> {
    let QueryResult::Rows { rows, .. } = result else {
        panic!("expected rows, got {result:?}")
    };
    rows
}

#[cfg(feature = "datafusion")]
fn ids(result: QueryResult) -> Vec<i64> {
    rows(result)
        .into_iter()
        .map(|row| match &row[0] {
            Some(Datum::Int8(value)) => *value,
            other => panic!("expected INT8 id, got {other:?}"),
        })
        .collect()
}

#[cfg(feature = "datafusion")]
const CHUNK_COLUMNS: [ColumnType; 3] = [ColumnType::Int8, ColumnType::Int4, ColumnType::Bytea];

#[cfg(feature = "datafusion")]
fn toast_relation(entry: &pg_engine::TableEntry) -> RelationDesc<'static> {
    let toast = entry.toast.unwrap();
    RelationDesc {
        rel_oid: toast.rel_oid,
        first_page: toast.first_page,
        columns: &CHUNK_COLUMNS,
    }
}

#[cfg(feature = "datafusion")]
fn raw_external_pointer(engine: &Engine, entry: &pg_engine::TableEntry, tid: Tid) -> ToastPointer {
    let columns: Vec<ColumnType> = entry.columns.iter().map(|column| column.col_type).collect();
    let guard = engine.storage().buffer_pool().pin(tid.page_id).unwrap();
    let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
    let bytes = SlottedPage::tuple(page, tid.slot_id).unwrap().unwrap();
    let (_header, values) = decode_tuple(bytes, &columns).unwrap();
    match &values[1] {
        Some(Datum::External(pointer)) => *pointer,
        other => panic!("expected external pointer, got {other:?}"),
    }
}

#[cfg(feature = "datafusion")]
fn chunk_tid(engine: &Engine, entry: &pg_engine::TableEntry, valueid: u32, wanted_seq: u32) -> Tid {
    for page_id in engine
        .heap()
        .relation_pages(&toast_relation(entry))
        .unwrap()
    {
        let guard = engine.storage().buffer_pool().pin(page_id).unwrap();
        let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
        for slot in 0..SlottedPage::slot_count(page) as u16 {
            let Some(bytes) = SlottedPage::tuple(page, slot).unwrap() else {
                continue;
            };
            let (_header, values) = decode_tuple(bytes, &CHUNK_COLUMNS).unwrap();
            if values[0] == Some(Datum::Int8(i64::from(valueid)))
                && values[1] == Some(Datum::Int4(wanted_seq as i32))
            {
                return Tid {
                    page_id,
                    slot_id: slot,
                };
            }
        }
    }
    panic!("chunk ({valueid}, {wanted_seq}) not found")
}

#[cfg(feature = "datafusion")]
fn create_items(engine: &Engine) {
    engine
        .create_table(
            "t",
            &[
                column("id", ColumnType::Int8),
                column("name", ColumnType::Text),
                column("score", ColumnType::Float8),
                column("v", ColumnType::Vector(2)),
            ],
        )
        .unwrap();
    for (id, name) in [(1, "one"), (2, "two"), (3, "three")] {
        engine
            .insert(
                "t",
                &[
                    Some(Datum::Int8(id)),
                    Some(Datum::Text(name.to_string())),
                    Some(Datum::Float8(id as f64 + 0.5)),
                    Some(Datum::Vector(vec![id as f32, 0.0])),
                ],
            )
            .unwrap();
    }
}

#[cfg(feature = "datafusion")]
#[test]
fn projection_order_limit_and_btree_filters_match_self_executor() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    create_items(&engine);

    let result = engine
        .exec_datafusion(
            None,
            "SELECT id, name FROM t WHERE id > 1 ORDER BY id LIMIT 1",
        )
        .unwrap();
    assert_eq!(ids(result.clone()), [2]);
    let QueryResult::Rows { columns, .. } = result else {
        panic!("expected rows")
    };
    assert_eq!(columns, ["id", "name"]);

    assert_eq!(
        ids(engine
            .exec_datafusion(None, "SELECT id FROM t WHERE id < 3")
            .unwrap()),
        [1, 2]
    );
    assert_eq!(
        ids(engine
            .exec_datafusion(None, "SELECT id FROM t WHERE id = 2")
            .unwrap()),
        [2]
    );
    assert!(ids(engine
        .exec_datafusion(None, "SELECT id FROM t WHERE id = 99")
        .unwrap())
    .is_empty());
}

#[cfg(feature = "datafusion")]
#[test]
fn complex_filters_aliases_aggregates_and_joins_work() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    create_items(&engine);

    assert_eq!(
        ids(engine
            .exec_datafusion(None, "SELECT id FROM t WHERE id > 1 AND id < 3")
            .unwrap()),
        [2]
    );

    let aliased = engine
        .exec_datafusion(None, "SELECT id AS user_id FROM t")
        .unwrap();
    assert_eq!(ids(aliased.clone()), [1, 2, 3]);
    let QueryResult::Rows { columns, .. } = aliased else {
        panic!("expected rows")
    };
    assert_eq!(columns, ["user_id"]);

    assert_eq!(
        ids(engine
            .exec_datafusion(None, "SELECT COUNT(id) FROM t")
            .unwrap()),
        [3]
    );
    let total = engine
        .exec_datafusion(None, "SELECT COUNT(id) AS total FROM t")
        .unwrap();
    assert_eq!(ids(total.clone()), [3]);
    let QueryResult::Rows { columns, .. } = total else {
        panic!("expected rows")
    };
    assert_eq!(columns, ["total"]);

    assert_eq!(
        ids(engine
            .exec_datafusion(None, "SELECT id + 1 FROM t")
            .unwrap()),
        [2, 3, 4]
    );

    engine
        .create_table(
            "u",
            &[
                column("id", ColumnType::Int8),
                column("name", ColumnType::Text),
            ],
        )
        .unwrap();
    engine
        .insert(
            "u",
            &[Some(Datum::Int8(2)), Some(Datum::Text("two".to_string()))],
        )
        .unwrap();
    engine
        .insert(
            "u",
            &[Some(Datum::Int8(4)), Some(Datum::Text("four".to_string()))],
        )
        .unwrap();
    assert_eq!(
        ids(engine
            .exec_datafusion(
                None,
                "SELECT t.id FROM t JOIN u ON t.id = u.id WHERE u.id = 2"
            )
            .unwrap()),
        [2]
    );
}

#[cfg(feature = "datafusion")]
#[test]
fn in_flight_and_aborted_rows_follow_snapshot_visibility() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    create_items(&engine);
    let txn = engine.begin_txn().unwrap();
    engine
        .exec(Some(&txn), "INSERT INTO t VALUES (4, 'four', 4.5, [4,0])")
        .unwrap();
    assert_eq!(
        ids(engine
            .exec_datafusion(Some(&txn), "SELECT id FROM t WHERE id = 4")
            .unwrap()),
        [4]
    );
    assert!(ids(engine
        .exec_datafusion(None, "SELECT id FROM t WHERE id = 4")
        .unwrap())
    .is_empty());
    txn.abort().unwrap();
    assert!(ids(engine
        .exec_datafusion(None, "SELECT id FROM t WHERE id = 4")
        .unwrap())
    .is_empty());
}

#[cfg(feature = "datafusion")]
#[test]
fn old_snapshot_stream_survives_update_and_vacuum() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine
        .create_table(
            "t",
            &[
                column("id", ColumnType::Int8),
                column("v", ColumnType::Text),
            ],
        )
        .unwrap();
    let old = "old".repeat(4_500);
    let tid = engine
        .insert("t", &[Some(Datum::Int8(1)), Some(Datum::Text(old.clone()))])
        .unwrap();
    let reader = engine.begin_txn().unwrap();
    let new = "new".repeat(4_500);
    engine
        .update(
            "t",
            tid,
            &[Some(Datum::Int8(1)), Some(Datum::Text(new.clone()))],
        )
        .unwrap();
    engine.vacuum("t").unwrap();

    let old_rows = rows(
        engine
            .exec_datafusion(Some(&reader), "SELECT v FROM t")
            .unwrap(),
    );
    assert_eq!(old_rows[0][0], Some(Datum::Text(old)));
    reader.abort().unwrap();
    engine.vacuum("t").unwrap();
    let new_rows = rows(engine.exec_datafusion(None, "SELECT v FROM t").unwrap());
    assert_eq!(new_rows[0][0], Some(Datum::Text(new)));
}

#[cfg(feature = "datafusion")]
#[test]
fn guard_is_released_after_execution_error() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine
        .create_table(
            "t",
            &[
                column("id", ColumnType::Int8),
                column("v", ColumnType::Text),
            ],
        )
        .unwrap();
    let tid = engine
        .insert(
            "t",
            &[Some(Datum::Int8(1)), Some(Datum::Text("x".repeat(4_500)))],
        )
        .unwrap();
    let entry = engine.describe_table("t").unwrap();
    let pointer = raw_external_pointer(&engine, &entry, tid);
    let chunk_tid = chunk_tid(&engine, &entry, pointer.va_valueid, 0);
    let xid = engine.txn_manager().begin_txn();
    let mut snapshot = Snapshot::everything();
    snapshot.set_current_xid(xid);
    engine
        .heap()
        .delete(DeleteContext {
            rel: toast_relation(&entry),
            snapshot: &snapshot,
            tid: chunk_tid,
            clog: engine.clog().as_ref(),
        })
        .unwrap();
    engine.txn_manager().commit_txn(xid).unwrap();

    let before = engine.oldest_snapshot_xmin();
    assert!(engine.exec_datafusion(None, "SELECT * FROM t").is_err());
    let after = engine.oldest_snapshot_xmin();
    assert_eq!(before, after, "SnapshotGuard leaked after execution error");
}

#[cfg(feature = "datafusion")]
#[test]
fn guard_is_released_after_collect_and_limit() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    create_items(&engine);
    let before = engine.oldest_snapshot_xmin();
    let result = engine
        .exec_datafusion(None, "SELECT id FROM t ORDER BY id LIMIT 1")
        .unwrap();
    assert_eq!(ids(result), [1]);
    let after = engine.oldest_snapshot_xmin();
    assert_eq!(before, after, "SnapshotGuard leaked after query completion");
}

#[cfg(feature = "datafusion")]
#[test]
fn drop_waits_for_explicit_ap_reader() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    create_items(&engine);
    let reader = engine.begin_txn().unwrap();
    assert_eq!(
        ids(engine
            .exec_datafusion(Some(&reader), "SELECT id FROM t")
            .unwrap()),
        [1, 2, 3]
    );

    let (sender, receiver) = std::sync::mpsc::channel();
    let dropping = Arc::clone(&engine);
    let handle = std::thread::spawn(move || {
        dropping.drop_table("t").unwrap();
        sender.send(()).unwrap();
    });
    std::thread::sleep(Duration::from_millis(50));
    assert!(
        receiver.try_recv().is_err(),
        "DROP did not wait for AP AccessShare"
    );
    reader.abort().unwrap();
    handle.join().unwrap();
    assert!(engine.describe_table("t").is_none());
}

#[cfg(feature = "datafusion")]
#[test]
fn arrow_types_and_null_vector_roundtrip() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine
        .create_table(
            "t",
            &[
                column("i4", ColumnType::Int4),
                column("i8", ColumnType::Int8),
                column("f8", ColumnType::Float8),
                column("txt", ColumnType::Text),
                column("bin", ColumnType::Bytea),
                column("ts", ColumnType::Timestamptz),
                column("u", ColumnType::Uuid),
                column("v", ColumnType::Vector(2)),
            ],
        )
        .unwrap();
    let uuid = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
    let values = vec![
        Some(Datum::Int4(7)),
        Some(Datum::Int8(-9_000_000_000)),
        Some(Datum::Float8(1.25)),
        Some(Datum::Text("x".repeat(4_500))),
        Some(Datum::Bytea(vec![0xde, 0xad])),
        Some(Datum::Timestamptz(1_234_567)),
        Some(Datum::Uuid(uuid)),
        None,
    ];
    engine.insert("t", &values).unwrap();
    let result = rows(engine.exec_datafusion(None, "SELECT * FROM t").unwrap());
    assert_eq!(result[0], values);
}

#[cfg(feature = "datafusion")]
#[test]
fn constant_projection_and_zero_batches_preserve_result_schema() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    create_items(&engine);

    let constant = rows(
        engine
            .exec_datafusion(None, "SELECT 1.5 AS constant_value FROM t")
            .unwrap(),
    );
    assert_eq!(constant[0][0], Some(Datum::Float8(1.5)));
    assert_eq!(constant.len(), 3);
    assert_eq!(
        ids(engine
            .exec_datafusion(None, "SELECT COUNT(*) FROM t")
            .unwrap()),
        [3]
    );

    let result = engine
        .exec_datafusion(None, "SELECT id, name FROM t WHERE id = NULL")
        .unwrap();
    let QueryResult::Rows { columns, rows } = result else {
        panic!("expected rows")
    };
    assert_eq!(columns, ["id", "name"]);
    assert!(rows.is_empty());
    engine.shutdown();
}

#[cfg(feature = "datafusion")]
#[test]
fn uuid_aliases_join_null_and_text_collisions_preserve_physical_types() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    let uuid = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
    for table in ["t", "other"] {
        engine
            .create_table(
                table,
                &[
                    column("id", ColumnType::Int8),
                    column("name", ColumnType::Text),
                    column("u", ColumnType::Uuid),
                ],
            )
            .unwrap();
    }
    engine
        .insert(
            "t",
            &[
                Some(Datum::Int8(1)),
                Some(Datum::Text("x".to_string())),
                Some(Datum::Uuid(uuid)),
            ],
        )
        .unwrap();
    engine
        .insert(
            "t",
            &[
                Some(Datum::Int8(2)),
                Some(Datum::Text("y".to_string())),
                None,
            ],
        )
        .unwrap();
    engine
        .insert(
            "other",
            &[
                Some(Datum::Int8(1)),
                Some(Datum::Text("x".to_string())),
                Some(Datum::Uuid(uuid)),
            ],
        )
        .unwrap();

    let aliased = rows(
        engine
            .exec_datafusion(None, "SELECT u AS renamed FROM t")
            .unwrap(),
    );
    assert_eq!(aliased[0][0], Some(Datum::Uuid(uuid)));
    let joined = rows(
        engine
            .exec_datafusion(None, "SELECT other.u FROM t JOIN other ON t.id = other.id")
            .unwrap(),
    );
    assert_eq!(joined[0][0], Some(Datum::Uuid(uuid)));
    let with_null = rows(
        engine
            .exec_datafusion(None, "SELECT u FROM t ORDER BY id")
            .unwrap(),
    );
    assert_eq!(with_null[1][0], None);

    let text = rows(
        engine
            .exec_datafusion(None, "SELECT name AS u FROM t")
            .unwrap(),
    );
    assert_eq!(text[0][0], Some(Datum::Text("x".to_string())));
    let literal = rows(
        engine
            .exec_datafusion(None, "SELECT 'literal' AS u FROM t")
            .unwrap(),
    );
    assert_eq!(literal[0][0], Some(Datum::Text("literal".to_string())));
    engine.shutdown();
}

#[cfg(feature = "datafusion")]
#[test]
fn uuid_string_predicates_support_equality_nested_join_and_null_semantics() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    let first = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
    let second = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440001").unwrap();
    for table in ["t", "other"] {
        engine
            .create_table(
                table,
                &[
                    column("id", ColumnType::Int8),
                    column("name", ColumnType::Text),
                    column("u", ColumnType::Uuid),
                ],
            )
            .unwrap();
    }
    for (id, value) in [(1, Some(first)), (2, Some(second)), (3, None)] {
        engine
            .insert(
                "t",
                &[
                    Some(Datum::Int8(id)),
                    Some(Datum::Text(first.to_string())),
                    value.map(Datum::Uuid),
                ],
            )
            .unwrap();
    }
    engine
        .insert(
            "other",
            &[
                Some(Datum::Int8(1)),
                Some(Datum::Text("other".to_string())),
                Some(Datum::Uuid(first)),
            ],
        )
        .unwrap();
    let literal = first.to_string();

    for sql in [
        format!("SELECT id FROM t WHERE u = '{literal}'"),
        format!("SELECT id FROM t WHERE '{literal}' = u"),
        format!("SELECT id FROM t WHERE id > 0 AND u = '{literal}'"),
        format!("SELECT id FROM t AS x WHERE x.u = '{literal}'"),
    ] {
        assert_eq!(
            ids(engine.exec_datafusion(None, &sql).unwrap()),
            [1],
            "{sql}"
        );
    }
    assert_eq!(
        ids(engine
            .exec_datafusion(None, &format!("SELECT id FROM t WHERE u != '{literal}'"))
            .unwrap()),
        [2]
    );

    let joined = rows(
        engine
            .exec_datafusion(
                None,
                &format!("SELECT other.u FROM t JOIN other ON t.id = other.id WHERE other.u = '{literal}'"),
            )
            .unwrap(),
    );
    assert_eq!(joined[0][0], Some(Datum::Uuid(first)));

    let text = rows(
        engine
            .exec_datafusion(
                None,
                &format!("SELECT name FROM t WHERE name = '{literal}'"),
            )
            .unwrap(),
    );
    assert_eq!(text[0][0], Some(Datum::Text(literal.clone())));

    let empty = engine
        .exec_datafusion(None, "SELECT id FROM t WHERE u = NULL")
        .unwrap();
    let QueryResult::Rows { columns, rows } = empty else {
        panic!("expected rows")
    };
    assert_eq!(columns, ["id"]);
    assert!(rows.is_empty());

    let before = engine.oldest_snapshot_xmin();
    let error = engine
        .exec_datafusion(None, "SELECT id FROM t WHERE u = 'not-a-uuid'")
        .unwrap_err();
    assert!(error.to_string().contains("invalid UUID"), "{error}");
    assert_eq!(engine.oldest_snapshot_xmin(), before);
    engine.shutdown();
}

#[cfg(feature = "datafusion")]
#[test]
fn datafusion_sync_and_async_calls_handle_tokio_runtime_flavors() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    create_items(&engine);
    let initial_stats = engine.query_stats().entries().len();

    let current = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    current.block_on(async {
        let error = engine
            .exec_datafusion(None, "SELECT id FROM t WHERE id = 2")
            .unwrap_err();
        assert!(matches!(error, EngineError::Unsupported(_)));
        assert!(error.to_string().contains("exec_datafusion_async"));
        assert_eq!(engine.query_stats().entries().len(), initial_stats);

        let txn = engine.begin_txn().unwrap();
        engine
            .exec(Some(&txn), "INSERT INTO t VALUES (4, 'four', 4.5, [4,0])")
            .unwrap();
        let own = engine
            .exec_datafusion_async(Some(&txn), "SELECT id FROM t WHERE id = 4")
            .await
            .unwrap();
        assert_eq!(ids(own), [4]);
        txn.abort().unwrap();
        assert_eq!(engine.query_stats().entries().len(), initial_stats + 2);
    });

    let multi = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    multi.block_on(async {
        let result = engine
            .exec_datafusion(None, "SELECT id FROM t WHERE id = 2")
            .unwrap();
        assert_eq!(ids(result), [2]);
    });
    assert_eq!(engine.query_stats().entries().len(), initial_stats + 3);
    engine.shutdown();
}

#[cfg(feature = "datafusion")]
#[test]
#[ignore = "manual latency benchmark; run with --ignored"]
fn datafusion_point_lookup_mean_latency_is_below_5ms() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    create_items(&engine);
    let started = Instant::now();
    for _ in 0..20 {
        assert_eq!(
            ids(engine
                .exec_datafusion(None, "SELECT id FROM t WHERE id = 2")
                .unwrap()),
            [2]
        );
    }
    let mean = started.elapsed().as_secs_f64() / 20.0;
    println!("DataFusion point lookup mean {mean:.6}s");
    assert!(
        mean < 0.005,
        "DataFusion point lookup mean {mean:.6}s exceeds 5ms"
    );
}

#[cfg(feature = "datafusion")]
#[test]
fn ann_and_row_locks_are_rejected() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    create_items(&engine);
    assert!(matches!(
        engine.exec_datafusion(None, "SELECT * FROM t ORDER BY v <-> '[1,0]' ASC LIMIT 1"),
        Err(EngineError::Unsupported(_))
    ));
    assert!(matches!(
        engine.exec_datafusion(None, "SELECT * FROM t FOR UPDATE"),
        Err(EngineError::Unsupported(_))
    ));
}

#[cfg(feature = "datafusion")]
#[test]
fn timestamp_literals_and_reversed_comparisons_work() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine
        .create_table(
            "t",
            &[
                column("id", ColumnType::Int8),
                column("ts", ColumnType::Timestamptz),
            ],
        )
        .unwrap();
    for (id, ts) in [
        (1, 1_700_000_000_000_000),
        (2, 1_800_000_000_000_000),
        (3, 1_900_000_000_000_000),
    ] {
        engine
            .insert("t", &[Some(Datum::Int8(id)), Some(Datum::Timestamptz(ts))])
            .unwrap();
    }
    for (sql, expected) in [
        (
            "SELECT id FROM t WHERE ts > '2024-01-01 00:00:00'",
            vec![2, 3],
        ),
        ("SELECT id FROM t WHERE ts = '2023-11-14 22:13:20'", vec![1]),
        ("SELECT id FROM t WHERE 2 > id", vec![1]),
        ("SELECT id FROM t WHERE 1 < id", vec![2, 3]),
    ] {
        assert_eq!(
            ids(engine.exec_datafusion(None, sql).unwrap()),
            expected,
            "{sql}"
        );
    }
    engine.shutdown();
}

#[cfg(feature = "datafusion")]
#[test]
fn old_snapshot_ap_with_in_flight_writer_uses_btree_fallback() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine
        .create_table("t", &[column("id", ColumnType::Int8)])
        .unwrap();
    engine.create_index("t", "id").unwrap();
    for id in 1..=3 {
        engine.insert("t", &[Some(Datum::Int8(id))]).unwrap();
    }

    let reader = engine.begin_txn().unwrap();
    let writer = engine.begin_txn().unwrap();
    engine
        .exec(Some(&writer), "INSERT INTO t VALUES (4)")
        .unwrap();
    assert_eq!(
        ids(engine
            .exec_datafusion(Some(&reader), "SELECT id FROM t WHERE id >= 1")
            .unwrap()),
        [1, 2, 3]
    );
    writer.abort().unwrap();
    reader.abort().unwrap();
    engine.shutdown();
}

#[cfg(feature = "datafusion")]
#[test]
fn limit_zero_and_count_on_empty_table() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    create_items(&engine);
    let result = engine
        .exec_datafusion(None, "SELECT id FROM t LIMIT 0")
        .unwrap();
    let QueryResult::Rows { columns, rows } = result else {
        panic!("expected rows");
    };
    assert_eq!(columns, ["id"]);
    assert!(rows.is_empty());

    engine
        .create_table("empty", &[column("id", ColumnType::Int8)])
        .unwrap();
    assert_eq!(
        ids(engine
            .exec_datafusion(None, "SELECT COUNT(*) FROM empty")
            .unwrap()),
        [0]
    );
    engine.shutdown();
}

#[cfg(feature = "datafusion")]
#[test]
fn wrong_instance_txn_is_rejected() {
    let tmp_a = TempDir::new().unwrap();
    let tmp_b = TempDir::new().unwrap();
    let engine_a = open(tmp_a.path());
    let engine_b = open(tmp_b.path());
    let txn_b = engine_b.begin_txn().unwrap();
    let error = engine_a
        .exec_datafusion(Some(&txn_b), "SELECT * FROM x")
        .unwrap_err();
    assert!(matches!(error, EngineError::InvalidArgument(_)));
    assert!(error.to_string().contains("different Engine"));
    txn_b.abort().unwrap();
    engine_a.shutdown();
    engine_b.shutdown();
}

#[cfg(feature = "datafusion")]
#[test]
fn or_filter_and_join_duplicate_columns_preserve_rows() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine
        .create_table(
            "t",
            &[
                column("id", ColumnType::Int8),
                column("v", ColumnType::Int8),
            ],
        )
        .unwrap();
    for (id, value) in [(1, 10), (2, 20), (3, 30)] {
        engine
            .insert("t", &[Some(Datum::Int8(id)), Some(Datum::Int8(value))])
            .unwrap();
    }
    assert_eq!(
        ids(engine
            .exec_datafusion(None, "SELECT id FROM t WHERE id = 1 OR id = 3")
            .unwrap()),
        [1, 3]
    );

    engine
        .create_table(
            "u",
            &[
                column("id", ColumnType::Int8),
                column("w", ColumnType::Int8),
            ],
        )
        .unwrap();
    for (id, value) in [(1, 100), (2, 200)] {
        engine
            .insert("u", &[Some(Datum::Int8(id)), Some(Datum::Int8(value))])
            .unwrap();
    }
    let joined = engine
        .exec_datafusion(None, "SELECT * FROM t JOIN u ON t.id = u.id ORDER BY t.id")
        .unwrap();
    let QueryResult::Rows { columns, rows } = joined else {
        panic!("expected rows");
    };
    assert_eq!(columns, ["id", "v", "id", "w"]);
    assert_eq!(
        rows,
        vec![
            vec![
                Some(Datum::Int8(1)),
                Some(Datum::Int8(10)),
                Some(Datum::Int8(1)),
                Some(Datum::Int8(100)),
            ],
            vec![
                Some(Datum::Int8(2)),
                Some(Datum::Int8(20)),
                Some(Datum::Int8(2)),
                Some(Datum::Int8(200)),
            ],
        ]
    );
    engine.shutdown();
}

#[cfg(not(feature = "datafusion"))]
#[test]
fn datafusion_is_explicitly_unsupported_without_feature() {
    let tmp = tempfile::TempDir::new().unwrap();
    let engine =
        std::sync::Arc::new(Engine::open(tmp.path(), EngineConfig::new(tmp.path())).unwrap());
    let error = engine.exec_datafusion(None, "SELECT 1").unwrap_err();
    assert!(matches!(error, EngineError::Unsupported(_)));
}
