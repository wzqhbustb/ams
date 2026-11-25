//! Phase 5b-1 Stage A: graph DDL, DML, derived indexes, and recovery.

use std::path::Path;
use std::sync::{Arc, Barrier};

#[cfg(feature = "test-harness")]
use std::process::Command;

#[cfg(feature = "test-harness")]
use pg_am_graph::{GraphItem, GraphKeyKind};
use pg_am_graph::{GraphKeyCodec, GraphMetadata, GraphStore, GRAPH_RECORD_EDGE, GRAPH_RECORD_NODE};
use pg_am_heap::{tuple::decode_tuple, SlottedPage};
use pg_catalog::system_tables::PG_INDEX;
use pg_engine::planner::AmKind;
use pg_engine::{Engine, EngineConfig, EngineError, QueryResult, Value};
use pg_storage::types::{Oid, PageId, PAGE_SIZE};
use tempfile::TempDir;

fn open(dir: &Path) -> Engine {
    Engine::open(dir, EngineConfig::new(dir)).unwrap()
}

fn affected(result: QueryResult) -> usize {
    match result {
        QueryResult::Affected(count) => count,
        other => panic!("expected affected count, got {other:?}"),
    }
}

fn graph_oid(engine: &Engine) -> Oid {
    let indexes = engine.indexes();
    assert_eq!(indexes.len(), 3);
    assert!(indexes.iter().all(|index| {
        index.kind == AmKind::Graph && index.key_type == pg_engine::ColumnType::Bytea
    }));
    indexes[0].table_oid
}

fn graph_table(engine: &Engine) -> String {
    format!("__graph_{}", graph_oid(engine).0)
}

fn graph_rows(engine: &Engine) -> Vec<(pg_engine::Tid, Vec<Value>)> {
    let table = graph_table(engine);
    engine.scan(&table, None).unwrap()
}

fn validate_graph_indexes(engine: &Engine) {
    for entry in engine.indexes() {
        let index = pg_am_btree::BTreeIndex::open(
            Arc::clone(engine.storage().buffer_pool()),
            Arc::clone(engine.storage().wal_writer()),
            entry.index_oid,
            entry.meta_page,
            pg_engine::ColumnType::Bytea,
        )
        .unwrap();
        index.validate().unwrap();
    }
}

fn assert_identity_absent(engine: &Engine, record_type: i32, id: i64) {
    for entry in engine.indexes() {
        if entry.column != "id" {
            continue;
        }
        let index = pg_am_btree::BTreeIndex::open(
            Arc::clone(engine.storage().buffer_pool()),
            Arc::clone(engine.storage().wal_writer()),
            entry.index_oid,
            entry.meta_page,
            pg_engine::ColumnType::Bytea,
        )
        .unwrap();
        assert!(
            index
                .lookup_all(&GraphKeyCodec::identity(record_type, id))
                .unwrap()
                .is_empty(),
            "stale identity entry for record_type={record_type}, id={id}"
        );
    }
}

#[cfg(feature = "test-harness")]
fn assert_graph_item_indexed(
    engine: &Engine,
    kind: GraphKeyKind,
    item: &GraphItem,
    tid: pg_engine::Tid,
) {
    let column = match kind {
        GraphKeyKind::Identity => "id",
        GraphKeyKind::Out => "src",
        GraphKeyKind::In => "dst",
    };
    let entry = engine
        .indexes()
        .into_iter()
        .find(|entry| entry.column == column)
        .expect("missing graph index");
    let index = pg_am_btree::BTreeIndex::open(
        Arc::clone(engine.storage().buffer_pool()),
        Arc::clone(engine.storage().wal_writer()),
        entry.index_oid,
        entry.meta_page,
        pg_engine::ColumnType::Bytea,
    )
    .unwrap();
    let key = GraphKeyCodec::encode(kind, item).unwrap();
    assert!(
        index.lookup_all(&key).unwrap().contains(&tid),
        "missing {kind:?} entry for {item:?} at {tid}"
    );
}

#[cfg(feature = "test-harness")]
fn assert_graph_item_has_index_entries(engine: &Engine, kind: GraphKeyKind, item: &GraphItem) {
    let column = match kind {
        GraphKeyKind::Identity => "id",
        GraphKeyKind::Out => "src",
        GraphKeyKind::In => "dst",
    };
    let entry = engine
        .indexes()
        .into_iter()
        .find(|entry| entry.column == column)
        .expect("missing graph index");
    let index = pg_am_btree::BTreeIndex::open(
        Arc::clone(engine.storage().buffer_pool()),
        Arc::clone(engine.storage().wal_writer()),
        entry.index_oid,
        entry.meta_page,
        pg_engine::ColumnType::Bytea,
    )
    .unwrap();
    let key = GraphKeyCodec::encode(kind, item).unwrap();
    assert!(
        !index.lookup_all(&key).unwrap().is_empty(),
        "missing {kind:?} entry for {item:?}"
    );
}

struct CommitFailGuard;

impl CommitFailGuard {
    fn arm() -> Self {
        pg_txn::manager::test_hooks::set_commit_txn_force_fail(true);
        Self
    }
}

impl Drop for CommitFailGuard {
    fn drop(&mut self) {
        pg_txn::manager::test_hooks::set_commit_txn_force_fail(false);
    }
}

#[test]
fn graph_dml_and_reopen_preserve_items_indexes_and_sequence() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    assert!(matches!(
        engine.exec(None, "CREATE GRAPH memory"),
        Ok(QueryResult::Ok)
    ));
    assert!(matches!(
        engine.exec(None, "CREATE GRAPH memory"),
        Err(EngineError::GraphExists(_))
    ));
    let internal_table = graph_table(&engine);
    assert!(matches!(
        engine.create_index(&internal_table, "id"),
        Err(EngineError::Unsupported(_))
    ));
    assert!(matches!(
        engine.drop_table(&internal_table),
        Err(EngineError::Unsupported(_))
    ));
    assert_eq!(
        affected(
            engine
                .exec(
                    None,
                    "GRAPH ADD NODE memory 1 PROPERTIES '{\"kind\":\"agent\"}'"
                )
                .unwrap()
        ),
        1
    );
    assert!(matches!(
        engine.exec(None, "GRAPH ADD NODE memory 1 PROPERTIES '{}'"),
        Err(EngineError::InvalidArgument(_))
    ));
    for id in [2, 3] {
        assert_eq!(
            affected(
                engine
                    .exec(
                        None,
                        &format!("GRAPH ADD NODE memory {id} PROPERTIES '{{}}'")
                    )
                    .unwrap()
            ),
            1
        );
    }
    assert!(matches!(
        engine.exec(None, "GRAPH ADD EDGE memory 1 99 PROPERTIES '{}'"),
        Err(EngineError::InvalidArgument(_))
    ));
    assert_eq!(
        affected(
            engine
                .exec(
                    None,
                    "GRAPH ADD EDGE memory 1 2 PROPERTIES '{\"weight\":1}'"
                )
                .unwrap()
        ),
        1
    );
    assert_eq!(
        affected(
            engine
                .exec(None, "GRAPH ADD EDGE memory 2 3 PROPERTIES '{}'")
                .unwrap()
        ),
        1
    );
    assert_eq!(
        affected(
            engine
                .exec(
                    None,
                    "GRAPH UPDATE NODE memory 1 PROPERTIES '{\"kind\":\"updated\"}'"
                )
                .unwrap()
        ),
        1
    );
    assert_eq!(
        affected(engine.exec(None, "GRAPH DELETE EDGE memory 2").unwrap()),
        1
    );
    assert_eq!(
        affected(
            engine
                .exec(None, "GRAPH ADD EDGE memory 3 1 PROPERTIES '{}'")
                .unwrap()
        ),
        1
    );
    assert_eq!(
        affected(engine.exec(None, "GRAPH DELETE NODE memory 1").unwrap()),
        1
    );
    let rows = graph_rows(&engine);
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|(_, values)| {
        values[0] == Some(pg_engine::Datum::Int4(GRAPH_RECORD_NODE))
            && matches!(values[1], Some(pg_engine::Datum::Int8(2 | 3)))
    }));
    validate_graph_indexes(&engine);
    engine.shutdown();

    let engine = open(tmp.path());
    assert_eq!(graph_rows(&engine).len(), 2);
    validate_graph_indexes(&engine);
    assert_eq!(
        affected(
            engine
                .exec(None, "GRAPH ADD EDGE memory 2 3 PROPERTIES '{}'")
                .unwrap()
        ),
        1
    );
    let edge = graph_rows(&engine)
        .into_iter()
        .find(|(_, values)| values[0] == Some(pg_engine::Datum::Int4(GRAPH_RECORD_EDGE)))
        .expect("edge inserted after reopen");
    assert_eq!(edge.1[1], Some(pg_engine::Datum::Int8(4)));
    engine.shutdown();
}

#[test]
fn graph_transactions_toast_vacuum_and_abort_are_consistent() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine.exec(None, "CREATE GRAPH memory").unwrap();
    let large = format!("{{\"text\":\"{}\"}}", "x".repeat(5000));

    let txn = engine.begin_txn().unwrap();
    engine
        .exec(
            Some(&txn),
            &format!("GRAPH ADD NODE memory 1 PROPERTIES '{large}'"),
        )
        .unwrap();
    engine
        .exec(Some(&txn), "GRAPH ADD NODE memory 2 PROPERTIES '{}'")
        .unwrap();
    txn.abort().unwrap();
    assert!(graph_rows(&engine).is_empty());
    assert_identity_absent(&engine, GRAPH_RECORD_NODE, 1);

    engine
        .exec(
            None,
            &format!("GRAPH ADD NODE memory 1 PROPERTIES '{large}'"),
        )
        .unwrap();
    engine
        .exec(None, "GRAPH ADD NODE memory 2 PROPERTIES '{}'")
        .unwrap();
    engine
        .exec(None, "GRAPH ADD EDGE memory 1 2 PROPERTIES '{}'")
        .unwrap();
    let table = graph_table(&engine);
    let rows = graph_rows(&engine);
    assert!(rows
        .iter()
        .any(|(_, values)| values[4] == Some(pg_engine::Datum::Jsonb(large.clone()))));
    engine.vacuum(&table).unwrap();
    assert!(graph_rows(&engine)
        .iter()
        .any(|(_, values)| values[4] == Some(pg_engine::Datum::Jsonb(large.clone()))));
    validate_graph_indexes(&engine);
    engine.shutdown();

    let engine = open(tmp.path());
    assert!(graph_rows(&engine)
        .iter()
        .any(|(_, values)| values[4] == Some(pg_engine::Datum::Jsonb(large.clone()))));
    validate_graph_indexes(&engine);
    engine.shutdown();
}

#[test]
fn graph_create_commit_failure_allows_retry_and_cleans_orphan_metadata() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    let hook = CommitFailGuard::arm();
    assert!(engine.exec(None, "CREATE GRAPH memory").is_err());
    drop(hook);
    assert!(engine.indexes().is_empty());
    engine.exec(None, "CREATE GRAPH memory").unwrap();
    let graph_oid = graph_oid(&engine);
    let metadata_path = GraphStore::metadata_path(tmp.path(), graph_oid);
    assert!(metadata_path.exists());
    engine.shutdown();

    let orphan_oid = Oid(900_001);
    let orphan_metadata =
        GraphMetadata::new(orphan_oid, "orphan", true, PageId(42), Oid(900_002)).unwrap();
    GraphStore::create(tmp.path(), orphan_metadata).unwrap();
    let orphan_path = tmp.path().join("graph").join(orphan_oid.0.to_string());
    assert!(orphan_path.exists());
    let engine = open(tmp.path());
    assert!(!orphan_path.exists());
    assert!(GraphStore::open(tmp.path(), graph_oid).unwrap().is_some());
    engine.shutdown();
}

#[test]
fn graph_undirected_self_loop_snapshot_and_input_validation() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine.exec(None, "CREATE GRAPH memory UNDIRECTED").unwrap();
    let graph_oid = graph_oid(&engine);
    assert!(
        !GraphStore::open(tmp.path(), graph_oid)
            .unwrap()
            .unwrap()
            .metadata()
            .directed
    );
    for sql in [
        "GRAPH ADD NODE memory 1 PROPERTIES NULL",
        "GRAPH ADD NODE memory 1 PROPERTIES 'not json'",
        "GRAPH ADD NODE memory 'not an id' PROPERTIES '{}'",
    ] {
        assert!(engine.exec(None, sql).is_err(), "must reject: {sql}");
    }
    engine
        .exec(None, "GRAPH ADD NODE memory 1 PROPERTIES '{\"v\":1}'")
        .unwrap();
    engine
        .exec(None, "GRAPH ADD NODE memory 2 PROPERTIES '{}'")
        .unwrap();
    for _ in 0..2 {
        engine
            .exec(None, "GRAPH ADD EDGE memory 1 1 PROPERTIES '{}'")
            .unwrap();
    }
    let old_snapshot = engine.begin_txn().unwrap();
    engine
        .exec(None, "GRAPH UPDATE NODE memory 1 PROPERTIES '{\"v\":2}'")
        .unwrap();
    let table = graph_table(&engine);
    let result = engine
        .exec(Some(&old_snapshot), &format!("SELECT * FROM {table}"))
        .unwrap();
    let QueryResult::Rows { rows, .. } = result else {
        panic!("expected rows")
    };
    assert!(rows
        .iter()
        .any(|row| row[4] == Some(pg_engine::Datum::Jsonb("{\"v\":1}".to_string()))));
    old_snapshot.abort().unwrap();
    assert!(graph_rows(&engine)
        .iter()
        .any(|(_, values)| values[4] == Some(pg_engine::Datum::Jsonb("{\"v\":2}".to_string()))));

    engine.exec(None, "GRAPH DELETE NODE memory 1").unwrap();
    assert!(graph_rows(&engine)
        .iter()
        .all(|(_, values)| { values[0] != Some(pg_engine::Datum::Int4(GRAPH_RECORD_EDGE)) }));
    validate_graph_indexes(&engine);
    engine.shutdown();
}

#[test]
fn graph_explicit_transactions_recheck_integrity_after_lock_wait() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine.exec(None, "CREATE GRAPH memory").unwrap();
    let first = engine.begin_txn().unwrap();
    let second = engine.begin_txn().unwrap();
    engine
        .exec(Some(&first), "GRAPH ADD NODE memory 1 PROPERTIES '{}'")
        .unwrap();
    first.commit().unwrap();
    assert!(
        engine
            .exec(Some(&second), "GRAPH ADD NODE memory 1 PROPERTIES '{}'")
            .is_err(),
        "stale explicit snapshot must not admit a duplicate node"
    );
    second.abort().unwrap();
    assert_eq!(graph_rows(&engine).len(), 1);

    engine
        .exec(None, "GRAPH ADD NODE memory 2 PROPERTIES '{}'")
        .unwrap();
    let first = engine.begin_txn().unwrap();
    let second = engine.begin_txn().unwrap();
    engine
        .exec(Some(&first), "GRAPH DELETE NODE memory 1")
        .unwrap();
    first.commit().unwrap();
    assert!(
        engine
            .exec(Some(&second), "GRAPH ADD EDGE memory 1 2 PROPERTIES '{}'")
            .is_err(),
        "stale explicit snapshot must not admit an edge to a deleted node"
    );
    second.abort().unwrap();
    assert!(graph_rows(&engine)
        .iter()
        .all(|(_, values)| { values[0] != Some(pg_engine::Datum::Int4(GRAPH_RECORD_EDGE)) }));
    validate_graph_indexes(&engine);
    engine.shutdown();
}

#[cfg(feature = "test-harness")]
const GRAPH_LOSER_CHILD: &str = "PG_GRAPH_LOSER_CHILD";
#[cfg(feature = "test-harness")]
const GRAPH_VACUUM_CHILD: &str = "PG_GRAPH_VACUUM_CHILD";

#[cfg(feature = "test-harness")]
fn spawn_graph_test(test_name: &str, child_env: &str, data_dir: &Path) -> std::process::ExitStatus {
    Command::new(std::env::current_exe().unwrap())
        .arg(test_name)
        .arg("--exact")
        .arg("--nocapture")
        .env(child_env, data_dir)
        .status()
        .unwrap()
}

#[cfg(feature = "test-harness")]
#[test]
fn graph_loser_delete_is_compensated_after_process_crash() {
    if std::env::var_os(GRAPH_LOSER_CHILD).is_some() {
        let data_dir = std::env::var_os(GRAPH_LOSER_CHILD).unwrap();
        let engine = open(Path::new(&data_dir));
        engine.exec(None, "CREATE GRAPH memory").unwrap();
        engine
            .exec(None, "GRAPH ADD NODE memory 1 PROPERTIES '{}'")
            .unwrap();
        engine
            .exec(None, "GRAPH ADD NODE memory 2 PROPERTIES '{}'")
            .unwrap();
        engine
            .exec(None, "GRAPH ADD EDGE memory 1 2 PROPERTIES '{}'")
            .unwrap();
        pg_engine::engine::crash_before_auto_commit_for_test();
        let _ = engine.exec(None, "GRAPH DELETE EDGE memory 1");
        unreachable!("crash hook must terminate the child process");
    }

    let tmp = TempDir::new().unwrap();
    let status = spawn_graph_test(
        "graph_loser_delete_is_compensated_after_process_crash",
        GRAPH_LOSER_CHILD,
        tmp.path(),
    );
    assert!(!status.success());
    let _ = std::fs::remove_file(tmp.path().join("lock"));
    let engine = open(tmp.path());
    let (edge_tid, _) = graph_rows(&engine)
        .into_iter()
        .find(|(_, values)| values[0] == Some(pg_engine::Datum::Int4(GRAPH_RECORD_EDGE)))
        .expect("loser delete left the live edge without index entries");
    let edge = GraphItem::edge(1, 1, 2, "{}").unwrap();
    for kind in [GraphKeyKind::Identity, GraphKeyKind::Out, GraphKeyKind::In] {
        assert_graph_item_indexed(&engine, kind, &edge, edge_tid);
    }
    validate_graph_indexes(&engine);
    engine.shutdown();

    // A second recovery is a no-op, not a duplicate insertion.
    let engine = open(tmp.path());
    assert_graph_item_indexed(&engine, GraphKeyKind::Identity, &edge, edge_tid);
    validate_graph_indexes(&engine);
    engine.shutdown();
}

#[cfg(feature = "test-harness")]
#[test]
fn graph_vacuum_crash_after_index_cleanup_converges() {
    if std::env::var_os(GRAPH_VACUUM_CHILD).is_some() {
        let data_dir = std::env::var_os(GRAPH_VACUUM_CHILD).unwrap();
        let engine = open(Path::new(&data_dir));
        engine.exec(None, "CREATE GRAPH memory").unwrap();
        engine
            .exec(None, "GRAPH ADD NODE memory 1 PROPERTIES '{\"v\":1}'")
            .unwrap();
        engine
            .exec(None, "GRAPH ADD NODE memory 2 PROPERTIES '{}'")
            .unwrap();
        engine
            .exec(None, "GRAPH UPDATE NODE memory 1 PROPERTIES '{\"v\":2}'")
            .unwrap();
        let table = graph_table(&engine);
        pg_engine::engine::crash_after_graph_index_cleanup_for_test();
        let _ = engine.vacuum(&table);
        unreachable!("crash hook must terminate the child process");
    }

    let tmp = TempDir::new().unwrap();
    let status = spawn_graph_test(
        "graph_vacuum_crash_after_index_cleanup_converges",
        GRAPH_VACUUM_CHILD,
        tmp.path(),
    );
    assert!(!status.success());
    let _ = std::fs::remove_file(tmp.path().join("lock"));
    let engine = open(tmp.path());
    let table = graph_table(&engine);
    engine.vacuum(&table).unwrap();
    let _ = graph_rows(&engine)
        .into_iter()
        .find(|(_, values)| {
            values[0] == Some(pg_engine::Datum::Int4(GRAPH_RECORD_NODE))
                && values[1] == Some(pg_engine::Datum::Int8(1))
        })
        .expect("updated node must remain visible");
    let node = GraphItem::node(1, "{\"v\":2}").unwrap();
    for kind in [GraphKeyKind::Identity, GraphKeyKind::Out, GraphKeyKind::In] {
        assert_graph_item_has_index_entries(&engine, kind, &node);
    }
    validate_graph_indexes(&engine);
    engine.shutdown();
}

fn graph_fixture(data_dir: &Path) -> GraphMetadata {
    let engine = open(data_dir);
    engine.exec(None, "CREATE GRAPH memory").unwrap();
    let graph_oid = graph_oid(&engine);
    let metadata = GraphStore::open(data_dir, graph_oid)
        .unwrap()
        .unwrap()
        .metadata()
        .clone();
    engine.shutdown();
    metadata
}

fn assert_reopen_corrupted(data_dir: &Path) {
    let result = Engine::open(data_dir, EngineConfig::new(data_dir));
    assert!(matches!(result, Err(EngineError::Corrupted(_))));
}

fn delete_pg_index_entry(engine: &Engine, index_oid: Oid) {
    let columns = PG_INDEX.column_types();
    let mut guard = engine
        .storage()
        .buffer_pool()
        .pin_mut(PG_INDEX.first_page)
        .unwrap();
    let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().unwrap();
    let mut deleted = false;
    for slot in 0..SlottedPage::slot_count(page) as u16 {
        let Some(bytes) = SlottedPage::tuple(page, slot).unwrap() else {
            continue;
        };
        let (_, values) = decode_tuple(bytes, &columns).unwrap();
        if values[0] == Some(pg_engine::Datum::Int8(index_oid.0 as i64)) {
            SlottedPage::delete_tuple(page, slot).unwrap();
            deleted = true;
            break;
        }
    }
    assert!(deleted, "pg_index entry was not found");
    drop(guard);
    engine.checkpoint().unwrap();
}

#[test]
fn graph_missing_side_file_is_corrupted() {
    let tmp = TempDir::new().unwrap();
    let metadata = graph_fixture(tmp.path());
    std::fs::remove_file(GraphStore::metadata_path(tmp.path(), metadata.oid)).unwrap();
    assert_reopen_corrupted(tmp.path());
}

#[test]
fn graph_side_file_first_page_mismatch_is_corrupted() {
    let tmp = TempDir::new().unwrap();
    let mut metadata = graph_fixture(tmp.path());
    metadata.first_page = PageId(metadata.first_page.0 + 1);
    std::fs::write(
        GraphStore::metadata_path(tmp.path(), metadata.oid),
        metadata.encode().unwrap(),
    )
    .unwrap();
    assert_reopen_corrupted(tmp.path());
}

#[test]
fn graph_missing_index_is_corrupted() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine.exec(None, "CREATE GRAPH memory").unwrap();
    delete_pg_index_entry(&engine, engine.indexes()[0].index_oid);
    engine.shutdown();
    assert_reopen_corrupted(tmp.path());
}

#[test]
fn graph_missing_sequence_is_corrupted() {
    let tmp = TempDir::new().unwrap();
    let mut metadata = graph_fixture(tmp.path());
    metadata.edge_sequence_oid = Oid(900_002);
    std::fs::write(
        GraphStore::metadata_path(tmp.path(), metadata.oid),
        metadata.encode().unwrap(),
    )
    .unwrap();
    assert_reopen_corrupted(tmp.path());
}

#[test]
fn malformed_adjacency_key_is_corrupted_not_a_delete_shortcut() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine.exec(None, "CREATE GRAPH memory").unwrap();
    engine
        .exec(None, "GRAPH ADD NODE memory 1 PROPERTIES '{}'")
        .unwrap();
    engine
        .exec(None, "GRAPH ADD NODE memory 2 PROPERTIES '{}'")
        .unwrap();
    engine
        .exec(None, "GRAPH ADD EDGE memory 1 2 PROPERTIES '{}'")
        .unwrap();
    let entry = engine
        .indexes()
        .into_iter()
        .find(|entry| entry.column == "src")
        .unwrap();
    let mut index = pg_am_btree::BTreeIndex::open(
        Arc::clone(engine.storage().buffer_pool()),
        Arc::clone(engine.storage().wal_writer()),
        entry.index_oid,
        entry.meta_page,
        pg_engine::ColumnType::Bytea,
    )
    .unwrap();
    let mut malformed = GraphKeyCodec::out(GRAPH_RECORD_EDGE, 1, i64::MIN).to_vec();
    malformed.push(0);
    index
        .insert(
            &malformed,
            pg_engine::Tid {
                page_id: pg_engine::PageId(999),
                slot_id: 0,
            },
        )
        .unwrap();

    let result = engine.exec(None, "GRAPH DELETE NODE memory 1");
    assert!(
        matches!(result, Err(EngineError::Corrupted(_))),
        "{result:?}"
    );
    assert!(graph_rows(&engine)
        .iter()
        .any(|(_, values)| { values[0] == Some(pg_engine::Datum::Int4(GRAPH_RECORD_EDGE)) }));
    engine.shutdown();
}

#[test]
fn graph_missing_side_file_and_all_indexes_is_corrupted() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine.exec(None, "CREATE GRAPH memory").unwrap();
    let graph_oid = graph_oid(&engine);
    for entry in engine.indexes() {
        delete_pg_index_entry(&engine, entry.index_oid);
    }
    std::fs::remove_file(GraphStore::metadata_path(tmp.path(), graph_oid)).unwrap();
    engine.shutdown();
    assert_reopen_corrupted(tmp.path());
}

#[test]
fn graph_cold_update_keeps_node_visible_to_concurrent_writer() {
    let tmp = TempDir::new().unwrap();
    let engine = Arc::new(open(tmp.path()));
    engine.exec(None, "CREATE GRAPH memory").unwrap();
    for id in 0..1000 {
        let properties = format!("{{\"v\":\"{}\"}}", "x".repeat(1000));
        engine
            .exec(
                None,
                &format!("GRAPH ADD NODE memory {id} PROPERTIES '{properties}'"),
            )
            .unwrap();
    }
    let table = graph_table(&engine);
    let before = engine
        .scan(&table, None)
        .unwrap()
        .into_iter()
        .find(|(_, values)| values[1] == Some(pg_engine::Datum::Int8(0)))
        .unwrap()
        .0;
    let first = engine.begin_txn().unwrap();
    let first_properties = format!("{{\"v\":\"{}\"}}", "z".repeat(1900));
    engine
        .exec(
            Some(&first),
            &format!("GRAPH UPDATE NODE memory 0 PROPERTIES '{first_properties}'"),
        )
        .unwrap();
    let identity = engine
        .indexes()
        .into_iter()
        .find(|entry| entry.column == "id")
        .unwrap();
    let index = pg_am_btree::BTreeIndex::open(
        Arc::clone(engine.storage().buffer_pool()),
        Arc::clone(engine.storage().wal_writer()),
        identity.index_oid,
        identity.meta_page,
        pg_engine::ColumnType::Bytea,
    )
    .unwrap();
    let index_tids = index
        .lookup_all(&GraphKeyCodec::identity(GRAPH_RECORD_NODE, 0))
        .unwrap();
    assert!(
        index_tids.iter().any(|tid| tid.page_id != before.page_id),
        "first update did not exercise the non-HOT path: {index_tids:?}"
    );

    let second_properties = "{}".to_string();
    let expected_properties = second_properties.clone();
    let barrier = Arc::new(Barrier::new(2));
    let second_engine = Arc::clone(&engine);
    let second_barrier = Arc::clone(&barrier);
    let second_thread = std::thread::spawn(move || {
        let second = second_engine.begin_txn().unwrap();
        second_barrier.wait();
        let result = second_engine.exec(
            Some(&second),
            &format!("GRAPH UPDATE NODE memory 0 PROPERTIES '{second_properties}'"),
        );
        (second, result)
    });
    barrier.wait();
    std::thread::sleep(std::time::Duration::from_millis(50));
    first.commit().unwrap();
    let (second, result) = second_thread.join().unwrap();
    match result {
        Ok(_) => second.commit().unwrap(),
        Err(EngineError::Heap(pg_engine::HeapError::TupleConcurrentlyUpdated(_))) => {
            second.abort().unwrap();
        }
        other => panic!("unexpected concurrent update result: {other:?}"),
    }
    assert!(graph_rows(&engine).iter().any(|(_, values)| {
        values[4] == Some(pg_engine::Datum::Jsonb(first_properties.clone()))
            || values[4] == Some(pg_engine::Datum::Jsonb(expected_properties.clone()))
    }));
    validate_graph_indexes(&engine);
    engine.shutdown();
}

#[test]
fn graph_uncommitted_delete_keeps_edge_visible_to_concurrent_delete() {
    let tmp = TempDir::new().unwrap();
    let engine = Arc::new(open(tmp.path()));
    engine.exec(None, "CREATE GRAPH memory").unwrap();
    engine
        .exec(None, "GRAPH ADD NODE memory 1 PROPERTIES '{}'")
        .unwrap();
    engine
        .exec(None, "GRAPH ADD NODE memory 2 PROPERTIES '{}'")
        .unwrap();
    engine
        .exec(None, "GRAPH ADD EDGE memory 1 2 PROPERTIES '{}'")
        .unwrap();
    let first = engine.begin_txn().unwrap();
    engine
        .exec(Some(&first), "GRAPH DELETE EDGE memory 1")
        .unwrap();

    let barrier = Arc::new(Barrier::new(2));
    let second_engine = Arc::clone(&engine);
    let second_barrier = Arc::clone(&barrier);
    let second_thread = std::thread::spawn(move || {
        let second = second_engine.begin_txn().unwrap();
        second_barrier.wait();
        let result = second_engine.exec(Some(&second), "GRAPH DELETE EDGE memory 1");
        (second, result)
    });
    barrier.wait();
    std::thread::sleep(std::time::Duration::from_millis(50));
    first.commit().unwrap();
    let (second, result) = second_thread.join().unwrap();
    match result {
        Ok(_) => second.commit().unwrap(),
        Err(EngineError::Heap(pg_engine::HeapError::TupleConcurrentlyUpdated(_))) => {
            second.abort().unwrap();
        }
        other => panic!("concurrent delete must not report a missing edge: {other:?}"),
    }
    assert!(graph_rows(&engine)
        .iter()
        .all(|(_, values)| { values[0] != Some(pg_engine::Datum::Int4(GRAPH_RECORD_EDGE)) }));
    validate_graph_indexes(&engine);
    engine.shutdown();
}

#[test]
fn graph_concurrent_create_graph_has_one_winner() {
    let tmp = TempDir::new().unwrap();
    let engine = Arc::new(open(tmp.path()));
    let barrier = Arc::new(Barrier::new(8));
    let mut creators = Vec::new();
    for _ in 0..8 {
        let engine = Arc::clone(&engine);
        let barrier = Arc::clone(&barrier);
        creators.push(std::thread::spawn(move || {
            barrier.wait();
            engine.exec(None, "CREATE GRAPH memory")
        }));
    }
    let successes = creators
        .into_iter()
        .filter_map(|creator| creator.join().unwrap().ok())
        .collect::<Vec<_>>();
    assert_eq!(successes.len(), 1);
    assert_eq!(engine.indexes().len(), 3);
    assert!(graph_rows(&engine).is_empty());
    validate_graph_indexes(&engine);
    engine.shutdown();
}

#[test]
fn graph_parameterized_dml_and_rollback() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine.exec(None, "CREATE GRAPH memory").unwrap();
    assert_eq!(
        affected(
            engine
                .exec_with_params(
                    None,
                    "GRAPH ADD NODE memory $1 PROPERTIES $2",
                    &[
                        Some(pg_engine::Datum::Int8(1)),
                        Some(pg_engine::Datum::Jsonb("{\"kind\":\"agent\"}".to_string())),
                    ],
                )
                .unwrap()
        ),
        1
    );
    engine
        .exec(None, "GRAPH ADD NODE memory 2 PROPERTIES '{}'")
        .unwrap();
    assert_eq!(
        affected(
            engine
                .exec_with_params(
                    None,
                    "GRAPH ADD EDGE memory $1 $2 PROPERTIES $3",
                    &[
                        Some(pg_engine::Datum::Int8(1)),
                        Some(pg_engine::Datum::Int8(2)),
                        Some(pg_engine::Datum::Jsonb("{}".to_string())),
                    ],
                )
                .unwrap()
        ),
        1
    );

    let txn = engine.begin_txn().unwrap();
    engine
        .exec_with_params(
            Some(&txn),
            "GRAPH ADD NODE memory $1 PROPERTIES $2",
            &[
                Some(pg_engine::Datum::Int8(3)),
                Some(pg_engine::Datum::Jsonb("{}".to_string())),
            ],
        )
        .unwrap();
    engine
        .exec_with_params(
            Some(&txn),
            "GRAPH ADD EDGE memory $1 $2 PROPERTIES $3",
            &[
                Some(pg_engine::Datum::Int8(2)),
                Some(pg_engine::Datum::Int8(3)),
                Some(pg_engine::Datum::Jsonb("{}".to_string())),
            ],
        )
        .unwrap();
    txn.abort().unwrap();
    assert_identity_absent(&engine, GRAPH_RECORD_NODE, 3);
    assert_identity_absent(&engine, GRAPH_RECORD_EDGE, 2);
    assert_eq!(graph_rows(&engine).len(), 3);
    validate_graph_indexes(&engine);
    engine.shutdown();
}

#[test]
fn graph_concurrent_integrity_failures_leave_no_dangling_items() {
    let tmp = TempDir::new().unwrap();
    let engine = Arc::new(open(tmp.path()));
    engine.exec(None, "CREATE GRAPH memory").unwrap();
    let barrier = Arc::new(Barrier::new(8));
    let mut writers = Vec::new();
    for _ in 0..8 {
        let engine = Arc::clone(&engine);
        let barrier = Arc::clone(&barrier);
        writers.push(std::thread::spawn(move || {
            barrier.wait();
            engine.exec(None, "GRAPH ADD NODE memory 1 PROPERTIES '{}'")
        }));
    }
    let results = writers
        .into_iter()
        .filter_map(|writer| writer.join().unwrap().ok())
        .collect::<Vec<_>>();
    assert_eq!(results.len(), 1);
    assert_eq!(graph_rows(&engine).len(), 1);

    engine
        .exec(None, "GRAPH ADD NODE memory 2 PROPERTIES '{}'")
        .unwrap();
    for _ in 0..12 {
        let barrier = Arc::new(Barrier::new(2));
        let deleter_engine = Arc::clone(&engine);
        let deleter_barrier = Arc::clone(&barrier);
        let deleter = std::thread::spawn(move || {
            deleter_barrier.wait();
            deleter_engine.exec(None, "GRAPH DELETE NODE memory 1")
        });
        let adder_engine = Arc::clone(&engine);
        let adder_barrier = Arc::clone(&barrier);
        let adder = std::thread::spawn(move || {
            adder_barrier.wait();
            adder_engine.exec(None, "GRAPH ADD EDGE memory 1 2 PROPERTIES '{}'")
        });
        let delete_result = deleter.join().unwrap();
        let add_result = adder.join().unwrap();
        if delete_result.is_err() {
            engine
                .exec(None, "GRAPH ADD NODE memory 1 PROPERTIES '{}'")
                .unwrap();
        }
        if add_result.is_err() {
            // The delete won; the next round recreates the node below.
        }
        if graph_rows(&engine).iter().any(|(_, values)| {
            values[0] == Some(pg_engine::Datum::Int4(GRAPH_RECORD_NODE))
                && values[1] == Some(pg_engine::Datum::Int8(1))
        }) {
            engine.exec(None, "GRAPH DELETE NODE memory 1").unwrap();
        }
        engine
            .exec(None, "GRAPH ADD NODE memory 1 PROPERTIES '{}'")
            .unwrap();
    }
    engine.exec(None, "GRAPH DELETE NODE memory 1").unwrap();
    assert!(graph_rows(&engine)
        .iter()
        .all(|(_, values)| { values[0] != Some(pg_engine::Datum::Int4(GRAPH_RECORD_EDGE)) }));
    validate_graph_indexes(&engine);
    engine.shutdown();
}
