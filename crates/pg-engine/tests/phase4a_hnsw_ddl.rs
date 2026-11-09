//! Phase 4a Stage B: SQL HNSW DDL and its M6 catalog/registry contract.

use pg_am_heap::{ColumnType, Datum};
use pg_engine::{Engine, EngineConfig, EngineError, QueryResult};
use pg_txn::Snapshot;
use tempfile::TempDir;

fn open(dir: &std::path::Path) -> Engine {
    Engine::open(dir, EngineConfig::new(dir)).unwrap()
}

fn create_vector_table(engine: &Engine, table: &str) {
    engine
        .exec(
            None,
            &format!("CREATE TABLE {table} (id INT4, v VECTOR(2))"),
        )
        .unwrap();
}

#[test]
fn sql_hnsw_ddl_creates_each_metric_and_a_live_sidecar_map() {
    for (opclass, expected_distances) in [
        ("vector_l2_ops", [0.0, 2.0]),
        ("vector_cosine_ops", [0.0, 1.0]),
        ("vector_ip_ops", [-1.0, 0.0]),
    ] {
        let tmp = TempDir::new().unwrap();
        let engine = open(tmp.path());
        create_vector_table(&engine, "t");
        engine
            .exec(
                None,
                &format!("CREATE INDEX ann_idx ON t USING hnsw (v {opclass})"),
            )
            .unwrap();
        engine
            .exec(None, "INSERT INTO t VALUES (1, [1,0]), (2, [0,1])")
            .unwrap();

        let indexes = engine.indexes();
        assert_eq!(indexes.len(), 1);
        assert!(matches!(indexes[0].key_type, ColumnType::Vector(2)));
        assert!(indexes[0].map_page.is_some());
        let rows = engine.scan("t", None).unwrap();
        assert_eq!(
            engine.hnsw_map_get(indexes[0].index_oid, 0).unwrap(),
            Some(rows[0].0)
        );

        // The graph metric chosen by SQL is observable through the unchanged
        // M6 API: squared L2, cosine distance, and negative inner product.
        let hits = engine
            .hnsw_search_visible("t", &[1.0, 0.0], 2, Some(64), &Snapshot::everything())
            .unwrap();
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].1, expected_distances[0], "{opclass}");
        assert_eq!(hits[1].1, expected_distances[1], "{opclass}");
        engine.shutdown();
    }
}

#[test]
fn sql_hnsw_ddl_rejects_duplicate_names_across_tables_and_reopen() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    create_vector_table(&engine, "a");
    create_vector_table(&engine, "b");
    engine
        .exec(
            None,
            "CREATE INDEX shared_name ON a USING hnsw (v vector_l2_ops)",
        )
        .unwrap();
    let err = engine
        .exec(
            None,
            "CREATE INDEX shared_name ON b USING hnsw (v vector_l2_ops)",
        )
        .unwrap_err();
    assert!(matches!(err, EngineError::IndexExists(name) if name == "shared_name"));
    engine.shutdown();

    let engine = open(tmp.path());
    let err = engine
        .exec(
            None,
            "CREATE INDEX shared_name ON b USING hnsw (v vector_l2_ops)",
        )
        .unwrap_err();
    assert!(matches!(err, EngineError::IndexExists(name) if name == "shared_name"));
    engine.shutdown();
}

#[test]
fn sql_hnsw_ddl_preserves_m6_creation_guards() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    create_vector_table(&engine, "t");
    engine
        .exec(
            None,
            "CREATE INDEX first_idx ON t USING hnsw (v vector_l2_ops)",
        )
        .unwrap();
    let err = engine
        .exec(
            None,
            "CREATE INDEX second_idx ON t USING hnsw (v vector_cosine_ops)",
        )
        .unwrap_err()
        .to_string();
    assert!(err.contains("already exists"), "{err}");

    create_vector_table(&engine, "nonempty");
    engine
        .exec(None, "INSERT INTO nonempty VALUES (1, [1,2])")
        .unwrap();
    let err = engine
        .exec(
            None,
            "CREATE INDEX nonempty_idx ON nonempty USING hnsw (v vector_l2_ops)",
        )
        .unwrap_err()
        .to_string();
    assert!(err.contains("is not empty"), "{err}");

    engine.exec(None, "CREATE TABLE scalars (v INT4)").unwrap();
    let err = engine
        .exec(
            None,
            "CREATE INDEX scalar_idx ON scalars USING hnsw (v vector_l2_ops)",
        )
        .unwrap_err()
        .to_string();
    assert!(err.contains("requires a VECTOR column"), "{err}");

    for sql in [
        "CREATE INDEX missing_opclass ON t USING hnsw (v)",
        "CREATE INDEX unknown_opclass ON t USING hnsw (v vector_unknown_ops)",
    ] {
        assert!(engine.exec(None, sql).is_err(), "must reject {sql}");
    }
    assert_eq!(engine.indexes().len(), 1);
    engine.shutdown();
}

#[test]
fn sql_hnsw_ddl_allows_the_automatic_name() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    create_vector_table(&engine, "t");
    let result = engine
        .exec(None, "CREATE INDEX ON t USING hnsw (v vector_l2_ops)")
        .unwrap();
    assert!(matches!(result, QueryResult::Ok));
    assert_eq!(engine.indexes().len(), 1);
    engine.shutdown();
}

#[test]
fn sql_hnsw_rows_remain_visible_to_the_existing_engine_scan() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    create_vector_table(&engine, "t");
    engine
        .exec(
            None,
            "CREATE INDEX ann_idx ON t USING hnsw (v vector_ip_ops)",
        )
        .unwrap();
    engine
        .exec(None, "INSERT INTO t VALUES (7, [3,4])")
        .unwrap();
    let rows = engine.scan("t", None).unwrap();
    assert_eq!(
        rows[0].1,
        vec![Some(Datum::Int4(7)), Some(Datum::Vector(vec![3.0, 4.0]))]
    );
    engine.shutdown();
}

#[test]
fn relation_names_are_unique_across_tables_indexes_and_system_relations() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    create_vector_table(&engine, "t");
    let err = engine
        .exec(None, "CREATE INDEX t ON t USING hnsw (v vector_l2_ops)")
        .unwrap_err();
    assert!(matches!(err, EngineError::IndexExists(name) if name == "t"));
    assert!(engine.indexes().is_empty());

    engine
        .exec(None, "CREATE INDEX x ON t USING hnsw (v vector_l2_ops)")
        .unwrap();
    let err = engine.exec(None, "CREATE TABLE x (id INT4)").unwrap_err();
    assert!(matches!(err, EngineError::TableExists(name) if name == "x"));

    let err = engine
        .exec(None, "CREATE TABLE pg_class (id INT4)")
        .unwrap_err();
    assert!(matches!(err, EngineError::TableExists(name) if name == "pg_class"));

    // Drop markers are not live relations and must not block reuse.
    engine.exec(None, "CREATE TABLE d (id INT4)").unwrap();
    engine.drop_table("d").unwrap();
    engine.exec(None, "CREATE TABLE d (id INT4)").unwrap();
    engine.shutdown();
}

#[test]
fn oversized_hnsw_name_fails_before_allocating_any_pages() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    create_vector_table(&engine, "t");
    let before = engine.storage().page_allocator().lock().next_page_id();
    let name = "x".repeat(9000);
    let err = engine
        .exec(
            None,
            &format!("CREATE INDEX {name} ON t USING hnsw (v vector_l2_ops)"),
        )
        .unwrap_err();
    assert!(matches!(err, EngineError::CatalogFull(_)), "{err}");
    let after = engine.storage().page_allocator().lock().next_page_id();
    assert_eq!(
        before, after,
        "catalog precheck must precede graph allocation"
    );
    assert!(engine.indexes().is_empty());
    engine.shutdown();
}

#[test]
fn dropped_table_names_can_be_reused_after_reopen() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine.exec(None, "CREATE TABLE d (id INT4)").unwrap();
    engine.shutdown();

    // The open-time catalog still contains the pre-DROP version. Name
    // validation must trust the fresh MVCC read for user relations.
    let engine = open(tmp.path());
    engine.drop_table("d").unwrap();
    engine.exec(None, "CREATE TABLE d (id INT4)").unwrap();
    engine.shutdown();

    let engine = open(tmp.path());
    engine.drop_table("d").unwrap();
    engine.shutdown();
    let engine = open(tmp.path());
    engine.exec(None, "CREATE TABLE d (id INT4)").unwrap();
    engine.shutdown();
}

#[test]
fn dropped_table_frees_hnsw_index_name_for_reuse_and_reopen() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    create_vector_table(&engine, "t");
    engine
        .exec(
            None,
            "CREATE INDEX ann_idx ON t USING hnsw (v vector_l2_ops)",
        )
        .unwrap();
    engine.drop_table("t").unwrap();
    engine.shutdown();

    let engine = open(tmp.path());
    create_vector_table(&engine, "t");
    engine
        .exec(
            None,
            "CREATE INDEX ann_idx ON t USING hnsw (v vector_l2_ops)",
        )
        .unwrap();
    engine.drop_table("t").unwrap();
    engine.shutdown();

    let engine = open(tmp.path());
    create_vector_table(&engine, "t");
    engine
        .exec(
            None,
            "CREATE INDEX ann_idx ON t USING hnsw (v vector_l2_ops)",
        )
        .unwrap();
    engine.shutdown();
}
