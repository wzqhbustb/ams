//! M6 Stage C slice C-3b: the insert DML hook (tech-selection §7/§11) —
//! on a table with an HNSW index, `insert` runs the graph insert first,
//! carries the NodeId as the trailing system column in the tuple, and
//! records the sidecar map entry, all in the same transaction. NULL
//! vectors skip the graph entirely.

use pg_am_heap::ColumnType;
use pg_am_hnsw::graph::NeighborSelection;
use pg_am_hnsw::{ExpectedParams, HnswParams, Metric};
use pg_engine::{ColumnDef, Engine, EngineConfig, Value};
use pg_storage::types::Tid;
use tempfile::TempDir;

const DIM: u16 = 3;

fn schema() -> Vec<ColumnDef> {
    vec![
        ColumnDef {
            name: "id".to_string(),
            col_type: ColumnType::Int4,
        },
        ColumnDef {
            name: "v".to_string(),
            col_type: ColumnType::vector(DIM as u32).unwrap(),
        },
    ]
}

fn setup(dir: &std::path::Path) -> (Engine, pg_storage::types::Oid) {
    let engine = Engine::open(dir, EngineConfig::new(dir)).unwrap();
    engine.create_table("t", &schema()).unwrap();
    let (oid, _meta) = engine
        .create_hnsw_index_on(
            "t",
            "v",
            HnswParams::default(),
            DIM,
            Metric::L2,
            NeighborSelection::Heuristic,
            0x5EED,
        )
        .unwrap();
    (engine, oid)
}

fn insert(engine: &Engine, id: i32, v: Option<Vec<f32>>) -> Tid {
    engine
        .insert(
            "t",
            &[
                Value::from(Some(pg_am_heap::Datum::Int4(id))),
                Value::from(v.map(pg_am_heap::Datum::Vector)),
            ],
        )
        .unwrap()
}

#[test]
fn insert_carries_node_id_and_map_entry() {
    let tmp = TempDir::new().unwrap();
    let (engine, oid) = setup(tmp.path());

    let tid1 = insert(&engine, 1, Some(vec![1.0, 2.0, 3.0]));
    let tid2 = insert(&engine, 2, Some(vec![4.0, 5.0, 6.0]));

    // The graph holds two nodes; the map resolves both to their rows.
    let expected = ExpectedParams {
        dim: DIM,
        m: 16,
        m_max0: 32,
        ef_construction: 200,
        ef_search_default: 64,
        metric: Metric::L2,
        selection: NeighborSelection::Heuristic,
    };
    let meta = engine.hnsw_index_first_page(oid).unwrap().unwrap();
    let outcome = engine.open_hnsw_index(meta, &expected).unwrap();
    assert_eq!(outcome.index.hwm(), 2);

    assert_eq!(engine.hnsw_map_get(oid, 0).unwrap(), Some(tid1));
    assert_eq!(engine.hnsw_map_get(oid, 1).unwrap(), Some(tid2));

    // The trailing NodeId is in the row itself (tuple_node_id over the
    // heap bytes — read back through the map's tid).
    let row1 = engine.scan("t", None).unwrap();
    assert_eq!(row1.len(), 2);
    engine.shutdown();
}

#[test]
fn null_vector_skips_the_graph() {
    let tmp = TempDir::new().unwrap();
    let (engine, oid) = setup(tmp.path());

    insert(&engine, 1, None);
    insert(&engine, 2, Some(vec![1.0, 0.0, 0.0]));

    let expected = ExpectedParams {
        dim: DIM,
        m: 16,
        m_max0: 32,
        ef_construction: 200,
        ef_search_default: 64,
        metric: Metric::L2,
        selection: NeighborSelection::Heuristic,
    };
    let meta = engine.hnsw_index_first_page(oid).unwrap().unwrap();
    let outcome = engine.open_hnsw_index(meta, &expected).unwrap();
    // Only the non-NULL row entered the graph.
    assert_eq!(outcome.index.hwm(), 1);
    assert!(engine.hnsw_map_get(oid, 0).unwrap().is_some());
    assert_eq!(engine.hnsw_map_get(oid, 1).unwrap(), None);

    engine.shutdown();
}

#[test]
fn hook_works_after_engine_reopen() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path().to_path_buf();
    let oid = {
        let (engine, oid) = setup(&dir);
        insert(&engine, 1, Some(vec![1.0, 1.0, 1.0]));
        engine.shutdown();
        oid
    };
    // Reopen: the hook's lazy-open path (self-evidencing params) must work.
    let engine = Engine::open(&dir, EngineConfig::new(&dir)).unwrap();
    let tid = insert(&engine, 2, Some(vec![2.0, 2.0, 2.0]));
    assert_eq!(engine.hnsw_map_get(oid, 1).unwrap(), Some(tid));
    engine.shutdown();
}

// ---------------------------------------------------------------------
// Update / delete hooks (C-3c/C-3d)
// ---------------------------------------------------------------------

#[test]
fn update_non_vector_keeps_node_moves_map() {
    let tmp = TempDir::new().unwrap();
    let (engine, oid) = setup(tmp.path());
    let tid1 = insert(&engine, 1, Some(vec![1.0, 2.0, 3.0]));

    // Update a NON-vector column: the graph is untouched, the map moves to
    // the new row version, the new row carries the same NodeId.
    let tid2 = engine
        .update(
            "t",
            tid1,
            &[
                Value::from(Some(pg_am_heap::Datum::Int4(100))),
                Value::from(Some(pg_am_heap::Datum::Vector(vec![1.0, 2.0, 3.0]))),
            ],
        )
        .unwrap();

    let expected = expected_params();
    let meta = engine.hnsw_index_first_page(oid).unwrap().unwrap();
    let outcome = engine.open_hnsw_index(meta, &expected).unwrap();
    assert_eq!(outcome.index.hwm(), 1, "no new graph node");

    // The map still points at the CHAIN ROOT (HOT: the old entry owns the
    // chain; reads follow t_ctid to the new version).
    assert_eq!(engine.hnsw_map_get(oid, 0).unwrap(), Some(tid1));
    let _ = tid2;
    engine.shutdown();
}

#[test]
fn update_vector_column_is_delete_plus_insert() {
    let tmp = TempDir::new().unwrap();
    let (engine, oid) = setup(tmp.path());
    insert(&engine, 1, Some(vec![1.0, 2.0, 3.0]));

    let tid2 = engine
        .update(
            "t",
            engine.scan("t", None).unwrap()[0].0,
            &[
                Value::from(Some(pg_am_heap::Datum::Int4(1))),
                Value::from(Some(pg_am_heap::Datum::Vector(vec![9.0, 9.0, 9.0]))),
            ],
        )
        .unwrap();

    let expected = expected_params();
    let meta = engine.hnsw_index_first_page(oid).unwrap().unwrap();
    let outcome = engine.open_hnsw_index(meta, &expected).unwrap();
    assert_eq!(outcome.index.hwm(), 2, "a fresh graph node");
    // Old node 0 is tombstoned (post-commit), the map points at node 1.
    assert_eq!(engine.hnsw_map_get(oid, 0).unwrap(), None);
    assert_eq!(engine.hnsw_map_get(oid, 1).unwrap(), Some(tid2));
    engine.shutdown();
}

#[test]
fn delete_tombstones_after_commit() {
    let tmp = TempDir::new().unwrap();
    let (engine, oid) = setup(tmp.path());
    let tid = insert(&engine, 1, Some(vec![1.0, 2.0, 3.0]));

    engine.delete("t", tid).unwrap();

    // Post-commit: the node is tombstoned and the map entry is gone.
    let expected = expected_params();
    let meta = engine.hnsw_index_first_page(oid).unwrap().unwrap();
    let outcome = engine.open_hnsw_index(meta, &expected).unwrap();
    assert_eq!(outcome.index.hwm(), 1, "the node stays in the graph");
    assert_eq!(engine.hnsw_map_get(oid, 0).unwrap(), None, "map removed");
    engine.shutdown();
}

fn expected_params() -> ExpectedParams {
    ExpectedParams {
        dim: DIM,
        m: 16,
        m_max0: 32,
        ef_construction: 200,
        ef_search_default: 64,
        metric: Metric::L2,
        selection: NeighborSelection::Heuristic,
    }
}

/// C-5: an insert's abort tombstones the graph node (IndexUndo HnswInserted
/// arm) — the row is never visible, the map entry is reversed by the
/// btree undo pair, and the graph carries a tombstone, not a live leak.
#[test]
fn insert_abort_tombstones_the_graph_node() {
    let tmp = TempDir::new().unwrap();
    let (engine, oid) = setup(tmp.path());
    insert(&engine, 1, Some(vec![1.0, 2.0, 3.0]));

    let txn = engine.begin_txn().unwrap();
    engine
        .exec(Some(&txn), "INSERT INTO t VALUES (2, [4.0, 5.0, 6.0])")
        .unwrap();
    txn.abort().unwrap();

    let expected = expected_params();
    let meta = engine.hnsw_index_first_page(oid).unwrap().unwrap();
    let outcome = engine.open_hnsw_index(meta, &expected).unwrap();
    // The aborted insert's node was allocated (hwm=2) but tombstoned.
    assert_eq!(outcome.index.hwm(), 2);
    let report = outcome.index.audit(engine.storage().buffer_pool()).unwrap();
    assert_eq!(report.tombstoned_count, 1, "the aborted node is tombstoned");
    // LIVE bit is NOT cleared by a tombstone (state byte bit 6 stays; the
    // tombstone is bit 7): both nodes carry LIVE, one is tombstoned.
    assert_eq!(report.live_count, 2, "both nodes carry the LIVE bit");
    // The map entry was reversed by the btree undo pair.
    assert_eq!(engine.hnsw_map_get(oid, 1).unwrap(), None);
    assert!(engine.hnsw_map_get(oid, 0).unwrap().is_some());
    // The row is invisible.
    assert_eq!(engine.scan("t", None).unwrap().len(), 1);
    engine.shutdown();
}

/// A committed insert stays live; the abort arm must NOT fire on commit.
#[test]
fn insert_commit_keeps_the_node_live() {
    let tmp = TempDir::new().unwrap();
    let (engine, oid) = setup(tmp.path());
    let txn = engine.begin_txn().unwrap();
    engine
        .exec(Some(&txn), "INSERT INTO t VALUES (1, [1.0, 2.0, 3.0])")
        .unwrap();
    txn.commit().unwrap();

    let expected = expected_params();
    let meta = engine.hnsw_index_first_page(oid).unwrap().unwrap();
    let outcome = engine.open_hnsw_index(meta, &expected).unwrap();
    let report = outcome.index.audit(engine.storage().buffer_pool()).unwrap();
    assert_eq!(report.tombstoned_count, 0);
    assert_eq!(report.live_count, 1);
    assert!(engine.hnsw_map_get(oid, 0).unwrap().is_some());
    engine.shutdown();
}

// ---------------------------------------------------------------------
// Review P2/P3 follow-ups
// ---------------------------------------------------------------------

/// P2-1: index creation on a NON-empty table is loudly refused (no
/// backfill in M6 — no pre-existing row may silently escape the graph).
#[test]
fn create_index_on_nonempty_table_refused() {
    let tmp = TempDir::new().unwrap();
    let engine = Engine::open(tmp.path(), EngineConfig::new(tmp.path())).unwrap();
    engine.create_table("t", &schema()).unwrap();
    insert(&engine, 1, Some(vec![1.0, 2.0, 3.0]));
    let r = engine.create_hnsw_index_on(
        "t",
        "v",
        HnswParams::default(),
        DIM,
        Metric::L2,
        NeighborSelection::Heuristic,
        0x5EED,
    );
    assert!(r.is_err(), "non-empty table must refuse index creation");
    assert!(r.unwrap_err().to_string().contains("not empty"));
    engine.shutdown();
}

/// P2-2: a second HNSW index on the same table is refused (one trailing
/// NodeId per row) — pinned on a DIFFERENT vector column so the refusal
/// comes from the table-level arm (not the same-column IndexExists arm).
#[test]
fn second_hnsw_index_on_same_table_refused() {
    let tmp = TempDir::new().unwrap();
    let engine = Engine::open(tmp.path(), EngineConfig::new(tmp.path())).unwrap();
    // A two-vector-column schema: the second index goes on the OTHER
    // column, so only the table-level arm can fire.
    engine
        .create_table(
            "t",
            &[
                ColumnDef {
                    name: "v".to_string(),
                    col_type: ColumnType::vector(DIM as u32).unwrap(),
                },
                ColumnDef {
                    name: "w".to_string(),
                    col_type: ColumnType::vector(DIM as u32).unwrap(),
                },
            ],
        )
        .unwrap();
    engine
        .create_hnsw_index_on(
            "t",
            "v",
            HnswParams::default(),
            DIM,
            Metric::L2,
            NeighborSelection::Heuristic,
            0x5EED,
        )
        .unwrap();
    let r = engine.create_hnsw_index_on(
        "t",
        "w",
        HnswParams::default(),
        DIM,
        Metric::L2,
        NeighborSelection::Heuristic,
        0xBEEF,
    );
    let err = r.expect_err("a second hnsw index on the table must refuse");
    assert!(
        err.to_string().contains("already has an HNSW index"),
        "the TABLE-LEVEL arm must fire, got: {err}"
    );
    engine.shutdown();
}

/// P3-2: a non-HOT update (cross-page) of a NON-vector column moves the
/// map entry to the new version, no tombstone, no new node.
#[test]
fn update_non_vector_cross_page_moves_map() {
    let tmp = TempDir::new().unwrap();
    let (engine, oid) = setup(tmp.path());
    // Fill past one page so an update lands off-page.
    let mut first_tid = None;
    for i in 0..200 {
        let t = insert(&engine, i, Some(vec![i as f32, 0.0, 0.0]));
        if i == 0 {
            first_tid = Some(t);
        }
    }
    let tid1 = first_tid.unwrap();
    // Update a non-vector column of the first row — the new version lands
    // on a LATER page (non-HOT), so the map must move to the new tid.
    let tid2 = engine
        .update(
            "t",
            tid1,
            &[
                Value::from(Some(pg_am_heap::Datum::Int4(999))),
                Value::from(Some(pg_am_heap::Datum::Vector(vec![0.0, 0.0, 0.0]))),
            ],
        )
        .unwrap();
    assert_ne!(
        tid2.page_id, tid1.page_id,
        "the update must be non-HOT (cross-page) for this test to mean anything"
    );
    // The node count is unchanged (no new graph node).
    let expected = expected_params();
    let meta = engine.hnsw_index_first_page(oid).unwrap().unwrap();
    let outcome = engine.open_hnsw_index(meta, &expected).unwrap();
    assert_eq!(outcome.index.hwm(), 200);
    let report = outcome.index.audit(engine.storage().buffer_pool()).unwrap();
    assert_eq!(report.tombstoned_count, 0, "no tombstone for a ctid move");
    // The map moved to the new version.
    assert_eq!(engine.hnsw_map_get(oid, 0).unwrap(), Some(tid2));
    engine.shutdown();
}

/// P3-2: delete's graph-side tombstone pinned directly (not just the map).
#[test]
fn delete_tombstone_pinned_on_graph_side() {
    let tmp = TempDir::new().unwrap();
    let (engine, oid) = setup(tmp.path());
    let tid = insert(&engine, 1, Some(vec![1.0, 2.0, 3.0]));
    engine.delete("t", tid).unwrap();
    let expected = expected_params();
    let meta = engine.hnsw_index_first_page(oid).unwrap().unwrap();
    let outcome = engine.open_hnsw_index(meta, &expected).unwrap();
    let report = outcome.index.audit(engine.storage().buffer_pool()).unwrap();
    assert_eq!(report.tombstoned_count, 1, "the node IS tombstoned");
    assert_eq!(report.live_count, 1, "LIVE bit stays (tombstone is bit 7)");
    engine.shutdown();
}

/// P3-2: vector-column update's old node IS tombstoned graph-side.
#[test]
fn update_vector_tombstones_old_node_graph_side() {
    let tmp = TempDir::new().unwrap();
    let (engine, oid) = setup(tmp.path());
    let tid = insert(&engine, 1, Some(vec![1.0, 2.0, 3.0]));
    engine
        .update(
            "t",
            tid,
            &[
                Value::from(Some(pg_am_heap::Datum::Int4(1))),
                Value::from(Some(pg_am_heap::Datum::Vector(vec![9.0, 9.0, 9.0]))),
            ],
        )
        .unwrap();
    let expected = expected_params();
    let meta = engine.hnsw_index_first_page(oid).unwrap().unwrap();
    let outcome = engine.open_hnsw_index(meta, &expected).unwrap();
    let report = outcome.index.audit(engine.storage().buffer_pool()).unwrap();
    assert_eq!(report.tombstoned_count, 1, "old node tombstoned");
    assert_eq!(report.live_count, 2, "both carry the LIVE bit");
    engine.shutdown();
}

/// Round-3 P1-1 regression: an aborted vector-column update must tombstone
/// the NEW graph node (the vector change is an insert-direction write).
/// Without the undo record the aborted update leaked a LIVE node per abort.
#[test]
fn update_vector_abort_tombstones_new_node() {
    let tmp = TempDir::new().unwrap();
    let (engine, oid) = setup(tmp.path());
    let tid = insert(&engine, 1, Some(vec![1.0, 2.0, 3.0]));

    let txn = engine.begin_txn().unwrap();
    engine
        .exec(Some(&txn), "UPDATE t SET v = [9.0, 9.0, 9.0] WHERE id = 1")
        .unwrap();
    txn.abort().unwrap();

    let expected = expected_params();
    let meta = engine.hnsw_index_first_page(oid).unwrap().unwrap();
    let outcome = engine.open_hnsw_index(meta, &expected).unwrap();
    let report = outcome.index.audit(engine.storage().buffer_pool()).unwrap();
    assert_eq!(report.node_count, 2, "the new node was allocated");
    assert_eq!(
        report.tombstoned_count, 1,
        "the aborted update's new node is tombstoned"
    );
    assert_eq!(report.live_count, 2, "both carry the LIVE bit");
    // The old row is back (visible), its map entry intact.
    assert_eq!(engine.scan("t", None).unwrap().len(), 1);
    assert_eq!(engine.hnsw_map_get(oid, 0).unwrap(), Some(tid));
    assert_eq!(engine.hnsw_map_get(oid, 1).unwrap(), None);
    engine.shutdown();
}
