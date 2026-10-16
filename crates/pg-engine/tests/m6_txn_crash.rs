//! M6 Stage C slice C-7: crash-semantics pins for the transactional write
//! path (tech-selection §6.2). kill = mem::forget (no checkpoint, no clean
//! shutdown), then reopen with redo and audit. The core pins:
//!
//! - **Zero graph undo on crash**: no HNSW undo records exist — recovery
//!   replays the graph records AS WRITTEN; visibility is heap-only.
//! - **P1 regression**: an in-flight DELETE whose pages got checkpointed
//!   (WAL-before-data) and then SIGKILLed must recover with the row
//!   visible, the node LIVE, and the map entry present.
//! - **Commit-covers flush**: an insert committed then killed (no
//!   checkpoint) must recover fully — the commit record's flush covers the
//!   graph records (WAL prefix property).

use pg_am_heap::ColumnType;
use pg_am_hnsw::graph::NeighborSelection;
use pg_am_hnsw::{ExpectedParams, HnswParams, Metric};
use pg_engine::{ColumnDef, Engine, EngineConfig};
use pg_storage::types::PageId;
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

fn expected() -> ExpectedParams {
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

fn insert(engine: &Engine, id: i32, v: Vec<f32>) {
    engine
        .exec(
            None,
            &format!(
                "INSERT INTO t VALUES ({id}, [{}, {}, {}])",
                v[0], v[1], v[2]
            ),
        )
        .unwrap();
}

fn kill(engine: Engine) {
    std::mem::forget(engine);
}

fn reopen(dir: &std::path::Path) -> Engine {
    let _ = std::fs::remove_file(dir.join("lock"));
    Engine::open(dir, EngineConfig::new(dir)).unwrap()
}

fn audit(engine: &Engine, oid: pg_storage::types::Oid) -> pg_am_hnsw::AuditReport {
    let meta: PageId = engine.hnsw_index_first_page(oid).unwrap().unwrap();
    let outcome = engine.open_hnsw_index(meta, &expected()).unwrap();
    outcome.index.audit(engine.storage().buffer_pool()).unwrap()
}

/// P1 regression (v1.4): in-flight delete × checkpoint × SIGKILL.
/// The delete's heap xmax is uncommitted; the checkpoint flushes dirty
/// pages (WAL-before-data may push the uncommitted tombstone... but the
/// GRAPH tombstone is deferred to post-commit, so it never exists in WAL).
/// Recovery must show: row VISIBLE (aborted), node LIVE, map entry PRESENT.
#[test]
fn inflight_delete_checkpoint_crash_recovers_live() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path().to_path_buf();
    let oid = {
        let (engine, oid) = setup(&dir);
        insert(&engine, 1, vec![1.0, 2.0, 3.0]);
        insert(&engine, 2, vec![4.0, 5.0, 6.0]);

        // In-flight delete (never committed) + checkpoint, then kill.
        let tid = engine.scan("t", None).unwrap()[0].0;
        let txn = engine.begin_txn().unwrap();
        engine
            .exec(Some(&txn), "DELETE FROM t WHERE id = 1")
            .unwrap();
        engine.checkpoint().unwrap();
        let _ = tid;
        std::mem::forget(txn); // never committed, never aborted
        kill(engine);
        oid
    };
    let engine = reopen(&dir);
    // The row is visible again (the delete aborted).
    assert_eq!(engine.scan("t", None).unwrap().len(), 2);
    // The graph: both nodes LIVE, zero tombstones.
    let report = audit(&engine, oid);
    assert_eq!(report.node_count, 2);
    assert_eq!(report.live_count, 2);
    assert_eq!(report.tombstoned_count, 0, "no graph tombstone may exist");
    // The map still resolves both nodes.
    assert!(engine.hnsw_map_get(oid, 0).unwrap().is_some());
    assert!(engine.hnsw_map_get(oid, 1).unwrap().is_some());
    engine.shutdown();
}

/// Commit-covers flush: an insert committed then killed (no checkpoint)
/// must recover fully — the commit record's flush covers the graph records
/// (WAL prefix property).
#[test]
fn committed_insert_survives_crash_via_commit_flush() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path().to_path_buf();
    let oid = {
        let (engine, oid) = setup(&dir);
        let txn = engine.begin_txn().unwrap();
        engine
            .exec(Some(&txn), "INSERT INTO t VALUES (1, [1.0, 2.0, 3.0])")
            .unwrap();
        txn.commit().unwrap();
        kill(engine); // no checkpoint
        oid
    };
    let engine = reopen(&dir);
    assert_eq!(engine.scan("t", None).unwrap().len(), 1);
    let report = audit(&engine, oid);
    assert_eq!(report.node_count, 1);
    assert_eq!(report.live_count, 1);
    assert!(engine.hnsw_map_get(oid, 0).unwrap().is_some());
    engine.shutdown();
}

/// An in-flight (never-committed) insert killed mid-transaction: the row
/// is invisible after recovery; the graph node is a residue (LIVE or
/// INITIALIZING — the WAL records may or may not have been flushed by the
/// group-commit worker) that the audit tolerates and the row filter hides.
/// The map entry is reversed by the btree undo pair at abort... except the
/// crash never ran the abort — the orphan map entry is filtered by the
/// row-level MVCC check (Stage D's pipeline) and counted in the leak
/// budget (vacuum reclaims).
#[test]
fn inflight_insert_crash_is_invisible_and_audited() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path().to_path_buf();
    let oid = {
        let (engine, oid) = setup(&dir);
        let txn = engine.begin_txn().unwrap();
        engine
            .exec(Some(&txn), "INSERT INTO t VALUES (1, [1.0, 2.0, 3.0])")
            .unwrap();
        std::mem::forget(txn);
        kill(engine);
        oid
    };
    let engine = reopen(&dir);
    // The row is NOT visible (the transaction never committed).
    assert_eq!(engine.scan("t", None).unwrap().len(), 0);
    // The graph may carry the residue node (legal: replayed as written).
    let report = audit(&engine, oid);
    assert!(
        report.node_count <= 1,
        "at most the residue node (in-flight insert's records may or may not be durable)"
    );
    engine.shutdown();
}
