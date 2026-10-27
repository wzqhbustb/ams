//! M6 Stage E (slice E-2): the tombstone half of vacuum's dead-row channel
//! (tech-selection §9 死行驱动补刀) — commit-crash / failure-skipped
//! post-commit cleanup converges at the next vacuum pass, under the Stage
//! D round-8 discrimination: a NodeId reused by a live version (non-HOT
//! non-vector UPDATE) must NEVER be tombstoned under it.

use pg_am_heap::ColumnType;
use pg_am_hnsw::graph::NeighborSelection;
use pg_am_hnsw::{HnswParams, Metric};
use pg_engine::{ColumnDef, Engine, EngineConfig};
use pg_txn::Snapshot;
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
            col_type: ColumnType::vector(u32::from(DIM)).unwrap(),
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

fn reopen(dir: &std::path::Path) -> Engine {
    let _ = std::fs::remove_file(dir.join("lock"));
    Engine::open(dir, EngineConfig::new(dir)).unwrap()
}

/// The crash-loser insert residue (row aborted by recovery undo, graph
/// node LIVE-or-INITIALIZING, map entry possibly dangling) converges at
/// the next vacuum pass: the dead-row channel tombstones the node when it
/// is LIVE (an INITIALIZING residue stays with the §8.3 orphan accounting)
/// and removes the map entry — never a false live node, never a dangling
/// entry.
#[test]
fn vacuum_tombstones_crash_residue_node_and_reclaims_orphan_entry() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path().to_path_buf();
    {
        let (engine, _oid) = setup(&dir);
        let txn = engine.begin_txn().unwrap();
        engine
            .exec(Some(&txn), "INSERT INTO t VALUES (1, [1.0, 2.0, 3.0])")
            .unwrap();
        std::mem::forget(txn); // never committed, never aborted
        std::mem::forget(engine); // kill: no checkpoint, no clean shutdown
    }
    let engine = reopen(&dir);
    // The row is invisible (the transaction never committed).
    assert_eq!(engine.scan("t", None).unwrap().len(), 0);

    engine.vacuum("t").unwrap();

    let report = engine.hnsw_leak_report("t", None).unwrap();
    assert!(
        report.node_count <= 1,
        "at most the residue node (the in-flight insert's records may or may not be durable)"
    );
    assert_eq!(
        report.initializing_count + report.tombstoned_count,
        report.node_count,
        "every residue node is INITIALIZING (orphan accounting) or vacuum-tombstoned — never plain-LIVE"
    );
    assert_eq!(
        report.orphan_map_entries, 0,
        "the dangling map entry (if the loser's put was durable) is reclaimed"
    );
    // And the search never returns the dead row.
    let hits = engine
        .hnsw_search_visible("t", &[1.0, 2.0, 3.0], 1, None, &Snapshot::everything())
        .unwrap();
    assert!(hits.is_empty());
    engine.shutdown();
}

/// The discrimination pin: a non-HOT non-vector UPDATE reuses the same
/// NodeId for the live new version — vacuum must remove the dead
/// version's map entry but NEVER tombstone the node (round-8's
/// registered discipline, now exercised end to end).
#[test]
fn vacuum_never_tombstones_a_node_id_reused_by_a_live_version() {
    let tmp = TempDir::new().unwrap();
    let (engine, oid) = setup(tmp.path());
    // A secondary index on id forces the update non-HOT.
    engine.create_index("t", "id").unwrap();
    engine
        .exec(None, "INSERT INTO t VALUES (1, [1.0, 2.0, 3.0])")
        .unwrap();

    // Old snapshot alive ⟹ the update's old-version map removal defers.
    let reader = engine.begin_txn().unwrap();
    let _old_snap = reader.snapshot();
    engine
        .exec(None, "UPDATE t SET id = 100 WHERE id = 1")
        .unwrap();
    reader.abort().unwrap();

    engine.vacuum("t").unwrap();

    let report = engine.hnsw_leak_report("t", None).unwrap();
    assert_eq!(report.node_count, 1);
    assert_eq!(
        report.tombstoned_count, 0,
        "the NodeId is reused by the LIVE version — vacuum must not tombstone it"
    );
    assert_eq!(report.orphan_map_entries, 0);
    // The live version keeps resolving through the same node.
    let t_new = engine.scan("t", None).unwrap()[0].0;
    assert_eq!(engine.hnsw_map_get(oid, 0).unwrap(), Some(t_new));
    let hits = engine
        .hnsw_search_visible("t", &[1.0, 2.0, 3.0], 1, None, &Snapshot::everything())
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].0, t_new);
    engine.shutdown();
}

/// The ordinary committed delete: post-commit already tombstoned the
/// node, so vacuum's channel is an idempotent no-op on the graph side
/// (and still removes nothing else).
#[test]
fn vacuum_is_idempotent_noop_for_post_commit_tombstones() {
    let tmp = TempDir::new().unwrap();
    let (engine, _oid) = setup(tmp.path());
    engine
        .exec(None, "INSERT INTO t VALUES (1, [1.0, 2.0, 3.0])")
        .unwrap();
    engine
        .exec(None, "INSERT INTO t VALUES (2, [4.0, 5.0, 6.0])")
        .unwrap();
    engine.exec(None, "DELETE FROM t WHERE id = 1").unwrap();

    engine.vacuum("t").unwrap();
    let first = engine.hnsw_leak_report("t", None).unwrap();
    assert_eq!(first.node_count, 2);
    assert_eq!(first.tombstoned_count, 1, "post-commit's tombstone stands");
    assert_eq!(first.orphan_map_entries, 0);

    // A second pass converges identically (idempotent re-entry).
    engine.vacuum("t").unwrap();
    let second = engine.hnsw_leak_report("t", None).unwrap();
    assert_eq!(second, first);
    engine.shutdown();
}

// ---------------------------------------------------------------------
// Slice E-3: hnsw_vacuum — the engine-driven local rebuild (§9 局部重建)
// ---------------------------------------------------------------------

/// The engine driver end to end: the leak ledger recommends, the rebuild
/// redirects in-edges, tombstoned NODES stay mapped (no physical
/// reclaim), search quality is untouched, and a second pass is a no-op.
#[test]
fn hnsw_vacuum_redirects_in_edges_and_reports() {
    let tmp = TempDir::new().unwrap();
    let (engine, _oid) = setup(tmp.path());
    for i in 0..12 {
        engine
            .exec(
                None,
                &format!("INSERT INTO t VALUES ({i}, [{}.0, 0.0, 0.0])", i),
            )
            .unwrap();
    }
    for id in [3, 7, 9] {
        engine
            .exec(None, &format!("DELETE FROM t WHERE id = {id}"))
            .unwrap();
    }

    // The leak ledger recommends (3/12 = 0.25 > 0.10 default).
    let before = engine.hnsw_leak_report("t", None).unwrap();
    assert_eq!(before.tombstoned_count, 3);
    assert!(before.rebuild_recommended);

    let stats = engine.hnsw_vacuum("t").unwrap();
    let r = stats.redirect;
    assert!(r.in_edges_found >= 1, "the tombstoned nodes had in-edges");
    assert_eq!(
        r.in_edges_found,
        r.edges_redirected + r.edges_dropped + r.edges_kept
    );
    assert!(
        r.edges_redirected + r.edges_dropped >= 1,
        "a 12-node graph has live substitutes somewhere"
    );
    // No physical reclaim: the tombstoned nodes stay mapped.
    assert_eq!(stats.leak_after.tombstoned_count, 3);

    // Search after the rebuild: only live rows, and the deleted rows'
    // neighborhoods still resolve (the redirect kept the graph useful).
    let hits = engine
        .hnsw_search_visible("t", &[3.0, 0.0, 0.0], 4, None, &Snapshot::everything())
        .unwrap();
    assert_eq!(hits.len(), 4);
    let live_ids: Vec<i32> = engine
        .scan("t", None)
        .unwrap()
        .into_iter()
        .map(|(_, vals)| match &vals[0] {
            Some(pg_am_heap::Datum::Int4(i)) => *i,
            other => panic!("unexpected id datum: {other:?}"),
        })
        .collect();
    assert_eq!(hits.len().min(live_ids.len()), hits.len());
    let hit_tids: std::collections::HashSet<_> = hits.iter().map(|(t, _)| *t).collect();
    let live_tids: std::collections::HashSet<_> = engine
        .scan("t", None)
        .unwrap()
        .into_iter()
        .map(|(t, _)| t)
        .collect();
    assert!(hit_tids.is_subset(&live_tids));

    // Idempotent re-entry.
    let second = engine.hnsw_vacuum("t").unwrap();
    assert_eq!(second.redirect.edges_redirected, 0);
    assert_eq!(second.redirect.edges_dropped, 0);
    engine.shutdown();
}

/// `hnsw_vacuum` on a table without an HNSW index fails loudly.
#[test]
fn hnsw_vacuum_requires_an_hnsw_index() {
    let tmp = TempDir::new().unwrap();
    let engine = Engine::open(tmp.path(), EngineConfig::new(tmp.path())).unwrap();
    engine
        .create_table(
            "plain",
            &[ColumnDef {
                name: "id".to_string(),
                col_type: ColumnType::Int4,
            }],
        )
        .unwrap();
    let err = engine.hnsw_vacuum("plain").unwrap_err();
    assert!(
        matches!(err, pg_engine::EngineError::IndexNotFound(_)),
        "plain table: {err}"
    );
    engine.shutdown();
}
