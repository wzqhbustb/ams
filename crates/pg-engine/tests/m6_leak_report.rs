//! M6 Stage E (slice E-1): the leak ledger (tech-selection §8.3/§9 监控) —
//! three-term residue accounting (tombstoned + crash residue + orphan map
//! entries), the configurable ratio threshold, and the rebuild
//! recommendation flag.

use pg_am_heap::ColumnType;
use pg_am_hnsw::graph::NeighborSelection;
use pg_am_hnsw::{HnswParams, Metric};
use pg_engine::{ColumnDef, Engine, EngineConfig, DEFAULT_HNSW_LEAK_THRESHOLD};
use pg_storage::types::Tid;
use tempfile::TempDir;

const DIM: u16 = 3;

fn setup(dir: &std::path::Path) -> (Engine, pg_storage::types::Oid) {
    let engine = Engine::open(dir, EngineConfig::new(dir)).unwrap();
    engine
        .create_table(
            "t",
            &[
                ColumnDef {
                    name: "id".to_string(),
                    col_type: ColumnType::Int4,
                },
                ColumnDef {
                    name: "v".to_string(),
                    col_type: ColumnType::vector(u32::from(DIM)).unwrap(),
                },
            ],
        )
        .unwrap();
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

fn insert(engine: &Engine, id: i32) -> Tid {
    engine
        .insert(
            "t",
            &[
                pg_engine::Value::from(Some(pg_am_heap::Datum::Int4(id))),
                pg_engine::Value::from(Some(pg_am_heap::Datum::Vector(vec![id as f32, 0.0, 0.0]))),
            ],
        )
        .unwrap()
}

/// A clean index: every residue counter is zero, the ratio is zero, no
/// rebuild is recommended.
#[test]
fn clean_index_reports_zero_leak() {
    let tmp = TempDir::new().unwrap();
    let (engine, _oid) = setup(tmp.path());
    for i in 0..12 {
        insert(&engine, i);
    }

    let report = engine.hnsw_leak_report("t", None).unwrap();
    assert_eq!(report.node_count, 12);
    assert_eq!(report.live_count, 12);
    assert_eq!(report.tombstoned_count, 0);
    assert_eq!(report.initializing_count, 0);
    assert_eq!(report.orphan_entry_count, 0);
    assert_eq!(report.orphan_map_entries, 0);
    assert_eq!(report.unreachable_live_count, 0);
    assert_eq!(report.unreachable_initializing_count, 0);
    assert_eq!(report.unreachable_tombstoned_count, 0);
    assert_eq!(report.leak_ratio, 0.0);
    assert_eq!(report.threshold, DEFAULT_HNSW_LEAK_THRESHOLD);
    assert!(!report.rebuild_recommended);
    engine.shutdown();
}

/// Committed deletes count as tombstones; the ratio trips the default 10%
/// threshold, and a caller-supplied threshold overrides it. Eager
/// post-commit cleanup leaves NO orphan map entries (no old snapshot).
#[test]
fn deletes_count_tombstones_and_trip_threshold() {
    let tmp = TempDir::new().unwrap();
    let (engine, _oid) = setup(tmp.path());
    let mut tids = Vec::new();
    for i in 0..12 {
        tids.push(insert(&engine, i));
    }
    for tid in tids.iter().take(3) {
        engine.delete("t", *tid).unwrap();
    }

    let report = engine.hnsw_leak_report("t", None).unwrap();
    assert_eq!(report.tombstoned_count, 3);
    assert_eq!(report.orphan_map_entries, 0);
    assert_eq!(report.leak_ratio, 0.25);
    assert!(
        report.rebuild_recommended,
        "0.25 > the default 0.10 threshold"
    );

    let lenient = engine.hnsw_leak_report("t", Some(0.5)).unwrap();
    assert_eq!(lenient.leak_ratio, 0.25);
    assert!(!lenient.rebuild_recommended, "0.25 < a 0.50 threshold");
    engine.shutdown();
}

/// Watermark-deferred map entries count as orphans (their rows are dead,
/// horizon-free), and the vacuum dead-row channel (Stage D round-8)
/// reclaims them — the ledger watches both sides.
#[test]
fn deferred_entries_count_as_orphans_and_clear_after_vacuum() {
    let tmp = TempDir::new().unwrap();
    let (engine, _oid) = setup(tmp.path());
    let mut tids = Vec::new();
    for i in 0..12 {
        tids.push(insert(&engine, i));
    }

    // Hold an old snapshot so both deletes' map removals defer.
    let reader = engine.begin_txn().unwrap();
    let _old_snap = reader.snapshot();
    for tid in tids.iter().take(2) {
        engine.delete("t", *tid).unwrap();
    }

    let report = engine.hnsw_leak_report("t", None).unwrap();
    assert_eq!(report.tombstoned_count, 2, "post-commit tombstones landed");
    assert_eq!(
        report.orphan_map_entries, 2,
        "deferred entries: rows dead (xmax committed) + entries residual"
    );

    reader.abort().unwrap();
    engine.vacuum("t").unwrap();

    let after = engine.hnsw_leak_report("t", None).unwrap();
    assert_eq!(after.orphan_map_entries, 0, "vacuum reclaimed the residue");
    assert_eq!(
        after.tombstoned_count, 2,
        "tombstones stay (Stage E's rebuild owns them)"
    );
    assert_eq!(after.leak_ratio, 2.0 / 12.0);
    engine.shutdown();
}

/// Threshold validation and the no-index error are loud.
#[test]
fn report_validates_threshold_and_missing_index() {
    let tmp = TempDir::new().unwrap();
    let (engine, _oid) = setup(tmp.path());
    insert(&engine, 1);

    for bad in [1.5, -0.1, f64::NAN, f64::INFINITY] {
        let err = engine.hnsw_leak_report("t", Some(bad)).unwrap_err();
        assert!(
            matches!(err, pg_engine::EngineError::InvalidArgument(_)),
            "threshold {bad} must be rejected: {err}"
        );
    }
    // Boundary values are legal.
    engine.hnsw_leak_report("t", Some(0.0)).unwrap();
    engine.hnsw_leak_report("t", Some(1.0)).unwrap();

    engine
        .create_table(
            "plain",
            &[ColumnDef {
                name: "id".to_string(),
                col_type: ColumnType::Int4,
            }],
        )
        .unwrap();
    let err = engine.hnsw_leak_report("plain", None).unwrap_err();
    assert!(
        matches!(err, pg_engine::EngineError::IndexNotFound(_)),
        "a table without an HNSW index: {err}"
    );
    engine.shutdown();
}
