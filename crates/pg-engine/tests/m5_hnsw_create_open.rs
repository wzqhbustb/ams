//! M5 Stage B slice 3b: engine-side first_page registration
//! (tech-selection §10.3 step 3) — create_hnsw_index /
//! hnsw_index_first_page / open_hnsw_index, with engine-reopen WAL
//! durability of the pg_rust_relpages row.

use pg_am_hnsw::graph::NeighborSelection;
use pg_am_hnsw::{ExpectedParams, HnswParams, Metric};
use pg_engine::{Engine, EngineConfig, EngineError};
use tempfile::TempDir;

const DIM: u16 = 128;
const SEED: u64 = 0x5EED_5EED;

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

/// create → first_page lookup → engine reopen → open roundtrip.
#[test]
fn create_register_reopen_open_roundtrip() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path();

    let (oid, meta_page) = {
        let engine = Engine::open(dir, EngineConfig::new(dir)).unwrap();
        let (oid, meta_page) = engine
            .create_hnsw_index(
                HnswParams::default(),
                DIM,
                Metric::L2,
                NeighborSelection::Heuristic,
                SEED,
            )
            .unwrap();
        // first_page lookup hits immediately after create.
        assert_eq!(engine.hnsw_index_first_page(oid).unwrap(), Some(meta_page));
        (oid, meta_page)
    };

    // Engine reopen: the pg_rust_relpages row is WAL-durable
    // (Engine::open runs storage recovery with the HNSW redo skeleton
    // already registered, engine.rs:687-693).
    let engine = Engine::open(dir, EngineConfig::new(dir)).unwrap();
    assert_eq!(engine.hnsw_index_first_page(oid).unwrap(), Some(meta_page));
    assert_eq!(
        engine
            .hnsw_index_first_page(pg_engine::Oid(9_999_999))
            .unwrap(),
        None
    );

    // open roundtrip: every parameter survives, hwm = 0, empty-graph
    // entry point.
    let outcome = engine.open_hnsw_index(meta_page, &expected()).unwrap();
    assert!(outcome.warnings.is_empty());
    let p = *outcome.index.params();
    assert_eq!(outcome.index.hwm(), 0);
    assert_eq!(outcome.index.entry_point(), u32::MAX); // NodeId::INVALID
    assert_eq!(outcome.index.max_level(), 0);
    assert_eq!(
        (
            p.dim,
            p.m,
            p.m_max0,
            p.ef_construction,
            p.ef_search_default,
            p.metric,
            p.selection,
            p.rng_seed
        ),
        (
            DIM,
            16,
            32,
            200,
            64,
            Metric::L2,
            NeighborSelection::Heuristic,
            SEED
        )
    );
}

/// open_hnsw_index fails loudly on a graph-destroying mismatch and warns
/// (not fails) on the ef_search_default performance knob.
#[test]
fn open_rejects_mismatch_and_warns_ef() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path();
    let engine = Engine::open(dir, EngineConfig::new(dir)).unwrap();
    let (_oid, meta_page) = engine
        .create_hnsw_index(
            HnswParams::default(),
            DIM,
            Metric::L2,
            NeighborSelection::Heuristic,
            SEED,
        )
        .unwrap();

    // Hard mismatch: wrong dim.
    let bad = ExpectedParams {
        dim: 64,
        ..expected()
    };
    assert!(matches!(
        engine.open_hnsw_index(meta_page, &bad),
        Err(EngineError::Hnsw(_))
    ));

    // WARN-level mismatch: ef_search_default passes through as exactly one
    // warning.
    let warn = ExpectedParams {
        ef_search_default: 128,
        ..expected()
    };
    let outcome = engine.open_hnsw_index(meta_page, &warn).unwrap();
    assert_eq!(outcome.warnings.len(), 1);
    assert!(outcome.warnings[0].contains("ef_search_default"));
}

/// Regression for the cross-restart OID collision (2026-09-17, Stage B
/// review round 3 P1 — reproduced live before the fix): the HNSW minimal
/// registration writes ONLY a `pg_rust_relpages` row, and the catalog's
/// startup next_oid correction used to be blind to relpages OIDs. A drop
/// without a checkpoint (the engine never auto-checkpoints, clean drops
/// included) rolled next_oid back and re-issued the same OID to the next
/// HNSW index — the second index became unreachable by OID, and
/// `hnsw_index_first_page` resolved the colliding OID to the FIRST index
/// (a silent wrong-graph open when the parameters match).
#[test]
fn hnsw_oids_do_not_collide_across_a_checkpointless_restart() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path();

    // Session 1: create index A, drop WITHOUT a checkpoint (plain drop —
    // the rollback window this regression lives in).
    let (oid_a, meta_a) = {
        let engine = Engine::open(dir, EngineConfig::new(dir)).unwrap();
        engine
            .create_hnsw_index(
                HnswParams::default(),
                DIM,
                Metric::L2,
                NeighborSelection::Heuristic,
                SEED,
            )
            .unwrap()
    };

    // Session 2: reopen, create index B. The OID must NOT repeat A's.
    let engine = Engine::open(dir, EngineConfig::new(dir)).unwrap();
    let (oid_b, meta_b) = engine
        .create_hnsw_index(
            HnswParams::default(),
            DIM,
            Metric::L2,
            NeighborSelection::Heuristic,
            SEED,
        )
        .unwrap();
    assert_ne!(
        oid_a, oid_b,
        "OID collision across a checkpointless restart"
    );
    // Each OID resolves to its OWN meta page (pre-fix: oid_b collided with
    // oid_a and resolved to meta_a — index B unreachable by OID).
    assert_eq!(engine.hnsw_index_first_page(oid_a).unwrap(), Some(meta_a));
    assert_eq!(engine.hnsw_index_first_page(oid_b).unwrap(), Some(meta_b));
    assert_ne!(meta_a, meta_b);
}
