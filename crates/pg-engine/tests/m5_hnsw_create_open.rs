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

/// 2026-09-20, Stage C slice 3 (the registered nano-3 acceptance): the
/// engine-side insert path — create → open → hnsw_insert ×5 → crash WITHOUT
/// a checkpoint (`mem::forget`) → engine reopen (slice-1 redo replays
/// 121–127) → first_page hit → open → hwm = 5 with the reference entry
/// point → continuation inserts allocate NodeIds 5 and 6.
///
/// The expected entry point is not hard-coded: it is derived from the
/// reference level stream (same seed — Algorithm 1 promotes the entry
/// point iff a node's drawn level exceeds every previous one).
#[test]
fn hnsw_insert_survives_a_checkpointless_crash() {
    /// Entry point after `n` inserts under the reference level stream.
    fn reference_entry_point(n: u32) -> u32 {
        let mut rng = pg_am_hnsw::rng::Xoshiro256StarStar::new(SEED);
        let mut ep = 0;
        let mut max_level = 0u8;
        for i in 0..n {
            let l = rng.next_level(16);
            if i == 0 || l > max_level {
                ep = i;
                max_level = l;
            }
        }
        ep
    }

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
        let mut index = engine
            .open_hnsw_index(meta_page, &expected())
            .unwrap()
            .index;
        for i in 0..5u32 {
            let id = engine
                .hnsw_insert(&mut index, &vec![i as f32; DIM as usize])
                .unwrap();
            assert_eq!(id, pg_am_hnsw::NodeId(i));
        }
        assert_eq!(index.hwm(), 5);
        assert_eq!(index.entry_point(), reference_entry_point(5));
        std::mem::forget(engine); // crash: no checkpoint
        (oid, meta_page)
    };

    // Reopen: storage recovery replays the five inserts (slice-1 redo
    // handlers), the catalog row is WAL-durable, and open re-derives the
    // runtime state.
    let engine = Engine::open(dir, EngineConfig::new(dir)).unwrap();
    assert_eq!(engine.hnsw_index_first_page(oid).unwrap(), Some(meta_page));
    let mut index = engine
        .open_hnsw_index(meta_page, &expected())
        .unwrap()
        .index;
    assert_eq!(index.hwm(), 5);
    assert_eq!(index.entry_point(), reference_entry_point(5));

    // Continuation inserts pick up the exact NodeId stream.
    for i in 5..7u32 {
        let id = engine
            .hnsw_insert(&mut index, &vec![i as f32; DIM as usize])
            .unwrap();
        assert_eq!(id, pg_am_hnsw::NodeId(i));
    }
    assert_eq!(index.hwm(), 7);
    assert_eq!(index.entry_point(), reference_entry_point(7));
}

/// 2026-09-21, Stage C slice 4: the engine-side search path — insert a
/// handful of vectors, search each (self is the nearest hit, L2 distance
/// exactly 0.0), then crash WITHOUT a checkpoint (`mem::forget`) and
/// verify the reopened index answers every query bitwise-identically
/// (slice-1 redo rebuilt the pages; the search path itself is read-only).
#[test]
fn hnsw_search_is_bitwise_stable_across_a_crash() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path();

    let vectors: Vec<Vec<f32>> = (0..12u32)
        .map(|i| {
            let mut v = vec![0.0f32; DIM as usize];
            v[(i as usize) % DIM as usize] = 1.0 + i as f32;
            v[(i as usize * 7 + 3) % DIM as usize] = 0.5;
            v
        })
        .collect();
    let bits = |hits: &[(pg_am_hnsw::NodeId, f64)]| -> Vec<(u32, u64)> {
        hits.iter().map(|(id, d)| (id.0, d.to_bits())).collect()
    };

    let (meta_page, before) = {
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
        let mut index = engine
            .open_hnsw_index(meta_page, &expected())
            .unwrap()
            .index;
        for v in &vectors {
            engine.hnsw_insert(&mut index, v).unwrap();
        }
        // Every vector recalls itself as the nearest hit at distance 0.
        let mut before = Vec::new();
        for (i, v) in vectors.iter().enumerate() {
            let hits = engine.hnsw_search(&index, v, 3, None).unwrap();
            assert_eq!(hits[0].0, pg_am_hnsw::NodeId(i as u32));
            assert_eq!(hits[0].1, 0.0, "L2 self-distance");
            before.push(bits(&hits));
        }
        std::mem::forget(engine); // crash: no checkpoint
        (meta_page, before)
    };

    let engine = Engine::open(dir, EngineConfig::new(dir)).unwrap();
    let index = engine
        .open_hnsw_index(meta_page, &expected())
        .unwrap()
        .index;
    assert_eq!(index.hwm(), vectors.len() as u64);
    for (i, v) in vectors.iter().enumerate() {
        let hits = engine.hnsw_search(&index, v, 3, None).unwrap();
        assert_eq!(
            bits(&hits),
            before[i],
            "query {i}: post-crash search must be bitwise identical"
        );
    }
}
