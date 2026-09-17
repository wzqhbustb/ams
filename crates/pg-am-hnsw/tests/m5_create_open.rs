//! M5 Stage B slice 3a: create/open protocol integration tests
//! (tech-selection §10.3) — pub-surface only; the open-repair and
//! skip-ahead internals are covered by index.rs's unit tests (they need
//! pub(crate) primitives).

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use pg_am_hnsw::graph::{Metric, NeighborSelection};
use pg_am_hnsw::rng::Xoshiro256StarStar;
use pg_am_hnsw::{index, ExpectedParams, HnswError, HnswParams};
use pg_storage::config::StorageConfig;
use pg_storage::engine::StorageEngine;

static COUNTER: AtomicUsize = AtomicUsize::new(0);

/// Manual temp dir (no tempfile dev-dependency — M4's dependency freeze;
/// page_init.rs uses the same pattern).
fn fresh_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pg_am_hnsw_m5_create_open-{}-{}-{tag}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

const DIM: u16 = 128;
const SEED: u64 = 0xC0FFEE;

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

/// create → open roundtrip: every parameter survives, hwm = 0, entry
/// point is the empty-graph sentinel, the rng sits at the seed's stream
/// start; and the whole thing is WAL-durable across an engine reopen.
#[test]
fn create_then_open_roundtrip() {
    let dir = fresh_dir("roundtrip");
    let config = StorageConfig::new(&dir);
    let (meta_page_id, dir_head) = {
        let engine = StorageEngine::open(&dir, &config).unwrap();
        let index = index::create(
            engine.buffer_pool(),
            engine.wal_writer(),
            HnswParams::default(),
            DIM,
            Metric::L2,
            NeighborSelection::Heuristic,
            SEED,
        )
        .unwrap();
        engine.wal_writer().flush().unwrap();
        (index.meta_page_id(), index.dir_head())
    };
    // First open, same engine lifetime not required — reopen to prove WAL
    // durability.
    let engine = StorageEngine::open_with_redo_handlers(
        &dir,
        &config,
        pg_am_hnsw::redo::hnsw_redo_handlers(),
        vec![],
    )
    .unwrap();
    let mut outcome = index::open(
        engine.buffer_pool(),
        engine.wal_writer(),
        meta_page_id,
        &expected(),
    )
    .unwrap();
    assert!(outcome.warnings.is_empty());
    let idx = &mut outcome.index;
    assert_eq!(idx.hwm(), 0);
    assert_eq!(idx.entry_point(), u32::MAX); // NodeId::INVALID, empty graph
    assert_eq!(idx.max_level(), 0);
    let p = *idx.params();
    assert_eq!(
        (
            p.dim,
            p.m,
            p.m_max0,
            p.ef_construction,
            p.ef_search_default,
            p.metric,
            p.selection,
            p.rng_seed,
            p.dir_head
        ),
        (
            DIM,
            16,
            32,
            200,
            64,
            Metric::L2,
            NeighborSelection::Heuristic,
            SEED,
            dir_head
        )
    );

    // rng sits at the seed's stream start: three draws match a fresh rng.
    let mut reference = Xoshiro256StarStar::new(SEED);
    for _ in 0..3 {
        assert_eq!(idx.next_level(), reference.next_level(16));
    }

    // Reopen the engine again: open still holds (WAL durable).
    drop(engine);
    let engine = StorageEngine::open_with_redo_handlers(
        &dir,
        &config,
        pg_am_hnsw::redo::hnsw_redo_handlers(),
        vec![],
    )
    .unwrap();
    let outcome = index::open(
        engine.buffer_pool(),
        engine.wal_writer(),
        meta_page_id,
        &expected(),
    )
    .unwrap();
    assert_eq!(outcome.index.hwm(), 0);

    let _ = std::fs::remove_dir_all(&dir);
}

/// open rejects each graph-destroying mismatch loudly; ef_search_default
/// mismatch is a warning (v1.4 nano WARN-not-fail).
#[test]
fn open_rejects_parameter_mismatches() {
    let dir = fresh_dir("mismatch");
    let config = StorageConfig::new(&dir);
    let meta_page_id = {
        let engine = StorageEngine::open(&dir, &config).unwrap();
        let index = index::create(
            engine.buffer_pool(),
            engine.wal_writer(),
            HnswParams::default(),
            DIM,
            Metric::L2,
            NeighborSelection::Heuristic,
            SEED,
        )
        .unwrap();
        engine.wal_writer().flush().unwrap();
        index.meta_page_id()
    };
    let engine = StorageEngine::open_with_redo_handlers(
        &dir,
        &config,
        pg_am_hnsw::redo::hnsw_redo_handlers(),
        vec![],
    )
    .unwrap();

    // Six hard mismatches, one each.
    let cases: Vec<ExpectedParams> = vec![
        ExpectedParams {
            dim: 64,
            ..expected()
        },
        ExpectedParams { m: 8, ..expected() },
        ExpectedParams {
            m_max0: 16,
            ..expected()
        },
        ExpectedParams {
            ef_construction: 100,
            ..expected()
        },
        ExpectedParams {
            metric: Metric::Cosine,
            ..expected()
        },
        ExpectedParams {
            selection: NeighborSelection::Simple,
            ..expected()
        },
    ];
    for (i, e) in cases.iter().enumerate() {
        assert!(
            matches!(
                index::open(engine.buffer_pool(), engine.wal_writer(), meta_page_id, e),
                Err(HnswError::InvalidArgument(_))
            ),
            "case {i} must fail loudly"
        );
    }

    // ef_search_default mismatch: Ok with exactly one warning naming it.
    let e = ExpectedParams {
        ef_search_default: 128,
        ..expected()
    };
    let outcome = index::open(engine.buffer_pool(), engine.wal_writer(), meta_page_id, &e).unwrap();
    assert_eq!(outcome.warnings.len(), 1);
    assert!(outcome.warnings[0].contains("ef_search_default"));

    let _ = std::fs::remove_dir_all(&dir);
}
