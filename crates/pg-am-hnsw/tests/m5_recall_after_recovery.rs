//! M5 Stage D slice 4: §11.3 ② — the post-recovery recall gate. The
//! page-resident HNSW index (WAL-backed, `HnswIndex`) must answer siftsmall
//! recall@10 >= 0.98 AFTER a crash + redo recovery — the acceptance that
//! recovery preserves not just structure (slices 1–3) but retrieval quality.
//!
//! Two tests:
//!
//! - `recall_after_recovery_siftsmall_gate` — dataset-gated (the strict
//!   `M4_REQUIRE_DATASET=1` trigger, verbatim from `recall_siftsmall.rs`:
//!   absent dataset + set trigger = panic, absent + unset = skip,
//!   non-Unicode trigger = panic). Runs in the recall-gate CI job
//!   (ci.yml), `--release` tier (10k paged inserts carry a per-insert
//!   success-boundary fsync; debug is needlessly slow).
//! - `recall_after_recovery_synthetic` — always-on companion so this file
//!   is never a pure-skip placeholder on dataset-less matrix jobs (same
//!   discipline as `recall_siftsmall.rs`'s companions).
//!
//! Both share the same three-phase crash shape:
//!
//! 1. build the first half of the vectors, `mem::forget` crash (no
//!    checkpoint — redo replays the whole window);
//! 2. reopen (redo-equipped engine) + open + audit clean, CONTINUE the
//!    insert stream (NodeId continuity pinned — skip-ahead re-syncs the
//!    level stream), finish the build, audit clean, take the pre-crash
//!    query results, `mem::forget` crash again;
//! 3. reopen + open + audit clean (exact counts, zero residues), then
//!    assert every query's `(NodeId, distance)` bits are identical to the
//!    pre-crash answers AND to a never-crashed in-memory twin replayed
//!    over the same vector stream (recovery changes NOTHING,
//!    behaviorally, at scale — the twin pin anchors both recoveries
//!    against a pristine reference, closing the "pre-crash answers were
//!    themselves taken post-recovery" asymmetry), and recall@k meets the
//!    threshold.

mod common;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use pg_am_hnsw::dataset::{read_fvecs, read_ivecs, recall_at_k};
use pg_am_hnsw::distance::l2_squared;
use pg_am_hnsw::graph::{Metric, NeighborSelection};
use pg_am_hnsw::index::{self, HnswIndex};
use pg_am_hnsw::{AuditReport, ExpectedParams, HnswParams, NodeId};
use pg_storage::config::StorageConfig;
use pg_storage::engine::StorageEngine;
use pg_storage::types::PageId;

static COUNTER: AtomicUsize = AtomicUsize::new(0);

/// Manual temp dir (no tempfile dev-dependency — M4's dependency freeze;
/// m5_insert_crash.rs uses the same pattern).
fn fresh_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pg_am_hnsw_m5_recall-{}-{}-{tag}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// One test configuration (everything determinism flows from).
struct Cfg {
    dim: u16,
    params: HnswParams,
    seed: u64,
}

fn expected(cfg: &Cfg) -> ExpectedParams {
    ExpectedParams {
        dim: cfg.dim,
        m: cfg.params.m(),
        m_max0: cfg.params.m_max0(),
        ef_construction: cfg.params.ef_construction(),
        ef_search_default: cfg.params.ef_search_default(),
        metric: Metric::L2,
        selection: NeighborSelection::Heuristic,
    }
}

/// `(NodeId, distance)` as exact bits for comparisons.
fn bits(hits: &[(NodeId, f64)]) -> Vec<(u32, u64)> {
    hits.iter().map(|(id, d)| (id.0, d.to_bits())).collect()
}

/// Create a fresh (engine, index) pair in a new directory.
fn start(dir: &Path, cfg: &Cfg) -> (StorageEngine, HnswIndex) {
    let engine = StorageEngine::open(dir, &StorageConfig::new(dir)).unwrap();
    let index = index::create(
        engine.buffer_pool(),
        engine.wal_writer(),
        cfg.params,
        cfg.dim,
        Metric::L2,
        NeighborSelection::Heuristic,
        cfg.seed,
    )
    .unwrap();
    (engine, index)
}

/// Insert `vectors[range]`, pinning the dense NodeId stream.
fn insert_range(
    engine: &StorageEngine,
    index: &mut HnswIndex,
    vectors: &[Vec<f32>],
    range: std::ops::Range<usize>,
) {
    for i in range {
        let id = index
            .insert(engine.buffer_pool(), engine.wal_writer(), &vectors[i])
            .unwrap();
        assert_eq!(id, NodeId(i as u32), "dense NodeId allocation");
    }
}

/// kill -9: forget the index handle and the engine (no checkpoint, no
/// clean shutdown), keep the directory.
fn crash(engine: StorageEngine, index: HnswIndex) {
    std::mem::forget(index);
    std::mem::forget(engine);
}

/// Recover: redo-equipped engine reopen + the open protocol + the §11.3
/// audit (which must PASS on a completed-build graph and report EXACT
/// counts with zero residues).
fn recover(dir: &Path, cfg: &Cfg, meta: PageId, n: usize) -> (StorageEngine, HnswIndex) {
    let engine = StorageEngine::open_with_redo_handlers(
        dir,
        &StorageConfig::new(dir),
        pg_am_hnsw::redo::hnsw_redo_handlers(),
        vec![],
    )
    .unwrap();
    let outcome = index::open(
        engine.buffer_pool(),
        engine.wal_writer(),
        meta,
        &expected(cfg),
    )
    .unwrap();
    assert!(
        outcome.warnings.is_empty(),
        "test configs match the creation params — no WARN expected: {:?}",
        outcome.warnings
    );
    let index = outcome.index;
    let report: AuditReport = index.audit(engine.buffer_pool()).unwrap();
    assert_eq!(report.node_count, n as u64, "audit node_count");
    assert_eq!(report.live_count, n as u64, "audit live_count");
    assert_eq!(report.initializing_count, 0, "completed build: no ghosts");
    assert_eq!(report.orphan_entry_count, 0, "completed build: no orphans");
    assert_eq!(
        report.hidden_high_level_count, 0,
        "completed build: no hidden high-level nodes"
    );
    assert_eq!(report.tombstoned_count, 0, "M5 has no tombstone producer");
    (engine, index)
}

/// The shared three-phase shape: build halves separated by a crash, a
/// second crash after completion, then the post-recovery query grid must
/// be BITWISE identical to the pre-crash one. Returns the per-query
/// retrieved ids for the recall scoring of the caller.
fn run_three_phases(
    cfg: &Cfg,
    vectors: &[Vec<f32>],
    queries: &[Vec<f32>],
    k: usize,
    ef: usize,
    tag: &str,
) -> Vec<Vec<NodeId>> {
    let n = vectors.len();
    let half = n / 2;
    let dir = fresh_dir(tag);

    // Phase 1: build the first half, crash mid-life (no checkpoint).
    let meta = {
        let (engine, mut index) = start(&dir, cfg);
        insert_range(&engine, &mut index, vectors, 0..half);
        let meta = index.meta_page_id();
        crash(engine, index);
        meta
    };

    // Phase 2: recover, CONTINUE the stream (skip-ahead re-sync), finish,
    // take the pre-crash answers, crash again.
    let pre_crash: Vec<Vec<(u32, u64)>> = {
        let (engine, mut index) = recover(&dir, cfg, meta, half);
        insert_range(&engine, &mut index, vectors, half..n);
        let report = index.audit(engine.buffer_pool()).unwrap();
        assert_eq!(report.node_count, n as u64, "post-continuation audit");
        let answers = queries
            .iter()
            .map(|q| bits(&index.search(engine.buffer_pool(), q, k, Some(ef)).unwrap()))
            .collect();
        crash(engine, index);
        answers
    };

    // Phase 3: recover again (replaying phase 2's own records too), then
    // the query grid must be bitwise identical to the pre-crash answers.
    let (engine, index) = recover(&dir, cfg, meta, n);
    let mut retrieved: Vec<Vec<NodeId>> = Vec::new();
    for (i, q) in queries.iter().enumerate() {
        let hits = index.search(engine.buffer_pool(), q, k, Some(ef)).unwrap();
        assert_eq!(
            bits(&hits),
            pre_crash[i],
            "query {i}: post-recovery answers must be bitwise identical to pre-crash"
        );
        retrieved.push(hits.into_iter().map(|(id, _)| id).collect());
    }

    // Pristine-twin pin: `pre_crash` was measured on a RECOVERED index, so
    // the loop above proves only that the second recovery is idempotent.
    // Anchor BOTH recoveries against a never-crashed reference: the
    // in-memory Hnsw replayed over the same vector stream (cross-form
    // bitwise parity, Stage C slice 3/4) must produce the same answers.
    let mut twin = pg_am_hnsw::Hnsw::new_with_neighbor_selection(
        cfg.dim,
        Metric::L2,
        cfg.params,
        cfg.seed,
        NeighborSelection::Heuristic,
    )
    .unwrap();
    for (i, v) in vectors.iter().enumerate() {
        assert_eq!(
            twin.insert(v).unwrap(),
            NodeId(i as u32),
            "twin: dense stream"
        );
    }
    for (i, q) in queries.iter().enumerate() {
        assert_eq!(
            bits(&twin.search(q, k, Some(ef)).unwrap()),
            pre_crash[i],
            "query {i}: recovered graph diverges from the never-crashed in-memory twin"
        );
    }

    // Clean up the data dir (~40 MB of pages + WAL for the siftsmall
    // shape): shut the engine down so its WAL worker stops and closes the
    // files, THEN remove the directory (the crash phases deliberately leak
    // via mem::forget; the final phase must not).
    drop(index);
    engine.shutdown();
    drop(engine);
    std::fs::remove_dir_all(&dir).unwrap();
    retrieved
}

/// `<workspace>/datasets/siftsmall` (same resolution as
/// `recall_siftsmall.rs`: the manifest dir nests under `crates/`).
fn siftsmall_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("datasets/siftsmall")
}

/// §11.3 ②: post-recovery recall@10 >= 0.98 on siftsmall, with the frozen
/// M4 acceptance parameters (M=16, m_max0=32, efC=200, ef=64, seed=42, L2)
/// so the number is directly comparable to the M4 in-memory gate (0.9990).
#[test]
fn recall_after_recovery_siftsmall_gate() {
    let dir = siftsmall_dir();
    let base_path = dir.join("siftsmall_base.fvecs");
    let query_path = dir.join("siftsmall_query.fvecs");
    let gt_path = dir.join("siftsmall_groundtruth.ivecs");
    let present = base_path.exists() && query_path.exists() && gt_path.exists();
    if !present {
        // Strict trigger (verbatim discipline from recall_siftsmall.rs):
        // a mis-set gate must never silently downgrade to a skip.
        let require = match std::env::var("M4_REQUIRE_DATASET") {
            Ok(v) => v == "1",
            Err(std::env::VarError::NotPresent) => false,
            Err(std::env::VarError::NotUnicode(_)) => {
                panic!("M4_REQUIRE_DATASET is set but not valid Unicode")
            }
        };
        if require {
            panic!(
                "M4_REQUIRE_DATASET=1 but siftsmall is missing at {} — the gate step must fetch it (scripts/fetch_datasets.sh) or the cache key is wrong; this panic is the anti-'green but never ran' property",
                dir.display()
            );
        }
        eprintln!(
            "siftsmall not found at {}; skipping post-recovery recall gate (set M4_REQUIRE_DATASET=1 to make a missing dataset a hard failure)",
            dir.display()
        );
        return;
    }

    let base = read_fvecs(&base_path).unwrap();
    let queries = read_fvecs(&query_path).unwrap();
    let gt = read_ivecs(&gt_path).unwrap();
    assert_eq!(base.len(), 10_000, "siftsmall base must be 10k vectors");
    assert_eq!(queries.len(), 100, "siftsmall query must be 100 vectors");
    assert_eq!(gt.len(), queries.len());

    let cfg = Cfg {
        dim: 128,
        params: HnswParams::new(16, 32, 200, 64).unwrap(),
        seed: 42,
    };
    let retrieved = run_three_phases(&cfg, &base, &queries, 10, 64, "siftsmall");
    let recall = recall_at_k(&gt, &retrieved, 10, base.len() as u32).unwrap();
    eprintln!("post-recovery siftsmall recall@10 @ ef=64: {recall:.4} (threshold 0.98)");
    assert!(
        recall >= 0.98,
        "post-recovery siftsmall recall@10 = {recall:.4} < 0.98 (§11.3 ② hard gate; \
         the in-memory M4 gate scores 0.9990 with these parameters — recovery must not degrade recall)"
    );
}

/// Always-on companion: the same three-phase crash shape on synthetic
/// clustered data, so dataset-less matrix jobs still exercise the full
/// recover → continue → recover → query pipeline. Loose recall threshold
/// (0.90, same discipline as recall_siftsmall's synthetic companion) — the
/// strong pin here is the bitwise pre/post-recovery equality inside
/// `run_three_phases`, not the recall number.
#[test]
fn recall_after_recovery_synthetic() {
    let dim = 8usize;
    let cfg = Cfg {
        dim: dim as u16,
        params: HnswParams::new(8, 16, 50, 32).unwrap(),
        seed: 0x5EED,
    };
    let mut gen = common::MixtureGen::new(dim, 4, 0xC0FFEE);
    let vectors: Vec<Vec<f32>> = (0..800).map(|_| gen.next_vec()).collect();
    let queries: Vec<Vec<f32>> = (0..20).map(|_| gen.next_vec()).collect();

    // Brute-force ground truth straight from the vector list (no in-memory
    // Hnsw involved): the crate's OWN public `l2_squared` (the exact function
    // `Metric::L2` dispatches to) — never a local re-implementation, so the
    // ground truth cannot silently diverge if the crate's accumulation
    // discipline ever changes. Sorted by (distance, NodeId ascending) — the
    // ivecs original-order convention's natural analog.
    let k = 5;
    let gt: Vec<Vec<i32>> = queries
        .iter()
        .map(|q| {
            let mut d: Vec<(u64, u32)> = vectors
                .iter()
                .enumerate()
                .map(|(i, v)| (l2_squared(v, q).unwrap().to_bits(), i as u32))
                .collect();
            d.sort_unstable();
            d[..k].iter().map(|&(_, i)| i as i32).collect()
        })
        .collect();

    let retrieved = run_three_phases(&cfg, &vectors, &queries, k, 32, "synthetic");
    let recall = recall_at_k(&gt, &retrieved, k, vectors.len() as u32).unwrap();
    eprintln!("post-recovery synthetic recall@5 @ ef=32: {recall:.4} (threshold 0.90)");
    assert!(
        recall >= 0.90,
        "post-recovery synthetic recall@5 = {recall:.4} < 0.90 — the recover/continue/query pipeline is broken"
    );
}
