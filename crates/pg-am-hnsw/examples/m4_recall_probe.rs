//! M4 Stage D recall probe (coding plan Stage D "recall probe" row):
//! build an HNSW graph over an fvecs dataset, then report recall@k against
//! the ivecs ground truth plus build/query/snapshot timings.
//!
//! This is a MEASUREMENT tool, not a test (aligned with the
//! m3_wal_bytes_probe precedent, 2026-09-03 Stage D round 1): no assertions,
//! parameters come from environment variables, and the printed output is the
//! value. Single-line `key=value` records so benchmark runs can be grepped.
//!
//! ```sh
//! M4_DATASET=datasets/siftsmall cargo run -p pg-am-hnsw --release --example m4_recall_probe
//! ```
//!
//! Environment:
//! - `M4_DATASET` (required): directory holding `*_base.fvecs`,
//!   `*_query.fvecs`, `*_groundtruth.ivecs` — the file-name prefix is
//!   auto-discovered from the directory contents (siftsmall/sift/gist share
//!   the layout).
//! - `M4_EF` (default 64), `M4_K` (default 10), `M4_SEED` (default 42),
//!   `M4_M` (default 16), `M4_EF_CONSTRUCTION` (default 200).
//! - `M4_SELECTION` = `heuristic` (default) | `simple` — the §4.3 A/B
//!   switch, routed through `Hnsw::new_with_neighbor_selection`.
//! - `M4_SNAPSHOT` = 0 (default) | 1: after the build, `snapshot::save` to a
//!   temp file, `snapshot::load` it back, and report both timings. The legal
//!   domain is exactly 0|1 — any other value panics loudly (2026-09-07,
//!   Stage D review round 3: `2` silently disabling the measurement would
//!   violate this tool's strict-parameter contract above).
//!
//! Memory note (2026-09-07, Stage D review round 3): at 1M-gist scale the
//! base set materializes to ~3.8 GB in memory — known and intentional for
//! this trusted local harness (the dataset parser's budget-gated variants
//! `read_*_with_budget` exist for untrusted inputs; the local call sites
//! deliberately use the unlimited wrappers, same dual-track discipline as
//! `snapshot::load`/`load_with_budget`).
//!
//! Parameter discipline (2026-09-03, Stage D review round 2 P2): env values
//! parse STRICTLY — a set-but-malformed or out-of-range value panics with
//! the variable name, the offending value, and the target type, never
//! silently falls back to the default (a measurement tool that quietly
//! measured under the wrong parameters would publish wrong numbers). Every
//! narrowing is a checked conversion. And every effective parameter is
//! printed (`params`/`selection` lines), so two A/B runs diff to exactly
//! the selection line — timing lines excluded by nature.
//!
//! Latency protocol: tech-selection §12's frozen口径 "预热后 3 轮取中位" —
//! one untimed warmup round, then three timed rounds; per-round P50/P99 are
//! printed raw (`p50_rounds_ms`/`p99_rounds_ms`) and their medians as
//! `p50_ms`/`p99_ms` (2026-09-03, Stage D review round 2: the pre-review
//! probe ran a single cold round). Build/snapshot timings stay single-shot
//! — the freeze covers query latency only.
//!
//! Metric is PINNED to L2 at the call site (Stage C residual discipline,
//! archived in stage_spec Stage C: the snapshot format carries no metric
//! field, and sift/siftsmall/gist are all L2 datasets — a runtime-supplied
//! metric would silently invalidate every recall number). If a future
//! non-L2 dataset appears, add an explicit per-dataset mapping here, not a
//! free-form env var.

use std::path::{Path, PathBuf};
use std::time::Instant;

use pg_am_hnsw::dataset::{read_fvecs, read_ivecs, recall_at_k};
use pg_am_hnsw::graph::{Hnsw, NeighborSelection};
use pg_am_hnsw::{snapshot, HnswParams, Metric};

/// Strict env parsing: absent -> default; present but malformed, out of
/// the target type's range, or NON-UNICODE -> loud panic naming the
/// variable, the value, and the expected type (see module docs for why no
/// silent fallback). 2026-09-08 Stage E self-review: the pre-fix
/// `Err(_) => default` also swallowed `VarError::NotUnicode` — a set but
/// non-UTF8 value silently fell back, the same hole the strict contract
/// was written to close.
fn env_parse<T>(name: &str, default: T) -> T
where
    T: std::str::FromStr + Copy,
{
    match std::env::var(name) {
        Err(std::env::VarError::NotPresent) => default,
        Err(std::env::VarError::NotUnicode(_)) => {
            panic!(
                "{name} is set but not valid Unicode (expected {})",
                std::any::type_name::<T>()
            )
        }
        Ok(v) => v.parse().unwrap_or_else(|_| {
            panic!(
                "{name}={v:?} is malformed or out of range (expected {})",
                std::any::type_name::<T>()
            )
        }),
    }
}

/// Strict env READ for string-valued variables (2026-09-08, Stage E review
/// round 3 P3 — finding 3): identical semantics to `env_parse` — unset ->
/// None (the caller applies its default), non-Unicode -> loud panic naming
/// the variable. The pre-fix M4_SELECTION match had `Err(_) => heuristic`,
/// which swallowed NotUnicode and silently changed the measured
/// configuration; routing every string variable (M4_DATASET, M4_SELECTION)
/// through here keeps one strict path for all env reads.
fn env_string(name: &str) -> Option<String> {
    match std::env::var(name) {
        Err(std::env::VarError::NotPresent) => None,
        Err(std::env::VarError::NotUnicode(_)) => {
            panic!("{name} is set but not valid Unicode")
        }
        Ok(v) => Some(v),
    }
}

/// Find the single file in `dir` whose name ends with `suffix` (the texmex
/// layout names files `<prefix>_base.fvecs` etc.; the prefix adapts to
/// whatever dataset was unpacked).
fn find_by_suffix(dir: &Path, suffix: &str) -> PathBuf {
    let mut matches: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("cannot read dataset dir {}: {e}", dir.display()))
        .map(|e| e.unwrap().path())
        .filter(|p| p.file_name().unwrap().to_string_lossy().ends_with(suffix))
        .collect();
    matches.sort();
    match matches.len() {
        1 => matches.pop().unwrap(),
        n => panic!(
            "expected exactly 1 *{suffix} file in {}, found {n}",
            dir.display()
        ),
    }
}

/// Nearest-rank percentile over sorted samples (ms). Nearest-rank rather
/// than interpolation: with ~100 queries the interpolated digits would be
/// false precision, and the rank value is an actually-observed latency.
/// 2026-09-03 Stage D review: rank = ceil(n*p/100), so the 0-indexed slot
/// is (n*p - 1)/100 — the pre-fix `n*p/100` read one slot too high and
/// reported the MAX as P99 at n = 100 (sorted[99]).
fn percentile(sorted: &[f64], p: usize) -> f64 {
    let n = sorted.len();
    debug_assert!(n > 0 && p >= 1, "percentile needs samples and p >= 1");
    let idx = (n * p - 1) / 100;
    sorted[idx.min(n - 1)]
}

/// Median of the three timed rounds (tech-selection §12 "3 轮取中位").
fn median3(mut v: [f64; 3]) -> f64 {
    v.sort_by(f64::total_cmp);
    v[1]
}

fn main() {
    let dir = PathBuf::from(
        env_string("M4_DATASET").expect("usage: M4_DATASET=<dir> [M4_EF=64] [M4_K=10] [M4_SEED=42] [M4_M=16] [M4_EF_CONSTRUCTION=200] [M4_SELECTION=heuristic|simple] [M4_SNAPSHOT=1]"),
    );
    let ef: usize = env_parse("M4_EF", 64);
    let k: usize = env_parse("M4_K", 10);
    let seed: u64 = env_parse("M4_SEED", 42);
    let m: u16 = env_parse("M4_M", 16);
    let ef_construction: u32 = env_parse("M4_EF_CONSTRUCTION", 200);
    let selection = match env_string("M4_SELECTION").as_deref() {
        Some("simple") => NeighborSelection::Simple,
        // Unset -> default; non-Unicode already panicked in env_string
        // (round 3 P3 finding 3 — the pre-fix Err(_) arm silently degraded
        // a non-UTF8 value to heuristic).
        None | Some("heuristic") => NeighborSelection::Heuristic,
        Some(other) => panic!("M4_SELECTION must be heuristic|simple, got {other:?}"),
    };
    let do_snapshot = match env_parse::<u8>("M4_SNAPSHOT", 0) {
        0 => false,
        1 => true,
        // 2026-09-07, Stage D review round 3: any other value is a typo for
        // one of the two legal states, and this tool's contract (module
        // docs) is loud failure, never silent fallback — a bare `== 1`
        // would have silently disabled the snapshot measurement for e.g.
        // M4_SNAPSHOT=2.
        other => panic!("M4_SNAPSHOT={other} is invalid (legal domain: 0|1)"),
    };

    // m_max0 = 2*M: the standard HNSW ratio (paper §4.1 / hnswlib default).
    // Checked widening-then-narrowing, not `2 * m`: M4_M > 32767 would
    // otherwise wrap/panic opaquely (2026-09-03 Stage D review round 2 P2).
    let m_max0 = u16::try_from(2 * u32::from(m))
        .unwrap_or_else(|_| panic!("M4_M={m} makes m_max0 = 2*M exceed u16::MAX"));
    // ef_search_default is set to ef so a snapshot-loaded graph (which gets
    // ef_search_default supplied at load) queries identically to the
    // in-memory one. Checked narrowing: M4_EF > u32::MAX must be loud.
    let ef_u32 = u32::try_from(ef).unwrap_or_else(|_| panic!("M4_EF={ef} exceeds u32::MAX"));

    // All effective parameters on ONE line (plus the selection line), so an
    // A/B pair of runs diffs to exactly `selection=` (timing lines aside).
    println!(
        "params dataset={} m={m} m_max0={m_max0} ef_construction={ef_construction} ef={ef} k={k} seed={seed}",
        dir.display()
    );
    println!(
        "selection={}",
        match selection {
            NeighborSelection::Heuristic => "heuristic",
            NeighborSelection::Simple => "simple",
        }
    );

    let base = read_fvecs(find_by_suffix(&dir, "_base.fvecs")).unwrap();
    let queries = read_fvecs(find_by_suffix(&dir, "_query.fvecs")).unwrap();
    let gt = read_ivecs(find_by_suffix(&dir, "_groundtruth.ivecs")).unwrap();
    let dim = base.first().map(|v| v.len()).unwrap_or(0);
    println!(
        "base_count={} query_count={} dim={dim}",
        base.len(),
        queries.len()
    );

    let params = HnswParams::new(m, m_max0, ef_construction, ef_u32).unwrap();
    let mut graph = Hnsw::new_with_neighbor_selection(
        u16::try_from(dim).expect("dim must fit u16"),
        Metric::L2, // pinned — see module docs (snapshot carries no metric)
        params,
        seed,
        selection,
    )
    .unwrap();

    let t0 = Instant::now();
    for v in &base {
        graph.insert(v).unwrap();
    }
    let build_seconds = t0.elapsed().as_secs_f64();
    println!("build_seconds={build_seconds:.3}");

    // Warmup round (untimed). recall is deterministic given (seed, params,
    // selection) — the graph never changes after the build — so it is
    // scored once from this round's retrieved sets, not re-scored per round
    // (2026-09-03, Stage D review round 2). The FULL (NodeId, f64) results
    // are kept: the M4_SNAPSHOT equivalence diff compares distances bitwise,
    // not just ids (2026-09-08, Stage E review round 3 P3 — finding 4).
    let mut retrieved_full: Vec<Vec<(pg_am_hnsw::NodeId, f64)>> = Vec::with_capacity(queries.len());
    for q in &queries {
        retrieved_full.push(graph.search(q, k, Some(ef)).unwrap());
    }
    let retrieved: Vec<Vec<pg_am_hnsw::NodeId>> = retrieved_full
        .iter()
        .map(|r| r.iter().map(|(id, _)| *id).collect())
        .collect();
    let base_count = u32::try_from(base.len()).expect("base_count must fit u32");
    let recall = recall_at_k(&gt, &retrieved, k, base_count).unwrap();
    println!("recall_at_{k}={recall:.4}");
    // 2026-09-03 Stage D round 2 (review P2): drop the base vectors once
    // recall is computed — nothing below reads them, and at 1M-gist scale
    // holding base (3.8 GB) + the graph + a snapshot reload concurrently
    // would overshoot a 16 GB runner. The snapshot section then peaks at
    // graph + file bytes + rebuilt graph.
    drop(base);

    // Three timed rounds (tech-selection §12 frozen protocol).
    let mut p50_rounds = [0.0f64; 3];
    let mut p99_rounds = [0.0f64; 3];
    for round in 0..3 {
        let mut latencies_ms = Vec::with_capacity(queries.len());
        for q in &queries {
            let t = Instant::now();
            let _ = graph.search(q, k, Some(ef)).unwrap();
            latencies_ms.push(t.elapsed().as_secs_f64() * 1000.0);
        }
        latencies_ms.sort_by(f64::total_cmp);
        p50_rounds[round] = percentile(&latencies_ms, 50);
        p99_rounds[round] = percentile(&latencies_ms, 99);
    }
    println!(
        "p50_rounds_ms={:.3},{:.3},{:.3}",
        p50_rounds[0], p50_rounds[1], p50_rounds[2]
    );
    println!(
        "p99_rounds_ms={:.3},{:.3},{:.3}",
        p99_rounds[0], p99_rounds[1], p99_rounds[2]
    );
    println!("p50_ms={:.3}", median3(p50_rounds));
    println!("p99_ms={:.3}", median3(p99_rounds));

    if do_snapshot {
        let snap_path = std::env::temp_dir().join(format!(
            "m4_recall_probe-{}-{}.snap",
            std::process::id(),
            seed
        ));
        let t = Instant::now();
        snapshot::save(&graph, &snap_path).unwrap();
        let save_seconds = t.elapsed().as_secs_f64();
        let t = Instant::now();
        // Metric pinned to L2 again at load: snapshot::load takes the metric
        // as a caller-supplied argument (format §3 freeze), so the pin must
        // be repeated at every call site.
        let loaded = snapshot::load(&snap_path, Metric::L2, ef_u32).unwrap();
        let load_seconds = t.elapsed().as_secs_f64();
        println!("snapshot_save_seconds={save_seconds:.3}");
        println!("snapshot_load_seconds={load_seconds:.3}");
        // Equivalence surface (2026-09-08, Stage E review round 1 P3-3;
        // round 3 P3 finding 4): re-run ALL queries on the loaded graph and
        // diff the FULL (NodeId, f64) sequences against the in-memory
        // graph's — distances compared BITWISE (`to_bits`, +0.0/-0.0
        // distinguishable, the same bit-exact contract
        // snapshot_roundtrip.rs asserts). The round-1 version compared
        // NodeIds only while its comment claimed (NodeId, f64) identity —
        // a distance-only drift would have passed silently. Plus the loaded
        // graph's own recall. No assertion per this tool's discipline, so a
        // nonzero snapshot_query_diffs is THE alarm: the recall number
        // above is only meaningful for a graph that round-trips intact.
        let mut loaded_full: Vec<Vec<(pg_am_hnsw::NodeId, f64)>> =
            Vec::with_capacity(queries.len());
        for q in &queries {
            loaded_full.push(loaded.search(q, k, Some(ef)).unwrap());
        }
        let diffs = loaded_full
            .iter()
            .zip(&retrieved_full)
            .filter(|(a, b)| {
                a.len() != b.len()
                    || a.iter()
                        .zip(b.iter())
                        .any(|((ia, da), (ib, db))| ia != ib || da.to_bits() != db.to_bits())
            })
            .count();
        let loaded_retrieved: Vec<Vec<pg_am_hnsw::NodeId>> = loaded_full
            .iter()
            .map(|r| r.iter().map(|(id, _)| *id).collect())
            .collect();
        let loaded_recall = recall_at_k(&gt, &loaded_retrieved, k, base_count).unwrap();
        println!("snapshot_recall_at_{k}={loaded_recall:.4}");
        println!("snapshot_query_diffs={diffs}");
        let _ = std::fs::remove_file(&snap_path);
    }
}
