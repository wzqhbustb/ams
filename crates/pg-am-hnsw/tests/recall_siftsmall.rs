//! M4 Stage D CI hard gate (coding plan Stage D, contract v1.3): build an
//! HNSW graph over siftsmall (10k x 128d) with the frozen acceptance
//! parameters and require recall@10 >= 0.98 at ef=64.
//!
//! Skip logic (v1.3 pinned, do NOT "simplify"): the hard-failure trigger is
//! exactly `M4_REQUIRE_DATASET=1`, set only by the dedicated fetch-equipped
//! gate step. GitHub Actions sets `CI=true` on EVERY runner, so a
//! `CI=true`-based hard failure would self-destruct every plain matrix job
//! (no dataset there) — that discrimination was explicitly rejected.
//!
//! Run tier: the dedicated gate step runs `--release` (debug build time for
//! 10k x 128d was unmeasured at contract time; the measured values land in
//! docs/phase2-m4-benchmarks.md).

mod common;

use std::path::PathBuf;

use pg_am_hnsw::dataset::{read_fvecs, read_ivecs, recall_at_k};
use pg_am_hnsw::{Hnsw, HnswParams, Metric, NodeId};

/// `<workspace>/datasets/siftsmall`. `CARGO_MANIFEST_DIR` is
/// `crates/pg-am-hnsw`, so the workspace root is TWO parents up — the
/// contract text says "上溯一级" but the manifest dir nests under `crates/`;
/// the deviation is recorded in the Stage D delivery report (2026-09-03).
fn siftsmall_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("datasets/siftsmall")
}

#[test]
fn siftsmall_recall_gate() {
    let dir = siftsmall_dir();
    let base_path = dir.join("siftsmall_base.fvecs");
    let query_path = dir.join("siftsmall_query.fvecs");
    let gt_path = dir.join("siftsmall_groundtruth.ivecs");
    let present = base_path.exists() && query_path.exists() && gt_path.exists();
    if !present {
        // 2026-09-08, Stage E review round 3 P3: read the trigger STRICTLY —
        // the pre-fix `as_deref() == Ok("1")` swallowed
        // `VarError::NotUnicode`, silently DOWNGRADING a (mis-)set gate
        // trigger into a skip. In a gate context that is the "green but
        // never ran" failure mode this variable exists to prevent, so a
        // non-Unicode value panics with the variable name.
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
            "siftsmall not found at {}; skipping recall gate (set M4_REQUIRE_DATASET=1 to make a missing dataset a hard failure)",
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

    // Frozen acceptance parameters (coding plan Stage D): M=16, m_max0=32,
    // ef_construction=200, ef_search=64, fixed seed for reproducibility.
    // Metric pinned to L2 — sift/siftsmall/gist are all L2 and the snapshot
    // format carries no metric (Stage C residual discipline).
    let params = HnswParams::new(16, 32, 200, 64).unwrap();
    let mut graph = Hnsw::new(128, Metric::L2, params, 42).unwrap();
    for v in &base {
        graph.insert(v).unwrap();
    }

    let k = 10;
    let ef = 64;
    let retrieved: Vec<Vec<NodeId>> = queries
        .iter()
        .map(|q| {
            graph
                .search(q, k, Some(ef))
                .unwrap()
                .into_iter()
                .map(|(id, _)| id)
                .collect()
        })
        .collect();
    let recall = recall_at_k(&gt, &retrieved, k, base.len() as u32).unwrap();
    eprintln!("siftsmall recall@10 @ ef=64: {recall:.4} (threshold 0.98)");
    assert!(
        recall >= 0.98,
        "siftsmall recall@10 = {recall:.4} < 0.98 (coding plan Stage D hard gate; \
         if this regressed, check the §4.3 heuristic and the deterministic-tie ordering first)"
    );
}

/// Always-on companion (2026-09-03, Stage D round 1): the recall SCORING
/// function gets a real workout even on dataset-less matrix jobs, so the
/// gate file is never a pure-skip placeholder. Known ground truth, known
/// retrieved sets, exact expected fractions — plus the error paths.
#[test]
fn recall_scoring_known_answer() {
    // One query: gt row [7, 3, 9, 1] (ivecs original order — NOT re-sorted),
    // k = 3 -> gt set {7, 3, 9}.
    let gt = vec![vec![7, 3, 9, 1]];
    // Retrieved {3, 7, 42}: hits {7, 3} -> 2/3. NodeId order in the result
    // (search output order) is irrelevant to set intersection by design.
    let ret = vec![vec![NodeId(3), NodeId(7), NodeId(42)]];
    let r = recall_at_k(&gt, &ret, 3, 1000).unwrap();
    assert!((r - 2.0 / 3.0).abs() < 1e-12, "recall = {r}");

    // Two queries, mixed scores: q0 3/3, q1 0/3 -> mean 0.5.
    let gt = vec![vec![1, 2, 3, 4], vec![10, 20, 30, 40]];
    let ret = vec![
        vec![NodeId(3), NodeId(1), NodeId(2)],
        vec![NodeId(0), NodeId(5), NodeId(6)],
    ];
    assert_eq!(recall_at_k(&gt, &ret, 3, 1000).unwrap(), 0.5);

    // Tie note (contract asks the k/(k+1) equidistant-tie policy to be
    // written down): the gt set is the literal ivecs prefix {5, 6, 9},
    // never re-sorted and never expanded for ties. Retrieving 9 (rank 3,
    // inside the prefix) scores full marks even though the file lists it
    // AFTER 5 and 6...
    let gt = vec![vec![5, 6, 9, 8]];
    let ret = vec![vec![NodeId(9), NodeId(5), NodeId(6)]];
    assert_eq!(recall_at_k(&gt, &ret, 3, 1000).unwrap(), 1.0);
    // ...while 8 (rank 4, OUTSIDE the prefix) scores nothing even if it is
    // equidistant to the rank-3 entry — the ivecs row's own order is the
    // referee, so a tie at the k/(k+1) boundary is decided by file order.
    let ret = vec![vec![NodeId(8), NodeId(5), NodeId(6)]];
    let r = recall_at_k(&gt, &ret, 3, 1000).unwrap();
    assert!((r - 2.0 / 3.0).abs() < 1e-12, "recall = {r}");

    // Error paths must fail loudly (never silently lower the score).
    use pg_am_hnsw::HnswError;
    assert!(matches!(
        recall_at_k(&gt, &ret, 0, 1000),
        Err(HnswError::InvalidArgument(_))
    ));
    assert!(matches!(
        recall_at_k(&gt, &ret[..0], 3, 1000),
        Err(HnswError::InvalidArgument(_))
    ));
    assert!(matches!(
        recall_at_k(&[vec![1, 2]], &ret[..1], 3, 1000),
        Err(HnswError::InvalidArgument(_))
    ));
}

/// Always-on end-to-end recall sanity on synthetic data (tests/common's
/// MixtureGen, §8.3): brute-force ground truth from `common::brute_force_all`
/// vs. real search, recall must be near-perfect on a tiny well-clustered
/// graph with a generous beam. Loose threshold (0.90) — this tests that the
/// scoring pipeline is wired correctly, not the §12 acceptance number.
#[test]
fn recall_scoring_synthetic_end_to_end() {
    let dim = 16;
    let params = HnswParams::new(8, 16, 100, 50).unwrap();
    let mut gen = common::MixtureGen::new(dim, 4, 0x5EED);
    let graph = common::build_graph(dim as u16, Metric::L2, params, 42, 300, &mut gen);

    let k = 5;
    let ef = 50;
    let mut gt: Vec<Vec<i32>> = Vec::new();
    let mut retrieved: Vec<Vec<NodeId>> = Vec::new();
    for _ in 0..20 {
        let q = gen.next_vec();
        gt.push(
            common::brute_force_all(&graph, &q)[..k]
                .iter()
                .map(|(id, _)| id.0 as i32)
                .collect(),
        );
        retrieved.push(
            graph
                .search(&q, k, Some(ef))
                .unwrap()
                .into_iter()
                .map(|(id, _)| id)
                .collect(),
        );
    }
    let recall = recall_at_k(&gt, &retrieved, k, graph.node_count() as u32).unwrap();
    assert!(
        recall >= 0.90,
        "synthetic recall@5 = {recall:.4} < 0.90 — scoring pipeline or search wiring is broken"
    );
}
