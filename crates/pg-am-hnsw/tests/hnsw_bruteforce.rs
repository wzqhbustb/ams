//! §8.3 correctness oracle (Stage B2): flood search (`ef = node_count`)
//! must equal a brute-force scan **exactly**, on synthetic Gaussian/uniform
//! mixture data, over the dim ∈ {2, 16, 128} × N ∈ {1k, 10k} matrix, plus
//! a 960-dim (gist-magnitude) smoke — the acceptance criterion names it.
//!
//! Premise and diagnostic path (§8.3, v1.1): the flood equality holds only
//! if the level-0 graph is entry-connected — with `ef ≥ node_count` the
//! beam never evicts, so search floods the whole *directed* reachable
//! component from the entry point. "ef large enough ⇒ exact" is NOT a
//! universal HNSW property outside that premise (see the graph.rs step-7
//! REACHABILITY NOTE for a parameter regime where it fails). Every cell
//! here therefore diagnoses **connectivity first, distances second**: the
//! directed reachability assertion fires before any vector comparison, so
//! a future connectivity regression is reported as connectivity, not as a
//! wall of distance mismatches.
//!
//! Exactness is bit-level: oracle distances come from the same frozen §5
//! functions the graph used, and both sides sort by `(distance, NodeId
//! ascending)`, so `Vec` equality is the honest comparison — no epsilon.
//!
//! Timing: the debug profile runs this file in ~100 s (dominated by the
//! 10k-node × 128-dim build; ~7 s in release — the release run is the
//! 耗时口径， coding plan Stage B). Query counts per cell are sized so the
//! ef=N full scans stay cheap; the flood visits every node per query by
//! construction.

mod common;

use common::*;
use pg_am_hnsw::{HnswParams, Metric, NodeId};

/// Matrix cell: `(graph seed, data seed, dim, N, metric, queries)`.
const AB_MATRIX: &[(u64, u64, u16, usize, Metric, usize)] = &[
    (41, 4041, 2, 1_000, Metric::L2, 25),
    (42, 4042, 2, 10_000, Metric::L2, 8),
    (43, 4043, 16, 1_000, Metric::L2, 25),
    (44, 4044, 16, 10_000, Metric::L2, 8),
    (45, 4045, 128, 1_000, Metric::L2, 20),
    (46, 4046, 128, 10_000, Metric::L2, 5),
    // Cosine smoke: flood equality is metric-agnostic (given connectivity);
    // the mixture never emits an exact zero vector, which Cosine rejects.
    (47, 4047, 16, 1_000, Metric::Cosine, 15),
];

#[test]
fn flood_search_equals_brute_force_matrix() {
    for &(gseed, dseed, dim, n, metric, n_queries) in AB_MATRIX {
        let mut gen = MixtureGen::new(usize::from(dim), 8, dseed);
        let g = build_graph(dim, metric, HnswParams::default(), gseed, n, &mut gen);

        // §8.3 diagnostic step 1: connectivity (directed — the notion the
        // flood search is bounded by). Default §4.2 params keep the 200/200
        // regime fact (graph.rs flood test); a failure here means the
        // premise broke, and distances are not even worth comparing.
        let reachable = directed_reachable_count(&g);
        assert_eq!(
            reachable, n,
            "§8.3 premise failed: directed level-0 reachability {reachable} < {n} \
             (seed={gseed} dim={dim} metric={metric:?}) — check connectivity before distances"
        );

        // §8.3 diagnostic step 2: distances — ef = N flood == brute force,
        // exact ordering, full k = N result set. Queries continue the SAME
        // generator (centers are drawn at `MixtureGen` construction, so a
        // fresh instance would put queries on different blobs than the data
        // — 2026-08-31 review P3); drawing past the N data points keeps
        // them in-distribution, per the §8 benchmark methodology.
        for qi in 0..n_queries {
            let q = gen.next_vec();
            let hits = g.search(&q, n, Some(n)).unwrap();
            let brute = brute_force_all(&g, &q);
            assert_eq!(
                hits.len(),
                n,
                "flood must return every node (seed={gseed} dim={dim} query {qi})"
            );
            assert_eq!(
                hits, brute,
                "§8.3: ef = N flood diverged from brute force (seed={gseed} dim={dim} \
                 metric={metric:?} query {qi})"
            );
            // partial-k slice: the top-10 prefix of the same frozen order
            let top10 = g.search(&q, 10, Some(n)).unwrap();
            assert_eq!(top10, brute[..10].to_vec());
        }
        eprintln!(
            "ab cell seed={gseed} dim={dim} N={n} metric={metric:?}: {n_queries} queries, exact"
        );
    }
}

#[test]
fn flood_search_equals_brute_force_960dim_smoke() {
    // gist-magnitude smoke (coding plan Stage B acceptance: "含 gist 量级
    // 的 960 维合成集冒烟"). N = 1k keeps the build cheap; three queries at
    // ef = N. Same diagnostic order as the matrix: connectivity first.
    let dim = 960u16;
    let n = 1_000usize;
    let mut gen = MixtureGen::new(usize::from(dim), 8, 4960);
    let g = build_graph(dim, Metric::L2, HnswParams::default(), 48, n, &mut gen);

    let reachable = directed_reachable_count(&g);
    assert_eq!(
        reachable, n,
        "§8.3 premise failed at 960-dim: directed reachability {reachable} < {n}"
    );

    for qi in 0..3 {
        let q = gen.next_vec(); // same generator as the data — in-distribution (review P3)
        let hits = g.search(&q, n, Some(n)).unwrap();
        let brute = brute_force_all(&g, &q);
        assert_eq!(hits, brute, "§8.3: 960-dim flood diverged (query {qi})");
        let top10 = g.search(&q, 10, Some(n)).unwrap();
        assert_eq!(top10, brute[..10].to_vec());
    }
    eprintln!("ab 960-dim smoke: N={n}, 3 queries, exact");
}

#[test]
fn flood_search_ip_metric_respects_reachable_component() {
    // §8.3 generalized to InnerProduct (2026-08-31 review P3): IP is not a
    // metric, and an entry-incomplete directed reachable component is a
    // LEGITIMATE outcome — the plain flood-equality cell cannot be
    // generalized to IP without first establishing the connectivity
    // premise (the reviewer's IP probe: flood returned 1950 nodes where
    // brute force scans 2000). This cell pins the generalization WITH the
    // premise: the ef = N flood returns **exactly** the directed reachable
    // component — no more, no less — in the exact frozen order. The
    // reachable count itself is pinned as a regime observation (not an
    // invariant) so silent drift in either direction shows up here.
    let dim = 16u16;
    let n = 2_000usize;
    let mut gen = MixtureGen::new(usize::from(dim), 8, 5116);
    let g = build_graph(
        dim,
        Metric::InnerProduct,
        HnswParams::default(),
        51,
        n,
        &mut gen,
    );

    let reachable = directed_reachable_set(&g);
    let reachable_count = reachable.iter().filter(|&&s| s).count();
    eprintln!("ip cell: directed reachable {reachable_count}/{n}");
    assert_eq!(
        reachable_count, REACHABLE_PIN,
        "regime observation drifted (IP, default params, seed 51/5116) — \
         investigate whether the change was intended, then re-pin"
    );

    for qi in 0..10 {
        let q = gen.next_vec(); // in-distribution (review P3)
        let hits = g.search(&q, n, Some(n)).unwrap();
        let mut expected: Vec<(NodeId, f64)> = (0..n)
            .filter(|&i| reachable[i])
            .map(|i| {
                let id = NodeId(i as u32);
                (id, dist_to(&g, &q, id))
            })
            .collect();
        expected.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap().then(a.0.cmp(&b.0)));
        assert_eq!(
            hits,
            expected,
            "§8.3 IP: ef = N flood diverged from the reachable component's exact order (query {qi})"
        );
    }
}

/// Pinned directed-reachable count of the IP cell above (measured
/// 2026-08-31: 1889/2000; a regime observation, NOT an invariant — IP is
/// not a metric and partial reachability is legitimate).
const REACHABLE_PIN: usize = 1889;
