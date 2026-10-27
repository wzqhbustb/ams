//! §9 property suite (Stage B2): the four graph invariants over a
//! multi-seed × multi-dim × multi-N matrix.
//!
//! - ① entry-point reachability (level-0, undirected) — see
//!   [`prop1_entry_reachability_default_param_regime`] for the parameter-
//!   regime caveat: this is a **regime fact**, not a universal invariant.
//! - ② bidirectional-edge asymmetry rate — baseline measurement only
//!   (§9 ②: the M6 acceptance threshold < 1% reuses this口径）.
//! - ③ level distribution vs. geometric expectation, chi-square with
//!   **tail-bin merging** where expected frequency < 5 (v1.1: an unmerged
//!   chi-square is statistically invalid).
//! - ④ ef monotonicity on synthetic data — asserted at **aggregate-mean**
//!   granularity (v1.1: per-query recall may dip occasionally as the beam's
//!   visited set shifts with ef; per-query hard assertions are
//!   "deterministic but wrongly-designed flaky").
//!
//! All seeds are explicit literals; there is no entropy source anywhere
//! (§4.1). Measured runtime: ~17 s debug / ~1.4 s release for this file,
//! dominated by the three 20k-node chi-square graph builds; release is the
//! timing口径 (coding plan Stage B acceptance command #2).

mod common;

use common::*;
use pg_am_hnsw::{Hnsw, HnswParams, Metric, NodeId};

// ---------------------------------------------------------------------
// §9 ① — entry reachability (level-0 BFS, undirected)
// ---------------------------------------------------------------------

/// Matrix cell: `(graph seed, data seed, dim, N)`.
const PROP1_MATRIX: &[(u64, u64, u16, usize)] = &[
    // default params, 3 seeds × 3 dims × 2 Ns
    (11, 1011, 2, 200),
    (11, 1012, 2, 1000),
    (12, 1013, 16, 200),
    (12, 1014, 16, 1000),
    (13, 1015, 128, 200),
    (13, 1016, 128, 1000),
    (14, 1017, 2, 200),
    (14, 1018, 16, 1000),
    (15, 1019, 128, 1000),
    // one larger cell for scale
    (16, 1020, 16, 5000),
];

#[test]
fn prop1_entry_reachability_default_param_regime() {
    // §9 ①: from the entry point, level-0 BFS reaches every node.
    //
    // REGIME FACT, NOT A UNIVERSAL INVARIANT (B1 measurement, recorded in
    // graph.rs's flood_search test and the step-7 REACHABILITY NOTE): at
    // extreme params (M=2 / M_max0=2 / ef_c=4) the shrink side can sever
    // the last bridging edge and the level-0 graph genuinely disconnects
    // (a 300-node graph had only 3 nodes reachable from the entry point);
    // at the frozen §4.2 defaults a 200-node graph was 200/200. This matrix
    // therefore uses default params only, and
    // [`prop1_known_disconnect_at_extreme_params_pinned`] pins the
    // disconnect as known behavior instead of asserting connectivity there.
    for &(gseed, dseed, dim, n) in PROP1_MATRIX {
        let mut gen = MixtureGen::new(usize::from(dim), 8, dseed);
        let g = build_graph(dim, Metric::L2, HnswParams::default(), gseed, n, &mut gen);
        let undirected = undirected_reachable_count(&g);
        let directed = directed_reachable_count(&g);
        eprintln!(
            "prop1 cell seed={gseed} dim={dim} N={n}: undirected {undirected}/{n}, directed {directed}/{n}"
        );
        assert_eq!(
            undirected,
            n,
            "§9 ①: undirected level-0 reachability < N at default params (seed={gseed} dim={dim} N={n})"
        );
        // Directed is the search-relevant notion (Algorithm 2 follows
        // out-edges) — 2026-08-31 review P3: asserting only the undirected
        // count while naming the property "entry reachability" left the
        // directed regime unguarded. All matrix cells measured directed
        // 100% at default params (B2), so assert it outright.
        assert_eq!(
            directed,
            n,
            "§9 ①: directed level-0 reachability < N at default params (seed={gseed} dim={dim} N={n})"
        );
    }
}

#[test]
fn prop1_known_disconnect_at_extreme_params_pinned() {
    // Companion to ①: at M=2 / M_max0=2 / ef_c=4 (the graph.rs
    // `line_graph` config), shrink repeatedly severs bridging edges and the
    // level-0 graph disconnects — the B1-measured regime fact above. This
    // test pins the behavior *as known* (reachable < total), so a future
    // change that silently alters the disconnect regime shows up here
    // rather than being discovered as a brute-force A/B failure. The
    // directed count is the search-relevant one (Algorithm 2 follows
    // out-edges); undirected is pinned alongside for the口径.
    //
    // Exact counts are pinned (deterministic, §4.1): at N=300 the
    // entry-reachable component shrinks to **3 nodes directed / 9
    // undirected** — the "入口可达仅 3" figure from the B1 measurement
    // reproduced. At N=40 the same config still spans 29/40 directed:
    // the disconnect grows with N as shrink keeps cutting bridges.
    let mut g = Hnsw::new(1, Metric::L2, HnswParams::new(2, 2, 4, 2).unwrap(), 11).unwrap();
    for i in 0..300u32 {
        g.insert(&[(i * 29 % 101) as f32]).unwrap();
    }
    let directed = directed_reachable_count(&g);
    let undirected = undirected_reachable_count(&g);
    eprintln!("prop1 extreme-params pin: directed {directed}/300, undirected {undirected}/300");
    assert_eq!(
        directed, 3,
        "known behavior (graph.rs REACHABILITY NOTE): at M=2/M_max0=2/ef_c=4 \
         the entry-reachable level-0 component collapses"
    );
    assert_eq!(undirected, 9, "same regime, undirected notion (§9 ①)");
    assert!(directed < g.node_count());
}

// ---------------------------------------------------------------------
// §9 ② — bidirectional-edge asymmetry rate (baseline口径）
// ---------------------------------------------------------------------

#[test]
fn prop2_asymmetry_rate_baseline() {
    // No hard threshold (§9 ② is "口径建立，M6 验收复用"): the numbers are
    // the deliverable — they land in the test log (`--nocapture`) and in
    // the Stage B2 report. The only assertion is that the statistic is
    // well-formed (a ratio in [0, 1] over a non-empty edge set), so a
    // future regression in the *measurement* fails loudly.
    let mut total_directed = 0usize;
    let mut total_missing = 0usize;
    for &(gseed, dseed, dim, n) in PROP1_MATRIX {
        let mut gen = MixtureGen::new(usize::from(dim), 8, dseed);
        let g = build_graph(dim, Metric::L2, HnswParams::default(), gseed, n, &mut gen);
        let (directed, missing) = asymmetry_stats(&g);
        total_directed += directed;
        total_missing += missing;
        eprintln!(
            "prop2 cell seed={gseed} dim={dim} N={n}: directed edges {directed}, \
             missing reverse {missing}, asymmetry {:.4}%",
            100.0 * missing as f64 / directed as f64
        );
    }
    assert!(total_directed > 0);
    assert!(total_missing <= total_directed);
    let rate = total_missing as f64 / total_directed as f64;
    eprintln!(
        "prop2 aggregate: {total_missing}/{total_directed} = {:.4}%",
        100.0 * rate
    );
    // Regression band around the Stage B measured baseline 15.35%
    // (tech-selection §9 ②, 2026-08-31 review P3): the exact value is
    // deterministic given the pinned seeds, but the band is deliberately
    // loose — it guards drift of the select/shrink machinery (a change
    // that silently doubles or halves the inherent asymmetry) without
    // re-pinning measurement noise.
    assert!(
        (0.05..=0.30).contains(&rate),
        "§9 ②: aggregate asymmetry {rate:.4} outside the [0.05, 0.30] band around the 0.1535 baseline"
    );
}

// ---------------------------------------------------------------------
// §9 ③ — level distribution vs. geometric expectation (chi-square)
// ---------------------------------------------------------------------

/// Chi-square statistic of the graph's level histogram against the frozen
/// geometric law (§4.1): `level = floor(-ln u · m_L)`, `m_L = 1/ln M`, so
/// `P(L ≥ l) = M^-l` and `P(L = l) = (M−1)·M^-(l+1)`.
///
/// Tail merging (§9 v1.1): individual bins for levels `0 .. l_star`, one
/// merged tail bin `{L ≥ l_star}`, where `l_star` is the largest `l` with
/// `N·M^-l ≥ 5` — every bin's expected frequency is then ≥ 5 (individual
/// bins are larger than the tail bin by the factor `(M−1)/M · M^(l_star−l)`
/// ≥ `5(M−1)/M`, and for M ≥ 2 the individual-bin expectation at
/// `l = l_star − 1` is `≥ 5·(M−1)/M · M = 5(M−1) ≥ 5`). An unmerged
/// chi-square against a geometric tail is statistically invalid (v1.1).
///
/// Returns `(chi2, df, bins)` where `df = bins − 1`: no parameter is
/// estimated from the sample (m_L is frozen by M), so the degree-of-
/// freedom loss is exactly one (the total-count constraint).
fn level_chi_square(g: &Hnsw) -> (f64, usize, Vec<(String, f64, f64)>) {
    let n = g.node_count() as f64;
    let m = f64::from(g.params().m());
    let l_star = ((n / 5.0).ln() / m.ln()).floor().max(1.0) as usize;

    let mut obs = vec![0.0f64; l_star + 1];
    for i in 0..g.node_count() {
        let l = usize::from(g.level(NodeId(i as u32)));
        obs[l.min(l_star)] += 1.0; // everything ≥ l_star lands in the tail bin
    }
    let mut bins = Vec::new();
    let mut chi2 = 0.0;
    for (l, &o) in obs.iter().enumerate() {
        let expected = if l < l_star {
            n * (m - 1.0) * m.powi(-(l as i32) - 1)
        } else {
            n * m.powi(-(l_star as i32)) // P(L ≥ l_star)
        };
        assert!(
            expected >= 5.0,
            "tail-merge invariant: every bin's expectation must be ≥ 5 (§9 v1.1)"
        );
        let label = if l < l_star {
            format!("L = {l}")
        } else {
            format!("L ≥ {l_star} (tail)")
        };
        chi2 += (o - expected).powi(2) / expected;
        bins.push((label, o, expected));
    }
    let df = bins.len() - 1;
    (chi2, df, bins)
}

/// Upper 0.1% critical values of χ²(df), df = 1..=10 (standard tables).
/// α = 0.001: the seeds are fixed so the test is deterministic, but the
/// threshold should still sit far enough out that a *legitimate* unlucky
/// draw is not reported as an algorithm bug — a false failure here would
/// send review down the wrong path.
///
/// Coverage limit: adding a config whose merged-bin count exceeds 11 (e.g.
/// M = 2 at N = 20k → l_star = 11 → df = 11) trips the assert below —
/// extend the table first (values verified against mpmath for df 1..=10).
fn chi2_critical_0_001(df: usize) -> f64 {
    const TABLE: [f64; 10] = [
        10.828, 13.816, 16.266, 18.467, 20.515, 22.458, 24.322, 26.124, 27.877, 29.588,
    ];
    assert!((1..=10).contains(&df), "critical table covers df 1..=10");
    TABLE[df - 1]
}

#[test]
fn prop3_level_distribution_chi_square() {
    // N ≥ 10k per config (§9 ③: smaller N carries no statistical meaning).
    // Configs: M=4 (more populated levels → more bins → sharper test) at
    // two seeds, and the frozen default M=16 at one. dim=2 keeps the build
    // cheap — the level law is independent of geometry, but the histogram
    // is read off the *graph* (`Hnsw::level`), not the raw PRNG, so an
    // insert-path bug that corrupted `levels[]` would still be caught.
    //
    // Recorded outcomes (2026-08-31, independently recomputed): seed 201
    // chi2 = 17.516 (df=5, crit 20.515, p ≈ 0.004) — passing but on the
    // low-probability side; seed 202 chi2 = 6.595, M=16 chi2 = 1.109. The
    // seeds are fixed, so these exact values are the deterministic baseline
    // any future regression compares against.
    let configs: &[(u64, HnswParams, usize)] = &[
        (201, HnswParams::new(4, 8, 64, 16).unwrap(), 20_000),
        (202, HnswParams::new(4, 8, 64, 16).unwrap(), 20_000),
        (203, HnswParams::default(), 20_000),
    ];
    for &(seed, params, n) in configs {
        let mut gen = MixtureGen::new(2, 4, 9000 + seed);
        let g = build_graph(2, Metric::L2, params, seed, n, &mut gen);
        let (chi2, df, bins) = level_chi_square(&g);
        let crit = chi2_critical_0_001(df);
        eprintln!(
            "prop3 M={} seed={seed} N={n}: chi2 = {chi2:.3}, df = {df}, crit(0.001) = {crit}",
            params.m()
        );
        for (label, o, e) in &bins {
            eprintln!("    {label}: observed {o:.0}, expected {e:.1}");
        }
        assert!(
            chi2 < crit,
            "§9 ③: level distribution rejected at α=0.001 (chi2 {chi2:.3} ≥ {crit}, df {df}; \
             M={}, seed {seed})",
            params.m()
        );
    }
}

// ---------------------------------------------------------------------
// §9 ④ — ef monotonicity, aggregate-mean recall (synthetic data)
// ---------------------------------------------------------------------

/// Mean recall@k of `search` against the brute-force oracle over a fixed
/// query set, at one ef value.
fn mean_recall(g: &Hnsw, queries: &[Vec<f32>], truths: &[Vec<NodeId>], k: usize, ef: usize) -> f64 {
    let mut sum = 0.0;
    for (q, truth) in queries.iter().zip(truths) {
        let hits = g.search(q, k, Some(ef)).unwrap();
        let hit = hits.iter().filter(|(id, _)| truth.contains(id)).count();
        sum += hit as f64 / k as f64;
    }
    sum / queries.len() as f64
}

#[test]
fn prop4_ef_recall_monotonic_in_aggregate() {
    // §9 ④ with the v1.1 granularity fix: the assertion is
    // **aggregate mean recall is non-decreasing in ef**, NOT per-query
    // monotonicity — the beam's visited set changes with ef, so a single
    // query's recall may legitimately dip at a larger ef.
    //
    // Tolerance: recalls are sums of hit/k over Q queries; a genuine
    // one-hit regression is 1/(k·Q) ≈ 1.6e-3 here, so a 1e-12 epsilon
    // absorbs only f64 summation dust, never a real dip.
    let k = 10;
    // The ladder includes 64 — the §12 CI acceptance beam width — so the
    // absolute floor below guards exactly the gate configuration
    // (2026-08-31 review P3: the old ladder skipped 64 and had no floor at
    // all; a uniformly degraded graph would still have passed monotonicity).
    let ef_ladder = [10usize, 20, 40, 64, 80, 160, 320];
    let cells: &[(u64, u64, u16, usize)] = &[
        (31, 3031, 16, 2000),
        (32, 3032, 2, 1000),
        (33, 3033, 128, 1000),
    ];
    for &(gseed, dseed, dim, n) in cells {
        let mut gen = MixtureGen::new(usize::from(dim), 8, dseed);
        let g = build_graph(dim, Metric::L2, HnswParams::default(), gseed, n, &mut gen);
        // Queries continue the SAME generator: `MixtureGen` draws its
        // cluster centers at construction, so a fresh instance would put
        // the queries on different blobs than the data (2026-08-31 review
        // P3 — off-distribution queries measured recall@10 ≈ 0.989 vs
        // 1.000 in-distribution at ef = 64). Drawing past the N data
        // points keeps queries in-distribution, matching the §8 benchmark
        // methodology (sift/gist ship same-distribution query sets).
        let queries: Vec<Vec<f32>> = (0..64).map(|_| gen.next_vec()).collect();
        let truths: Vec<Vec<NodeId>> = queries
            .iter()
            .map(|q| {
                brute_force_all(&g, q)[..k]
                    .iter()
                    .map(|(id, _)| *id)
                    .collect()
            })
            .collect();

        let mut prev = 0.0;
        let mut means = Vec::new();
        for &ef in &ef_ladder {
            let mean = mean_recall(&g, &queries, &truths, k, ef);
            means.push((ef, mean));
            assert!(
                mean >= prev - 1e-12,
                "§9 ④ (v1.1 aggregate): mean recall@k dipped at ef={ef}: {mean} < {prev} \
                 (seed={gseed} dim={dim} N={n})"
            );
            if ef == 64 {
                // §12 CI acceptance beam width: absolute floor 0.95
                // (2026-08-31 review P3). Measured well above it (≈0.99+
                // in-distribution), so the floor only fires on a genuine
                // quality regression, never on seed luck.
                assert!(
                    mean >= 0.95,
                    "§12 gate: mean recall@10 at ef=64 below the 0.95 floor: {mean} \
                     (seed={gseed} dim={dim} N={n})"
                );
            }
            prev = mean;
        }
        eprintln!("prop4 cell seed={gseed} dim={dim} N={n}: {means:?}");
    }
}
