//! Shared helpers for the Stage B2 integration suites (§8.3 A/B oracle and
//! the §9 property four-pack). Everything here is deterministic: the only
//! entropy source is the crate's own explicitly-seeded
//! [`Xoshiro256StarStar`] (§4.1 — the `rand` crate stays banned, §10; the
//! dependency freeze {thiserror, crc32fast} forbids new dev-dependencies
//! too, so test data is generated with the crate's PRNG).

#![allow(dead_code)] // each integration binary uses a subset

use pg_am_hnsw::distance;
use pg_am_hnsw::rng::Xoshiro256StarStar;
use pg_am_hnsw::{Hnsw, Metric, NodeId};

/// Uniform `u ∈ [0, 1)` from the top 53 bits — the frozen u64→f64 conversion
/// of §4.1 v1.2, reused so test data and graph level draws share one
/// bit-exact convention.
fn next_open01(rng: &mut Xoshiro256StarStar) -> f64 {
    (rng.next_u64() >> 11) as f64 * (1.0 / 9_007_199_254_740_992.0)
}

/// Uniform `u ∈ (0, 1)` (log-safe) by redraw — same scheme as
/// `next_level`'s `u == 0.0` redraw (§4.1 v1.2).
fn next_open1(rng: &mut Xoshiro256StarStar) -> f64 {
    loop {
        let u = next_open01(rng);
        if u > 0.0 {
            return u;
        }
    }
}

/// Standard normal via Box–Muller (one variate per call; the twin is
/// discarded — determinism does not need the pairing).
fn next_gaussian(rng: &mut Xoshiro256StarStar) -> f64 {
    let u1 = next_open1(rng);
    let u2 = next_open01(rng);
    (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos()
}

/// Deterministic synthetic data: a Gaussian/uniform mixture (§8.3's
/// "随机高斯/均匀混合数据"). 80% of points are N(center_c, I) around one of
/// `clusters` random centers in [-10, 10)^dim; 20% are uniform in
/// [-10, 10)^dim. Cluster structure gives HNSW a non-trivial topology
/// (well-separated blobs plus background noise), unlike i.i.d. Gaussian
/// where all pairwise distances concentrate.
pub struct MixtureGen {
    rng: Xoshiro256StarStar,
    centers: Vec<f32>,
    dim: usize,
    clusters: usize,
}

impl MixtureGen {
    pub fn new(dim: usize, clusters: usize, seed: u64) -> Self {
        let mut rng = Xoshiro256StarStar::new(seed);
        let mut centers = vec![0.0f32; clusters * dim];
        for c in &mut centers {
            *c = (next_open01(&mut rng) * 20.0 - 10.0) as f32;
        }
        Self {
            rng,
            centers,
            dim,
            clusters,
        }
    }

    pub fn next_vec(&mut self) -> Vec<f32> {
        let mut v = vec![0.0f32; self.dim];
        if next_open01(&mut self.rng) < 0.8 {
            let c = (next_open01(&mut self.rng) * self.clusters as f64) as usize % self.clusters;
            for (d, x) in v.iter_mut().enumerate() {
                *x = (f64::from(self.centers[c * self.dim + d]) + next_gaussian(&mut self.rng))
                    as f32;
            }
        } else {
            for x in &mut v {
                *x = (next_open01(&mut self.rng) * 20.0 - 10.0) as f32;
            }
        }
        v
    }
}

/// Insert `n` vectors from `gen` into a fresh graph; returns the graph.
pub fn build_graph(
    dim: u16,
    metric: Metric,
    params: pg_am_hnsw::HnswParams,
    graph_seed: u64,
    n: usize,
    gen: &mut MixtureGen,
) -> Hnsw {
    let mut g = Hnsw::new(dim, metric, params, graph_seed).unwrap();
    for _ in 0..n {
        g.insert(&gen.next_vec()).unwrap();
    }
    g
}

/// Distance from `query` to stored node `id` under the graph's own metric —
/// the same frozen functions the graph uses (§5), so values are bit-equal
/// to the ones search computed.
pub fn dist_to(g: &Hnsw, query: &[f32], id: NodeId) -> f64 {
    let r = match g.metric() {
        Metric::L2 => distance::l2_squared(query, g.vector(id)),
        Metric::Cosine => distance::cosine(query, g.vector(id)),
        Metric::InnerProduct => distance::negative_inner_product(query, g.vector(id)),
    };
    r.expect("test vectors are finite and non-zero")
}

/// Brute-force oracle (§8.3): every node scored and sorted by the frozen
/// output order `(distance, NodeId ascending)` — the exact ordering
/// [`Hnsw::search`] contracts to.
///
/// Known limitation (Stage B adversarial review P3-2, recorded): the vectors
/// are read back through `g.vector(id)`, so an arena indexing bug inside
/// `Hnsw` would corrupt both sides of the comparison identically and this
/// oracle could not catch it. Coverage/sort-order bugs ARE caught (the oracle
/// iterates and ranks independently). Vector-content correctness rests on
/// distance.rs's known-answer tests plus graph.rs's
/// `hand_derived_graph_search_is_consistent`, which scores against a
/// test-side independent positions array. Replaying the inserted data from
/// the harness instead is possible but was judged not worth the plumbing.
pub fn brute_force_all(g: &Hnsw, query: &[f32]) -> Vec<(NodeId, f64)> {
    let mut v: Vec<(NodeId, f64)> = (0..g.node_count())
        .map(|i| {
            let id = NodeId(i as u32);
            (id, dist_to(g, query, id))
        })
        .collect();
    v.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap().then(a.0.cmp(&b.0)));
    v
}

/// Count of nodes reachable from the entry point following **directed**
/// level-0 edges — the reachability notion that bounds a flood search
/// (Algorithm 2 follows out-edges only). §8.3 diagnostic step 1.
pub fn directed_reachable_count(g: &Hnsw) -> usize {
    directed_reachable_set(g).iter().filter(|&&s| s).count()
}

/// The directed reachable component itself (entry point, level 0, out-edges
/// only), as a `Vec<bool>` indexed by `NodeId` — the §8.3 diagnostic path
/// for metrics where a partial component is legitimate (InnerProduct; see
/// `flood_search_ip_metric_respects_reachable_component`).
pub fn directed_reachable_set(g: &Hnsw) -> Vec<bool> {
    let n = g.node_count();
    let mut seen = vec![false; n];
    if n == 0 {
        return seen;
    }
    let mut stack = vec![g.entry_point().unwrap()];
    while let Some(u) = stack.pop() {
        if seen[u.index()] {
            continue;
        }
        seen[u.index()] = true;
        stack.extend_from_slice(g.neighbors(u, 0));
    }
    seen
}

/// Count of nodes reachable from the entry point on level 0 with edges
/// **undirected** (reverse adjacency rebuilt from the read-only accessors) —
/// the connectivity notion of §9 invariant ①. Weaker than directed
/// reachability: an asymmetric edge pair still connects the graph
/// undirectedly (see the step-7 REACHABILITY NOTE in graph.rs).
pub fn undirected_reachable_count(g: &Hnsw) -> usize {
    let n = g.node_count();
    if n == 0 {
        return 0;
    }
    let mut rev: Vec<Vec<NodeId>> = vec![Vec::new(); n];
    for i in 0..n {
        let id = NodeId(i as u32);
        for &nb in g.neighbors(id, 0) {
            rev[nb.index()].push(id);
        }
    }
    let mut seen = vec![false; n];
    let mut stack = vec![g.entry_point().unwrap()];
    while let Some(u) = stack.pop() {
        if seen[u.index()] {
            continue;
        }
        seen[u.index()] = true;
        stack.extend_from_slice(g.neighbors(u, 0));
        stack.extend_from_slice(&rev[u.index()]);
    }
    seen.iter().filter(|&&s| s).count()
}

/// Bidirectional-edge asymmetry statistics (§9 invariant ②): over all
/// directed edges `u → v` on every level, count those whose reverse `v → u`
/// is absent on the same level. Returns `(directed_edges, missing_reverse)`
/// aggregated over all levels. The ratio is a *measurement baseline* for the
/// M6 acceptance criterion (< 1%) — §9 ② is a口径-establishing statistic,
/// not a hard threshold.
pub fn asymmetry_stats(g: &Hnsw) -> (usize, usize) {
    let n = g.node_count();
    let mut directed = 0usize;
    let mut missing = 0usize;
    for i in 0..n {
        let id = NodeId(i as u32);
        for l in 0..=g.level(id) {
            for &nb in g.neighbors(id, l) {
                directed += 1;
                if !g.neighbors(nb, l).contains(&id) {
                    missing += 1;
                }
            }
        }
    }
    (directed, missing)
}
