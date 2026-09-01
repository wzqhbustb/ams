//! Graph structure and the HNSW core algorithms — paper Algorithms 1/2/4/5
//! (Malkov & Yashunin, TPAMI 2018), tech-selection §4/§6.
//!
//! **Stage B deliverable.** This module holds:
//!
//! - the SoA layout (§6): `vectors` contiguous arena, `levels`, per-node
//!   per-level adjacency, `entry_point`, `max_level`;
//! - insert (Algorithm 1) with the neighbor-selection heuristic on both the
//!   select and the shrink side (Algorithm 4, `extend_candidates = false` on
//!   all levels, `keep_pruned = true`, §4.3) — both sides call the *same*
//!   function (`Hnsw::select_neighbors`, private), with different input
//!   roles;
//! - greedy layer descent + layer-0 beam search (Algorithms 2/5, §4.4).
//!
//! Determinism prerequisites (§4.1) bind every line here: all sort keys are
//! `(distance, NodeId ascending)` (the `Ord` of the internal heap element
//! `Cand`); HashMap/HashSet iteration order is banned from the algorithm
//! path (the visited set is a bitset indexed by `NodeId`); the PRNG is one
//! explicitly-seeded instance per graph ([`crate::rng`]) and never enters
//! snapshots (§4.1 prerequisite ③). The same insert sequence with the same
//! seed produces a byte-identical graph.
//!
//! Adjacency lists are kept **sorted by `NodeId` ascending**. Order carries
//! no algorithmic meaning (membership is a set), so a canonical order is
//! free; it makes the in-memory form directly comparable in step-by-step
//! tests and maps one-to-one onto the §3 node-record encoding (Stage C).

use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;

use crate::distance;
use crate::error::{HnswError, Result};
use crate::params::{HnswParams, NodeId};
use crate::rng::Xoshiro256StarStar;

/// Distance metric of a graph instance (§5). The metric is a runtime
/// property of the graph, **not** part of the frozen snapshot header (§3) —
/// a snapshot stores geometry only, and the caller supplies the metric again
/// at load time (Stage C).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Metric {
    /// Squared Euclidean distance `Σ(aᵢ−bᵢ)²` (§5: no square root — ordering
    /// is equivalent and the `sqrt` is saved).
    L2,
    /// Cosine distance `1 − (a·b)/(|a||b|)`; zero vectors are rejected loudly
    /// at every entry point (§5).
    Cosine,
    /// Negative inner product `−a·b` (§5: negation turns max-IP retrieval
    /// into min-distance search, so the search code needs zero branches).
    InnerProduct,
}

impl Metric {
    /// Dispatch to the frozen distance functions (§5). Never re-implemented
    /// here — the f64-accumulator chains in [`crate::distance`] are the
    /// cross-platform determinism mechanism.
    fn distance(self, a: &[f32], b: &[f32]) -> Result<f64> {
        match self {
            Metric::L2 => distance::l2_squared(a, b),
            Metric::Cosine => distance::cosine(a, b),
            Metric::InnerProduct => distance::negative_inner_product(a, b),
        }
    }
}

/// Neighbor-selection mode (§4.3 A/B control). `Heuristic` is the frozen
/// production path (paper Algorithm 4, `extend_candidates = false`,
/// `keep_pruned = true`); `Simple` is the control group (paper Algorithm 3 —
/// take the nearest `M`).
///
/// Reachable but unsupported (2026-08-31 review P3 wording fix):
/// `#[doc(hidden)]` hides the item from rendered docs but keeps it *named
/// and callable* from downstream crates — that is deliberate, because the
/// Stage D A/B harness lives in integration tests / examples, which are
/// downstream-crate positions. "Not part of the public API contract" means:
/// **zero stability guarantee** — the switch may change or be deleted in
/// any commit without notice; production callers use [`Hnsw::new`].
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NeighborSelection {
    /// Paper Algorithm 4 with `extend_candidates = false` (all levels) and
    /// `keep_pruned = true` (occluded candidates refill in ascending-distance
    /// order up to the limit — §4.3, connectivity-first).
    Heuristic,
    /// Paper Algorithm 3: take the `limit` nearest candidates by
    /// `(distance, NodeId ascending)`. Control group only.
    Simple,
}

/// Heap element: a candidate or kept result keyed by `(distance, NodeId
/// ascending)` — the frozen tie-break of §4.1 prerequisite ①. `BinaryHeap`
/// is a max-heap, so `BinaryHeap<Cand>` keeps the *worst* retained element
/// on top (the result set), and `BinaryHeap<Reverse<Cand>>` pops the
/// *nearest* unprocessed candidate first (the candidate frontier).
///
/// Distances are always finite: §5 entry validation rejects NaN and ±inf
/// components at every entry point, and the frozen distance functions map
/// finite inputs to finite outputs (f32 components promote exactly to f64;
/// even at the u16 dimension ceiling of 65535 the worst-case accumulation
/// (~3e82) stays orders of magnitude inside f64 range). The `expect` in
/// `cmp` is therefore unreachable.
#[derive(Debug, Clone, Copy)]
struct Cand {
    dist: f64,
    id: NodeId,
}

impl PartialEq for Cand {
    fn eq(&self, other: &Self) -> bool {
        self.dist == other.dist && self.id == other.id
    }
}

impl Eq for Cand {}

impl Ord for Cand {
    fn cmp(&self, other: &Self) -> Ordering {
        self.dist
            .partial_cmp(&other.dist)
            .expect("distances are finite (§5 entry validation)")
            .then(self.id.cmp(&other.id))
    }
}

impl PartialOrd for Cand {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// In-memory HNSW graph (§6 SoA layout).
///
/// `NodeId`s are allocated densely and never reused (§3 stability contract):
/// node `i` owns `vectors[i*dim .. (i+1)*dim]`, `levels[i]`, and
/// `adjacency[i]`. `levels[i]` is the node's top level and always equals
/// `adjacency[i].len() - 1` — the two representations of the §3
/// `level_count == top_level + 1` identity, kept in lockstep at insert time
/// (levels never change afterwards; M4 has no deletes).
#[derive(Debug)]
pub struct Hnsw {
    dim: u16,
    metric: Metric,
    params: HnswParams,
    selection: NeighborSelection,
    /// One explicitly-seeded PRNG per graph (§4.1): same seed + same insert
    /// sequence => byte-identical graph. Never serialized (§4.1 ③).
    rng: Xoshiro256StarStar,
    /// All vectors in one contiguous arena (§6: distance computation is
    /// memory-bandwidth bound; the contiguous layout is prefetch-friendly).
    vectors: Vec<f32>,
    /// `levels[i]` = top level of node `i` (== `adjacency[i].len() - 1`).
    levels: Vec<u8>,
    /// `adjacency[i][l]` = node `i`'s neighbors on level `l`, sorted by
    /// `NodeId` ascending; level 0 always exists.
    adjacency: Vec<Vec<Vec<NodeId>>>,
    /// Entry point: the first node of an empty graph, afterwards the node
    /// with the highest level (Algorithm 1). `None` iff the graph is empty.
    entry_point: Option<NodeId>,
    /// Top level of the entry-point node; 0 for an empty graph (§3 encoding
    /// invariant: empty graph => `max_level == 0`).
    max_level: u8,
}

impl Hnsw {
    /// Create an empty graph (§6). `dim` is fixed for the graph's lifetime —
    /// the same graph never mixes dimensions (§3). `seed` feeds the level
    /// draws (§4.1 determinism). `dim == 0` is rejected (§5: `dim = 0` fails
    /// at every entry point, graph construction included).
    pub fn new(dim: u16, metric: Metric, params: HnswParams, seed: u64) -> Result<Self> {
        Self::build(dim, metric, params, seed, NeighborSelection::Heuristic)
    }

    /// Construction-time A/B switch for the neighbor-selection mode (§4.3).
    ///
    /// Reachable but unsupported (same wording as [`NeighborSelection`]):
    /// `pub` because the Stage D harness calls it from integration
    /// tests/examples; `#[doc(hidden)]` because it carries **no stability
    /// guarantee** and may be removed once the A/B data is collected.
    /// Production callers use [`Hnsw::new`].
    #[doc(hidden)]
    pub fn new_with_neighbor_selection(
        dim: u16,
        metric: Metric,
        params: HnswParams,
        seed: u64,
        selection: NeighborSelection,
    ) -> Result<Self> {
        Self::build(dim, metric, params, seed, selection)
    }

    fn build(
        dim: u16,
        metric: Metric,
        params: HnswParams,
        seed: u64,
        selection: NeighborSelection,
    ) -> Result<Self> {
        if dim == 0 {
            return Err(HnswError::InvalidArgument(
                "dim = 0 graph (§5: rejected at every entry point)".to_string(),
            ));
        }
        Ok(Self {
            dim,
            metric,
            params,
            selection,
            rng: Xoshiro256StarStar::new(seed),
            vectors: Vec::new(),
            levels: Vec::new(),
            adjacency: Vec::new(),
            entry_point: None,
            max_level: 0,
        })
    }

    /// Vector dimension, fixed at construction (§3: same graph never mixes
    /// dimensions).
    pub fn dim(&self) -> u16 {
        self.dim
    }

    /// The graph's distance metric (§5).
    pub fn metric(&self) -> Metric {
        self.metric
    }

    /// Construction parameters (§4.2).
    pub fn params(&self) -> HnswParams {
        self.params
    }

    /// Number of nodes ever inserted (NodeIds are dense: valid ids are
    /// exactly `NodeId(0) .. NodeId(node_count)`).
    pub fn node_count(&self) -> usize {
        self.levels.len()
    }

    /// Entry point, or `None` on an empty graph.
    pub fn entry_point(&self) -> Option<NodeId> {
        self.entry_point
    }

    /// Top level of the entry-point node; 0 for an empty graph (the §3
    /// encoding invariant, so Stage C can serialize this value verbatim).
    pub fn max_level(&self) -> u8 {
        self.max_level
    }

    /// Top level of `node` (== `level_count - 1` in the §3 encoding).
    ///
    /// Panics if `node` is out of range (read-only inspection API for tests
    /// and the Stage C serializer — index like a `Vec`).
    pub fn level(&self, node: NodeId) -> u8 {
        self.levels[node.index()]
    }

    /// The stored vector of `node` (`dim` components of the contiguous
    /// arena, §6).
    ///
    /// Panics if `node` is out of range.
    pub fn vector(&self, node: NodeId) -> &[f32] {
        let start = node.index() * usize::from(self.dim);
        &self.vectors[start..start + usize::from(self.dim)]
    }

    /// `node`'s neighbor list on `level`, sorted by `NodeId` ascending.
    ///
    /// Panics if `node` is out of range or `level` exceeds the node's top
    /// level.
    pub fn neighbors(&self, node: NodeId, level: u8) -> &[NodeId] {
        &self.adjacency[node.index()][usize::from(level)]
    }

    /// `node`'s whole per-level adjacency (`[level][neighbors]`), level 0
    /// first — the exact shape [`crate::encoding`] node records round-trip
    /// (Stage C serializes this slice directly).
    ///
    /// Panics if `node` is out of range.
    pub fn node_adjacency(&self, node: NodeId) -> &[Vec<NodeId>] {
        &self.adjacency[node.index()]
    }

    /// Insert a vector (paper Algorithm 1); returns the freshly allocated
    /// [`NodeId`] (dense, never reused — §3).
    ///
    /// Entry validation (§5): wrong dimension, non-finite (NaN/±inf)
    /// components, and — for [`Metric::Cosine`] — zero vectors are loud
    /// errors at *every* entry point, including the first insert into an
    /// empty graph (which computes no pairwise distances, so the check is
    /// done explicitly here via the metric's self-distance).
    pub fn insert(&mut self, vector: &[f32]) -> Result<NodeId> {
        self.validate_entry_vector(vector, "insert")?;
        if self.node_count() == u32::MAX as usize {
            // Defensive: ~4 billion nodes. The INVALID sentinel must stay
            // unallocated (§3), so the id space is exhausted one slot early.
            return Err(HnswError::InvalidOperation(
                "NodeId space (u32) exhausted; NodeId::INVALID must stay unallocated (§3)"
                    .to_string(),
            ));
        }
        let id = NodeId(self.node_count() as u32);
        let level = self.rng.next_level(self.params.m());
        self.vectors.extend_from_slice(vector);
        self.levels.push(level);
        self.adjacency
            .push(vec![Vec::new(); usize::from(level) + 1]);

        // Empty graph: the first node becomes the entry point (Algorithm 1).
        let Some(mut ep) = self.entry_point else {
            self.entry_point = Some(id);
            self.max_level = level;
            return Ok(id);
        };

        // Phase 1: greedy descent (ef = 1) from the entry point down to the
        // new node's own top level (Algorithm 1, first loop).
        for l in ((level + 1)..=self.max_level).rev() {
            ep = self.search_layer(vector, &[ep], 1, l)[0].id;
        }

        // Phase 2: from min(level, max_level) down to 0, beam-search with
        // ef_construction, select neighbors, connect both sides, shrink
        // over-capacity lists through the same heuristic (§4.3 "both sides").
        let ef_construction = self.params.ef_construction() as usize;
        let m = usize::from(self.params.m());
        for l in (0..=level.min(self.max_level)).rev() {
            let w = self.search_layer(vector, &[ep], ef_construction, l);
            // w is non-empty (ep itself is always in the result set).
            let neighbors = self.select_neighbors(&w, m);
            for &nb in &neighbors {
                self.push_edge(nb, l, id);
                let list_len = self.adjacency[nb.index()][usize::from(l)].len();
                if list_len > self.m_max(l) {
                    self.shrink(nb, l);
                }
            }
            // The new node's own list is published only after the backward
            // edges are in place — nothing in the loop above reads it.
            self.adjacency[id.index()][usize::from(l)] = neighbors;
            ep = w[0].id; // nearest result carries the descent (Algorithm 1)
        }

        // New top-level node: it becomes the entry point (Algorithm 1).
        if level > self.max_level {
            self.entry_point = Some(id);
            self.max_level = level;
        }
        Ok(id)
    }

    /// k-nearest-neighbor search (paper Algorithm 5 = greedy descent +
    /// Algorithm 2 beam search on level 0).
    ///
    /// Returns up to `k` hits as `(NodeId, distance)`, sorted ascending by
    /// `(distance, NodeId)` (§4.1 prerequisite ①, §8.2 output ordering).
    /// `ef` is the beam width; `None` uses the construction default
    /// `ef_search_default` (§4.4). The **only** per-query invariant is
    /// `ef >= k` (coding plan Stage B / §4.4 disambiguation): per-query `ef`
    /// may go below `M` — the `>= M` constraint binds the construction
    /// default only and lives in [`HnswParams`].
    ///
    /// `k == 0` and searching an empty graph both return an empty vector
    /// (after entry validation, §5 — validation holds at every entry point,
    /// including degenerate ones).
    pub fn search(&self, query: &[f32], k: usize, ef: Option<usize>) -> Result<Vec<(NodeId, f64)>> {
        self.validate_entry_vector(query, "search")?;
        let ef = ef.unwrap_or(self.params.ef_search_default() as usize);
        if ef < k {
            return Err(HnswError::InvalidArgument(format!(
                "ef = {ef} < k = {k} (§4.4: the beam must fit the result set; this is the only per-query invariant)"
            )));
        }
        let Some(mut ep) = self.entry_point else {
            return Ok(Vec::new()); // empty graph
        };
        if k == 0 {
            return Ok(Vec::new());
        }
        // Greedy descent (ef = 1) through the upper layers, then a beam
        // search of width ef on level 0 (Algorithm 5).
        for l in (1..=self.max_level).rev() {
            ep = self.search_layer(query, &[ep], 1, l)[0].id;
        }
        let w = self.search_layer(query, &[ep], ef, 0);
        Ok(w.into_iter().take(k).map(|c| (c.id, c.dist)).collect())
    }

    /// Entry validation shared by `insert` and `search` (§5): exact
    /// dimension match, no non-finite components, and (cosine) no zero
    /// vector. Funnels through the metric's self-distance so the frozen
    /// distance-layer validation (`distance::validate_pair` + the cosine
    /// zero check) stays the single implementation of these rules.
    fn validate_entry_vector(&self, v: &[f32], what: &str) -> Result<()> {
        if v.len() != usize::from(self.dim) {
            return Err(HnswError::InvalidArgument(format!(
                "{what}: vector has {} components, graph dim is {} (§3: same graph never mixes dimensions)",
                v.len(),
                self.dim
            )));
        }
        self.metric.distance(v, v).map(|_| ())
    }

    /// Distance between the query slice and a stored node. Only called with
    /// validated vectors, so the distance function cannot fail (dimension
    /// match is an arena invariant; finiteness and cosine non-zeroness were
    /// checked at the entry points).
    fn dist_to_query(&self, query: &[f32], node: NodeId) -> f64 {
        self.metric
            .distance(query, self.vector(node))
            .expect("graph-internal distances are over entry-validated vectors (§5)")
    }

    /// Distance between two stored nodes (heuristic occlusion checks and
    /// shrink-side candidate distances).
    fn dist_between(&self, a: NodeId, b: NodeId) -> f64 {
        self.metric
            .distance(self.vector(a), self.vector(b))
            .expect("graph-internal distances are over entry-validated vectors (§5)")
    }

    /// Per-level out-edge cap: `M_max0` on level 0, `M` elsewhere (§4.2).
    fn m_max(&self, level: u8) -> usize {
        if level == 0 {
            usize::from(self.params.m_max0())
        } else {
            usize::from(self.params.m())
        }
    }

    /// Paper Algorithm 2 (SEARCH-LAYER): beam search on one level.
    ///
    /// `entry_points` must be nodes present on `level`; `ef >= 1`. Returns
    /// up to `ef` nearest visited elements sorted ascending by `(distance,
    /// NodeId)`. The visited set is a bitset indexed by `NodeId` (§4.1
    /// prerequisite ②: no hash-iteration order may enter the algorithm).
    ///
    /// The break condition compares **distances only** (paper semantics:
    /// stop when the nearest unprocessed candidate is farther than the
    /// worst retained element); every ordering that influences the *output*
    /// goes through [`Cand`]'s total order, so ties are deterministic.
    fn search_layer(
        &self,
        query: &[f32],
        entry_points: &[NodeId],
        ef: usize,
        level: u8,
    ) -> Vec<Cand> {
        debug_assert!(ef >= 1, "beam width must be at least 1");
        let n = self.node_count();
        let mut visited = vec![0u64; n / 64 + 1];
        let mut candidates: BinaryHeap<Reverse<Cand>> = BinaryHeap::new();
        let mut results: BinaryHeap<Cand> = BinaryHeap::new();
        for &ep in entry_points {
            visited[ep.index() / 64] |= 1u64 << (ep.index() % 64);
            let c = Cand {
                dist: self.dist_to_query(query, ep),
                id: ep,
            };
            candidates.push(Reverse(c));
            results.push(c);
        }
        while let Some(Reverse(c)) = candidates.pop() {
            let worst = results.peek().copied().expect("results never empties");
            if c.dist > worst.dist {
                break; // nearest candidate is beyond the worst kept result
            }
            for &e in &self.adjacency[c.id.index()][usize::from(level)] {
                let (word, bit) = (e.index() / 64, 1u64 << (e.index() % 64));
                if visited[word] & bit != 0 {
                    continue;
                }
                visited[word] |= bit;
                let cand = Cand {
                    dist: self.dist_to_query(query, e),
                    id: e,
                };
                // Admission compares the FULL frozen order (distance, NodeId
                // ascending): at a full beam an equidistant candidate with a
                // smaller NodeId displaces the larger-id worst, so the
                // result set is the ef-best under the total order — not
                // whatever the discovery order happened to seat first
                // (2026-08-31 review P1). The distance-only *break*
                // condition above is the paper's early termination and
                // stays distance-only.
                if results.len() < ef || cand < *results.peek().expect("non-empty") {
                    candidates.push(Reverse(cand));
                    results.push(cand);
                    if results.len() > ef {
                        results.pop(); // evict the worst
                    }
                }
            }
        }
        // into_sorted_vec yields ascending (distance, NodeId) — the frozen
        // output order.
        results.into_sorted_vec()
    }

    /// Neighbor selection — **the single function behind both sides of §4.3**
    /// (hard constraint "select and shrink share one heuristic"):
    ///
    /// - select side (Algorithm 1): `candidates` are the beam-search results
    ///   for the inserted vector, `limit` is `M`;
    /// - shrink side: `candidates` are the over-capacity list of an existing
    ///   node *with distances recomputed against that node's own vector*,
    ///   `limit` is `M_max(level)`.
    ///
    /// The input role is carried entirely by `Cand::dist` = "distance to the
    /// reference vector of this call" — the function itself never asks which
    /// side it is serving. `candidates` is sorted internally, so callers
    /// cannot supply a non-canonical order.
    ///
    /// Heuristic mode = paper Algorithm 4 with `extend_candidates = false`
    /// (all levels, §4.3 v1.3) and `keep_pruned = true`: a candidate is
    /// occluded (discarded) iff some already-kept element is strictly closer
    /// to it than the reference vector is; occluded candidates then refill
    /// the result in ascending-distance order up to `limit` (connectivity
    /// first — full `M` edges per node). Simple mode = Algorithm 3 (nearest
    /// `limit`), the §4.3 A/B control group. Both return the chosen ids
    /// sorted by `NodeId` ascending (canonical adjacency order).
    fn select_neighbors(&self, candidates: &[Cand], limit: usize) -> Vec<NodeId> {
        let mut sorted: Vec<Cand> = candidates.to_vec();
        sorted.sort();
        if sorted.len() <= limit {
            let mut ids: Vec<NodeId> = sorted.iter().map(|c| c.id).collect();
            ids.sort();
            return ids;
        }
        let chosen: Vec<Cand> = match self.selection {
            NeighborSelection::Simple => sorted[..limit].to_vec(),
            NeighborSelection::Heuristic => {
                let mut kept: Vec<Cand> = Vec::with_capacity(limit);
                let mut occluded: Vec<Cand> = Vec::new();
                for &c in &sorted {
                    if kept.len() == limit {
                        break; // Algorithm 4's main loop stops at |R| = M
                    }
                    // "closer to q than to any element of R": occlusion uses
                    // a strict comparison — a tie keeps the candidate
                    // (deterministic either way; R is scanned exhaustively).
                    let shadowed = kept.iter().any(|&r| self.dist_between(c.id, r.id) < c.dist);
                    if shadowed {
                        occluded.push(c); // ascending order is preserved
                    } else {
                        kept.push(c);
                    }
                }
                // keep_pruned = true: refill from the occluded candidates in
                // ascending-distance order up to the limit (§4.3).
                let mut out = kept;
                for &c in &occluded {
                    if out.len() == limit {
                        break;
                    }
                    out.push(c);
                }
                out
            }
        };
        let mut ids: Vec<NodeId> = chosen.iter().map(|c| c.id).collect();
        ids.sort();
        ids
    }

    /// Insert `to` into `node`'s level-`level` adjacency list, keeping the
    /// canonical `NodeId`-ascending order. `to` must not already be present
    /// (a fresh node cannot be reachable from its own insertion search).
    fn push_edge(&mut self, node: NodeId, level: u8, to: NodeId) {
        let list = &mut self.adjacency[node.index()][usize::from(level)];
        match list.binary_search(&to) {
            Ok(_) => debug_assert!(false, "duplicate edge {node:?} -> {to:?} on level {level}"),
            Err(pos) => list.insert(pos, to),
        }
    }

    /// Shrink an over-capacity adjacency list back to `M_max(level)` by
    /// re-running the *same* selection heuristic on it (§4.3 "both sides"):
    /// the list's owner plays the reference-vector role, so candidate
    /// distances are recomputed against the owner before the call.
    fn shrink(&mut self, owner: NodeId, level: u8) {
        let list = &self.adjacency[owner.index()][usize::from(level)];
        let cands: Vec<Cand> = list
            .iter()
            .map(|&c| Cand {
                dist: self.dist_between(owner, c),
                id: c,
            })
            .collect();
        let kept = self.select_neighbors(&cands, self.m_max(level));
        self.adjacency[owner.index()][usize::from(level)] = kept;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Full adjacency dump, ids as raw u32: `dump()[i][l]` = node `i`'s
    /// neighbors on level `l`. The step-by-step tests assert this whole
    /// vector after every single insert.
    fn dump(g: &Hnsw) -> Vec<Vec<Vec<u32>>> {
        (0..g.node_count())
            .map(|i| {
                (0..=g.level(NodeId(i as u32)))
                    .map(|l| {
                        g.neighbors(NodeId(i as u32), l)
                            .iter()
                            .map(|n| n.0)
                            .collect::<Vec<u32>>()
                    })
                    .collect()
            })
            .collect()
    }

    fn line_graph(seed: u64) -> Hnsw {
        // dim=1 L2 playground: distances are squared position differences,
        // hand-computable to the last ulp (integer positions).
        Hnsw::new(1, Metric::L2, HnswParams::new(2, 2, 4, 2).unwrap(), seed).unwrap()
    }

    #[test]
    fn rejects_dim_zero_graph() {
        assert!(matches!(
            Hnsw::new(0, Metric::L2, HnswParams::default(), 1),
            Err(HnswError::InvalidArgument(_))
        ));
    }

    #[test]
    fn first_insert_becomes_entry_point() {
        let mut g = line_graph(1);
        assert_eq!(g.entry_point(), None);
        assert_eq!(g.max_level(), 0); // §3 empty-graph invariant
        let id = g.insert(&[3.0]).unwrap();
        assert_eq!(id, NodeId(0));
        assert_eq!(g.entry_point(), Some(NodeId(0)));
        assert_eq!(g.max_level(), g.level(NodeId(0)));
        assert_eq!(g.vector(NodeId(0)), &[3.0]);
        let expected: Vec<Vec<Vec<u32>>> = vec![vec![vec![]]];
        assert_eq!(dump(&g), expected);
    }

    #[test]
    fn insert_validates_vectors_loudly() {
        let mut g = line_graph(1);
        assert!(matches!(
            g.insert(&[1.0, 2.0]),
            Err(HnswError::InvalidArgument(_)) // dimension mismatch
        ));
        assert!(matches!(
            g.insert(&[f32::NAN]),
            Err(HnswError::InvalidArgument(_))
        ));
        assert!(matches!(
            g.insert(&[f32::INFINITY]),
            Err(HnswError::InvalidArgument(_))
        ));
        assert_eq!(g.node_count(), 0, "rejected inserts must not allocate ids");
        // Cosine zero vector: loud even as the very first node (no pairwise
        // distance would otherwise be computed).
        let mut gc = Hnsw::new(2, Metric::Cosine, HnswParams::default(), 1).unwrap();
        assert!(matches!(gc.insert(&[0.0, 0.0]), Err(HnswError::ZeroVector)));
        assert!(gc.insert(&[1.0, 0.0]).is_ok());
        assert!(matches!(gc.insert(&[0.0, 0.0]), Err(HnswError::ZeroVector)));
    }

    #[test]
    fn search_validates_ef_k_and_query() {
        let mut g = line_graph(1);
        assert_eq!(g.search(&[0.0], 1, None).unwrap(), vec![]); // empty graph
        g.insert(&[0.0]).unwrap();
        g.insert(&[2.0]).unwrap();
        assert!(matches!(
            g.search(&[0.0], 3, Some(2)),
            Err(HnswError::InvalidArgument(_)) // ef < k
        ));
        // per-query ef may go below M (§4.4 disambiguation): only ef >= k binds
        assert_eq!(g.search(&[0.0], 1, Some(1)).unwrap().len(), 1);
        assert_eq!(g.search(&[0.0], 0, Some(0)).unwrap(), vec![]); // k = 0
        assert!(matches!(
            g.search(&[0.0, 1.0], 1, None),
            Err(HnswError::InvalidArgument(_)) // query dim mismatch
        ));
        assert!(matches!(
            g.search(&[f32::NAN], 1, None),
            Err(HnswError::InvalidArgument(_))
        ));
        let gc = Hnsw::new(2, Metric::Cosine, HnswParams::default(), 1).unwrap();
        assert!(matches!(
            gc.search(&[0.0, 0.0], 1, None),
            Err(HnswError::ZeroVector) // zero query rejected even on an empty graph
        ));
    }

    #[test]
    fn full_beam_tie_displaces_by_node_id() {
        // 2026-08-31 review P1: at a full beam, admission compares the full
        // (distance, NodeId ascending) order — an equidistant candidate with
        // a smaller NodeId displaces the larger-id worst. Hand-built graph
        // (struct literal, in-module): query at 0.0; node 0 (p=2) and node 1
        // (p=−2) are equidistant (d=4); the search starts at node 1, so
        // node 0 is the late-discovered smaller-id tie. Pre-fix the
        // distance-only admission kept node 1; post-fix node 0 displaces it.
        let g = Hnsw {
            dim: 1,
            metric: Metric::L2,
            params: HnswParams::new(2, 2, 4, 2).unwrap(),
            selection: NeighborSelection::Heuristic,
            rng: Xoshiro256StarStar::new(1),
            vectors: vec![2.0, -2.0],
            levels: vec![0, 0],
            adjacency: vec![vec![vec![NodeId(1)]], vec![vec![NodeId(0)]]],
            entry_point: Some(NodeId(1)),
            max_level: 0,
        };
        let w = g.search_layer(&[0.0], &[NodeId(1)], 1, 0);
        assert_eq!(
            w,
            vec![Cand {
                dist: 4.0,
                id: NodeId(0)
            }]
        );
        // Same through the public API (max_level = 0 => no descent, ef = 1).
        assert_eq!(
            g.search(&[0.0], 1, Some(1)).unwrap(),
            vec![(NodeId(0), 4.0)]
        );
        // The distance-only BREAK condition is untouched: a strictly farther
        // candidate still terminates the walk even with a smaller NodeId.
        let w2 = g.search_layer(&[10.0], &[NodeId(0)], 1, 0);
        assert_eq!(
            w2,
            vec![Cand {
                dist: 64.0,
                id: NodeId(0)
            }]
        );
    }

    #[test]
    fn ties_break_by_node_id_ascending() {
        // query at 0.0; nodes at -1.0 and +1.0 are equidistant (dist 1).
        let mut g = line_graph(7);
        g.insert(&[1.0]).unwrap(); // NodeId(0)
        g.insert(&[-1.0]).unwrap(); // NodeId(1)
        let hits = g.search(&[0.0], 2, Some(2)).unwrap();
        assert_eq!(hits, vec![(NodeId(0), 1.0), (NodeId(1), 1.0)]);
    }

    #[test]
    fn same_seed_same_graph_byte_level_structure() {
        let build = |seed: u64| {
            let mut g = line_graph(seed);
            for i in 0..32 {
                g.insert(&[(i * 7 % 13) as f32]).unwrap();
            }
            g
        };
        let a = build(99);
        let b = build(99);
        assert_eq!(dump(&a), dump(&b), "§4.1: same seed + same sequence");
        assert_eq!(a.entry_point(), b.entry_point());
        let c = build(100);
        assert_ne!(
            dump(&a),
            dump(&c),
            "test premise: seeds 99/100 must draw different levels"
        );
    }

    #[test]
    fn flood_search_equals_brute_force_on_small_graph() {
        // §8.3's correctness oracle, small-scale smoke (the full multi-dim /
        // multi-N A/B is B2's property suite): with ef = node_count the beam
        // never evicts, so search floods the level-0 connected component and
        // must equal a brute-force scan — *provided* level-0 connectivity.
        // The diagnostic order (§8.3) is: check connectivity first, then
        // distances. Both halves are asserted here, in that order.
        //
        // Params are the frozen defaults (M=16, M_max0=32, ef_c=200): the
        // §9 ① connectivity premise holds there. (At pathological params
        // like M=2/M_max0=2/ef_c=4 the shrink side can sever the last
        // bridging edge and the level-0 graph genuinely disconnects — a
        // parameter-regime fact, not an algorithm bug; B2's property suite
        // owns the multi-parameter matrix.)
        let mut g = Hnsw::new(1, Metric::L2, HnswParams::default(), 5).unwrap();
        for i in 0..200u32 {
            g.insert(&[(i * 37 % 211) as f32]).unwrap();
        }
        // premise: every node reachable from the entry point on level 0
        // (BFS over the read-only accessors)
        let mut seen = vec![false; g.node_count()];
        let mut stack = vec![g.entry_point().unwrap()];
        while let Some(n) = stack.pop() {
            if seen[n.index()] {
                continue;
            }
            seen[n.index()] = true;
            stack.extend_from_slice(g.neighbors(n, 0));
        }
        assert!(
            seen.iter().all(|&s| s),
            "§9 ① premise failed: level-0 graph is not entry-connected"
        );
        // oracle: ef = node_count flood == brute force, exact ordering
        let q = [100.0f32];
        let n = g.node_count();
        let hits = g.search(&q, n, Some(n)).unwrap();
        let mut brute: Vec<(NodeId, f64)> = (0..n)
            .map(|i| {
                (
                    NodeId(i as u32),
                    distance::l2_squared(&q, g.vector(NodeId(i as u32))).unwrap(),
                )
            })
            .collect();
        brute.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap().then(a.0.cmp(&b.0)));
        assert_eq!(hits, brute);
    }

    #[test]
    fn adjacency_caps_hold_under_churn() {
        // Shrink evidence: after 300 inserts at M=2/M_max0=2 no list exceeds
        // its per-level cap (§4.2), and every list stays NodeId-sorted.
        let mut g = line_graph(11);
        for i in 0..300 {
            g.insert(&[(i * 29 % 101) as f32]).unwrap();
        }
        for i in 0..g.node_count() {
            let id = NodeId(i as u32);
            for l in 0..=g.level(id) {
                let ns = g.neighbors(id, l);
                // M = M_max0 = 2 here, so both per-level caps (§4.2) are 2
                let cap = 2;
                assert!(ns.len() <= cap, "node {i} level {l}: {} > {cap}", ns.len());
                assert!(ns.windows(2).all(|w| w[0] < w[1]), "canonical order");
                assert!(!ns.contains(&id), "no self-loop");
            }
        }
    }

    #[test]
    fn simple_selection_mode_builds_valid_graphs() {
        // §4.3 A/B control group smoke test (recall data collection is Stage
        // D's job). Asserted here: the structural invariants that do not
        // depend on the selection mode — caps, canonical order, no self-loop,
        // no empty list, determinism, and a working search. Connectivity is
        // deliberately NOT asserted: at these aggressive params
        // (M=2/M_max0=2) shrink can sever bridging edges under either mode
        // (see flood_search_equals_brute_force_on_small_graph).
        let build = || {
            let mut g = Hnsw::new_with_neighbor_selection(
                1,
                Metric::L2,
                HnswParams::new(2, 2, 4, 2).unwrap(),
                5,
                NeighborSelection::Simple,
            )
            .unwrap();
            for i in 0..64 {
                g.insert(&[(i * 13 % 29) as f32]).unwrap();
            }
            g
        };
        let g = build();
        assert_eq!(
            dump(&g),
            dump(&build()),
            "control path is deterministic too"
        );
        for i in 0..g.node_count() {
            let id = NodeId(i as u32);
            let ns = g.neighbors(id, 0);
            assert!(ns.len() <= 2, "node {i}: simple-mode cap violated");
            assert!(
                !ns.is_empty(),
                "node {i}: pinned-seed expectation — every node happened to be \
                 selected at least once (NOT an invariant: the first node could \
                 in principle never be selected and keep an empty list)"
            );
            assert!(!ns.contains(&id), "node {i}: self-loop");
        }
        // search works and respects its own ordering contract (hit count is
        // bounded by the reachable component, which shrink may legitimately
        // shrink at these params — no connectivity assertion here)
        let hits = g.search(&[7.0], 5, Some(16)).unwrap();
        assert!(!hits.is_empty());
        assert!(hits
            .windows(2)
            .all(|w| { w[0].1 < w[1].1 || (w[0].1 == w[1].1 && w[0].0 < w[1].0) }));
    }

    #[test]
    fn multi_level_entry_point_tracking() {
        // The highest node owns the entry point; a strictly higher insert
        // replaces it, an equal/lower one does not (Algorithm 1).
        let mut g = line_graph(3);
        let mut expected_max = 0u8;
        let mut expected_entry: Option<NodeId> = None;
        for i in 0..50 {
            let id = g.insert(&[(i * 7 % 11) as f32]).unwrap();
            let l = g.level(id);
            if i == 0 || l > expected_max {
                expected_max = l;
                expected_entry = Some(id);
                assert_eq!(g.entry_point(), Some(id));
                assert_eq!(g.max_level(), l);
            } else {
                assert_eq!(g.max_level(), expected_max);
                assert_eq!(
                    g.entry_point(),
                    expected_entry,
                    "an equal/lower insert must not replace the entry point"
                );
            }
        }
    }

    // ------------------------------------------------------------------
    // Known small graph, hand-derived step by step (coding plan Stage B:
    // "each step's adjacency checked against a paper hand-derivation").
    //
    // Setup: dim=1, L2 (distances = squared position differences, exact),
    // M = 2, M_max0 = 2, ef_construction = 4, seed = 664.
    //
    // Level draws (rng.rs §4.1, pinned by rng's own known-answer tests;
    // M = 2 => m_L = 1/ln 2): the first eight draws for seed 664 are
    //   node:   0 1 2 3 4 5 6 7
    //   level:  0 0 1 0 0 1 0 0
    // (verified against the crate's Xoshiro256StarStar — the levels are an
    // input to the derivation below, not something the graph code may
    // influence.)
    //
    // Insertion order and positions on the line:
    //   node 0: p=0.0   node 1: p=4.0   node 2: p=2.0   node 3: p=1.0
    //   node 4: p=3.0   node 5: p=5.0   node 6: p=6.0   node 7: p=2.0 (dup)
    //
    // Notation: d(a,b) = (p_a − p_b)². All heaps order by (dist, NodeId asc).
    // ef_construction = 4 covers every pre-existing node through step 4, so
    // the beam floods all reachable nodes and the candidate sets below are
    // exact. From step 5 on (5+ pre-existing nodes) |W| is capped at ef = 4:
    // the flood premise no longer holds, but the candidate sets stay exact
    // (the exact top-4 — at step 5 node 0 is visited yet refused admission,
    // and step 6's beam caps harder still).
    //
    // ── step 0: insert node 0 (p=0, level 0)
    //    Empty graph => entry = 0, max_level = 0. Adjacency: 0: [[]].
    //
    // ── step 1: insert node 1 (p=4, level 0)
    //    Level 0 search from ep=0: candidates {(16, 0)}. |W| = 1 ≤ M = 2
    //    => select {0}. Edge 1↔0. Lists: 0:[1], 1:[0]. No list over cap 2.
    //
    // ── step 2: insert node 2 (p=2, level 1)
    //    level 1 > max_level 0 => no descent; shared levels: 0 only.
    //    Level 0 search from ep=0 floods {0,1}: candidates to p=2:
    //    (d=4, 0), (d=4, 1) — tie broken by NodeId => [(4,0), (4,1)].
    //    Heuristic, limit M=2: keep 0 (R empty). c=(4,1): d(1,0) = 16 > 4
    //    => not occluded => keep 1. R = {0,1} (full, loop stops).
    //    Edges 2↔0, 2↔1. List of 0: [1,2] (cap 2 OK). List of 1: [0,2] OK.
    //    level 1 > max_level => entry = 2, max_level = 1.
    //    Level-1 list of node 2: [] (no other node reaches level 1 yet).
    //
    // ── step 3: insert node 3 (p=1, level 0)
    //    Descent: levels (0+1..=1) = level 1, greedy from ep=2: node 2 is
    //    alone on level 1 => ep stays 2.
    //    Level 0 search from 2 floods {0,1,2}: distances to p=1:
    //    d(3→0)=1, d(3→1)=9, d(3→2)=1 => candidates [(1,0), (1,2), (9,1)].
    //    Heuristic, limit 2: keep 0. c=(1,2): d(2,0) = 4 > 1 => keep 2.
    //    R = {0,2}. Edges 3↔0, 3↔2.
    //    List of 0: [1,2] + 3 = [1,2,3] > M_max0 = 2 => SHRINK node 0.
    //      Candidates with distances to node 0 (p=0): (1,1) [p=4: d=16 —
    //      careful: to node 1 d=16, to node 2 d=4, to node 3 d=1] =>
    //      sorted: [(1,3), (4,2), (16,1)].
    //      Heuristic limit 2: keep 3. c=(4,2): d(2,3) = 1 < 4 => OCCLUDED.
    //      c=(16,1): d(1,3) = 9 < 16 => OCCLUDED. R = {3}, refill by
    //      ascending distance: +2 => R = {3,2} => sorted ids [2,3].
    //      Node 0's list becomes [2,3]. (The heuristic threw away node 1 —
    //      the "long edge" d=16 — because node 3 occludes everything; this
    //      is exactly Algorithm 4's navigability trade-off, and with
    //      keep_pruned the list still refills to the cap.)
    //    List of 2: [0,1] + 3 = [0,1,3] > 2 => SHRINK node 2 (p=2):
    //      candidates: d(2→0)=4, d(2→1)=4, d(2→3)=1 => [(1,3), (4,0), (4,1)].
    //      keep 3. c=(4,0): d(0,3) = 1 < 4 => OCCLUDED. c=(4,1): d(1,3) = 9
    //      > 4 => keep 1. R = {3,1} => sorted [1,3]. Node 2's list: [1,3].
    //    Graph after step 3:
    //      0: [2,3]   1: [0,2]   2: [1,3]   3: [0,2]   (level 0)
    //      2: [] (level 1); entry = 2, max_level = 1.
    //
    // ── step 4: insert node 4 (p=3, level 0)
    //    Descent level 1: still only node 2 => ep = 2.
    //    Level 0 from 2 floods {0,1,2,3}: distances to p=3:
    //    d(0)=9, d(1)=1, d(2)=1, d(3)=4 => [(1,1), (1,2), (4,3), (9,0)].
    //    Heuristic limit 2: keep 1. c=(1,2): d(2,1) = 4 > 1 => keep 2.
    //    R = {1,2}. Edges 4↔1, 4↔2.
    //    List of 1: [0,2] + 4 = [0,2,4] > 2 => SHRINK node 1 (p=4):
    //      d(1→0)=16, d(1→2)=4, d(1→4)=1 => [(1,4), (4,2), (16,0)].
    //      keep 4. c=(4,2): d(2,4) = 1 < 4 => OCCLUDED. c=(16,0): d(0,4)=9
    //      < 16 => OCCLUDED. Refill: +2 => R = {4,2} => [2,4].
    //    List of 2: [1,3] + 4 = [1,3,4] > 2 => SHRINK node 2 (p=2):
    //      d(2→1)=4, d(2→3)=1, d(2→4)=1 => [(1,3), (1,4), (4,1)].
    //      keep 3. c=(1,4): d(4,3) = 4, NOT < 1 => not occluded => keep 4.
    //      R = {3,4} => [3,4]. (4 < 1 is false under either comparison rule,
    //      so this step does NOT discriminate strict-vs-non-strict occlusion;
    //      the strict-< rule is pinned by the genuine ties in step 7:
    //      d(3,2) = 1 vs c.dist = 1 and d(3,7) = 1 vs 1.)
    //    Graph after step 4:
    //      0: [2,3]  1: [2,4]  2: [3,4]  3: [0,2]  4: [1,2]   (level 0)
    //
    // ── step 5: insert node 5 (p=5, level 1)
    //    level 1 == max_level 1 => descent loop empty. Shared levels: 1, 0.
    //    Level 1 search from ep=2 (ef_c=4): node 2 alone => candidates
    //    [(9,2)] (d(2,5) = 9). |W| = 1 ≤ M => select {2}. Edge 5↔2 at
    //    level 1. Node 2's level-1 list: [5] ≤ cap M=2, no shrink.
    //    ep for the next level = nearest result = 2.
    //    Level 0 from 2 (ef=4 < 5 pre-existing nodes — the first capped
    //    beam): visits all of {0..4} but node 0 (d=25) is refused admission
    //    (|W| = ef and 25 NOT < 16). W = exact top-4:
    //    [(1,1), (4,4), (9,2), (16,3)].
    //    Heuristic limit 2: keep 1. c=(4,4): d(4,1) = 1 < 4 => OCCLUDED.
    //    c=(9,2): d(2,1) = 4 < 9 => OCCLUDED. c=(16,3): d(3,1) = 9 < 16
    //    => OCCLUDED. R = {1}, keep_pruned refill ascending: +4
    //    => R = {1,4}. Edges 5↔1, 5↔4.
    //    List of 1: [2,4] + 5 = [2,4,5] > 2 => SHRINK node 1 (p=4):
    //      d(1→2)=4, d(1→4)=1, d(1→5)=1 => [(1,4), (1,5), (4,2)].
    //      keep 4. c=(1,5): d(5,4) = 4 > 1... careful: occlusion compares
    //      d(candidate, kept) vs d(candidate, owner): d(5,4) = (5−3)² = 4,
    //      c.dist = d(5,1) = 1 => 4 > 1 => not occluded => keep 5.
    //      R = {4,5} => [4,5].
    //    List of 4: [1,2] + 5 = [1,2,5] > 2 => SHRINK node 4 (p=3):
    //      d(4→1)=1, d(4→2)=1, d(4→5)=4 => [(1,1), (1,2), (4,5)].
    //      keep 1. c=(1,2): d(2,1) = 4 > 1 => keep 2. R = {1,2} => [1,2].
    //    level 1 == max_level, NOT > => entry stays 2.
    //    Graph after step 5 (level 0 / level 1):
    //      0: [2,3]      1: [4,5]    2: [3,4]    3: [0,2]
    //      4: [1,2]      5: [1,4]
    //      level 1: 2: [5], 5: [2]. entry = 2, max_level = 1.
    //
    // ── step 6: insert node 6 (p=6, level 0)
    //    Descent level 1 from ep=2 (greedy ef=1): neighbors of 2 at level 1:
    //    {5}; d(5→6) = 1 < d(2→6) = 16 => ep becomes 5.
    //    Level 0 from 5, ef=4 < 6 nodes: the beam cap binds even harder
    //    than at step 5 (where it first bound); derived via Algorithm 2 (distances to p=6:
    //    d(0)=36, d(1)=4, d(2)=16, d(3)=25, d(4)=9, d(5)=1):
    //      C={(1,5)}, W={(1,5)}. Pop (1,5); worst (1,5); 1 > 1 false.
    //        neighbors of 5 @L0: [1,4]: d(1)=4, d(4)=9, |W| < 4 admits both
    //        => C={(4,1),(9,4)}, W={(1,5),(4,1),(9,4)}.
    //      Pop (4,1); worst (9,4); 4 > 9 false. neighbors of 1: [4,5],
    //        both visited.
    //      Pop (9,4); worst (9,4); 9 > 9 false. neighbors of 4: [1,2]:
    //        1 visited; d(2)=16, |W|=3 < 4 admits => C={(16,2)},
    //        W={(1,5),(4,1),(9,4),(16,2)} (|W| = ef, no eviction).
    //      Pop (16,2); worst (16,2); 16 > 16 false. neighbors of 2: [3,4]:
    //        4 visited; d(3)=25, |W| = ef and 25 NOT < 16 => rejected.
    //        C empty => done.
    //      W ascending: [(1,5),(4,1),(9,4),(16,2)].
    //    Heuristic limit 2: keep 5 (R empty). c=(4,1): d(1,5) = (4−5)² = 1
    //    < 4 => OCCLUDED. c=(9,4): d(4,5) = (3−5)² = 4 < 9 => OCCLUDED.
    //    c=(16,2): d(2,5) = 9 < 16 => OCCLUDED. R = {5}, keep_pruned refill
    //    by ascending distance: +1 => R = {5,1} => sorted [1,5].
    //    Edges 6↔1, 6↔5.
    //    List of 1: [4,5] + 6 = [4,5,6] > 2 => SHRINK node 1 (p=4):
    //      d(1→4)=1, d(1→5)=1, d(1→6)=4 => [(1,4),(1,5),(4,6)].
    //      keep 4. c=(1,5): d(5,4) = (3−5)² = 4 > 1 => not occluded =>
    //      keep 5. R = {4,5} => [4,5] (list content unchanged).
    //    List of 5: [1,4] + 6 = [1,4,6] > 2 => SHRINK node 5 (p=5):
    //      d(5→1)=1, d(5→4)=4, d(5→6)=1 => [(1,1),(1,6),(4,4)].
    //      keep 1. c=(1,6): d(6,1) = (6−4)² = 4 > 1 => keep 6.
    //      R = {1,6} => [1,6].
    //    Graph after step 6 (level 0):
    //      0: [2,3]  1: [4,5]  2: [3,4]  3: [0,2]  4: [1,2]  5: [1,6]  6: [1,5]
    //      level 1: 2: [5], 5: [2]. entry = 2, max_level = 1.
    //      (Reachability check: 2→4→1→5→6 still spans everything from the
    //      entry point; the asymmetry 4→2 vs 2→4 is what step 7 breaks.)
    //
    // ── step 7: insert node 7 (p=2, level 0) — duplicate position of node 2
    //    (§1: no dedup inside the index — duplicates are independent nodes).
    //    Descent level 1 from 2: d(5→2) = 9 > d(2→2) = 0 => ep stays 2.
    //    Level 0 from 2, ef=4 (Algorithm 2):
    //      C={(0,2)}, W={(0,2)}. Pop (0,2); worst (0,2). neighbors [3,4]:
    //        d(3)=1, d(4)=1 => C={(1,3),(1,4)}, W={(0,2),(1,3),(1,4)}.
    //      Pop (1,3); worst (1,4). neighbors of 3: [0,2]: d(0)=4, |W|<4 =>
    //        C={(1,4),(4,0)}, W += (4,0) = 4 elements.
    //      Pop (1,4); worst (4,0). neighbors of 4: [1,2]: d(1)=4, NOT < 4
    //        and |W|=ef => rejected. 2 visited.
    //      Pop (4,0); worst (4,0): 4 > 4 false => continue. neighbors of 0:
    //        [2,3] both visited. C empty => done.
    //      W ascending: [(0,2),(1,3),(1,4),(4,0)].)
    //    Heuristic limit 2: keep 2 (d=0). c=(1,3): d(3,2) = 1, NOT < 1 (tie)
    //    => keep 3. R = {2,3} full. Edges 7↔2, 7↔3.
    //    List of 2: [3,4] + 7 = [3,4,7] > 2 => SHRINK node 2 (p=2):
    //      d(2→3)=1, d(2→4)=1, d(2→7)=0 => [(0,7),(1,3),(1,4)].
    //      keep 7. c=(1,3): d(3,7) = 1, NOT < 1 => keep 3. R = {7,3} => [3,7].
    //    List of 3: [0,2] + 7 = [0,2,7] > 2 => SHRINK node 3 (p=1):
    //      d(3→0)=1, d(3→2)=1, d(3→7)=1 => [(1,0),(1,2),(1,7)].
    //      keep 0. c=(1,2): d(2,0) = 4 > 1 => keep 2. R = {0,2} => [0,2].
    //      (node 7 was never evaluated — the loop stopped at |R| = 2; had
    //      it been reached it would have been occluded by node 2:
    //      d(7,2) = 0 < 1, not by node 0: d(7,0) = 4 > 1.)
    //    Graph after step 7 (level 0):
    //      0: [2,3]  1: [4,5]  2: [3,7]  3: [0,2]  4: [1,2]  5: [1,6]
    //      6: [1,5]  7: [2,3]
    //      level 1: 2: [5], 5: [2]. entry = 2, max_level = 1.
    //      REACHABILITY NOTE: node 2's shrink just dropped the 2→4 edge
    //      while 4→2 survives — the level-0 graph stays ONE weak component
    //      (4−2 undirected), but the directed component reachable from the
    //      entry point is now only {0,2,3,7}. This is the M=2/M_max0=2
    //      parameter regime, not an algorithm bug; the search-consistency
    //      test below restricts its brute-force oracle to the reachable
    //      component exactly as §8.3's diagnostic path prescribes
    //      (connectivity first, distances second).
    // ------------------------------------------------------------------

    /// Expected full adjacency after each insert of the hand-derived trace
    /// (level-0 lists first; multi-level nodes carry their level-1 list as a
    /// second entry). See the derivation block above.
    fn hand_trace_expectations() -> Vec<Vec<Vec<Vec<u32>>>> {
        vec![
            // step 0
            vec![vec![vec![]]],
            // step 1
            vec![vec![vec![1]], vec![vec![0]]],
            // step 2 (node 2 has level 1: second inner vec is its L1 list)
            vec![vec![vec![1, 2]], vec![vec![0, 2]], vec![vec![0, 1], vec![]]],
            // step 3
            vec![
                vec![vec![2, 3]],
                vec![vec![0, 2]],
                vec![vec![1, 3], vec![]],
                vec![vec![0, 2]],
            ],
            // step 4
            vec![
                vec![vec![2, 3]],
                vec![vec![2, 4]],
                vec![vec![3, 4], vec![]],
                vec![vec![0, 2]],
                vec![vec![1, 2]],
            ],
            // step 5
            vec![
                vec![vec![2, 3]],
                vec![vec![4, 5]],
                vec![vec![3, 4], vec![5]],
                vec![vec![0, 2]],
                vec![vec![1, 2]],
                vec![vec![1, 4], vec![2]],
            ],
            // step 6
            vec![
                vec![vec![2, 3]],
                vec![vec![4, 5]],
                vec![vec![3, 4], vec![5]],
                vec![vec![0, 2]],
                vec![vec![1, 2]],
                vec![vec![1, 6], vec![2]],
                vec![vec![1, 5]],
            ],
            // step 7
            vec![
                vec![vec![2, 3]],
                vec![vec![4, 5]],
                vec![vec![3, 7], vec![5]],
                vec![vec![0, 2]],
                vec![vec![1, 2]],
                vec![vec![1, 6], vec![2]],
                vec![vec![1, 5]],
                vec![vec![2, 3]],
            ],
        ]
    }

    #[test]
    fn hand_derived_small_graph_step_by_step() {
        let positions = [0.0f32, 4.0, 2.0, 1.0, 3.0, 5.0, 6.0, 2.0];
        // The level draws this derivation depends on (see the comment block):
        // seed 664, M = 2 => levels [0,0,1,0,0,1,0,0]. Asserted explicitly so
        // an rng drift fails here with a clear message, not as graph noise.
        let expected_levels = [0u8, 0, 1, 0, 0, 1, 0, 0];
        let mut g = line_graph(664);
        let expected = hand_trace_expectations();
        for (step, &p) in positions.iter().enumerate() {
            let id = g.insert(&[p]).unwrap();
            assert_eq!(
                g.level(id),
                expected_levels[step],
                "step {step}: level draw drifted"
            );
            assert_eq!(
                dump(&g),
                expected[step],
                "step {step} (inserted p={p}): adjacency diverged from the hand derivation"
            );
        }
        // pinned structure facts of the final graph
        assert_eq!(g.entry_point(), Some(NodeId(2)));
        assert_eq!(g.max_level(), 1);
        assert_eq!(g.level(NodeId(2)), 1);
        assert_eq!(g.level(NodeId(5)), 1);
        for i in [0, 1, 3, 4, 6, 7] {
            assert_eq!(g.level(NodeId(i)), 0);
        }
    }

    #[test]
    fn hand_derived_graph_search_is_consistent() {
        // The hand-derived final graph, queried. The level-0 graph is ONE
        // weak component, but only {0,2,3,7} is reachable from the entry
        // point following directed edges (see the step-7 REACHABILITY NOTE).
        // §8.3's diagnostic order applies: the flood oracle is brute force
        // *restricted to the reachable component* — connectivity first,
        // distances second.
        let positions = [0.0f32, 4.0, 2.0, 1.0, 3.0, 5.0, 6.0, 2.0];
        let mut g = line_graph(664);
        for &p in &positions {
            g.insert(&[p]).unwrap();
        }
        // directed BFS from the entry point over the read-only accessors
        let mut seen = vec![false; g.node_count()];
        let mut stack = vec![g.entry_point().unwrap()];
        while let Some(n) = stack.pop() {
            if seen[n.index()] {
                continue;
            }
            seen[n.index()] = true;
            stack.extend_from_slice(g.neighbors(n, 0));
        }
        let reachable: Vec<u32> = (0..8).filter(|&i| seen[i as usize]).collect();
        assert_eq!(reachable, vec![0, 2, 3, 7], "hand-derived component");

        let q = [2.5f32];
        let hits = g.search(&q, 8, Some(8)).unwrap();
        let mut brute: Vec<(NodeId, f64)> = reachable
            .iter()
            .map(|&i| {
                (
                    NodeId(i),
                    distance::l2_squared(&q, &[positions[i as usize]]).unwrap(),
                )
            })
            .collect();
        brute.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap().then(a.0.cmp(&b.0)));
        assert_eq!(hits, brute, "flood == brute force on the component");
        // partial k: the top-3 slice of the same ordering
        let top3 = g.search(&q, 3, Some(8)).unwrap();
        assert_eq!(top3, brute[..3].to_vec());
    }
}
