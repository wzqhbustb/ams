//! Graph structure and the HNSW core algorithms — paper Algorithms 1/2/4/5
//! (Malkov & Yashunin, TPAMI 2018), tech-selection §4/§6.
//!
//! **Stage B deliverable** — Stage A only freezes the module boundary. What
//! lands here:
//!
//! - the SoA layout (§6): `vectors` contiguous arena, `levels`, per-node
//!   per-level adjacency, `entry_point`, `max_level`;
//! - insert (Algorithm 1) with the neighbor-selection heuristic on both the
//!   select and the shrink side (Algorithm 4, `extend_candidates = false` on
//!   all levels, `keep_pruned = true`, §4.3);
//! - greedy layer descent + layer-0 beam search (Algorithms 2/5, §4.4).
//!
//! Determinism prerequisites (§4.1) bind every line here: all sort keys are
//! `(distance, NodeId ascending)`; HashMap/HashSet iteration order is banned
//! from the algorithm path (visited = bitset or `BTreeSet`); the PRNG is one
//! explicitly-seeded instance per graph ([`crate::rng`]).
