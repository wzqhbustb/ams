//! pg_rust in-memory HNSW vector index — Phase 2 M4 (Stages A–C done).
//!
//! Stage A delivers the foundations every later stage builds on (coding plan
//! Stage A; tech-selection references per module):
//!
//! - [`params`]: the [`NodeId`] newtype (§3 stability contract: densely
//!   increasing, never reused, snapshot-round-trip stable) and
//!   [`HnswParams`] with construction-time validation (§4.2/§4.4).
//! - [`encoding`]: frozen snapshot byte-stream primitives (§3) — header and
//!   node-record codecs, the CRC32 prefix convention (§7), and the full
//!   load-validation checklist.
//! - [`distance`]: L2-squared / cosine / negative inner product, f32
//!   elements with an f64 accumulator in scalar loops (§5) — the
//!   non-reassociable f64 chain is the cross-platform determinism
//!   mechanism, so no fast-math-style optimizations are allowed here.
//! - [`rng`]: hand-written, explicitly-seeded xoshiro256** plus the
//!   geometric level draw (§4.1); no `rand` crate (§10).
//! - [`error`]: [`HnswError`] (thiserror, workspace convention).
//! - [`graph`]: the HNSW core — [`Hnsw`] with the §6 SoA layout, insert
//!   (paper Algorithm 1) with the §4.3 heuristic on both the select and the
//!   shrink side (Algorithm 4, one shared function), and greedy descent +
//!   level-0 beam search (Algorithms 2/5).
//! - [`snapshot`]: the `save`/`load` file API (§7, Stage C) — atomic file
//!   writes, the full load-validation checklist, and read-only
//!   snapshot-loaded graphs (continuation-insert semantics punted to
//!   tech-selection v1.6).
//! - [`dataset`]: fvecs/ivecs parsing and recall@k scoring (Stage D local
//!   toolchain) — shared by the recall probe and the CI hard gate.

#![warn(missing_docs)]
#![warn(rust_2018_idioms)]

pub mod dataset;
pub mod distance;
pub mod encoding;
pub mod error;
pub mod graph;
pub mod params;
pub mod rng;
pub mod snapshot;

pub use error::{HnswError, Result};
pub use graph::{Hnsw, Metric};
pub use params::{HnswParams, NodeId};
