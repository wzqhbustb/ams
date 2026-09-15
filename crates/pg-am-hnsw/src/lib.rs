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
//! - [`page`]: HNSW page types and the page-initialization chain (Phase 2
//!   M5 Stage 0) — node/directory/meta page headers and the post-image FPI
//!   durability anchor (`log_page_init`, tech-selection §8.1 step 1).
//! - [`redo`]: the redo-handler registry skeleton (Phase 2 M5 Stage 0 —
//!   empty; the seven handlers land in Stage C).
//! - `apply`: the seven physical application primitives (Phase 2 M5 Stage A,
//!   `pub(crate)`) — one per WAL record type, pure application, shared by
//!   redo and the normal write path (tech-selection §10.2 task 2).
//! - `validate`: the single-point validation funnel (Phase 2 M5 Stage A,
//!   `pub(crate)`) — the §10.1 frozen-checklist subset, same-page/same-record
//!   decidable items only (tech-selection §10.1/§10.2).

#![warn(missing_docs)]
#![warn(rust_2018_idioms)]

// Stage A (2026-09-14): apply/validate are pub(crate) with no non-test
// caller yet — the redo handlers and the normal write path (their intended
// consumers) land in Stage C, so today's only callers are the unit tests.
// Same discipline as tests/common/mod.rs's `#![allow(dead_code)]`.
#[allow(dead_code)]
pub(crate) mod apply;
pub mod dataset;
pub mod distance;
pub mod encoding;
pub mod error;
pub mod graph;
pub mod page;
pub mod params;
pub mod redo;
pub mod rng;
pub mod snapshot;
#[allow(dead_code)]
pub(crate) mod validate;

pub use error::{HnswError, Result};
pub use graph::{Hnsw, Metric};
pub use params::{HnswParams, NodeId};
