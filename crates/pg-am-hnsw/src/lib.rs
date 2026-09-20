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
//! - [`redo`]: the seven crash-recovery redo handlers, one per WAL record
//!   kind 121–127 (Phase 2 M5 Stage C slice 1) — bounded decode, pd_lsn
//!   guard, funnel validation, application, stamp.
//! - `apply`: the seven physical application primitives (Phase 2 M5 Stage A,
//!   `pub(crate)`) — one per WAL record type, pure application, shared by
//!   redo and the normal write path (tech-selection §10.2 task 2).
//! - `validate`: the single-point validation funnel (Phase 2 M5 Stage A,
//!   `pub(crate)`) — the §10.1 frozen-checklist subset, same-page/same-record
//!   decidable items only (tech-selection §10.1/§10.2).

#![warn(missing_docs)]
#![warn(rust_2018_idioms)]

// Stage C slice 1 (2026-09-17): the redo handlers now consume both modules
// for real; the remaining `allow` covers only the items still waiting for
// the Stage C write path / search consumers (same discipline as before).
#[allow(dead_code)]
pub(crate) mod apply;
pub mod dataset;
// Stage B slice 1 (2026-09-15): node-entry and directory-chain format
// owners. Their consumers (index.rs / open-time chain walk) land in
// slice 3; today's callers are the primitives and the unit tests — same
// dead_code discipline as `apply`.
#[allow(dead_code)]
pub(crate) mod dir;

/// Stage B slice 3a (2026-09-15): the index lifecycle — creation and open
/// (tech-selection §10.3). See [`index`].
pub mod index;

// Stage B slice 2 (2026-09-15): meta-page layout owner. Its consumer
// (index.rs creation/open protocol) lands in slice 3 — same dead_code
// discipline as `dir`.
pub mod distance;
pub mod encoding;
pub mod error;
pub mod graph;
#[allow(dead_code)]
pub(crate) mod meta;

// See `dir` above for the Stage B dead_code rationale.
#[allow(dead_code)]
pub(crate) mod node;
pub mod page;
pub mod params;
pub mod redo;
pub mod rng;
pub mod snapshot;

pub(crate) mod validate;

pub use error::{HnswError, Result};
pub use graph::{Hnsw, Metric, NeighborSelection};
pub use index::{ExpectedParams, HnswIndex, OpenOutcome};
pub use params::{HnswParams, NodeId};
