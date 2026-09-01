//! pg_rust in-memory HNSW vector index — Phase 2 M4 (Stage A).
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
//!
//! [`graph`] (the HNSW core — paper Algorithms 1/2/4/5, §4/§6) is Stage B;
//! [`snapshot`] (the `save`/`load` file API, §7) is Stage C. Both are
//! placeholder modules for now.

#![warn(missing_docs)]
#![warn(rust_2018_idioms)]

pub mod distance;
pub mod encoding;
pub mod error;
pub mod graph;
pub mod params;
pub mod rng;
pub mod snapshot;

pub use error::{HnswError, Result};
pub use params::{HnswParams, NodeId};
