//! Tier 2 interface reservations (M3 Stage G; tech-selection §9).
//!
//! Tier 2 indexes (HNSW, inverted, …) follow the base table **asynchronously**:
//! a reader tails the WAL, applies changes into the secondary structure, and
//! advances a per-index freshness watermark; the planner later compares that
//! watermark against the WAL write position to decide "index or full-table
//! fallback". M3 **defines the contracts only — no implementation ships**:
//!
//! - [`WalTailReader`]: ordered subscription to flushed WAL records from a
//!   given LSN.
//! - [`WatermarkRegistry`]: per-index `applied_lsn` store the tail reader
//!   advances and the planner queries.
//!
//! The planner-side hook lives with the access methods instead:
//! `AccessMethod::freshness()` (default `None`, so heap/btree are untouched)
//! — see `pg-am-heap`'s `access_method` module.

use crate::error::Result;
use crate::types::{Lsn, Oid};
use crate::wal::WalRecord;

/// Ordered tail subscription over the WAL (tech-selection §9).
///
/// **Reservation only**: M3 defines this trait so Tier 2 index followership
/// locks onto one shape; no reader ships and nothing registers one.
///
/// Contract for the eventual implementation:
///
/// - **Source**: only *flushed* records are yielded (a tail reader must
///   never observe a record that a crash could revoke); records are yielded
///   exactly once, in strictly ascending LSN order, starting at the first
///   record at or after `start`.
/// - **Backpressure**: the iterator is pull-based — the reader produces a
///   record only when the consumer asks for it, so a slow Tier 2 apply loop
///   naturally throttles WAL decoding; no internal unbounded buffering is
///   permitted (at most one in-flight record). Note (§9): this
///   materialized-iterator shape shares its risk with online vacuum's
///   materialization problem and may be revisited as a callback/stream at
///   implementation time.
/// - **Resume**: a consumer that stops at record `r` resumes by calling
///   `tail_from(r.lsn successor)` again — there is no cursor object and no
///   server-side subscription state. Combined with
///   [`WatermarkRegistry`], the resume point is durably recoverable: read
///   the watermark, tail from it.
/// - **Blocking vs. EOF**: reaching the current flush frontier ends the
///   iterator (`None`); the caller decides whether to poll again or back
///   off. The iterator never blocks waiting for new WAL.
pub trait WalTailReader: Send + Sync {
    /// Return an iterator yielding flushed WAL records at or after `start`,
    /// in ascending LSN order (see the trait docs for the backpressure and
    /// resume contract).
    fn tail_from(&self, start: Lsn) -> Box<dyn Iterator<Item = Result<WalRecord>> + '_>;
}

/// Per-index freshness watermark store (tech-selection §9).
///
/// Each Tier 2 index owns one watermark: the LSN up to which its contents
/// reflect the base table (`index_oid -> applied_lsn`). The tail reader
/// advances it as records are applied; the planner reads it (via
/// `AccessMethod::freshness`, which is expected to consult a registry) to
/// judge index freshness.
///
/// **Reservation only**: no implementation ships — not even the in-memory
/// one that would already satisfy future needs (§9). The eventual
/// implementation must make `set_watermark` monotone per index (a stale
/// tail reader must never move a watermark backwards); whether that rule
/// lives in the trait or the implementation is deferred to that phase.
pub trait WatermarkRegistry: Send + Sync {
    /// Read the current watermark for `index_oid`; `None` if the index has
    /// never applied any record (or is unknown to the registry).
    fn watermark(&self, index_oid: Oid) -> Option<Lsn>;

    /// Record that `index_oid` has applied WAL up to `applied_lsn`.
    fn set_watermark(&self, index_oid: Oid, applied_lsn: Lsn) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Compile-stub proof: both reserved traits are implementable without
    /// shipping an implementation.
    struct StubTail;
    struct StubRegistry;

    impl WalTailReader for StubTail {
        fn tail_from(&self, _start: Lsn) -> Box<dyn Iterator<Item = Result<WalRecord>> + '_> {
            unimplemented!("M3 ships the contract only (tech-selection §9)")
        }
    }

    impl WatermarkRegistry for StubRegistry {
        fn watermark(&self, _index_oid: Oid) -> Option<Lsn> {
            unimplemented!("M3 ships the contract only (tech-selection §9)")
        }
        fn set_watermark(&self, _index_oid: Oid, _applied_lsn: Lsn) -> Result<()> {
            unimplemented!("M3 ships the contract only (tech-selection §9)")
        }
    }

    #[test]
    fn stub_impls_compile() {
        let tail: &dyn WalTailReader = &StubTail;
        let registry: &dyn WatermarkRegistry = &StubRegistry;
        let _ = (tail, registry);
    }
}
