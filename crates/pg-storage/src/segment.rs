//! SegmentedStorage interface reservation (M3 Stage G; tech-selection §8).
//!
//! Phase 3 (Inverted) and Phase 5 (TimeSeries) are both segment-based
//! architectures (ROADMAP.md). M3 **defines the contract only — it ships no
//! implementation**: the [`SegmentedStorage`] trait, the [`SegmentState`]
//! one-way lifecycle machine, and the [`SegmentId`] identity newtype.
//!
//! WAL side: the `SegmentSeal = 110` / `SegmentMerge = 111` discriminants
//! were already reserved in [`crate::wal::record::WalRecordType`] (Stage 0, the M1+M2 baseline);
//! they parse via `from_u8` and recovery hard-fails on them because no redo
//! handler is registered. This module only pins the **payload contracts**
//! (see the per-method docs); the implementation phase registers the redo
//! handlers and adds the payload structs.
//!
//! (Not to be confused with [`crate::wal::segment`], which manages the
//! 16 MiB WAL segment *files* — unrelated to logical storage segments.)

use serde::{Deserialize, Serialize};

use crate::error::Result;

/// Identity of a logical storage segment.
///
/// Segment IDs are allocated by the owning access method's
/// [`SegmentedStorage::create_segment`]; M3 assigns no numeric range or
/// allocation discipline — that is part of the unwritten implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SegmentId(pub u64);

/// Lifecycle of a segment: a **one-way** state machine.
///
/// Legal transitions (each method on [`SegmentedStorage`] drives one edge):
///
/// ```text
/// Active --freeze--> Frozen --seal--> Sealed --merge--> Merging --merge--> Retired
/// ```
///
/// (seal always precedes merge; there are no other edges — no rollback, no
/// skip, no self-loop)
///
/// - `Active`: writable and readable.
/// - `Frozen`: no more writes; still readable. Reversible-in-spirit designs
///   (thaw) are deliberately excluded — freezing is the first commitment
///   step toward immutability.
/// - `Sealed`: immutable and eligible for compaction/merge.
/// - `Merging`: participating in an in-flight [`SegmentedStorage::merge`].
/// - `Retired`: fully superseded by a merge output; reclaimable.
///
/// The machine is one-way: no transition ever returns a segment to an
/// earlier state. Retired segments are never resurrected; their ID space is
/// not reused within a storage instance's lifetime (implementation-phase
/// contract, stated here so callers do not depend on ID reuse).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SegmentState {
    /// Writable and readable (initial state after creation).
    Active,
    /// Write-stopped, still readable (after `freeze`).
    Frozen,
    /// Immutable, eligible for compaction (after `seal`).
    Sealed,
    /// Participating in an in-flight merge.
    Merging,
    /// Superseded by a merge output; reclaimable (terminal).
    Retired,
}

impl SegmentState {
    /// Whether `next` is a legal successor of `self` in the one-way
    /// lifecycle (see the enum docs for the machine).
    ///
    /// This pure predicate is the whole of M3's state-machine code: it
    /// exists so the contract is executable and testable, and so the
    /// implementation phase cannot silently widen the transition set.
    pub fn can_transition_to(self, next: SegmentState) -> bool {
        use SegmentState::*;
        matches!(
            (self, next),
            (Active, Frozen) | (Frozen, Sealed) | (Sealed, Merging) | (Merging, Retired)
        )
    }
}

/// Segment-based storage contract for Phase 3 (Inverted) and Phase 5
/// (TimeSeries) access methods (tech-selection §8).
///
/// **Reservation only**: M3 defines this trait so the two phases lock onto
/// one segment lifecycle instead of each inventing its own. No
/// implementation is shipped, no caller exists, and no redo handler is
/// registered for the reserved WAL discriminants. Changing any signature
/// after this reservation requires a revision-record entry (预留即承诺).
///
/// WAL payload contracts (recorded here because no payload structs exist
/// yet; both payloads are bincode-serialized like every other M1–M3
/// record):
///
/// - `SegmentSeal = 110`: payload carries **exactly one `SegmentId`** —
///   the segment being sealed. Sealing is per-segment; batching seal
///   records is an implementation-phase decision, not part of this
///   contract.
/// - `SegmentMerge = 111`: payload carries **the input `SegmentId` list
///   (merge sources, in merge order) followed by the target `SegmentId`**
///   (the merge output). On redo, the inputs transition to `Retired` and
///   the target is installed as the merged segment; the record must be
///   sufficient to reconstruct that outcome idempotently.
///
/// Both records replay unconditionally (no transaction context): segment
/// lifecycle operations are administrative, not transactional.
pub trait SegmentedStorage: Send + Sync {
    /// Allocate a new segment in the [`SegmentState::Active`] state and
    /// return its ID.
    fn create_segment(&self) -> Result<SegmentId>;

    /// Stop writes to `id` (`Active` → `Frozen`); the segment remains
    /// readable. Freezing a non-`Active` segment is an error.
    fn freeze(&self, id: SegmentId) -> Result<()>;

    /// Make `id` immutable (`Frozen` → `Sealed`); sealed segments become
    /// eligible for compaction and merge. Emits a `SegmentSeal = 110` WAL
    /// record (payload contract: the single sealed `SegmentId` — see the
    /// trait docs). Sealing a non-`Frozen` segment is an error.
    fn seal(&self, id: SegmentId) -> Result<()>;

    /// Merge the sealed segments `ids` into one new segment and return the
    /// target's ID (`Sealed` → `Merging` → `Retired` for each input; the
    /// target is created `Sealed`). Emits a `SegmentMerge = 111` WAL
    /// record (payload contract: input `SegmentId` list + target
    /// `SegmentId` — see the trait docs). Merging any non-`Sealed` input
    /// is an error. On `Err` return, every input is guaranteed to still be
    /// `Sealed`: `Merging` is redo-internal idempotency semantics, never a
    /// state an input can be left in (the one-way machine has no rollback
    /// edge, so a failed merge must not strand segments).
    fn merge(&self, ids: &[SegmentId]) -> Result<SegmentId>;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Compile-stub proof: the reserved trait is implementable and the
    /// state machine compiles — without shipping an implementation.
    struct StubStorage;

    impl SegmentedStorage for StubStorage {
        fn create_segment(&self) -> Result<SegmentId> {
            unimplemented!("M3 ships the contract only (tech-selection §8)")
        }
        fn freeze(&self, _id: SegmentId) -> Result<()> {
            unimplemented!("M3 ships the contract only (tech-selection §8)")
        }
        fn seal(&self, _id: SegmentId) -> Result<()> {
            unimplemented!("M3 ships the contract only (tech-selection §8)")
        }
        fn merge(&self, _ids: &[SegmentId]) -> Result<SegmentId> {
            unimplemented!("M3 ships the contract only (tech-selection §8)")
        }
    }

    #[test]
    fn stub_impl_compiles() {
        let storage: &dyn SegmentedStorage = &StubStorage;
        let _ = storage;
    }

    #[test]
    fn state_machine_is_one_way() {
        use SegmentState::*;
        // The legal path end to end.
        assert!(Active.can_transition_to(Frozen));
        assert!(Frozen.can_transition_to(Sealed));
        assert!(Sealed.can_transition_to(Merging));
        assert!(Merging.can_transition_to(Retired));
        // No way backwards, no skipping, no self-loops.
        assert!(!Frozen.can_transition_to(Active));
        assert!(!Sealed.can_transition_to(Frozen));
        assert!(!Retired.can_transition_to(Active));
        assert!(!Active.can_transition_to(Sealed));
        assert!(!Active.can_transition_to(Active));
        assert!(!Merging.can_transition_to(Sealed));
    }
}
