//! Slotted page operations (tech-selection §二).
//!
//! A heap page is a raw `&mut [u8; PAGE_SIZE]`; this module provides
//! type-state-free functions that interpret it as a slotted page:
//!
//! ```text
//! ┌ PageHeader (32 B, pg_storage::page) ────────────────┐
//! │ LinePointer array  (grows down from offset 32)      │
//! │ ─── free space: pd_lower .. pd_upper ───            │
//! │ Tuple data         (grows up toward the LP array)   │
//! │ Special space      (all relations: 16 B page chain) │
//! └─────────────────────────────────────────────────────┘
//! ```
//!
//! # Special space and the heap page chain (Stage K)
//!
//! Every heap-AM relation — user tables **and** the system catalogs — is
//! initialized with [`HEAP_SPECIAL_SIZE`] bytes of special space
//! ([`SlottedPage::init_with_special`]). The special space holds the forward
//! pointer of the relation's page chain:
//!
//! | Relative offset | Field                              |
//! |-----------------|------------------------------------|
//! | `pd_special+0..8`  | next page id (LE u64; 0 = no next page) |
//! | `pd_special+8..16` | reserved, always zero                 |
//!
//! # Invariants
//!
//! - `PAGE_HEADER_SIZE <= pd_lower <= pd_upper <= pd_special <= PAGE_SIZE`.
//!   `pd_lower` and `pd_upper` are the authoritative header fields maintained
//!   by every mutation here; [`debug_assert_invariants`] checks them. The
//!   special space is not tuple space: `pd_upper <= pd_special` already keeps
//!   tuple regions out of it.
//! - The LP array only ever grows (TID stability, §二 "关键约束"): deletion
//!   marks the LP [`LpFlags::Unused`], and `add_tuple` recycles `Unused`
//!   slots before appending a new LP.
//! - Tuple regions of live (non-`Unused`) LPs lie inside
//!   `[pd_upper, pd_special)` and never overlap.
//!
//! This stage is a pure in-memory format layer: no WAL, no buffer pool.
//! Physical space reclamation arrives with [`SlottedPage::compact`] (M3
//! Stage B): dead slots become `Unused` and live tuple bytes are restaged
//! contiguously, without ever moving or renumbering an LP array entry
//! (§4.1 stage 4 — slot ids are TID components).

use pg_storage::page::{PageHeader, PAGE_HEADER_SIZE};
use pg_storage::types::{PageId, PAGE_SIZE};

use crate::error::{HeapError, Result};
use crate::line_pointer::{LinePointer, LpFlags, LINE_POINTER_SIZE};

/// Special-space size of every heap-AM page in bytes (Stage K page chain).
///
/// Layout: `[pd_special, pd_special+8)` = next page id (LE u64, 0 = none);
/// `[pd_special+8, pd_special+16)` = reserved, always zero. All relations —
/// user tables and system catalogs alike — use this geometry, so the chain
/// machinery applies uniformly (system catalogs are single-page in practice,
/// their next pointer stays 0).
pub const HEAP_SPECIAL_SIZE: usize = 16;

/// Slotted-page operations on a raw heap page.
///
/// All methods are associated functions taking the page buffer explicitly;
/// there is no owned state.
pub struct SlottedPage;

impl SlottedPage {
    /// Initialize a fresh page with no special space: 32-byte header,
    /// `pd_lower = 32`, `pd_upper = pd_special = PAGE_SIZE`.
    ///
    /// Convenience wrapper for tests/benchmarks that do not exercise the page
    /// chain. **All production relations (user tables and system catalogs)
    /// use [`SlottedPage::init_with_special`] with [`HEAP_SPECIAL_SIZE`]** —
    /// this wrapper has no production callers left.
    pub fn init(page: &mut [u8; PAGE_SIZE]) {
        Self::init_with_special(page, 0);
    }

    /// Initialize a fresh page with `special_size` bytes of special space:
    /// `pd_special = PAGE_SIZE - special_size`, `pd_upper = pd_special`,
    /// `pd_lower = 32` (§二).
    pub fn init_with_special(page: &mut [u8; PAGE_SIZE], special_size: usize) {
        debug_assert!(special_size <= PAGE_SIZE - PAGE_HEADER_SIZE);
        // Zero the whole page, not just the header: a recycled buffer-pool
        // frame still holds the previous tenant's bytes, and the LP array /
        // tuple region must start clean.
        page.fill(0);
        PageHeader::init_page(page);
        let pd_special = (PAGE_SIZE - special_size) as u16;
        page[16..18].copy_from_slice(&pd_special.to_le_bytes()); // pd_upper
        page[18..20].copy_from_slice(&pd_special.to_le_bytes()); // pd_special
        if cfg!(debug_assertions) {
            debug_assert_invariants(page);
        }
    }

    /// Initialize the page only if it has never been initialized.
    ///
    /// A validly initialized heap page always has `pd_upper >= PAGE_HEADER_SIZE`
    /// (it starts at `pd_special` and only shrinks toward `pd_lower`), so
    /// `pd_upper == 0` uniquely identifies a fresh, all-zero page — for example
    /// one materialized by extending the data file with zeros during recovery,
    /// before any `HeapInsert` redo has run against it.
    pub fn init_if_fresh(page: &mut [u8; PAGE_SIZE]) {
        Self::init_if_fresh_with_special(page, 0);
    }

    /// [`SlottedPage::init_if_fresh`] with an explicit special-space size.
    /// Heap pages (user relations and their redo path) pass
    /// [`HEAP_SPECIAL_SIZE`].
    pub fn init_if_fresh_with_special(page: &mut [u8; PAGE_SIZE], special_size: usize) {
        if Self::header(page).pd_upper == 0 {
            Self::init_with_special(page, special_size);
        }
    }

    /// Decode the page header.
    pub fn header(page: &[u8; PAGE_SIZE]) -> PageHeader {
        PageHeader::read_from(page)
    }

    /// Number of line pointer slots (including `Unused` ones).
    ///
    /// Infallible: derives the count from `pd_lower` arithmetic only. A
    /// corrupted header yields a garbage count but never panics; mutation and
    /// dereference paths go through `SlottedPage::checked_header` instead.
    pub fn slot_count(page: &[u8; PAGE_SIZE]) -> usize {
        let header = Self::header(page);
        (header.pd_lower as usize).saturating_sub(PAGE_HEADER_SIZE) / LINE_POINTER_SIZE
    }

    /// Contiguous free space in bytes (`pd_upper - pd_lower`, saturating at 0
    /// for a corrupted header).
    pub fn free_space(page: &[u8; PAGE_SIZE]) -> usize {
        let header = Self::header(page);
        header.pd_upper.saturating_sub(header.pd_lower) as usize
    }

    /// Write the page-chain forward pointer into the special space
    /// (`None` clears it to 0, Stage K). The reserved trailing 8 bytes are
    /// left untouched (they are zeroed by [`SlottedPage::init_with_special`]).
    ///
    /// Returns [`HeapError::Corrupted`] if the header geometry is inconsistent
    /// or the page does not carry exactly [`HEAP_SPECIAL_SIZE`] bytes of
    /// special space — symmetric with [`SlottedPage::next_page`]. The check is
    /// a real `Result`, not a debug assertion: redo applies chain relinks to
    /// pages recovered from disk (an untrusted source), and on a corrupt page
    /// an unchecked write would panic out of bounds or silently land in the
    /// tuple region.
    pub fn set_next_page(page: &mut [u8; PAGE_SIZE], next: Option<PageId>) -> Result<()> {
        let header = Self::checked_header(page)?;
        if header.pd_special as usize != PAGE_SIZE - HEAP_SPECIAL_SIZE {
            return Err(HeapError::Corrupted(format!(
                "page chain pointer requires special_size {HEAP_SPECIAL_SIZE}, page has pd_special={}",
                header.pd_special
            )));
        }
        let off = header.pd_special as usize;
        let raw = next.map(|p| p.0).unwrap_or(0);
        page[off..off + 8].copy_from_slice(&raw.to_le_bytes());
        Ok(())
    }

    /// Read the page-chain forward pointer from the special space; `Ok(None)`
    /// means "no next page" (Stage K).
    ///
    /// Returns [`HeapError::Corrupted`] if the header geometry is inconsistent
    /// or the page does not carry exactly [`HEAP_SPECIAL_SIZE`] bytes of
    /// special space — corrupted page bytes must never cause a panic.
    pub fn next_page(page: &[u8; PAGE_SIZE]) -> Result<Option<PageId>> {
        let header = Self::checked_header(page)?;
        if header.pd_special as usize != PAGE_SIZE - HEAP_SPECIAL_SIZE {
            return Err(HeapError::Corrupted(format!(
                "page chain pointer requires special_size {HEAP_SPECIAL_SIZE}, page has pd_special={}",
                header.pd_special
            )));
        }
        let off = header.pd_special as usize;
        let raw = u64::from_le_bytes(page[off..off + 8].try_into().unwrap());
        Ok(if raw == 0 { None } else { Some(PageId(raw)) })
    }

    /// Read the line pointer at `slot`. Returns [`HeapError::InvalidSlot`]
    /// if `slot` is out of range, or [`HeapError::Corrupted`] if the header
    /// geometry is inconsistent (M2 has no page checksums, so corrupted
    /// bytes can reach this layer; they must never cause a panic).
    pub fn line_pointer(page: &[u8; PAGE_SIZE], slot: u16) -> Result<LinePointer> {
        let header = Self::checked_header(page)?;
        let slot_count = (header.pd_lower as usize - PAGE_HEADER_SIZE) / LINE_POINTER_SIZE;
        if slot as usize >= slot_count {
            return Err(HeapError::InvalidSlot(slot));
        }
        // checked_header guarantees pd_lower <= PAGE_SIZE, so this slice is
        // always in bounds.
        let off = PAGE_HEADER_SIZE + slot as usize * LINE_POINTER_SIZE;
        Ok(LinePointer::from_le_bytes(
            page[off..off + LINE_POINTER_SIZE].try_into().unwrap(),
        ))
    }

    /// Decode the header and validate the geometry the LP array depends on:
    /// `PAGE_HEADER_SIZE <= pd_lower <= pd_upper <= pd_special <= PAGE_SIZE`
    /// and `pd_lower` on a line-pointer boundary.
    fn checked_header(page: &[u8; PAGE_SIZE]) -> Result<PageHeader> {
        let header = Self::header(page);
        let (lower, upper, special) = (
            header.pd_lower as usize,
            header.pd_upper as usize,
            header.pd_special as usize,
        );
        if lower < PAGE_HEADER_SIZE
            || (lower - PAGE_HEADER_SIZE) % LINE_POINTER_SIZE != 0
            || lower > upper
            || upper > special
            || special > PAGE_SIZE
        {
            return Err(HeapError::Corrupted(format!(
                "bad page header geometry: pd_lower={lower} pd_upper={upper} pd_special={special}"
            )));
        }
        Ok(header)
    }

    /// Return the first [`LpFlags::Unused`] slot available for recycling, or
    /// `None` when the LP array has no `Unused` entry (the next tuple must
    /// append a new LP at `slot_count`).
    ///
    /// Pure read (M3 tech-selection §4.6): slot selection is a deliberate
    /// two-step protocol — the caller picks the slot with this function (or
    /// `slot_count` for an append), writes it into the WAL record, and only
    /// then places the tuple with [`SlottedPage::add_tuple_at`]. Slot
    /// assignment is thus carried by the WAL record itself, and redo never
    /// has to reproduce the online writer's choice by re-running first-fit
    /// against a page that may have diverged.
    ///
    /// Never panics on a corrupted header: `pd_lower` is clamped into the
    /// page before the LP array is walked (same policy as
    /// [`SlottedPage::slot_count`]); mutation paths validate geometry via
    /// `SlottedPage::checked_header` instead.
    pub fn first_fit_slot(page: &[u8; PAGE_SIZE]) -> Option<u16> {
        let header = Self::header(page);
        let pd_lower = (header.pd_lower as usize).min(PAGE_SIZE);
        let slot_count = pd_lower.saturating_sub(PAGE_HEADER_SIZE) / LINE_POINTER_SIZE;
        // Reads the LP array directly; `line_pointer()` would re-decode the
        // header per slot.
        for slot in 0..slot_count {
            let off = PAGE_HEADER_SIZE + slot * LINE_POINTER_SIZE;
            let lp =
                LinePointer::from_le_bytes(page[off..off + LINE_POINTER_SIZE].try_into().unwrap());
            if lp.flags() == LpFlags::Unused {
                return Some(slot as u16);
            }
        }
        None
    }

    /// Insert `bytes` at the caller-selected `slot` (§4.6 slot addressing).
    ///
    /// `slot` must be either exactly `slot_count(page)` — appending a new LP
    /// at `pd_lower`, which costs an extra [`LINE_POINTER_SIZE`] of free
    /// space — or an existing [`LpFlags::Unused`] slot, which is recycled in
    /// place. Tuple bytes are placed at `pd_upper - len`.
    ///
    /// Anything else is [`HeapError::InvalidSlot`]: an out-of-range slot, or
    /// one still referencing a tuple. Online writers and redo both address
    /// slots explicitly, so a mismatch means the WAL stream and the page
    /// disagree — that must hard-fail, never silently relocate the tuple
    /// (redo maps the error to `MetadataCorrupted`, same as the pre-§4.6
    /// slot-divergence check).
    pub fn add_tuple_at(page: &mut [u8; PAGE_SIZE], slot: u16, bytes: &[u8]) -> Result<()> {
        let len = bytes.len();
        let header = Self::checked_header(page)?;
        // The largest tuple that can ever fit: special space is reserved for
        // the page chain, not tuple data. Saturating: a degenerate (but
        // geometry-valid) header with pd_special == PAGE_HEADER_SIZE yields 0.
        let max_tuple =
            (header.pd_special as usize).saturating_sub(PAGE_HEADER_SIZE + LINE_POINTER_SIZE);
        if len == 0 {
            return Err(HeapError::InvalidArgument(
                "cannot insert an empty tuple".to_string(),
            ));
        }
        if len > max_tuple {
            return Err(HeapError::TupleTooLarge(len));
        }

        let slot_count = (header.pd_lower as usize - PAGE_HEADER_SIZE) / LINE_POINTER_SIZE;
        let appending = if slot as usize > slot_count {
            return Err(HeapError::InvalidSlot(slot));
        } else if slot as usize == slot_count {
            true
        } else {
            // Recycling: only an Unused LP may be overwritten.
            let off = PAGE_HEADER_SIZE + slot as usize * LINE_POINTER_SIZE;
            let lp =
                LinePointer::from_le_bytes(page[off..off + LINE_POINTER_SIZE].try_into().unwrap());
            if lp.flags() != LpFlags::Unused {
                return Err(HeapError::InvalidSlot(slot));
            }
            false
        };

        let lp_cost = if appending { LINE_POINTER_SIZE } else { 0 };
        let free = (header.pd_upper - header.pd_lower) as usize;
        if free < len + lp_cost {
            return Err(HeapError::PageFull {
                needed: len + lp_cost,
                available: free,
            });
        }

        // Place the tuple bytes at the top of the free space.
        let new_upper = header.pd_upper as usize - len;
        page[new_upper..new_upper + len].copy_from_slice(bytes);
        Self::set_line_pointer(
            page,
            slot,
            LinePointer::new(new_upper as u16, LpFlags::Normal, len as u16),
        );
        if appending {
            Self::set_pd_lower(page, header.pd_lower + LINE_POINTER_SIZE as u16);
        }
        Self::set_pd_upper(page, new_upper as u16);

        if cfg!(debug_assertions) {
            debug_assert_invariants(page);
        }
        Ok(())
    }

    /// Insert `bytes` as a new tuple, returning its slot id.
    ///
    /// Prefers recycling an [`LpFlags::Unused`] slot (LP array only grows,
    /// keeping TIDs stable); otherwise appends a new LP at `pd_lower`.
    ///
    /// Since §4.6 this is just the composition of the two explicit steps —
    /// [`SlottedPage::first_fit_slot`] (falling back to `slot_count`) plus
    /// [`SlottedPage::add_tuple_at`] — and its external behavior is unchanged.
    /// Callers that write WAL must NOT use this convenience wrapper: they
    /// pick the slot first, log it, then place at it (see `HeapAM::insert`).
    pub fn add_tuple(page: &mut [u8; PAGE_SIZE], bytes: &[u8]) -> Result<u16> {
        let slot = Self::first_fit_slot(page).unwrap_or(Self::slot_count(page) as u16);
        Self::add_tuple_at(page, slot, bytes)?;
        Ok(slot)
    }

    /// Compact the page in place (M3 Stage B, tech-selection §4.5): kill the
    /// dead slots listed in `dead_slots` (LP → [`LpFlags::Unused`], the same
    /// semantics as [`SlottedPage::delete_tuple`]), move the surviving tuple
    /// bytes into a contiguous region ending at `pd_special`, reclaiming the
    /// holes the killed tuples and earlier fragmentation left, and reset
    /// `pd_upper` accordingly.
    ///
    /// HARD INVARIANT (§4.1 stage 4): LP array entries are never moved or
    /// renumbered — a slot id is a TID component referenced by index entries
    /// and HOT `t_ctid`s. Only tuple *bytes* relocate; each surviving LP
    /// keeps its slot, flags, and length and is re-pointed at its tuple's new
    /// offset. `pd_lower` is unchanged, so `slot_count` is too.
    ///
    /// `dead_slots` must be strictly ascending — the exact order the
    /// `HeapCleanup` WAL payload carries, so the online path and its redo
    /// handler run the same function on the same arguments and converge
    /// byte-for-byte ("replay = re-execute the same physical operation",
    /// §4.5 replay convergence). Each listed slot must reference a live (`Normal`)
    /// or `Dead` line pointer; an `Unused` one means the caller and the page
    /// disagree (e.g. a replay the `pd_lsn` guard should have skipped) and
    /// is a hard error, never a silent skip.
    ///
    /// # Kill-list contract (caller's obligation)
    ///
    /// `compact()` is a purely physical primitive: it does NOT consult MVCC
    /// visibility or HOT chain structure. The caller (Stage C's vacuum)
    /// guarantees every listed slot is reclaimable. For HOT chains this means
    /// the chain must be ENTIRELY dead — never kill:
    /// (a) any member still referenced by a predecessor's `t_ctid` pointer
    ///     (a killed middle/tail member leaves a dangling `t_ctid` pointing at
    ///     an `Unused`, later recycled, slot — the chain walks into garbage);
    /// (b) the chain ROOT while any member is still alive (nothing points at
    ///     the root via `t_ctid` — the INDEX entry does: killing it strands
    ///     the live members from both seqscan and index scan, and once the
    ///     root's slot is recycled the index entry resolves to the WRONG row).
    /// The chain-liveness decision — which versions any snapshot can still
    /// reach — belongs to `scan_dead_tuples`' horizon logic plus chain
    /// grouping (tech-selection §4.4), not here.
    ///
    /// Deterministic layout: surviving tuples are restaged in ascending slot
    /// order, packed downward from `pd_special`; the abandoned data region is
    /// zeroed. Identical pre-image + identical `dead_slots` ⇒ identical
    /// post-image bytes.
    pub fn compact(page: &mut [u8; PAGE_SIZE], dead_slots: &[u16]) -> Result<()> {
        let header = Self::checked_header(page)?;
        let slot_count = (header.pd_lower as usize - PAGE_HEADER_SIZE) / LINE_POINTER_SIZE;

        // Validate the kill list BEFORE mutating anything: strictly
        // ascending (the WAL payload contract), in range, and each slot
        // holding a tuple (Normal or Dead). Redirect is never written by
        // this system; Unused means the slot is already dead space.
        let mut prev: Option<u16> = None;
        for &slot in dead_slots {
            if prev.is_some_and(|p| slot <= p) {
                return Err(HeapError::InvalidArgument(format!(
                    "compact dead_slots must be strictly ascending, got {slot} after {}",
                    prev.unwrap()
                )));
            }
            prev = Some(slot);
            let lp = Self::line_pointer(page, slot)?;
            if !matches!(lp.flags(), LpFlags::Normal | LpFlags::Dead) {
                return Err(HeapError::InvalidSlot(slot));
            }
        }

        // Stage the surviving tuples (bytes copied out, ascending slot
        // order). Any non-Unused LP not on the kill list keeps its bytes —
        // including Dead entries the caller chose not to kill.
        let pd_special = header.pd_special as usize;
        let mut staged: Vec<(u16, LpFlags, u16, Vec<u8>)> = Vec::new();
        for slot in 0..slot_count as u16 {
            let off = PAGE_HEADER_SIZE + slot as usize * LINE_POINTER_SIZE;
            let lp =
                LinePointer::from_le_bytes(page[off..off + LINE_POINTER_SIZE].try_into().unwrap());
            if lp.flags() == LpFlags::Unused || dead_slots.binary_search(&slot).is_ok() {
                continue;
            }
            let (o, l) = (lp.off() as usize, lp.len() as usize);
            if o < header.pd_upper as usize || o + l > pd_special {
                return Err(HeapError::Corrupted(format!(
                    "slot {slot}: tuple region [{o}, {}) outside [{}, {pd_special})",
                    o + l,
                    header.pd_upper
                )));
            }
            staged.push((slot, lp.flags(), lp.len(), page[o..o + l].to_vec()));
        }

        // Clear the entire data region (the staged bytes are safe in the
        // side buffer), then restage downward from pd_special.
        page[header.pd_upper as usize..pd_special].fill(0);
        let mut cursor = pd_special;
        for (slot, flags, len, bytes) in &staged {
            cursor -= *len as usize;
            page[cursor..cursor + *len as usize].copy_from_slice(bytes);
            Self::set_line_pointer(page, *slot, LinePointer::new(cursor as u16, *flags, *len));
        }

        // Kill the dead slots (delete_tuple semantics: flags → Unused,
        // offset/length kept for forensic value).
        for &slot in dead_slots {
            let lp = Self::line_pointer(page, slot)?;
            Self::set_line_pointer(page, slot, lp.with_flags(LpFlags::Unused));
        }

        // pd_lower never moves (LP array stable); pd_upper absorbs every
        // reclaimed hole.
        Self::set_pd_upper(page, cursor as u16);

        if cfg!(debug_assertions) {
            debug_assert_invariants(page);
        }
        Ok(())
    }

    /// Mark the tuple at `slot` as deleted (LP → [`LpFlags::Unused`]).
    ///
    /// The physical bytes are not reclaimed in-place (no compaction in this
    /// stage); the slot becomes recyclable by [`SlottedPage::add_tuple`].
    /// Returns [`HeapError::InvalidSlot`] if the slot is out of range or does
    /// not hold a live (`Normal`) tuple.
    pub fn delete_tuple(page: &mut [u8; PAGE_SIZE], slot: u16) -> Result<()> {
        let lp = Self::line_pointer(page, slot)?;
        if lp.flags() != LpFlags::Normal {
            return Err(HeapError::InvalidSlot(slot));
        }
        Self::set_line_pointer(page, slot, lp.with_flags(LpFlags::Unused));
        if cfg!(debug_assertions) {
            debug_assert_invariants(page);
        }
        Ok(())
    }

    /// Return the tuple bytes at `slot`, or `Ok(None)` if the slot is out of
    /// range or not in [`LpFlags::Normal`] state.
    ///
    /// Returns [`HeapError::Corrupted`] if the header geometry or the line
    /// pointer's offset/length are inconsistent — corrupted page bytes must
    /// never cause an out-of-bounds panic (M2 has no page checksums).
    ///
    /// LP REDIRECT hops are followed iteratively, bounded by the page's slot
    /// count (post-Stage-S review B4): NOTHING in this system writes
    /// REDIRECT line pointers (HOT chains keep the old tuple in place and
    /// follow `t_ctid` — the pre-prune in-place style, see
    /// docs/stage_spec.md Stage S), so a redirect chain longer than the slot
    /// count is only reachable on a corrupt page, where the previous
    /// unbounded recursion would have overflowed the stack.
    pub fn tuple(page: &[u8; PAGE_SIZE], slot: u16) -> Result<Option<&[u8]>> {
        let mut cur = slot;
        for _ in 0..=Self::slot_count(page) {
            let lp = match Self::line_pointer(page, cur) {
                Ok(lp) => lp,
                Err(HeapError::InvalidSlot(_)) => return Ok(None),
                Err(e) => return Err(e),
            };
            if lp.flags() == LpFlags::Redirect {
                cur = lp.off();
                continue;
            }
            if lp.flags() != LpFlags::Normal {
                return Ok(None);
            }
            let header = Self::checked_header(page)?;
            let off = lp.off() as usize;
            let end = off + lp.len() as usize;
            if off < header.pd_upper as usize || end > header.pd_special as usize {
                return Err(HeapError::Corrupted(format!(
                    "slot {slot}: tuple region [{off}, {end}) outside [{}, {})",
                    header.pd_upper, header.pd_special
                )));
            }
            // checked_header guarantees pd_special <= PAGE_SIZE, so this
            // slice is always in bounds.
            return Ok(Some(&page[off..end]));
        }
        Err(HeapError::Corrupted(format!(
            "slot {slot}: LP REDIRECT chain longer than the page's slot count"
        )))
    }

    /// Write a line pointer into the LP array.
    fn set_line_pointer(page: &mut [u8; PAGE_SIZE], slot: u16, lp: LinePointer) {
        let off = PAGE_HEADER_SIZE + slot as usize * LINE_POINTER_SIZE;
        page[off..off + LINE_POINTER_SIZE].copy_from_slice(&lp.to_le_bytes());
    }

    /// Update `pd_lower` in the header (offset 14..16, §二).
    fn set_pd_lower(page: &mut [u8; PAGE_SIZE], pd_lower: u16) {
        page[14..16].copy_from_slice(&pd_lower.to_le_bytes());
    }

    /// Update `pd_upper` in the header (offset 16..18, §二).
    fn set_pd_upper(page: &mut [u8; PAGE_SIZE], pd_upper: u16) {
        page[16..18].copy_from_slice(&pd_upper.to_le_bytes());
    }
}

/// Assert the slotted-page invariants listed in the module docs:
/// `pd_lower <= pd_upper`, LP regions within `[pd_upper, pd_special)`, and
/// no overlapping tuple regions.
///
/// Intended for tests and `debug_assert!` use; compiled in always so
/// integration tests and proptests can call it.
pub fn debug_assert_invariants(page: &[u8; PAGE_SIZE]) {
    let header = SlottedPage::header(page);
    assert!(header.pd_lower as usize >= PAGE_HEADER_SIZE);
    assert!(header.pd_lower <= header.pd_upper);
    assert!(header.pd_upper <= header.pd_special);
    assert!(header.pd_special as usize <= PAGE_SIZE);
    assert_eq!(
        (header.pd_lower as usize - PAGE_HEADER_SIZE) % LINE_POINTER_SIZE,
        0
    );

    let mut regions: Vec<(usize, usize)> = Vec::new();
    for slot in 0..SlottedPage::slot_count(page) {
        let lp = SlottedPage::line_pointer(page, slot as u16).unwrap();
        if lp.flags() == LpFlags::Unused {
            continue;
        }
        let off = lp.off() as usize;
        let end = off + lp.len() as usize;
        assert!(
            off >= header.pd_upper as usize,
            "slot {slot} below pd_upper"
        );
        assert!(
            end <= header.pd_special as usize,
            "slot {slot} past pd_special"
        );
        regions.push((off, end));
    }
    regions.sort_unstable();
    for pair in regions.windows(2) {
        assert!(
            pair[0].1 <= pair[1].0,
            "overlapping tuple regions: {:?} vs {:?}",
            pair[0],
            pair[1]
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh_page() -> [u8; PAGE_SIZE] {
        let mut page = [0u8; PAGE_SIZE];
        SlottedPage::init(&mut page);
        page
    }

    #[test]
    fn init_sets_header_fields() {
        let page = fresh_page();
        let header = SlottedPage::header(&page);
        assert_eq!(header.pd_lower, PAGE_HEADER_SIZE as u16);
        assert_eq!(header.pd_upper, PAGE_SIZE as u16);
        assert_eq!(header.pd_special, PAGE_SIZE as u16);
        assert_eq!(SlottedPage::slot_count(&page), 0);
        assert_eq!(SlottedPage::free_space(&page), PAGE_SIZE - PAGE_HEADER_SIZE);
    }

    #[test]
    fn add_and_read_back() {
        let mut page = fresh_page();
        let slot = SlottedPage::add_tuple(&mut page, b"hello heap").unwrap();
        assert_eq!(slot, 0);
        assert_eq!(
            SlottedPage::tuple(&page, slot).unwrap(),
            Some(&b"hello heap"[..])
        );
        assert_eq!(SlottedPage::slot_count(&page), 1);
    }

    #[test]
    fn delete_recycles_slot() {
        let mut page = fresh_page();
        let s0 = SlottedPage::add_tuple(&mut page, b"aaaa").unwrap();
        let _s1 = SlottedPage::add_tuple(&mut page, b"bbbb").unwrap();
        SlottedPage::delete_tuple(&mut page, s0).unwrap();
        assert_eq!(SlottedPage::tuple(&page, s0).unwrap(), None);
        // The recycled slot is reused for the next insert.
        let s2 = SlottedPage::add_tuple(&mut page, b"cccc").unwrap();
        assert_eq!(s2, s0);
        assert_eq!(SlottedPage::tuple(&page, s2).unwrap(), Some(&b"cccc"[..]));
        assert_eq!(SlottedPage::slot_count(&page), 2);
    }

    #[test]
    fn page_full_is_reported() {
        let mut page = fresh_page();
        let big = vec![0xAB; PAGE_SIZE];
        assert!(matches!(
            SlottedPage::add_tuple(&mut page, &big),
            Err(HeapError::TupleTooLarge(_))
        ));
        // Fill the page with maximal tuples.
        let chunk = vec![0xCD; 1000];
        while SlottedPage::free_space(&page) >= 1000 + LINE_POINTER_SIZE {
            SlottedPage::add_tuple(&mut page, &chunk).unwrap();
        }
        let err = SlottedPage::add_tuple(&mut page, &chunk).unwrap_err();
        assert!(matches!(err, HeapError::PageFull { .. }));
    }

    #[test]
    fn invalid_slots_rejected() {
        let mut page = fresh_page();
        assert!(matches!(
            SlottedPage::delete_tuple(&mut page, 0),
            Err(HeapError::InvalidSlot(0))
        ));
        assert_eq!(SlottedPage::tuple(&page, 7).unwrap(), None);
        assert!(matches!(
            SlottedPage::line_pointer(&page, 0),
            Err(HeapError::InvalidSlot(0))
        ));
    }

    #[test]
    fn empty_tuple_rejected_as_invalid_argument() {
        let mut page = fresh_page();
        assert!(matches!(
            SlottedPage::add_tuple(&mut page, &[]),
            Err(HeapError::InvalidArgument(_))
        ));
    }

    #[test]
    fn init_with_special_reserves_special_space() {
        let mut page = [0u8; PAGE_SIZE];
        SlottedPage::init_with_special(&mut page, HEAP_SPECIAL_SIZE);
        let header = SlottedPage::header(&page);
        assert_eq!(header.pd_lower, PAGE_HEADER_SIZE as u16);
        assert_eq!(header.pd_special as usize, PAGE_SIZE - HEAP_SPECIAL_SIZE);
        assert_eq!(header.pd_upper, header.pd_special);
        assert_eq!(SlottedPage::slot_count(&page), 0);
        assert_eq!(
            SlottedPage::free_space(&page),
            PAGE_SIZE - HEAP_SPECIAL_SIZE - PAGE_HEADER_SIZE
        );
        debug_assert_invariants(&page);
    }

    #[test]
    fn next_page_round_trip() {
        let mut page = [0u8; PAGE_SIZE];
        SlottedPage::init_with_special(&mut page, HEAP_SPECIAL_SIZE);
        // A freshly initialized page has no successor.
        assert_eq!(SlottedPage::next_page(&page).unwrap(), None);

        SlottedPage::set_next_page(&mut page, Some(PageId(42))).unwrap();
        assert_eq!(SlottedPage::next_page(&page).unwrap(), Some(PageId(42)));

        SlottedPage::set_next_page(&mut page, None).unwrap();
        assert_eq!(SlottedPage::next_page(&page).unwrap(), None);

        // The reserved trailing 8 bytes stay zero.
        let off = PAGE_SIZE - HEAP_SPECIAL_SIZE;
        assert!(page[off + 8..off + 16].iter().all(|&b| b == 0));
        debug_assert_invariants(&page);
    }

    #[test]
    fn next_page_rejects_special_less_page() {
        // A catalog-style page (special_size = 0) has no chain pointer;
        // reading one must be an error, not a read of tuple bytes.
        let page = fresh_page();
        assert!(matches!(
            SlottedPage::next_page(&page),
            Err(HeapError::Corrupted(_))
        ));
        // Writing one must be a hard error too (F2): the page may come from
        // disk during redo, so the check cannot be a debug-only assertion.
        let mut page = fresh_page();
        assert!(matches!(
            SlottedPage::set_next_page(&mut page, Some(PageId(42))),
            Err(HeapError::Corrupted(_))
        ));
    }

    #[test]
    fn special_space_shrinks_tuple_capacity() {
        let mut page = [0u8; PAGE_SIZE];
        SlottedPage::init_with_special(&mut page, HEAP_SPECIAL_SIZE);
        let bytes = vec![0x5Au8; 100];
        let mut n = 0usize;
        while SlottedPage::free_space(&page) >= 100 + LINE_POINTER_SIZE {
            SlottedPage::add_tuple(&mut page, &bytes).unwrap();
            n += 1;
        }
        debug_assert_invariants(&page);
        // The special space is not usable for tuples.
        assert_eq!(n, (PAGE_SIZE - HEAP_SPECIAL_SIZE - PAGE_HEADER_SIZE) / 104);
        // And the chain pointer survives a full page.
        SlottedPage::set_next_page(&mut page, Some(PageId(7))).unwrap();
        assert_eq!(SlottedPage::next_page(&page).unwrap(), Some(PageId(7)));
        debug_assert_invariants(&page);
    }
}
