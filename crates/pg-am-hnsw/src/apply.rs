//! Physical application primitives — Phase 2 M5 Stage A (tech-selection
//! §10.2 task 2, v1.7 signatures; coding plan Stage A).
//!
//! Seven `pub(crate)` primitives, one per WAL record type (121–127), shared
//! by the redo handlers (Stage C) and the normal write path (Stage C),
//! plus the non-mutating [`select_slot`] (2026-09-15, round 3 P1-1) that
//! lets the normal path pick the slot BEFORE the WAL append (WAL-first):
//!
//! - [`append_node`] — §8.1 step 3: create a node entry (fixed-size
//!   reservation by drawn level, §7.2; state INITIALIZING); a convenience
//!   composition of `select_slot` + `apply_node_at`;
//! - [`dir_append`] — §8.1 step 4: publish `node_id → (page, slot)` at the
//!   directory tail;
//! - [`set_neighbors`] — §8.1 steps 5/6: in-place rewrite of one level's
//!   neighbor list (the entry never moves or grows);
//! - [`apply_meta`] — §8.1 step 7: meta-page entry-point / max-level
//!   post-image;
//! - [`publish_live`] — §8.1 step 8: flip the state bit to LIVE;
//! - [`dir_link`] — §8.1 step 2: point the old directory tail at the next
//!   chain page;
//! - [`apply_tombstone`] — HnswNodeTombstone (124) application; the
//!   tombstone SEMANTICS land in M6 (tech-selection §1 scope split), M5
//!   only applies the bit.
//!
//! **Zero validation discipline** (§10.2 v1.9 P2-2): these primitives are
//! pure application — every business rule (dimension, L_max, capacities,
//! ordering, self-loops) is validated in the `validate` funnel, never here.
//! The only guards are structural: a physically impossible application (page
//! full, missing slot, content overflowing the reserved region) fails loudly
//! — that is not validation, it is the buffer-overrun invariant.
//!
//! Primitives take the page BUFFER as the application target; the PageIds
//! carried in the WAL records (§4.2 self-containment) are resolved to
//! buffers by the caller (the redo handler pins the page — 2026-09-14
//! clarification of the v1.7 "physical page parameter" wording).

use pg_storage::page::{
    decode_line_pointer, encode_line_pointer, LINE_POINTER_SIZE, PAGE_HEADER_SIZE,
};
use pg_storage::types::{PageId, PAGE_SIZE};

use crate::dir::DIR_ENTRIES_PER_PAGE;
use crate::error::{HnswError, Result};
use crate::meta::{META_OFF_ENTRY_POINT, META_OFF_MAX_LEVEL};
use crate::node::{
    level_region_pos, level_region_size, state_offset, NodeGeometry, STATE_LIVE_BIT,
    STATE_TOMBSTONE_BIT,
};
use crate::page::{dir_count, dir_ordinal, DIR_HEADER_SIZE, DIR_OFF_COUNT, DIR_OFF_NEXT};

// ---------------------------------------------------------------------
// Line-pointer access — consumed from pg_storage::page since 2026-09-15
// (Stage B slice 1 adjudication: the layout moved up to pg-storage as the
// single source of truth for new consumers; pg-am-hnsw must not depend on
// pg-am-heap, tech-selection §2). The golden pin in this file's tests
// keeps freezing the shared byte layout across the crates.
// ---------------------------------------------------------------------

/// Read the line pointer at `slot`; returns `(offset, length)` of the
/// entry, or `None` when the slot does not exist or is not `LP_NORMAL`.
///
/// Page-content bounds (2026-09-14, Stage A review round 2 P2-1): pages
/// carry no checksum, so every value READ FROM the page is untrusted —
/// `pd_lower` itself and the LP's off/len are clamped to the page size
/// before any slice is formed (redo-path no-panic discipline, error.rs:
/// corrupted bytes must never panic).
///
/// Tuple-region bounds (2026-09-15, round 3 P2): page-size clamping alone
/// let a forged LP point into the header / LP array / free space and have
/// publish-live / tombstone / neighbor writes land there. The tuple region
/// is `[pd_upper, pd_special)` — clamp both header values, then require the
/// entry to lie inside it.
fn read_lp(page: &[u8; PAGE_SIZE], slot: u16) -> Option<(u16, u16)> {
    let pd_lower = u16::from_le_bytes(page[14..16].try_into().unwrap()) as usize;
    if !(PAGE_HEADER_SIZE..=PAGE_SIZE).contains(&pd_lower)
        || (pd_lower - PAGE_HEADER_SIZE) % LINE_POINTER_SIZE != 0
    {
        return None;
    }
    let pd_upper = u16::from_le_bytes(page[16..18].try_into().unwrap()) as usize;
    let pd_special = u16::from_le_bytes(page[18..20].try_into().unwrap()) as usize;
    if pd_upper < pd_lower || pd_upper > pd_special || pd_special > PAGE_SIZE {
        return None;
    }
    let idx = PAGE_HEADER_SIZE + usize::from(slot) * LINE_POINTER_SIZE;
    if idx + LINE_POINTER_SIZE > pd_lower {
        return None;
    }
    let (off, len, is_normal) = decode_line_pointer(page[idx..idx + 4].try_into().unwrap());
    if !is_normal {
        return None;
    }
    let (off, len) = (usize::from(off), usize::from(len));
    if off < pd_upper || off + len > pd_special {
        return None;
    }
    Some((off as u16, len as u16))
}

fn write_lp(page: &mut [u8; PAGE_SIZE], slot: u16, off: u16, len: u16) {
    let idx = PAGE_HEADER_SIZE + usize::from(slot) * LINE_POINTER_SIZE;
    page[idx..idx + 4].copy_from_slice(&encode_line_pointer(off, len));
}

fn pd_lower(page: &[u8; PAGE_SIZE]) -> u16 {
    u16::from_le_bytes(page[14..16].try_into().unwrap())
}

fn pd_upper(page: &[u8; PAGE_SIZE]) -> u16 {
    u16::from_le_bytes(page[16..18].try_into().unwrap())
}

/// pd_special (bytes 18..20 of the 32-byte header) — the end of the tuple
/// area; AM-private space starts there.
fn pd_special(page: &[u8; PAGE_SIZE]) -> u16 {
    u16::from_le_bytes(page[18..20].try_into().unwrap())
}

fn set_pd_lower(page: &mut [u8; PAGE_SIZE], v: u16) {
    page[14..16].copy_from_slice(&v.to_le_bytes());
}

fn set_pd_upper(page: &mut [u8; PAGE_SIZE], v: u16) {
    page[16..18].copy_from_slice(&v.to_le_bytes());
}

// ---------------------------------------------------------------------
// Node-entry layout (tech-selection §7.2 — fixed-size by drawn level):
//
// ```text
// entry := vector:f32[dim] | state:u8 | per-level { count:u16 | neighbors:u32[cap] }
// state := top_level:6 (bits 0-5) | state:1 (bit 6: 0=INITIALIZING, 1=LIVE)
//          | tombstone:1 (bit 7)
// cap(level 0) = m_max0, cap(level > 0) = m   — always fully RESERVED;
// only the first `count` ids are live, the rest stay zero.
// ```
// ---------------------------------------------------------------------

/// The level's reserved region inside `entry`, geometry-checked
/// (2026-09-14, review P2-2): the (dim, level) geometry must fit the
/// entry's actual reserved length — a mismatched dim or a level above the
/// entry's top_level would slice out of bounds (panic in debug AND
/// release), so reject loudly first.
fn checked_region<'a>(
    entry: &'a [u8],
    geo: NodeGeometry,
    level: u8,
    what: &str,
) -> Result<&'a [u8]> {
    let (region_off, cap) = level_region_pos(geo, level);
    let region_end = region_off + level_region_size(cap);
    if region_end > entry.len() {
        return Err(HnswError::Corrupted(format!(
            "{what}: level {level} region [{region_off}..{region_end}) exceeds the {}-byte entry (dim/level geometry mismatch)",
            entry.len()
        )));
    }
    Ok(&entry[region_off..region_end])
}

/// Mutable variant of [`checked_region`].
fn checked_region_mut<'a>(
    entry: &'a mut [u8],
    geo: NodeGeometry,
    level: u8,
    what: &str,
) -> Result<&'a mut [u8]> {
    let (region_off, cap) = level_region_pos(geo, level);
    let region_end = region_off + level_region_size(cap);
    if region_end > entry.len() {
        return Err(HnswError::Corrupted(format!(
            "{what}: level {level} region [{region_off}..{region_end}) exceeds the {}-byte entry (dim/level geometry mismatch)",
            entry.len()
        )));
    }
    Ok(&mut entry[region_off..region_end])
}

/// The region's live-neighbor count, capacity-checked (a corrupt count
/// above the reserved capacity is loud, never an out-of-region read).
fn checked_count(region: &[u8], geo: NodeGeometry, level: u8, what: &str) -> Result<usize> {
    let (_, cap) = level_region_pos(geo, level);
    let count = u16::from_le_bytes(region[..2].try_into().unwrap()) as usize;
    if count > usize::from(cap) {
        return Err(HnswError::Corrupted(format!(
            "{what}: stored count {count} exceeds reserved capacity {cap} of level {level}"
        )));
    }
    Ok(count)
}

fn entry_at<'a>(page: &'a [u8; PAGE_SIZE], slot: u16, what: &str) -> Result<&'a [u8]> {
    let (off, len) = read_lp(page, slot).ok_or_else(|| {
        HnswError::Corrupted(format!(
            "{what}: slot {slot} is not a live entry on this node page"
        ))
    })?;
    Ok(&page[usize::from(off)..usize::from(off) + usize::from(len)])
}

fn entry_at_mut<'a>(page: &'a mut [u8; PAGE_SIZE], slot: u16, what: &str) -> Result<&'a mut [u8]> {
    let (off, len) = read_lp(page, slot).ok_or_else(|| {
        HnswError::Corrupted(format!(
            "{what}: slot {slot} is not a live entry on this node page"
        ))
    })?;
    Ok(&mut page[usize::from(off)..usize::from(off) + usize::from(len)])
}

/// The entry's state byte, bounds-checked (2026-09-14, Stage A review
/// P2-2): a `dim` that disagrees with the entry's actual reserved length
/// would index out of bounds (slice panic) — reject loudly instead.
fn state_byte<'a>(entry: &'a [u8], dim: u16, what: &str) -> Result<&'a u8> {
    entry.get(state_offset(dim)).ok_or_else(|| {
        HnswError::Corrupted(format!(
            "{what}: state offset {} exceeds the {}-byte entry (dim geometry mismatch)",
            state_offset(dim),
            entry.len()
        ))
    })
}

/// Mutable variant of [`state_byte`].
fn state_byte_mut<'a>(entry: &'a mut [u8], dim: u16, what: &str) -> Result<&'a mut u8> {
    let off = state_offset(dim);
    let len = entry.len();
    entry.get_mut(off).ok_or_else(|| {
        HnswError::Corrupted(format!(
            "{what}: state offset {off} exceeds the {len}-byte entry (dim geometry mismatch)"
        ))
    })
}

// ---------------------------------------------------------------------
// The seven primitives.
// ---------------------------------------------------------------------

/// Slotted-page bounds for the append path (2026-09-15, Stage A review
/// round 3 P2): `pd_lower` must be header-sized AND 4-byte-aligned (LP
/// array entries are 4 bytes — a torn `pd_lower` would silently round the
/// slot count), with `pd_lower <= pd_upper <= PAGE_SIZE`. Shared by
/// [`select_slot`] and [`apply_node_at`].
fn append_bounds(page: &[u8; PAGE_SIZE], what: &str) -> Result<(usize, usize)> {
    let lower = pd_lower(page) as usize;
    let upper = pd_upper(page) as usize;
    let special = pd_special(page) as usize;
    if lower < PAGE_HEADER_SIZE
        || (lower - PAGE_HEADER_SIZE) % LINE_POINTER_SIZE != 0
        || upper < lower
        || upper > PAGE_SIZE
    {
        return Err(HnswError::Corrupted(format!(
            "{what}: page is not slotted-initialized (pd_lower={lower}, pd_upper={upper})"
        )));
    }
    // 2026-09-15, Stage A review round 5 P2 item 1: the tuple area is
    // [upper, special) — a corrupt pd_special outside that interval would
    // make the append math trust phantom free space (same clamp discipline
    // as read_lp's tuple-area check).
    if special < upper || special > PAGE_SIZE {
        return Err(HnswError::Corrupted(format!(
            "{what}: pd_special {special} outside [pd_upper {upper}, {PAGE_SIZE}]"
        )));
    }
    Ok((lower, upper))
}

/// §8.1 step 3, SELECTION form (2026-09-15, round 3 P1-1 — WAL-first):
/// pick the slot a `len`-byte entry would occupy WITHOUT modifying the
/// page. The normal path calls this first, carries the returned slot in
/// the WAL record, appends + flushes the record, and only then applies
/// [`apply_node_at`] — selection → WAL → application. Redo takes the
/// record's slot as authoritative and calls `apply_node_at` directly.
///
/// Same failure contract as the old allocate-then-write form:
/// `InvalidOperation` when the page has no room (the caller allocates a
/// fresh page per §8.1 step 1).
pub(crate) fn select_slot(node_page: &[u8; PAGE_SIZE], len: usize) -> Result<u16> {
    let (lower, upper) = append_bounds(node_page, "select_slot")?;
    if upper - lower < len + LINE_POINTER_SIZE {
        return Err(HnswError::InvalidOperation(format!(
            "node page has no room for a {len}-byte entry (free {} < {len} + 4 LP)",
            upper - lower
        )));
    }
    Ok(((lower - PAGE_HEADER_SIZE) / LINE_POINTER_SIZE) as u16)
}

/// §8.1 step 3: allocate a slot on `node_page` and create the node entry —
/// fixed-size reservation by `top_level` (level 0 reserved at `m_max0`,
/// upper levels at `m`, §7.2), state = INITIALIZING, every level's
/// `count = 0`. `node_id` is carried for symmetry with the WAL record (the
/// entry itself stores no NodeId — position is identity, §3).
///
/// Returns the allocated slot. Fails with `InvalidOperation` when the page
/// has no room (the caller then allocates a fresh page per §8.1 step 1).
///
/// Composition (2026-09-15, round 3 P1-1): `select_slot` + `apply_node_at`
/// — one implementation of the append, so the convenience wrapper can
/// never drift from the WAL-first pair the real paths use.
pub(crate) fn append_node(
    node_page: &mut [u8; PAGE_SIZE],
    node_id: u32,
    top_level: u8,
    geo: NodeGeometry,
    vector: &[f32],
) -> Result<u16> {
    let slot = select_slot(node_page, geo.entry_size(top_level))?;
    apply_node_at(node_page, slot, node_id, top_level, geo, vector)?;
    Ok(slot)
}

/// Write the INITIALIZING entry content shared by [`append_node`] and
/// [`apply_node_at`]: zero-fill, vector, state byte (top_level in bits
/// 0-5, INITIALIZING with bit 6 clear, no tombstone with bit 7 clear).
///
/// Structural guard (2026-09-15, round 3 P3-1): `top_level` is 6 bits
/// (state byte bits 0-5) — reject loudly instead of the old silent
/// `& 0x3F` mask (64 wrapped to 0, corrupting the entry's own level
/// structure). Checked BEFORE any byte is written.
fn write_entry(entry: &mut [u8], top_level: u8, dim: u16, vector: &[f32]) -> Result<()> {
    if top_level > 63 {
        return Err(HnswError::Corrupted(format!(
            "top_level {top_level} exceeds the 6-bit state-byte field (max 63)"
        )));
    }
    entry.fill(0);
    for (i, &x) in vector.iter().enumerate() {
        entry[4 * i..4 * i + 4].copy_from_slice(&x.to_le_bytes());
    }
    entry[state_offset(dim)] = top_level;
    Ok(())
}

/// §8.1 step 3, REDO form (2026-09-14, Stage A review P3-1): write the
/// node entry AT `slot` — create it when `slot` is the next free slot,
/// overwrite it when the existing entry is INITIALIZING (the idempotent-
/// replay form named by the §10.1 NodeInit checklist: "target slot free
/// or INITIALIZING"). [`append_node`] serves the normal path
/// (allocate-then-write); this form serves redo, where the record's
/// slot_id is authoritative. A LIVE target or a slot beyond the next free
/// one means the record does not match the page state — loud `Corrupted`
/// (an already-applied record must be skipped by the pd_lsn guard, never
/// overwritten).
pub(crate) fn apply_node_at(
    node_page: &mut [u8; PAGE_SIZE],
    slot: u16,
    _node_id: u32,
    top_level: u8,
    geo: NodeGeometry,
    vector: &[f32],
) -> Result<()> {
    // Structural guards, same class as append_node's (review P2-1/P2-2).
    if vector.len() != usize::from(geo.dim) {
        return Err(HnswError::Corrupted(format!(
            "apply_node_at: vector has {} components, dim is {}",
            vector.len(),
            geo.dim
        )));
    }
    let len = geo.entry_size(top_level);
    // Page-content bounds (review round 2 P2-1; round 3 P2 moved the clamp
    // into the shared `append_bounds`, adding the 4-byte pd_lower alignment
    // check).
    let (lower, upper) = append_bounds(node_page, "apply_node_at")?;
    let next_free = ((lower - PAGE_HEADER_SIZE) / LINE_POINTER_SIZE) as u16;
    match read_lp(node_page, slot) {
        Some((off, lp_len)) => {
            // Occupied: only the INITIALIZING overwrite is legal, and the
            // reserved length must match the record's geometry.
            if usize::from(lp_len) != len {
                return Err(HnswError::Corrupted(format!(
                    "apply_node_at: slot {slot} entry is {lp_len} bytes, record implies {len} (geometry mismatch)"
                )));
            }
            let entry = &mut node_page[usize::from(off)..usize::from(off) + usize::from(lp_len)];
            if state_byte(entry, geo.dim, "apply_node_at")? & STATE_LIVE_BIT != 0 {
                return Err(HnswError::Corrupted(format!(
                    "apply_node_at: slot {slot} is already LIVE — replay must be skipped by the pd_lsn guard, not overwritten"
                )));
            }
            write_entry(entry, top_level, geo.dim, vector)?;
        }
        None => {
            if slot != next_free {
                return Err(HnswError::Corrupted(format!(
                    "apply_node_at: record targets slot {slot} but the next free slot is {next_free} (gap or stale record)"
                )));
            }
            if upper - lower < len + LINE_POINTER_SIZE {
                return Err(HnswError::InvalidOperation(format!(
                    "node page has no room for a {len}-byte entry (free {} < {len} + 4 LP)",
                    upper - lower
                )));
            }
            let off = (upper - len) as u16;
            let entry = &mut node_page[usize::from(off)..usize::from(off) + len];
            write_entry(entry, top_level, geo.dim, vector)?;
            write_lp(node_page, slot, off, len as u16);
            set_pd_lower(node_page, (lower + LINE_POINTER_SIZE) as u16);
            set_pd_upper(node_page, off);
        }
    }
    Ok(())
}

/// §8.1 step 4: append the 10-byte mapping entry `(target_page, target_slot)`
/// for `node_id` at the directory tail page and bump its count (§7.1 entry
/// layout: `PageId u64 + SlotId u16`; entries are position-is-identity).
///
/// Fails with `InvalidOperation` when the tail page already holds
/// [`DIR_ENTRIES_PER_PAGE`] entries (the caller then DirLinks a fresh page,
/// §8.1 step 2).
///
/// Idempotence (2026-09-15, Stage A review round 3 P3): `node_id` is the
/// chain high-water mark this append publishes (v1.2: NodeId allocation ==
/// mapping publication), so it keys idempotent replay — the primitive
/// itself is N=3 byte-identical (coding-plan Stage A acceptance), not
/// merely pd_lsn-guarded at the handler:
/// - `hwm == node_id` → append (first application);
/// - `hwm >  node_id` → already applied → no-op, returns the entry's
///   position (replay is byte-identical);
/// - `hwm <  node_id` → gap → loud `Corrupted` (the record does not match
///   the chain state);
/// - `node_id` below this page's base → the record belongs to an earlier
///   chain page — loud `Corrupted`.
pub(crate) fn dir_append(
    dir_tail_page: &mut [u8; PAGE_SIZE],
    node_id: u32,
    target_page: PageId,
    target_slot: u16,
) -> Result<u32> {
    let count = dir_count(dir_tail_page);
    // Header sanity (round 3 P3): ordinal/count are read from the
    // (checksum-less) page — a corrupt count above capacity would make the
    // append math below slice out of the page, and the ordinal multiply
    // must not wrap (debug builds panic on overflow).
    if count > DIR_ENTRIES_PER_PAGE {
        return Err(HnswError::Corrupted(format!(
            "dir_append: directory header count {count} exceeds capacity {DIR_ENTRIES_PER_PAGE}"
        )));
    }
    let ordinal = dir_ordinal(dir_tail_page);
    let page_base = ordinal
        .checked_mul(u64::from(DIR_ENTRIES_PER_PAGE))
        .ok_or_else(|| {
            HnswError::Corrupted(format!(
                "dir_append: directory ordinal {ordinal} overflows the chain math"
            ))
        })?;
    // 2026-09-15, Stage A review round 5 P2 item 2: same checked-arithmetic
    // discipline as the ordinal multiply above — a wrapped high-water mark
    // would flip every comparison below into nonsense, loudly.
    let hwm = page_base.checked_add(u64::from(count)).ok_or_else(|| {
        HnswError::Corrupted(format!(
            "dir_append: high-water mark computation overflows (ordinal {ordinal}, count {count})"
        ))
    })?;
    let id = u64::from(node_id);
    if id < page_base {
        return Err(HnswError::Corrupted(format!(
            "dir_append: node_id {node_id} predates this directory page (ordinal {ordinal}, base {page_base})"
        )));
    }
    if id < hwm {
        // 2026-09-15, Stage A review round 5 P2 item 3: idempotent replay
        // must be byte-identical, not merely position-present — compare the
        // existing 10-byte entry with the record's (target_page,
        // target_slot). The position is guaranteed in-page: id - page_base
        // < count <= DIR_ENTRIES_PER_PAGE.
        let idx = (id - page_base) as usize;
        let off = PAGE_HEADER_SIZE + DIR_HEADER_SIZE + idx * 10;
        let existing_page = u64::from_le_bytes(dir_tail_page[off..off + 8].try_into().unwrap());
        let existing_slot =
            u16::from_le_bytes(dir_tail_page[off + 8..off + 10].try_into().unwrap());
        if existing_page != target_page.0 || existing_slot != target_slot {
            return Err(HnswError::Corrupted(format!(
                "dir_append: entry {node_id} already maps to ({existing_page}, {existing_slot}), record says ({}, {target_slot}) — the directory post-image disagrees with the record",
                target_page.0
            )));
        }
        // Idempotent replay: byte-identical re-application.
        return Ok((id - page_base) as u32);
    }
    if id > hwm {
        return Err(HnswError::Corrupted(format!(
            "dir_append: node_id {node_id} leaves a gap at the chain high-water mark {hwm}"
        )));
    }
    if count >= DIR_ENTRIES_PER_PAGE {
        return Err(HnswError::InvalidOperation(format!(
            "directory page is full ({count} entries, capacity {DIR_ENTRIES_PER_PAGE})"
        )));
    }
    let off = PAGE_HEADER_SIZE + DIR_HEADER_SIZE + count as usize * 10;
    dir_tail_page[off..off + 8].copy_from_slice(&target_page.0.to_le_bytes());
    dir_tail_page[off + 8..off + 10].copy_from_slice(&target_slot.to_le_bytes());
    dir_tail_page[DIR_OFF_COUNT..DIR_OFF_COUNT + 4].copy_from_slice(&(count + 1).to_le_bytes());
    Ok(count)
}

/// §8.1 steps 5/6: rewrite one level's neighbor list of the entry at
/// `slot` IN PLACE — `content` is the full new list (post-image), the
/// remaining reserved capacity is zero-filled (so repeated application is
/// byte-identical, §11.2 idempotence). The entry never moves or grows
/// (§7.2). Structural guard only: `content` must fit the reserved region.
pub(crate) fn set_neighbors(
    node_page: &mut [u8; PAGE_SIZE],
    slot: u16,
    geo: NodeGeometry,
    level: u8,
    content: &[u32],
) -> Result<()> {
    let (_, cap) = level_region_pos(geo, level);
    let entry = entry_at_mut(node_page, slot, "set_neighbors")?;
    if content.len() > usize::from(cap) {
        return Err(HnswError::Corrupted(format!(
            "set_neighbors: {} ids overflow the reserved capacity {cap} of level {level}",
            content.len()
        )));
    }
    let region = checked_region_mut(entry, geo, level, "set_neighbors")?;
    region[..2].copy_from_slice(&(content.len() as u16).to_le_bytes());
    region[2..].fill(0);
    for (i, &id) in content.iter().enumerate() {
        region[2 + 4 * i..2 + 4 * i + 4].copy_from_slice(&id.to_le_bytes());
    }
    Ok(())
}

/// §8.1 step 7: write the meta page's entry-point / max-level post-image.
pub(crate) fn apply_meta(meta_page: &mut [u8; PAGE_SIZE], entry_point: u32, max_level: u8) {
    meta_page[META_OFF_ENTRY_POINT..META_OFF_ENTRY_POINT + 4]
        .copy_from_slice(&entry_point.to_le_bytes());
    meta_page[META_OFF_MAX_LEVEL] = max_level;
}

/// §8.1 step 8: flip the entry's state bit INITIALIZING → LIVE (bit 6 of
/// the state byte, §7.2 bit layout).
pub(crate) fn publish_live(node_page: &mut [u8; PAGE_SIZE], slot: u16, dim: u16) -> Result<()> {
    let entry = entry_at_mut(node_page, slot, "publish_live")?;
    *state_byte_mut(entry, dim, "publish_live")? |= STATE_LIVE_BIT;
    Ok(())
}

/// §8.1 step 2: point the old directory tail's `next` at the freshly
/// allocated chain page (§7.1 self-describing header).
pub(crate) fn dir_link(old_tail_page: &mut [u8; PAGE_SIZE], new_dir_page: PageId) {
    old_tail_page[DIR_OFF_NEXT..DIR_OFF_NEXT + 8].copy_from_slice(&new_dir_page.0.to_le_bytes());
}

/// HnswNodeTombstone (124) application: set the entry's tombstone bit
/// (bit 7). Format only — the semantics (search filtering, reclamation)
/// land in M6 (tech-selection §1).
pub(crate) fn apply_tombstone(node_page: &mut [u8; PAGE_SIZE], slot: u16, dim: u16) -> Result<()> {
    let entry = entry_at_mut(node_page, slot, "apply_tombstone")?;
    *state_byte_mut(entry, dim, "apply_tombstone")? |= STATE_TOMBSTONE_BIT;
    Ok(())
}

// ---------------------------------------------------------------------
// Read-side accessors (tests + the Stage C/D consumers).
// ---------------------------------------------------------------------

/// The entry's top level (state byte bits 0-5).
pub(crate) fn entry_top_level(node_page: &[u8; PAGE_SIZE], slot: u16, dim: u16) -> Result<u8> {
    let entry = entry_at(node_page, slot, "entry_top_level")?;
    Ok(state_byte(entry, dim, "entry_top_level")? & 0x3F)
}

/// Whether the entry is LIVE (state byte bit 6).
pub(crate) fn entry_is_live(node_page: &[u8; PAGE_SIZE], slot: u16, dim: u16) -> Result<bool> {
    let entry = entry_at(node_page, slot, "entry_is_live")?;
    Ok(state_byte(entry, dim, "entry_is_live")? & STATE_LIVE_BIT != 0)
}

/// Whether the entry is tombstoned (state byte bit 7).
pub(crate) fn entry_is_tombstoned(
    node_page: &[u8; PAGE_SIZE],
    slot: u16,
    dim: u16,
) -> Result<bool> {
    let entry = entry_at(node_page, slot, "entry_is_tombstoned")?;
    Ok(state_byte(entry, dim, "entry_is_tombstoned")? & STATE_TOMBSTONE_BIT != 0)
}

/// Zero-allocation iterator over a level's live neighbor ids (2026-09-15,
/// Stage A review round 3 P3): the Vec-returning reader allocated one Vec
/// per call — unfit for the Stage C search hot path, where neighbor lists
/// are read per visited node per layer. This borrows the page; the Vec
/// form ([`entry_neighbors`]) is now a thin `.collect()` wrapper for
/// tests and cold callers.
#[derive(Debug)]
pub(crate) struct NeighborIter<'a> {
    region: &'a [u8],
    next: usize,
    count: usize,
}

impl Iterator for NeighborIter<'_> {
    type Item = u32;

    fn next(&mut self) -> Option<u32> {
        if self.next >= self.count {
            return None;
        }
        let i = self.next;
        self.next += 1;
        Some(u32::from_le_bytes(
            self.region[2 + 4 * i..2 + 4 * i + 4].try_into().unwrap(),
        ))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.count - self.next;
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for NeighborIter<'_> {}

/// Borrow `level`'s live neighbor ids of the entry at `slot` as an
/// iterator — the search hot-path form (see [`NeighborIter`]).
pub(crate) fn neighbor_iter(
    node_page: &[u8; PAGE_SIZE],
    slot: u16,
    geo: NodeGeometry,
    level: u8,
) -> Result<NeighborIter<'_>> {
    let entry = entry_at(node_page, slot, "neighbor_iter")?;
    let region = checked_region(entry, geo, level, "neighbor_iter")?;
    let count = checked_count(region, geo, level, "neighbor_iter")?;
    Ok(NeighborIter {
        region,
        next: 0,
        count,
    })
}

/// Read `level`'s live neighbor ids of the entry at `slot` (allocating
/// convenience wrapper over [`neighbor_iter`]).
pub(crate) fn entry_neighbors(
    node_page: &[u8; PAGE_SIZE],
    slot: u16,
    geo: NodeGeometry,
    level: u8,
) -> Result<Vec<u32>> {
    Ok(neighbor_iter(node_page, slot, geo, level)?.collect())
}

/// Zero-allocation iterator over the entry's vector components — same
/// hot-path rationale as [`NeighborIter`] (2026-09-15, round 3 P3).
#[derive(Debug)]
pub(crate) struct VectorIter<'a> {
    entry: &'a [u8],
    next: usize,
    dim: usize,
}

impl Iterator for VectorIter<'_> {
    type Item = f32;

    fn next(&mut self) -> Option<f32> {
        if self.next >= self.dim {
            return None;
        }
        let i = self.next;
        self.next += 1;
        Some(f32::from_le_bytes(
            self.entry[4 * i..4 * i + 4].try_into().unwrap(),
        ))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.dim - self.next;
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for VectorIter<'_> {}

/// Borrow the entry's vector (`dim` f32 components) as an iterator — the
/// search hot-path form (see [`VectorIter`]).
pub(crate) fn vector_iter(
    node_page: &[u8; PAGE_SIZE],
    slot: u16,
    dim: u16,
) -> Result<VectorIter<'_>> {
    let entry = entry_at(node_page, slot, "vector_iter")?;
    // Structural guard (2026-09-14, review P2-2): dim must fit the entry.
    if entry.len() < 4 * usize::from(dim) {
        return Err(HnswError::Corrupted(format!(
            "vector_iter: dim {dim} exceeds the {}-byte entry (dim geometry mismatch)",
            entry.len()
        )));
    }
    Ok(VectorIter {
        entry,
        next: 0,
        dim: usize::from(dim),
    })
}

/// Read the entry's vector (`dim` f32 components; allocating convenience
/// wrapper over [`vector_iter`]).
pub(crate) fn entry_vector(node_page: &[u8; PAGE_SIZE], slot: u16, dim: u16) -> Result<Vec<f32>> {
    Ok(vector_iter(node_page, slot, dim)?.collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::{dir_count, dir_next, init_dir_page, init_node_page};

    const DIM: u16 = 4;
    const M: u16 = 4;
    const M_MAX0: u16 = 8;
    const GEO: NodeGeometry = NodeGeometry {
        dim: DIM,
        m: M,
        m_max0: M_MAX0,
    };

    fn node_page() -> [u8; PAGE_SIZE] {
        let mut p = [0u8; PAGE_SIZE];
        init_node_page(&mut p);
        p
    }

    fn dir_page(ordinal: u64) -> [u8; PAGE_SIZE] {
        let mut p = [0u8; PAGE_SIZE];
        init_dir_page(&mut p, ordinal);
        p
    }

    #[test]
    fn append_node_creates_fixed_size_initializing_entry() {
        let mut page = node_page();
        let slot = append_node(&mut page, 7, 2, GEO, &[1.0, 2.0, 3.0, 4.0]).unwrap();
        assert_eq!(slot, 0);
        assert_eq!(entry_top_level(&page, slot, DIM).unwrap(), 2);
        assert!(!entry_is_live(&page, slot, DIM).unwrap());
        assert!(!entry_is_tombstoned(&page, slot, DIM).unwrap());
        assert_eq!(
            entry_vector(&page, slot, DIM).unwrap(),
            vec![1.0, 2.0, 3.0, 4.0]
        );
        for l in 0..=2u8 {
            assert!(entry_neighbors(&page, slot, GEO, l).unwrap().is_empty());
        }
        let slot2 = append_node(&mut page, 8, 0, GEO, &[9.0; 4]).unwrap();
        assert_eq!(slot2, 1);
    }

    #[test]
    fn dir_append_writes_10b_entries_and_counts_up() {
        let mut page = dir_page(0);
        assert_eq!(dir_append(&mut page, 0, PageId(11), 3).unwrap(), 0);
        assert_eq!(dir_append(&mut page, 1, PageId(12), 4).unwrap(), 1);
        assert_eq!(dir_count(&page), 2);
        let base = PAGE_HEADER_SIZE + DIR_HEADER_SIZE;
        assert_eq!(
            u64::from_le_bytes(page[base..base + 8].try_into().unwrap()),
            11
        );
        assert_eq!(
            u16::from_le_bytes(page[base + 8..base + 10].try_into().unwrap()),
            3
        );
    }

    #[test]
    fn dir_append_full_page_boundary() {
        let mut page = dir_page(0);
        for i in 0..DIR_ENTRIES_PER_PAGE {
            dir_append(&mut page, i, PageId(11), i as u16).unwrap();
        }
        assert_eq!(dir_count(&page), DIR_ENTRIES_PER_PAGE);
        // Entry 813 must fail loudly — the caller then DirLinks (§8.1 step 2).
        let err = dir_append(&mut page, 813, PageId(12), 0).unwrap_err();
        assert!(matches!(err, HnswError::InvalidOperation(_)));
        dir_link(&mut page, PageId(200));
        assert_eq!(dir_next(&page), PageId(200));
    }

    /// 2026-09-15, Stage A review round 5 P2 items 1-3: the new guards —
    /// pd_special interval check, checked high-water arithmetic, and the
    /// idempotent-replay post-image comparison.
    #[test]
    fn dir_append_round5_guards() {
        // Item 1: corrupt pd_special (below pd_upper) fails append_bounds
        // loudly even on a well-typed page.
        let mut page = node_page();
        page[18..20].copy_from_slice(&10u16.to_le_bytes()); // pd_special < pd_upper
        assert!(matches!(
            append_node(&mut page, 1, 0, GEO, &[1.0; 4]),
            Err(HnswError::Corrupted(_))
        ));

        // Item 2: an ordinal at the overflow edge of the chain math is a
        // loud Corrupted, never a wrapped high-water mark.
        let mut page = dir_page(u64::MAX / u64::from(DIR_ENTRIES_PER_PAGE) + 1);
        assert!(matches!(
            dir_append(&mut page, 0, PageId(11), 0),
            Err(HnswError::Corrupted(_))
        ));

        // Item 3a: idempotent replay with a MATCHING post-image passes
        // (byte-identical re-application).
        let mut page = dir_page(0);
        dir_append(&mut page, 0, PageId(11), 3).unwrap();
        dir_append(&mut page, 1, PageId(12), 4).unwrap();
        let before = page;
        // Re-apply the same two records (id < hwm): no-op, byte-identical.
        dir_append(&mut page, 0, PageId(11), 3).unwrap();
        dir_append(&mut page, 1, PageId(12), 4).unwrap();
        assert_eq!(page, before);

        // Item 3b: replay with a DISAGREEING post-image is a loud Corrupted.
        let err = dir_append(&mut page, 1, PageId(99), 7).unwrap_err();
        assert!(matches!(err, HnswError::Corrupted(_)));
        assert!(err.to_string().contains("post-image disagrees"), "{err}");
    }

    #[test]
    fn set_neighbors_in_place_and_idempotent_overwrite() {
        let mut page = node_page();
        let slot = append_node(&mut page, 7, 0, GEO, &[1.0; 4]).unwrap();
        set_neighbors(&mut page, slot, GEO, 0, &[1, 5, 9]).unwrap();
        assert_eq!(entry_neighbors(&page, slot, GEO, 0).unwrap(), vec![1, 5, 9]);
        // §11.2: same post-image applied N=3 -> byte-identical page.
        let before = page;
        for _ in 0..3 {
            set_neighbors(&mut page, slot, GEO, 0, &[1, 5, 9]).unwrap();
        }
        assert_eq!(page, before);
        // Shrink to a shorter list: the freed tail must be zeroed (byte-exact
        // idempotence would otherwise leak the old ids).
        set_neighbors(&mut page, slot, GEO, 0, &[2]).unwrap();
        let after_shrink = page;
        set_neighbors(&mut page, slot, GEO, 0, &[2]).unwrap();
        assert_eq!(page, after_shrink);
        assert_eq!(entry_neighbors(&page, slot, GEO, 0).unwrap(), vec![2]);
    }

    #[test]
    fn set_neighbors_rejects_overflow_and_missing_slot() {
        let mut page = node_page();
        let slot = append_node(&mut page, 7, 0, GEO, &[1.0; 4]).unwrap();
        // Capacity of level 0 is m_max0 = 8; 9 ids overflow (structural
        // guard, not funnel validation).
        let err = set_neighbors(&mut page, slot, GEO, 0, &[1, 2, 3, 4, 5, 6, 7, 8, 9]).unwrap_err();
        assert!(matches!(err, HnswError::Corrupted(_)));
        assert!(set_neighbors(&mut page, 99, GEO, 0, &[1]).is_err());
    }

    #[test]
    fn set_neighbors_full_capacity_and_top_level_boundaries() {
        let mut page = node_page();
        // top_level = 63: the bit layout's maximum (6 bits).
        let slot = append_node(&mut page, 1, 63, GEO, &[1.0; 4]).unwrap();
        assert_eq!(entry_top_level(&page, slot, DIM).unwrap(), 63);
        let full0: Vec<u32> = (0..M_MAX0 as u32).collect();
        set_neighbors(&mut page, slot, GEO, 0, &full0).unwrap();
        assert_eq!(
            entry_neighbors(&page, slot, GEO, 0).unwrap().len(),
            M_MAX0 as usize
        );
        let full_l: Vec<u32> = (0..M as u32).collect();
        set_neighbors(&mut page, slot, GEO, 63, &full_l).unwrap();
        assert_eq!(
            entry_neighbors(&page, slot, GEO, 63).unwrap().len(),
            M as usize
        );
    }

    #[test]
    fn publish_live_and_tombstone_flip_state_bits_idempotently() {
        let mut page = node_page();
        let slot = append_node(&mut page, 7, 0, GEO, &[1.0; 4]).unwrap();
        assert!(!entry_is_live(&page, slot, DIM).unwrap());
        publish_live(&mut page, slot, DIM).unwrap();
        assert!(entry_is_live(&page, slot, DIM).unwrap());
        let before = page;
        for _ in 0..3 {
            publish_live(&mut page, slot, DIM).unwrap();
        }
        assert_eq!(page, before);

        apply_tombstone(&mut page, slot, DIM).unwrap();
        assert!(entry_is_tombstoned(&page, slot, DIM).unwrap());
        // Tombstone does not clear LIVE (independent bits, §7.2).
        assert!(entry_is_live(&page, slot, DIM).unwrap());
        let before = page;
        for _ in 0..3 {
            apply_tombstone(&mut page, slot, DIM).unwrap();
        }
        assert_eq!(page, before);
    }

    #[test]
    fn apply_meta_writes_entry_point_and_max_level() {
        let mut page = [0u8; PAGE_SIZE];
        apply_meta(&mut page, 42, 3);
        assert_eq!(
            u32::from_le_bytes(
                page[META_OFF_ENTRY_POINT..META_OFF_ENTRY_POINT + 4]
                    .try_into()
                    .unwrap()
            ),
            42
        );
        assert_eq!(page[META_OFF_MAX_LEVEL], 3);
        let before = page;
        for _ in 0..3 {
            apply_meta(&mut page, 42, 3);
        }
        assert_eq!(page, before);
    }

    // -----------------------------------------------------------------
    // 2026-09-14, Stage A review round 1: structural-guard pins (P2-1 /
    // P2-2), the redo form `apply_node_at` (P3-1), the 813 derivation
    // (P3-4), and the LP layout golden pin (adjudication ②).
    // -----------------------------------------------------------------

    #[test]
    fn append_node_rejects_wrong_length_vector_loudly() {
        // P2-1: dim+1 would corrupt the state byte / level-0 region (and a
        // large-enough len would panic); dim-1 would zero-pad a wrong
        // vector. Both must fail loudly, NOT panic.
        let mut page = node_page();
        assert!(matches!(
            append_node(&mut page, 7, 0, GEO, &[1.0; 5]).unwrap_err(),
            HnswError::Corrupted(_)
        ));
        assert!(matches!(
            append_node(&mut page, 7, 0, GEO, &[1.0; 3]).unwrap_err(),
            HnswError::Corrupted(_)
        ));
        // And an uninitialized page must not underflow the slot math.
        let mut raw = [0u8; PAGE_SIZE];
        assert!(matches!(
            append_node(&mut raw, 7, 0, GEO, &[1.0; 4]).unwrap_err(),
            HnswError::Corrupted(_)
        ));
    }

    #[test]
    fn geometry_mismatch_is_loud_never_a_panic() {
        // P2-2: (dim, level) geometry that disagrees with the entry's
        // reserved length must fail loudly in every accessor/mutator.
        let mut page = node_page();
        let slot = append_node(&mut page, 7, 1, GEO, &[1.0; 4]).unwrap();
        // level above the entry's top_level (region would exceed the entry).
        assert!(matches!(
            set_neighbors(&mut page, slot, GEO, 2, &[1]).unwrap_err(),
            HnswError::Corrupted(_)
        ));
        assert!(matches!(
            entry_neighbors(&page, slot, GEO, 2).unwrap_err(),
            HnswError::Corrupted(_)
        ));
        // dim large enough that the state byte / vector would be OOB.
        // (A wrong dim that still lands in-bounds — e.g. DIM+4 — is NOT
        // structurally detectable: the entry stores no dim (§7.2), so that
        // case is funnel territory — meta.dim validation — not the
        // buffer-overrun invariant's.)
        let big_dim = 20;
        assert!(matches!(
            publish_live(&mut page, slot, big_dim).unwrap_err(),
            HnswError::Corrupted(_)
        ));
        assert!(matches!(
            entry_top_level(&page, slot, big_dim).unwrap_err(),
            HnswError::Corrupted(_)
        ));
        assert!(matches!(
            entry_vector(&page, slot, big_dim).unwrap_err(),
            HnswError::Corrupted(_)
        ));
        assert!(matches!(
            apply_tombstone(&mut page, slot, big_dim).unwrap_err(),
            HnswError::Corrupted(_)
        ));
    }

    #[test]
    fn apply_node_at_redo_form_create_overwrite_and_rejections() {
        // P3-1: create at the record's slot, INITIALIZING overwrite is the
        // idempotent replay form (byte-identical), LIVE / gap are loud.
        let mut page = node_page();
        apply_node_at(&mut page, 0, 7, 1, GEO, &[1.0, 2.0, 3.0, 4.0]).unwrap();
        assert_eq!(entry_top_level(&page, 0, DIM).unwrap(), 1);
        assert!(!entry_is_live(&page, 0, DIM).unwrap());

        // Gap: record targets slot 2 while the next free slot is 1.
        assert!(matches!(
            apply_node_at(&mut page, 2, 8, 0, GEO, &[9.0; 4]).unwrap_err(),
            HnswError::Corrupted(_)
        ));

        // INITIALIZING overwrite ×3 = byte-identical idempotent replay.
        let mut replayed = page;
        for _ in 0..3 {
            apply_node_at(&mut replayed, 0, 7, 1, GEO, &[1.0, 2.0, 3.0, 4.0]).unwrap();
        }
        assert_eq!(replayed, page);

        // LIVE target: replay must be skipped by the pd_lsn guard, so
        // reaching the primitive with a LIVE entry is a contract violation.
        publish_live(&mut page, 0, DIM).unwrap();
        assert!(matches!(
            apply_node_at(&mut page, 0, 7, 1, GEO, &[1.0, 2.0, 3.0, 4.0]).unwrap_err(),
            HnswError::Corrupted(_)
        ));
    }

    #[test]
    fn dir_entries_per_page_matches_the_format_constant() {
        // P3-4: derived, not a literal — 813 = ⌊(8192 − 32 − 24) / 10⌋.
        assert_eq!(DIR_ENTRIES_PER_PAGE, 813);
    }

    #[test]
    fn line_pointer_layout_golden_pin() {
        // 2026-09-15 Stage B slice 1: the LP layout's single source of
        // truth is now pg-storage's `page` module — this pin freezes the
        // shared byte layout ACROSS the crates (off:15 | flags:2 |
        // len:15, LP_NORMAL = 1); a drift on either side turns red here.
        // The formulaic derivation is intentionally NOT used as the
        // expectation (a broken implementation would drift in lockstep).
        assert_eq!(
            u32::from_le_bytes(encode_line_pointer(8000, 96)),
            0x00C0_9F40
        );
        let mut page = node_page();
        let off = 8000u16;
        let len = 96u16;
        write_lp(&mut page, 0, off, len);
        let raw = u32::from_le_bytes(
            page[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + 4]
                .try_into()
                .unwrap(),
        );
        // Literal golden (2026-09-14 review round 2 P3-3): a formulaic
        // expectation derived with the same expression as write_lp would
        // drift in lockstep with a broken implementation — the literal
        // 0x00C0_9F40 is off=8000(0x1F40) | LP_NORMAL<<15 | len=96<<17,
        // computed once from pg-am-heap/src/line_pointer.rs:5-9.
        assert_eq!(raw, 0x00C0_9F40);
        // read_lp only sees LPs below pd_lower — bump it as a real
        // allocation would before asserting the read-side roundtrip; the
        // tuple-region check (2026-09-15 round 3 P2) also requires the
        // entry to lie in [pd_upper, pd_special), so lower pd_upper too.
        set_pd_lower(&mut page, (PAGE_HEADER_SIZE + LINE_POINTER_SIZE) as u16);
        set_pd_upper(&mut page, off);
        assert_eq!(read_lp(&page, 0), Some((off, len)));
    }

    // -----------------------------------------------------------------
    // 2026-09-15, Stage A review round 3: top_level loud reject (P3-1),
    // WAL-first select_slot (P1-1), tuple-region + pd_lower alignment
    // guards (P2), dir_append node_id-keyed idempotence (P3),
    // zero-allocation iterators (P3).
    // -----------------------------------------------------------------

    #[test]
    fn write_entry_rejects_top_level_above_63_loudly() {
        // P3-1: the old `& 0x3F` silently wrapped 64 → 0. Now a loud
        // Corrupted BEFORE any byte is written (page untouched).
        let mut page = node_page();
        let pristine = page;
        assert!(matches!(
            append_node(&mut page, 7, 64, GEO, &[1.0; 4]).unwrap_err(),
            HnswError::Corrupted(_)
        ));
        assert_eq!(page, pristine);
        assert!(matches!(
            apply_node_at(&mut page, 0, 7, 100, GEO, &[1.0; 4]).unwrap_err(),
            HnswError::Corrupted(_)
        ));
        assert_eq!(page, pristine);
        // 63 remains the legal maximum (the boundary test above pins it).
        assert!(append_node(&mut page, 7, 63, GEO, &[1.0; 4]).is_ok());
    }

    #[test]
    fn select_slot_is_pure_and_feeds_the_wal_first_flow() {
        // P1-1: selection must not modify the page — the normal path is
        // select → WAL append/flush → apply_node_at.
        let mut page = node_page();
        let pristine = page;
        let len = GEO.entry_size(1);
        assert_eq!(select_slot(&page, len).unwrap(), 0);
        assert_eq!(select_slot(&page, len).unwrap(), 0); // pure: same answer
        assert_eq!(page, pristine);
        // The selected slot goes into the record; application lands there.
        apply_node_at(&mut page, 0, 7, 1, GEO, &[1.0; 4]).unwrap();
        assert_eq!(select_slot(&page, len).unwrap(), 1);
        // append_node is exactly the composition (same end state).
        let mut composed = pristine;
        let slot = append_node(&mut composed, 7, 1, GEO, &[1.0; 4]).unwrap();
        assert_eq!(slot, 0);
        assert_eq!(composed, page);
        // A len that cannot fit → InvalidOperation (the caller DirLinks a
        // fresh page, §8.1 step 1).
        assert!(matches!(
            select_slot(&page, PAGE_SIZE).unwrap_err(),
            HnswError::InvalidOperation(_)
        ));
    }

    #[test]
    fn append_paths_reject_misaligned_or_torn_headers_loudly() {
        // P2: a pd_lower that is not 4-byte aligned would silently round
        // the slot count — loud, never a panic.
        let mut page = node_page();
        page[14..16].copy_from_slice(&35u16.to_le_bytes()); // 32 + 3
        assert!(matches!(
            select_slot(&page, GEO.entry_size(0)).unwrap_err(),
            HnswError::Corrupted(_)
        ));
        assert!(matches!(
            append_node(&mut page, 7, 0, GEO, &[1.0; 4]).unwrap_err(),
            HnswError::Corrupted(_)
        ));
        assert!(matches!(
            apply_node_at(&mut page, 0, 7, 0, GEO, &[1.0; 4]).unwrap_err(),
            HnswError::Corrupted(_)
        ));
    }

    #[test]
    fn read_lp_rejects_entries_outside_the_tuple_region() {
        // P2: a forged LP pointing into the LP array / free space / past
        // pd_special must not become a publish/tombstone/neighbor target.
        let mut page = node_page();
        let slot = append_node(&mut page, 7, 0, GEO, &[1.0; 4]).unwrap();
        let (real_off, real_len) = read_lp(&page, slot).unwrap();
        let forged = [
            pd_lower(&page),        // inside the LP array
            pd_upper(&page) - 4,    // inside the free space
            (PAGE_SIZE - 4) as u16, // off+len overruns pd_special
        ];
        for off in forged {
            write_lp(&mut page, slot, off, real_len);
            assert!(matches!(
                entry_top_level(&page, slot, DIM).unwrap_err(),
                HnswError::Corrupted(_)
            ));
            assert!(matches!(
                publish_live(&mut page, slot, DIM).unwrap_err(),
                HnswError::Corrupted(_)
            ));
        }
        // Restore: the real entry still reads fine.
        write_lp(&mut page, slot, real_off, real_len);
        assert_eq!(entry_top_level(&page, slot, DIM).unwrap(), 0);
    }

    #[test]
    fn dir_append_is_node_id_keyed_idempotent() {
        // P3: node_id IS the chain HWM — replay skips, gaps are loud.
        let mut page = dir_page(0);
        dir_append(&mut page, 0, PageId(11), 0).unwrap();
        dir_append(&mut page, 1, PageId(11), 1).unwrap();
        dir_append(&mut page, 2, PageId(12), 0).unwrap();
        // N=3 replay of the node_id=1 record: byte-identical, and it
        // returns the entry's position.
        let before = page;
        for _ in 0..3 {
            assert_eq!(dir_append(&mut page, 1, PageId(11), 1).unwrap(), 1);
        }
        assert_eq!(page, before);
        // Gap: a node_id ahead of the HWM is loud.
        assert!(matches!(
            dir_append(&mut page, 5, PageId(13), 0).unwrap_err(),
            HnswError::Corrupted(_)
        ));
        // The next legal id still appends.
        assert_eq!(dir_append(&mut page, 3, PageId(13), 0).unwrap(), 3);
        // A record belonging to an EARLIER chain page is loud here...
        let mut later = dir_page(1);
        assert!(matches!(
            dir_append(&mut later, 0, PageId(11), 0).unwrap_err(),
            HnswError::Corrupted(_)
        ));
        // ...while the page's first legal id (ordinal 1 × 813) lands at 0.
        assert_eq!(
            dir_append(&mut later, DIR_ENTRIES_PER_PAGE, PageId(20), 0).unwrap(),
            0
        );
        // A corrupt header count (> capacity) is loud, never an OOB write.
        let mut corrupt = dir_page(0);
        corrupt[DIR_OFF_COUNT..DIR_OFF_COUNT + 4].copy_from_slice(&5000u32.to_le_bytes());
        assert!(matches!(
            dir_append(&mut corrupt, 0, PageId(11), 0).unwrap_err(),
            HnswError::Corrupted(_)
        ));
    }

    #[test]
    fn dir_append_replay_on_a_full_page_skips_without_error() {
        let mut page = dir_page(0);
        for i in 0..DIR_ENTRIES_PER_PAGE {
            dir_append(&mut page, i, PageId(11), i as u16).unwrap();
        }
        let before = page;
        // Replaying an already-applied record against the full page is a
        // no-op — NOT InvalidOperation (the chain has since moved on).
        assert_eq!(
            dir_append(&mut page, DIR_ENTRIES_PER_PAGE - 1, PageId(11), 812).unwrap(),
            DIR_ENTRIES_PER_PAGE - 1
        );
        assert_eq!(page, before);
    }

    #[test]
    fn dir_link_is_byte_idempotent() {
        let mut page = dir_page(0);
        dir_link(&mut page, PageId(200));
        let before = page;
        for _ in 0..3 {
            dir_link(&mut page, PageId(200));
        }
        assert_eq!(page, before);
    }

    #[test]
    fn iterators_match_the_vec_readers() {
        // P3: NeighborIter/VectorIter are the hot-path zero-allocation
        // forms; they must yield exactly what the Vec wrappers collect.
        let mut page = node_page();
        let slot = append_node(&mut page, 7, 2, GEO, &[1.0, 2.0, 3.0, 4.0]).unwrap();
        set_neighbors(&mut page, slot, GEO, 0, &[1, 5, 9]).unwrap();
        set_neighbors(&mut page, slot, GEO, 2, &[3, 7]).unwrap();
        let mut it = neighbor_iter(&page, slot, GEO, 0).unwrap();
        assert_eq!(it.len(), 3);
        assert_eq!(it.by_ref().collect::<Vec<_>>(), vec![1, 5, 9]);
        assert_eq!(it.len(), 0);
        assert_eq!(it.next(), None);
        assert_eq!(
            neighbor_iter(&page, slot, GEO, 2)
                .unwrap()
                .collect::<Vec<_>>(),
            vec![3, 7]
        );
        assert_eq!(neighbor_iter(&page, slot, GEO, 1).unwrap().count(), 0);
        let v: Vec<f32> = vector_iter(&page, slot, DIM).unwrap().collect();
        assert_eq!(v, vec![1.0, 2.0, 3.0, 4.0]);
        assert_eq!(entry_neighbors(&page, slot, GEO, 0).unwrap(), vec![1, 5, 9]);
        // Geometry mismatch is loud through the iterator constructors too
        // (dim 30 → 120 bytes > the 87-byte top_level=2 entry).
        assert!(matches!(
            neighbor_iter(&page, slot, GEO, 3).unwrap_err(),
            HnswError::Corrupted(_)
        ));
        assert!(matches!(
            vector_iter(&page, slot, 30).unwrap_err(),
            HnswError::Corrupted(_)
        ));
    }
}
