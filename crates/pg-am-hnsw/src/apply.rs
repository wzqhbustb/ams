//! Physical application primitives — Phase 2 M5 Stage A (tech-selection
//! §10.2 task 2, v1.7 signatures; coding plan Stage A).
//!
//! Seven `pub(crate)` primitives, one per WAL record type (121–127), shared
//! by the redo handlers (Stage C) and the normal write path (Stage C):
//!
//! - [`append_node`] — §8.1 step 3: create a node entry (fixed-size
//!   reservation by drawn level, §7.2; state INITIALIZING);
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

use pg_storage::page::PAGE_HEADER_SIZE;
use pg_storage::types::{PageId, PAGE_SIZE};

use crate::error::{HnswError, Result};
use crate::page::{dir_count, DIR_HEADER_SIZE, DIR_OFF_COUNT, DIR_OFF_NEXT};

// ---------------------------------------------------------------------
// Line-pointer access (re-derived from the slotted-page line-pointer
// layout, pg-am-heap/src/line_pointer.rs:16 — 4 bytes, off:15 | flags:2 |
// len:15; `LP_NORMAL = 1`, PostgreSQL's LP_NORMAL value). pg-am-hnsw must
// NOT depend on pg-am-heap (tech-selection §2), so the layout is
// re-derived here — same discipline as pg-storage's
// `MAX_HEAP_CLEANUP_SLOTS` re-derivation (wal/record.rs:298; 2026-09-14
// review nano: an earlier draft cited record.rs:262-269, stale).
// ---------------------------------------------------------------------

/// Line-pointer size in bytes (layout contract).
const LINE_POINTER_SIZE: usize = 4;
/// `LP_NORMAL` — the slot points at a live tuple.
const LP_NORMAL: u32 = 1;

/// Directory entries per directory page (§7.1 format constant, derived —
/// 2026-09-14 review P3-4: not a bare literal, one source with the header
/// layout constants): `⌊(PAGE_SIZE − 32 PageHeader − 24 dir header) / 10⌋`.
pub(crate) const DIR_ENTRIES_PER_PAGE: u32 =
    ((PAGE_SIZE - PAGE_HEADER_SIZE - DIR_HEADER_SIZE) / 10) as u32;

/// Read the line pointer at `slot`; returns `(offset, length)` of the
/// entry, or `None` when the slot does not exist or is not `LP_NORMAL`.
///
/// Page-content bounds (2026-09-14, Stage A review round 2 P2-1): pages
/// carry no checksum, so every value READ FROM the page is untrusted —
/// `pd_lower` itself and the LP's off/len are clamped to the page size
/// before any slice is formed (redo-path no-panic discipline, error.rs:
/// corrupted bytes must never panic).
fn read_lp(page: &[u8; PAGE_SIZE], slot: u16) -> Option<(u16, u16)> {
    let pd_lower = u16::from_le_bytes(page[14..16].try_into().unwrap()) as usize;
    if !(PAGE_HEADER_SIZE..=PAGE_SIZE).contains(&pd_lower)
        || (pd_lower - PAGE_HEADER_SIZE) % LINE_POINTER_SIZE != 0
    {
        return None;
    }
    let idx = PAGE_HEADER_SIZE + usize::from(slot) * LINE_POINTER_SIZE;
    if idx + LINE_POINTER_SIZE > pd_lower {
        return None;
    }
    let raw = u32::from_le_bytes(page[idx..idx + 4].try_into().unwrap());
    if (raw >> 15) & 0x3 != LP_NORMAL {
        return None;
    }
    let (off, len) = ((raw & 0x7FFF) as usize, ((raw >> 17) & 0x7FFF) as usize);
    if off < PAGE_HEADER_SIZE || off + len > PAGE_SIZE {
        return None;
    }
    Some((off as u16, len as u16))
}

fn write_lp(page: &mut [u8; PAGE_SIZE], slot: u16, off: u16, len: u16) {
    let idx = PAGE_HEADER_SIZE + usize::from(slot) * LINE_POINTER_SIZE;
    let raw = (u32::from(off) & 0x7FFF) | (LP_NORMAL << 15) | ((u32::from(len) & 0x7FFF) << 17);
    page[idx..idx + 4].copy_from_slice(&raw.to_le_bytes());
}

fn pd_lower(page: &[u8; PAGE_SIZE]) -> u16 {
    u16::from_le_bytes(page[14..16].try_into().unwrap())
}

fn pd_upper(page: &[u8; PAGE_SIZE]) -> u16 {
    u16::from_le_bytes(page[16..18].try_into().unwrap())
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

const STATE_LIVE_BIT: u8 = 1 << 6;
const STATE_TOMBSTONE_BIT: u8 = 1 << 7;

/// The fixed-size geometry of a node entry (§7.2): vector dimension plus
/// the two capacity parameters — grouped because the entry-creating
/// primitives need all three and the flat triple pushed their arity past
/// clippy's limit (2026-09-14, Stage A review fix).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct NodeGeometry {
    /// Vector dimension.
    pub dim: u16,
    /// Upper-level neighbor capacity.
    pub m: u16,
    /// Level-0 neighbor capacity.
    pub m_max0: u16,
}

impl NodeGeometry {
    /// Fixed size of a node entry with `top_level` upper levels (§7.2
    /// capacity formula).
    pub(crate) fn entry_size(&self, top_level: u8) -> usize {
        entry_size(self.dim, self.m, self.m_max0, top_level)
    }
}

/// Fixed size of a node entry with `top_level` upper levels (§7.2 capacity
/// formula): `4·dim + 1 + (2 + 4·m_max0) + top_level·(2 + 4·m)`.
pub(crate) fn entry_size(dim: u16, m: u16, m_max0: u16, top_level: u8) -> usize {
    4 * usize::from(dim)
        + 1
        + level_region_size(m_max0)
        + usize::from(top_level) * level_region_size(m)
}

/// Reserved byte size of one level's neighbor region (`2 + 4·cap`).
fn level_region_size(cap: u16) -> usize {
    2 + 4 * usize::from(cap)
}

/// Byte offset of `level`'s neighbor region inside an entry.
fn level_offset(dim: u16, m: u16, m_max0: u16, level: u8) -> usize {
    debug_assert!(
        level >= 1,
        "level_offset is for upper levels; level 0 uses the base"
    );
    4 * usize::from(dim)
        + 1
        + level_region_size(m_max0)
        + usize::from(level - 1) * level_region_size(m)
}

const fn level_base_offset(dim: u16) -> usize {
    4 * dim as usize + 1
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

fn state_offset(dim: u16) -> usize {
    4 * usize::from(dim)
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

/// §8.1 step 3: allocate a slot on `node_page` and create the node entry —
/// fixed-size reservation by `top_level` (level 0 reserved at `m_max0`,
/// upper levels at `m`, §7.2), state = INITIALIZING, every level's
/// `count = 0`. `node_id` is carried for symmetry with the WAL record (the
/// entry itself stores no NodeId — position is identity, §3).
///
/// Returns the allocated slot. Fails with `InvalidOperation` when the page
/// has no room (the caller then allocates a fresh page per §8.1 step 1).
pub(crate) fn append_node(
    node_page: &mut [u8; PAGE_SIZE],
    _node_id: u32,
    top_level: u8,
    geo: NodeGeometry,
    vector: &[f32],
) -> Result<u16> {
    // Structural guards (2026-09-14, Stage A review P2-1/P2-2 — the
    // buffer-overrun invariant, NOT business validation, same class as
    // set_neighbors' capacity guard):
    // - a vector whose length differs from `dim` would silently corrupt the
    //   state byte / level-0 region (len > dim) or zero-pad a wrong vector
    //   (len < dim); a large-enough len would panic on the slice;
    // - an uninitialized page (pd_lower < header, or upper < lower) would
    //   underflow the slot/free computation below.
    if vector.len() != usize::from(geo.dim) {
        return Err(HnswError::Corrupted(format!(
            "append_node: vector has {} components, dim is {}",
            vector.len(),
            geo.dim
        )));
    }
    let len = geo.entry_size(top_level);
    let lower = pd_lower(node_page) as usize;
    let upper = pd_upper(node_page) as usize;
    // Page-content bounds (review round 2 P2-1): pd_lower/pd_upper are
    // read from the (checks um-less) page — clamp before the free/slot
    // math, never let a torn value slice out of the page.
    if lower < PAGE_HEADER_SIZE || upper < lower || upper > PAGE_SIZE {
        return Err(HnswError::Corrupted(format!(
            "append_node: page is not slotted-initialized (pd_lower={lower}, pd_upper={upper})"
        )));
    }
    if upper - lower < len + LINE_POINTER_SIZE {
        return Err(HnswError::InvalidOperation(format!(
            "node page has no room for a {len}-byte entry (free {} < {len} + 4 LP)",
            upper - lower
        )));
    }
    let slot = ((lower - PAGE_HEADER_SIZE) / LINE_POINTER_SIZE) as u16;
    let off = (upper - len) as u16;
    let entry = &mut node_page[usize::from(off)..usize::from(off) + len];
    write_entry(entry, top_level, geo.dim, vector);
    write_lp(node_page, slot, off, len as u16);
    set_pd_lower(node_page, (lower + LINE_POINTER_SIZE) as u16);
    set_pd_upper(node_page, off);
    Ok(slot)
}

/// Write the INITIALIZING entry content shared by [`append_node`] and
/// [`apply_node_at`]: zero-fill, vector, state byte (top_level in bits
/// 0-5, INITIALIZING with bit 6 clear, no tombstone with bit 7 clear).
fn write_entry(entry: &mut [u8], top_level: u8, dim: u16, vector: &[f32]) {
    entry.fill(0);
    for (i, &x) in vector.iter().enumerate() {
        entry[4 * i..4 * i + 4].copy_from_slice(&x.to_le_bytes());
    }
    entry[state_offset(dim)] = top_level & 0x3F;
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
    let lower = pd_lower(node_page) as usize;
    let upper = pd_upper(node_page) as usize;
    // Page-content bounds (review round 2 P2-1), same clamp as append_node.
    if lower < PAGE_HEADER_SIZE || upper < lower || upper > PAGE_SIZE {
        return Err(HnswError::Corrupted(format!(
            "apply_node_at: page is not slotted-initialized (pd_lower={lower}, pd_upper={upper})"
        )));
    }
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
            write_entry(entry, top_level, geo.dim, vector);
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
            write_entry(entry, top_level, geo.dim, vector);
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
pub(crate) fn dir_append(
    dir_tail_page: &mut [u8; PAGE_SIZE],
    _node_id: u32,
    target_page: PageId,
    target_slot: u16,
) -> Result<u32> {
    let count = dir_count(dir_tail_page);
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
    let (region_off, cap) = if level == 0 {
        (level_base_offset(geo.dim), geo.m_max0)
    } else {
        (level_offset(geo.dim, geo.m, geo.m_max0, level), geo.m)
    };
    let entry = entry_at_mut(node_page, slot, "set_neighbors")?;
    if content.len() > usize::from(cap) {
        return Err(HnswError::Corrupted(format!(
            "set_neighbors: {} ids overflow the reserved capacity {cap} of level {level}",
            content.len()
        )));
    }
    // Structural guard (2026-09-14, review P2-2): the (dim, level)
    // geometry must fit the entry's actual reserved length — a mismatched
    // dim or a level above the entry's top_level would slice out of bounds
    // (panic in debug AND release), so reject loudly first.
    let region_end = region_off + level_region_size(cap);
    if region_end > entry.len() {
        return Err(HnswError::Corrupted(format!(
            "set_neighbors: level {level} region [{region_off}..{region_end}) exceeds the {}-byte entry (dim/level geometry mismatch)",
            entry.len()
        )));
    }
    let region = &mut entry[region_off..region_end];
    region[..2].copy_from_slice(&(content.len() as u16).to_le_bytes());
    region[2..].fill(0);
    for (i, &id) in content.iter().enumerate() {
        region[2 + 4 * i..2 + 4 * i + 4].copy_from_slice(&id.to_le_bytes());
    }
    Ok(())
}

/// Meta-page field offsets (2026-09-14, Stage A — the FULL meta layout is
/// Stage B's deliverable; these two positions are frozen now and Stage B
/// extends the layout around them, format-constant discipline of §7.1).
/// Layout contract (review P3-2): the meta page is the 32-byte PageHeader
/// plus a RAW field area — the line-pointer array is NEVER used on it
/// (offset 32 would otherwise alias LP[0]; tech-selection §6's "slotted
/// page" wording refers to the shared header only).
pub(crate) const META_OFF_ENTRY_POINT: usize = PAGE_HEADER_SIZE;
/// See [`META_OFF_ENTRY_POINT`].
pub(crate) const META_OFF_MAX_LEVEL: usize = PAGE_HEADER_SIZE + 4;

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

/// Read `level`'s live neighbor ids of the entry at `slot`.
pub(crate) fn entry_neighbors(
    node_page: &[u8; PAGE_SIZE],
    slot: u16,
    geo: NodeGeometry,
    level: u8,
) -> Result<Vec<u32>> {
    let (region_off, cap) = if level == 0 {
        (level_base_offset(geo.dim), geo.m_max0)
    } else {
        (level_offset(geo.dim, geo.m, geo.m_max0, level), geo.m)
    };
    let entry = entry_at(node_page, slot, "entry_neighbors")?;
    // Structural guard (2026-09-14, review P2-2 — same class as
    // set_neighbors'): the (dim, level) geometry must fit the entry.
    let region_end = region_off + level_region_size(cap);
    if region_end > entry.len() {
        return Err(HnswError::Corrupted(format!(
            "entry_neighbors: level {level} region [{region_off}..{region_end}) exceeds the {}-byte entry (dim/level geometry mismatch)",
            entry.len()
        )));
    }
    let region = &entry[region_off..region_end];
    let count = u16::from_le_bytes(region[..2].try_into().unwrap()) as usize;
    if count > usize::from(cap) {
        return Err(HnswError::Corrupted(format!(
            "entry_neighbors: stored count {count} exceeds reserved capacity {cap} of level {level}"
        )));
    }
    let mut out = Vec::with_capacity(count);
    for i in 0..count {
        out.push(u32::from_le_bytes(
            region[2 + 4 * i..2 + 4 * i + 4].try_into().unwrap(),
        ));
    }
    Ok(out)
}

/// Read the entry's vector (`dim` f32 components).
pub(crate) fn entry_vector(node_page: &[u8; PAGE_SIZE], slot: u16, dim: u16) -> Result<Vec<f32>> {
    let entry = entry_at(node_page, slot, "entry_vector")?;
    // Structural guard (2026-09-14, review P2-2): dim must fit the entry.
    if entry.len() < 4 * usize::from(dim) {
        return Err(HnswError::Corrupted(format!(
            "entry_vector: dim {dim} exceeds the {}-byte entry (dim geometry mismatch)",
            entry.len()
        )));
    }
    let mut out = Vec::with_capacity(usize::from(dim));
    for i in 0..usize::from(dim) {
        out.push(f32::from_le_bytes(
            entry[4 * i..4 * i + 4].try_into().unwrap(),
        ));
    }
    Ok(out)
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
        // Adjudication ②: pg-am-hnsw must not depend on pg-am-heap (§2),
        // so the LP layout is re-derived — this golden pin freezes the
        // shared bit layout (off:15 | flags:2 | len:15, LP_NORMAL = 1)
        // without a cross-crate dependency. Computed from
        // pg-am-heap/src/line_pointer.rs:5-9.
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
        // allocation would before asserting the read-side roundtrip.
        set_pd_lower(&mut page, (PAGE_HEADER_SIZE + LINE_POINTER_SIZE) as u16);
        assert_eq!(read_lp(&page, 0), Some((off, len)));
    }
}
