//! Node-entry format ownership — Phase 2 M5 Stage B slice 1
//! (tech-selection §7.2: fixed-size-by-level node entries).
//!
//! This module is the SINGLE source of truth for the node-entry byte
//! layout: the vector, the 1-byte state field (`top_level:6 | state:1 |
//! tombstone:1`, §7.2 bit layout), and the per-level reserved neighbor
//! regions. Moved out of `apply.rs` (2026-09-15, Stage B slice 1) so the
//! application primitives consume the format instead of owning it — the
//! §7.1/§7.2 discipline is that any layout change is a format revision,
//! and a format revision has exactly one place to land.
//!
//! Layout (§7.2):
//!
//! ```text
//! entry := vector:f32[dim] | state:u8 | per-level { count:u16 | neighbors:u32[cap] }
//! state := top_level:6 (bits 0-5) | state:1 (bit 6: 0=INITIALIZING, 1=LIVE)
//!          | tombstone:1 (bit 7)
//! cap(level 0) = m_max0, cap(level > 0) = m  — always fully RESERVED;
//! only the first `count` ids are live, the rest stay zero.
//! ```

use pg_storage::page::LINE_POINTER_SIZE;
use pg_storage::page::PAGE_HEADER_SIZE;
use pg_storage::types::PAGE_SIZE;

use crate::error::{HnswError, Result};

/// State byte, LIVE bit (bit 6): 0 = INITIALIZING, 1 = LIVE.
pub(crate) const STATE_LIVE_BIT: u8 = 1 << 6;
/// State byte, tombstone bit (bit 7) — format delivered in M5, semantics
/// land in M6 (tech-selection §1).
pub(crate) const STATE_TOMBSTONE_BIT: u8 = 1 << 7;

/// The fixed-size geometry of a node entry (§7.2): vector dimension plus
/// the two capacity parameters — grouped because the entry-creating
/// primitives need all three (2026-09-14, Stage A review fix for arity).
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
pub(crate) fn level_region_size(cap: u16) -> usize {
    2 + 4 * usize::from(cap)
}

/// Byte offset of `level`'s neighbor region inside an entry (upper levels
/// only; level 0 uses [`level_base_offset`]).
pub(crate) fn level_offset(dim: u16, m: u16, m_max0: u16, level: u8) -> usize {
    debug_assert!(
        level >= 1,
        "level_offset is for upper levels; level 0 uses the base"
    );
    4 * usize::from(dim)
        + 1
        + level_region_size(m_max0)
        + usize::from(level - 1) * level_region_size(m)
}

/// Byte offset of level 0's neighbor region inside an entry.
pub(crate) const fn level_base_offset(dim: u16) -> usize {
    4 * dim as usize + 1
}

/// Byte offset of the state byte inside an entry (`4·dim`).
pub(crate) fn state_offset(dim: u16) -> usize {
    4 * usize::from(dim)
}

/// `(region offset, capacity)` of `level`'s neighbor region inside an
/// entry — single source shared by the write and read sides (2026-09-15,
/// round 3 P3).
pub(crate) fn level_region_pos(geo: NodeGeometry, level: u8) -> (usize, u16) {
    if level == 0 {
        (level_base_offset(geo.dim), geo.m_max0)
    } else {
        (level_offset(geo.dim, geo.m, geo.m_max0, level), geo.m)
    }
}

/// Usable tuple area of a node page at creation-geometry check time (§7.2):
/// page minus the 32-byte header minus ONE line pointer. **Parameterized
/// on `PAGE_SIZE`** (v1.5 P2-3: the 16k feature recomputes from the same
/// formula; the M5 acceptance matrix pins 8 KB = 8156).
pub(crate) const NODE_PAGE_USABLE: usize = PAGE_SIZE - PAGE_HEADER_SIZE - LINE_POINTER_SIZE;

/// Creation-time geometry hard check (§7.2 capacity invariant, product-
/// linked): the worst-case entry — `dim` wide, with the redraw-bounded
/// maximum of `L_max = ⌊53·ln2/ln M⌋` upper levels (rng.rs hard ceiling) —
/// must fit one page. At the frozen defaults (m=16, m_max0=32) this is
/// `dim <= 1791`; the check runs per (dim, m, m_max0) triple, never as a
/// hardcoded constant.
///
/// **Precondition: `m >= 2`** (2026-09-16, review nano — now stated, same
/// discipline as `rng::l_max`'s): the ln degenerates below 2 and is not
/// defended here. Both production callers enforce it upstream — `HnswIndex`
/// creation goes through `HnswParams::new` (rejects m < 2), and
/// `read_meta` rejects `m < 2` BEFORE calling this — so no legal caller
/// can carry it.
pub(crate) fn check_creation_geometry(dim: u16, m: u16, m_max0: u16) -> Result<()> {
    if dim == 0 {
        return Err(HnswError::InvalidArgument(
            "creation geometry: dim = 0 (M4 §5: rejected at every entry point)".to_string(),
        ));
    }
    let l_max = crate::rng::l_max(m);
    let worst = entry_size(dim, m, m_max0, l_max);
    if worst > NODE_PAGE_USABLE {
        return Err(HnswError::InvalidArgument(format!(
            "creation geometry rejected: worst-case entry 4·{dim} + 1 + (2 + 4·{m_max0}) + {l_max}·(2 + 4·{m}) = {worst} > page usable {NODE_PAGE_USABLE} (dim/m/m_max0 product-linked check, §7.2)"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// §7.2 format pins: any change here is a format revision.
    #[test]
    fn state_bits_are_the_frozen_layout() {
        assert_eq!(STATE_LIVE_BIT, 0x40);
        assert_eq!(STATE_TOMBSTONE_BIT, 0x80);
    }

    #[test]
    fn entry_size_tiers_are_frozen() {
        // Frozen defaults dim=128 / m=16 / m_max0=32:
        // L=0 -> 4·128 + 1 + (2 + 4·32) = 512 + 1 + 130 = 643.
        assert_eq!(entry_size(128, 16, 32, 0), 643);
        // L=13 (the M=16 redraw ceiling) -> 643 + 13·66 = 1501.
        assert_eq!(entry_size(128, 16, 32, 13), 1501);
        // The worst-case default geometry exactly fits the 8 KB usable
        // area at dim = 1791, and overflows at 1792 (§7.2's hard cap).
        assert_eq!(entry_size(1791, 16, 32, 13), 8153);
        assert!(entry_size(1791, 16, 32, 13) <= NODE_PAGE_USABLE);
        assert!(entry_size(1792, 16, 32, 13) > NODE_PAGE_USABLE);
        assert_eq!(NODE_PAGE_USABLE, 8156);
    }

    #[test]
    fn creation_geometry_check_is_product_linked() {
        // Defaults: 1791 passes, 1792 is loudly rejected, dim = 0 rejected.
        check_creation_geometry(1791, 16, 32).unwrap();
        assert!(check_creation_geometry(1792, 16, 32).is_err());
        assert!(check_creation_geometry(0, 16, 32).is_err());
        // m = 2 drives L_max to 53: worst entry = 4·dim + 1 + 130 + 53·10
        // (2 + 4·2 = 10 per upper level) = 4·dim + 661 — so dim ≤
        // ⌊(8156 − 661)/4⌋ = 1873 passes, 1874 fails. (m_max0 = 32 kept to
        // isolate the L_max linkage.)
        assert_eq!(crate::rng::l_max(2), 53);
        check_creation_geometry(1873, 2, 32).unwrap();
        assert!(check_creation_geometry(1874, 2, 32).is_err());
        // A wide m_max0 tightens the same formula (2 + 4·64 = 258 at level 0).
        assert!(check_creation_geometry(1791, 16, 64).is_err());
    }
}
