//! Line pointer bit-field encoding (tech-selection §二).
//!
//! Each line pointer is a 32-bit value stored in the LP array that grows
//! downward from offset 32 (`PAGE_HEADER_SIZE`):
//!
//! ```text
//! bits 0..14   lp_off:   u15 (tuple offset in page)
//! bits 15..16  lp_flags: u2  (00 UNUSED / 01 NORMAL / 10 REDIRECT / 11 DEAD)
//! bits 17..31  lp_len:   u15 (tuple length in bytes)
//! ```
//!
//! All 32 bits are allocated (15 + 2 + 15). The flag values
//! match PostgreSQL's `LP_*` constants.

/// Size of one line pointer in bytes.
pub const LINE_POINTER_SIZE: usize = 4;

const LP_OFF_MASK: u32 = 0x7FFF;
const LP_FLAGS_SHIFT: u32 = 15;
const LP_FLAGS_MASK: u32 = 0x3;
const LP_LEN_SHIFT: u32 = 17;

/// State of a line pointer (values match PG's `LP_*` constants).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum LpFlags {
    /// Slot is empty and may be recycled by `add_tuple`/`add_tuple_at`.
    Unused = 0,
    /// Slot points to a live tuple.
    Normal = 1,
    /// Slot redirects to another slot (HOT chains, M2c).
    Redirect = 2,
    /// Slot points to a dead tuple awaiting vacuum.
    Dead = 3,
}

impl LpFlags {
    fn from_bits(bits: u32) -> Self {
        match bits & LP_FLAGS_MASK {
            0 => LpFlags::Unused,
            1 => LpFlags::Normal,
            2 => LpFlags::Redirect,
            _ => LpFlags::Dead,
        }
    }
}

/// A 32-bit line pointer: `lp_off:15 / lp_flags:2 / lp_len:15`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LinePointer(u32);

impl LinePointer {
    /// A zeroed line pointer (`Unused`, offset 0, length 0), as found on a
    /// freshly initialized page beyond `pd_lower`.
    pub const ZERO: LinePointer = LinePointer(0);

    /// Build a line pointer. `off` and `len` must fit in 15 bits.
    pub fn new(off: u16, flags: LpFlags, len: u16) -> Self {
        debug_assert!(off <= LP_OFF_MASK as u16, "lp_off overflows u15");
        debug_assert!(len <= LP_OFF_MASK as u16, "lp_len overflows u15");
        LinePointer(
            (off as u32 & LP_OFF_MASK)
                | ((flags as u32) << LP_FLAGS_SHIFT)
                | ((len as u32) << LP_LEN_SHIFT),
        )
    }

    /// Decode a line pointer from its raw 32-bit representation.
    pub fn from_bits(bits: u32) -> Self {
        LinePointer(bits)
    }

    /// The raw 32-bit representation.
    pub fn bits(self) -> u32 {
        self.0
    }

    /// Offset of the tuple within the page (`lp_off`, bits 0..14).
    pub fn off(self) -> u16 {
        (self.0 & LP_OFF_MASK) as u16
    }

    /// Slot state (`lp_flags`, bits 15..16).
    pub fn flags(self) -> LpFlags {
        LpFlags::from_bits(self.0 >> LP_FLAGS_SHIFT)
    }

    /// Tuple length in bytes (`lp_len`, bits 17..31).
    // `is_empty` is meaningless for a line pointer; allow the lint.
    #[allow(clippy::len_without_is_empty)]
    pub fn len(self) -> u16 {
        (self.0 >> LP_LEN_SHIFT) as u16
    }

    /// Return a copy with the flags field replaced (offset/length kept).
    pub fn with_flags(self, flags: LpFlags) -> Self {
        LinePointer::new(self.off(), flags, self.len())
    }

    /// Decode from little-endian bytes.
    pub fn from_le_bytes(bytes: [u8; LINE_POINTER_SIZE]) -> Self {
        LinePointer(u32::from_le_bytes(bytes))
    }

    /// Encode as little-endian bytes.
    pub fn to_le_bytes(self) -> [u8; LINE_POINTER_SIZE] {
        self.0.to_le_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bit_field_round_trip() {
        let lp = LinePointer::new(0x1234, LpFlags::Normal, 0x4321);
        assert_eq!(lp.off(), 0x1234);
        assert_eq!(lp.flags(), LpFlags::Normal);
        assert_eq!(lp.len(), 0x4321);

        let decoded = LinePointer::from_bits(lp.bits());
        assert_eq!(decoded, lp);
    }

    #[test]
    fn flags_do_not_bleed_into_neighbors() {
        for flags in [
            LpFlags::Unused,
            LpFlags::Normal,
            LpFlags::Redirect,
            LpFlags::Dead,
        ] {
            let lp = LinePointer::new(1, flags, 2);
            assert_eq!(lp.flags(), flags);
            assert_eq!(lp.off(), 1);
            assert_eq!(lp.len(), 2);
        }
    }

    #[test]
    fn max_values_fit() {
        let lp = LinePointer::new(0x7FFF, LpFlags::Dead, 0x7FFF);
        assert_eq!(lp.off(), 0x7FFF);
        assert_eq!(lp.len(), 0x7FFF);
        assert_eq!(lp.flags(), LpFlags::Dead);
    }

    #[test]
    fn zero_is_unused() {
        let lp = LinePointer::ZERO;
        assert_eq!(lp.flags(), LpFlags::Unused);
        assert_eq!(lp.off(), 0);
        assert_eq!(lp.len(), 0);
    }

    #[test]
    fn le_bytes_round_trip() {
        let lp = LinePointer::new(4096, LpFlags::Normal, 128);
        let bytes = lp.to_le_bytes();
        assert_eq!(LinePointer::from_le_bytes(bytes), lp);
    }
}
