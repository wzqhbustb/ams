//! Slotted-page header (32 bytes) and the `pd_lsn` authority contract.
//!
//! The M2 slotted-page format reserves the first 32 bytes of every page for
//! a header (26 bytes of fields + 6 bytes of padding), matching the layout
//! in `docs/phase1-m2-tech-selection.md` §二. Two different maintenance
//! levels exist:
//!
//! - The **buffer pool** maintains only `pd_lsn` (`page[0..8]`) on every
//!   page it touches (via the FPI path in `pin_mut`); the remaining header
//!   fields stay zero until an access method initializes them.
//! - **Access methods** (slotted heap from Stage G, B+Tree later) call
//!   [`PageHeader::init_page`] and own the rest of the header for their
//!   pages.
//!
//! Header field layout:
//!
//! | Offset  | Field               |
//! |---------|---------------------|
//! | 0..8    | pd_lsn (u64)        |
//! | 8..12   | pd_checksum (u32)   |
//! | 12..14  | pd_flags (u16)      |
//! | 14..16  | pd_lower (u16)      |
//! | 16..18  | pd_upper (u16)      |
//! | 18..20  | pd_special (u16)    |
//! | 20..22  | pd_pagesize_version |
//! | 22..26  | pd_prune_xid (u32)  |
//! | 26..32  | padding (zeroed)    |
//!
//! # `pd_lsn` authority contract (v2.3-10 / §11.5)
//!
//! `page[0..8]` is the **authoritative** source of a page's LSN. The buffer
//! pool's frame metadata keeps only a read-only cache (`cached_lsn`); any
//! path that needs the page LSN (e.g. `flush_frame` enforcing
//! WAL-before-data) reads `page[0..8]` directly. Writers that modify a page
//! under WAL (currently only the FPI path in `pin_mut`) must update `pd_lsn`
//! via [`set_page_pd_lsn`].

use crate::types::{Lsn, PAGE_SIZE};

/// Total size of the page header in bytes (26 bytes of fields + 6 padding).
///
/// `pd_lower` of a freshly initialized page equals this value: the line
/// pointer array starts at offset 32, so tuple payloads are 8-byte aligned.
pub const PAGE_HEADER_SIZE: usize = 32;

/// Value stored in `pd_pagesize_version` for the M2 page format.
pub const PAGE_FORMAT_VERSION: u16 = 1;

/// Read the page's authoritative LSN from `page[0..8]`.
///
/// A zeroed page yields [`Lsn::INVALID`], which callers treat as "no WAL
/// record has ever touched this page" (matching M1's `page_lsn = INVALID`).
pub fn page_pd_lsn(page: &[u8]) -> Lsn {
    Lsn(u64::from_le_bytes(
        page[0..8].try_into().expect("page is at least 8 bytes"),
    ))
}

/// Write the page's authoritative LSN into `page[0..8]`.
pub fn set_page_pd_lsn(page: &mut [u8], lsn: Lsn) {
    page[0..8].copy_from_slice(&lsn.0.to_le_bytes());
}

/// Read the page's `pd_flags` from `page[12..14]` (AM-specific usage; see
/// the layout table above and the allocation registry on `PageHeader`).
///
/// 2026-09-14, M5 Stage 0 review round 5 P3-3: added so AMs (pg-am-hnsw's
/// page type tags first) stop hardcoding the offset — one owner of the
/// header layout, same discipline as [`page_pd_lsn`].
pub fn page_pd_flags(page: &[u8]) -> u16 {
    u16::from_le_bytes(page[12..14].try_into().expect("page is at least 14 bytes"))
}

/// Write the page's `pd_flags` into `page[12..14]`.
pub fn set_page_pd_flags(page: &mut [u8], flags: u16) {
    page[12..14].copy_from_slice(&flags.to_le_bytes());
}

/// The 32-byte slotted-page header (26 bytes of fields + 6 bytes padding).
///
/// The 6-byte padding after `pd_prune_xid` keeps tuple payloads 8-byte
/// aligned; it is always written as zeros.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageHeader {
    /// Authoritative LSN of the last WAL record that modified this page.
    pub pd_lsn: Lsn,
    /// Page checksum (M2 writes 0; Phase 7b may enable real checksums).
    pub pd_checksum: u32,
    /// Page flags (AM-specific).
    ///
    /// Allocation register (2026-09-11, M5 Stage 0 review nano-2): bits
    /// 8–15 are owned by pg-am-btree (`btpo_level` 8–11, `btpo_flags`
    /// 12–15, pg-am-btree/src/page.rs); bits 0–7 are the HNSW page-type
    /// tag (1=NODE, 2=DIR, 3=META, pg-am-hnsw/src/page.rs); heap does not
    /// use this field. New AMs must register here before claiming a bit.
    pub pd_flags: u16,
    /// Offset to the end of the line pointer array (start of free space).
    pub pd_lower: u16,
    /// Offset to the beginning of tuple data (end of free space).
    pub pd_upper: u16,
    /// Offset to AM-private special space.
    pub pd_special: u16,
    /// Page format version ([`PAGE_FORMAT_VERSION`]).
    pub pd_pagesize_version: u16,
    /// Oldest XID that may prune HOT chains (used from M2b HOT updates).
    pub pd_prune_xid: u32,
}

impl PageHeader {
    /// Header for a freshly initialized page: no line pointers, the whole
    /// page is free space, and no WAL record has touched it yet.
    pub fn new(page_size: u16) -> Self {
        Self {
            pd_lsn: Lsn::INVALID,
            pd_checksum: 0,
            pd_flags: 0,
            pd_lower: PAGE_HEADER_SIZE as u16,
            pd_upper: page_size,
            pd_special: page_size,
            pd_pagesize_version: PAGE_FORMAT_VERSION,
            pd_prune_xid: 0,
        }
    }

    /// Decode the header from the first 32 bytes of `page`.
    pub fn read_from(page: &[u8]) -> Self {
        debug_assert!(page.len() >= PAGE_HEADER_SIZE);
        Self {
            pd_lsn: page_pd_lsn(page),
            pd_checksum: u32::from_le_bytes(page[8..12].try_into().unwrap()),
            pd_flags: u16::from_le_bytes(page[12..14].try_into().unwrap()),
            pd_lower: u16::from_le_bytes(page[14..16].try_into().unwrap()),
            pd_upper: u16::from_le_bytes(page[16..18].try_into().unwrap()),
            pd_special: u16::from_le_bytes(page[18..20].try_into().unwrap()),
            pd_pagesize_version: u16::from_le_bytes(page[20..22].try_into().unwrap()),
            pd_prune_xid: u32::from_le_bytes(page[22..26].try_into().unwrap()),
        }
    }

    /// Encode the header into the first 32 bytes of `page`, zeroing the
    /// 6-byte padding.
    pub fn write_to(&self, page: &mut [u8]) {
        debug_assert!(page.len() >= PAGE_HEADER_SIZE);
        set_page_pd_lsn(page, self.pd_lsn);
        page[8..12].copy_from_slice(&self.pd_checksum.to_le_bytes());
        page[12..14].copy_from_slice(&self.pd_flags.to_le_bytes());
        page[14..16].copy_from_slice(&self.pd_lower.to_le_bytes());
        page[16..18].copy_from_slice(&self.pd_upper.to_le_bytes());
        page[18..20].copy_from_slice(&self.pd_special.to_le_bytes());
        page[20..22].copy_from_slice(&self.pd_pagesize_version.to_le_bytes());
        page[22..26].copy_from_slice(&self.pd_prune_xid.to_le_bytes());
        page[26..PAGE_HEADER_SIZE].fill(0);
    }

    /// Initialize the header region of a fresh (zeroed) page.
    pub fn init_page(page: &mut [u8; PAGE_SIZE]) {
        Self::new(PAGE_SIZE as u16).write_to(page);
    }
}

// ---------------------------------------------------------------------
// Line-pointer layout — single source of truth (2026-09-15, Phase 2 M5
// Stage B adjudication): the layout's historical owner is
// pg-am-heap/src/line_pointer.rs:16 (4 bytes, off:15 | flags:2 | len:15;
// `LP_NORMAL = 1`, the PostgreSQL `LP_NORMAL` value). pg-am-hnsw's M5
// node pages reuse the same slotted-page discipline but must not depend
// on pg-am-heap (tech-selection §2), so the layout now lives here for all
// new consumers. Migrating pg-am-heap itself onto this module is a
// registered FUTURE refactor — not done in this stage (minimal change).
// ---------------------------------------------------------------------

/// Line-pointer size in bytes (layout contract).
pub const LINE_POINTER_SIZE: usize = 4;

/// `LP_NORMAL` — the slot points at a live tuple (PostgreSQL's value).
pub const LP_NORMAL: u32 = 1;

/// Encode a line pointer: `off:15 | flags(LP_NORMAL):2 | len:15`
/// (little-endian), i.e.
/// `(off & 0x7FFF) | (LP_NORMAL << 15) | ((len & 0x7FFF) << 17)`.
pub fn encode_line_pointer(off: u16, len: u16) -> [u8; 4] {
    let raw = (u32::from(off) & 0x7FFF) | (LP_NORMAL << 15) | ((u32::from(len) & 0x7FFF) << 17);
    raw.to_le_bytes()
}

/// Decode a line pointer, returning `(off, len, is_normal)`.
pub fn decode_line_pointer(raw: [u8; 4]) -> (u16, u16, bool) {
    let raw = u32::from_le_bytes(raw);
    let off = (raw & 0x7FFF) as u16;
    let len = ((raw >> 17) & 0x7FFF) as u16;
    (off, len, (raw >> 15) & 0x3 == LP_NORMAL)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_header_has_lower_at_header_size() {
        let header = PageHeader::new(PAGE_SIZE as u16);
        assert_eq!(header.pd_lsn, Lsn::INVALID);
        assert_eq!(header.pd_lower, PAGE_HEADER_SIZE as u16);
        assert_eq!(header.pd_upper, PAGE_SIZE as u16);
        assert_eq!(header.pd_special, PAGE_SIZE as u16);
        assert_eq!(header.pd_pagesize_version, PAGE_FORMAT_VERSION);
        assert_eq!(header.pd_prune_xid, 0);
    }

    #[test]
    fn header_write_read_roundtrip() {
        let mut page = [0u8; PAGE_SIZE];
        let mut header = PageHeader::new(PAGE_SIZE as u16);
        header.pd_lsn = Lsn(4096);
        header.pd_flags = 0x0003;
        header.pd_lower = 64;
        header.pd_upper = 8000;
        header.pd_prune_xid = 42;
        header.write_to(&mut page);

        // Padding must stay zeroed.
        assert!(page[26..PAGE_HEADER_SIZE].iter().all(|&b| b == 0));

        let decoded = PageHeader::read_from(&page);
        assert_eq!(decoded, header);
    }

    /// Golden pin for the line-pointer layout (2026-09-15 Stage B):
    /// `encode(8000, 96)` must produce the LE bytes of `0x00C0_9F40` —
    /// off=8000(0x1F40) | LP_NORMAL<<15 | len=96<<17, the same frozen value
    /// pg-am-hnsw's apply.rs pins on its side (the pin's purpose is
    /// cross-crate NON-drift; the formulaic derivation is intentionally NOT
    /// used as the expectation).
    #[test]
    fn line_pointer_layout_golden_pin_and_roundtrip() {
        let enc = encode_line_pointer(8000, 96);
        assert_eq!(u32::from_le_bytes(enc), 0x00C0_9F40);
        assert_eq!(decode_line_pointer(enc), (8000, 96, true));
        // Non-normal flags decode as is_normal = false.
        let mut raw = u32::from_le_bytes(enc);
        raw = (raw & !(0x3 << 15)) | (3 << 15);
        assert!(!decode_line_pointer(raw.to_le_bytes()).2);
        // Roundtrip across the full 15-bit ranges.
        for (off, len) in [(0u16, 0u16), (0x7FFF, 0x7FFF), (32, 8192), (1, 1)] {
            let (d_off, d_len, normal) = decode_line_pointer(encode_line_pointer(off, len));
            assert_eq!((d_off, d_len, normal), (off, len, true));
        }
    }

    #[test]
    fn pd_lsn_helpers_roundtrip() {
        let mut page = [0u8; PAGE_SIZE];
        assert_eq!(page_pd_lsn(&page), Lsn::INVALID);

        set_page_pd_lsn(&mut page, Lsn(0x0102_0304_0506_0708));
        assert_eq!(page_pd_lsn(&page), Lsn(0x0102_0304_0506_0708));
        // Bytes beyond the header's first 8 are untouched.
        assert!(page[8..].iter().all(|&b| b == 0));
    }
}
