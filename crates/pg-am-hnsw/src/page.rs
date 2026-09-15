//! HNSW page types and the page-initialization chain — Phase 2 M5 Stage 0
//! (tech-selection §7.1/§7.2 layouts, §8.1 step 1, §10.3 creation protocol,
//! v1.9 P1).
//!
//! Every HNSW page starts with pg-storage's 32-byte slotted [`PageHeader`];
//! the page type rides in `pd_flags` (u16, documented as AM-specific in
//! pg-storage). The three page kinds are NODE (fixed-size node entries),
//! DIR (directory chain pages, with the 24-byte self-describing header of
//! §7.1), and META (index parameters; field layout is Stage B's deliverable
//! — Stage 0 only types and initializes it).
//!
//! The initialization chain (v1.9 P1, the A1 contract of
//! `buffer_pool.rs:424-442`): `new_page` → init the HNSW header →
//! [`log_page_init`] (post-image `FullPageImage` + stamp `pd_lsn`) → first
//! content record. **The post-image is the INITIALIZED page, never the zero
//! page** — a freelist-recycled page still carries its previous tenant's
//! bytes on disk, and without a post-image FPI the pd_lsn-guarded redo would
//! read them back as valid content. Newly allocated (zero-filled) pages take
//! the same chain with no exception: an all-zero header is not a legal
//! slotted page (`pd_lower = 0`), so there is no "fresh page shortcut".
//!
//! Whole-page zeroing contract (2026-09-11, Stage 0 review round 2 P1): the
//! `init_*` functions zero-fill the ENTIRE page before writing the header —
//! the FPI is a whole-page post-image, so any byte the init left untouched
//! would leak a previous tenant's bytes into the WAL and onto disk.

use pg_storage::page::{
    page_pd_flags, set_page_pd_flags, set_page_pd_lsn, PageHeader, PAGE_HEADER_SIZE,
};
use pg_storage::types::{Lsn, PageId, PAGE_SIZE};
use pg_storage::wal::record::WalRecord;
use pg_storage::wal::writer::WalWriter;

use crate::error::{HnswError, Result};

/// Page type tags, carried in `pd_flags` (pg-storage documents `pd_flags`
/// as AM-specific). `0` = uninitialized/zero page — never a legal HNSW page.
pub const PAGE_TYPE_NODE: u16 = 1;
/// Directory chain page (self-describing header, §7.1).
pub const PAGE_TYPE_DIR: u16 = 2;
/// Index meta page (§6; field layout is Stage B's deliverable).
pub const PAGE_TYPE_META: u16 = 3;

/// Directory-page self-describing header layout (§7.1 — format constants):
/// version u8 | flags u8 | reserved u16 | ordinal u64 | count u32 |
/// next u64, 24 bytes right after the 32-byte `PageHeader`.
pub const DIR_HEADER_SIZE: usize = 24;

/// Directory header version (§7.1 format constant; any change is a format
/// revision).
pub const DIR_FORMAT_VERSION: u8 = 1;

/// Directory header field offsets (§7.1 format constants; pub for the
/// apply.rs primitives — 2026-09-14 M5 Stage A, single implementation).
pub const DIR_OFF_VERSION: usize = PAGE_HEADER_SIZE;
/// See [`DIR_OFF_VERSION`].
pub const DIR_OFF_FLAGS: usize = PAGE_HEADER_SIZE + 1;
/// See [`DIR_OFF_VERSION`].
pub const DIR_OFF_RESERVED: usize = PAGE_HEADER_SIZE + 2;
/// See [`DIR_OFF_VERSION`].
pub const DIR_OFF_ORDINAL: usize = PAGE_HEADER_SIZE + 4;
/// See [`DIR_OFF_VERSION`].
pub const DIR_OFF_COUNT: usize = PAGE_HEADER_SIZE + 12;
/// See [`DIR_OFF_VERSION`].
pub const DIR_OFF_NEXT: usize = PAGE_HEADER_SIZE + 16;

/// Map a pg-storage failure to the crate error type (2026-09-11, M5 Stage
/// 0): the dependency edge's error channel is the new
/// [`HnswError::Storage`] variant; Stage C (write path) may refine the
/// mapping, this is the loud baseline.
fn storage_err(context: &str, e: pg_storage::error::StorageError) -> HnswError {
    HnswError::Storage(format!("{context}: {e}"))
}

/// Initialize `page` as an HNSW NODE page (32B slotted header +
/// `pd_flags = PAGE_TYPE_NODE`).
pub fn init_node_page(page: &mut [u8; PAGE_SIZE]) {
    page.fill(0);
    write_header(page, PAGE_TYPE_NODE);
}

/// Initialize `page` as the index META page (`pd_flags = PAGE_TYPE_META`;
/// parameter fields land in Stage B).
pub fn init_meta_page(page: &mut [u8; PAGE_SIZE]) {
    page.fill(0);
    write_header(page, PAGE_TYPE_META);
}

/// Initialize `page` as a directory-chain page with chain ordinal
/// `ordinal` (0 for the chain head), empty (`count = 0`) and unlinked
/// (`next = PageId::INVALID`) — §7.1's self-describing header.
pub fn init_dir_page(page: &mut [u8; PAGE_SIZE], ordinal: u64) {
    page.fill(0);
    write_header(page, PAGE_TYPE_DIR);
    page[DIR_OFF_VERSION] = DIR_FORMAT_VERSION;
    page[DIR_OFF_FLAGS] = 0;
    page[DIR_OFF_RESERVED..DIR_OFF_RESERVED + 2].copy_from_slice(&[0, 0]);
    page[DIR_OFF_ORDINAL..DIR_OFF_ORDINAL + 8].copy_from_slice(&ordinal.to_le_bytes());
    page[DIR_OFF_COUNT..DIR_OFF_COUNT + 4].copy_from_slice(&0u32.to_le_bytes());
    page[DIR_OFF_NEXT..DIR_OFF_NEXT + 8].copy_from_slice(&PageId::INVALID.0.to_le_bytes());
}

fn write_header(page: &mut [u8; PAGE_SIZE], page_type: u16) {
    // 2026-09-11, M5 Stage 0 review nano-1: use pg-storage's own writer
    // (PageHeader::write_to, page.rs:142-153) instead of a hand-copied
    // field-by-field encoding — one implementation of the header layout.
    //
    // Callers must zero-fill the page FIRST (all three init_* entry points
    // do): `log_page_init`'s FPI is a whole-page post-image, so anything the
    // init did not write would leak the previous tenant's bytes into the WAL
    // and onto disk (2026-09-11, Stage 0 review round 2 P1 — the recycled-
    // page contract is whole-page, not header-only).
    PageHeader::new(PAGE_SIZE as u16).write_to(page);
    // The pd_flags offset is owned by pg-storage's accessors (2026-09-14,
    // review round 5 P3-3 — no hardcoded 12..14 here).
    set_page_pd_flags(page, page_type);
}

/// The page's type tag (`pd_flags`), or 0 for an uninitialized page.
pub fn page_type(page: &[u8; PAGE_SIZE]) -> u16 {
    page_pd_flags(page)
}

/// Directory chain ordinal (valid only on `PAGE_TYPE_DIR` pages).
pub fn dir_ordinal(page: &[u8; PAGE_SIZE]) -> u64 {
    u64::from_le_bytes(
        page[DIR_OFF_ORDINAL..DIR_OFF_ORDINAL + 8]
            .try_into()
            .unwrap(),
    )
}

/// Directory entry count on this page.
pub fn dir_count(page: &[u8; PAGE_SIZE]) -> u32 {
    u32::from_le_bytes(page[DIR_OFF_COUNT..DIR_OFF_COUNT + 4].try_into().unwrap())
}

/// The next directory page in the chain (`PageId::INVALID` = chain tail).
pub fn dir_next(page: &[u8; PAGE_SIZE]) -> PageId {
    PageId(u64::from_le_bytes(
        page[DIR_OFF_NEXT..DIR_OFF_NEXT + 8].try_into().unwrap(),
    ))
}

/// Append a post-image `FullPageImage` of a freshly initialized page and
/// stamp its `pd_lsn` — the durability anchor for page initialization
/// (heap/btree `log_page_init` precedent, pg-am-heap/src/heap_am.rs:264 /
/// pg-am-btree/src/index.rs:3299-3310; the A1 contract of
/// `buffer_pool.rs:424-442`). Without it a freelist-recycled page would
/// recover with its previous tenant's bytes.
pub fn log_page_init(
    wal_writer: &WalWriter,
    page_id: PageId,
    page: &mut [u8; PAGE_SIZE],
) -> Result<Lsn> {
    let image = page.to_vec();
    let record =
        WalRecord::full_page_image(page_id, image).map_err(|e| storage_err("FPI encode", e))?;
    let lsn = wal_writer
        .append(record)
        .map_err(|e| storage_err("log_page_init WAL append", e))?;
    set_page_pd_lsn(page, lsn);
    Ok(lsn)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_and_meta_init_have_type_tags() {
        let mut page = [0u8; PAGE_SIZE];
        init_node_page(&mut page);
        assert_eq!(page_type(&page), PAGE_TYPE_NODE);
        let mut page = [0u8; PAGE_SIZE];
        init_meta_page(&mut page);
        assert_eq!(page_type(&page), PAGE_TYPE_META);
    }

    #[test]
    fn dir_init_writes_the_self_describing_header() {
        let mut page = [0u8; PAGE_SIZE];
        init_dir_page(&mut page, 7);
        assert_eq!(page_type(&page), PAGE_TYPE_DIR);
        assert_eq!(page[DIR_OFF_VERSION], DIR_FORMAT_VERSION);
        assert_eq!(dir_ordinal(&page), 7);
        assert_eq!(dir_count(&page), 0);
        assert_eq!(dir_next(&page), PageId::INVALID);
    }

    /// The init chain covers BOTH page provenances (v1.9 P1): a freshly
    /// zeroed page and a page with a previous tenant's junk — the init
    /// content is identical, and the junk is gone.
    #[test]
    fn init_overwrites_any_previous_content() {
        let mut page = [0xAB; PAGE_SIZE];
        init_dir_page(&mut page, 0);
        // Read back through the accessors (no offset literals here either,
        // 2026-09-14 round 6 P3): the type tag is the HNSW directory type,
        // not the junk pattern, and the whole page is zero-filled past the
        // headers — the round-2 P1 contract that `init_zero_fills_the_whole_page`
        // pins field by field.
        assert_eq!(page_type(&page), PAGE_TYPE_DIR);
        assert_eq!(dir_ordinal(&page), 0);
        assert!(
            page[56..].iter().all(|&b| b == 0),
            "the whole-page zero-fill contract covers the entry area too"
        );
    }

    /// 2026-09-11, Stage 0 review round 2 P1: the FPI post-image is the
    /// WHOLE page — init must zero-fill everything outside the header so no
    /// previous-tenant byte can leak into the WAL. Pin it: init a fully
    /// junked page and assert every byte outside the initialized region is
    /// zero (the initialized region = 32B PageHeader + 24B dir header).
    #[test]
    fn init_zero_fills_the_whole_page() {
        let mut page = [0xAB; PAGE_SIZE];
        init_dir_page(&mut page, 3);
        // Header fields written by init:
        assert_eq!(page_type(&page), PAGE_TYPE_DIR);
        assert_eq!(dir_ordinal(&page), 3);
        assert_eq!(page[32], DIR_FORMAT_VERSION);
        // Everything past the two headers must be zero — a junk byte there
        // means the FPI would carry it into the WAL. (Inside the headers,
        // pd_lsn/pd_checksum/padding are zero by PageHeader::new; the
        // meaningful fields are asserted above.)
        assert!(
            page[56..].iter().all(|&b| b == 0),
            "bytes past the headers must be zero after init"
        );
        // Same for the node and meta inits (header-only writes).
        let mut page = [0xCD; PAGE_SIZE];
        init_node_page(&mut page);
        assert!(
            page[32..].iter().all(|&b| b == 0),
            "bytes past the 32B header must be zero after init"
        );
    }
}
