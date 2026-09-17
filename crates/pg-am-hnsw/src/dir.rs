//! Directory-chain ownership — Phase 2 M5 Stage B slice 1
//! (tech-selection §7.1: self-describing directory page chain).
//!
//! This module owns the directory chain's derived constants and the chain
//! verifier: the directory is how NodeIds map to `(page, slot)` —
//! position-is-identity, and the chain-derived high-water mark
//! `HWM = tail.ordinal × DIR_ENTRIES_PER_PAGE + tail.count` is the NodeId
//! allocation frontier (v1.2). [`check_dir_chain`] is the single
//! implementation of BOTH the §11.3 chain-structure audit (Stage D
//! consumes it) and the open-time HWM derivation (same function, single
//! implementation — 2026-09-15 Stage B slice 1).

use pg_storage::page::PAGE_HEADER_SIZE;
use pg_storage::types::{PageId, PAGE_SIZE};

use crate::error::{HnswError, Result};
use crate::page::{dir_count, dir_next, dir_ordinal, page_type, DIR_HEADER_SIZE, PAGE_TYPE_DIR};

/// Directory entries per directory page (§7.1 format constant, derived —
/// 2026-09-14 review P3-4: not a bare literal, one source with the header
/// layout constants): `⌊(PAGE_SIZE − 32 PageHeader − 24 dir header) / 10⌋`.
/// At 8 KB pages this is 813.
pub(crate) const DIR_ENTRIES_PER_PAGE: u32 =
    ((PAGE_SIZE - PAGE_HEADER_SIZE - DIR_HEADER_SIZE) / 10) as u32;

/// Sanity ceiling for a directory-chain walk: bounds both the loop (a
/// cyclic chain would otherwise spin forever) and the HWM math.
const MAX_DIR_PAGES: u64 = 1 << 32;

/// Outcome of a verified directory-chain walk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DirChainInfo {
    /// Number of pages in the chain (>= 1).
    pub page_count: u64,
    /// Chain-derived high-water mark: the next unallocated NodeId.
    pub hwm: u64,
}

/// Walk the directory chain from `head`, verifying the §11.3 chain-structure
/// invariants and deriving the high-water mark. `fetch` resolves a PageId to
/// that page's bytes (the caller pins/reads the page; this function does no
/// I/O — 2026-09-15 Stage B slice 1).
///
/// The four §11.3 assertions, each loudly rejected on violation:
/// 1. ordinals are consecutive from 0 (page `i` has `ordinal == i`);
/// 2. middle pages are exactly full (`count == DIR_ENTRIES_PER_PAGE`) — the
///    "link only when the tail is full" protocol premise;
/// 3. `next` forms a single acyclic chain (non-tail `next` is a real page,
///    != itself, never revisits — a plain `Vec<PageId>` visited set, the
///    M4 no-HashMap guardrail discipline; a cycle is Corrupted);
/// 4. every page's `count` is within `[0, DIR_ENTRIES_PER_PAGE]`, every page
///    is `PAGE_TYPE_DIR`, and the chain length never exceeds the sanity
///    ceiling [`MAX_DIR_PAGES`] (death-loop guard).
///
/// `hwm = tail.ordinal × DIR_ENTRIES_PER_PAGE + tail.count`, computed with
/// checked arithmetic (overflow is loud, same discipline as apply.rs's
/// round-5 guards).
pub(crate) fn check_dir_chain<F>(head: PageId, mut fetch: F) -> Result<DirChainInfo>
where
    F: FnMut(PageId) -> Result<[u8; PAGE_SIZE]>,
{
    if head == PageId::INVALID {
        return Err(HnswError::InvalidArgument(
            "check_dir_chain: chain head must be a real page (PageId::INVALID)".to_string(),
        ));
    }
    let mut visited: Vec<PageId> = Vec::new();
    let mut current = head;
    let mut expected_ordinal = 0u64;
    loop {
        if visited.len() as u64 >= MAX_DIR_PAGES {
            return Err(HnswError::Corrupted(format!(
                "check_dir_chain: chain exceeds the sanity ceiling of {MAX_DIR_PAGES} pages (cycle or runaway chain)"
            )));
        }
        if visited.contains(&current) {
            return Err(HnswError::Corrupted(format!(
                "check_dir_chain: cycle detected at page {}",
                current.0
            )));
        }
        let page = fetch(current)?;
        if page_type(&page) != PAGE_TYPE_DIR {
            return Err(HnswError::Corrupted(format!(
                "check_dir_chain: page {} is not a directory page",
                current.0
            )));
        }
        let ordinal = dir_ordinal(&page);
        if ordinal != expected_ordinal {
            return Err(HnswError::Corrupted(format!(
                "check_dir_chain: page {} has ordinal {ordinal}, expected {expected_ordinal} (ordinals must be consecutive from 0)",
                current.0
            )));
        }
        let count = dir_count(&page);
        if count > DIR_ENTRIES_PER_PAGE {
            return Err(HnswError::Corrupted(format!(
                "check_dir_chain: page {} has count {count} > capacity {DIR_ENTRIES_PER_PAGE}",
                current.0
            )));
        }
        let next = dir_next(&page);
        if next == PageId::INVALID {
            // Chain tail: middle-page fullness checked for every page
            // before this one; derive the high-water mark.
            let hwm = ordinal
                .checked_mul(u64::from(DIR_ENTRIES_PER_PAGE))
                .and_then(|base| base.checked_add(u64::from(count)))
                .ok_or_else(|| {
                    HnswError::Corrupted(format!(
                        "check_dir_chain: high-water mark computation overflows (ordinal {ordinal}, count {count})"
                    ))
                })?;
            // 2026-09-16, mainline Stage B review round 2 P3-1: NodeId is
            // u32 (§3 frozen) but the chain arithmetic admits more entries
            // than that — Stage C's insert would truncate `hwm as u32`
            // and hand two nodes the same id. Reject at the open/audit
            // boundary: loud, never truncated.
            check_hwm_node_id_space(hwm)?;
            return Ok(DirChainInfo {
                page_count: expected_ordinal + 1,
                hwm,
            });
        }
        // Middle page: must be exactly full, and `next` must be a real
        // page that is not the page itself.
        if count != DIR_ENTRIES_PER_PAGE {
            return Err(HnswError::Corrupted(format!(
                "check_dir_chain: middle page {} has count {count}, expected exactly {DIR_ENTRIES_PER_PAGE} (link only when the tail is full)",
                current.0
            )));
        }
        if next == current {
            return Err(HnswError::Corrupted(format!(
                "check_dir_chain: page {} links to itself",
                current.0
            )));
        }
        visited.push(current);
        current = next;
        expected_ordinal += 1;
    }
}

/// The chain-derived high-water mark must fit the u32 NodeId space (§3 —
/// 2026-09-16, review round 2 P3-1). Factored out of [`check_dir_chain`]
/// because reaching this boundary through a synthetic chain would need
/// ~5.3M directory pages — the rule is unit-tested directly here.
fn check_hwm_node_id_space(hwm: u64) -> Result<()> {
    if hwm > u64::from(u32::MAX) {
        return Err(HnswError::Corrupted(format!(
            "check_dir_chain: high-water mark {hwm} exceeds the u32 NodeId space (§3)"
        )));
    }
    Ok(())
}

/// Read the `idx`-th 10-byte mapping entry `(target_page, target_slot)`
/// from a directory page (§7.1 entry layout: `PageId u64 + SlotId u16`,
/// position-is-identity). `idx` must be below the page's count —
/// out-of-range is a loud `Corrupted` (2026-09-15, M5 Stage B slice 3a:
/// the open-repair path reads entry 0 through this).
///
/// Page-content bounds (2026-09-16, Stage B adversarial review P3-2):
/// `count` itself is read from the (checksum-less) page and was previously
/// used unclamped — a corrupt `count > 813` with `idx` in `[814, count)`
/// would slice past PAGE_SIZE and panic, against the redo-path no-panic
/// discipline (apply.rs `read_lp`'s clamping lineage). Clamp first.
pub(crate) fn dir_entry(page: &[u8; PAGE_SIZE], idx: u32) -> Result<(PageId, u16)> {
    let count = dir_count(page);
    if count > DIR_ENTRIES_PER_PAGE {
        return Err(HnswError::Corrupted(format!(
            "dir_entry: directory count {count} exceeds capacity {DIR_ENTRIES_PER_PAGE}"
        )));
    }
    if idx >= count {
        return Err(HnswError::Corrupted(format!(
            "dir_entry: index {idx} >= directory count {count}"
        )));
    }
    let off = PAGE_HEADER_SIZE + DIR_HEADER_SIZE + idx as usize * 10;
    let target_page = PageId(u64::from_le_bytes(page[off..off + 8].try_into().unwrap()));
    let target_slot = u16::from_le_bytes(page[off + 8..off + 10].try_into().unwrap());
    Ok((target_page, target_slot))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::{init_dir_page, init_node_page, DIR_OFF_COUNT, DIR_OFF_NEXT};
    use std::collections::BTreeMap;

    const CAP: u32 = DIR_ENTRIES_PER_PAGE;

    fn dir_page(ordinal: u64, count: u32, next: PageId) -> [u8; PAGE_SIZE] {
        let mut p = [0u8; PAGE_SIZE];
        init_dir_page(&mut p, ordinal);
        p[DIR_OFF_COUNT..DIR_OFF_COUNT + 4].copy_from_slice(&count.to_le_bytes());
        p[DIR_OFF_NEXT..DIR_OFF_NEXT + 8].copy_from_slice(&next.0.to_le_bytes());
        p
    }

    /// A page fetch over a static BTreeMap of synthetic pages (BTreeMap
    /// iteration order is irrelevant here — fetch is by key; the M4
    /// no-HashMap guardrail targets the ALGORITHM path, and BTreeMap is the
    /// sanctioned alternative).
    fn fetcher(
        pages: BTreeMap<PageId, [u8; PAGE_SIZE]>,
    ) -> impl FnMut(PageId) -> Result<[u8; PAGE_SIZE]> {
        move |id| {
            pages
                .get(&id)
                .copied()
                .ok_or_else(|| HnswError::Corrupted(format!("fetch: page {} missing", id.0)))
        }
    }

    #[test]
    fn legal_chain_derives_hwm() {
        let pages = BTreeMap::from([
            (PageId(1), dir_page(0, CAP, PageId(2))),
            (PageId(2), dir_page(1, 5, PageId::INVALID)),
        ]);
        let info = check_dir_chain(PageId(1), fetcher(pages)).unwrap();
        assert_eq!(info.page_count, 2);
        assert_eq!(info.hwm, u64::from(CAP) + 5); // 818 at 8 KB
        assert_eq!(info.hwm, 813 + 5);
    }

    #[test]
    fn single_empty_head_is_hwm_zero() {
        let pages = BTreeMap::from([(PageId(1), dir_page(0, 0, PageId::INVALID))]);
        let info = check_dir_chain(PageId(1), fetcher(pages)).unwrap();
        assert_eq!(info.page_count, 1);
        assert_eq!(info.hwm, 0);
    }

    #[test]
    fn chain_defects_are_loud() {
        // Ordinal gap: page claims ordinal 1 at position 0.
        let pages = BTreeMap::from([(PageId(1), dir_page(1, 0, PageId::INVALID))]);
        assert!(check_dir_chain(PageId(1), fetcher(pages)).is_err());

        // Middle page not exactly full.
        let pages = BTreeMap::from([
            (PageId(1), dir_page(0, CAP - 1, PageId(2))),
            (PageId(2), dir_page(1, 0, PageId::INVALID)),
        ]);
        assert!(check_dir_chain(PageId(1), fetcher(pages)).is_err());

        // Cycle: page 1 links back to page 1's predecessor... a 2-cycle.
        let pages = BTreeMap::from([
            (PageId(1), dir_page(0, CAP, PageId(2))),
            (PageId(2), dir_page(1, CAP, PageId(1))),
        ]);
        assert!(check_dir_chain(PageId(1), fetcher(pages)).is_err());

        // Self-link.
        let pages = BTreeMap::from([(PageId(1), dir_page(0, CAP, PageId(1)))]);
        assert!(check_dir_chain(PageId(1), fetcher(pages)).is_err());

        // count above capacity.
        let pages = BTreeMap::from([(PageId(1), dir_page(0, CAP + 1, PageId::INVALID))]);
        assert!(check_dir_chain(PageId(1), fetcher(pages)).is_err());

        // Wrong page type (a NODE page on the chain).
        let mut node = [0u8; PAGE_SIZE];
        init_node_page(&mut node);
        let pages = BTreeMap::from([(PageId(1), node)]);
        assert!(check_dir_chain(PageId(1), fetcher(pages)).is_err());

        // Missing page (dangling next).
        let pages = BTreeMap::from([(PageId(1), dir_page(0, CAP, PageId(99)))]);
        assert!(check_dir_chain(PageId(1), fetcher(pages)).is_err());

        // Invalid head.
        let pages = BTreeMap::from([(PageId(1), dir_page(0, 0, PageId::INVALID))]);
        assert!(check_dir_chain(PageId::INVALID, fetcher(pages)).is_err());
    }

    /// 2026-09-16, review round 2 P3-1: the NodeId-space guard at the
    /// exact u32 boundary (tested directly — a synthetic chain long enough
    /// to reach it through `check_dir_chain` would need ~5.3M pages).
    #[test]
    fn hwm_node_id_space_guard_boundary() {
        check_hwm_node_id_space(u64::from(u32::MAX)).unwrap();
        assert!(check_hwm_node_id_space(u64::from(u32::MAX) + 1).is_err());
    }

    /// 2026-09-16, Stage B adversarial review P3-2: `dir_entry` must clamp
    /// the page-content `count` BEFORE using it — a corrupt `count > 813`
    /// with `idx >= 814` would otherwise slice past PAGE_SIZE (redo-path
    /// no-panic discipline).
    #[test]
    fn dir_entry_clamps_corrupt_count() {
        let mut page = dir_page(0, CAP + 1, PageId::INVALID);
        assert!(dir_entry(&page, CAP).is_err(), "count > capacity is loud");
        // A legal count but out-of-range idx is also loud (original check).
        page[DIR_OFF_COUNT..DIR_OFF_COUNT + 4].copy_from_slice(&1u32.to_le_bytes());
        assert!(dir_entry(&page, 1).is_err());
        // …while idx 0 succeeds on a well-formed single-entry page.
        let mut page = dir_page(0, 1, PageId::INVALID);
        let off = PAGE_HEADER_SIZE + DIR_HEADER_SIZE;
        page[off..off + 8].copy_from_slice(&42u64.to_le_bytes());
        page[off + 8..off + 10].copy_from_slice(&7u16.to_le_bytes());
        assert_eq!(dir_entry(&page, 0).unwrap(), (PageId(42), 7));
    }
}
