//! M3 Stage B (tech-selection §4.6): explicit slot addressing —
//! `SlottedPage::first_fit_slot` / `SlottedPage::add_tuple_at` units, and the
//! `add_tuple` degraded-composition behavior-parity checks.

use pg_am_heap::line_pointer::{LpFlags, LINE_POINTER_SIZE};
use pg_am_heap::slotted_page::{debug_assert_invariants, SlottedPage, HEAP_SPECIAL_SIZE};
use pg_am_heap::HeapError;
use pg_storage::types::PAGE_SIZE;

fn fresh_page() -> [u8; PAGE_SIZE] {
    let mut page = [0u8; PAGE_SIZE];
    SlottedPage::init_with_special(&mut page, HEAP_SPECIAL_SIZE);
    page
}

#[test]
fn first_fit_slot_is_none_without_unused_slots() {
    let mut page = fresh_page();
    assert_eq!(SlottedPage::first_fit_slot(&page), None);
    SlottedPage::add_tuple(&mut page, b"aaaa").unwrap();
    SlottedPage::add_tuple(&mut page, b"bbbb").unwrap();
    // Live (Normal) slots are never a first-fit target.
    assert_eq!(SlottedPage::first_fit_slot(&page), None);
}

#[test]
fn first_fit_slot_finds_the_first_unused_middle_slot() {
    let mut page = fresh_page();
    let s0 = SlottedPage::add_tuple(&mut page, b"aaaa").unwrap();
    let s1 = SlottedPage::add_tuple(&mut page, b"bbbb").unwrap();
    let s2 = SlottedPage::add_tuple(&mut page, b"cccc").unwrap();
    SlottedPage::delete_tuple(&mut page, s1).unwrap();
    SlottedPage::delete_tuple(&mut page, s2).unwrap();
    // First-fit: the LOWEST Unused slot wins, not the most recent.
    assert_eq!(SlottedPage::first_fit_slot(&page), Some(s1));
    SlottedPage::delete_tuple(&mut page, s0).unwrap();
    assert_eq!(SlottedPage::first_fit_slot(&page), Some(s0));
}

#[test]
fn add_tuple_at_appends_at_slot_count() {
    let mut page = fresh_page();
    let free_before = SlottedPage::free_space(&page);
    SlottedPage::add_tuple_at(&mut page, 0, b"hello").unwrap();
    assert_eq!(SlottedPage::slot_count(&page), 1);
    assert_eq!(SlottedPage::tuple(&page, 0).unwrap(), Some(&b"hello"[..]));
    // Appending costs the tuple plus one new line pointer.
    assert_eq!(
        SlottedPage::free_space(&page),
        free_before - 5 - LINE_POINTER_SIZE
    );
    debug_assert_invariants(&page);
}

#[test]
fn add_tuple_at_recycles_unused_slot_without_lp_cost() {
    let mut page = fresh_page();
    let s0 = SlottedPage::add_tuple(&mut page, b"aaaa").unwrap();
    let _s1 = SlottedPage::add_tuple(&mut page, b"bbbb").unwrap();
    SlottedPage::delete_tuple(&mut page, s0).unwrap();

    let free_before = SlottedPage::free_space(&page);
    SlottedPage::add_tuple_at(&mut page, s0, b"cc").unwrap();
    // Recycling reuses the existing LP: no slot_count growth, no LP cost.
    assert_eq!(SlottedPage::slot_count(&page), 2);
    assert_eq!(SlottedPage::free_space(&page), free_before - 2);
    assert_eq!(SlottedPage::tuple(&page, s0).unwrap(), Some(&b"cc"[..]));
    let lp = SlottedPage::line_pointer(&page, s0).unwrap();
    assert_eq!(lp.flags(), LpFlags::Normal);
    debug_assert_invariants(&page);
}

#[test]
fn add_tuple_at_rejects_slots_it_must_not_touch() {
    let mut page = fresh_page();
    let s0 = SlottedPage::add_tuple(&mut page, b"aaaa").unwrap();

    // Beyond slot_count: out of range.
    assert!(matches!(
        SlottedPage::add_tuple_at(&mut page, 5, b"xx"),
        Err(HeapError::InvalidSlot(5))
    ));
    // A live (Normal) slot: occupied — hard error, never an overwrite.
    assert!(matches!(
        SlottedPage::add_tuple_at(&mut page, s0, b"xx"),
        Err(HeapError::InvalidSlot(0))
    ));
    // The original tuple is untouched by the failed attempts.
    assert_eq!(SlottedPage::tuple(&page, s0).unwrap(), Some(&b"aaaa"[..]));
    assert_eq!(SlottedPage::slot_count(&page), 1);
}

#[test]
fn add_tuple_at_reports_page_full_for_both_shapes() {
    let mut page = fresh_page();
    let big = vec![0xCD; 1000];
    while SlottedPage::free_space(&page) >= 1000 + LINE_POINTER_SIZE {
        SlottedPage::add_tuple(&mut page, &big).unwrap();
    }
    let slot_count = SlottedPage::slot_count(&page) as u16;
    // Append shape: not enough room for tuple + new LP.
    assert!(matches!(
        SlottedPage::add_tuple_at(&mut page, slot_count, &big),
        Err(HeapError::PageFull { .. })
    ));
    // Recycle shape: an Unused slot exists but the bytes still do not fit.
    SlottedPage::delete_tuple(&mut page, 0).unwrap();
    assert!(matches!(
        SlottedPage::add_tuple_at(&mut page, 0, &vec![0xEE; PAGE_SIZE / 2]),
        Err(HeapError::PageFull { .. })
    ));
    // Validation parity with add_tuple: empty and oversized tuples.
    assert!(matches!(
        SlottedPage::add_tuple_at(&mut page, 0, &[]),
        Err(HeapError::InvalidArgument(_))
    ));
    assert!(matches!(
        SlottedPage::add_tuple_at(&mut page, 0, &vec![0xAB; PAGE_SIZE]),
        Err(HeapError::TupleTooLarge(_))
    ));
}

/// `add_tuple` is exactly `first_fit_slot().unwrap_or(slot_count)` +
/// `add_tuple_at`: drive two pages through the same mixed workload — one via
/// `add_tuple`, one via the explicit pair — and require identical slot
/// assignments and identical page bytes (external behavior unchanged).
#[test]
fn add_tuple_matches_the_explicit_composition() {
    let mut implicit = fresh_page();
    let mut explicit = fresh_page();
    let tuples: [&[u8]; 6] = [b"t0", b"t1-longer", b"t2", b"t3-x", b"t4", b"t5-yy"];
    for (i, bytes) in tuples.iter().enumerate() {
        let slot_a = SlottedPage::add_tuple(&mut implicit, bytes).unwrap();
        let slot_b = SlottedPage::first_fit_slot(&explicit)
            .unwrap_or(SlottedPage::slot_count(&explicit) as u16);
        SlottedPage::add_tuple_at(&mut explicit, slot_b, bytes).unwrap();
        assert_eq!(slot_a, slot_b, "slot assignment diverged at insert {i}");
        assert_eq!(slot_a as usize, i, "fresh pages append");
    }
    // Punch holes in both, then refill through both APIs.
    for s in [1u16, 3] {
        SlottedPage::delete_tuple(&mut implicit, s).unwrap();
        SlottedPage::delete_tuple(&mut explicit, s).unwrap();
    }
    for bytes in [&b"r1"[..], b"r3"] {
        let slot_a = SlottedPage::add_tuple(&mut implicit, bytes).unwrap();
        let slot_b = SlottedPage::first_fit_slot(&explicit)
            .unwrap_or(SlottedPage::slot_count(&explicit) as u16);
        SlottedPage::add_tuple_at(&mut explicit, slot_b, bytes).unwrap();
        assert_eq!(slot_a, slot_b);
    }
    assert_eq!(slot_reuse_check(&implicit), vec![1, 3]);
    assert_eq!(implicit, explicit, "page bytes must be identical");
    debug_assert_invariants(&implicit);
}

/// Collect the slots whose tuple starts with b'r' (the refilled holes above).
fn slot_reuse_check(page: &[u8; PAGE_SIZE]) -> Vec<u16> {
    let mut out = Vec::new();
    for slot in 0..SlottedPage::slot_count(page) as u16 {
        if let Some(bytes) = SlottedPage::tuple(page, slot).unwrap() {
            if bytes.starts_with(b"r") {
                out.push(slot);
            }
        }
    }
    out
}
