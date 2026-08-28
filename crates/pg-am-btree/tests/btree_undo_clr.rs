//! Stage S acceptance tests for ARIES undo of incomplete B+Tree splits.
//!
//! A split is a three-record protocol (Prepare / Copy / Commit). A crash
//! between Prepare and Commit leaves the left page flagged
//! `SPLIT_INCOMPLETE` with its downlink missing. Redo alone cannot fix that:
//! the records that would finish the split were never written. Recovery
//! therefore runs an undo pass that finishes the split and logs a
//! `BTreeSplitCLR` compensation record so the repair itself is crash-safe.
//!
//! These tests assert:
//!
//! - undo finishes the split: `SPLIT_INCOMPLETE` is cleared, the downlink
//!   exists, and the strict [`BTreeIndex::validate`] passes;
//! - the CLR is idempotent: crashing again and replaying it converges on the
//!   same pages, in particular the moved entries are never copied twice;
//! - both shapes are covered — a root split (new root + meta page update) and
//!   a non-root split (downlink inserted into an existing parent).

use std::path::Path;
use std::sync::Arc;

use pg_am_btree::page::{BtreePage, BTREE_FLAG_SPLIT_INCOMPLETE};
use pg_am_btree::{btree_redo_handlers, BTreeAM, BTreeIndex, BTreeUndoHandler};

use pg_am_heap::slotted_page::SlottedPage;
use pg_am_heap::tuple::ColumnType;
use pg_am_heap::HeapUndoHandler;
use pg_storage::config::StorageConfig;
use pg_storage::engine::StorageEngine;
use pg_storage::types::{Lsn, Oid, PageId, Tid, PAGE_SIZE};

use tempfile::TempDir;

const REL_OID: Oid = Oid(16_401);

fn tid(i: u64) -> Tid {
    Tid {
        page_id: PageId(51_000 + i),
        slot_id: i as u16,
    }
}

fn key(i: i32) -> Vec<u8> {
    pg_am_btree::encode_i32(i).to_vec()
}

fn decode(rows: &[(Vec<u8>, Tid)]) -> Vec<i32> {
    rows.iter()
        .map(|(k, _)| pg_am_btree::decode_i32(k.clone().try_into().unwrap()))
        .collect()
}

/// Create an index and fill its root leaf until the next entry would no
/// longer fit (so the next insert would split). Returns the key count.
fn create_and_fill(dir: &Path, config: &StorageConfig) -> (StorageEngine, BTreeIndex, i32) {
    let engine = StorageEngine::open(dir, config).unwrap();
    let am = BTreeAM::new(
        Arc::clone(engine.buffer_pool()),
        Arc::clone(engine.wal_writer()),
    );
    let mut index = am.create_index(REL_OID, ColumnType::Int4).unwrap();
    // i32 key (4B) + tid trailer (10B) + line pointer (4B).
    const ENTRY_BYTES: usize = 4 + 10 + 4;
    let mut n = 0i32;
    while index.page_free_space(index.root_page()).unwrap() >= ENTRY_BYTES {
        index.insert(&key(n), tid(n as u64)).unwrap();
        n += 1;
    }
    assert!(n > 100, "a page must hold a meaningful number of entries");
    (engine, index, n)
}

fn recover(dir: &Path, config: &StorageConfig, meta_page: PageId) -> (StorageEngine, BTreeIndex) {
    let engine = StorageEngine::open_with_redo_handlers(
        dir,
        config,
        btree_redo_handlers(),
        vec![Box::new(HeapUndoHandler), Box::new(BTreeUndoHandler)],
    )
    .unwrap();
    let am = BTreeAM::new(
        Arc::clone(engine.buffer_pool()),
        Arc::clone(engine.wal_writer()),
    );
    let index = am.open_index(REL_OID, meta_page, ColumnType::Int4).unwrap();
    (engine, index)
}

fn assert_all_keys(index: &BTreeIndex, n: i32) {
    let rows = index.range_scan(None, None).unwrap();
    let got = decode(&rows);
    let want: Vec<i32> = (0..n).collect();
    assert_eq!(got, want, "leaf chain must hold every key exactly once");
    for i in 0..n {
        assert_eq!(
            index.lookup(&key(i)).unwrap(),
            Some(tid(i as u64)),
            "key {i} must be reachable by point lookup"
        );
    }
}

fn chain_from(engine: &StorageEngine, start: PageId) -> Vec<PageId> {
    let mut out = vec![start];
    loop {
        let cur = *out.last().unwrap();
        let guard = engine.buffer_pool().pin(cur).unwrap();
        let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
        let next = BtreePage::next(page).unwrap();
        drop(guard);
        if next == PageId::INVALID {
            return out;
        }
        out.push(next);
        assert!(out.len() < 1000, "sibling chain cycle");
    }
}

/// `btpo_flags` / slot count / pd_lsn of a page.
type PageState = (u8, usize, Lsn);

fn page_state(engine: &StorageEngine, page_id: PageId) -> PageState {
    let guard = engine.buffer_pool().pin(page_id).unwrap();
    let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
    (
        BtreePage::flags(page).unwrap(),
        SlottedPage::slot_count(page),
        pg_storage::page::page_pd_lsn(page),
    )
}

/// Crash with only `BTreeSplitPrepare` durable, then recover: the undo pass
/// must finish the split end to end.
#[test]
fn test_recovery_incomplete_split_undo() {
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());

    let (meta_page, n, left, right) = {
        let (engine, index, n) = create_and_fill(tmp.path(), &config);
        let left = index.root_page();
        let st = index.split_prepare(left).unwrap();
        engine.wal_writer().flush().unwrap();
        std::mem::forget(engine);
        (index.meta_page(), n, st.left, st.right)
    };

    let (engine, index) = recover(tmp.path(), &config, meta_page);

    let (left_flags, left_slots, _) = page_state(&engine, left);
    let (_, right_slots, _) = page_state(&engine, right);
    assert_eq!(
        left_flags & BTREE_FLAG_SPLIT_INCOMPLETE,
        0,
        "undo must clear SPLIT_INCOMPLETE"
    );
    assert_eq!(
        left_slots + right_slots,
        n as usize,
        "the two halves must together hold every key exactly once"
    );
    assert!(
        right_slots > 0,
        "undo must move the upper half onto the right page"
    );

    assert_ne!(index.root_page(), left, "a root split needs a new root");
    assert_eq!(index.tree_level(), 1);
    assert_eq!(chain_from(&engine, left), vec![left, right]);
    assert_all_keys(&index, n);
    index
        .validate()
        .unwrap_or_else(|e| panic!("recovered tree must validate: {e}"));
    drop(engine);
}

/// Replaying the CLR must be a no-op. Recovering three times in a row (each
/// time abandoning the engine without a checkpoint, so the whole WAL including
/// the CLR is replayed again) must converge on identical pages.
///
/// This is the regression test for the CLR re-running its split copy: because
/// `apply_split_copy` appends to the right page, an unguarded replay moved the
/// upper half a second time and doubled the right page's entries.
#[test]
fn test_clr_idempotent() {
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());

    let (meta_page, n, left, right) = {
        let (engine, index, n) = create_and_fill(tmp.path(), &config);
        let left = index.root_page();
        let st = index.split_prepare(left).unwrap();
        engine.wal_writer().flush().unwrap();
        std::mem::forget(engine);
        (index.meta_page(), n, st.left, st.right)
    };

    let mut previous: Option<([PageState; 3], PageId)> = None;
    for round in 1..=3 {
        let (engine, index) = recover(tmp.path(), &config, meta_page);
        let root = index.root_page();
        let state = [
            page_state(&engine, left),
            page_state(&engine, right),
            page_state(&engine, root),
        ];

        assert_all_keys(&index, n);
        assert_eq!(index.tree_level(), 1, "round {round}");
        index
            .validate()
            .unwrap_or_else(|e| panic!("round {round} must validate: {e}"));

        if let Some((want_state, want_root)) = &previous {
            assert_eq!(&root, want_root, "round {round} moved the root");
            assert_eq!(
                &state, want_state,
                "round {round} diverged: replaying the CLR is not idempotent"
            );
        }
        previous = Some((state, root));

        std::mem::forget(engine); // crash again with the CLR in the WAL
    }
}

/// Crash with `BTreeSplitPrepare` + `BTreeSplitCopy` durable: redo already
/// moved the entries, so undo must finish the split *without* moving them
/// again, and the follow-up replay must stay idempotent too.
#[test]
fn test_clr_after_copy_crash() {
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());

    let (meta_page, n, left, right, copy_start_slot) = {
        let (engine, index, n) = create_and_fill(tmp.path(), &config);
        let left = index.root_page();
        let st = index.split_prepare(left).unwrap();
        index.split_copy(&st).unwrap();
        engine.wal_writer().flush().unwrap();
        std::mem::forget(engine);
        (index.meta_page(), n, st.left, st.right, st.copy_start_slot)
    };

    let (engine, index) = recover(tmp.path(), &config, meta_page);
    let (left_flags, left_slots, _) = page_state(&engine, left);
    let (_, right_slots, _) = page_state(&engine, right);
    assert_eq!(left_flags & BTREE_FLAG_SPLIT_INCOMPLETE, 0);
    assert_eq!(
        left_slots, copy_start_slot as usize,
        "the left page must keep exactly the entries below the split slot"
    );
    assert_eq!(
        right_slots,
        n as usize - copy_start_slot as usize,
        "the moved entries must appear on the right page exactly once"
    );
    assert_all_keys(&index, n);
    index
        .validate()
        .unwrap_or_else(|e| panic!("recovered tree must validate: {e}"));

    let root = index.root_page();
    let r1 = [
        page_state(&engine, left),
        page_state(&engine, right),
        page_state(&engine, root),
    ];
    std::mem::forget(engine);

    let (engine, index) = recover(tmp.path(), &config, meta_page);
    assert_eq!(index.root_page(), root);
    assert_eq!(
        [
            page_state(&engine, left),
            page_state(&engine, right),
            page_state(&engine, root),
        ],
        r1,
        "replaying Prepare+Copy+CLR must converge"
    );
    assert_all_keys(&index, n);
    drop(engine);
}

/// The non-root shape: the split's downlink goes into an existing parent
/// instead of creating a new root, and the meta page is left alone.
#[test]
fn test_clr_non_root_split_undo() {
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());

    let (meta_page, n, root_before, left, right) = {
        let (engine, mut index, mut n) = create_and_fill(tmp.path(), &config);
        // Grow past the first root split so the leaf we break is not the root.
        while index.tree_level() == 0 {
            index.insert(&key(n), tid(n as u64)).unwrap();
            n += 1;
        }
        for _ in 0..50 {
            index.insert(&key(n), tid(n as u64)).unwrap();
            n += 1;
        }
        assert_eq!(index.tree_level(), 1);

        // Break the leftmost leaf, which now has a parent.
        let (leaf, _path, _) = index.descend_to_leaf(&key(0), &tid(0)).unwrap();
        assert_ne!(leaf, index.root_page(), "the target leaf must not be root");
        let st = index.split_prepare(leaf).unwrap();
        engine.wal_writer().flush().unwrap();
        let root_before = index.root_page();
        std::mem::forget(engine);
        (index.meta_page(), n, root_before, st.left, st.right)
    };

    let (engine, index) = recover(tmp.path(), &config, meta_page);
    assert_eq!(
        index.root_page(),
        root_before,
        "a non-root split must not install a new root"
    );
    assert_eq!(index.tree_level(), 1, "the tree must not grow a level");

    let (left_flags, _, _) = page_state(&engine, left);
    assert_eq!(left_flags & BTREE_FLAG_SPLIT_INCOMPLETE, 0);
    assert_eq!(
        chain_from(&engine, left)[..2],
        [left, right],
        "the new right sibling must be spliced into the chain"
    );
    assert_all_keys(&index, n);
    index
        .validate()
        .unwrap_or_else(|e| panic!("recovered tree must validate: {e}"));

    // And the repair survives a second crash.
    let r1_left = page_state(&engine, left);
    let r1_right = page_state(&engine, right);
    std::mem::forget(engine);

    let (engine, index) = recover(tmp.path(), &config, meta_page);
    assert_eq!(page_state(&engine, left), r1_left);
    assert_eq!(page_state(&engine, right), r1_right);
    assert_eq!(index.root_page(), root_before);
    assert_all_keys(&index, n);
    drop(engine);
}

// ---------------------------------------------------------------------
// Post-Stage-S adversarial review: C1 / C2 / H3 regression tests
// ---------------------------------------------------------------------

/// Run `f` on a worker thread under a watchdog: a latch-order or recovery
/// bug must fail the test in bounded time, never hang CI. All scenarios
/// below are fully deterministic (no sleeps, no races) — the timeout only
/// guards against deadlocks and infinite loops.
fn with_watchdog(f: impl FnOnce() + Send + 'static) {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        f();
        let _ = tx.send(());
    });
    rx.recv_timeout(std::time::Duration::from_secs(300))
        .expect("test deadlocked or exceeded the watchdog budget");
}

/// Bytes a downlink occupies on an internal page (i32 key 4B + child 8B +
/// one line pointer).
const INTERNAL_ENTRY_BYTES: usize = 4 + 8 + 4;

/// Create an index and fill its root leaf with EVEN keys `0, 2, …, 2n-2`
/// until the next entry would no longer fit, leaving the odd keys available
/// as pending inserts that land in a chosen half of the split.
fn create_and_fill_even(dir: &Path, config: &StorageConfig) -> (StorageEngine, BTreeIndex, i32) {
    let engine = StorageEngine::open(dir, config).unwrap();
    let am = BTreeAM::new(
        Arc::clone(engine.buffer_pool()),
        Arc::clone(engine.wal_writer()),
    );
    let mut index = am.create_index(REL_OID, ColumnType::Int4).unwrap();
    const ENTRY_BYTES: usize = 4 + 10 + 4;
    let mut n = 0i32;
    while index.page_free_space(index.root_page()).unwrap() >= ENTRY_BYTES {
        index.insert(&key(2 * n), tid(n as u64)).unwrap();
        n += 1;
    }
    assert!(n > 100, "a page must hold a meaningful number of entries");
    (engine, index, n)
}

/// Assert the leaf chain holds exactly `want_keys` (sorted, i32 keys), the
/// pending key maps to `pending_tid` (when given), and every even key
/// `2*i` maps to `tid(i)`.
fn assert_keys_with_pending(index: &BTreeIndex, want_keys: &[i32], pending: Option<(i32, u64)>) {
    let rows = index.range_scan(None, None).unwrap();
    assert_eq!(
        decode(&rows),
        want_keys,
        "leaf chain must hold exactly the expected keys"
    );
    for &k in want_keys {
        let expect = if let Some((pk, pt)) = pending {
            if pk == k {
                tid(pt)
            } else {
                tid((k / 2) as u64)
            }
        } else {
            tid((k / 2) as u64)
        };
        assert_eq!(index.lookup(&key(k)).unwrap(), Some(expect), "key {k}");
    }
}

/// Expected key set after a split of the even-key fill plus one pending odd
/// key: every even `0..2n-2` plus `pending`.
fn even_keys_plus(n: i32, pending: i32) -> Vec<i32> {
    (0..2 * n - 1)
        .filter(|k| k % 2 == 0 || *k == pending)
        .collect()
}

// ---------------------------------------------------------------------
// C1: pending insert / deletes in the Copy→Commit window
// ---------------------------------------------------------------------

/// Review C1, main variant: crash between Copy and Commit with the split's
/// pending entry WAL-logged into the LEFT half (the online protocol's ~50%
/// case). WAL at crash: Prepare, Copy, BTreeInsert(left). Redo leaves the
/// left page with `copy_start_slot + 1` entries. The pre-fix undo read the
/// separator from left[copy_start_slot] — the pending entry, not the right
/// page's first key — and re-appended it to the right page's END, silently
/// corrupting the right page's sort order and the parent's key ranges.
/// The fix keys every decision off the RIGHT page's content; this test
/// fails on pre-fix code at `validate` (right page entries out of order).
#[test]
fn test_clr_pending_insert_landed_left() {
    with_watchdog(|| {
        let tmp = TempDir::new().unwrap();
        let config = StorageConfig::new(tmp.path());

        const PENDING_TID: u64 = 99_000;
        let (meta_page, n, left, right, s) = {
            let (engine, mut index, n) = create_and_fill_even(tmp.path(), &config);
            let left = index.root_page();
            let st = index.split_prepare(left).unwrap();
            index.split_copy(&st).unwrap();
            // The pending entry, applied after Copy exactly as the online
            // protocol does: key 2s-1 sorts just below the right page's
            // first entry (key 2s), so it lands in the LEFT half.
            let pending = 2 * st.copy_start_slot as i32 - 1;
            index.insert(&key(pending), tid(PENDING_TID)).unwrap();
            assert_eq!(index.lookup(&key(pending)).unwrap(), Some(tid(PENDING_TID)));
            engine.wal_writer().flush().unwrap();
            std::mem::forget(engine); // kill -9: Prepare, Copy, BTreeInsert(left) durable
            (
                index.meta_page(),
                n,
                st.left,
                st.right,
                st.copy_start_slot as i32,
            )
        };

        let pending = 2 * s - 1;
        let want = even_keys_plus(n, pending);
        let (engine, index) = recover(tmp.path(), &config, meta_page);
        assert_keys_with_pending(&index, &want, Some((pending, PENDING_TID)));
        assert_eq!(index.tree_level(), 1);
        assert_eq!(chain_from(&engine, left), vec![left, right]);
        let (left_flags, left_slots, _) = page_state(&engine, left);
        assert_eq!(left_flags & BTREE_FLAG_SPLIT_INCOMPLETE, 0);
        assert_eq!(
            left_slots,
            s as usize + 1,
            "the left page keeps its half PLUS the pending entry"
        );
        index
            .validate()
            .unwrap_or_else(|e| panic!("recovered tree must validate: {e}"));

        // A second crash + recovery replays Prepare+Copy+Insert+CLR and
        // must converge on identical pages.
        let root = index.root_page();
        let r1 = [
            page_state(&engine, left),
            page_state(&engine, right),
            page_state(&engine, root),
        ];
        std::mem::forget(engine);
        let (engine, index) = recover(tmp.path(), &config, meta_page);
        assert_eq!(index.root_page(), root);
        assert_eq!(
            [
                page_state(&engine, left),
                page_state(&engine, right),
                page_state(&engine, root),
            ],
            r1,
            "replaying the in-window insert + CLR must converge"
        );
        assert_keys_with_pending(&index, &want, Some((pending, PENDING_TID)));
        index.validate().unwrap();
        drop(engine);
    });
}

/// Review C1, mirror variant: the pending entry lands in the RIGHT half.
/// WAL at crash: Prepare, Copy, BTreeInsert(right). This shape happened to
/// survive the pre-fix logic (a no-op move); it is kept as regression
/// coverage so the rewrite cannot break it.
#[test]
fn test_clr_pending_insert_landed_right() {
    with_watchdog(|| {
        let tmp = TempDir::new().unwrap();
        let config = StorageConfig::new(tmp.path());

        const PENDING_TID: u64 = 99_001;
        let (meta_page, n, left, right, s) = {
            let (engine, mut index, n) = create_and_fill_even(tmp.path(), &config);
            let left = index.root_page();
            let st = index.split_prepare(left).unwrap();
            index.split_copy(&st).unwrap();
            // Key 2s+1 sorts between the right page's first entry (2s) and
            // its second (2s+2): it lands in the RIGHT half.
            let pending = 2 * st.copy_start_slot as i32 + 1;
            index.insert(&key(pending), tid(PENDING_TID)).unwrap();
            engine.wal_writer().flush().unwrap();
            std::mem::forget(engine); // kill -9: Prepare, Copy, BTreeInsert(right) durable
            (
                index.meta_page(),
                n,
                st.left,
                st.right,
                st.copy_start_slot as i32,
            )
        };

        let pending = 2 * s + 1;
        let want = even_keys_plus(n, pending);
        let (engine, index) = recover(tmp.path(), &config, meta_page);
        assert_keys_with_pending(&index, &want, Some((pending, PENDING_TID)));
        assert_eq!(chain_from(&engine, left), vec![left, right]);
        let (left_flags, left_slots, _) = page_state(&engine, left);
        assert_eq!(left_flags & BTREE_FLAG_SPLIT_INCOMPLETE, 0);
        assert_eq!(
            left_slots, s as usize,
            "the left page keeps exactly its half"
        );
        index
            .validate()
            .unwrap_or_else(|e| panic!("recovered tree must validate: {e}"));

        let root = index.root_page();
        let r1 = [
            page_state(&engine, left),
            page_state(&engine, right),
            page_state(&engine, root),
        ];
        std::mem::forget(engine);
        let (engine, index) = recover(tmp.path(), &config, meta_page);
        assert_eq!(
            [
                page_state(&engine, left),
                page_state(&engine, right),
                page_state(&engine, index.root_page()),
            ],
            r1
        );
        assert_keys_with_pending(&index, &want, Some((pending, PENDING_TID)));
        index.validate().unwrap();
        drop(engine);
    });
}

/// Review C1, delete variant: a concurrent transaction deletes keys below
/// the split slot in the Copy→Commit window (the delete path never checks
/// `SPLIT_INCOMPLETE`). WAL at crash: Prepare, Copy, BTreeDelete×3(left).
/// The left page ends up with `copy_start_slot - 3` entries; the pre-fix
/// undo fed `copy_start_slot` to the move anyway and hard-failed recovery
/// with "copy_start_slot outside slot count". The fix never counts slots on
/// the left page when the right page is non-empty.
#[test]
fn test_clr_delete_in_window_below_split_slot() {
    with_watchdog(|| {
        let tmp = TempDir::new().unwrap();
        let config = StorageConfig::new(tmp.path());

        let (meta_page, n, left, right) = {
            let (engine, mut index, n) = create_and_fill_even(tmp.path(), &config);
            let left = index.root_page();
            let st = index.split_prepare(left).unwrap();
            index.split_copy(&st).unwrap();
            // In-window deletes of the three smallest keys — all below the
            // split slot, shrinking the left page past it.
            for i in 0..3i32 {
                index.delete(&key(2 * i), tid(i as u64)).unwrap();
            }
            engine.wal_writer().flush().unwrap();
            std::mem::forget(engine); // kill -9: Prepare, Copy, BTreeDelete×3 durable
            (index.meta_page(), n, st.left, st.right)
        };

        let want: Vec<i32> = (3..n).map(|i| 2 * i).collect();
        let (engine, index) = recover(tmp.path(), &config, meta_page);
        assert_keys_with_pending(&index, &want, None);
        for i in 0..3i32 {
            assert_eq!(
                index.lookup(&key(2 * i)).unwrap(),
                None,
                "key {} was deleted in-window and must stay deleted",
                2 * i
            );
        }
        assert_eq!(chain_from(&engine, left), vec![left, right]);
        let (left_flags, _, _) = page_state(&engine, left);
        assert_eq!(left_flags & BTREE_FLAG_SPLIT_INCOMPLETE, 0);
        index
            .validate()
            .unwrap_or_else(|e| panic!("recovered tree must validate: {e}"));

        let root = index.root_page();
        let r1 = [
            page_state(&engine, left),
            page_state(&engine, right),
            page_state(&engine, root),
        ];
        std::mem::forget(engine);
        let (engine, index) = recover(tmp.path(), &config, meta_page);
        assert_eq!(
            [
                page_state(&engine, left),
                page_state(&engine, right),
                page_state(&engine, index.root_page()),
            ],
            r1,
            "replaying the in-window deletes + CLR must converge"
        );
        assert_keys_with_pending(&index, &want, None);
        index.validate().unwrap();
        drop(engine);
    });
}

/// Review C1, corner variant: in-window deletes drain the ENTIRE right half
/// before the crash. The right page is empty but has tuple-byte scars
/// (`pd_upper < pd_special`), which distinguishes "Copy ran, everything was
/// deleted" from "Copy never ran". Completing the split is then impossible
/// (no first entry to anchor the separator) and re-moving would resurrect
/// the deletes, so the undo ABANDONS the split: the empty right page is
/// spliced out of the chain, `SPLIT_INCOMPLETE` is cleared, and — this
/// being a root split — the ROOT flag is preserved. Pre-fix code hard-failed
/// recovery with "no entries to move on either page".
#[test]
fn test_clr_right_half_deleted_in_window_unlinks() {
    with_watchdog(|| {
        let tmp = TempDir::new().unwrap();
        let config = StorageConfig::new(tmp.path());

        let (meta_page, _n, left, right, s) = {
            let (engine, mut index, n) = create_and_fill_even(tmp.path(), &config);
            let left = index.root_page();
            let st = index.split_prepare(left).unwrap();
            index.split_copy(&st).unwrap();
            // Delete every key that moved right (keys 2s..2n-2): the right
            // page drains to empty inside the Copy→Commit window.
            for i in st.copy_start_slot as i32..n {
                index.delete(&key(2 * i), tid(i as u64)).unwrap();
            }
            engine.wal_writer().flush().unwrap();
            std::mem::forget(engine); // kill -9
            (
                index.meta_page(),
                n,
                st.left,
                st.right,
                st.copy_start_slot as i32,
            )
        };

        let want: Vec<i32> = (0..s).map(|i| 2 * i).collect();
        let (engine, index) = recover(tmp.path(), &config, meta_page);
        assert_keys_with_pending(&index, &want, None);
        // The split was abandoned: no new root, the orphan is out of the
        // chain, and the left page is whole again.
        assert_eq!(
            index.tree_level(),
            0,
            "an abandoned root split keeps level 0"
        );
        assert_eq!(index.root_page(), left);
        assert_eq!(
            chain_from(&engine, left),
            vec![left],
            "the drained right page must be spliced out of the chain"
        );
        let (left_flags, left_slots, _) = page_state(&engine, left);
        assert_eq!(left_flags & BTREE_FLAG_SPLIT_INCOMPLETE, 0);
        assert_eq!(left_slots, s as usize);
        index
            .validate()
            .unwrap_or_else(|e| panic!("recovered tree must validate: {e}"));

        std::mem::forget(engine);
        let (engine, index) = recover(tmp.path(), &config, meta_page);
        assert_eq!(index.root_page(), left);
        assert_eq!(chain_from(&engine, left), vec![left]);
        assert_keys_with_pending(&index, &want, None);
        index.validate().unwrap();
        // The orphan stays allocated (leaked, corrupting nothing); make sure
        // nothing resurrected it into the chain.
        let _ = right;
        drop(engine);
    });
}

// ---------------------------------------------------------------------
// C2: undo-time cascade when the parent has no room for the downlink
// ---------------------------------------------------------------------

/// Build the C2 base state: a level-1 tree whose ROOT (the only internal
/// page) cannot hold another downlink, plus a leaf split that crashed
/// between Copy and Commit. The online Commit would have cascaded; undo
/// must do the same instead of dying with `PageFull` (which the already-
/// flushed CLR would replay forever — a permanent brick).
fn setup_parent_full_crash(
    dir: &Path,
    config: &StorageConfig,
) -> (PageId, i32, PageId, PageId, PageId) {
    let (engine, mut index, mut n) = create_and_fill(dir, config);
    while index.tree_level() == 0 {
        index.insert(&key(n), tid(n as u64)).unwrap();
        n += 1;
    }
    // Fill the root until it cannot hold another downlink. The check runs
    // before every insert, and a leaf split consumes exactly one internal
    // entry, so the root never splits online here.
    while index.page_free_space(index.root_page()).unwrap() >= INTERNAL_ENTRY_BYTES {
        index.insert(&key(n), tid(n as u64)).unwrap();
        n += 1;
    }
    assert_eq!(index.tree_level(), 1);

    let (leaf, _, _) = index.descend_to_leaf(&key(0), &tid(0)).unwrap();
    assert_ne!(leaf, index.root_page(), "the target leaf must not be root");
    let st = index.split_prepare(leaf).unwrap();
    index.split_copy(&st).unwrap();
    engine.wal_writer().flush().unwrap();
    let root_before = index.root_page();
    std::mem::forget(engine); // kill -9: leaf split pending, parent full
    (index.meta_page(), n, root_before, st.left, st.right)
}

/// Review C2: the pending leaf split's parent is full. Undo must split the
/// parent first — here the parent IS the root, so this also covers
/// root-split-during-undo — then insert the downlink. Pre-fix code hit
/// `PageFull` from `insert_entry_at` AFTER the CLR was already flushed;
/// replaying that CLR on the next open failed the same way, so the database
/// could never open again. This test fails pre-fix at the first `recover`.
#[test]
fn test_undo_cascade_parent_full() {
    with_watchdog(|| {
        let tmp = TempDir::new().unwrap();
        let config = StorageConfig::new(tmp.path());
        let (meta_page, n, root_before, left, right) = setup_parent_full_crash(tmp.path(), &config);

        let (engine, index) = recover(tmp.path(), &config, meta_page);
        // The undo cascade split the full root: the tree grew a level.
        assert_eq!(index.tree_level(), 2, "the cascade must promote the root");
        assert_ne!(index.root_page(), root_before);
        assert_all_keys(&index, n);
        assert_eq!(
            chain_from(&engine, left)[..2],
            [left, right],
            "the split twin must be spliced into the chain"
        );
        let (left_flags, _, _) = page_state(&engine, left);
        assert_eq!(left_flags & BTREE_FLAG_SPLIT_INCOMPLETE, 0);
        index
            .validate()
            .unwrap_or_else(|e| panic!("cascaded tree must validate: {e}"));

        // Second recovery: the cascade CLR + leaf CLR replay as no-ops.
        let root = index.root_page();
        let r1 = [
            page_state(&engine, left),
            page_state(&engine, right),
            page_state(&engine, root),
        ];
        std::mem::forget(engine);
        let (engine, index) = recover(tmp.path(), &config, meta_page);
        assert_eq!(index.root_page(), root);
        assert_eq!(
            [
                page_state(&engine, left),
                page_state(&engine, right),
                page_state(&engine, index.root_page()),
            ],
            r1,
            "replaying the cascade must converge"
        );
        assert_all_keys(&index, n);
        index.validate().unwrap();
        drop(engine);
    });
}

/// Review C2: crash mid-cascade. The hook fails the first recovery right
/// after the parent's (root's) cascade CLR is durable — equivalent to a
/// kill -9 in that exact instant. The second recovery must replay the
/// cascade CLR as a no-op and finish the original leaf split. Deterministic:
/// the failure is injected by a thread-local counter, not by timing.
#[test]
fn test_undo_cascade_crash_mid_cascade_recovers() {
    with_watchdog(|| {
        let tmp = TempDir::new().unwrap();
        let config = StorageConfig::new(tmp.path());
        let (meta_page, n, _root_before, _left, _right) =
            setup_parent_full_crash(tmp.path(), &config);

        // First recovery: arm the hook so undo dies immediately after the
        // cascade's CLR (root split) completes — the leaf split is still
        // unfinished, its CLR never written.
        pg_am_btree::index::UNDO_CASCADE_FAILURES.with(|f| f.set(1));
        let crashed = StorageEngine::open_with_redo_handlers(
            tmp.path(),
            &config,
            btree_redo_handlers(),
            vec![Box::new(HeapUndoHandler), Box::new(BTreeUndoHandler)],
        );
        assert!(
            crashed.is_err(),
            "the injected mid-cascade crash must abort the first recovery"
        );

        // Second recovery (hook exhausted): must converge.
        let (engine, index) = recover(tmp.path(), &config, meta_page);
        assert_eq!(index.tree_level(), 2);
        assert_all_keys(&index, n);
        index
            .validate()
            .unwrap_or_else(|e| panic!("re-recovered cascade must validate: {e}"));

        // And a third crash/recovery stays put.
        std::mem::forget(engine);
        let (engine, index) = recover(tmp.path(), &config, meta_page);
        assert_eq!(index.tree_level(), 2);
        assert_all_keys(&index, n);
        index.validate().unwrap();
        drop(engine);
    });
}

/// Review C2, multi-level: the pending leaf split's parent is full AND the
/// parent of THAT page (the root) is full too, so undo must cascade twice —
/// split the level-1 parent, then split the root — before the leaf's
/// downlink lands. Constructed deterministically with ~600-byte Text keys
/// (~13 entries per page at every level); the interior level-1 page is
/// filled with duplicate keys (distinct TIDs) routing into its range.
///
/// Post-Stage-S C2 deep review: the first recovery is additionally crashed
/// BETWEEN cascade levels via `UNDO_CASCADE_FAILURES` — the level-1 parent's
/// split CLR is durable while the root's cascade CLR and the leaf's CLR are
/// not — and the second recovery must re-derive the rest and converge.
#[test]
fn test_undo_cascade_multi_level() {
    with_watchdog(|| {
        const REL_OID_TEXT: Oid = Oid(16_402);
        fn big_key(i: u64) -> Vec<u8> {
            let mut k = format!("{i:06}").into_bytes();
            k.resize(600, b'x');
            k
        }
        // Internal entry: 600B key + 8B child + 4B line pointer.
        const BIG_INTERNAL: usize = 600 + 8 + 4;

        let tmp = TempDir::new().unwrap();
        let config = StorageConfig::new(tmp.path());

        let (meta_page, total) = {
            let engine = StorageEngine::open(tmp.path(), &config).unwrap();
            let am = BTreeAM::new(
                Arc::clone(engine.buffer_pool()),
                Arc::clone(engine.wal_writer()),
            );
            let mut index = am.create_index(REL_OID_TEXT, ColumnType::Text).unwrap();

            // Grow to a level-2 tree.
            let mut i = 0u64;
            while index.tree_level() < 2 {
                index.insert(&big_key(i), tid(i)).unwrap();
                i += 1;
            }
            // Fill the root (level 2) until it cannot hold another downlink.
            // Only level-1 cascades consume root space (exactly one entry
            // each), so the root never splits online here.
            while index.page_free_space(index.root_page()).unwrap() >= BIG_INTERNAL {
                index.insert(&big_key(i), tid(i)).unwrap();
                i += 1;
            }
            assert_eq!(index.tree_level(), 2);

            // Fill the LEFTMOST level-1 page P with duplicates of the small
            // keys in its range (distinct TIDs make each insert unique).
            let (_leaf0, path0, _) = index.descend_to_leaf(&big_key(0), &tid(0)).unwrap();
            let p_page = *path0.last().expect("level-2 tree: path ends at P");
            let mut dup = 0u64;
            while index.page_free_space(p_page).unwrap() >= BIG_INTERNAL {
                index
                    .insert(&big_key(dup % 3), tid(1_000_000 + dup))
                    .unwrap();
                dup += 1;
            }

            // Pending split: the leftmost leaf (under the now-full P),
            // crashed between Copy and Commit.
            let (leaf, _, _) = index.descend_to_leaf(&big_key(0), &tid(0)).unwrap();
            let st = index.split_prepare(leaf).unwrap();
            index.split_copy(&st).unwrap();
            engine.wal_writer().flush().unwrap();
            std::mem::forget(engine); // kill -9
            (index.meta_page(), (i + dup) as usize)
        };

        // Post-Stage-S C2 deep review: crash BETWEEN cascade levels. The hook
        // fails the first recovery right after the level-1 page P's split CLR
        // is durable — P's right page is initialized (init FPI durable) and
        // stamped by P's CLR, but the root's cascade CLR and the leaf's own
        // CLR were never written. The second recovery must replay P's CLR as
        // a no-op, re-derive the leaf's downlink target from the post-cascade
        // pages, run the remaining root cascade, and converge.
        pg_am_btree::index::UNDO_CASCADE_FAILURES.with(|f| f.set(1));
        let crashed = StorageEngine::open_with_redo_handlers(
            tmp.path(),
            &config,
            btree_redo_handlers(),
            vec![Box::new(HeapUndoHandler), Box::new(BTreeUndoHandler)],
        );
        assert!(
            crashed.is_err(),
            "the injected inter-level crash must abort the first recovery"
        );

        let (engine, index) = recover(tmp.path(), &config, meta_page);
        // Two cascade levels: P split, then the root split → level 3.
        assert_eq!(
            index.tree_level(),
            3,
            "the undo cascade must climb two full levels and promote the root"
        );
        let rows = index.range_scan(None, None).unwrap();
        assert_eq!(rows.len(), total, "no entry lost or duplicated");
        assert!(index.lookup(&big_key(0)).unwrap().is_some());
        index
            .validate()
            .unwrap_or_else(|e| panic!("multi-level cascaded tree must validate: {e}"));

        // Third recovery (the second one replayed the inter-level-crash
        // state) converges.
        let root = index.root_page();
        std::mem::forget(engine);
        let (engine, index) = recover(tmp.path(), &config, meta_page);
        assert_eq!(index.root_page(), root);
        assert_eq!(index.tree_level(), 3);
        let rows = index.range_scan(None, None).unwrap();
        assert_eq!(rows.len(), total);
        index.validate().unwrap();
        drop(engine);
    });
}

// ---------------------------------------------------------------------
// H3: incomplete splits invisible to redo (Prepare before the checkpoint)
// ---------------------------------------------------------------------

/// Review H3, Prepare-only variant: the split's Prepare is logged, both
/// pages are flushed by a completed checkpoint, and the crash comes before
/// Commit. Redo starts at the checkpoint and never sees the Prepare, so the
/// incomplete-split tracker is empty; only the undo-time page scan for
/// `SPLIT_INCOMPLETE` can find the split. Pre-fix, recovery left the flag
/// set forever (writes to the range wedge on the restart budget); this test
/// fails pre-fix at `validate` ("still SPLIT_INCOMPLETE").
#[test]
fn test_undo_scan_split_prepare_before_checkpoint() {
    with_watchdog(|| {
        let tmp = TempDir::new().unwrap();
        let config = StorageConfig::new(tmp.path());

        let (meta_page, n, left, right) = {
            let (engine, index, n) = create_and_fill(tmp.path(), &config);
            let left = index.root_page();
            let st = index.split_prepare(left).unwrap();
            engine.wal_writer().flush().unwrap();
            // The checkpoint flushes both pages (SPLIT_INCOMPLETE durable)
            // and moves the redo start PAST the Prepare record.
            engine.trigger_checkpoint().unwrap();
            std::mem::forget(engine); // kill -9 before Commit
            (index.meta_page(), n, st.left, st.right)
        };

        let (engine, index) = recover(tmp.path(), &config, meta_page);
        let (left_flags, left_slots, _) = page_state(&engine, left);
        assert_eq!(
            left_flags & BTREE_FLAG_SPLIT_INCOMPLETE,
            0,
            "the page scan must find and finish the checkpoint-hidden split"
        );
        assert!(left_slots > 0);
        assert_ne!(index.root_page(), left, "a root split needs a new root");
        assert_eq!(index.tree_level(), 1);
        assert_eq!(chain_from(&engine, left), vec![left, right]);
        assert_all_keys(&index, n);
        index
            .validate()
            .unwrap_or_else(|e| panic!("recovered tree must validate: {e}"));

        // Second recovery: the CLR is in the post-checkpoint WAL tail and
        // replays as a no-op; the scan finds nothing flagged.
        std::mem::forget(engine);
        let (engine, index) = recover(tmp.path(), &config, meta_page);
        assert_eq!(index.tree_level(), 1);
        assert_all_keys(&index, n);
        index.validate().unwrap();
        drop(engine);
    });
}

/// Review H3, Copy variant: Prepare AND Copy both predate the checkpoint,
/// so the checkpoint flushes the post-copy images (left truncated, right
/// holding the moved half). The scan-driven undo must finish the split
/// WITHOUT moving anything (the right page is non-empty) and take the
/// separator from the right page's first entry.
#[test]
fn test_undo_scan_split_copy_before_checkpoint() {
    with_watchdog(|| {
        let tmp = TempDir::new().unwrap();
        let config = StorageConfig::new(tmp.path());

        let (meta_page, n, left, right, copy_start_slot) = {
            let (engine, index, n) = create_and_fill(tmp.path(), &config);
            let left = index.root_page();
            let st = index.split_prepare(left).unwrap();
            index.split_copy(&st).unwrap();
            engine.wal_writer().flush().unwrap();
            engine.trigger_checkpoint().unwrap();
            std::mem::forget(engine); // kill -9 before Commit
            (index.meta_page(), n, st.left, st.right, st.copy_start_slot)
        };

        let (engine, index) = recover(tmp.path(), &config, meta_page);
        let (left_flags, left_slots, _) = page_state(&engine, left);
        assert_eq!(left_flags & BTREE_FLAG_SPLIT_INCOMPLETE, 0);
        assert_eq!(left_slots, copy_start_slot as usize);
        let (_, right_slots, _) = page_state(&engine, right);
        assert_eq!(
            right_slots,
            n as usize - copy_start_slot as usize,
            "the moved entries must appear on the right page exactly once"
        );
        assert_eq!(index.tree_level(), 1);
        assert_eq!(chain_from(&engine, left), vec![left, right]);
        assert_all_keys(&index, n);
        index
            .validate()
            .unwrap_or_else(|e| panic!("recovered tree must validate: {e}"));

        std::mem::forget(engine);
        let (engine, index) = recover(tmp.path(), &config, meta_page);
        assert_eq!(index.tree_level(), 1);
        assert_all_keys(&index, n);
        index.validate().unwrap();
        drop(engine);
    });
}
