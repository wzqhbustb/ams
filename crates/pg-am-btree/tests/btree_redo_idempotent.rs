//! Stage M redo idempotency: replaying each B+Tree handler ten times yields
//! the same page state as replaying it once (recovery may re-run any prefix
//! of records after a crash *during* recovery, §11.6).

use pg_am_btree::page::{
    self, BtreePage, BTREE_FLAG_LEAF, BTREE_FLAG_ROOT, BTREE_FLAG_SPLIT_INCOMPLETE,
};
use pg_am_btree::redo::{
    BTreeDeleteHandler, BTreeInsertHandler, BTreeSplitCommitHandler, BTreeSplitCopyHandler,
    BTreeSplitPrepareHandler,
};

use pg_am_heap::slotted_page::SlottedPage;
use pg_storage::clog::NoOpClogAccessor;
use pg_storage::config::StorageConfig;
use pg_storage::engine::StorageEngine;
use pg_storage::page::page_pd_lsn;
use pg_storage::recovery::{
    ActiveXactTable, DirtyPageTable, IncompleteSplitTracker, RedoContext, RedoHandler,
};
use pg_storage::types::{Lsn, PageId, Tid, PAGE_SIZE};
use pg_storage::wal::record::WalRecord;

use tempfile::TempDir;

fn tid(i: u64) -> Tid {
    Tid {
        page_id: PageId(42_000 + i),
        slot_id: i as u16,
    }
}

fn key(i: u8) -> Vec<u8> {
    vec![i]
}

/// Apply `handler` to `record` ten times against the engine's buffer pool.
fn apply_ten(handler: &dyn RedoHandler, record: &WalRecord, engine: &StorageEngine) {
    let clog = NoOpClogAccessor;
    let mut att = ActiveXactTable::new();
    let mut dpt = DirtyPageTable::new();
    for _ in 0..10 {
        let mut incomplete_splits = IncompleteSplitTracker::new();
        let mut ctx = RedoContext {
            buffer_pool: Some(engine.buffer_pool()),
            page_allocator: engine.page_allocator(),
            clog: &clog,
            att: &mut att,
            dpt: &mut dpt,
            incomplete_splits: &mut incomplete_splits,
        };
        handler.apply(record, &mut ctx).unwrap();
    }
}

fn apply_once(handler: &dyn RedoHandler, record: &WalRecord, engine: &StorageEngine) {
    apply_n(handler, record, engine, 1);
}

fn apply_n(handler: &dyn RedoHandler, record: &WalRecord, engine: &StorageEngine, n: usize) {
    let clog = NoOpClogAccessor;
    let mut att = ActiveXactTable::new();
    let mut dpt = DirtyPageTable::new();
    for _ in 0..n {
        let mut incomplete_splits = IncompleteSplitTracker::new();
        let mut ctx = RedoContext {
            buffer_pool: Some(engine.buffer_pool()),
            page_allocator: engine.page_allocator(),
            clog: &clog,
            att: &mut att,
            dpt: &mut dpt,
            incomplete_splits: &mut incomplete_splits,
        };
        handler.apply(record, &mut ctx).unwrap();
    }
}

/// Byte snapshot of a page.
fn page_image(engine: &StorageEngine, page_id: PageId) -> Vec<u8> {
    let guard = engine.buffer_pool().pin(page_id).unwrap();
    guard.page().to_vec()
}

/// Initialize `page_id` as a leaf holding `keys` (entries `key -> tid(i)`),
/// with the given extra flags and `pd_lsn`.
fn seed_leaf(
    engine: &StorageEngine,
    page_id: PageId,
    keys: &[u8],
    flags: u8,
    next: PageId,
    pd_lsn: Lsn,
) {
    let mut guard = engine.buffer_pool().pin_mut(page_id).unwrap();
    let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().unwrap();
    BtreePage::init(page, 0, BTREE_FLAG_LEAF | flags);
    for (i, k) in keys.iter().enumerate() {
        let entry = page::encode_leaf_entry(&key(*k), tid(i as u64));
        BtreePage::insert_entry_at(page, i as u16, &entry).unwrap();
    }
    BtreePage::set_next(page, next);
    pg_storage::page::set_page_pd_lsn(page, pd_lsn);
}

#[test]
fn btree_insert_redo_is_idempotent() {
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());
    let engine = StorageEngine::open(tmp.path(), &config).unwrap();

    let page_id = engine.buffer_pool().new_page().unwrap().page_id();
    let entry = page::encode_leaf_entry(&key(7), tid(7));
    let mut record =
        WalRecord::btree_insert(page_id, 0, 0, BTREE_FLAG_LEAF, entry.clone()).unwrap();
    record.lsn = Lsn(1_000);

    apply_once(&BTreeInsertHandler, &record, &engine);
    let after_one = page_image(&engine, page_id);
    apply_n(&BTreeInsertHandler, &record, &engine, 9);

    assert_eq!(page_image(&engine, page_id), after_one);
    let guard = engine.buffer_pool().pin(page_id).unwrap();
    let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
    assert_eq!(page_pd_lsn(page), Lsn(1_000));
    assert_eq!(SlottedPage::slot_count(page), 1);
    assert_eq!(SlottedPage::tuple(page, 0).unwrap(), Some(entry.as_slice()));
    assert_eq!(BtreePage::flags(page).unwrap(), BTREE_FLAG_LEAF);
}

#[test]
fn btree_delete_redo_is_idempotent() {
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());
    let engine = StorageEngine::open(tmp.path(), &config).unwrap();

    let page_id = engine.buffer_pool().new_page().unwrap().page_id();
    seed_leaf(&engine, page_id, &[1, 2], 0, PageId::INVALID, Lsn(1_000));

    let mut record = WalRecord::btree_delete(page_id, 0).unwrap();
    record.lsn = Lsn(2_000);
    apply_once(&BTreeDeleteHandler, &record, &engine);
    let after_one = page_image(&engine, page_id);
    apply_n(&BTreeDeleteHandler, &record, &engine, 9);

    assert_eq!(page_image(&engine, page_id), after_one);
    let guard = engine.buffer_pool().pin(page_id).unwrap();
    let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
    assert_eq!(page_pd_lsn(page), Lsn(2_000));
    assert_eq!(SlottedPage::slot_count(page), 1);
    let (k, t) = page::decode_leaf_entry(SlottedPage::tuple(page, 0).unwrap().unwrap()).unwrap();
    assert_eq!(k, key(2).as_slice());
    assert_eq!(t, tid(1));
}

/// Build the post-Prepare state shared by the Prepare/Copy idempotency
/// tests: a 4-entry leaf `left` linked to an initialized empty `right`.
fn prepare_state(engine: &StorageEngine) -> (PageId, PageId) {
    let left = engine.buffer_pool().new_page().unwrap().page_id();
    let right = engine.buffer_pool().new_page().unwrap().page_id();
    seed_leaf(engine, left, &[1, 2, 3, 4], 0, PageId::INVALID, Lsn(1_000));
    (left, right)
}

#[test]
fn split_prepare_redo_is_idempotent() {
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());
    let engine = StorageEngine::open(tmp.path(), &config).unwrap();
    let (left, right) = prepare_state(&engine);

    let mut record =
        WalRecord::btree_split_prepare(left, right, 0, PageId::INVALID, key(4)).unwrap();
    record.lsn = Lsn(2_000);
    apply_once(&BTreeSplitPrepareHandler, &record, &engine);
    let left_after_one = page_image(&engine, left);
    let right_after_one = page_image(&engine, right);
    apply_n(&BTreeSplitPrepareHandler, &record, &engine, 9);

    assert_eq!(page_image(&engine, left), left_after_one);
    assert_eq!(page_image(&engine, right), right_after_one);

    let guard = engine.buffer_pool().pin(left).unwrap();
    let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
    assert_eq!(BtreePage::next(page).unwrap(), right);
    assert_eq!(
        BtreePage::flags(page).unwrap(),
        BTREE_FLAG_LEAF | BTREE_FLAG_SPLIT_INCOMPLETE
    );
    assert_eq!(page_pd_lsn(page), Lsn(2_000));
    drop(guard);

    let guard = engine.buffer_pool().pin(right).unwrap();
    let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
    assert_eq!(BtreePage::prev(page).unwrap(), left);
    assert_eq!(BtreePage::next(page).unwrap(), PageId::INVALID);
    assert_eq!(BtreePage::flags(page).unwrap(), BTREE_FLAG_LEAF);
    assert_eq!(SlottedPage::slot_count(page), 0);
    assert_eq!(page_pd_lsn(page), Lsn(2_000));
}

#[test]
fn split_copy_redo_is_idempotent() {
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());
    let engine = StorageEngine::open(tmp.path(), &config).unwrap();
    let (left, right) = prepare_state(&engine);

    // Drive the state to "Prepare applied" first (pd_lsn = 2000 on both).
    let mut prepare =
        WalRecord::btree_split_prepare(left, right, 0, PageId::INVALID, key(4)).unwrap();
    prepare.lsn = Lsn(2_000);
    apply_once(&BTreeSplitPrepareHandler, &prepare, &engine);

    let mut copy = WalRecord::btree_split_copy(left, right, 2, Lsn(2_000)).unwrap();
    copy.lsn = Lsn(3_000);
    apply_once(&BTreeSplitCopyHandler, &copy, &engine);
    let left_after_one = page_image(&engine, left);
    let right_after_one = page_image(&engine, right);
    apply_n(&BTreeSplitCopyHandler, &copy, &engine, 9);

    assert_eq!(page_image(&engine, left), left_after_one);
    assert_eq!(page_image(&engine, right), right_after_one);

    // Left keeps slots [0, 2), right received [2, 4).
    let guard = engine.buffer_pool().pin(left).unwrap();
    let lpage: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
    assert_eq!(SlottedPage::slot_count(lpage), 2);
    assert_eq!(page_pd_lsn(lpage), Lsn(3_000));
    drop(guard);
    let guard = engine.buffer_pool().pin(right).unwrap();
    let rpage: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
    assert_eq!(SlottedPage::slot_count(rpage), 2);
    assert_eq!(page_pd_lsn(rpage), Lsn(3_000));
    let (k0, _) = page::decode_leaf_entry(SlottedPage::tuple(rpage, 0).unwrap().unwrap()).unwrap();
    let (k1, _) = page::decode_leaf_entry(SlottedPage::tuple(rpage, 1).unwrap().unwrap()).unwrap();
    assert_eq!(k0, key(3).as_slice());
    assert_eq!(k1, key(4).as_slice());
}

#[test]
fn split_commit_redo_is_idempotent() {
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());
    let engine = StorageEngine::open(tmp.path(), &config).unwrap();
    let (left, right) = prepare_state(&engine);

    // Post-Copy state.
    let mut prepare =
        WalRecord::btree_split_prepare(left, right, 0, PageId::INVALID, key(4)).unwrap();
    prepare.lsn = Lsn(2_000);
    apply_once(&BTreeSplitPrepareHandler, &prepare, &engine);
    let mut copy = WalRecord::btree_split_copy(left, right, 2, Lsn(2_000)).unwrap();
    copy.lsn = Lsn(3_000);
    apply_once(&BTreeSplitCopyHandler, &copy, &engine);

    // A fresh root page seeded with (low_key -> left), as the online root
    // split does before the Commit.
    let parent = engine.buffer_pool().new_page().unwrap().page_id();
    {
        let mut guard = engine.buffer_pool().pin_mut(parent).unwrap();
        let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().unwrap();
        BtreePage::init(page, 1, BTREE_FLAG_ROOT);
    }
    let seed = page::encode_internal_entry(&key(1), left);
    let mut seed_rec = WalRecord::btree_insert(parent, 0, 1, BTREE_FLAG_ROOT, seed).unwrap();
    seed_rec.lsn = Lsn(4_000);
    apply_once(&BTreeInsertHandler, &seed_rec, &engine);

    let mut commit = WalRecord::btree_split_commit(left, right, parent, key(3), 1).unwrap();
    commit.lsn = Lsn(5_000);
    apply_once(&BTreeSplitCommitHandler, &commit, &engine);
    let parent_after_one = page_image(&engine, parent);
    let left_after_one = page_image(&engine, left);
    apply_n(&BTreeSplitCommitHandler, &commit, &engine, 9);

    assert_eq!(page_image(&engine, parent), parent_after_one);
    assert_eq!(page_image(&engine, left), left_after_one);

    let guard = engine.buffer_pool().pin(parent).unwrap();
    let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
    assert_eq!(SlottedPage::slot_count(page), 2);
    let (k0, c0) =
        page::decode_internal_entry(SlottedPage::tuple(page, 0).unwrap().unwrap()).unwrap();
    let (k1, c1) =
        page::decode_internal_entry(SlottedPage::tuple(page, 1).unwrap().unwrap()).unwrap();
    assert_eq!((k0, c0), (key(1).as_slice(), left));
    assert_eq!((k1, c1), (key(3).as_slice(), right));
    assert_eq!(page_pd_lsn(page), Lsn(5_000));
    drop(guard);

    let guard = engine.buffer_pool().pin(left).unwrap();
    let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
    assert_eq!(BtreePage::flags(page).unwrap(), BTREE_FLAG_LEAF);
    assert_eq!(page_pd_lsn(page), Lsn(5_000));
}

/// Replaying the whole Prepare→Copy→Commit sequence twice (crash during
/// recovery) must converge to the same final state.
#[test]
fn full_split_sequence_replayed_twice() {
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());
    let engine = StorageEngine::open(tmp.path(), &config).unwrap();
    let (left, right) = prepare_state(&engine);

    let mut prepare =
        WalRecord::btree_split_prepare(left, right, 0, PageId::INVALID, key(4)).unwrap();
    prepare.lsn = Lsn(2_000);
    let mut copy = WalRecord::btree_split_copy(left, right, 2, Lsn(2_000)).unwrap();
    copy.lsn = Lsn(3_000);
    let parent = engine.buffer_pool().new_page().unwrap().page_id();
    {
        let mut guard = engine.buffer_pool().pin_mut(parent).unwrap();
        let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().unwrap();
        BtreePage::init(page, 1, BTREE_FLAG_ROOT);
    }
    let seed = page::encode_internal_entry(&key(1), left);
    let mut seed_rec = WalRecord::btree_insert(parent, 0, 1, BTREE_FLAG_ROOT, seed).unwrap();
    seed_rec.lsn = Lsn(4_000);
    let mut commit = WalRecord::btree_split_commit(left, right, parent, key(3), 1).unwrap();
    commit.lsn = Lsn(5_000);

    let handlers: [&dyn RedoHandler; 4] = [
        &BTreeSplitPrepareHandler,
        &BTreeSplitCopyHandler,
        &BTreeInsertHandler,
        &BTreeSplitCommitHandler,
    ];
    let records = [&prepare, &copy, &seed_rec, &commit];

    for (handler, record) in handlers.iter().zip(records.iter()) {
        apply_once(*handler, record, &engine);
    }
    let images: Vec<Vec<u8>> = [left, right, parent]
        .iter()
        .map(|p| page_image(&engine, *p))
        .collect();

    // Second full replay (crash during recovery, replay from the top).
    for (handler, record) in handlers.iter().zip(records.iter()) {
        apply_once(*handler, record, &engine);
    }
    let images2: Vec<Vec<u8>> = [left, right, parent]
        .iter()
        .map(|p| page_image(&engine, *p))
        .collect();

    assert_eq!(images, images2, "replayed sequence must converge");
}

#[test]
fn split_prepare_redo_validates_high_key() {
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());
    let engine = StorageEngine::open(tmp.path(), &config).unwrap();
    let (left, right) = prepare_state(&engine);

    // The recorded high key (key(9)) does not match the left page's actual
    // maximum (key(4)): the WAL stream is inconsistent with the page.
    let mut record =
        WalRecord::btree_split_prepare(left, right, 0, PageId::INVALID, key(9)).unwrap();
    record.lsn = Lsn(2_000);
    let clog = NoOpClogAccessor;
    let mut att = ActiveXactTable::new();
    let mut dpt = DirtyPageTable::new();
    let mut incomplete_splits = IncompleteSplitTracker::new();
    let mut ctx = RedoContext {
        buffer_pool: Some(engine.buffer_pool()),
        page_allocator: engine.page_allocator(),
        clog: &clog,
        att: &mut att,
        dpt: &mut dpt,
        incomplete_splits: &mut incomplete_splits,
    };
    assert!(BTreeSplitPrepareHandler.apply(&record, &mut ctx).is_err());
}

#[test]
fn apply_ten_helper_is_used() {
    // Keep the helper referenced (it documents the "ten times" acceptance
    // phrasing); the per-handler tests above use apply_once + apply_n to
    // snapshot the state after the first apply.
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());
    let engine = StorageEngine::open(tmp.path(), &config).unwrap();
    let page_id = engine.buffer_pool().new_page().unwrap().page_id();
    let entry = page::encode_leaf_entry(&key(1), tid(1));
    let mut record = WalRecord::btree_insert(page_id, 0, 0, BTREE_FLAG_LEAF, entry).unwrap();
    record.lsn = Lsn(1_000);
    apply_ten(&BTreeInsertHandler, &record, &engine);
    let guard = engine.buffer_pool().pin(page_id).unwrap();
    let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
    assert_eq!(SlottedPage::slot_count(page), 1);
}

/// P2-2: the left page is NOT the pre-copy image (its pd_lsn is neither the
/// anchor nor the copy LSN — a state a torn base page or interrupted replay
/// can produce), but the right page already holds the copy. Redo must
/// rebuild the left page from its current content — not silently skip.
#[test]
fn split_copy_redo_rebuilds_left_when_right_already_has_copy() {
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());
    let engine = StorageEngine::open(tmp.path(), &config).unwrap();
    let left = engine.buffer_pool().new_page().unwrap().page_id();
    let right = engine.buffer_pool().new_page().unwrap().page_id();

    // Left: full pre-copy content, but stamped with an "intermediate" LSN
    // (2_500: neither the anchor 2_000 nor the record's 3_000).
    seed_leaf(
        &engine,
        left,
        &[1, 2, 3, 4],
        BTREE_FLAG_SPLIT_INCOMPLETE,
        right,
        Lsn(2_500),
    );
    // Right: already holds the copied upper half, stamped at the copy LSN.
    {
        let mut guard = engine.buffer_pool().pin_mut(right).unwrap();
        let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().unwrap();
        BtreePage::init_right_page(page, left, PageId::INVALID, 0);
        for (i, k) in [3u8, 4].iter().enumerate() {
            let entry = page::encode_leaf_entry(&key(*k), tid(2 + i as u64));
            SlottedPage::add_tuple(page, &entry).unwrap();
        }
        pg_storage::page::set_page_pd_lsn(page, Lsn(3_000));
    }

    let mut copy = WalRecord::btree_split_copy(left, right, 2, Lsn(2_000)).unwrap();
    copy.lsn = Lsn(3_000);
    apply_once(&BTreeSplitCopyHandler, &copy, &engine);

    // The left page is rebuilt to the kept half and stamped at the record.
    let guard = engine.buffer_pool().pin(left).unwrap();
    let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
    assert_eq!(page_pd_lsn(page), Lsn(3_000));
    assert_eq!(SlottedPage::slot_count(page), 2);
    let (k0, _) = page::decode_leaf_entry(SlottedPage::tuple(page, 0).unwrap().unwrap()).unwrap();
    let (k1, _) = page::decode_leaf_entry(SlottedPage::tuple(page, 1).unwrap().unwrap()).unwrap();
    assert_eq!(k0, key(1).as_slice());
    assert_eq!(k1, key(2).as_slice());
    drop(guard);

    // The right page is untouched.
    let guard = engine.buffer_pool().pin(right).unwrap();
    let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
    assert_eq!(SlottedPage::slot_count(page), 2);
    assert_eq!(page_pd_lsn(page), Lsn(3_000));
}

/// P2-2: neither page matches the record (left is not the pre-copy image,
/// right does not hold the copy) — the WAL stream and the pages disagree,
/// so redo must hard-fail instead of silently skipping.
#[test]
fn split_copy_redo_hard_fails_when_neither_side_has_copy() {
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());
    let engine = StorageEngine::open(tmp.path(), &config).unwrap();
    let left = engine.buffer_pool().new_page().unwrap().page_id();
    let right = engine.buffer_pool().new_page().unwrap().page_id();

    seed_leaf(
        &engine,
        left,
        &[1, 2, 3, 4],
        BTREE_FLAG_SPLIT_INCOMPLETE,
        right,
        Lsn(2_500),
    );
    // Right: initialized by Prepare but EMPTY (no copy), behind the record.
    {
        let mut guard = engine.buffer_pool().pin_mut(right).unwrap();
        let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().unwrap();
        BtreePage::init_right_page(page, left, PageId::INVALID, 0);
        pg_storage::page::set_page_pd_lsn(page, Lsn(2_000));
    }

    let mut copy = WalRecord::btree_split_copy(left, right, 2, Lsn(2_000)).unwrap();
    copy.lsn = Lsn(3_000);
    let clog = NoOpClogAccessor;
    let mut att = ActiveXactTable::new();
    let mut dpt = DirtyPageTable::new();
    let mut incomplete_splits = IncompleteSplitTracker::new();
    let mut ctx = RedoContext {
        buffer_pool: Some(engine.buffer_pool()),
        page_allocator: engine.page_allocator(),
        clog: &clog,
        att: &mut att,
        dpt: &mut dpt,
        incomplete_splits: &mut incomplete_splits,
    };
    assert!(BTreeSplitCopyHandler.apply(&copy, &mut ctx).is_err());
}
