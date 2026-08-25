//! B+Tree redo handlers (tech-selection §13.3, §11.6).
//!
//! Five stateless handlers replay the records produced by
//! [`crate::index::BTreeIndex`]: [`BTreeInsertHandler`],
//! [`BTreeDeleteHandler`] and the three split handlers. They follow the heap
//! redo style: no owned state (the buffer pool arrives via [`RedoContext`]),
//! idempotent under the authoritative page LSN, and every inconsistency is a
//! hard failure (`StorageError::MetadataCorrupted`), never a silent skip of
//! something the record says must exist.
//!
//! # Idempotency anchors
//!
//! - `BTreeInsert` / `BTreeDelete`: `page.pd_lsn >= record.lsn` skips.
//! - `BTreeSplitPrepare`: both pages guarded independently by
//!   `pd_lsn < record.lsn`. The right page is **fully re-initialized** (any
//!   previous tenant's bytes on a recycled page are overwritten — every such
//!   earlier write has a lower LSN than the Prepare record). The left page's
//!   pre-Prepare `btpo_next` cannot be re-read from the left page once its
//!   post-Prepare image is durable, so it travels in the payload as
//!   `left_old_next`.
//! - `BTreeSplitCopy`: applies while `left_page.pd_lsn == left_page_pre_lsn`
//!   (§13.3 P2-9). The moved entries are **recomputed** from the left
//!   page's pre-copy image; the payload stays O(20 bytes). The online path
//!   flushes the right page's post-copy image before releasing the left
//!   page's latch, so "left truncated and durable, right not" cannot occur
//!   on the online path. When the anchor does not match (only reachable via
//!   crash-interleaved checkpoint flushes), the handler branches on how far
//!   the two pages are past the copy (Stage N review, P1-1):
//!   `left_lsn >= record.lsn` means the left page is already past the copy —
//!   the right page is then either also past it (skip; true idempotency) or
//!   missing it (hard-fail: with correct flush ordering the right page is
//!   always durable before the left, so "left durable and right not" is
//!   unreachable; reaching it means the moved entries are lost). A left page
//!   BEHIND the copy is rebuilt from its current content
//!   when the right page already holds the copy (sound: content stamped
//!   below the copy LSN cannot include post-copy inserts); a left page
//!   behind the copy with the right page ALSO missing it is genuine
//!   corruption and hard-fails.
//! - `BTreeSplitCommit`: the parent downlink is inserted only while
//!   `parent_page.pd_lsn < record.lsn` (the downlink is logged by exactly
//!   one record — the Commit — so this guard is precise); the left page's
//!   `SPLIT_INCOMPLETE`/`ROOT` clear is guarded the same way.
//!
//! Freshly allocated pages materialize as zeros during replay (their
//! `PageAlloc` record runs first). `BTreeInsert` redo initializes such a
//! page from the record's `level`/`flags` fields — this is how a new root
//! created by a root split recovers without any separate init record.

use pg_am_heap::slotted_page::SlottedPage;
use pg_storage::buffer_pool::BufferPool;
use pg_storage::error::{Result, StorageError};
use pg_storage::page::{page_pd_lsn, set_page_pd_lsn};
use pg_storage::recovery::{RedoContext, RedoHandler};
use pg_storage::types::{Lsn, PAGE_SIZE};
use pg_storage::wal::record::{
    BTreeDeleteRecord, BTreeInsertRecord, BTreeSplitCLRRecord, BTreeSplitCommitRecord,
    BTreeSplitCopyRecord, BTreeSplitPrepareRecord, WalRecord,
};
use pg_storage::wal::WalRecordType;

use crate::error::BTreeError;
use crate::index::{apply_split_clr, apply_split_copy};
use crate::page::{self, BtreePage};

/// The B+Tree redo handlers, ready for injection into the recovery registry
/// before a crash-recovery replay (see `Engine::open_with_redo_handlers`).
pub fn btree_redo_handlers() -> Vec<Box<dyn RedoHandler>> {
    vec![
        Box::new(BTreeInsertHandler),
        Box::new(BTreeDeleteHandler),
        Box::new(BTreeSplitPrepareHandler),
        Box::new(BTreeSplitCopyHandler),
        Box::new(BTreeSplitCommitHandler),
        Box::new(BTreeSplitClrRedoHandler),
    ]
}

/// Redo handler for `BTreeInsert`: leaf/internal entry inserts, new-root
/// seeds, and meta-page root records.
pub struct BTreeInsertHandler;

impl RedoHandler for BTreeInsertHandler {
    fn kind(&self) -> WalRecordType {
        WalRecordType::BTreeInsert
    }

    fn apply(&self, record: &WalRecord, ctx: &mut RedoContext<'_>) -> Result<()> {
        let rec = BTreeInsertRecord::decode(&record.payload)?;
        let pool = require_pool(ctx)?;
        let mut guard = pool.pin_mut(rec.page_id)?;
        let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().expect("frame is PAGE_SIZE");

        if page_pd_lsn(page) >= record.lsn {
            return Ok(());
        }
        // A zero page is fresh (materialized by PageAlloc during replay);
        // initialize it with the level/flags the record carries.
        BtreePage::init_if_fresh(page, rec.level, rec.flags);
        BtreePage::insert_entry_at(page, rec.slot_id, &rec.tuple_bytes)
            .map_err(btree_to_storage)?;
        stamp_pd_lsn(page, record.lsn);
        Ok(())
    }
}

/// Redo handler for `BTreeDelete`: physical removal of one index entry.
pub struct BTreeDeleteHandler;

impl RedoHandler for BTreeDeleteHandler {
    fn kind(&self) -> WalRecordType {
        WalRecordType::BTreeDelete
    }

    fn apply(&self, record: &WalRecord, ctx: &mut RedoContext<'_>) -> Result<()> {
        let rec = BTreeDeleteRecord::decode(&record.payload)?;
        let pool = require_pool(ctx)?;
        let mut guard = pool.pin_mut(rec.page_id)?;
        let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().expect("frame is PAGE_SIZE");

        if page_pd_lsn(page) >= record.lsn {
            return Ok(());
        }
        BtreePage::remove_entry_at(page, rec.slot_id).map_err(btree_to_storage)?;
        stamp_pd_lsn(page, record.lsn);
        Ok(())
    }
}

/// Redo handler for `BTreeSplitPrepare` (§13.3 step 1).
pub struct BTreeSplitPrepareHandler;

impl RedoHandler for BTreeSplitPrepareHandler {
    fn kind(&self) -> WalRecordType {
        WalRecordType::BTreeSplitPrepare
    }

    fn apply(&self, record: &WalRecord, ctx: &mut RedoContext<'_>) -> Result<()> {
        let rec = BTreeSplitPrepareRecord::decode(&record.payload)?;
        let pool = require_pool(ctx)?;
        ctx.incomplete_splits.mark_prepare(
            rec.left_page,
            rec.new_right_page,
            rec.level,
            rec.left_old_next,
            // B8: carried into the CLR's diagnostic redo_ref_lsn at undo time.
            record.lsn,
        );

        // Right page: full re-initialization (see the module docs).
        {
            let mut guard = pool.pin_mut(rec.new_right_page)?;
            let page: &mut [u8; PAGE_SIZE] =
                guard.page_mut().try_into().expect("frame is PAGE_SIZE");
            let rpd = page_pd_lsn(page);
            if rpd < record.lsn {
                BtreePage::init_right_page(page, rec.left_page, rec.left_old_next, rec.level);
                stamp_pd_lsn(page, record.lsn);
            }
        }

        // Left page: mark split-incomplete and link to the new right page.
        {
            let mut guard = pool.pin_mut(rec.left_page)?;
            let page: &mut [u8; PAGE_SIZE] =
                guard.page_mut().try_into().expect("frame is PAGE_SIZE");
            if page_pd_lsn(page) < record.lsn {
                BtreePage::init_if_fresh(page, rec.level, BtreePage::flags_for_level(rec.level));
                // §13.3: high_key_bytes is a redo validation marker — the
                // left page's pre-split maximum key must match it.
                let count = SlottedPage::slot_count(page) as u16;
                if count > 0 {
                    let bytes = SlottedPage::tuple(page, count - 1)
                        .map_err(BTreeError::Heap)
                        .map_err(btree_to_storage)?
                        .ok_or_else(|| {
                            StorageError::MetadataCorrupted(format!(
                                "split prepare redo: left page {} last slot unreadable",
                                rec.left_page
                            ))
                        })?;
                    let key = if rec.level == 0 {
                        page::decode_leaf_entry(bytes).map_err(btree_to_storage)?.0
                    } else {
                        page::decode_internal_entry(bytes)
                            .map_err(btree_to_storage)?
                            .0
                    };
                    if key != rec.high_key_bytes.as_slice() {
                        return Err(StorageError::MetadataCorrupted(format!(
                            "split prepare redo: left page {} high key diverged from record",
                            rec.left_page
                        )));
                    }
                }
                BtreePage::apply_prepare_left(page, rec.new_right_page)
                    .map_err(btree_to_storage)?;
                stamp_pd_lsn(page, record.lsn);
            }
        }
        Ok(())
    }
}

/// Redo handler for `BTreeSplitCopy` (§13.3 step 2, minimal payload).
pub struct BTreeSplitCopyHandler;

impl RedoHandler for BTreeSplitCopyHandler {
    fn kind(&self) -> WalRecordType {
        WalRecordType::BTreeSplitCopy
    }

    fn apply(&self, record: &WalRecord, ctx: &mut RedoContext<'_>) -> Result<()> {
        let rec = BTreeSplitCopyRecord::decode(&record.payload)?;
        let pool = require_pool(ctx)?;
        ctx.incomplete_splits
            .mark_copy(rec.left_page, rec.copy_start_slot);

        let mut left_guard = pool.pin_mut(rec.left_page)?;
        let left_lsn = page_pd_lsn(left_guard.page());
        if left_lsn != rec.left_page_pre_lsn {
            // The left page is NOT the pre-copy image the anchor expects.
            // Which recovery applies depends on how far the two pages are
            // past the copy (Stage N review, P1-1):
            let mut right_guard = pool.pin_mut(rec.right_page)?;
            let right_lsn = page_pd_lsn(right_guard.page());
            if left_lsn >= record.lsn {
                // The left page is already PAST the copy: its content is
                // final (truncated by the copy, possibly amended by later
                // inserts), so it must never be rebuilt or re-stamped here.
                if right_lsn >= record.lsn {
                    // Both pages are past the copy: fully applied already —
                    // skip. This is the true idempotency case (replaying
                    // the same record twice, or both post-copy images made
                    // durable before the crash).
                    return Ok(());
                }
                // Left past the copy, right's POOLED image still lacking
                // it. As a DISK state this is impossible — the online
                // flush discipline (split_copy flushes the right page
                // before the left page's latch is released; the left page
                // is eviction-proof until then) makes the right page's
                // post-copy image durable before the left's can be. But
                // mid-replay the right page's pooled image can lag its
                // on-disk image: an old full-page image from the page's
                // PREVIOUS identity (freelist recycling — M3 vacuum is the
                // first freelist producer) replays unconditionally and
                // regresses the page, and forward replay has rebuilt it
                // only up to this record. The moved entries are gone from
                // the pooled left page (truncated), so adopt the right
                // page's durable on-disk image — post-copy by the
                // discipline — instead of rebuilding anything.
                // (M3 Stage D churn finding; regression test:
                // `split_copy_redo_right_page_regressed_by_stale_fpi`.)
                drop(right_guard);
                let disk_lsn = pool.force_reload_from_disk(rec.right_page)?;
                if disk_lsn >= record.lsn {
                    return Ok(());
                }
                // The right page's DISK image also lacks the copy: the
                // flush discipline itself was violated — the moved entries
                // exist nowhere durable. Genuine corruption; hard-fail
                // loudly, never a silent skip (§11.6, v2.3-24).
                return Err(StorageError::MetadataCorrupted(format!(
                    "split copy redo: left page {} is past the copy (pd_lsn {:?} >= {:?}) \
                     and right page {} lacks it even on disk (pd_lsn {:?}); \
                     the moved entries are lost",
                    rec.left_page, left_lsn, record.lsn, rec.right_page, disk_lsn
                )));
            }
            if right_lsn >= record.lsn {
                // Left is neither the anchor image nor past the copy, but
                // the right page already holds the moved entries: only the
                // left page's post-copy rebuild is missing (its truncation
                // never reached disk, or an earlier replay was interrupted).
                // Rebuild the left page from its current content — sound
                // because left_lsn < copy lsn means the content cannot
                // include post-copy inserts (those are stamped at or beyond
                // the copy). If the copy was already fully applied this
                // rebuild is a deterministic no-op re-pack.
                let left_page: &mut [u8; PAGE_SIZE] = left_guard
                    .page_mut()
                    .try_into()
                    .expect("frame is PAGE_SIZE");
                let right_page: &mut [u8; PAGE_SIZE] = right_guard
                    .page_mut()
                    .try_into()
                    .expect("frame is PAGE_SIZE");
                apply_split_copy(left_page, right_page, rec.copy_start_slot, false)
                    .map_err(btree_to_storage)?;
                stamp_pd_lsn(left_page, record.lsn);
                return Ok(());
            }
            // The left page sits BETWEEN the anchor and the copy record (or
            // before it) AND the right page lacks the copy: the WAL stream
            // and the pages disagree. Hard-fail loudly, never a silent skip
            // (§11.6, v2.3-24).
            return Err(StorageError::MetadataCorrupted(format!(
                "split copy redo: left page {} is not the pre-copy image (pd_lsn {:?} != \
                 anchor {:?}) and is not past the copy either (record lsn {:?}); right page {} \
                 lacks the copy (pd_lsn {:?})",
                rec.left_page,
                left_lsn,
                rec.left_page_pre_lsn,
                record.lsn,
                rec.right_page,
                right_lsn
            )));
        }

        // Recompute the moved entries from the left page's pre-copy image.
        let mut right_guard = pool.pin_mut(rec.right_page)?;
        let right_lsn = page_pd_lsn(right_guard.page());
        {
            let left_page: &mut [u8; PAGE_SIZE] = left_guard
                .page_mut()
                .try_into()
                .expect("frame is PAGE_SIZE");
            let right_page: &mut [u8; PAGE_SIZE] = right_guard
                .page_mut()
                .try_into()
                .expect("frame is PAGE_SIZE");
            // When the right page's post-copy image is already durable it
            // holds the moved entries; only the left page's rebuild is
            // missing then. Both branches rebuild the left page, so redo
            // always converges to the online path's byte-identical result.
            let move_to_right = right_lsn < record.lsn;
            apply_split_copy(left_page, right_page, rec.copy_start_slot, move_to_right)
                .map_err(btree_to_storage)?;
            if move_to_right {
                stamp_pd_lsn(right_page, record.lsn);
            }
            stamp_pd_lsn(left_page, record.lsn);
        }
        // Flush the right page before the left guard is released: the online
        // path maintains this invariant (index.rs split_copy), and without it
        // a crash between the left page flush and the right page flush would
        // leave the moved entries durable nowhere — Copy is a minimal payload
        // that cannot recompute them from scratch.
        drop(right_guard);
        pool.flush(rec.right_page)?;
        Ok(())
    }
}

/// Redo handler for `BTreeSplitCommit` (§13.3 step 3).
pub struct BTreeSplitCommitHandler;

impl RedoHandler for BTreeSplitCommitHandler {
    fn kind(&self) -> WalRecordType {
        WalRecordType::BTreeSplitCommit
    }

    fn apply(&self, record: &WalRecord, ctx: &mut RedoContext<'_>) -> Result<()> {
        let rec = BTreeSplitCommitRecord::decode(&record.payload)?;
        let pool = require_pool(ctx)?;
        ctx.incomplete_splits.clear(rec.left_page);

        // Parent: insert the downlink `(separator_key, right_page)`.
        {
            let mut guard = pool.pin_mut(rec.parent_page)?;
            let page: &mut [u8; PAGE_SIZE] =
                guard.page_mut().try_into().expect("frame is PAGE_SIZE");
            if page_pd_lsn(page) < record.lsn {
                if SlottedPage::header(page).pd_upper == 0 {
                    // The parent must have been initialized by an earlier
                    // record (a fresh root's seed `BTreeInsert`, or the
                    // parent's own split records) — replaying in LSN order
                    // guarantees it ran first.
                    return Err(StorageError::MetadataCorrupted(format!(
                        "split commit redo: parent page {} has no prior image",
                        rec.parent_page
                    )));
                }
                let entry = page::encode_internal_entry(&rec.separator_key, rec.right_page);
                BtreePage::insert_entry_at(page, rec.parent_insert_slot, &entry)
                    .map_err(btree_to_storage)?;
                stamp_pd_lsn(page, record.lsn);
            }
        }

        // Left: clear SPLIT_INCOMPLETE (and ROOT, for a root split).
        {
            let mut guard = pool.pin_mut(rec.left_page)?;
            let page: &mut [u8; PAGE_SIZE] =
                guard.page_mut().try_into().expect("frame is PAGE_SIZE");
            if page_pd_lsn(page) < record.lsn {
                if SlottedPage::header(page).pd_upper == 0 {
                    return Err(StorageError::MetadataCorrupted(format!(
                        "split commit redo: left page {} has no prior image",
                        rec.left_page
                    )));
                }
                BtreePage::apply_commit_left(page).map_err(btree_to_storage)?;
                stamp_pd_lsn(page, record.lsn);
            }
        }
        Ok(())
    }
}

/// Redo handler for `BTreeSplitCLR` (Stage S, §11.3): replays a compensation
/// log record emitted during undo to finish an incomplete B+Tree split. The
/// CLR combines the Copy, downlink insertion, and Commit-clear into one
/// record. It shares `apply_split_clr` with the undo path, so redo converges on
/// the same pages the undo pass produced.
pub struct BTreeSplitClrRedoHandler;

impl RedoHandler for BTreeSplitClrRedoHandler {
    fn kind(&self) -> WalRecordType {
        WalRecordType::BTreeSplitCLR
    }

    fn apply(&self, record: &WalRecord, ctx: &mut RedoContext<'_>) -> Result<()> {
        let rec = BTreeSplitCLRRecord::decode(&record.payload)?;
        let pool = require_pool(ctx)?;
        // A CLR means the split is finished — clear it from the tracker so
        // the undo handler does not re-finish it.
        ctx.incomplete_splits.clear(rec.left_page);
        apply_split_clr(pool, &rec, record.lsn).map_err(btree_to_storage)
    }
}

/// Recovery always opens the buffer pool before replay (Stage I reorder), so
/// a missing pool is a programming error rather than a recoverable
/// condition.
fn require_pool<'a>(ctx: &RedoContext<'a>) -> Result<&'a BufferPool> {
    ctx.buffer_pool.ok_or_else(|| {
        StorageError::InvalidOperation(
            "btree redo requires a buffer pool in RedoContext".to_string(),
        )
    })
}

/// Advance the page's authoritative `pd_lsn` to `max(lsn, current)`.
fn stamp_pd_lsn(page: &mut [u8; PAGE_SIZE], lsn: Lsn) {
    let new_lsn = lsn.max(page_pd_lsn(page));
    set_page_pd_lsn(page, new_lsn);
}

/// Map a B+Tree-layer error into a storage error for the redo dispatch. A
/// B+Tree failure during redo (a page that cannot hold a logged entry, a
/// diverged slot) indicates on-disk inconsistency, not a routine condition.
fn btree_to_storage(e: BTreeError) -> StorageError {
    match e {
        BTreeError::Storage(s) => s,
        other => StorageError::MetadataCorrupted(format!("btree redo: {other}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pg_am_heap::slotted_page::SlottedPage;
    use pg_storage::buffer_pool::BufferPool;
    use pg_storage::clog::NoOpClogAccessor;
    use pg_storage::config::StorageConfig;
    use pg_storage::engine::StorageEngine;
    use pg_storage::page::set_page_pd_lsn;
    use pg_storage::recovery::{ActiveXactTable, DirtyPageTable, IncompleteSplitTracker};
    use pg_storage::types::{PageId, Tid, TxnId};

    use crate::page::{encode_leaf_entry, BTREE_FLAG_LEAF};

    const N: u16 = 10;
    const COPY_START: u16 = 5;
    const PRE_LSN: Lsn = Lsn(1_000);
    const COPY_LSN: Lsn = Lsn(2_000);
    const L2: Lsn = Lsn(3_000);

    fn key(i: u16) -> Vec<u8> {
        crate::encode_i32(i as i32).to_vec()
    }

    fn tid(i: u16) -> Tid {
        Tid {
            page_id: PageId(42_000 + i as u64),
            slot_id: i,
        }
    }

    struct Harness {
        _tmp: tempfile::TempDir,
        engine: StorageEngine,
        clog: NoOpClogAccessor,
        att: ActiveXactTable,
        dpt: DirtyPageTable,
        incomplete_splits: IncompleteSplitTracker,
    }

    impl Harness {
        fn new() -> Self {
            let tmp = tempfile::TempDir::new().unwrap();
            let config = StorageConfig::new(tmp.path());
            let engine = StorageEngine::open(tmp.path(), &config).unwrap();
            Self {
                _tmp: tmp,
                engine,
                clog: NoOpClogAccessor,
                att: ActiveXactTable::new(),
                dpt: DirtyPageTable::new(),
                incomplete_splits: IncompleteSplitTracker::new(),
            }
        }

        fn pool(&self) -> &BufferPool {
            self.engine.buffer_pool()
        }

        fn ctx(&mut self) -> RedoContext<'_> {
            RedoContext {
                buffer_pool: Some(self.engine.buffer_pool()),
                page_allocator: self.engine.page_allocator(),
                clog: &self.clog,
                att: &mut self.att,
                dpt: &mut self.dpt,
                incomplete_splits: &mut self.incomplete_splits,
            }
        }
    }

    /// Build a leaf page holding keys `0..entry_count` with the given
    /// `pd_lsn` (the crafted "left" states).
    fn build_leaf(pool: &BufferPool, entry_count: u16, pd_lsn: Lsn) -> PageId {
        let mut guard = pool.new_page().unwrap();
        let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().unwrap();
        BtreePage::init(page, 0, BTREE_FLAG_LEAF);
        for i in 0..entry_count {
            SlottedPage::add_tuple(page, &encode_leaf_entry(&key(i), tid(i))).unwrap();
        }
        set_page_pd_lsn(page, pd_lsn);
        guard.page_id()
    }

    /// Build a right page in its Prepare-initialized state (header only, no
    /// entries) with the given `pd_lsn`.
    fn build_prepared_right(pool: &BufferPool, left: PageId, pd_lsn: Lsn) -> PageId {
        let mut guard = pool.new_page().unwrap();
        let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().unwrap();
        BtreePage::init_right_page(page, left, PageId::INVALID, 0);
        set_page_pd_lsn(page, pd_lsn);
        guard.page_id()
    }

    fn copy_record(left: PageId, right: PageId) -> WalRecord {
        let mut rec = WalRecord::btree_split_copy(left, right, COPY_START, PRE_LSN).unwrap();
        rec.lsn = COPY_LSN;
        rec.txn_id = TxnId::INVALID;
        rec
    }

    fn slot_keys(page: &[u8; PAGE_SIZE]) -> Vec<i32> {
        let count = SlottedPage::slot_count(page) as u16;
        (0..count)
            .map(|s| {
                let bytes = SlottedPage::tuple(page, s).unwrap().unwrap();
                let (k, _) = crate::page::decode_leaf_entry(bytes).unwrap();
                crate::decode_i32(k.try_into().unwrap())
            })
            .collect()
    }

    /// Anchor mismatch, left page already PAST the copy (pd_lsn = L2 >=
    /// record lsn), right page stuck at its Prepare image: with correct
    /// flush ordering this is unreachable (the right page is always flushed
    /// before the left guard is released), so redo hard-fails rather than
    /// reconstructing from a page whose moved entries are already gone.
    #[test]
    fn copy_redo_left_past_copy_right_missing_hard_fails() {
        let mut h = Harness::new();
        let left = build_leaf(h.pool(), N, L2);
        let right = build_prepared_right(h.pool(), left, PRE_LSN);

        let err = BTreeSplitCopyHandler
            .apply(&copy_record(left, right), &mut h.ctx())
            .unwrap_err();
        assert!(
            matches!(err, StorageError::MetadataCorrupted(_)),
            "expected MetadataCorrupted, got {err:?}"
        );
    }

    /// Anchor mismatch, BOTH pages already past the copy with the left page
    /// carrying post-copy inserts: the copy was fully applied before the crash
    /// — redo skips (true idempotency), and the left page's post-copy content
    /// must survive byte-for-byte. This is the regression guard for the Stage N
    /// both-past fix: an earlier version incorrectly rebuilt the left page,
    /// silently dropping entries inserted after the copy.
    #[test]
    fn copy_redo_both_pages_past_copy_skips() {
        let mut h = Harness::new();
        let left = build_leaf(h.pool(), COPY_START, L2); // truncated, final
                                                         // Add post-copy inserts to the left page: entries at slots >=
                                                         // COPY_START inserted after the copy was applied. They must
                                                         // survive the idempotent skip untouched — rebuilding the left page
                                                         // would silently drop them.
        {
            let mut guard = h.pool().pin_mut(left).unwrap();
            let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().unwrap();
            SlottedPage::add_tuple(page, &encode_leaf_entry(&key(50), tid(50))).unwrap();
            SlottedPage::add_tuple(page, &encode_leaf_entry(&key(51), tid(51))).unwrap();
        }
        let right = build_prepared_right(h.pool(), left, PRE_LSN);
        // Right already holds the moved half and is stamped past the copy.
        {
            let mut guard = h.pool().pin_mut(right).unwrap();
            let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().unwrap();
            for i in COPY_START..N {
                SlottedPage::add_tuple(page, &encode_leaf_entry(&key(i), tid(i))).unwrap();
            }
            set_page_pd_lsn(page, COPY_LSN);
        }

        BTreeSplitCopyHandler
            .apply(&copy_record(left, right), &mut h.ctx())
            .unwrap();

        // Left: truncated entries + post-copy inserts all survive.
        let guard = h.pool().pin(left).unwrap();
        let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
        assert_eq!(
            slot_keys(page),
            (0..COPY_START as i32).chain([50, 51]).collect::<Vec<_>>()
        );
        assert_eq!(page_pd_lsn(page), L2, "left page pd_lsn must be untouched");
        drop(guard);
        let guard = h.pool().pin(right).unwrap();
        let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
        assert_eq!(
            SlottedPage::slot_count(page),
            (N - COPY_START) as usize,
            "idempotent skip must not duplicate the moved entries"
        );
        assert_eq!(page_pd_lsn(page), COPY_LSN);
    }

    /// Anchor mismatch, left page in an intermediate state (pre_lsn <
    /// left_lsn < record lsn) with the right page missing the copy: the
    /// pages and the WAL disagree — hard failure, never a silent skip.
    #[test]
    fn copy_redo_left_intermediate_state_hard_fails() {
        let mut h = Harness::new();
        let left = build_leaf(h.pool(), N, Lsn(1_500));
        let right = build_prepared_right(h.pool(), left, PRE_LSN);

        let err = BTreeSplitCopyHandler
            .apply(&copy_record(left, right), &mut h.ctx())
            .unwrap_err();
        assert!(
            matches!(err, StorageError::MetadataCorrupted(_)),
            "expected MetadataCorrupted, got {err:?}"
        );
    }

    /// Anchor mismatch with the left page past the copy but holding NOTHING
    /// at or beyond copy_start_slot (already truncated) while the right
    /// page is empty: the moved entries exist nowhere — recovering an empty
    /// right page silently would lose them, so this hard-fails.
    #[test]
    fn copy_redo_left_past_copy_but_no_moved_entries_hard_fails() {
        let mut h = Harness::new();
        let left = build_leaf(h.pool(), COPY_START, L2); // truncated, nothing to move
        let right = build_prepared_right(h.pool(), left, PRE_LSN);

        let err = BTreeSplitCopyHandler
            .apply(&copy_record(left, right), &mut h.ctx())
            .unwrap_err();
        assert!(
            matches!(err, StorageError::MetadataCorrupted(_)),
            "expected MetadataCorrupted, got {err:?}"
        );
    }
}
