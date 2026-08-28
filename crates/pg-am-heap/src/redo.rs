//! Heap redo handlers (M2a Stage I; HeapCleanup added in M3 Stage B).
//!
//! Five handlers replay the heap WAL records produced by [`crate::heap_am`]
//! and by vacuum's compaction (M3): [`HeapInsertHandler`],
//! [`HeapUpdateHandler`], [`HeapDeleteHandler`], [`HeapHotUpdateHandler`],
//! [`HeapCleanupRedoHandler`]. They are
//! stateless — the buffer pool and page allocator arrive via [`RedoContext`] —
//! so [`crate::heap_am::HeapAM::redo_handlers`] can hand fresh boxes to the
//! recovery registry (which `pg-storage` cannot construct itself, as it must
//! not depend on this crate).
//!
//! # Idempotency (tech-selection §11.6)
//!
//! Recovery replays from the checkpoint redo point and may re-run any prefix of
//! records after a crash *during* recovery. Each handler is therefore guarded by
//! the authoritative page LSN: if `page_pd_lsn(page) >= record.lsn`, the change
//! is already durable and the handler is a no-op. Otherwise it applies the
//! change and stamps `pd_lsn = max(record.lsn, pd_lsn)`. Slot assignment is
//! explicit (M3 §4.6): the WAL record carries the slot chosen online, and redo
//! places the tuple with [`SlottedPage::add_tuple_at`] at exactly that slot —
//! no dependence on the online writer's first-fit choice being reproducible
//! against a page vacuum's `compact()` may have left with `Unused` slots.
//!
//! Freshly allocated heap pages may be materialized as all-zero bytes when the
//! data file is extended during replay (the `PageAlloc` record, which has a
//! lower LSN, runs first). [`SlottedPage::init_if_fresh_with_special`]
//! initializes such a page — with the Stage K [`HEAP_SPECIAL_SIZE`] special
//! space, so a recovered page is indistinguishable from one initialized on the
//! forward path — before the first tuple is placed.
//!
//! # Page-chain recovery (Stage K)
//!
//! The handlers never walk or repair the relation page chain: they locate
//! pages directly by `record.page_id`. Chain links do not need redo support
//! here because the forward path logs a post-image `FullPageImage` of the old
//! tail page when it links a new page (see [`crate::heap_am`] "Durability of
//! chain links"); the storage engine's default `FullPageImageRedoHandler`
//! restores those links unconditionally during the same replay, so the chain
//! `HeapAM::seed_from_chain` walks after open is complete.

use crate::error::HeapError;
use crate::slotted_page::{SlottedPage, HEAP_SPECIAL_SIZE};
use crate::tuple::{
    TupleHeader, HEAP_HOT_UPDATED, HEAP_UPDATED, HEAP_XMAX_IS_SHARE, HEAP_XMAX_LOCK_ONLY,
    TUPLE_HEADER_SIZE,
};
use pg_storage::buffer_pool::BufferPool;
use pg_storage::error::{Result, StorageError};
use pg_storage::page::{page_pd_lsn, set_page_pd_lsn};
use pg_storage::recovery::{RedoContext, RedoHandler};
use pg_storage::types::{Lsn, PageId, Tid, PAGE_SIZE};
use pg_storage::wal::record::{
    HeapCleanupRecord, HeapDeleteRecord, HeapHotUpdateRecord, HeapInsertRecord, HeapUpdateRecord,
    WalRecord,
};
use pg_storage::wal::WalRecordType;

/// The heap redo handlers, ready for injection into the recovery registry
/// before a crash-recovery replay (see `Engine::open_with_redo_handlers`).
///
/// `pg-storage` owns the registry but cannot depend on this crate, so the
/// caller opening the engine must pass these in.
///
/// Every heap record type with a producer MUST have a handler here (Stage 0
/// hard-fail convention: recovery errors out on an unregistered type) —
/// record and handler ship in the same stage (§4.5).
pub fn heap_redo_handlers() -> Vec<Box<dyn RedoHandler>> {
    vec![
        Box::new(HeapInsertHandler),
        Box::new(HeapUpdateHandler),
        Box::new(HeapDeleteHandler),
        Box::new(HeapHotUpdateHandler),
        Box::new(HeapCleanupRedoHandler),
    ]
}

/// Redo handler for `HeapInsert` records.
pub struct HeapInsertHandler;

impl RedoHandler for HeapInsertHandler {
    fn kind(&self) -> WalRecordType {
        WalRecordType::HeapInsert
    }

    fn apply(&self, record: &WalRecord, ctx: &mut RedoContext<'_>) -> Result<()> {
        let rec = HeapInsertRecord::decode(&record.payload)?;
        let pool = require_pool(ctx)?;
        let mut guard = pool.pin_mut(rec.page_id)?;
        let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().expect("frame is PAGE_SIZE");

        if page_pd_lsn(page) >= record.lsn {
            return Ok(());
        }
        SlottedPage::init_if_fresh_with_special(page, HEAP_SPECIAL_SIZE);
        // §4.6: place the tuple at the slot the record carries. A slot the
        // page cannot honor (out of range, or still occupied) means the page
        // on disk is inconsistent with the WAL stream (e.g. a torn base
        // page) — hard-fail rather than silently writing the tuple elsewhere
        // (§11.6: redo never skips silently).
        SlottedPage::add_tuple_at(page, rec.slot_id, &rec.tuple_bytes).map_err(heap_to_storage)?;
        stamp_pd_lsn(page, record.lsn);
        Ok(())
    }
}

/// Redo handler for `HeapUpdate` records (stamp old version + insert new one).
pub struct HeapUpdateHandler;

impl RedoHandler for HeapUpdateHandler {
    fn kind(&self) -> WalRecordType {
        WalRecordType::HeapUpdate
    }

    fn apply(&self, record: &WalRecord, ctx: &mut RedoContext<'_>) -> Result<()> {
        let rec = HeapUpdateRecord::decode(&record.payload)?;
        let pool = require_pool(ctx)?;

        // Same-page update (the fast path in `HeapAM::update`): both the old
        // version's t_xmax stamp and the new version's insert are logged under a
        // single LSN on one page. They MUST be applied under one pd_lsn guard —
        // stamping the old tuple first advances pd_lsn to record.lsn, so a
        // second, independently guarded block would (wrongly) skip the insert
        // and lose the new version on recovery.
        if rec.old_tid.page_id == rec.new_tid.page_id {
            let mut guard = pool.pin_mut(rec.old_tid.page_id)?;
            let page: &mut [u8; PAGE_SIZE] =
                guard.page_mut().try_into().expect("frame is PAGE_SIZE");
            if page_pd_lsn(page) < record.lsn {
                SlottedPage::init_if_fresh_with_special(page, HEAP_SPECIAL_SIZE);
                stamp_deleted(page, rec.old_tid, rec.xmax_old, true).map_err(heap_to_storage)?;
                // §4.6: the new version goes to the slot the record carries.
                SlottedPage::add_tuple_at(page, rec.new_tid.slot_id, &rec.new_tuple_bytes)
                    .map_err(heap_to_storage)?;
                stamp_pd_lsn(page, record.lsn);
            }
            return Ok(());
        }

        // Cross-page update: the old and new versions live on different pages
        // that may have reached disk at different times before the crash, so
        // each is guarded independently.
        //
        // Old page: stamp t_xmax + HEAP_UPDATED.
        {
            let mut guard = pool.pin_mut(rec.old_tid.page_id)?;
            let page: &mut [u8; PAGE_SIZE] =
                guard.page_mut().try_into().expect("frame is PAGE_SIZE");
            if page_pd_lsn(page) < record.lsn {
                SlottedPage::init_if_fresh_with_special(page, HEAP_SPECIAL_SIZE);
                stamp_deleted(page, rec.old_tid, rec.xmax_old, true).map_err(heap_to_storage)?;
                stamp_pd_lsn(page, record.lsn);
            }
        }

        // New page: append the new version at the recorded slot (§4.6).
        {
            let mut guard = pool.pin_mut(rec.new_tid.page_id)?;
            let page: &mut [u8; PAGE_SIZE] =
                guard.page_mut().try_into().expect("frame is PAGE_SIZE");
            if page_pd_lsn(page) < record.lsn {
                SlottedPage::init_if_fresh_with_special(page, HEAP_SPECIAL_SIZE);
                SlottedPage::add_tuple_at(page, rec.new_tid.slot_id, &rec.new_tuple_bytes)
                    .map_err(heap_to_storage)?;
                stamp_pd_lsn(page, record.lsn);
            }
        }
        Ok(())
    }
}

/// Redo handler for `HeapDelete` records (logical delete: stamp t_xmax).
pub struct HeapDeleteHandler;

impl RedoHandler for HeapDeleteHandler {
    fn kind(&self) -> WalRecordType {
        WalRecordType::HeapDelete
    }

    fn apply(&self, record: &WalRecord, ctx: &mut RedoContext<'_>) -> Result<()> {
        let rec = HeapDeleteRecord::decode(&record.payload)?;
        let pool = require_pool(ctx)?;
        let mut guard = pool.pin_mut(rec.tid.page_id)?;
        let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().expect("frame is PAGE_SIZE");

        if page_pd_lsn(page) >= record.lsn {
            return Ok(());
        }
        stamp_deleted(page, rec.tid, rec.xmax, false).map_err(heap_to_storage)?;
        stamp_pd_lsn(page, record.lsn);
        Ok(())
    }
}

/// Redo handler for `HeapHotUpdate` records (same-page HOT update: stamp
/// old version with t_ctid + HOT flags, insert new version).
pub struct HeapHotUpdateHandler;

impl RedoHandler for HeapHotUpdateHandler {
    fn kind(&self) -> WalRecordType {
        WalRecordType::HeapHotUpdate
    }

    fn apply(&self, record: &WalRecord, ctx: &mut RedoContext<'_>) -> Result<()> {
        let rec = HeapHotUpdateRecord::decode(&record.payload)?;
        let pool = require_pool(ctx)?;
        let mut guard = pool.pin_mut(rec.page_id)?;
        let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().expect("frame is PAGE_SIZE");

        if page_pd_lsn(page) >= record.lsn {
            return Ok(());
        }
        SlottedPage::init_if_fresh_with_special(page, HEAP_SPECIAL_SIZE);
        let old_tid = Tid {
            page_id: rec.page_id,
            slot_id: rec.old_slot,
        };
        let new_tid = Tid {
            page_id: rec.page_id,
            slot_id: rec.new_slot,
        };
        stamp_hot_update(page, old_tid, new_tid, rec.xmax).map_err(heap_to_storage)?;
        // §4.6: the new HEAP_ONLY version goes to the recorded slot.
        SlottedPage::add_tuple_at(page, rec.new_slot, &rec.new_tuple_bytes)
            .map_err(heap_to_storage)?;
        stamp_pd_lsn(page, record.lsn);
        Ok(())
    }
}

/// Redo handler for `HeapCleanup` records (M3 Stage B, tech-selection §4.5):
/// replay vacuum's page compaction — and, when the record carries one, the
/// page-chain unlink — by re-executing the SAME physical operation the
/// online path ran.
pub struct HeapCleanupRedoHandler;

impl RedoHandler for HeapCleanupRedoHandler {
    fn kind(&self) -> WalRecordType {
        WalRecordType::HeapCleanup
    }

    fn apply(&self, record: &WalRecord, ctx: &mut RedoContext<'_>) -> Result<()> {
        let rec = HeapCleanupRecord::decode(&record.payload)?;
        let pool = require_pool(ctx)?;

        // Compacted page: pd_lsn idempotency guard, then the SAME
        // `SlottedPage::compact` with the same (ascending) dead_slots the
        // online path used — replay convergence is "replay = re-execute",
        // never a parallel reimplementation (§4.5). A fresh (all-zero) page
        // cannot hold the listed slots, so compact hard-fails on it — the
        // correct response to a WAL stream whose page-content records were
        // lost.
        {
            let mut guard = pool.pin_mut(rec.page_id)?;
            let page: &mut [u8; PAGE_SIZE] =
                guard.page_mut().try_into().expect("frame is PAGE_SIZE");
            if page_pd_lsn(page) < record.lsn {
                SlottedPage::init_if_fresh_with_special(page, HEAP_SPECIAL_SIZE);
                SlottedPage::compact(page, &rec.dead_slots).map_err(heap_to_storage)?;
                stamp_pd_lsn(page, record.lsn);
            }
        }

        // Chain unlink: relink the predecessor's `next_page` past the
        // spliced-out page. Guarded by the predecessor's OWN pd_lsn — the
        // two pages may have reached disk at different times before the
        // crash (same policy as a cross-page `HeapUpdate`).
        if rec.unlink_prev_page != PageId::INVALID {
            let mut guard = pool.pin_mut(rec.unlink_prev_page)?;
            let page: &mut [u8; PAGE_SIZE] =
                guard.page_mut().try_into().expect("frame is PAGE_SIZE");
            if page_pd_lsn(page) < record.lsn {
                SlottedPage::init_if_fresh_with_special(page, HEAP_SPECIAL_SIZE);
                let next = if rec.unlink_next_page == PageId::INVALID {
                    None
                } else {
                    Some(rec.unlink_next_page)
                };
                // The predecessor page comes from disk — an untrusted source
                // — so the geometry check inside set_next_page is a hard
                // Result here, not a debug assertion (F2).
                SlottedPage::set_next_page(page, next).map_err(heap_to_storage)?;
                stamp_pd_lsn(page, record.lsn);
            }
        }
        Ok(())
    }
}

/// Recovery always opens the buffer pool before replay (Stage I reorder), so a
/// missing pool is a programming error rather than a recoverable condition.
fn require_pool<'a>(ctx: &RedoContext<'a>) -> Result<&'a BufferPool> {
    ctx.buffer_pool.ok_or_else(|| {
        StorageError::InvalidOperation(
            "heap redo requires a buffer pool in RedoContext".to_string(),
        )
    })
}

/// Advance the page's authoritative `pd_lsn` to `max(lsn, current)`, sequencing
/// the read before the write to satisfy the borrow checker.
fn stamp_pd_lsn(page: &mut [u8; PAGE_SIZE], lsn: Lsn) {
    let new_lsn = lsn.max(page_pd_lsn(page));
    set_page_pd_lsn(page, new_lsn);
}

/// Stamp `t_xmax` (and optionally `HEAP_UPDATED`) onto the tuple at `tid`'s
/// slot, mirroring `HeapAM::stamp_deleted` for the redo path.
///
/// Accepted asymmetry (M2b trade-off): the live delete/update path stamps
/// BOTH `t_xmax` and the deleting command's `t_cid`, but redo restores only
/// `t_xmax` — the WAL delete/update record does not carry the command id.
/// This is benign because `t_cid` only matters to the deleting transaction
/// itself (same-txn "own delete" visibility, §7.2), and a transaction whose
/// delete is being replayed by definition did not survive crash recovery:
/// no post-recovery snapshot ever carries that XID as `current_xid`, so the
/// stale `t_cid` is never consulted. Revisit when subtransactions or
/// statement-level rollback arrive (they reintroduce same-XID
/// command-id-sensitive visibility after recovery-adjacent aborts).
fn stamp_deleted(
    page: &mut [u8; PAGE_SIZE],
    tid: Tid,
    xmax: pg_storage::types::TxnId,
    updated: bool,
) -> std::result::Result<(), HeapError> {
    let lp = SlottedPage::line_pointer(page, tid.slot_id)?;
    if lp.flags() != crate::line_pointer::LpFlags::Normal {
        return Err(HeapError::TupleNotFound(tid));
    }
    let off = lp.off() as usize;
    let mut header = TupleHeader::read_from(&page[off..off + TUPLE_HEADER_SIZE])?;
    header.t_xmax = xmax;
    if updated {
        header.t_infomask |= HEAP_UPDATED;
    }
    // A replayed delete/update is a REAL xmax stamp: clear a lock-only bit
    // that may sit on the flushed page image (M2c Stage P — lock-only
    // stamps are not WAL-logged, so a page flushed while a FOR UPDATE lock
    // was held can carry one; leaving it set would mask the replayed
    // delete from visibility and resurrect the row). IS_SHARE (H5) goes
    // with it. Mirrors the live path's `HeapAM::stamp_deleted`.
    header.t_infomask &= !(HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_IS_SHARE);
    header.write_to(&mut page[off..off + TUPLE_HEADER_SIZE]);
    Ok(())
}

/// Stamp `t_xmax`, `HEAP_UPDATED`, `t_ctid`, and `HEAP_HOT_UPDATED` onto the
/// old tuple at `old_tid`'s slot, mirroring `HeapAM::stamp_hot_update` for
/// the redo path. Like `stamp_deleted`, `t_cid` is not restored (the WAL
/// record does not carry it — see [`stamp_deleted`]'s comment).
fn stamp_hot_update(
    page: &mut [u8; PAGE_SIZE],
    old_tid: Tid,
    new_tid: Tid,
    xmax: pg_storage::types::TxnId,
) -> std::result::Result<(), HeapError> {
    let lp = SlottedPage::line_pointer(page, old_tid.slot_id)?;
    if lp.flags() != crate::line_pointer::LpFlags::Normal {
        return Err(HeapError::TupleNotFound(old_tid));
    }
    let off = lp.off() as usize;
    let mut header = TupleHeader::read_from(&page[off..off + TUPLE_HEADER_SIZE])?;
    header.t_xmax = xmax;
    header.t_infomask |= HEAP_UPDATED;
    header.t_infomask &= !(HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_IS_SHARE);
    header.t_ctid = new_tid;
    header.t_infomask2 |= HEAP_HOT_UPDATED;
    header.write_to(&mut page[off..off + TUPLE_HEADER_SIZE]);
    Ok(())
}

/// Map a heap-layer error into a storage error for the redo dispatch. A heap
/// failure during redo (e.g. a page that cannot hold a logged tuple) indicates
/// on-disk inconsistency, not a routine condition.
fn heap_to_storage(e: HeapError) -> StorageError {
    match e {
        HeapError::Storage(s) => s,
        other => StorageError::MetadataCorrupted(format!("heap redo: {other}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tuple::{encode_tuple, ColumnType, Datum};
    use pg_storage::types::{PageId, TxnId};

    /// T2 (M2c Stage P review): redo of a real delete/update must CLEAR a
    /// lock-only bit found on the page image. A page flushed while a FOR
    /// UPDATE lock was held carries `HEAP_XMAX_LOCK_ONLY` on disk (lock
    /// stamps are not WAL-logged); replaying the real delete over it must
    /// not leave the bit set, or the visibility mask would resurrect a
    /// replayed-deleted tuple.
    #[test]
    fn redo_stamp_deleted_clears_lock_only() {
        for updated in [false, true] {
            let mut page = [0u8; PAGE_SIZE];
            SlottedPage::init_with_special(&mut page, HEAP_SPECIAL_SIZE);
            let mut header = TupleHeader::new(
                TxnId(7),
                TxnId(9), // the crashed locker's stamp
                0,
                [0; 16],
                Tid {
                    page_id: PageId(1),
                    slot_id: 0,
                },
                0,
            );
            header.t_infomask = HEAP_XMAX_LOCK_ONLY;
            let bytes = encode_tuple(header, &[ColumnType::Int4], &[Some(Datum::Int4(1))]).unwrap();
            let slot = SlottedPage::add_tuple(&mut page, &bytes).unwrap();
            let tid = Tid {
                page_id: PageId(1),
                slot_id: slot,
            };

            stamp_deleted(&mut page, tid, TxnId(10), updated).unwrap();

            let bytes = SlottedPage::tuple(&page, slot).unwrap().unwrap();
            let h = TupleHeader::read_from(&bytes[..TUPLE_HEADER_SIZE]).unwrap();
            assert_eq!(h.t_xmax, TxnId(10));
            assert_eq!(
                h.t_infomask & HEAP_XMAX_LOCK_ONLY,
                0,
                "redo must clear LOCK_ONLY (updated={updated})"
            );
            assert_eq!(h.t_infomask & HEAP_UPDATED != 0, updated);
        }
    }
}
