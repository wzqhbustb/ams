//! M3 Stage B (tech-selection §4.5/§4.6): `HeapCleanup` redo convergence,
//! idempotency, and slot reuse after compaction across all four online
//! write paths.
//!
//! The "online compact" here is exactly what Stage C's vacuum will do:
//! append the `HeapCleanup` WAL record (ascending `dead_slots`), run the
//! shared [`SlottedPage::compact`] primitive on the latched page, then stamp
//! `pd_lsn`. Stage B has no vacuum driver yet, so the tests perform those
//! three steps directly — the point under test is that crash recovery
//! replays the SAME operation to the SAME bytes.

use std::sync::Arc;

use pg_am_heap::access_method::{
    AccessMethod, InsertContext, RelationDesc, ScanContext, UpdatableAM, UpdateContext,
};
use pg_am_heap::tuple::{
    encode_tuple, ColumnType, Datum, TupleHeader, HEAP_HOT_UPDATED, HEAP_ONLY_TUPLE,
    TUPLE_HEADER_SIZE,
};
use pg_am_heap::{
    heap_redo_handlers, HeapAM, HeapCleanupRedoHandler, HeapInsertHandler, SlottedPage,
};

use pg_storage::clog::NoOpClogAccessor;
use pg_storage::config::StorageConfig;
use pg_storage::engine::StorageEngine;
use pg_storage::page::{page_pd_lsn, set_page_pd_lsn};
use pg_storage::recovery::{
    ActiveXactTable, DirtyPageTable, IncompleteSplitTracker, RedoContext, RedoHandler,
};
use pg_storage::types::{Lsn, Oid, PageId, Tid, TxnId, PAGE_SIZE};
use pg_storage::wal::record::{HeapCleanupRecord, WalRecord};

use pg_txn::Snapshot;

use tempfile::TempDir;

const COLUMNS: [ColumnType; 2] = [ColumnType::Int4, ColumnType::Text];
const REL_OID: Oid = Oid(16_384);

/// A snapshot whose own transaction is `xid` (so freshly inserted tuples
/// carry `t_xmin = xid`), otherwise "see everything committed".
fn writer_snapshot(xid: u64) -> Snapshot {
    let mut snap = Snapshot::everything();
    snap.set_current_xid(TxnId(xid));
    snap
}

/// Encode a `(Int4, Text)` row with the given inserting XID.
fn encode_row(xid: u64, id: i32, name: &str) -> Vec<u8> {
    let header = TupleHeader::new(
        TxnId(xid),
        TxnId::INVALID,
        0,
        [0u8; 16],
        Tid {
            page_id: PageId(0),
            slot_id: 0,
        },
        0,
    );
    encode_tuple(
        header,
        &COLUMNS,
        &[Some(Datum::Int4(id)), Some(Datum::Text(name.to_string()))],
    )
    .unwrap()
}

/// A relation descriptor for the test heap. `first_page` is the chain head;
/// the AM rebuilds the rest of the page list by walking the on-disk chain.
fn rel(first_page: PageId) -> RelationDesc<'static> {
    RelationDesc {
        rel_oid: REL_OID,
        first_page,
        columns: &COLUMNS,
    }
}

fn open_heap(tmp: &TempDir) -> (StorageEngine, HeapAM) {
    let config = StorageConfig::new(tmp.path());
    let engine = StorageEngine::open(tmp.path(), &config).unwrap();
    let heap = HeapAM::new(
        Arc::clone(engine.buffer_pool()),
        Arc::clone(engine.wal_writer()),
    );
    (engine, heap)
}

/// Reopen after a simulated kill -9, replaying the heap WAL (the registered
/// handler set includes `HeapCleanupRedoHandler` — an unregistered record
/// type would hard-fail recovery here).
fn recover_heap(tmp: &TempDir) -> (StorageEngine, HeapAM) {
    let config = StorageConfig::new(tmp.path());
    let engine = StorageEngine::open_with_redo_handlers(
        tmp.path(),
        &config,
        heap_redo_handlers(),
        Vec::new(),
    )
    .unwrap();
    let heap = HeapAM::new(
        Arc::clone(engine.buffer_pool()),
        Arc::clone(engine.wal_writer()),
    );
    (engine, heap)
}

/// The online half of vacuum compaction (Stage C will drive this from
/// `Engine::vacuum`): pin for write FIRST — `pin_mut` may emit the
/// per-checkpoint-cycle FPI of the PRE-compact image, and the `HeapCleanup`
/// record must sort after it in the WAL, or recovery's unconditional FPI
/// replay would roll the page back past the compact (F1: the reverse order
/// silently undoes the compaction and the next slot-addressed redo hard-fails
/// on an occupied slot) — then append the record, run the shared `compact()`
/// primitive, and stamp `pd_lsn`, all while holding the page's write latch.
fn online_compact(engine: &StorageEngine, page_id: PageId, dead_slots: &[u16]) {
    let mut guard = engine.buffer_pool().pin_mut(page_id).unwrap();
    let rec = WalRecord::heap_cleanup(
        page_id,
        dead_slots.to_vec(),
        PageId::INVALID,
        PageId::INVALID,
    )
    .unwrap();
    let lsn = engine.wal_writer().append(rec).unwrap();
    let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().unwrap();
    SlottedPage::compact(page, dead_slots).unwrap();
    set_page_pd_lsn(page, lsn);
}

/// Insert a row, returning its TID.
fn insert_row(heap: &HeapAM, first_page: PageId, snap: &Snapshot, id: i32, name: &str) -> Tid {
    let tuple = encode_row(snap.current_xid().0, id, name);
    let mut tid = Tid {
        page_id: PageId(0),
        slot_id: 0,
    };
    heap.insert(InsertContext {
        rel: rel(first_page),
        snapshot: snap,
        tuple: &tuple,
        out_tid: Some(&mut tid),
    })
    .unwrap();
    tid
}

/// Every visible row as (Tid, id, name), in chain/scan order.
fn scan_rows(heap: &HeapAM, first_page: PageId) -> Vec<(Tid, i32, String)> {
    let snap = Snapshot::everything();
    let rows = heap
        .scan(ScanContext {
            rel: rel(first_page),
            snapshot: &snap,
            clog: &NoOpClogAccessor,
        })
        .unwrap();
    let mut out: Vec<(Tid, i32, String)> = rows
        .into_iter()
        .map(|(tid, values)| {
            let id = match values[0] {
                Some(Datum::Int4(v)) => v,
                ref other => panic!("unexpected id datum: {other:?}"),
            };
            let name = match &values[1] {
                Some(Datum::Text(s)) => s.clone(),
                other => panic!("unexpected name datum: {other:?}"),
            };
            (tid, id, name)
        })
        .collect();
    out.sort_by_key(|(_, id, _)| *id);
    out
}

/// `test_heap_cleanup_redo_converges`: an online compact vs crash-recovery
/// replay of the same WAL stream must produce byte-identical pages.
#[test]
fn test_heap_cleanup_redo_converges() {
    let tmp = TempDir::new().unwrap();

    let (first_page, online_image) = {
        let (engine, heap) = open_heap(&tmp);
        let first_page = heap.create_heap(REL_OID).unwrap();
        let snap = writer_snapshot(100);
        for i in 0..6 {
            insert_row(&heap, first_page, &snap, i, &format!("row-{i}"));
        }
        // Kill three middle slots, leaving holes and dead tuple bytes behind.
        online_compact(&engine, first_page, &[1, 3, 4]);

        let guard = engine.buffer_pool().pin(first_page).unwrap();
        let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
        let image = *page;
        drop(guard);

        engine.wal_writer().flush().unwrap();
        std::mem::forget(engine); // simulate kill -9: no graceful shutdown
        (first_page, image)
    };

    let (engine, _heap) = recover_heap(&tmp);
    let guard = engine.buffer_pool().pin(first_page).unwrap();
    let recovered: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
    assert_eq!(
        recovered, &online_image,
        "crash-recovery replay must reproduce the online compact byte-for-byte"
    );
    // Sanity on the converged content: slots 1/3/4 are Unused, 0/2/5 live.
    assert_eq!(SlottedPage::slot_count(recovered), 6);
    for live in [0u16, 2, 5] {
        assert!(SlottedPage::tuple(recovered, live).unwrap().is_some());
    }
    for dead in [1u16, 3, 4] {
        assert_eq!(SlottedPage::tuple(recovered, dead).unwrap(), None);
    }
}

/// `test_heap_cleanup_redo_idempotent`: replaying the same `HeapCleanup`
/// record (plus a chain-unlink variant) ten times changes nothing after the
/// first application — the `pd_lsn` guard makes every replay a no-op.
#[test]
fn test_heap_cleanup_redo_idempotent() {
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());
    let engine = StorageEngine::open(tmp.path(), &config).unwrap();

    let prev_page = {
        let mut guard = engine.buffer_pool().new_page().unwrap();
        let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().unwrap();
        SlottedPage::init_with_special(page, pg_am_heap::HEAP_SPECIAL_SIZE);
        guard.page_id()
    };
    let page_id = {
        let guard = engine.buffer_pool().new_page().unwrap();
        guard.page_id()
    };
    // Chain prev_page -> page_id.
    {
        let mut guard = engine.buffer_pool().pin_mut(prev_page).unwrap();
        let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().unwrap();
        SlottedPage::set_next_page(page, Some(page_id)).unwrap();
    }

    // Seed four tuples through the real insert redo path (lsns 100..=103).
    let clog = NoOpClogAccessor;
    let mut att = ActiveXactTable::new();
    let mut dpt = DirtyPageTable::new();
    let insert_handler = HeapInsertHandler;
    for slot in 0..4u16 {
        let mut rec = WalRecord::heap_insert(
            page_id,
            slot,
            encode_row(100, slot as i32, &format!("seed-{slot}")),
            TxnId(100),
        )
        .unwrap();
        rec.lsn = Lsn(100 + slot as u64);
        let mut incomplete_splits = IncompleteSplitTracker::new();
        let mut ctx = RedoContext {
            buffer_pool: Some(engine.buffer_pool()),
            page_allocator: engine.page_allocator(),
            clog: &clog,
            att: &mut att,
            dpt: &mut dpt,
            incomplete_splits: &mut incomplete_splits,
        };
        insert_handler.apply(&rec, &mut ctx).unwrap();
    }

    // Compaction + unlink: kill slots 1 and 3, splice page_id out of the
    // chain (it was prev_page's successor and had no successor itself).
    let mut cleanup = WalRecord::heap_cleanup(
        page_id,
        vec![1, 3],
        prev_page,
        PageId::INVALID, // the unlinked page was the tail
    )
    .unwrap();
    cleanup.lsn = Lsn(200);

    let cleanup_handler = HeapCleanupRedoHandler;
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
        cleanup_handler.apply(&cleanup, &mut ctx).unwrap();
    }

    let guard = engine.buffer_pool().pin(page_id).unwrap();
    let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
    assert_eq!(page_pd_lsn(page), Lsn(200));
    assert_eq!(SlottedPage::slot_count(page), 4, "LP array never shrinks");
    assert!(SlottedPage::tuple(page, 0).unwrap().is_some());
    assert!(SlottedPage::tuple(page, 2).unwrap().is_some());
    assert_eq!(SlottedPage::tuple(page, 1).unwrap(), None);
    assert_eq!(SlottedPage::tuple(page, 3).unwrap(), None);
    drop(guard);

    let guard = engine.buffer_pool().pin(prev_page).unwrap();
    let prev: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
    assert_eq!(page_pd_lsn(prev), Lsn(200));
    assert_eq!(
        SlottedPage::next_page(prev).unwrap(),
        None,
        "the predecessor's chain pointer is relinked past the spliced page"
    );
}

/// §4.6/R4 acceptance, insert path: compact() creates Unused middle slots,
/// the online insert takes the first-fit slot and carries it in the WAL
/// record, and crash redo places the tuple at exactly that slot — no
/// slot-diverged hard fail, no silent relocation.
#[test]
fn test_slot_reuse_after_compact_redo_insert() {
    let tmp = TempDir::new().unwrap();

    let first_page = {
        let (engine, heap) = open_heap(&tmp);
        let first_page = heap.create_heap(REL_OID).unwrap();
        let snap = writer_snapshot(100);
        for i in 0..4 {
            insert_row(&heap, first_page, &snap, i, &format!("v-{i}"));
        }
        online_compact(&engine, first_page, &[1, 2]);

        // The insert must recycle slot 1 (lowest Unused), not append at 4.
        let tid = insert_row(&heap, first_page, &snap, 100, "recycled");
        assert_eq!(
            tid,
            Tid {
                page_id: first_page,
                slot_id: 1
            },
            "online insert must take the first-fit Unused slot"
        );

        engine.wal_writer().flush().unwrap();
        std::mem::forget(engine);
        first_page
    };

    let (_engine, heap) = recover_heap(&tmp);
    let rows = scan_rows(&heap, first_page);
    assert_eq!(rows.len(), 3, "slots 0, 3 and the recycled slot 1 survive");
    let recycled = rows.iter().find(|(_, id, _)| *id == 100).unwrap();
    assert_eq!(
        recycled.0.slot_id, 1,
        "redo placed the tuple at the WAL-carried slot"
    );
    assert_eq!(recycled.2, "recycled");
}

/// §4.6/R4 acceptance, HOT update path: the new HEAP_ONLY version recycles a
/// compacted Unused slot on the same page, and crash redo reproduces the
/// chain exactly (old slot stamped t_ctid -> recycled slot).
#[test]
fn test_slot_reuse_after_compact_redo_hot_update() {
    let tmp = TempDir::new().unwrap();

    let first_page = {
        let (engine, heap) = open_heap(&tmp);
        let first_page = heap.create_heap(REL_OID).unwrap();
        let snap = writer_snapshot(100);
        let target = insert_row(&heap, first_page, &snap, 1, "original");
        for i in 0..3 {
            insert_row(&heap, first_page, &snap, 10 + i, &format!("filler-{i}"));
        }
        online_compact(&engine, first_page, &[1, 2]);

        let new_tuple = encode_row(100, 1, "hot-updated");
        let mut new_tid = Tid {
            page_id: PageId(0),
            slot_id: 0,
        };
        heap.update(UpdateContext {
            clog: &NoOpClogAccessor,
            hot_eligible: true,
            rel: rel(first_page),
            snapshot: &snap,
            old_tid: target,
            new_tuple: &new_tuple,
            out_tid: Some(&mut new_tid),
        })
        .unwrap();
        assert_eq!(
            new_tid,
            Tid {
                page_id: first_page,
                slot_id: 1
            },
            "the HOT new version must take the first-fit Unused slot"
        );

        engine.wal_writer().flush().unwrap();
        std::mem::forget(engine);
        first_page
    };

    let (engine, heap) = recover_heap(&tmp);

    // The updated row is visible through the chain at the recycled slot.
    let rows = scan_rows(&heap, first_page);
    let updated = rows.iter().find(|(_, id, _)| *id == 1).unwrap();
    assert_eq!(updated.0.slot_id, 1);
    assert_eq!(updated.2, "hot-updated");

    // The physical chain survived replay exactly: slot 0 carries
    // HEAP_HOT_UPDATED + t_ctid -> slot 1, and slot 1 is HEAP_ONLY.
    let guard = engine.buffer_pool().pin(first_page).unwrap();
    let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
    let old = SlottedPage::tuple(page, 0).unwrap().unwrap();
    let old_header = TupleHeader::read_from(&old[..TUPLE_HEADER_SIZE]).unwrap();
    assert!(old_header.t_infomask2 & HEAP_HOT_UPDATED != 0);
    assert_eq!(
        old_header.t_ctid,
        Tid {
            page_id: first_page,
            slot_id: 1
        }
    );
    let new = SlottedPage::tuple(page, 1).unwrap().unwrap();
    let new_header = TupleHeader::read_from(&new[..TUPLE_HEADER_SIZE]).unwrap();
    assert!(new_header.t_infomask2 & HEAP_ONLY_TUPLE != 0);
}

/// §4.6/R4 acceptance, same-page non-HOT update path: the new version
/// recycles a compacted Unused slot under a plain `HeapUpdate` record.
#[test]
fn test_slot_reuse_after_compact_redo_same_page_update() {
    let tmp = TempDir::new().unwrap();

    let first_page = {
        let (engine, heap) = open_heap(&tmp);
        let first_page = heap.create_heap(REL_OID).unwrap();
        let snap = writer_snapshot(100);
        let target = insert_row(&heap, first_page, &snap, 1, "original");
        for i in 0..3 {
            insert_row(&heap, first_page, &snap, 10 + i, &format!("filler-{i}"));
        }
        online_compact(&engine, first_page, &[1, 2]);

        let new_tuple = encode_row(100, 1, "same-page-updated");
        let mut new_tid = Tid {
            page_id: PageId(0),
            slot_id: 0,
        };
        heap.update(UpdateContext {
            clog: &NoOpClogAccessor,
            hot_eligible: false,
            rel: rel(first_page),
            snapshot: &snap,
            old_tid: target,
            new_tuple: &new_tuple,
            out_tid: Some(&mut new_tid),
        })
        .unwrap();
        assert_eq!(
            new_tid,
            Tid {
                page_id: first_page,
                slot_id: 1
            },
            "the same-page new version must take the first-fit Unused slot"
        );

        engine.wal_writer().flush().unwrap();
        std::mem::forget(engine);
        first_page
    };

    let (_engine, heap) = recover_heap(&tmp);
    let rows = scan_rows(&heap, first_page);
    assert_eq!(
        rows.len(),
        2,
        "two killed slots + the stamped old version are gone; the survivor filler and the new version remain"
    );
    let updated = rows.iter().find(|(_, id, _)| *id == 1).unwrap();
    assert_eq!(updated.0.slot_id, 1);
    assert_eq!(updated.2, "same-page-updated");
}

/// §4.6/R4 acceptance, cross-page update path: `acquire_page_with_room`
/// reverse-scans from the tail, so a compacted MIDDLE page becomes the new
/// version's target; its first-fit Unused slot is carried by the record and
/// reproduced by redo.
#[test]
fn test_slot_reuse_after_compact_redo_cross_page_update() {
    let tmp = TempDir::new().unwrap();
    // ~3.5 KiB text: exactly two rows fit per 8 KiB page, so the chain grows
    // one page per two inserts and a single-row hole leaves room for one.
    let big = |tag: &str| format!("{tag}-{}", "x".repeat(3500));

    let (first_page, middle_page, expected_slot) = {
        let (engine, heap) = open_heap(&tmp);
        let first_page = heap.create_heap(REL_OID).unwrap();
        let snap = writer_snapshot(100);

        let mut tids = Vec::new();
        for i in 0..6 {
            tids.push(insert_row(
                &heap,
                first_page,
                &snap,
                i,
                &big(&format!("r{i}")),
            ));
        }
        let page_a = tids[0].page_id;
        let page_b = tids[2].page_id;
        let page_c = tids[4].page_id;
        assert_eq!(tids[1].page_id, page_a, "two big rows per page");
        assert_eq!(tids[3].page_id, page_b);
        assert_eq!(tids[5].page_id, page_c);
        assert!(page_a != page_b && page_b != page_c && page_a != page_c);

        // Compact the MIDDLE page: kill r2 (its slot 0), leaving one Unused
        // slot and room for exactly one more big tuple.
        let killed_slot = tids[2].slot_id;
        online_compact(&engine, page_b, &[killed_slot]);

        // Update r0 (page A is full): the reverse scan skips the full tail
        // page C and lands the new version on the compacted middle page B,
        // in its first-fit Unused slot.
        let new_tuple = encode_row(100, 0, &big("r0-new"));
        let mut new_tid = Tid {
            page_id: PageId(0),
            slot_id: 0,
        };
        heap.update(UpdateContext {
            clog: &NoOpClogAccessor,
            hot_eligible: false,
            rel: rel(first_page),
            snapshot: &snap,
            old_tid: tids[0],
            new_tuple: &new_tuple,
            out_tid: Some(&mut new_tid),
        })
        .unwrap();
        assert_eq!(
            new_tid.page_id, page_b,
            "the new version must land on the compacted middle page"
        );
        assert_eq!(
            new_tid.slot_id, killed_slot,
            "the new version must take the middle page's first-fit Unused slot"
        );

        engine.wal_writer().flush().unwrap();
        std::mem::forget(engine);
        (first_page, page_b, killed_slot)
    };

    let (_engine, heap) = recover_heap(&tmp);
    let rows = scan_rows(&heap, first_page);
    assert_eq!(
        rows.len(),
        5,
        "r2 was compacted away; r0's old version is dead"
    );
    let relocated = rows.iter().find(|(_, id, _)| *id == 0).unwrap();
    assert_eq!(relocated.0.page_id, middle_page);
    assert_eq!(
        relocated.0.slot_id, expected_slot,
        "redo placed the cross-page new version at the WAL-carried slot"
    );
    assert!(relocated.2.starts_with("r0-new"));
    // The middle page's surviving row is intact alongside the newcomer.
    let r3 = rows.iter().find(|(_, id, _)| *id == 3).unwrap();
    assert_eq!(r3.0.page_id, middle_page);
}

/// F1 regression: a checkpoint BETWEEN the seed inserts and the compact makes
/// the compact the page's first touch in the new checkpoint cycle, so
/// `pin_mut` emits an FPI of the pre-compact image. The `HeapCleanup` record
/// must sort AFTER that FPI in the WAL — recovery replays FPIs
/// unconditionally, so the reverse order would roll the page back past the
/// compaction, and the next slot-addressed redo would hit an occupied slot
/// and hard-fail. (Red-green: with the append-before-pin order this test
/// fails at reopen with a slot divergence.)
#[test]
fn test_compact_after_checkpoint_replay_converges() {
    let tmp = TempDir::new().unwrap();

    let first_page = {
        let (engine, heap) = open_heap(&tmp);
        let first_page = heap.create_heap(REL_OID).unwrap();
        let snap = writer_snapshot(100);
        for i in 0..4 {
            insert_row(&heap, first_page, &snap, i, &format!("v-{i}"));
        }
        // Complete a checkpoint between the seed and the compact: the
        // compact's pin_mut is now the page's first touch in the new cycle
        // and MUST emit the pre-compact FPI before the HeapCleanup record.
        engine.trigger_checkpoint().unwrap();

        online_compact(&engine, first_page, &[1, 2]);
        let tid = insert_row(&heap, first_page, &snap, 100, "recycled");
        assert_eq!(tid.slot_id, 1, "insert recycles the first-fit Unused slot");

        engine.wal_writer().flush().unwrap();
        std::mem::forget(engine);
        first_page
    };

    let (_engine, heap) = recover_heap(&tmp);
    let rows = scan_rows(&heap, first_page);
    assert_eq!(rows.len(), 3, "slots 0, 3 and the recycled slot 1 survive");
    let recycled = rows.iter().find(|(_, id, _)| *id == 100).unwrap();
    assert_eq!(recycled.0.slot_id, 1);
    assert_eq!(recycled.2, "recycled");
}

/// F5 negative: a CRC-valid `HeapCleanup` record whose kill list is NOT
/// ascending (a producer bug, or on-disk corruption that kept the CRC
/// intact) must make recovery HARD-FAIL — never panic, never silently apply.
#[test]
fn test_heap_cleanup_non_ascending_kill_list_hard_fails_recovery() {
    let tmp = TempDir::new().unwrap();

    {
        let (engine, heap) = open_heap(&tmp);
        let first_page = heap.create_heap(REL_OID).unwrap();
        let snap = writer_snapshot(100);
        for i in 0..4 {
            insert_row(&heap, first_page, &snap, i, &format!("v-{i}"));
        }

        // Handcraft the poison record, bypassing the constructor's
        // validation: encode a legitimate [1, 2] record, then swap the two
        // slot bytes in the payload tail. bincode's standard config encodes
        // these small u16 varints as single bytes, so the tail swap yields
        // exactly the non-ascending list [2, 1].
        let mut poison =
            WalRecord::heap_cleanup(first_page, vec![1, 2], PageId::INVALID, PageId::INVALID)
                .unwrap();
        let n = poison.payload.len();
        poison.payload.swap(n - 2, n - 1);
        let decoded = HeapCleanupRecord::decode(&poison.payload).unwrap();
        assert_eq!(
            decoded.dead_slots,
            vec![2, 1],
            "the byte surgery must produce the non-ascending list"
        );

        // The WAL writer computes the CRC over whatever payload it is given,
        // so the record on disk is CRC-valid with a poisoned payload.
        engine.wal_writer().append(poison).unwrap();
        engine.wal_writer().flush().unwrap();
        std::mem::forget(engine);
    }

    let config = StorageConfig::new(tmp.path());
    let err = StorageEngine::open_with_redo_handlers(
        tmp.path(),
        &config,
        heap_redo_handlers(),
        Vec::new(),
    )
    .expect_err("recovery must hard-fail on a non-ascending kill list");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("ascending"),
        "the failure must name the violated contract, got: {msg}"
    );
}
