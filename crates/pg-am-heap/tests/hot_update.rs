//! Stage S: HOT (Heap-Only Tuple) update integration tests.
//!
//! Covers:
//! - Same-page HOT update: scan follows t_ctid chain, returns new values
//! - Aborted HOT update: old version stays visible (MVCC safety)
//! - 3-hop HOT chain: chain following with multiple updates
//! - HOT update crash recovery: HeapHotUpdate redo reconstructs the chain

use std::sync::Arc;

use pg_am_heap::access_method::{
    AccessMethod, InsertContext, RelationDesc, ScanContext, UpdatableAM, UpdateContext,
};
use pg_am_heap::tuple::{
    encode_tuple, ColumnType, Datum, TupleHeader, HEAP_HOT_UPDATED, HEAP_ONLY_TUPLE,
    TUPLE_HEADER_SIZE,
};
use pg_am_heap::{heap_redo_handlers, HeapAM};

use pg_storage::clog::NoOpClogAccessor;
use pg_storage::config::StorageConfig;
use pg_storage::engine::StorageEngine;
use pg_storage::types::{Oid, PageId, Tid, TxnId};

use pg_txn::{ClogAccessor, CommitWal, InMemoryClogAccessor, Snapshot, TxnManager};

use tempfile::TempDir;

const COLUMNS: [ColumnType; 2] = [ColumnType::Int4, ColumnType::Text];
const REL_OID: Oid = Oid(16_384);

fn encode_row(xid: TxnId, id: i32, name: &str) -> Vec<u8> {
    let header = TupleHeader::new(
        xid,
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

fn rel(first_page: PageId) -> RelationDesc<'static> {
    RelationDesc {
        rel_oid: REL_OID,
        first_page,
        columns: &COLUMNS,
    }
}

fn writer_snapshot(xid: TxnId) -> Snapshot {
    let mut snap = Snapshot::everything();
    snap.set_current_xid(xid);
    snap
}

/// Insert one row as `xid` and return its TID.
fn insert_as(heap: &HeapAM, first_page: PageId, xid: TxnId, id: i32, name: &str) -> Tid {
    let snap = writer_snapshot(xid);
    let tuple = encode_row(xid, id, name);
    let mut out_tid = Tid {
        page_id: PageId(0),
        slot_id: 0,
    };
    heap.insert(InsertContext {
        rel: rel(first_page),
        snapshot: &snap,
        tuple: &tuple,
        out_tid: Some(&mut out_tid),
    })
    .unwrap();
    out_tid
}

/// HOT-update the row at `old_tid` with new values, keeping key columns
/// unchanged (`hot_eligible = true`).
fn hot_update(
    heap: &HeapAM,
    first_page: PageId,
    old_tid: Tid,
    xid: TxnId,
    id: i32,
    name: &str,
) -> Tid {
    let snap = writer_snapshot(xid);
    let tuple = encode_row(xid, id, name);
    let mut out_tid = Tid {
        page_id: PageId(0),
        slot_id: 0,
    };
    heap.update(UpdateContext {
        rel: rel(first_page),
        snapshot: &snap,
        old_tid,
        new_tuple: &tuple,
        out_tid: Some(&mut out_tid),
        clog: &NoOpClogAccessor,
        hot_eligible: true,
    })
    .unwrap();
    out_tid
}

/// Read the header of the tuple at `tid` for flag assertions.
fn read_header(buffer_pool: &pg_storage::buffer_pool::BufferPool, tid: Tid) -> TupleHeader {
    let guard = buffer_pool.pin(tid.page_id).unwrap();
    let page: &[u8] = guard.page();
    let page_arr: &[u8; pg_storage::types::PAGE_SIZE] =
        page.try_into().expect("frame is PAGE_SIZE");
    let bytes = pg_am_heap::slotted_page::SlottedPage::tuple(page_arr, tid.slot_id)
        .unwrap()
        .unwrap();
    TupleHeader::read_from(&bytes[..TUPLE_HEADER_SIZE]).unwrap()
}

/// Insert → HOT update (same key) → scan returns new values; the old tuple
/// carries `HEAP_HOT_UPDATED` and the new tuple carries `HEAP_ONLY_TUPLE`.
#[test]
fn test_hot_update_page_local() {
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());
    let engine = StorageEngine::open(tmp.path(), &config).unwrap();
    let heap = HeapAM::new(
        Arc::clone(engine.buffer_pool()),
        Arc::clone(engine.wal_writer()),
    );
    let first_page = heap.create_heap(REL_OID).unwrap();

    // Insert (id=1, "alice") as xid 100.
    let tid0 = insert_as(&heap, first_page, TxnId(100), 1, "alice");

    // HOT update to (id=1, "bob") as xid 200 — key column unchanged.
    let tid1 = hot_update(&heap, first_page, tid0, TxnId(200), 1, "bob");
    assert_eq!(tid1.page_id, tid0.page_id, "HOT update must stay same-page");
    assert_ne!(tid1.slot_id, tid0.slot_id, "new version gets a new slot");

    // Old tuple: HEAP_HOT_UPDATED set, t_ctid points to new version.
    let h0 = read_header(engine.buffer_pool(), tid0);
    assert!(
        h0.t_infomask2 & HEAP_HOT_UPDATED != 0,
        "old tuple must have HEAP_HOT_UPDATED"
    );
    assert_eq!(h0.t_ctid, tid1, "t_ctid must point to new version");

    // New tuple: HEAP_ONLY_TUPLE set.
    let h1 = read_header(engine.buffer_pool(), tid1);
    assert!(
        h1.t_infomask2 & HEAP_ONLY_TUPLE != 0,
        "new tuple must have HEAP_ONLY_TUPLE"
    );

    // Scan with NoOpClogAccessor (all committed) returns the new version.
    let scan_snap = Snapshot::everything();
    let rows = heap
        .scan(ScanContext {
            rel: rel(first_page),
            snapshot: &scan_snap,
            clog: &NoOpClogAccessor,
        })
        .unwrap();
    assert_eq!(rows.len(), 1, "exactly one visible row");
    assert_eq!(rows[0].0, tid1, "scan returns the new version's TID");
    assert_eq!(rows[0].1[0], Some(Datum::Int4(1)));
    assert_eq!(rows[0].1[1], Some(Datum::Text("bob".to_string())));
}

/// Aborted HOT update: the old version stays visible (MVCC safety).
#[test]
fn test_hot_update_aborted() {
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());
    let engine = StorageEngine::open(tmp.path(), &config).unwrap();

    let clog = Arc::new(InMemoryClogAccessor::new());
    let wal: Arc<dyn CommitWal> = Arc::clone(engine.wal_writer()) as Arc<dyn CommitWal>;
    let mgr = TxnManager::new(
        engine.txn_id_clock(),
        wal,
        Arc::clone(&clog) as Arc<dyn ClogAccessor>,
    );

    let heap = HeapAM::new(
        Arc::clone(engine.buffer_pool()),
        Arc::clone(engine.wal_writer()),
    );
    let first_page = heap.create_heap(REL_OID).unwrap();

    // Insert and commit (xid 100).
    let xid100 = mgr.begin_txn();
    let tid0 = insert_as(&heap, first_page, xid100, 1, "alice");
    mgr.commit_txn(xid100).unwrap();

    // HOT update as xid 200, then ABORT.
    let xid200 = mgr.begin_txn();
    let snap200 = writer_snapshot(xid200);
    let tuple = encode_row(xid200, 1, "bob");
    heap.update(UpdateContext {
        rel: rel(first_page),
        snapshot: &snap200,
        old_tid: tid0,
        new_tuple: &tuple,
        out_tid: None,
        clog: clog.as_ref(),
        hot_eligible: true,
    })
    .unwrap();
    mgr.abort_txn(xid200).unwrap();

    // Scan must see the old version (aborted update never took effect).
    let reader_snap = Snapshot::everything();
    let rows = heap
        .scan(ScanContext {
            rel: rel(first_page),
            snapshot: &reader_snap,
            clog: clog.as_ref(),
        })
        .unwrap();
    assert_eq!(rows.len(), 1, "old version must be visible after abort");
    assert_eq!(rows[0].1[0], Some(Datum::Int4(1)));
    assert_eq!(rows[0].1[1], Some(Datum::Text("alice".to_string())));
}

/// Three successive HOT updates form a 3-hop chain; scan returns the latest.
#[test]
fn test_hot_update_chain_3() {
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());
    let engine = StorageEngine::open(tmp.path(), &config).unwrap();
    let heap = HeapAM::new(
        Arc::clone(engine.buffer_pool()),
        Arc::clone(engine.wal_writer()),
    );
    let first_page = heap.create_heap(REL_OID).unwrap();

    // Insert (id=1, "v0") as xid 100.
    let tid0 = insert_as(&heap, first_page, TxnId(100), 1, "v0");

    // Three successive HOT updates — each key column unchanged.
    let tid1 = hot_update(&heap, first_page, tid0, TxnId(200), 1, "v1");
    let tid2 = hot_update(&heap, first_page, tid1, TxnId(300), 1, "v2");
    let tid3 = hot_update(&heap, first_page, tid2, TxnId(400), 1, "v3");

    // Scan (NoOpClogAccessor = all committed) must return the latest version.
    let scan_snap = Snapshot::everything();
    let rows = heap
        .scan(ScanContext {
            rel: rel(first_page),
            snapshot: &scan_snap,
            clog: &NoOpClogAccessor,
        })
        .unwrap();
    assert_eq!(rows.len(), 1, "exactly one visible row");
    assert_eq!(rows[0].0, tid3, "scan returns the latest version's TID");
    assert_eq!(rows[0].1[1], Some(Datum::Text("v3".to_string())));
}

/// HOT update crash recovery: the HeapHotUpdate redo handler reconstructs
/// the chain after a simulated crash.
#[test]
fn test_hot_update_crash_recovery() {
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());

    let first_page = {
        let engine = StorageEngine::open(tmp.path(), &config).unwrap();
        let heap = HeapAM::new(
            Arc::clone(engine.buffer_pool()),
            Arc::clone(engine.wal_writer()),
        );
        let first_page = heap.create_heap(REL_OID).unwrap();

        // Insert (id=1, "alice") as xid 100.
        let tid0 = insert_as(&heap, first_page, TxnId(100), 1, "alice");

        // HOT update to (id=1, "bob") as xid 200.
        let _tid1 = hot_update(&heap, first_page, tid0, TxnId(200), 1, "bob");

        engine.wal_writer().flush().unwrap();
        std::mem::forget(engine); // crash
        first_page
    };

    // Reopen with heap redo handlers (includes HeapHotUpdateHandler).
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

    let scan_snap = Snapshot::everything();
    let rows = heap
        .scan(ScanContext {
            rel: rel(first_page),
            snapshot: &scan_snap,
            clog: &NoOpClogAccessor,
        })
        .unwrap();
    assert_eq!(rows.len(), 1, "one visible row after recovery");
    assert_eq!(rows[0].1[0], Some(Datum::Int4(1)));
    assert_eq!(rows[0].1[1], Some(Datum::Text("bob".to_string())));
}

/// Post-Stage-S review H1: a 20-hop HOT chain (the pre-H1 hardcoded 8-hop
/// cap silently dropped every version past the eighth) is followed to its
/// end by the heap scan.
#[test]
fn test_hot_update_chain_20_followed_to_end() {
    const UPDATES: u64 = 20;
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());
    let engine = StorageEngine::open(tmp.path(), &config).unwrap();
    let heap = HeapAM::new(
        Arc::clone(engine.buffer_pool()),
        Arc::clone(engine.wal_writer()),
    );
    let first_page = heap.create_heap(REL_OID).unwrap();

    let mut tid = insert_as(&heap, first_page, TxnId(100), 1, "v0");
    for i in 1..=UPDATES {
        let name = format!("v{i}");
        tid = hot_update(&heap, first_page, tid, TxnId(100 + i), 1, &name);
        assert_eq!(tid.page_id, first_page, "update {i} must stay same-page");
    }

    let scan_snap = Snapshot::everything();
    let rows = heap
        .scan(ScanContext {
            rel: rel(first_page),
            snapshot: &scan_snap,
            clog: &NoOpClogAccessor,
        })
        .unwrap();
    assert_eq!(rows.len(), 1, "exactly one visible row");
    assert_eq!(rows[0].0, tid, "scan returns the 20-hop chain tail");
    assert_eq!(
        rows[0].1[1],
        Some(Datum::Text(format!("v{UPDATES}"))),
        "scan must follow the chain to its end, not stop at the cap"
    );
}
