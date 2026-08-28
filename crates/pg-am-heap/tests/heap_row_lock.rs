//! AM-level coverage for the M2c Stage P row-lock pieces (tech-selection
//! §9.1) that the engine-level tests (`pg-engine/tests/m2c_locks.rs`) do
//! not reach directly:
//!
//! - `HeapAM::lock_tuple` (lock-only stamp): the locked row stays visible
//!   to scans, blocks a concurrent locker until commit, and is overwritable
//!   once the locker's stamp goes terminal;
//! - the crash-recovery carve-out in the §9.1 gate: a `t_xmax` whose CLOG
//!   entry reads `InProgress` but whose XID is NOT in the active set
//!   belongs to a crashed transaction and must be treated as aborted
//!   (waiting would spin forever — recovery-end ATT abort marking is still
//!   open, §11.3).
//!
//! Acceptance: `cargo test -p pg-am-heap --test heap_row_lock`

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use pg_am_heap::access_method::{
    AccessMethod, DeleteContext, InsertContext, RelationDesc, ScanContext, UpdatableAM,
    UpdateContext,
};
use pg_am_heap::tuple::{encode_tuple, ColumnType, Datum, TupleHeader};
use pg_am_heap::{HeapAM, HeapError};

use pg_storage::config::StorageConfig;
use pg_storage::engine::StorageEngine;
use pg_storage::types::{Oid, PageId, Tid, TxnId};

use pg_txn::{ClogAccessor, CommitWal, InMemoryClogAccessor, RowWaiter, Snapshot, TxnManager};

use tempfile::TempDir;

const COLUMNS: [ColumnType; 2] = [ColumnType::Int4, ColumnType::Text];
const REL_OID: Oid = Oid(16_384);

struct Rig {
    _tmp: TempDir,
    engine: StorageEngine,
    clog: Arc<InMemoryClogAccessor>,
    mgr: Arc<TxnManager>,
    heap: Arc<HeapAM>,
    first_page: PageId,
}

/// Open storage + CLOG + a TxnManager, and a HeapAM WITH the row waiter
/// installed (the Stage P configuration; `HeapAM::new` alone is the legacy
/// no-waiter mode).
fn rig() -> Rig {
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());
    let engine = StorageEngine::open(tmp.path(), &config).unwrap();

    let clog = Arc::new(InMemoryClogAccessor::new());
    let wal: Arc<dyn CommitWal> = Arc::clone(engine.wal_writer()) as Arc<dyn CommitWal>;
    let mgr = Arc::new(TxnManager::new(
        engine.txn_id_clock(),
        wal,
        Arc::clone(&clog) as Arc<dyn ClogAccessor>,
    ));

    let mut heap = HeapAM::new(
        Arc::clone(engine.buffer_pool()),
        Arc::clone(engine.wal_writer()),
    );
    heap.set_row_waiter(Arc::clone(&mgr) as Arc<dyn RowWaiter>);
    let heap = Arc::new(heap);
    let first_page = heap.create_heap(REL_OID).unwrap();
    Rig {
        _tmp: tmp,
        engine,
        clog,
        mgr,
        heap,
        first_page,
    }
}

fn rel(first_page: PageId) -> RelationDesc<'static> {
    RelationDesc {
        rel_oid: REL_OID,
        first_page,
        columns: &COLUMNS,
    }
}

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

fn snap_for(xid: TxnId) -> Snapshot {
    let mut snap = Snapshot::everything();
    snap.set_current_xid(xid);
    snap
}

fn insert_as(rig: &Rig, xid: TxnId, id: i32, name: &str) -> Tid {
    let tuple = encode_row(xid, id, name);
    let mut out_tid = Tid {
        page_id: PageId(0),
        slot_id: 0,
    };
    rig.heap
        .insert(InsertContext {
            rel: rel(rig.first_page),
            snapshot: &snap_for(xid),
            tuple: &tuple,
            out_tid: Some(&mut out_tid),
        })
        .unwrap();
    out_tid
}

fn visible_rows(rig: &Rig) -> Vec<(Tid, Vec<Option<Datum>>)> {
    rig.heap
        .scan(ScanContext {
            rel: rel(rig.first_page),
            snapshot: &Snapshot::everything(),
            clog: rig.clog.as_ref(),
        })
        .unwrap()
}

/// A lock-only stamp must not hide the row from scans (a lock is not a
/// delete), must block a concurrent locker while the locker is active, and
/// must be overwritable once the locker's transaction ends.
#[test]
fn lock_only_stamp_visible_blocks_then_overwritable() {
    let rig = rig();
    let xid_i = rig.mgr.begin_txn();
    let tid = insert_as(&rig, xid_i, 1, "row");
    rig.mgr.commit_txn(xid_i).unwrap();

    // Transaction L locks the row (FOR UPDATE path).
    let xid_l = rig.mgr.begin_txn();
    rig.heap
        .lock_tuple(tid, &snap_for(xid_l), rig.clog.as_ref())
        .unwrap();

    // The locked row is still visible to a plain scan.
    assert_eq!(visible_rows(&rig).len(), 1, "LOCK_ONLY must not delete");

    // A concurrent locker blocks until L commits...
    let done = Arc::new(AtomicBool::new(false));
    let done2 = Arc::clone(&done);
    let heap2 = Arc::clone(&rig.heap);
    let mgr2 = Arc::clone(&rig.mgr);
    let clog2 = Arc::clone(&rig.clog) as Arc<dyn ClogAccessor>;
    let handle = thread::spawn(move || {
        let xid_w = mgr2.begin_txn();
        let clog_ref: &dyn ClogAccessor = clog2.as_ref();
        heap2.lock_tuple(tid, &snap_for(xid_w), clog_ref).unwrap();
        done2.store(true, Ordering::SeqCst);
        xid_w
    });
    thread::sleep(Duration::from_millis(300));
    assert!(
        !done.load(Ordering::SeqCst),
        "second lock_tuple must block behind the active locker"
    );
    rig.mgr.commit_txn(xid_l).unwrap();
    let xid_w = handle.join().unwrap();

    // ...and once BOTH stamps are terminal the row is updatable again (a
    // committed lock-only stamp is overwritten, never "concurrently
    // updated").
    rig.mgr.commit_txn(xid_w).unwrap();
    let xid_u = rig.mgr.begin_txn();
    let new_tuple = encode_row(xid_u, 1, "updated");
    rig.heap
        .update(UpdateContext {
            rel: rel(rig.first_page),
            snapshot: &snap_for(xid_u),
            old_tid: tid,
            new_tuple: &new_tuple,
            out_tid: None,
            clog: rig.clog.as_ref(),
            hot_eligible: false,
        })
        .unwrap();
    rig.mgr.commit_txn(xid_u).unwrap();

    let rows = visible_rows(&rig);
    assert_eq!(rows.len(), 1);
    assert!(matches!(&rows[0].1[1], Some(Datum::Text(s)) if s == "updated"));

    rig.engine.shutdown();
}

/// Crash-recovery carve-out: a tuple whose `t_xmax` was stamped by a
/// transaction that CRASHED (CLOG still reads `InProgress`, but the XID is
/// in no post-recovery active set) must be treated as aborted — the gate
/// must NOT wait on it (the wait would return instantly and spin).
///
/// Simulated by driving the delete through one `TxnManager` and the
/// post-"restart" update through a SECOND manager over the same CLOG: the
/// deleter's XID is unknown to the second manager's active set, exactly
/// like a crashed transaction after recovery.
#[test]
fn crashed_in_progress_stamper_is_treated_as_aborted() {
    let rig = rig();
    let xid_i = rig.mgr.begin_txn();
    let tid = insert_as(&rig, xid_i, 1, "doomed");
    rig.mgr.commit_txn(xid_i).unwrap();

    // The "crashed" transaction: stamps t_xmax, never commits.
    let xid_d = rig.mgr.begin_txn();
    rig.heap
        .delete(DeleteContext {
            rel: rel(rig.first_page),
            snapshot: &snap_for(xid_d),
            tid,
            clog: rig.clog.as_ref(),
        })
        .unwrap();
    // No commit/abort: xid_d vanishes with its manager, as a crash would.

    // "Post-recovery": a fresh manager (empty active set) over the same
    // CLOG, and a heap whose waiter is that manager.
    let wal2: Arc<dyn CommitWal> = Arc::clone(rig.engine.wal_writer()) as Arc<dyn CommitWal>;
    let mgr2 = Arc::new(TxnManager::new(
        rig.engine.txn_id_clock(),
        wal2,
        Arc::clone(&rig.clog) as Arc<dyn ClogAccessor>,
    ));
    let mut heap2 = HeapAM::new(
        Arc::clone(rig.engine.buffer_pool()),
        Arc::clone(rig.engine.wal_writer()),
    );
    heap2.set_row_waiter(Arc::clone(&mgr2) as Arc<dyn RowWaiter>);

    // The update must PROCEED (stamp treated as aborted) — with a wait it
    // would spin forever, so the assertion doubles as a livelock tripwire.
    let xid_u = mgr2.begin_txn();
    let new_tuple = encode_row(xid_u, 1, "survived");
    heap2
        .update(UpdateContext {
            rel: rel(rig.first_page),
            snapshot: &snap_for(xid_u),
            old_tid: tid,
            new_tuple: &new_tuple,
            out_tid: None,
            clog: rig.clog.as_ref(),
            hot_eligible: false,
        })
        .unwrap();
    mgr2.commit_txn(xid_u).unwrap();

    let rows = visible_rows(&rig);
    assert_eq!(rows.len(), 1);
    assert!(matches!(&rows[0].1[1], Some(Datum::Text(s)) if s == "survived"));

    rig.engine.shutdown();
}

/// F4 (M2c Stage P review): the gate distinguishes my own LOCK_ONLY stamp
/// (idempotent re-lock) from my own REAL delete/update stamp — re-locking
/// the latter would re-add the lock-only bit on a deleted row, and the
/// visibility mask would resurrect it.
#[test]
fn self_real_delete_stamp_cannot_be_relocked() {
    let rig = rig();
    let xid_i = rig.mgr.begin_txn();
    let tid = insert_as(&rig, xid_i, 1, "row");
    rig.mgr.commit_txn(xid_i).unwrap();

    // Same transaction: delete, then attempt to lock the same version.
    let xid_d = rig.mgr.begin_txn();
    rig.heap
        .delete(DeleteContext {
            rel: rel(rig.first_page),
            snapshot: &snap_for(xid_d),
            tid,
            clog: rig.clog.as_ref(),
        })
        .unwrap();
    let err = rig
        .heap
        .lock_tuple(tid, &snap_for(xid_d), rig.clog.as_ref())
        .unwrap_err();
    assert!(
        matches!(err, HeapError::InvalidArgument(_)),
        "re-locking my own real delete stamp must be rejected, got {err:?}"
    );
    rig.mgr.abort_txn(xid_d).unwrap();

    // Idempotent: re-locking my own LOCK_ONLY stamp is fine.
    let xid_l = rig.mgr.begin_txn();
    rig.heap
        .lock_tuple(tid, &snap_for(xid_l), rig.clog.as_ref())
        .unwrap();
    rig.heap
        .lock_tuple(tid, &snap_for(xid_l), rig.clog.as_ref())
        .unwrap();
    rig.mgr.commit_txn(xid_l).unwrap();

    rig.engine.shutdown();
}
