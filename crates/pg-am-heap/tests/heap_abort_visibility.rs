//! End-to-end abort invisibility (Stage J P1 fix).
//!
//! Drives the full path: `TxnManager::begin_txn` → `HeapAM::insert` →
//! `abort_txn` → `HeapAM::scan` with the *real* `InMemoryClogAccessor`.
//! The aborted transaction's tuple must be invisible to a later scan, and —
//! as the control — a committed transaction's tuple must be visible. Before
//! this fix `scan` hardcoded `NoOpClogAccessor` (everything reads committed),
//! so abort invisibility had no end-to-end path at all.

use std::sync::Arc;

use pg_am_heap::access_method::{AccessMethod, InsertContext, RelationDesc, ScanContext};
use pg_am_heap::tuple::{encode_tuple, ColumnType, Datum, TupleHeader};
use pg_am_heap::HeapAM;

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

/// Insert one row as `xid` (tuple `t_xmin = xid`, snapshot's own transaction).
fn insert_as(heap: &HeapAM, first_page: PageId, xid: TxnId, id: i32, name: &str) -> Tid {
    let mut snap = Snapshot::everything();
    snap.set_current_xid(xid);
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

#[test]
fn aborted_insert_is_invisible_committed_is_visible() {
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

    // Transaction A: insert then ABORT.
    let xid_a = mgr.begin_txn();
    insert_as(&heap, first_page, xid_a, 1, "aborted-row");
    mgr.abort_txn(xid_a).unwrap();

    // A later reader with the real CLOG sees nothing.
    let reader_snap = Snapshot::everything();
    let rows = heap
        .scan(ScanContext {
            rel: rel(first_page),
            snapshot: &reader_snap,
            clog: clog.as_ref(),
        })
        .unwrap();
    assert!(
        rows.is_empty(),
        "aborted transaction's tuple must be invisible, got {} rows",
        rows.len()
    );

    // Control — transaction B: insert then COMMIT.
    let xid_b = mgr.begin_txn();
    insert_as(&heap, first_page, xid_b, 2, "committed-row");
    mgr.commit_txn(xid_b).unwrap();

    let rows = heap
        .scan(ScanContext {
            rel: rel(first_page),
            snapshot: &reader_snap,
            clog: clog.as_ref(),
        })
        .unwrap();
    assert_eq!(rows.len(), 1, "only the committed tuple is visible");
    assert_eq!(rows[0].1[0], Some(Datum::Int4(2)));
    assert_eq!(rows[0].1[1], Some(Datum::Text("committed-row".to_string())));

    engine.shutdown();
}

#[test]
fn in_progress_insert_is_invisible_to_others_but_visible_to_self() {
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

    let xid = mgr.begin_txn();
    insert_as(&heap, first_page, xid, 1, "pending");

    // Another reader: the inserter is neither committed nor the reader itself.
    let other_snap = Snapshot::everything();
    let rows = heap
        .scan(ScanContext {
            rel: rel(first_page),
            snapshot: &other_snap,
            clog: clog.as_ref(),
        })
        .unwrap();
    assert!(rows.is_empty(), "in-progress insert must be invisible");

    // The inserting transaction itself sees its own write (the insert
    // stamped t_cid=0; the scan uses curcid=1, so t_cid < curcid → visible
    // as "written by an earlier command" — §7.2 / v2.3-Q4).
    let mut own_snap = Snapshot::everything();
    own_snap.set_current_xid(xid);
    own_snap.set_curcid(1);
    let rows = heap
        .scan(ScanContext {
            rel: rel(first_page),
            snapshot: &own_snap,
            clog: clog.as_ref(),
        })
        .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "a transaction sees its own uncommitted write"
    );

    mgr.commit_txn(xid).unwrap();
    engine.shutdown();
}

/// `scan_dead_tuples` collects BOTH kinds of dead rows: aborted inserts
/// (xmin aborted — dead regardless of `oldest_xmin`) and committed deletes
/// (xmax committed and older than `oldest_xmin`). Before the Stage J fix the
/// aborted insert was never reported, leaking dead space until M3 vacuum.
#[test]
fn scan_dead_tuples_collects_aborted_inserts_and_committed_deletes() {
    use pg_am_heap::access_method::{DeleteContext, Vacuumable};

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

    // Row A: inserted then ABORTED — dead by the xmin-aborted rule.
    let xid_a = mgr.begin_txn();
    let tid_a = insert_as(&heap, first_page, xid_a, 1, "aborted-row");
    mgr.abort_txn(xid_a).unwrap();

    // Row B: inserted and committed — alive for now.
    let xid_b = mgr.begin_txn();
    let tid_b = insert_as(&heap, first_page, xid_b, 2, "committed-row");
    mgr.commit_txn(xid_b).unwrap();

    // Before the delete: A is already dead (xmin rule, no bound dependency),
    // B is alive. Only A is reported.
    let dead = heap
        .scan_dead_tuples(rel(first_page), TxnId(1000), clog.as_ref())
        .unwrap();
    assert_eq!(dead, vec![tid_a], "only the aborted insert is dead so far");

    // Delete B as xid_c and commit — B becomes dead by the xmax rule.
    let xid_c = mgr.begin_txn();
    let mut snap_c = Snapshot::everything();
    snap_c.set_current_xid(xid_c);
    heap.delete(DeleteContext {
        clog: clog.as_ref(),
        rel: rel(first_page),
        snapshot: &snap_c,
        tid: tid_b,
    })
    .unwrap();
    mgr.commit_txn(xid_c).unwrap();

    let mut dead = heap
        .scan_dead_tuples(rel(first_page), TxnId(1000), clog.as_ref())
        .unwrap();
    dead.sort();
    let mut expected = vec![tid_a, tid_b];
    expected.sort();
    assert_eq!(
        dead, expected,
        "both the aborted insert and the committed delete must be collected"
    );

    // Control: with a bound BELOW the deleter's XID, B is not yet collectable
    // (a live snapshot could still see it); A still is (xmin rule has no
    // oldest_xmin dependency).
    let dead = heap
        .scan_dead_tuples(rel(first_page), xid_c, clog.as_ref())
        .unwrap();
    assert_eq!(dead, vec![tid_a]);

    engine.shutdown();
}

/// Reverse case of the deleter rule: a tuple deleted by an ABORTED
/// transaction is NOT dead — the delete never took effect, and the row must
/// stay visible and uncollectable.
#[test]
fn aborted_deleter_tuple_is_not_dead() {
    use pg_am_heap::access_method::{DeleteContext, Vacuumable};

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

    // Insert and commit a row.
    let xid_b = mgr.begin_txn();
    let tid_b = insert_as(&heap, first_page, xid_b, 2, "committed-row");
    mgr.commit_txn(xid_b).unwrap();

    // Delete it as xid_c, then ABORT the delete.
    let xid_c = mgr.begin_txn();
    let mut snap_c = Snapshot::everything();
    snap_c.set_current_xid(xid_c);
    heap.delete(DeleteContext {
        clog: clog.as_ref(),
        rel: rel(first_page),
        snapshot: &snap_c,
        tid: tid_b,
    })
    .unwrap();
    mgr.abort_txn(xid_c).unwrap();

    // The tuple still carries t_xmax = xid_c on the page, but the aborted
    // deleter means it is NOT collectable...
    let dead = heap
        .scan_dead_tuples(rel(first_page), TxnId(1000), clog.as_ref())
        .unwrap();
    assert!(
        dead.is_empty(),
        "aborted deleter must not mark the tuple dead, got {dead:?}"
    );

    // ...and it remains visible to a later scan (the delete never happened).
    let rows = heap
        .scan(ScanContext {
            rel: rel(first_page),
            snapshot: &Snapshot::everything(),
            clog: clog.as_ref(),
        })
        .unwrap();
    assert_eq!(rows.len(), 1, "row survives the aborted delete");

    engine.shutdown();
}

/// Regression for the LP-flags-only liveness check (Stage K review P1-2):
/// a logically deleted tuple keeps its LP `Normal`, so a second delete or an
/// update used to overwrite the existing `t_xmax` — and if the overwriter
/// aborted, the aborted stamp erased the committed one and the row came
/// back to life. Liveness now reads the tuple header (`t_xmax == INVALID`
/// required), so both operations must fail with `TupleNotFound`, and the
/// original committed delete must stay effective.
#[test]
fn delete_of_deleted_and_update_of_deleted_are_rejected() {
    use pg_am_heap::access_method::{DeleteContext, UpdatableAM, UpdateContext};
    use pg_am_heap::HeapError;

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

    // Insert and commit a row, then delete it (committed).
    let xid_b = mgr.begin_txn();
    let tid = insert_as(&heap, first_page, xid_b, 1, "doomed");
    mgr.commit_txn(xid_b).unwrap();

    let xid_d1 = mgr.begin_txn();
    let mut snap_d1 = Snapshot::everything();
    snap_d1.set_current_xid(xid_d1);
    heap.delete(DeleteContext {
        clog: clog.as_ref(),
        rel: rel(first_page),
        snapshot: &snap_d1,
        tid,
    })
    .unwrap();
    mgr.commit_txn(xid_d1).unwrap();

    // Second delete of the same row must be rejected (previously this
    // overwrote t_xmax; if this deleter aborted, the row resurrected).
    let xid_d2 = mgr.begin_txn();
    let mut snap_d2 = Snapshot::everything();
    snap_d2.set_current_xid(xid_d2);
    let err = heap
        .delete(DeleteContext {
            clog: clog.as_ref(),
            rel: rel(first_page),
            snapshot: &snap_d2,
            tid,
        })
        .unwrap_err();
    assert!(
        matches!(err, HeapError::TupleNotFound(_)),
        "delete of an already-deleted tuple must fail, got {err:?}"
    );
    mgr.abort_txn(xid_d2).unwrap();

    // Update of the deleted row must also be rejected.
    let xid_u = mgr.begin_txn();
    let mut snap_u = Snapshot::everything();
    snap_u.set_current_xid(xid_u);
    let err = heap
        .update(UpdateContext {
            clog: clog.as_ref(),
            hot_eligible: false,
            rel: rel(first_page),
            snapshot: &snap_u,
            old_tid: tid,
            new_tuple: &encode_row(xid_u, 2, "zombie"),
            out_tid: None,
        })
        .unwrap_err();
    assert!(
        matches!(err, HeapError::TupleNotFound(_)),
        "update of an already-deleted tuple must fail, got {err:?}"
    );
    mgr.abort_txn(xid_u).unwrap();

    // The original committed delete still holds: the row stays invisible.
    let rows = heap
        .scan(ScanContext {
            rel: rel(first_page),
            snapshot: &Snapshot::everything(),
            clog: clog.as_ref(),
        })
        .unwrap();
    assert!(rows.is_empty(), "deleted row must stay deleted: {rows:?}");

    engine.shutdown();
}

/// Regression for the stranded-row bug (Stage K review P1-1): a tuple whose
/// deleter ABORTED must be deletable/updatable again — the liveness check
/// consults the CLOG, so an aborted stamp does not count as a delete.
/// Previously such a tuple stayed visible yet was permanently unmodifiable
/// and uncollectable.
#[test]
fn aborted_deleter_tuple_can_be_deleted_again() {
    use pg_am_heap::access_method::{DeleteContext, UpdatableAM, UpdateContext};

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

    // Insert + commit a row, then delete it as xid_c and ABORT the delete:
    // the stamp remains but the delete never took effect.
    let xid_b = mgr.begin_txn();
    let tid = insert_as(&heap, first_page, xid_b, 1, "revisable");
    mgr.commit_txn(xid_b).unwrap();

    let xid_c = mgr.begin_txn();
    let mut snap_c = Snapshot::everything();
    snap_c.set_current_xid(xid_c);
    heap.delete(DeleteContext {
        rel: rel(first_page),
        snapshot: &snap_c,
        tid,
        clog: clog.as_ref(),
    })
    .unwrap();
    mgr.abort_txn(xid_c).unwrap();

    // A second delete (xid_d, committed) must SUCCEED: the aborted stamp
    // does not block it.
    let xid_d = mgr.begin_txn();
    let mut snap_d = Snapshot::everything();
    snap_d.set_current_xid(xid_d);
    heap.delete(DeleteContext {
        rel: rel(first_page),
        snapshot: &snap_d,
        tid,
        clog: clog.as_ref(),
    })
    .expect("delete after an aborted deleter must be allowed");
    mgr.commit_txn(xid_d).unwrap();

    let rows = heap
        .scan(ScanContext {
            rel: rel(first_page),
            snapshot: &Snapshot::everything(),
            clog: clog.as_ref(),
        })
        .unwrap();
    assert!(
        rows.is_empty(),
        "row must be deleted by the committed retry"
    );

    // Same for update: another row, aborted delete, then a committed update
    // must succeed and the new version becomes visible.
    let xid_b2 = mgr.begin_txn();
    let tid2 = insert_as(&heap, first_page, xid_b2, 2, "updatable");
    mgr.commit_txn(xid_b2).unwrap();

    let xid_c2 = mgr.begin_txn();
    let mut snap_c2 = Snapshot::everything();
    snap_c2.set_current_xid(xid_c2);
    heap.delete(DeleteContext {
        rel: rel(first_page),
        snapshot: &snap_c2,
        tid: tid2,
        clog: clog.as_ref(),
    })
    .unwrap();
    mgr.abort_txn(xid_c2).unwrap();

    let xid_u = mgr.begin_txn();
    let mut snap_u = Snapshot::everything();
    snap_u.set_current_xid(xid_u);
    heap.update(UpdateContext {
        rel: rel(first_page),
        snapshot: &snap_u,
        old_tid: tid2,
        new_tuple: &encode_row(xid_u, 3, "updated"),
        out_tid: None,
        clog: clog.as_ref(),
        hot_eligible: false,
    })
    .expect("update after an aborted deleter must be allowed");
    mgr.commit_txn(xid_u).unwrap();

    let rows = heap
        .scan(ScanContext {
            rel: rel(first_page),
            snapshot: &Snapshot::everything(),
            clog: clog.as_ref(),
        })
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1[0], Some(Datum::Int4(3)));

    engine.shutdown();
}
