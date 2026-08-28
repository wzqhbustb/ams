//! M3 Stage C (tech-selection §4.2/§4.4): HOT chain grouping and
//! `Vacuumable::collect_index_keys`.
//!
//! Four fixtures pin the exact output contract:
//!
//! 1. standalone dead tuples map to THEMSELVES (tid + own column values);
//! 2. a FULLY-dead HOT chain maps to its chain ROOT only (root tid + the
//!    ROOT's column values — the root owns the chain's index entries);
//! 3. a PARTIALLY-dead chain contributes NOTHING (no prune, no redirect);
//! 4. a page whose tuples are all dead yields one entry per tuple.
//!
//! Deadness itself is `scan_dead_tuples`' job (horizon rules); grouping only
//! decides structure: a chain is fully dead iff every member reachable from
//! the root via `t_ctid` is in the dead set.

use std::sync::Arc;

use pg_am_heap::access_method::{
    AccessMethod, DeleteContext, InsertContext, RelationDesc, UpdatableAM, UpdateContext,
    Vacuumable,
};
use pg_am_heap::tuple::{encode_tuple, ColumnType, Datum, TupleHeader};
use pg_am_heap::HeapAM;

use pg_storage::config::StorageConfig;
use pg_storage::engine::StorageEngine;
use pg_storage::types::{Oid, PageId, Tid, TxnId};

use pg_txn::{ClogAccessor, CommitWal, InMemoryClogAccessor, Snapshot, TxnManager};

use tempfile::TempDir;

const COLUMNS: [ColumnType; 2] = [ColumnType::Int4, ColumnType::Text];
const REL_OID: Oid = Oid(16_384);
/// Every fixture transaction ends long before this horizon.
const HORIZON: TxnId = TxnId(1000);

fn encode_row(xid: TxnId, id: i32, name: Option<&str>) -> Vec<u8> {
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
        &[
            Some(Datum::Int4(id)),
            name.map(|s| Datum::Text(s.to_string())),
        ],
    )
    .unwrap()
}

/// A heap wired like the engine wires it (real CLOG + txn manager, page
/// allocator installed), minus the SQL layer.
struct Fixture {
    engine: StorageEngine,
    heap: HeapAM,
    mgr: TxnManager,
    clog: Arc<InMemoryClogAccessor>,
    first_page: PageId,
}

impl Fixture {
    fn new(tmp: &TempDir) -> Self {
        let config = StorageConfig::new(tmp.path());
        let engine = StorageEngine::open(tmp.path(), &config).unwrap();
        let clog = Arc::new(InMemoryClogAccessor::new());
        let wal: Arc<dyn CommitWal> = Arc::clone(engine.wal_writer()) as Arc<dyn CommitWal>;
        let mgr = TxnManager::new(
            engine.txn_id_clock(),
            wal,
            Arc::clone(&clog) as Arc<dyn ClogAccessor>,
        );
        let mut heap = HeapAM::new(
            Arc::clone(engine.buffer_pool()),
            Arc::clone(engine.wal_writer()),
        );
        heap.set_page_allocator(Arc::clone(engine.page_allocator()));
        let first_page = heap.create_heap(REL_OID).unwrap();
        Fixture {
            engine,
            heap,
            mgr,
            clog,
            first_page,
        }
    }

    fn rel(&self) -> RelationDesc<'static> {
        RelationDesc {
            rel_oid: REL_OID,
            first_page: self.first_page,
            columns: &COLUMNS,
        }
    }

    /// Insert one row as `xid` (tuple `t_xmin = xid`), returning its TID.
    fn insert(&self, xid: TxnId, id: i32, name: Option<&str>) -> Tid {
        let mut snap = Snapshot::everything();
        snap.set_current_xid(xid);
        let tuple = encode_row(xid, id, name);
        let mut out_tid = Tid {
            page_id: PageId(0),
            slot_id: 0,
        };
        self.heap
            .insert(InsertContext {
                rel: self.rel(),
                snapshot: &snap,
                tuple: &tuple,
                out_tid: Some(&mut out_tid),
            })
            .unwrap();
        out_tid
    }

    /// Logically delete the tuple at `tid` as `xid` (stamps `t_xmax`).
    fn delete(&self, xid: TxnId, tid: Tid) {
        let mut snap = Snapshot::everything();
        snap.set_current_xid(xid);
        self.heap
            .delete(DeleteContext {
                rel: self.rel(),
                snapshot: &snap,
                tid,
                clog: self.clog.as_ref(),
            })
            .unwrap();
    }

    /// HOT-update the tuple at `old_tid` as `xid` (same-page, unchanged key
    /// columns): the old version gets `t_ctid` + `HEAP_HOT_UPDATED`, the new
    /// version lands on the same page marked `HEAP_ONLY_TUPLE`.
    fn hot_update(&self, xid: TxnId, old_tid: Tid, id: i32, name: Option<&str>) -> Tid {
        let mut snap = Snapshot::everything();
        snap.set_current_xid(xid);
        let new_tuple = encode_row(xid, id, name);
        let mut out_tid = Tid {
            page_id: PageId(0),
            slot_id: 0,
        };
        self.heap
            .update(UpdateContext {
                rel: self.rel(),
                snapshot: &snap,
                old_tid,
                new_tuple: &new_tuple,
                out_tid: Some(&mut out_tid),
                clog: self.clog.as_ref(),
                hot_eligible: true,
            })
            .unwrap();
        assert_eq!(
            out_tid.page_id, old_tid.page_id,
            "fixture requires the HOT same-page path"
        );
        out_tid
    }

    fn dead(&self) -> Vec<Tid> {
        self.heap
            .scan_dead_tuples(self.rel(), HORIZON, self.clog.as_ref())
            .unwrap()
    }

    fn keys(&self, dead: &[Tid]) -> Vec<(Tid, Vec<Option<Datum>>)> {
        self.heap.collect_index_keys(self.rel(), dead).unwrap()
    }
}

/// Fixture 1: standalone dead tuples (no HOT chains) map to themselves,
/// values decoded from their own bytes; a NULL column stays `None` in the
/// vector (the engine skips NULL keys when encoding, its existing
/// convention). The live row must not appear.
#[test]
fn standalone_dead_tuples_map_to_themselves() {
    let tmp = TempDir::new().unwrap();
    let fx = Fixture::new(&tmp);

    let xa = fx.mgr.begin_txn();
    let t0 = fx.insert(xa, 1, Some("a"));
    let _live = fx.insert(xa, 2, Some("b"));
    let t2 = fx.insert(xa, 3, None); // NULL text column
    fx.mgr.commit_txn(xa).unwrap();

    let xd = fx.mgr.begin_txn();
    fx.delete(xd, t0);
    fx.delete(xd, t2);
    fx.mgr.commit_txn(xd).unwrap();

    let dead = fx.dead();
    assert_eq!(dead, vec![t0, t2], "scan order: slot ascending");

    let keys = fx.keys(&dead);
    assert_eq!(
        keys,
        vec![
            (
                t0,
                vec![Some(Datum::Int4(1)), Some(Datum::Text("a".to_string()))]
            ),
            (t2, vec![Some(Datum::Int4(3)), None]),
        ],
        "each standalone dead tuple maps to itself with its own column values"
    );
    fx.engine.shutdown();
}

/// Fixture 2: a fully-dead HOT chain (root killed by the updater, tail killed
/// by the final deleter, both committed below the horizon) yields EXACTLY ONE
/// entry — the chain root, with the ROOT's column values, never the tail's.
#[test]
fn fully_dead_hot_chain_returns_root_only() {
    let tmp = TempDir::new().unwrap();
    let fx = Fixture::new(&tmp);

    let xa = fx.mgr.begin_txn();
    let root = fx.insert(xa, 7, Some("root-v"));
    fx.mgr.commit_txn(xa).unwrap();

    let xb = fx.mgr.begin_txn();
    let member = fx.hot_update(xb, root, 7, Some("member-v"));
    fx.mgr.commit_txn(xb).unwrap();

    // The final killer deletes the visible tail version.
    let xc = fx.mgr.begin_txn();
    fx.delete(xc, member);
    fx.mgr.commit_txn(xc).unwrap();

    let dead = fx.dead();
    assert_eq!(
        dead,
        vec![root, member],
        "both chain members are dead (xmax committed, < horizon)"
    );

    let keys = fx.keys(&dead);
    assert_eq!(
        keys,
        vec![(
            root,
            vec![
                Some(Datum::Int4(7)),
                Some(Datum::Text("root-v".to_string()))
            ]
        )],
        "one entry for the whole chain: the root, with the root's values"
    );
    fx.engine.shutdown();
}

/// Fixture 3: a partially-dead chain (root dead via committed HOT update,
/// tail still live) contributes NOTHING — the dead root stays in place
/// (§4.2: no prune, no t_ctid redirect).
#[test]
fn partially_dead_chain_returns_nothing() {
    let tmp = TempDir::new().unwrap();
    let fx = Fixture::new(&tmp);

    let xa = fx.mgr.begin_txn();
    let root = fx.insert(xa, 9, Some("root-v"));
    fx.mgr.commit_txn(xa).unwrap();

    let xb = fx.mgr.begin_txn();
    let _member = fx.hot_update(xb, root, 9, Some("member-v"));
    fx.mgr.commit_txn(xb).unwrap();

    let dead = fx.dead();
    assert_eq!(dead, vec![root], "only the root is dead");

    let keys = fx.keys(&dead);
    assert!(
        keys.is_empty(),
        "a partially-dead chain must contribute no index-cleanup entry, got {keys:?}"
    );
    fx.engine.shutdown();
}

/// Fixture 4: a page whose entire contents are dead yields one entry per
/// tuple (each is a standalone dead tuple); reclaim will turn this page
/// empty, but grouping itself is content-agnostic.
#[test]
fn page_of_only_dead_tuples_yields_each_of_them() {
    let tmp = TempDir::new().unwrap();
    let fx = Fixture::new(&tmp);

    let xa = fx.mgr.begin_txn();
    let t0 = fx.insert(xa, 1, Some("x"));
    let t1 = fx.insert(xa, 2, Some("y"));
    fx.mgr.commit_txn(xa).unwrap();

    let xd = fx.mgr.begin_txn();
    fx.delete(xd, t0);
    fx.delete(xd, t1);
    fx.mgr.commit_txn(xd).unwrap();

    let dead = fx.dead();
    assert_eq!(dead, vec![t0, t1]);
    let keys = fx.keys(&dead);
    assert_eq!(
        keys,
        vec![
            (
                t0,
                vec![Some(Datum::Int4(1)), Some(Datum::Text("x".to_string()))]
            ),
            (
                t1,
                vec![Some(Datum::Int4(2)), Some(Datum::Text("y".to_string()))]
            ),
        ]
    );
    fx.engine.shutdown();
}

/// Aborted-inserter tuples (dead by rule 1, regardless of horizon) take part
/// in chain grouping like any other dead tuple: an aborted standalone insert
/// maps to itself.
#[test]
fn aborted_insert_is_a_standalone_dead_tuple() {
    let tmp = TempDir::new().unwrap();
    let fx = Fixture::new(&tmp);

    let xa = fx.mgr.begin_txn();
    let orphan = fx.insert(xa, 5, Some("orphan"));
    fx.mgr.abort_txn(xa).unwrap();

    let dead = fx.dead();
    assert_eq!(
        dead,
        vec![orphan],
        "aborted inserter → dead unconditionally"
    );
    let keys = fx.keys(&dead);
    assert_eq!(
        keys,
        vec![(
            orphan,
            vec![
                Some(Datum::Int4(5)),
                Some(Datum::Text("orphan".to_string()))
            ]
        )]
    );
    fx.engine.shutdown();
}
