//! M3 Stage C (tech-selection §12.1): crash windows of vacuum's physical
//! half.
//!
//! - **Window ②**: the `HeapCleanup` carrying compaction + chain unlink is
//!   durable, but the `PageFree` for the unlinked page never reached the WAL
//!   before the crash. Recovery must land on the precise accepted contract:
//!   the unlinked page is OFF the chain AND NOT in the freelist (a single-page
//!   leak — the documented trade-off of the unlink-then-free order), while
//!   chain traversal and heap scans stay consistent.
//! - **Compact-mid-crash convergence**, multi-page: an online `reclaim`
//!   spanning several pages (compactions plus one unlink+free) vs
//!   crash-recovery replay of the same WAL stream must produce byte-identical
//!   pages — the multi-page extension of Stage B's convergence test.

use std::sync::Arc;

use pg_am_heap::access_method::{
    AccessMethod, DeleteContext, InsertContext, RelationDesc, ScanContext, Vacuumable,
};
use pg_am_heap::tuple::{encode_tuple, ColumnType, Datum, TupleHeader};
use pg_am_heap::{heap_redo_handlers, HeapAM, SlottedPage};

use pg_storage::clog::NoOpClogAccessor;
use pg_storage::config::StorageConfig;
use pg_storage::engine::StorageEngine;
use pg_storage::types::{Oid, PageId, Tid, TxnId, PAGE_SIZE};

use pg_txn::{ClogAccessor, CommitWal, InMemoryClogAccessor, Snapshot, TxnManager};

use tempfile::TempDir;

const COLUMNS: [ColumnType; 2] = [ColumnType::Int4, ColumnType::Text];
const REL_OID: Oid = Oid(16_384);
const HORIZON: TxnId = TxnId(1000);

fn encode_row(xid: TxnId, id: i32, text_len: usize) -> Vec<u8> {
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
            Some(Datum::Text(format!("r{id}-{}", "x".repeat(text_len)))),
        ],
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

    fn insert(&self, xid: TxnId, id: i32, text_len: usize) -> Tid {
        let mut snap = Snapshot::everything();
        snap.set_current_xid(xid);
        let tuple = encode_row(xid, id, text_len);
        let mut out_tid = Tid {
            page_id: PageId(0),
            slot_id: 0,
        };
        self.heap
            .insert(InsertContext {
                rel: rel(self.first_page),
                snapshot: &snap,
                tuple: &tuple,
                out_tid: Some(&mut out_tid),
            })
            .unwrap();
        out_tid
    }

    fn delete(&self, xid: TxnId, tid: Tid) {
        let mut snap = Snapshot::everything();
        snap.set_current_xid(xid);
        self.heap
            .delete(DeleteContext {
                rel: rel(self.first_page),
                snapshot: &snap,
                tid,
                clog: self.clog.as_ref(),
            })
            .unwrap();
    }

    fn dead(&self) -> Vec<Tid> {
        self.heap
            .scan_dead_tuples(rel(self.first_page), HORIZON, self.clog.as_ref())
            .unwrap()
    }
}

/// Reopen after a simulated kill -9, replaying the heap + txn WAL (the
/// registered handler set includes `HeapCleanupRedoHandler`; the txn
/// handlers cover the fixture's commit/abort records).
fn recover_heap(tmp: &TempDir) -> (StorageEngine, HeapAM) {
    let config = StorageConfig::new(tmp.path());
    let mut handlers = heap_redo_handlers();
    handlers.extend(pg_txn::redo::txn_redo_handlers());
    let engine =
        StorageEngine::open_with_redo_handlers(tmp.path(), &config, handlers, Vec::new()).unwrap();
    let heap = HeapAM::new(
        Arc::clone(engine.buffer_pool()),
        Arc::clone(engine.wal_writer()),
    );
    (engine, heap)
}

/// Post-recovery scan: `NoOpClogAccessor` reads every XID as committed, so a
/// tuple with any `t_xmax` stamp is invisible and the rest are visible —
/// exactly the settled state the committed fixture transactions left behind.
fn visible_ids(heap: &HeapAM, first_page: PageId) -> Vec<i32> {
    let rows = heap
        .scan(ScanContext {
            rel: rel(first_page),
            snapshot: &Snapshot::everything(),
            clog: &NoOpClogAccessor,
        })
        .unwrap();
    let mut ids: Vec<i32> = rows
        .into_iter()
        .map(|(_, values)| match values[0] {
            Some(Datum::Int4(v)) => v,
            ref other => panic!("unexpected id datum: {other:?}"),
        })
        .collect();
    ids.sort_unstable();
    ids
}

/// Crash window ② (§12.1): `HeapCleanup` (compaction + unlink of the empty
/// tail page) is flushed; the crash hits before `free_page` runs — no
/// `PageFree` exists. Recovery contract, precisely: the page is OFF the chain
/// (the unlink replayed) AND NOT in the freelist (no PageFree to replay) —
/// the accepted single-page leak — and both chain traversal and heap scans
/// are consistent with that.
#[test]
fn crash_between_unlink_and_free_leaks_one_page_cleanly() {
    let tmp = TempDir::new().unwrap();

    let (first_page, page_b) = {
        let fx = Fixture::new(&tmp);
        let xa = fx.mgr.begin_txn();
        let t0 = fx.insert(xa, 0, 3500);
        let t1 = fx.insert(xa, 1, 3500);
        let t2 = fx.insert(xa, 2, 3500);
        let t3 = fx.insert(xa, 3, 3500);
        fx.mgr.commit_txn(xa).unwrap();
        let page_a = t0.page_id;
        let page_b = t2.page_id;
        assert_eq!(t1.page_id, page_a);
        assert_eq!(t3.page_id, page_b);
        assert_ne!(page_a, page_b);

        // Page B entirely dead.
        let xd = fx.mgr.begin_txn();
        fx.delete(xd, t2);
        fx.delete(xd, t3);
        fx.mgr.commit_txn(xd).unwrap();

        // Drive reclaim's unlink half MANUALLY, stopping before free_page:
        // compact + unlink under one HeapCleanup record (B is the tail:
        // prev = A, relink target = None), flush, then "crash".
        let kills: Vec<u16> = fx
            .dead()
            .iter()
            .filter(|t| t.page_id == page_b)
            .map(|t| t.slot_id)
            .collect();
        assert_eq!(kills, vec![0, 1]);
        fx.heap
            .compact_page(page_b, &kills, page_a, PageId::INVALID)
            .unwrap();
        fx.engine.wal_writer().flush().unwrap();
        std::mem::forget(fx.engine); // kill -9: PageFree never written
        (fx.first_page, page_b)
    };

    let (engine, heap) = recover_heap(&tmp);

    // Off-chain: seeding from the head walks the relinked chain and never
    // reaches page B; the predecessor's next pointer is cleared.
    assert_eq!(
        heap.relation_pages(&rel(first_page)).unwrap(),
        vec![first_page]
    );
    {
        let guard = engine.buffer_pool().pin(first_page).unwrap();
        let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
        assert_eq!(
            SlottedPage::next_page(page).unwrap(),
            None,
            "the unlink replayed: the chain terminates at the head"
        );
    }

    // NOT in the freelist: no PageFree was ever written, so freeing page B
    // now must SUCCEED (a freelist resident would trip the double-free
    // guard). This is the documented single-page leak of crash window ②:
    // the page is unreachable from the chain and unknown to the allocator.
    engine
        .page_allocator()
        .lock()
        .free_page(page_b)
        .expect("page B must NOT be on the freelist (double-free would error)");

    // Heap scan consistency: the live rows survive, the dead page's rows are
    // gone, and no dangling content is reachable.
    assert_eq!(visible_ids(&heap, first_page), vec![0, 1]);

    // The dead scan agrees: nothing left to collect on the surviving chain.
    let dead = heap
        .scan_dead_tuples(rel(first_page), HORIZON, &NoOpClogAccessor)
        .unwrap();
    assert!(dead.is_empty());
    engine.shutdown();
}

/// Compact-mid-crash convergence, multi-page: an online `reclaim` that
/// compacts two pages and unlinks+frees a third, crashed right after the WAL
/// flush, must be replayed by recovery to BYTE-IDENTICAL pages (the Stage B
/// single-page convergence guarantee extended across a whole vacuum pass).
#[test]
fn reclaim_mid_crash_redo_converges_multi_page() {
    let tmp = TempDir::new().unwrap();

    let (first_page, page_a, page_b, page_c, image_a, image_c) = {
        let fx = Fixture::new(&tmp);
        // Two ~3.5 KiB rows per page: A(0,1) B(2,3) C(4,5).
        let xa = fx.mgr.begin_txn();
        let mut tids = Vec::new();
        for i in 0..6 {
            tids.push(fx.insert(xa, i, 3500));
        }
        fx.mgr.commit_txn(xa).unwrap();
        let page_a = tids[0].page_id;
        let page_b = tids[2].page_id;
        let page_c = tids[4].page_id;
        assert_eq!(tids[1].page_id, page_a);
        assert_eq!(tids[3].page_id, page_b);
        assert_eq!(tids[5].page_id, page_c);

        // Kill row 0 (page A keeps row 1), ALL of page B, row 4 (page C
        // keeps row 5): reclaim will compact A and C in place and
        // unlink+free B (A's next pointer is relinked straight to C).
        let xd = fx.mgr.begin_txn();
        for &t in &[tids[0], tids[2], tids[3], tids[4]] {
            fx.delete(xd, t);
        }
        fx.mgr.commit_txn(xd).unwrap();

        fx.heap.reclaim(rel(fx.first_page), &fx.dead()).unwrap();
        assert_eq!(
            fx.heap.relation_pages(&rel(fx.first_page)).unwrap(),
            vec![page_a, page_c]
        );

        // Capture the surviving pages' post-reclaim bytes.
        let image_a = {
            let guard = fx.engine.buffer_pool().pin(page_a).unwrap();
            let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
            *page
        };
        let image_c = {
            let guard = fx.engine.buffer_pool().pin(page_c).unwrap();
            let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
            *page
        };

        fx.engine.wal_writer().flush().unwrap();
        std::mem::forget(fx.engine); // kill -9 mid-vacuum
        (fx.first_page, page_a, page_b, page_c, image_a, image_c)
    };

    let (engine, heap) = recover_heap(&tmp);

    // Byte-for-byte convergence on both surviving pages — including page A's
    // relinked next pointer (replayed from B's HeapCleanup unlink branch).
    {
        let guard = engine.buffer_pool().pin(page_a).unwrap();
        let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
        assert_eq!(page, &image_a, "page A must replay byte-identically");
        assert_eq!(SlottedPage::next_page(page).unwrap(), Some(page_c));
    }
    {
        let guard = engine.buffer_pool().pin(page_c).unwrap();
        let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
        assert_eq!(page, &image_c, "page C must replay byte-identically");
    }

    // Chain and heap scan are consistent with the online end state.
    assert_eq!(
        heap.relation_pages(&rel(first_page)).unwrap(),
        vec![page_a, page_c]
    );
    assert_eq!(visible_ids(&heap, first_page), vec![1, 5]);

    // Page B's PageFree WAS durable here (it preceded the flush), so the
    // rebuilt freelist hands it back out.
    let reused = heap.create_heap(Oid(16_385)).unwrap();
    assert_eq!(reused, page_b, "the freed page is back on the freelist");
    engine.shutdown();
}

/// F1/R2 regression: a checkpoint BETWEEN the fixture writes and the reclaim
/// makes the reclaim the predecessor page's FIRST touch in the new checkpoint
/// cycle, so `pin_mut(prev)` emits the PRE-unlink FPI. That FPI must sort
/// BEFORE the `HeapCleanup` record in the WAL — recovery replays FPIs
/// unconditionally, so the reverse order would roll the predecessor back to
/// "still linked to the spliced page" while the spliced page's `PageFree`
/// still replays onto the freelist: the chain then points at a page the
/// allocator can rehand to another relation (structural corruption).
///
/// Red-green: with the append-before-`pin_mut(prev)` order this test fails —
/// the recovered page A still points at the freed page B.
#[test]
fn unlink_prev_fpi_precedes_heap_cleanup_across_checkpoint() {
    let tmp = TempDir::new().unwrap();

    let (first_page, page_a, page_b, page_c, image_a, image_c) = {
        let fx = Fixture::new(&tmp);
        // Two ~3.5 KiB rows per page: A(0,1) B(2,3) C(4,5).
        let xa = fx.mgr.begin_txn();
        let mut tids = Vec::new();
        for i in 0..6 {
            tids.push(fx.insert(xa, i, 3500));
        }
        fx.mgr.commit_txn(xa).unwrap();
        let page_a = tids[0].page_id;
        let page_b = tids[2].page_id;
        let page_c = tids[4].page_id;
        assert_eq!(tids[1].page_id, page_a);
        assert_eq!(tids[3].page_id, page_b);
        assert_eq!(tids[5].page_id, page_c);

        // All of page B dead; row 4 on page C dead (so C is also compacted —
        // its pin_mut emits a pre-compact FPI of its own, same cycle).
        let xd = fx.mgr.begin_txn();
        for &t in &[tids[2], tids[3], tids[4]] {
            fx.delete(xd, t);
        }
        fx.mgr.commit_txn(xd).unwrap();

        // Complete a checkpoint between the fixture and the reclaim: every
        // page's next pin_mut is its first touch of the new cycle and emits
        // an FPI. Page A (the unlink predecessor) is exactly the cold page
        // the F1 scenario needs.
        fx.engine.trigger_checkpoint().unwrap();

        fx.heap.reclaim(rel(fx.first_page), &fx.dead()).unwrap();
        assert_eq!(
            fx.heap.relation_pages(&rel(fx.first_page)).unwrap(),
            vec![page_a, page_c]
        );

        let image_a = {
            let guard = fx.engine.buffer_pool().pin(page_a).unwrap();
            let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
            *page
        };
        let image_c = {
            let guard = fx.engine.buffer_pool().pin(page_c).unwrap();
            let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
            *page
        };

        fx.engine.wal_writer().flush().unwrap();
        std::mem::forget(fx.engine); // kill -9 right after the vacuum
        (fx.first_page, page_a, page_b, page_c, image_a, image_c)
    };

    let (engine, heap) = recover_heap(&tmp);

    // The relink survived replay: the pre-unlink FPI of page A was ordered
    // BEFORE the HeapCleanup record, so replaying the FPI (unconditionally)
    // and then the unlink branch reproduces A's online bytes exactly.
    {
        let guard = engine.buffer_pool().pin(page_a).unwrap();
        let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
        assert_eq!(page, &image_a, "page A must replay byte-identically");
        assert_eq!(
            SlottedPage::next_page(page).unwrap(),
            Some(page_c),
            "the unlink must survive replay — a rolled-back relink leaves the \
             chain pointing at the freed page"
        );
    }
    {
        let guard = engine.buffer_pool().pin(page_c).unwrap();
        let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
        assert_eq!(page, &image_c, "page C must replay byte-identically");
    }

    assert_eq!(
        heap.relation_pages(&rel(first_page)).unwrap(),
        vec![page_a, page_c],
        "the spliced page is off the chain after recovery"
    );
    assert_eq!(visible_ids(&heap, first_page), vec![0, 1, 5]);

    // Page B is on the freelist (its PageFree replayed) — and, crucially,
    // NOT also on the chain.
    let reused = heap.create_heap(Oid(16_385)).unwrap();
    assert_eq!(reused, page_b);
    engine.shutdown();
}
