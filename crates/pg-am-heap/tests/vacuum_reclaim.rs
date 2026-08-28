//! M3 Stage C (tech-selection §4.1 stages 4/5, §4.2): `Vacuumable::reclaim`.
//!
//! Covers: dead slots killed and holes reclaimed via `compact()`; fully-empty
//! pages unlinked from the chain and returned to the allocator (unlink THEN
//! free — the irreversible order); per-page eviction from the AM's in-memory
//! page-list cache; space/page reuse by later inserts without cross-talk; the
//! kill-list contract (partially-dead chains are never touched); and the
//! watchdog-guarded concurrency note of §4.1 (a reader blocked on the page
//! latch during compaction only ever sees the whole pre- or post-image).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use pg_am_heap::access_method::{
    AccessMethod, DeleteContext, InsertContext, RelationDesc, ScanContext, UpdatableAM,
    UpdateContext, Vacuumable,
};
use pg_am_heap::tuple::{encode_tuple, ColumnType, Datum, TupleHeader};
use pg_am_heap::{HeapAM, SlottedPage};

use pg_storage::config::StorageConfig;
use pg_storage::engine::StorageEngine;
use pg_storage::types::{Oid, PageId, Tid, TxnId, PAGE_SIZE};

use pg_txn::{ClogAccessor, CommitWal, InMemoryClogAccessor, Snapshot, TxnManager};

use tempfile::TempDir;

const COLUMNS: [ColumnType; 2] = [ColumnType::Int4, ColumnType::Text];
const REL_OID: Oid = Oid(16_384);
const HORIZON: TxnId = TxnId(1000);
/// Watchdog margin: every concurrency test must FAIL, never hang.
const WATCHDOG: Duration = Duration::from_secs(30);

fn encode_row_sized(xid: TxnId, id: i32, text_len: usize) -> Vec<u8> {
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

    fn insert(&self, xid: TxnId, id: i32, text_len: usize) -> Tid {
        let mut snap = Snapshot::everything();
        snap.set_current_xid(xid);
        let tuple = encode_row_sized(xid, id, text_len);
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

    fn dead(&self) -> Vec<Tid> {
        self.heap
            .scan_dead_tuples(self.rel(), HORIZON, self.clog.as_ref())
            .unwrap()
    }

    /// Every VISIBLE row id (scan order), through the real CLOG.
    fn visible_ids(&self) -> Vec<i32> {
        let rows = self
            .heap
            .scan(ScanContext {
                rel: self.rel(),
                snapshot: &Snapshot::everything(),
                clog: self.clog.as_ref(),
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

    fn free_space(&self, page_id: PageId) -> usize {
        let guard = self.engine.buffer_pool().pin(page_id).unwrap();
        let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
        SlottedPage::free_space(page)
    }
}

/// Compaction-only reclaim: the killed slot becomes `Unused`, the hole is
/// reclaimed into contiguous free space, and a later insert RECYCLES the
/// freed slot (first-fit) and space on the same page.
#[test]
fn reclaim_kills_slots_and_reclaims_holes() {
    let tmp = TempDir::new().unwrap();
    let fx = Fixture::new(&tmp);

    let xa = fx.mgr.begin_txn();
    let t0 = fx.insert(xa, 0, 2000);
    let t1 = fx.insert(xa, 1, 2000);
    assert_eq!(t0.page_id, t1.page_id);
    fx.mgr.commit_txn(xa).unwrap();

    let xd = fx.mgr.begin_txn();
    fx.delete(xd, t0);
    fx.mgr.commit_txn(xd).unwrap();

    let free_before = fx.free_space(t0.page_id);
    fx.heap.reclaim(fx.rel(), &fx.dead()).unwrap();
    let free_after = fx.free_space(t0.page_id);

    // Slot killed: LP is Unused, tuple unreadable.
    {
        let guard = fx.engine.buffer_pool().pin(t0.page_id).unwrap();
        let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
        assert_eq!(SlottedPage::tuple(page, t0.slot_id).unwrap(), None);
        assert!(SlottedPage::tuple(page, t1.slot_id).unwrap().is_some());
        pg_am_heap::slotted_page::debug_assert_invariants(page);
    }
    // The hole joined contiguous free space (tuple bytes + nothing else; the
    // LP entry itself stays — slot numbers are TID components).
    assert!(
        free_after >= free_before + 2000,
        "hole reclaimed: free {free_before} -> {free_after}"
    );

    // A later insert recycles the Unused slot AND the reclaimed bytes.
    let xb = fx.mgr.begin_txn();
    let recycled = fx.insert(xb, 10, 500);
    fx.mgr.commit_txn(xb).unwrap();
    assert_eq!(
        recycled,
        Tid {
            page_id: t0.page_id,
            slot_id: t0.slot_id
        },
        "first-fit recycles the compacted slot"
    );
    assert_eq!(fx.visible_ids(), vec![1, 10]);
    fx.engine.shutdown();
}

/// Empty-page release: a page that becomes fully empty is unlinked from the
/// chain (unlink info rides the same HeapCleanup record), evicted from the
/// AM's page-list cache, and pushed onto the allocator freelist — in that
/// order. Later inserts never land on the unlinked page while it is off the
/// chain; the allocator eventually rehands it as a fresh tail with zero
/// cross-talk.
#[test]
fn reclaim_unlinks_and_frees_empty_pages() {
    let tmp = TempDir::new().unwrap();
    let fx = Fixture::new(&tmp);

    // ~3.5 KiB rows: exactly two per 8 KiB page.
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

    // Page B becomes entirely dead; page A keeps row 1.
    let xd = fx.mgr.begin_txn();
    fx.delete(xd, t2);
    fx.delete(xd, t3);
    fx.mgr.commit_txn(xd).unwrap();

    assert_eq!(
        fx.heap.relation_pages(&fx.rel()).unwrap(),
        vec![page_a, page_b]
    );
    fx.heap.reclaim(fx.rel(), &fx.dead()).unwrap();

    // Unlinked: the on-disk chain and the in-memory cache both skip page B.
    assert_eq!(
        fx.heap.relation_pages(&fx.rel()).unwrap(),
        vec![page_a],
        "the unlinked page is evicted from the per-relation cache"
    );
    {
        let guard = fx.engine.buffer_pool().pin(page_a).unwrap();
        let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
        assert_eq!(SlottedPage::next_page(page).unwrap(), None);
    }
    assert_eq!(fx.visible_ids(), vec![0, 1]);

    // Cache eviction, observable: a small insert reverse-scans the cached
    // page list. Had page B stayed cached (as the tail, with a full page of
    // free space), the insert would land on it — a row written into an
    // unlinked page, logically unreachable. It must land on page A.
    let xb = fx.mgr.begin_txn();
    let small = fx.insert(xb, 10, 50);
    fx.mgr.commit_txn(xb).unwrap();
    assert_eq!(
        small.page_id, page_a,
        "insert after unlink must never land on the unlinked page"
    );

    // Freelist reuse: page A is full (rows 0/1 alive + the small row), so
    // the next big insert forces chain extension; the allocator pops page B
    // (LIFO) as the fresh tail, re-initialized — no trace of the old tenant.
    let xc = fx.mgr.begin_txn();
    let big1 = fx.insert(xc, 20, 3500); // forces the extension
    let big2 = fx.insert(xc, 21, 3500); // shares the new tail
    fx.mgr.commit_txn(xc).unwrap();
    assert_eq!(
        big1.page_id, page_b,
        "the freed page is rehanded by the allocator as the new tail"
    );
    assert_eq!(big2.page_id, page_b);
    assert_eq!(big1.slot_id, 0, "the reused page starts from a clean init");
    assert_eq!(
        fx.visible_ids(),
        vec![0, 1, 10, 20, 21],
        "the reused page carries only the new tenant's rows"
    );
    fx.engine.shutdown();
}

/// The chain HEAD is never unlinked or freed, even when it becomes fully
/// empty: it is the relation's anchor (`RelationDesc::first_page`).
#[test]
fn empty_chain_head_is_compacted_but_kept() {
    let tmp = TempDir::new().unwrap();
    let fx = Fixture::new(&tmp);

    let xa = fx.mgr.begin_txn();
    let t0 = fx.insert(xa, 0, 100);
    let t1 = fx.insert(xa, 1, 100);
    fx.mgr.commit_txn(xa).unwrap();
    let xd = fx.mgr.begin_txn();
    fx.delete(xd, t0);
    fx.delete(xd, t1);
    fx.mgr.commit_txn(xd).unwrap();

    fx.heap.reclaim(fx.rel(), &fx.dead()).unwrap();

    assert_eq!(
        fx.heap.relation_pages(&fx.rel()).unwrap(),
        vec![fx.first_page],
        "the empty head stays on the chain"
    );
    assert!(fx.visible_ids().is_empty());
    // The head was NOT freed: the next allocation is a fresh page id.
    let other = fx.heap.create_heap(Oid(16_385)).unwrap();
    assert_ne!(other, fx.first_page);

    // The compacted head takes new rows again (slot recycling).
    let xb = fx.mgr.begin_txn();
    let tid = fx.insert(xb, 9, 100);
    fx.mgr.commit_txn(xb).unwrap();
    assert_eq!(tid.page_id, fx.first_page);
    assert_eq!(tid.slot_id, 0, "first-fit recycles the killed slot");
    assert_eq!(fx.visible_ids(), vec![9]);
    fx.engine.shutdown();
}

/// Kill-list contract (§4.2, `SlottedPage::compact` docs): reclaiming the
/// dead ROOT of a partially-dead HOT chain must kill NOTHING — the root stays
/// readable and the live tail reachable. The raw `scan_dead_tuples` output
/// names the root; reclaim's grouping re-derivation filters it out.
#[test]
fn reclaim_never_touches_partially_dead_chains() {
    let tmp = TempDir::new().unwrap();
    let fx = Fixture::new(&tmp);

    let xa = fx.mgr.begin_txn();
    let root = fx.insert(xa, 7, 100);
    fx.mgr.commit_txn(xa).unwrap();

    let xb = fx.mgr.begin_txn();
    let mut snap = Snapshot::everything();
    snap.set_current_xid(xb);
    let new_tuple = encode_row_sized(xb, 7, 120);
    let mut member = Tid {
        page_id: PageId(0),
        slot_id: 0,
    };
    fx.heap
        .update(UpdateContext {
            rel: fx.rel(),
            snapshot: &snap,
            old_tid: root,
            new_tuple: &new_tuple,
            out_tid: Some(&mut member),
            clog: fx.clog.as_ref(),
            hot_eligible: true,
        })
        .unwrap();
    fx.mgr.commit_txn(xb).unwrap();

    let free_before = fx.free_space(root.page_id);
    let dead = fx.dead();
    assert_eq!(dead, vec![root], "only the root is dead");
    fx.heap.reclaim(fx.rel(), &dead).unwrap();

    // Nothing killed: the root tuple is still physically present (its index
    // entry anchors the live tail), and no hole was reclaimed.
    {
        let guard = fx.engine.buffer_pool().pin(root.page_id).unwrap();
        let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
        assert!(SlottedPage::tuple(page, root.slot_id).unwrap().is_some());
        assert!(SlottedPage::tuple(page, member.slot_id).unwrap().is_some());
    }
    assert_eq!(fx.free_space(root.page_id), free_before);
    assert_eq!(fx.visible_ids(), vec![7]);
    fx.engine.shutdown();
}

/// Regression (Stage C adversarial review): when chain-ADJACENT pages all
/// become empty in one reclaim pass, each unlink must relink the nearest
/// STILL-CHAINED predecessor — never a page already spliced out. Relinking a
/// removed page would leave the live chain pointing at a page that is then
/// freed (structural corruption once the allocator rehands it).
#[test]
fn consecutive_empty_pages_unlink_to_live_predecessor() {
    let tmp = TempDir::new().unwrap();
    let fx = Fixture::new(&tmp);

    // Chain A(0,1) → B(2,3) → C(4,5); B and C both become fully dead.
    let xa = fx.mgr.begin_txn();
    let mut tids = Vec::new();
    for i in 0..6 {
        tids.push(fx.insert(xa, i, 3500));
    }
    fx.mgr.commit_txn(xa).unwrap();
    let (page_a, page_b, page_c) = (tids[0].page_id, tids[2].page_id, tids[4].page_id);
    assert_eq!(tids[1].page_id, page_a);
    assert_eq!(tids[3].page_id, page_b);
    assert_eq!(tids[5].page_id, page_c);

    let xd = fx.mgr.begin_txn();
    for &t in &tids[2..6] {
        fx.delete(xd, t);
    }
    fx.mgr.commit_txn(xd).unwrap();

    fx.heap.reclaim(fx.rel(), &fx.dead()).unwrap();

    // Both empty pages are off the chain; the head's next pointer is cleared
    // (NOT left pointing at B's freed-and-relinked ghost).
    assert_eq!(fx.heap.relation_pages(&fx.rel()).unwrap(), vec![page_a]);
    {
        let guard = fx.engine.buffer_pool().pin(page_a).unwrap();
        let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
        assert_eq!(SlottedPage::next_page(page).unwrap(), None);
    }
    assert_eq!(fx.visible_ids(), vec![0, 1]);

    // Both pages made the freelist (LIFO: B freed first, then C).
    let reuse1 = fx.heap.create_heap(Oid(16_385)).unwrap();
    let reuse2 = fx.heap.create_heap(Oid(16_386)).unwrap();
    assert_eq!(
        [reuse1, reuse2],
        [page_c, page_b],
        "both spliced pages returned to the allocator"
    );
    fx.engine.shutdown();
}

/// Generalized R1 coverage: page-id order ≠ chain order. Freed low-id pages
/// come back via the allocator's LIFO freelist as LATER tails of the chain
/// (chain [1, 4, 3, 2, 5] below), and then two CHAIN-adjacent pages become
/// empty in one reclaim pass while the kill list is processed in page-ID
/// order (2 before 3 — the reverse of their chain order). The unlink must
/// resolve each predecessor as the nearest STILL-ON-CHAIN left neighbor in
/// CHAIN order (the `removed` set), never by page id and never a page
/// already spliced out.
#[test]
fn consecutive_empty_pages_unlink_nonmonotonic_page_ids() {
    let tmp = TempDir::new().unwrap();
    let fx = Fixture::new(&tmp);

    // Phase 1: chain A(0,1) → B(2,3) → C(4,5) → D(6,7), fresh ascending ids.
    let xa = fx.mgr.begin_txn();
    let mut tids = Vec::new();
    for i in 0..8 {
        tids.push(fx.insert(xa, i, 3500));
    }
    fx.mgr.commit_txn(xa).unwrap();
    let page_a = tids[0].page_id;
    let page_d = tids[6].page_id;

    // Empty B and C, reclaim: both unlinked and freed (page-id order: B
    // first, then C). Freelist = [B, C], chain = A → D.
    let xd = fx.mgr.begin_txn();
    for &t in &tids[2..6] {
        fx.delete(xd, t);
    }
    fx.mgr.commit_txn(xd).unwrap();
    fx.heap.reclaim(fx.rel(), &fx.dead()).unwrap();
    assert_eq!(
        fx.heap.relation_pages(&fx.rel()).unwrap(),
        vec![page_a, page_d]
    );

    // Phase 2: six more big rows force three extensions. The allocator pops
    // the freelist LIFO — C first, then B, then a fresh page E — so the
    // chain grows A → D → C → B → E: chain order is NOT page-id order.
    let xb = fx.mgr.begin_txn();
    let mut tids2 = Vec::new();
    for i in 10..16 {
        tids2.push(fx.insert(xb, i, 3500));
    }
    fx.mgr.commit_txn(xb).unwrap();
    let page_c = tids2[0].page_id;
    let page_b = tids2[2].page_id;
    let page_e = tids2[4].page_id;
    assert_eq!(tids2[1].page_id, page_c);
    assert_eq!(tids2[3].page_id, page_b);
    assert_eq!(tids2[5].page_id, page_e);
    assert!(
        page_b < page_c && page_c < page_d && page_d < page_e,
        "fixture sanity: B and C are reused low ids, E is fresh"
    );
    assert_eq!(
        fx.heap.relation_pages(&fx.rel()).unwrap(),
        vec![page_a, page_d, page_c, page_b, page_e],
        "chain order [1, 4, 3, 2, 5]: page-id order != chain order"
    );

    // Phase 3: empty C and B — CHAIN-adjacent (D → C → B → E). reclaim
    // processes the kill map in page-ID order: B(id 2) BEFORE C(id 3), the
    // reverse of their chain order. B's unlink relinks C.next past B to E;
    // C's unlink must then find D (not the already-spliced B) and relink
    // D.next past C to E.
    let xd2 = fx.mgr.begin_txn();
    for &t in &tids2[0..4] {
        fx.delete(xd2, t);
    }
    fx.mgr.commit_txn(xd2).unwrap();
    fx.heap.reclaim(fx.rel(), &fx.dead()).unwrap();

    // Chain links end up correct: A → D → E, each live predecessor pointing
    // past ALL unlinked pages.
    assert_eq!(
        fx.heap.relation_pages(&fx.rel()).unwrap(),
        vec![page_a, page_d, page_e]
    );
    for (page, expect_next) in [
        (page_a, Some(page_d)),
        (page_d, Some(page_e)),
        (page_e, None),
    ] {
        let guard = fx.engine.buffer_pool().pin(page).unwrap();
        let p: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
        assert_eq!(
            SlottedPage::next_page(p).unwrap(),
            expect_next,
            "chain link of {page}"
        );
    }

    // Validate-style traversal: a FRESH HeapAM (empty page cache) re-seeds
    // by walking the on-disk next pointers and must see exactly A → D → E.
    let fresh = HeapAM::new(
        Arc::clone(fx.engine.buffer_pool()),
        Arc::clone(fx.engine.wal_writer()),
    );
    assert_eq!(
        fresh.relation_pages(&fx.rel()).unwrap(),
        vec![page_a, page_d, page_e],
        "on-disk chain traversal agrees with the cache"
    );

    // Heap scan consistent: only the rows on A, D, E survive.
    assert_eq!(fx.visible_ids(), vec![0, 1, 6, 7, 14, 15]);

    // Both spliced pages returned to the allocator (freed B then C — page-id
    // processing order — so the LIFO freelist pops C first).
    let reuse1 = fx.heap.create_heap(Oid(16_385)).unwrap();
    let reuse2 = fx.heap.create_heap(Oid(16_386)).unwrap();
    assert_eq!([reuse1, reuse2], [page_c, page_b]);
    fx.engine.shutdown();
}

/// §4.1 concurrency note: page-level read/write pins make compaction atomic
/// to readers. A reader spinning on `pin` while reclaim compacts the page
/// only ever observes the whole pre-image or the whole post-image — never a
/// half-compacted page. Watchdog-guarded (the regression must FAIL, not hang
/// `cargo test`).
#[test]
fn readers_during_compaction_see_no_half_compacted_page() {
    let tmp = TempDir::new().unwrap();
    let fx = Fixture::new(&tmp);

    let xa = fx.mgr.begin_txn();
    let mut tids = Vec::new();
    for i in 0..4 {
        tids.push(fx.insert(xa, i, 200));
    }
    fx.mgr.commit_txn(xa).unwrap();
    let page_id = tids[0].page_id;
    let kill_slots: Vec<u16> = vec![tids[1].slot_id, tids[2].slot_id];
    let xd = fx.mgr.begin_txn();
    for &t in &tids[1..3] {
        fx.delete(xd, t);
    }
    fx.mgr.commit_txn(xd).unwrap();

    // Reader: spin on read pins until told to stop, asserting per-read
    // consistency on every iteration — both kill-target slots in the SAME
    // state (a half-compacted page would show exactly one killed) plus the
    // slotted-page invariants. Its verdict comes back through the channel.
    let stop = Arc::new(AtomicBool::new(false));
    let (tx, rx) = mpsc::channel();
    // Start gate: the reader signals after its FIRST completed read pass, and
    // the main thread only reclaims after receiving it. Without this gate a
    // release-build reclaim of a 4-tuple page can finish before the reader is
    // ever scheduled, `stop` is already set when the loop first runs, and the
    // `reads > 0` assertion flakes on a zero-iteration pass (observed: solo
    // release 10 runs, 4 failures — test-design defect, not a product bug).
    let (started_tx, started_rx) = mpsc::channel();
    let reader = {
        let pool = Arc::clone(fx.engine.buffer_pool());
        let stop = Arc::clone(&stop);
        let kill_slots = kill_slots.clone();
        thread::spawn(move || {
            let mut reads = 0u64;
            let mut started_tx = Some(started_tx);
            while !stop.load(Ordering::Relaxed) {
                let guard = pool.pin(page_id).unwrap();
                let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
                let present: Vec<bool> = kill_slots
                    .iter()
                    .map(|&s| SlottedPage::tuple(page, s).unwrap().is_some())
                    .collect();
                if present[0] != present[1] {
                    let _ = tx.send(Err(format!(
                        "half-compacted page observed: kill slots in mixed states {present:?}"
                    )));
                    return;
                }
                pg_am_heap::slotted_page::debug_assert_invariants(page);
                drop(guard);
                reads += 1;
                if let Some(started) = started_tx.take() {
                    let _ = started.send(());
                }
            }
            let _ = tx.send(Ok(reads));
        })
    };

    // Wait (watchdog-guarded) for the reader's first completed pass, THEN
    // reclaim on the main thread while the reader spins on the same page.
    started_rx
        .recv_timeout(WATCHDOG)
        .unwrap_or_else(|e| panic!("reader never completed a first read after {WATCHDOG:?}: {e}"));
    fx.heap.reclaim(fx.rel(), &fx.dead()).unwrap();
    stop.store(true, Ordering::Relaxed);

    // Watchdog: the reader's verdict must arrive promptly; a trip FAILS the
    // test instead of hanging.
    let verdict = rx
        .recv_timeout(WATCHDOG)
        .unwrap_or_else(|e| panic!("reader watchdog tripped after {WATCHDOG:?}: {e}"));
    reader.join().expect("reader thread panicked");
    let reads = verdict.unwrap_or_else(|err| panic!("{err}"));
    assert!(reads > 0, "the reader must have observed the page");

    // Post-state: both kill slots are gone, the survivors intact.
    let guard = fx.engine.buffer_pool().pin(page_id).unwrap();
    let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
    assert_eq!(SlottedPage::tuple(page, kill_slots[0]).unwrap(), None);
    assert_eq!(SlottedPage::tuple(page, kill_slots[1]).unwrap(), None);
    assert!(SlottedPage::tuple(page, tids[0].slot_id).unwrap().is_some());
    assert!(SlottedPage::tuple(page, tids[3].slot_id).unwrap().is_some());
    drop(guard);
    assert_eq!(fx.visible_ids(), vec![0, 3]);
    fx.engine.shutdown();
}
