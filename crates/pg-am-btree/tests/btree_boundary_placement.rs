//! TEMP-DEBUG: deterministic single-threaded repro of the stale-prev
//! boundary misplacement behind the concurrent duplicate-run disorder.

use std::sync::Arc;

use pg_am_btree::{BTreeAM, BTreeIndex};

use pg_am_heap::tuple::ColumnType;
use pg_storage::config::StorageConfig;
use pg_storage::engine::StorageEngine;
use pg_storage::types::{Oid, PageId, Tid};

use tempfile::TempDir;

const REL_OID: Oid = Oid(16_404);

fn tid(i: u64) -> Tid {
    Tid {
        page_id: PageId(9_000_000 + i / 60_000),
        slot_id: (i % 60_000) as u16,
    }
}

fn key(i: i32) -> Vec<u8> {
    pg_am_btree::key::encode_i32(i).to_vec()
}

#[test]
fn stale_prev_boundary_misplacement() {
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());
    let engine = StorageEngine::open(tmp.path(), &config).unwrap();
    let am = BTreeAM::new(
        Arc::clone(engine.buffer_pool()),
        Arc::clone(engine.wal_writer()),
    );
    let mut index: BTreeIndex = am.create_index(REL_OID, ColumnType::Int4).unwrap();

    // 106 entries: keys 0..=102 (tid = key) plus a key-51 run with big tids.
    // Slot layout: 0..=50 = keys 0..=50; 51 = (51,51); 52 = (51,9000);
    // 53 = (51,9001); 54 = (51,9002); 55..=105 = keys 52..=102.
    for i in 0..=102u64 {
        index.insert(&key(i as i32), tid(i)).unwrap();
    }
    for t in [9001u64, 9002] {
        index.insert(&key(51), tid(t)).unwrap();
    }
    index.insert(&key(51), tid(9000)).unwrap();
    // ^ inserts keep (key, tid) order: (51,51) < (51,9000) < (51,9001) < (51,9002)

    // Forced median split #1 (public crash-test steps): 106 entries split at
    // slot 53: L = slots 0..=52 (keys 0..=50 + (51,51) + (51,9000)),
    // R = [(51,9001), (51,9002), keys 52..=102], sep(R) = 51.
    let st = index.split_prepare(index.root_page()).unwrap();
    index.split_copy(&st).unwrap();
    let mut path = Vec::new();
    index.split_commit(&st, &mut path).unwrap();
    assert_eq!(index.tree_level(), 1);
    let left = st.left;

    // Forced median split #2 of L (53 entries, split at slot 26): M gets
    // keys 26..=50 plus (51,51) and (51,9000) — so M.last = (51,9000).
    // Chain L -> M -> R, but R.prev stays L (Prepare never re-points
    // old_next.prev): the stale link.
    let st2 = index.split_prepare(left).unwrap();
    index.split_copy(&st2).unwrap();
    let mut path2 = vec![index.root_page()];
    index.split_commit(&st2, &mut path2).unwrap();

    // The probe: (51,4000) sorts between M's (51,51) and M.last =
    // (51,9000) — the correct position is INSIDE M. Pre-fix the placement
    // walk consults R.prev = L (stale, skipping M): L.last = (25,25) <
    // (51,4000) -> STAY -> inserts into R's slot 0, leaving
    // M.last = (51,9000) > R.first = (51,4000): the run's chain order is
    // broken and lookup_all returns 9000 before 4000.
    index.insert(&key(51), tid(4000)).unwrap();

    let all = index.lookup_all(&key(51)).unwrap();
    let want = vec![tid(51), tid(4000), tid(9000), tid(9001), tid(9002)];
    assert_eq!(all, want, "key 51 run out of order across the boundary");
    index.validate().unwrap();
}

/// Regression for the concurrent lost-key family root cause: a slot-0
/// (left-edge) insert whose placement verdict went STALE across
/// `pin_leaf_for_insert`'s drop-and-re-latch window used to land at slot 0
/// even after a concurrent insert claimed the page's left edge in the
/// window — breaking the page's `(key, tid)` order (validate's "entries
/// out of order"), which then stranded keys across splits ("key lost
/// across the split", "scanner missed committed key") and inverted
/// duplicate-run tid order.
///
/// Deterministic: thread A inserts the LARGER key into the empty root
/// leaf, which routes through the slot-0 protocol; the test hook parks A
/// inside the unlatched window until the main thread has landed the
/// SMALLER key — strictly inside the window. Pre-fix, A then inserts at
/// the stale slot 0 and `validate` fails with "entries out of order at
/// slot 1"; post-fix, A's re-validation restarts the placement and key 1
/// lands at slot 1.
#[test]
fn slot0_insert_revalidates_after_relatch_window() {
    use pg_am_btree::index::{SLOT0_WINDOW_ENTERED, SLOT0_WINDOW_PARK, SLOT0_WINDOW_RELEASE};
    use std::sync::atomic::Ordering;
    use std::time::{Duration, Instant};

    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());
    let engine = StorageEngine::open(tmp.path(), &config).unwrap();
    let meta_page = {
        let am = BTreeAM::new(
            Arc::clone(engine.buffer_pool()),
            Arc::clone(engine.wal_writer()),
        );
        am.create_index(REL_OID, ColumnType::Int4)
            .unwrap()
            .meta_page()
    };

    SLOT0_WINDOW_ENTERED.store(0, Ordering::SeqCst);
    SLOT0_WINDOW_RELEASE.store(false, Ordering::SeqCst);

    // Thread A inserts the LARGER key: it lands at slot 0 of the empty
    // root leaf and parks in the drop-and-re-latch window holding NO latch.
    let pool = Arc::clone(engine.buffer_pool());
    let wal = Arc::clone(engine.wal_writer());
    let a = std::thread::spawn(move || {
        SLOT0_WINDOW_PARK.with(|c| c.set(true));
        let am = BTreeAM::new(pool, wal);
        let mut index = am.open_index(REL_OID, meta_page, ColumnType::Int4).unwrap();
        index.insert(&key(1), tid(1)).unwrap();
    });

    // Wait (bounded) for A to park inside the window, then land the
    // SMALLER key — strictly inside A's unlatched window.
    let deadline = Instant::now() + Duration::from_secs(30);
    while SLOT0_WINDOW_ENTERED.load(Ordering::SeqCst) == 0 {
        assert!(
            Instant::now() < deadline,
            "thread A never reached the slot-0 re-latch window"
        );
        std::thread::yield_now();
    }
    {
        let am = BTreeAM::new(
            Arc::clone(engine.buffer_pool()),
            Arc::clone(engine.wal_writer()),
        );
        let mut index = am.open_index(REL_OID, meta_page, ColumnType::Int4).unwrap();
        index.insert(&key(0), tid(0)).unwrap();
    }
    SLOT0_WINDOW_RELEASE.store(true, Ordering::SeqCst);
    a.join().unwrap();

    let am = BTreeAM::new(
        Arc::clone(engine.buffer_pool()),
        Arc::clone(engine.wal_writer()),
    );
    let index = am.open_index(REL_OID, meta_page, ColumnType::Int4).unwrap();
    assert_eq!(index.lookup(&key(0)).unwrap(), Some(tid(0)));
    assert_eq!(index.lookup(&key(1)).unwrap(), Some(tid(1)));
    index.validate().unwrap();
}
