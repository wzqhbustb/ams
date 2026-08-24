//! Stage M acceptance crash tests (coding-plan Stage M):
//! `test_btree_split_crash_after_prepare` / `..._after_copy` /
//! `..._after_commit`.
//!
//! Each test drives a root-leaf split one WAL step at a time through the
//! internal step API (`split_prepare` / `split_copy` / `split_commit`),
//! abandons the engine mid-protocol (`mem::forget`: no checkpoint, no
//! shutdown — a kill -9), then reopens with the B+Tree redo handlers and
//! asserts:
//!
//! - the tree is structurally sound for the reached step: the `btpo_next`
//!   chain walks end to end, and (after Commit) the strict
//!   [`BTreeIndex::validate`] passes;
//! - no key is lost or duplicated: the full leaf-chain scan equals the
//!   inserted set and every point lookup hits (Blink right hops reach keys
//!   on a right sibling whose downlink was never committed);
//! - replay is idempotent: crashing *again* after recovery and replaying
//!   the same records a second time converges to the same state.

use std::path::Path;
use std::sync::Arc;

use pg_am_btree::page::{BtreePage, BTREE_FLAG_SPLIT_INCOMPLETE};
use pg_am_btree::{btree_redo_handlers, BTreeAM, BTreeIndex, BTreeUndoHandler};

use pg_am_heap::slotted_page::SlottedPage;
use pg_am_heap::tuple::ColumnType;
use pg_am_heap::HeapUndoHandler;
use pg_storage::config::StorageConfig;
use pg_storage::engine::StorageEngine;
use pg_storage::types::{Oid, PageId, Tid, PAGE_SIZE};

use tempfile::TempDir;

const REL_OID: Oid = Oid(16_388);

fn tid(i: u64) -> Tid {
    Tid {
        page_id: PageId(42_000 + i),
        slot_id: i as u16,
    }
}

fn key(i: i32) -> Vec<u8> {
    pg_am_btree::encode_i32(i).to_vec()
}

fn decode(rows: &[(Vec<u8>, Tid)]) -> Vec<i32> {
    rows.iter()
        .map(|(k, _)| pg_am_btree::decode_i32(k.clone().try_into().unwrap()))
        .collect()
}

/// Create an index and fill its root leaf until the next entry would no
/// longer fit (the next insert would split). Returns the key count.
fn create_and_fill(dir: &Path, config: &StorageConfig) -> (StorageEngine, BTreeIndex, i32) {
    let engine = StorageEngine::open(dir, config).unwrap();
    let am = BTreeAM::new(
        Arc::clone(engine.buffer_pool()),
        Arc::clone(engine.wal_writer()),
    );
    let mut index = am.create_index(REL_OID, ColumnType::Int4).unwrap();
    // i32 key (4B) + tid trailer (10B) + line pointer (4B).
    const ENTRY_BYTES: usize = 4 + 10 + 4;
    let mut n = 0i32;
    while index.page_free_space(index.root_page()).unwrap() >= ENTRY_BYTES {
        index.insert(&key(n), tid(n as u64)).unwrap();
        n += 1;
    }
    assert!(n > 100, "a page must hold a meaningful number of entries");
    (engine, index, n)
}

/// Reopen after the simulated crash with the B+Tree redo handlers
/// registered, and rebuild the index handle from the meta page.
fn recover(dir: &Path, config: &StorageConfig, meta_page: PageId) -> (StorageEngine, BTreeIndex) {
    let engine = StorageEngine::open_with_redo_handlers(
        dir,
        config,
        btree_redo_handlers(),
        vec![Box::new(HeapUndoHandler), Box::new(BTreeUndoHandler)],
    )
    .unwrap();
    let am = BTreeAM::new(
        Arc::clone(engine.buffer_pool()),
        Arc::clone(engine.wal_writer()),
    );
    let index = am.open_index(REL_OID, meta_page, ColumnType::Int4).unwrap();
    (engine, index)
}

/// The no-key-lost-no-key-duplicated assertion: the full leaf-chain scan
/// equals `0..n` in order and every point lookup hits.
fn assert_all_keys(index: &BTreeIndex, n: i32) {
    let rows = index.range_scan(None, None).unwrap();
    let got = decode(&rows);
    let want: Vec<i32> = (0..n).collect();
    assert_eq!(got, want, "leaf chain must hold every key exactly once");
    for i in 0..n {
        assert_eq!(
            index.lookup(&key(i)).unwrap(),
            Some(tid(i as u64)),
            "key {i} must be reachable by point lookup"
        );
    }
}

/// Walk the `btpo_next` chain from `start`, returning the page ids.
fn chain_from(engine: &StorageEngine, start: PageId) -> Vec<PageId> {
    let mut out = vec![start];
    loop {
        let cur = *out.last().unwrap();
        let guard = engine.buffer_pool().pin(cur).unwrap();
        let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
        let next = BtreePage::next(page).unwrap();
        drop(guard);
        if next == PageId::INVALID {
            return out;
        }
        out.push(next);
        assert!(out.len() < 1000, "sibling chain cycle");
    }
}

/// Read `btpo_flags` / slot count / pd_lsn of a page.
fn page_state(engine: &StorageEngine, page_id: PageId) -> (u8, usize, pg_storage::types::Lsn) {
    let guard = engine.buffer_pool().pin(page_id).unwrap();
    let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
    (
        BtreePage::flags(page).unwrap(),
        SlottedPage::slot_count(page),
        pg_storage::page::page_pd_lsn(page),
    )
}

/// Crash with only `BTreeSplitPrepare` durable: the left page is marked
/// SPLIT_INCOMPLETE and linked to an initialized but empty right page.
/// Recovery redo replays Prepare; the undo handler finishes the split
/// (copy + downlink + commit-clear), so the recovered tree is fully valid.
#[test]
fn test_btree_split_crash_after_prepare() {
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());

    let (meta_page, n, left, right) = {
        let (engine, index, n) = create_and_fill(tmp.path(), &config);
        let left = index.root_page();
        let st = index.split_prepare(left).unwrap();
        engine.wal_writer().flush().unwrap();
        std::mem::forget(engine); // kill -9: Prepare durable, nothing else
        (index.meta_page(), n, st.left, st.right)
    };

    // Recovery 1: redo replays Prepare; undo finishes the split.
    let (engine, index) = recover(tmp.path(), &config, meta_page);
    assert_all_keys(&index, n);
    assert_ne!(index.root_page(), left, "undo must create a new root");
    assert_eq!(index.tree_level(), 1);
    assert_eq!(
        chain_from(&engine, left),
        vec![left, right],
        "the sibling chain must walk left -> right end to end"
    );
    let (left_flags, _, _) = page_state(&engine, left);
    assert_eq!(
        left_flags & BTREE_FLAG_SPLIT_INCOMPLETE,
        0,
        "undo must clear SPLIT_INCOMPLETE"
    );
    index
        .validate()
        .unwrap_or_else(|e| panic!("recovered tree must validate: {e}"));

    // Snapshot the post-undo page state so recovery 2 can be compared against
    // it: replaying the CLR must not move the entries a second time.
    let new_root = index.root_page();
    let r1_state = [
        page_state(&engine, left),
        page_state(&engine, right),
        page_state(&engine, new_root),
    ];

    std::mem::forget(engine); // crash again, mid/after recovery

    // Recovery 2: CLR replay is idempotent — same state.
    let (engine, index) = recover(tmp.path(), &config, meta_page);
    assert_all_keys(&index, n);
    assert_eq!(index.tree_level(), 1);
    assert_eq!(index.root_page(), new_root);
    assert_eq!(
        [
            page_state(&engine, left),
            page_state(&engine, right),
            page_state(&engine, new_root),
        ],
        r1_state,
        "replaying the CLR must converge on the state undo produced"
    );
    let (left_flags, _, _) = page_state(&engine, left);
    assert_eq!(left_flags & BTREE_FLAG_SPLIT_INCOMPLETE, 0);
    drop(engine);
}

/// Crash with `BTreeSplitPrepare` + `BTreeSplitCopy` durable: the left page
/// is truncated, the right page holds the upper half, but no downlink was
/// ever committed (and, being a root split, the new root was never
/// created). Recovery redo replays Prepare+Copy; the undo handler finishes
/// the split (downlink + commit-clear), so the recovered tree is fully valid.
#[test]
fn test_btree_split_crash_after_copy() {
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());

    let (meta_page, n, left, right, copy_start_slot) = {
        let (engine, index, n) = create_and_fill(tmp.path(), &config);
        let left = index.root_page();
        let st = index.split_prepare(left).unwrap();
        index.split_copy(&st).unwrap();
        engine.wal_writer().flush().unwrap();
        std::mem::forget(engine); // kill -9: Prepare + Copy durable
        (index.meta_page(), n, st.left, st.right, st.copy_start_slot)
    };

    // Recovery 1: redo replays Prepare+Copy; undo finishes the split.
    let (engine, index) = recover(tmp.path(), &config, meta_page);
    assert_all_keys(&index, n);
    assert_ne!(index.root_page(), left, "undo must create a new root");
    assert_eq!(index.tree_level(), 1);
    assert_eq!(chain_from(&engine, left), vec![left, right]);
    let (left_flags, left_slots, _) = page_state(&engine, left);
    assert_eq!(left_flags & BTREE_FLAG_SPLIT_INCOMPLETE, 0);
    assert_eq!(left_slots, copy_start_slot as usize);
    let (_, right_slots, _) = page_state(&engine, right);
    assert_eq!(right_slots, n as usize - copy_start_slot as usize);
    std::mem::forget(engine); // crash again

    // Recovery 2: CLR replay is idempotent — same state.
    let (engine, index) = recover(tmp.path(), &config, meta_page);
    assert_all_keys(&index, n);
    assert_eq!(index.tree_level(), 1);
    let (left_flags, left_slots, _) = page_state(&engine, left);
    assert_eq!(left_flags & BTREE_FLAG_SPLIT_INCOMPLETE, 0);
    assert_eq!(left_slots, copy_start_slot as usize);
    let (_, right_slots, _) = page_state(&engine, right);
    assert_eq!(right_slots, n as usize - copy_start_slot as usize);
    drop(engine);
}

/// Crash with the full split (Prepare + Copy + Commit) durable: recovery
/// must restore a fully valid two-level tree — new root from the meta page,
/// downlinks in place, no SPLIT_INCOMPLETE left behind.
#[test]
fn test_btree_split_crash_after_commit() {
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());

    let (meta_page, n, left, right) = {
        let (engine, mut index, n) = create_and_fill(tmp.path(), &config);
        let left = index.root_page();
        let st = index.split_prepare(left).unwrap();
        index.split_copy(&st).unwrap();
        // The split page is the root: an empty path drives the new-root
        // branch of Commit.
        index.split_commit(&st, &mut Vec::new()).unwrap();
        assert_eq!(index.tree_level(), 1);
        engine.wal_writer().flush().unwrap();
        std::mem::forget(engine); // kill -9: the whole split is durable
        (index.meta_page(), n, st.left, st.right)
    };

    // Recovery 1.
    let (engine, index) = recover(tmp.path(), &config, meta_page);
    assert_eq!(
        index.tree_level(),
        1,
        "meta page must point at the new root"
    );
    assert_ne!(index.root_page(), left);
    assert_all_keys(&index, n);
    index
        .validate()
        .unwrap_or_else(|e| panic!("recovered tree must pass strict validation: {e}"));
    assert_eq!(chain_from(&engine, left), vec![left, right]);
    let (left_flags, _, _) = page_state(&engine, left);
    assert_eq!(left_flags & BTREE_FLAG_SPLIT_INCOMPLETE, 0);
    // The root holds exactly two downlinks.
    let (_, root_slots, _) = page_state(&engine, index.root_page());
    assert_eq!(root_slots, 2);
    std::mem::forget(engine); // crash again

    // Recovery 2: idempotent re-replay of Prepare/Copy/Commit.
    let (engine, index) = recover(tmp.path(), &config, meta_page);
    assert_eq!(index.tree_level(), 1);
    assert_all_keys(&index, n);
    index.validate().unwrap();
    let (_, root_slots, _) = page_state(&engine, index.root_page());
    assert_eq!(root_slots, 2, "Commit redo must not duplicate the downlink");
    drop(engine);
}

// ---------------------------------------------------------------------
// Stage Q review (F1): delete's WAL record must name the re-validated page
// ---------------------------------------------------------------------

/// A delete that right-hops onto a split twin must WAL-log the TWIN's page
/// id, not the descent's stale one — redo keys records off `rec.page_id`,
/// so the stale id loses the delete (or removes an innocent entry).
///
/// Drive an inserter that keeps splitting the right-edge leaf while a
/// deleter removes EVERY OTHER key right at that frontier (oversized Text
/// keys: ~13 entries/leaf, so splits fire every few inserts and interpose
/// between the deleter's descent and its leaf latch with high probability),
/// then kill -9 and recover: every deleted key must STAY deleted and every
/// surviving key must be intact. (The online pre-crash state would pass
/// even with the bug — the wrong page id only bites at redo time, which is
/// why this test goes through a crash.)
///
/// Why every-other-key: deleting ALL of a leaf's entries leaves a page with
/// ≤1 live entries whose tuple bytes are dead space (M2b reclaims it only
/// via a split, and a split refuses < 2 entries) — a pre-existing Stage M
/// boundary, out of scope here. Deleting half keeps every leaf splittable
/// while exercising the same hop window.
#[test]
fn test_concurrent_split_delete_crash_recovers_deletions() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    const REL_OID_TEXT: Oid = Oid(16_389);
    /// ~600-byte keys: ~13 entries per 8 KB leaf, so the frontier splits
    /// constantly while the deleter works it.
    fn big_key(i: u64) -> Vec<u8> {
        let mut k = format!("{i:06}").into_bytes();
        k.resize(600, b'x');
        k
    }
    /// The deleter removes even race keys; odd ones survive.
    fn deleted(i: u64) -> bool {
        i % 2 == 0
    }

    const SEED: u64 = 120; // pre-existing keys (untouched by the race)
    const RACE: u64 = 240; // keys inserted at the frontier; half deleted

    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());

    let meta_page = {
        let engine = Arc::new(StorageEngine::open(tmp.path(), &config).unwrap());
        let meta_page = {
            let am = BTreeAM::new(
                Arc::clone(engine.buffer_pool()),
                Arc::clone(engine.wal_writer()),
            );
            let mut index = am.create_index(REL_OID_TEXT, ColumnType::Text).unwrap();
            for i in 0..SEED {
                index.insert(&big_key(i), tid(i)).unwrap();
            }
            index.meta_page()
        };

        let watermark = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = mpsc::channel();
        {
            let engine = Arc::clone(&engine);
            let watermark = Arc::clone(&watermark);
            thread::spawn(move || {
                let inserter = {
                    let engine = Arc::clone(&engine);
                    let watermark = Arc::clone(&watermark);
                    thread::spawn(move || {
                        let am = BTreeAM::new(
                            Arc::clone(engine.buffer_pool()),
                            Arc::clone(engine.wal_writer()),
                        );
                        let mut index = am
                            .open_index(REL_OID_TEXT, meta_page, ColumnType::Text)
                            .unwrap();
                        for i in 0..RACE {
                            let k = SEED + i;
                            index.insert(&big_key(k), tid(k)).unwrap();
                            watermark.store((k + 1) as usize, Ordering::SeqCst);
                        }
                    })
                };
                let deleter = {
                    let engine = Arc::clone(&engine);
                    let watermark = Arc::clone(&watermark);
                    thread::spawn(move || {
                        let am = BTreeAM::new(
                            Arc::clone(engine.buffer_pool()),
                            Arc::clone(engine.wal_writer()),
                        );
                        let mut index = am
                            .open_index(REL_OID_TEXT, meta_page, ColumnType::Text)
                            .unwrap();
                        for i in (0..RACE).filter(|i| deleted(*i)) {
                            let k = SEED + i;
                            // Delete right behind the insertion frontier:
                            // the target leaf is the one being split.
                            while (watermark.load(Ordering::SeqCst) as u64) <= k {
                                thread::yield_now();
                            }
                            index.delete(&big_key(k), tid(k)).unwrap();
                        }
                    })
                };
                inserter.join().unwrap();
                deleter.join().unwrap();
                let _ = tx.send(());
            });
        }
        rx.recv_timeout(Duration::from_secs(300))
            .expect("concurrent insert+delete deadlocked or ran too long");

        // Online sanity: deleted race keys are gone, survivors intact.
        {
            let am = BTreeAM::new(
                Arc::clone(engine.buffer_pool()),
                Arc::clone(engine.wal_writer()),
            );
            let index = am
                .open_index(REL_OID_TEXT, meta_page, ColumnType::Text)
                .unwrap();
            for i in 0..RACE {
                let k = SEED + i;
                let expect = if deleted(i) { None } else { Some(tid(k)) };
                assert_eq!(index.lookup(&big_key(k)).unwrap(), expect, "key {k}");
            }
            for i in 0..SEED {
                assert_eq!(index.lookup(&big_key(i)).unwrap(), Some(tid(i)));
            }
        }

        engine.wal_writer().flush().unwrap();
        std::mem::forget(engine); // kill -9
        meta_page
    };

    // Recovery: redo replays the BTreeDelete records against the pages they
    // name. With the wrong page id (F1), a delete is lost (race key still
    // present) or mis-applied to an innocent slot (seed key missing).
    let (_engine, index) = recover(tmp.path(), &config, meta_page);
    let survivors: Vec<u64> = (0..SEED)
        .chain((0..RACE).filter(|i| !deleted(*i)).map(|i| SEED + i))
        .collect();
    let rows = index.range_scan(None, None).unwrap();
    assert_eq!(
        rows.len(),
        survivors.len(),
        "exactly the seed + odd race keys must survive recovery"
    );
    for (expect, (k, t)) in survivors.iter().zip(rows.iter()) {
        assert_eq!(k.as_slice(), big_key(*expect).as_slice());
        assert_eq!(*t, tid(*expect));
    }
    for i in (0..RACE).filter(|i| deleted(*i)) {
        assert_eq!(
            index.lookup(&big_key(SEED + i)).unwrap(),
            None,
            "deleted race key {} reappeared after recovery",
            SEED + i
        );
    }
    index.validate().unwrap();
}

// ---------------------------------------------------------------------
// Stage Q review (H2): root split on a freelist-recycled page needs an FPI
// ---------------------------------------------------------------------

/// A root split that reuses a FREELIST-RECYCLED page must recover cleanly.
/// The recycled page's on-disk image is its previous tenant's bytes
/// (`pd_upper != 0`), so without `create_new_root`'s post-image FPI the
/// seed `BTreeInsert`'s `init_if_fresh` would not fire and redo would apply
/// the insert onto garbage geometry (silently corrupting the root — or
/// failing recovery outright).
///
/// Drive: manufacture two "previous tenant" pages with heap-style geometry,
/// flush + free them, then fill the root leaf until the root split pops the
/// freelist (LIFO: the right twin gets one, the NEW ROOT gets the other).
/// kill -9, recover, and require a fully intact tree.
#[test]
fn test_root_split_on_recycled_page_crash_recovers() {
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());

    let (meta_page, n, recycled_root) = {
        let engine = StorageEngine::open(tmp.path(), &config).unwrap();
        let am = BTreeAM::new(
            Arc::clone(engine.buffer_pool()),
            Arc::clone(engine.wal_writer()),
        );
        let mut index = am.create_index(REL_OID, ColumnType::Int4).unwrap();

        // Two "previous tenants": heap-initialized pages (pd_upper ==
        // PAGE_SIZE, no btree special space) made durable, then freed.
        // Freelist pop is LIFO: the split's right twin gets `victim2`, the
        // NEW ROOT gets `victim1`.
        let mut freed = Vec::new();
        for _ in 0..2 {
            let victim = {
                let mut guard = engine.buffer_pool().new_page().unwrap();
                SlottedPage::init(guard.page_mut().try_into().unwrap());
                guard.page_id()
            };
            engine.buffer_pool().flush(victim).unwrap();
            freed.push(victim);
        }
        {
            let mut allocator = engine.page_allocator().lock();
            for victim in &freed {
                allocator.free_page(*victim).unwrap();
            }
        }

        // Fill the root leaf; the next insert triggers the root split.
        const ENTRY_BYTES: usize = 4 + 10 + 4;
        let mut i = 0i32;
        while index.page_free_space(index.root_page()).unwrap() >= ENTRY_BYTES {
            index.insert(&key(i), tid(i as u64)).unwrap();
            i += 1;
        }
        index.insert(&key(i), tid(i as u64)).unwrap();
        let n = i + 1;
        assert_eq!(index.tree_level(), 1, "the root must have been promoted");
        assert_eq!(
            index.root_page(),
            freed[0],
            "the new root must be the recycled page (freelist is LIFO)"
        );
        engine.wal_writer().flush().unwrap();
        std::mem::forget(engine); // kill -9
        (index.meta_page(), n, freed[0])
    };

    // Recovery: without the FPI, redo's init_if_fresh sees the recycled
    // page's non-zero pd_upper and skips initialization — the seed insert
    // then lands on heap garbage geometry.
    let (_engine, index) = recover(tmp.path(), &config, meta_page);
    assert_eq!(index.root_page(), recycled_root);
    assert_eq!(index.tree_level(), 1);
    assert_all_keys(&index, n);
    index
        .validate()
        .unwrap_or_else(|e| panic!("recycled-root tree must validate after recovery: {e}"));
}

// ---------------------------------------------------------------------
// Stage T P0: split Commit's cycle FPIs must precede the Commit record
// ---------------------------------------------------------------------

/// Regression for the Stage T stress-harness P0 (concurrent checkpoint +
/// split commit): `split_commit` used to append the `BTreeSplitCommit`
/// record FIRST and only then `pin_mut` the parent and the left page. When
/// a checkpoint had opened a new FPI cycle between the split's Copy and its
/// Commit, those `pin_mut`s fired the pages' cycle FPIs at LSNs AFTER the
/// Commit record, capturing PRE-commit images. Recovery replays an FPI
/// unconditionally and patches `pd_lsn` to the FPI's LSN, so the Commit
/// redo's `pd_lsn` guard then skipped the downlink insert / flag clear —
/// resurrecting `SPLIT_INCOMPLETE` on an already-committed split while the
/// parent kept (or lost) its downlink, and the undo-time page scan emitted
/// a spurious finishing CLR with a duplicate downlink.
///
/// The test forces the exact interleaving deterministically, no threads:
///
/// 1. build a two-level tree (root split via the normal insert path);
/// 2. Prepare+Copy a split of EACH leaf (both `SPLIT_INCOMPLETE`);
/// 3. flush the tree pages (they now have on-disk images, `needs_fpi`) and
///    publish a checkpoint LSN past every page's `pd_lsn` — the next
///    modification of each page owes a cycle FPI;
/// 4. Commit both splits through the step API.
///
/// WAL assertions (the invariant): every Commit is preceded by the cycle
/// FPI of each page it modifies, and no FPI of those pages appears after
/// its Commit. Then a kill -9 + recovery proves the end-to-end consequence:
/// the tree must validate with every key reachable.
#[test]
fn test_btree_split_commit_fpi_precedes_commit_record() {
    use pg_storage::wal::reader::WalReader;
    use pg_storage::wal::record::{BTreeSplitCommitRecord, FullPageImageRecord, WalRecordType};

    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());

    let (meta_page, n, l0, l1, root) = {
        let (engine, mut index, n) = create_and_fill(tmp.path(), &config);
        let l0 = index.root_page();
        // Split the full root leaf through the normal insert path: the root
        // is promoted to an internal page over leaves [l0, l1].
        index.insert(&key(n), tid(n as u64)).unwrap();
        let n = n + 1;
        assert_eq!(index.tree_level(), 1, "the root must have been promoted");
        let root = index.root_page();
        let l1 = chain_from(&engine, l0)[1];

        // Prepare + Copy a split of each leaf, pre-checkpoint.
        let st_right = index.split_prepare(l1).unwrap();
        index.split_copy(&st_right).unwrap();
        let st_left = index.split_prepare(l0).unwrap();
        index.split_copy(&st_left).unwrap();

        // Give the tree pages on-disk images (needs_fpi) and open a new FPI
        // cycle past every page's pd_lsn — the exact state a concurrent
        // checkpoint creates between a split's Copy and its Commit.
        let pool = Arc::clone(engine.buffer_pool());
        for p in [l0, l1, root] {
            pool.flush(p).unwrap();
        }
        let ckpt = engine.wal_writer().current_lsn();
        pool.set_checkpoint_lsn(ckpt);

        // Commit both splits. Each Commit modifies the root (downlink) and
        // its left page (flag clear); each owes those pages' cycle FPIs.
        index.split_commit(&st_right, &mut vec![root]).unwrap();
        index.split_commit(&st_left, &mut vec![root]).unwrap();

        // WAL invariant: for each Commit, the cycle FPI of every page it
        // modifies precedes the Commit record, and no FPI of those pages
        // follows it. (Pre-fix, the FPIs landed right AFTER their Commit.)
        engine.wal_writer().flush().unwrap();
        let mut reader = WalReader::open(tmp.path().join("wal"), config.wal_segment_size).unwrap();
        let mut commits: Vec<(u64, BTreeSplitCommitRecord)> = Vec::new();
        let mut fpis: Vec<(u64, PageId)> = Vec::new();
        loop {
            let lsn = reader.current_lsn();
            match reader.next_record() {
                Ok(Some(rec)) => match rec.record_type {
                    WalRecordType::BTreeSplitCommit => {
                        commits
                            .push((lsn.0, BTreeSplitCommitRecord::decode(&rec.payload).unwrap()));
                    }
                    WalRecordType::FullPageImage => {
                        let fpi: FullPageImageRecord = bincode::serde::decode_from_slice(
                            &rec.payload,
                            bincode::config::standard(),
                        )
                        .unwrap()
                        .0;
                        fpis.push((lsn.0, fpi.page_id));
                    }
                    _ => {}
                },
                Ok(None) => break,
                Err(e) => panic!("WAL scan failed at {lsn:?}: {e}"),
            }
        }
        let fpi_after = |page: PageId, lsn: u64| {
            fpis.iter()
                .filter(|(l, p)| *p == page && *l > lsn)
                .map(|(l, _)| *l)
                .collect::<Vec<_>>()
        };
        let fpi_between = |page: PageId, lo: u64, hi: u64| {
            // `lo` inclusive: `ckpt` is the next unallocated LSN, so the
            // first post-publish append lands exactly at `lo`. `hi`
            // exclusive: the Commit itself is not an FPI.
            fpis.iter().any(|(l, p)| *p == page && *l >= lo && *l < hi)
        };
        // Note: split #1's Commit also names l0 as its left page (l0 was the
        // root leaf then) — match the LAST commit for each left page.
        let c_right = commits
            .iter()
            .rfind(|(_, c)| c.left_page == l1)
            .expect("commit for the right-leaf split")
            .0;
        let c_left = commits
            .iter()
            .rfind(|(_, c)| c.left_page == l0)
            .expect("commit for the left-leaf split")
            .0;
        assert!(ckpt.0 < c_right && c_right < c_left, "commit order");
        // The right-leaf Commit modifies root + l1: both cycle FPIs must
        // precede it, and none may follow.
        assert!(
            fpi_between(root, ckpt.0, c_right),
            "root's cycle FPI must precede its Commit"
        );
        assert!(
            fpi_between(l1, ckpt.0, c_right),
            "left page's cycle FPI must precede its Commit"
        );
        assert_eq!(
            fpi_after(root, c_right),
            Vec::<u64>::new(),
            "no root FPI may land after its split Commit (pre-fix bug shape)"
        );
        assert_eq!(
            fpi_after(l1, c_right),
            Vec::<u64>::new(),
            "no left-page FPI may land after its split Commit (pre-fix bug shape)"
        );
        // The left-leaf Commit modifies root + l0. Root was already stamped
        // this cycle by the previous Commit (no new FPI owed); l0's cycle
        // FPI must precede its Commit.
        assert!(
            fpi_between(l0, c_right, c_left),
            "left page's cycle FPI must precede its Commit"
        );
        assert_eq!(
            fpi_after(l0, c_left),
            Vec::<u64>::new(),
            "no left-page FPI may land after its split Commit (pre-fix bug shape)"
        );

        std::mem::forget(engine); // kill -9
        (index.meta_page(), n, l0, l1, root)
    };

    // End-to-end: recovery must converge on the committed two-level-plus
    // tree — every key reachable, no resurrected SPLIT_INCOMPLETE, no
    // duplicate downlink from a spurious finishing CLR.
    let (engine, index) = recover(tmp.path(), &config, meta_page);
    assert_all_keys(&index, n);
    index
        .validate()
        .unwrap_or_else(|e| panic!("recovered tree must pass strict validation: {e}"));
    for p in [l0, l1] {
        let (flags, _, _) = page_state(&engine, p);
        assert_eq!(
            flags & BTREE_FLAG_SPLIT_INCOMPLETE,
            0,
            "page {p:?} must not resurrect SPLIT_INCOMPLETE"
        );
    }
    let (_, root_slots, _) = page_state(&engine, root);
    assert_eq!(root_slots, 4, "root holds downlinks to the four leaves");
    drop(engine);
}

// ---------------------------------------------------------------------
// Stage T P0 residual: third-party writes vs an in-flight split Commit
// ---------------------------------------------------------------------

/// All `(lsn, page_id)` FullPageImage records in the WAL, in order.
fn wal_fpis(dir: &Path, config: &StorageConfig) -> Vec<(u64, PageId)> {
    use pg_storage::wal::reader::WalReader;
    use pg_storage::wal::record::{FullPageImageRecord, WalRecordType};
    let mut reader = WalReader::open(dir.join("wal"), config.wal_segment_size).unwrap();
    let mut out = Vec::new();
    loop {
        let lsn = reader.current_lsn();
        match reader.next_record() {
            Ok(Some(rec)) => {
                if rec.record_type == WalRecordType::FullPageImage {
                    let fpi: FullPageImageRecord = bincode::serde::decode_from_slice(
                        &rec.payload,
                        bincode::config::standard(),
                    )
                    .unwrap()
                    .0;
                    out.push((lsn.0, fpi.page_id));
                }
            }
            Ok(None) => break,
            Err(e) => panic!("WAL scan failed at {lsn:?}: {e}"),
        }
    }
    out
}

/// All `BTreeSplitCommit` records as `(lsn, left_page)`, in order.
fn wal_split_commits(dir: &Path, config: &StorageConfig) -> Vec<(u64, PageId)> {
    use pg_storage::wal::reader::WalReader;
    use pg_storage::wal::record::{BTreeSplitCommitRecord, WalRecordType};
    let mut reader = WalReader::open(dir.join("wal"), config.wal_segment_size).unwrap();
    let mut out = Vec::new();
    loop {
        let lsn = reader.current_lsn();
        match reader.next_record() {
            Ok(Some(rec)) => {
                if rec.record_type == WalRecordType::BTreeSplitCommit {
                    let c = BTreeSplitCommitRecord::decode(&rec.payload).unwrap();
                    out.push((lsn.0, c.left_page));
                }
            }
            Ok(None) => break,
            Err(e) => panic!("WAL scan failed at {lsn:?}: {e}"),
        }
    }
    out
}

/// Stage T P0 residual regression: a third-party leaf write that lands on a
/// SPLIT_INCOMPLETE page (its split's Commit is in flight) must proceed as
/// a designed Stage S in-window write but must NEVER emit the page's cycle
/// FPI. Pre-fix, the optimistic path's plain `pin_mut` would fire that FPI
/// in the (Commit append, Commit apply) window whenever a checkpoint
/// published after the split's pre-touch: a post-Commit FPI with a
/// pre-Commit image — the exact resurrection shape of the original P0.
///
/// Deterministic construction (no threads): drive a leaf split to Copy via
/// the step API (left page SPLIT_INCOMPLETE), open a new FPI cycle past the
/// page's pd_lsn, then run a third-party insert AND delete against the left
/// page. Both must succeed (in-window writes are designed), the page must
/// gain/lose exactly those entries, and no FPI for the page may be emitted.
/// The split then commits through the step API; crash + recover must
/// converge and validate.
#[test]
fn test_third_party_write_on_committing_leaf_escalates_without_fpi() {
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());

    let (engine, mut index, n) = create_and_fill(tmp.path(), &config);
    let l0 = index.root_page();
    // Promote the root via the normal insert path: leaves [l0, r1] under an
    // internal root.
    index.insert(&key(n), tid(n as u64)).unwrap();
    let n = n + 1;
    assert_eq!(index.tree_level(), 1);
    let root = index.root_page();

    // Drive a split of l0 to Copy: l0 is now SPLIT_INCOMPLETE, Commit never
    // issued yet.
    let st = index.split_prepare(l0).unwrap();
    index.split_copy(&st).unwrap();
    let slots_after_copy = page_state(&engine, l0).1;

    // Open a new FPI cycle past l0's pd_lsn: l0 owes a cycle FPI to the
    // next plain pin_mut.
    let pool = Arc::clone(engine.buffer_pool());
    pool.flush(l0).unwrap();
    let ckpt = engine.wal_writer().current_lsn();
    pool.set_checkpoint_lsn(ckpt);

    let fpis_before = wal_fpis(tmp.path(), &config)
        .into_iter()
        .filter(|(_, p)| *p == l0)
        .count();

    // Third-party in-window insert + delete against the SPLIT_INCOMPLETE
    // page: both must SUCCEED (Stage S designed case) while emitting NO
    // FPI for the page — a post-Commit FPI with a pre-Commit image is the
    // P0 shape.
    index.insert(&key(0), tid(900_000)).unwrap();
    index.delete(&key(0), tid(0)).unwrap();
    assert_eq!(
        page_state(&engine, l0).1,
        slots_after_copy, // +1 insert, -1 delete
        "the in-window writes must apply to the committing page"
    );
    let fpis_after = wal_fpis(tmp.path(), &config)
        .into_iter()
        .filter(|(_, p)| *p == l0)
        .count();
    assert_eq!(
        fpis_before, fpis_after,
        "third-party pins of a committing page must not emit FPIs"
    );

    // Complete the split; the tree validates with the in-window results.
    index.split_commit(&st, &mut vec![root]).unwrap();
    index.validate().unwrap();

    // WAL hygiene around the Commit: the in-window writes advanced l0's
    // pd_lsn past `ckpt`, so the Commit owes no cycle FPI here (the
    // due-FPI pre-touch shape is covered by
    // test_btree_split_commit_fpi_precedes_commit_record); the essential
    // invariant is that NO FPI of l0 may land after the Commit record.
    engine.wal_writer().flush().unwrap();
    let commits = wal_split_commits(tmp.path(), &config);
    let c = commits
        .iter()
        .rfind(|(_, left)| *left == l0)
        .expect("commit for the l0 split")
        .0;
    let fpis = wal_fpis(tmp.path(), &config);
    assert!(
        !fpis.iter().any(|(l, p)| *p == l0 && *l > c),
        "no FPI of the split page may land after its Commit record"
    );

    // Crash + recover: the tree converges (every key except the deleted
    // (key(0), tid(0)) pair, plus the duplicate (key(0), tid(900000))).
    let meta_page = index.meta_page();
    std::mem::forget(engine);
    let (_engine, index) = recover(tmp.path(), &config, meta_page);
    index
        .validate()
        .unwrap_or_else(|e| panic!("recovered tree must validate: {e}"));
    assert_eq!(index.lookup(&key(0)).unwrap(), Some(tid(900_000)));
    assert_all_keys_except_deleted(&index, n);
}

/// `assert_all_keys` variant tolerating the test's delete/duplicate:
/// expects every key 0..n present (key 0 now maps to the duplicate tid).
fn assert_all_keys_except_deleted(index: &BTreeIndex, n: i32) {
    let rows = index.range_scan(None, None).unwrap();
    assert_eq!(rows.len(), n as usize, "one delete + one duplicate insert");
    for i in 0..n {
        assert!(
            index.lookup(&key(i)).unwrap().is_some(),
            "key {i} must be reachable"
        );
    }
}

/// Guarded-path companion of
/// `test_btree_split_commit_fpi_precedes_commit_record` (which drives the
/// public step API): drives the REAL online path — `insert` → pessimistic
/// pass → `split_commit_guarded` — with a checkpoint cycle opened right
/// before the triggering insert, and asserts the Commit's WAL-order
/// invariant (cycle FPIs before the Commit record, none after).
///
/// # Why this test does not discriminate pre/post-fix by itself
///
/// In the guarded path the descent write-latches `st.left` BEFORE the
/// Commit append, so its cycle FPI fires at descent time — always ahead of
/// the Commit — and the pre-fix apply re-pin only fires a stale FPI if a
/// checkpoint publishes in the (Copy, Commit apply) slice INSIDE
/// `insert()`, which no external interposition can reach deterministically
/// (no test hook exists there). That slice is closed by construction (the
/// pre-touch fires any newly-due FPI before the append; the apply uses
/// `pin_mut_without_fpi`); the discriminating, externally-drivable proof
/// lives in the two step-API tests, which share the same fix. This test
/// remains as the online-path regression net.
#[test]
fn test_btree_split_commit_guarded_fpi_precedes_commit_record() {
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());

    let (engine, mut index, n) = create_and_fill(tmp.path(), &config);
    let l0 = index.root_page();
    // Promote the root: leaves [l0, r1] under an internal root.
    index.insert(&key(n), tid(n as u64)).unwrap();
    let mut next_tid = n as u64 + 1;
    assert_eq!(index.tree_level(), 1);
    let root = index.root_page();

    // Fill l0 to the brim with duplicates of key(0) — they provably land on
    // l0 (it owns key(0): r1's separator is the median key). Duplicates
    // order by (key, tid).
    const ENTRY_BYTES: usize = 4 + 10 + 4;
    while index.page_free_space(l0).unwrap() >= ENTRY_BYTES {
        index.insert(&key(0), tid(next_tid)).unwrap();
        next_tid += 1;
    }

    // Open a new FPI cycle past every page's pd_lsn; the split below owes
    // l0 (and the root) a cycle FPI.
    let pool = Arc::clone(engine.buffer_pool());
    pool.flush(l0).unwrap();
    pool.flush(root).unwrap();
    let ckpt = engine.wal_writer().current_lsn();
    pool.set_checkpoint_lsn(ckpt);

    // The triggering insert: l0 is full → pessimistic pass → split →
    // split_commit_guarded (the online guarded path), parent = root with
    // room for the downlink.
    index.insert(&key(0), tid(next_tid)).unwrap();

    engine.wal_writer().flush().unwrap();
    let commits = wal_split_commits(tmp.path(), &config);
    let c = commits
        .iter()
        .rfind(|(_, left)| *left == l0)
        .expect("commit for the l0 split")
        .0;
    assert!(ckpt.0 < c, "the commit must follow the checkpoint publish");
    let fpis = wal_fpis(tmp.path(), &config);
    assert!(
        fpis.iter().any(|(l, p)| *p == l0 && *l >= ckpt.0 && *l < c),
        "guarded Commit: left page's cycle FPI must precede the Commit record"
    );
    assert!(
        fpis.iter()
            .any(|(l, p)| *p == root && *l >= ckpt.0 && *l < c),
        "guarded Commit: parent cycle FPI (fired at the descent latch) must precede the Commit"
    );
    assert!(
        !fpis.iter().any(|(l, p)| *p == l0 && *l > c),
        "no FPI of the split page may land after its Commit record"
    );

    // Crash + recover: converges, validates, no resurrected flag.
    let meta_page = index.meta_page();
    std::mem::forget(engine);
    let (engine, index) = recover(tmp.path(), &config, meta_page);
    index
        .validate()
        .unwrap_or_else(|e| panic!("recovered tree must validate: {e}"));
    let (flags, _, _) = page_state(&engine, l0);
    assert_eq!(flags & BTREE_FLAG_SPLIT_INCOMPLETE, 0);
    drop(engine);
}

/// Guarded root-split Commit FPI ordering (post-Stage-T review, item 1):
/// the online root-promotion path must emit the NEW ROOT's cycle FPI before
/// the Commit record even when a checkpoint flushed the brand-new root in
/// the (create_new_root, Commit append) window. Pre-fix, the root branch's
/// apply used a plain `pin_mut(new_root)`, which — with the new root
/// flushed (`needs_fpi = true`) and a cycle published past the seed LSN —
/// fired that FPI AFTER the Commit with a pre-downlink image: FPI replay
/// would wipe the slot-1 downlink, orphaning the right twin silently (no
/// SPLIT_INCOMPLETE remains, so undo never repairs it).
///
/// Deterministic via the `SPLIT_COMMIT_ROOT_CKPT_HOOK` test hook, which
/// simulates exactly that checkpoint inside the guarded Commit on the real
/// online insert path.
#[test]
fn test_btree_split_commit_guarded_root_branch_new_root_fpi_order() {
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());

    let (engine, mut index, n) = create_and_fill(tmp.path(), &config);
    let old_root = index.root_page();
    assert_eq!(index.tree_level(), 0);

    // Arm the one-shot hook: inside the guarded root-split Commit, right
    // after create_new_root, it flushes every dirty page (the new root now
    // has an on-disk image, needs_fpi = true) and publishes the current LSN
    // as the checkpoint LSN — the exact (create_new_root, append) window.
    pg_am_btree::index::SPLIT_COMMIT_ROOT_CKPT_HOOK.with(|c| c.set(true));
    index.insert(&key(n), tid(n as u64)).unwrap(); // triggers the root split
    assert_eq!(
        index.tree_level(),
        1,
        "the insert must have promoted the root"
    );
    let new_root = index.root_page();
    assert_ne!(new_root, old_root);
    let n = n + 1;

    // WAL order: the root split's Commit C must be preceded by the new
    // root's cycle FPI (fired by the pre-touch once the hook made it due),
    // and no FPI of the new root may follow C.
    engine.wal_writer().flush().unwrap();
    let commits = wal_split_commits(tmp.path(), &config);
    let c = commits
        .iter()
        .rfind(|(_, left)| *left == old_root)
        .expect("commit for the root split")
        .0;
    let fpis = wal_fpis(tmp.path(), &config);
    assert!(
        fpis.iter().any(|(l, p)| *p == new_root && *l < c),
        "the new root's cycle FPI must precede the Commit record"
    );
    assert!(
        !fpis.iter().any(|(l, p)| *p == new_root && *l > c),
        "no FPI of the new root may land after its Commit record (pre-fix bug shape)"
    );

    // Crash + recover: the promoted tree must converge — every key
    // reachable, the right twin linked from the new root.
    let meta_page = index.meta_page();
    std::mem::forget(engine);
    let (engine, index) = recover(tmp.path(), &config, meta_page);
    assert_eq!(index.tree_level(), 1);
    assert_eq!(index.root_page(), new_root);
    assert_all_keys(&index, n);
    index
        .validate()
        .unwrap_or_else(|e| panic!("recovered tree must validate: {e}"));
    let (_, root_slots, _) = page_state(&engine, new_root);
    assert_eq!(root_slots, 2, "the new root must hold both downlinks");
    drop(engine);
}
