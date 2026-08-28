//! Stage Q concurrency tests: true latch coupling + optimistic/pessimistic
//! writes under multi-threaded access (coding-plan Stage Q acceptance:
//! `cargo test -p pg-am-btree --test btree_concurrent`).
//!
//! Every test drives one shared tree from N per-thread transient handles
//! (the same model the engine uses per DML) and asserts:
//!
//! - no deadlock (a supervisor watchdog fails the test instead of hanging,
//!   the same pattern as `cross_page_update_under_contention`);
//! - Blink no-miss reads (committed keys are always visible to later scans);
//! - exact final contents and a clean quiescent-state `validate()`.
//!
//! Thread counts and key volumes are env-tunable (`BTREE_TEST_THREADS`,
//! `BTREE_TEST_KEYS`, `BTREE_SOAK_THREADS`, `BTREE_SOAK_KEYS`) so CI stays
//! fast while soak runs can scale up to 100+ connections.
//!
//! # 1h soak (coding-plan Stage Q acceptance: 100 conn mixed INSERT + range
//! scan, no miss)
//!
//! `soak_mixed_insert_scan_no_miss` is `#[ignore]`d by default and runs for
//! `BTREE_SOAK_SECS` seconds (default 3600) with `BTREE_SOAK_THREADS`
//! connections (default 100) plus 4 scanners. Run it in release mode:
//!
//! ```sh
//! cargo test -p pg-am-btree --test btree_concurrent --release -- \
//!     --ignored soak --nocapture
//! # scaled smoke (what CI/local verification does):
//! BTREE_SOAK_SECS=60 BTREE_SOAK_THREADS=32 \
//!     cargo test -p pg-am-btree --test btree_concurrent --release -- \
//!     --ignored soak --nocapture
//! ```

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use pg_am_btree::index::SPLIT_ALLOC_FAILURES;
use pg_am_btree::key::{decode_i32, encode_i32};
use pg_am_btree::{BTreeAM, BTreeIndex};

use pg_am_heap::tuple::ColumnType;
use pg_storage::config::StorageConfig;
use pg_storage::engine::StorageEngine;
use pg_storage::types::{Oid, PageId, Tid};

use tempfile::TempDir;

const REL_OID: Oid = Oid(16_387);

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn tid(i: u64) -> Tid {
    Tid {
        page_id: PageId(9_000_000 + i / 60_000),
        slot_id: (i % 60_000) as u16,
    }
}

fn key(i: i32) -> Vec<u8> {
    encode_i32(i).to_vec()
}

fn setup() -> (TempDir, Arc<StorageEngine>, PageId) {
    setup_with_config(|_| {})
}

/// Setup with a config tweak (e.g. a tiny buffer pool).
fn setup_with_config(
    tweak: impl FnOnce(&mut StorageConfig),
) -> (TempDir, Arc<StorageEngine>, PageId) {
    let tmp = TempDir::new().unwrap();
    let mut config = StorageConfig::new(tmp.path());
    tweak(&mut config);
    let engine = Arc::new(StorageEngine::open(tmp.path(), &config).unwrap());
    let am = BTreeAM::new(
        Arc::clone(engine.buffer_pool()),
        Arc::clone(engine.wal_writer()),
    );
    let index = am.create_index(REL_OID, ColumnType::Int4).unwrap();
    (tmp, engine, index.meta_page())
}

/// Build a fresh per-thread handle (the engine's per-DML model).
fn open_handle(engine: &StorageEngine, meta_page: PageId) -> BTreeIndex {
    let am = BTreeAM::new(
        Arc::clone(engine.buffer_pool()),
        Arc::clone(engine.wal_writer()),
    );
    am.open_index(REL_OID, meta_page, ColumnType::Int4).unwrap()
}

/// Run `f` (which spawns and joins the workers) in a supervisor thread and
/// fail on timeout instead of hanging on a latch deadlock.
fn run_with_watchdog<F>(name: &str, timeout: Duration, f: F)
where
    F: FnOnce() + Send + 'static,
{
    let (tx, rx) = mpsc::channel();
    let name = name.to_string();
    thread::spawn(move || {
        f();
        let _ = tx.send(());
    });
    match rx.recv_timeout(timeout) {
        Ok(()) => {}
        Err(mpsc::RecvTimeoutError::Timeout) => panic!("{name}: deadlocked or ran too long"),
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            panic!("{name}: a worker thread panicked (see above)")
        }
    }
}

/// N threads insert DISJOINT ascending key ranges into one tree; afterwards
/// every key must point-lookup to its own TID and the tree must validate.
#[test]
fn concurrent_disjoint_inserts_all_found() {
    let threads = env_usize("BTREE_TEST_THREADS", 12);
    let per_thread = env_usize("BTREE_TEST_KEYS", 1_500);

    let (_tmp, engine, meta_page) = setup();
    let engine2 = Arc::clone(&engine);
    run_with_watchdog("disjoint inserts", Duration::from_secs(300), move || {
        let mut handles = Vec::new();
        for t in 0..threads {
            let engine = Arc::clone(&engine2);
            handles.push(thread::spawn(move || {
                let mut index = open_handle(&engine, meta_page);
                let base = (t * per_thread) as i32;
                for i in 0..per_thread {
                    let k = base + i as i32;
                    index.insert(&key(k), tid(k as u64)).unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    });

    let index = open_handle(&engine, meta_page);
    let total = (threads * per_thread) as i32;
    assert!(index.tree_level() >= 1, "18k keys must have split the root");
    for i in 0..total {
        assert_eq!(
            index.lookup(&key(i)).unwrap(),
            Some(tid(i as u64)),
            "key {i} must be found"
        );
    }
    index.validate().unwrap();
}

/// Writers insert disjoint ranges while scanners repeatedly range-scan: a
/// scanner must never miss a key whose insert returned BEFORE its scan
/// started (tracked via a shared committed-set snapshot taken first).
#[test]
fn concurrent_insert_and_range_scan_no_miss() {
    let writers = env_usize("BTREE_TEST_THREADS", 8);
    let per_writer = env_usize("BTREE_TEST_KEYS", 800);
    const SCANNERS: usize = 2;

    let (_tmp, engine, meta_page) = setup();
    let committed: Arc<Mutex<BTreeSet<i32>>> = Arc::new(Mutex::new(BTreeSet::new()));
    let writers_done = Arc::new(AtomicUsize::new(0));

    let engine2 = Arc::clone(&engine);
    let committed2 = Arc::clone(&committed);
    let writers_done2 = Arc::clone(&writers_done);
    run_with_watchdog("insert+scan no-miss", Duration::from_secs(300), move || {
        let mut handles = Vec::new();
        for w in 0..writers {
            let engine = Arc::clone(&engine2);
            let committed = Arc::clone(&committed2);
            let writers_done = Arc::clone(&writers_done2);
            handles.push(thread::spawn(move || {
                let mut index = open_handle(&engine, meta_page);
                let base = (w * per_writer) as i32;
                for i in 0..per_writer {
                    let k = base + i as i32;
                    index.insert(&key(k), tid(k as u64)).unwrap();
                    // Publish only after the insert fully returned: anything
                    // in the set is durably applied to the tree.
                    committed.lock().unwrap().insert(k);
                }
                writers_done.fetch_add(1, Ordering::SeqCst);
            }));
        }
        for _ in 0..SCANNERS {
            let engine = Arc::clone(&engine2);
            let committed = Arc::clone(&committed2);
            let writers_done = Arc::clone(&writers_done2);
            handles.push(thread::spawn(move || {
                let index = open_handle(&engine, meta_page);
                let mut window = 0usize;
                while writers_done.load(Ordering::SeqCst) < writers {
                    // Snapshot first: the scan below must contain every
                    // snapshotted key in its range. Keys inserted during the
                    // scan are legitimately absent from the snapshot and are
                    // not asserted on.
                    let snapshot: Vec<i32> = committed.lock().unwrap().iter().copied().collect();
                    if snapshot.is_empty() {
                        thread::yield_now();
                        continue;
                    }
                    let w = window % writers;
                    window += 1;
                    let lo = (w * per_writer) as i32;
                    let hi = lo + per_writer as i32;
                    let rows = index.range_scan(Some(&key(lo)), Some(&key(hi))).unwrap();
                    let got: BTreeSet<i32> = rows
                        .iter()
                        .map(|(k, _)| decode_i32(k.clone().try_into().unwrap()))
                        .collect();
                    for k in snapshot.into_iter().filter(|k| *k >= lo && *k < hi) {
                        assert!(
                            got.contains(&k),
                            "scanner missed committed key {k} in range [{lo}, {hi})"
                        );
                    }
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    });

    // Final state: exact total, in order, and a clean validate.
    let index = open_handle(&engine, meta_page);
    let rows = index.range_scan(None, None).unwrap();
    assert_eq!(rows.len(), writers * per_writer);
    index.validate().unwrap();
}

/// Split storm: all threads insert INTERLEAVED ascending keys, so every
/// thread hammers the same right edge and splits fire constantly. Asserts
/// no deadlock (watchdog), exact final count, and a clean validate.
#[test]
fn concurrent_split_storm_interleaved() {
    let threads = env_usize("BTREE_TEST_THREADS", 12);
    let per_thread = env_usize("BTREE_TEST_KEYS", 1_500);

    let (_tmp, engine, meta_page) = setup();
    let engine2 = Arc::clone(&engine);
    run_with_watchdog("split storm", Duration::from_secs(300), move || {
        let mut handles = Vec::new();
        for t in 0..threads {
            let engine = Arc::clone(&engine2);
            handles.push(thread::spawn(move || {
                let mut index = open_handle(&engine, meta_page);
                for i in 0..per_thread {
                    let k = (i * threads + t) as i32;
                    index.insert(&key(k), tid(k as u64)).unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    });

    let index = open_handle(&engine, meta_page);
    let rows = index.range_scan(None, None).unwrap();
    assert_eq!(
        rows.len(),
        threads * per_thread,
        "exact key count after the storm"
    );
    for (i, (k, t)) in rows.iter().enumerate() {
        assert_eq!(k.as_slice(), key(i as i32).as_slice());
        assert_eq!(*t, tid(i as u64));
    }
    index.validate().unwrap();
}

/// Duplicate keys: every thread inserts the SAME key range with distinct
/// TIDs (duplicates are allowed; the pair `(key, tid)` stays unique).
#[test]
fn concurrent_duplicate_keys_lookup_all() {
    let threads = env_usize("BTREE_TEST_THREADS", 8);
    let keys = env_usize("BTREE_TEST_KEYS", 600);

    let (_tmp, engine, meta_page) = setup();
    let engine2 = Arc::clone(&engine);
    run_with_watchdog("duplicate keys", Duration::from_secs(300), move || {
        let mut handles = Vec::new();
        for t in 0..threads {
            let engine = Arc::clone(&engine2);
            handles.push(thread::spawn(move || {
                let mut index = open_handle(&engine, meta_page);
                for i in 0..keys {
                    index
                        .insert(&key(i as i32), tid((t * keys + i) as u64))
                        .unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    });

    let index = open_handle(&engine, meta_page);
    for k in 0..keys as i32 {
        let all = index.lookup_all(&key(k)).unwrap();
        assert_eq!(
            all.len(),
            threads,
            "key {k} must have exactly one entry per thread"
        );
        for (idx, t) in all.iter().enumerate() {
            // Thread t inserted this key with tid(t * keys + k); lookup_all
            // returns them in ascending (key, tid) order.
            assert_eq!(
                *t,
                tid((idx * keys + k as usize) as u64),
                "key {k} out of order"
            );
        }
    }
    index.validate().unwrap();
}

/// Root-split race: threads start from an empty single-leaf root and race
/// to fill it; the root-generation machinery (meta re-read + ROOT flag
/// check under the root write latch) must let exactly one promotion win
/// and every insert must land in the one resulting tree.
#[test]
fn concurrent_root_split_race() {
    const THREADS: usize = 4;
    const PER_THREAD: usize = 1_000;

    let (_tmp, engine, meta_page) = setup();
    let engine2 = Arc::clone(&engine);
    run_with_watchdog("root split race", Duration::from_secs(300), move || {
        let mut handles = Vec::new();
        for t in 0..THREADS {
            let engine = Arc::clone(&engine2);
            handles.push(thread::spawn(move || {
                let mut index = open_handle(&engine, meta_page);
                let base = (t * PER_THREAD) as i32;
                for i in 0..PER_THREAD {
                    let k = base + i as i32;
                    index.insert(&key(k), tid(k as u64)).unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    });

    let index = open_handle(&engine, meta_page);
    assert!(index.tree_level() >= 1, "the root must have been promoted");
    let rows = index.range_scan(None, None).unwrap();
    assert_eq!(rows.len(), THREADS * PER_THREAD);
    index.validate().unwrap();
}

/// Coding-plan smoke: 100 connections × concurrent INSERT + range scans
/// with the no-miss assertion (Stage Q acceptance scenario). CI-friendly
/// volume by default; scale up with BTREE_SOAK_THREADS / BTREE_SOAK_KEYS.
#[test]
fn concurrent_hundred_thread_smoke() {
    let threads = env_usize("BTREE_SOAK_THREADS", 100);
    let per_thread = env_usize("BTREE_SOAK_KEYS", 200);
    const SCANNERS: usize = 4;

    let (_tmp, engine, meta_page) = setup();
    let committed: Arc<Mutex<BTreeSet<i32>>> = Arc::new(Mutex::new(BTreeSet::new()));
    let writers_done = Arc::new(AtomicUsize::new(0));

    let engine2 = Arc::clone(&engine);
    let committed2 = Arc::clone(&committed);
    let writers_done2 = Arc::clone(&writers_done);
    run_with_watchdog("100-thread smoke", Duration::from_secs(600), move || {
        let mut handles = Vec::new();
        for t in 0..threads {
            let engine = Arc::clone(&engine2);
            let committed = Arc::clone(&committed2);
            let writers_done = Arc::clone(&writers_done2);
            handles.push(thread::spawn(move || {
                let mut index = open_handle(&engine, meta_page);
                let base = (t * per_thread) as i32;
                for i in 0..per_thread {
                    let k = base + i as i32;
                    index.insert(&key(k), tid(k as u64)).unwrap();
                    committed.lock().unwrap().insert(k);
                }
                writers_done.fetch_add(1, Ordering::SeqCst);
            }));
        }
        for _ in 0..SCANNERS {
            let engine = Arc::clone(&engine2);
            let committed = Arc::clone(&committed2);
            let writers_done = Arc::clone(&writers_done2);
            handles.push(thread::spawn(move || {
                let index = open_handle(&engine, meta_page);
                let mut window = 0usize;
                while writers_done.load(Ordering::SeqCst) < threads {
                    let snapshot: Vec<i32> = committed.lock().unwrap().iter().copied().collect();
                    if snapshot.is_empty() {
                        thread::yield_now();
                        continue;
                    }
                    let w = window % threads;
                    window += 1;
                    let lo = (w * per_thread) as i32;
                    let hi = lo + per_thread as i32;
                    let rows = index.range_scan(Some(&key(lo)), Some(&key(hi))).unwrap();
                    let got: BTreeSet<i32> = rows
                        .iter()
                        .map(|(k, _)| decode_i32(k.clone().try_into().unwrap()))
                        .collect();
                    for k in snapshot.into_iter().filter(|k| *k >= lo && *k < hi) {
                        assert!(
                            got.contains(&k),
                            "scanner missed committed key {k} in range [{lo}, {hi})"
                        );
                    }
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    });

    let index = open_handle(&engine, meta_page);
    let rows = index.range_scan(None, None).unwrap();
    assert_eq!(rows.len(), threads * per_thread);
    index.validate().unwrap();
}

/// Space-reservation restart: artificial split-page allocation failures
/// must be absorbed by the release-and-restart path, not bubble as errors.
/// Single-threaded (the retry logic is thread-count independent). The hook
/// is thread-local, so no other test (or thread) can consume this test's
/// injected failures; the final assertion proves all 3 failures fired and
/// were retried.
#[test]
fn split_alloc_failure_restarts_and_succeeds() {
    let (_tmp, engine, meta_page) = setup();
    let mut index = open_handle(&engine, meta_page);

    SPLIT_ALLOC_FAILURES.with(|c| c.set(3));
    for i in 0..2_000i32 {
        index.insert(&key(i), tid(i as u64)).unwrap();
    }
    assert_eq!(
        SPLIT_ALLOC_FAILURES.with(|c| c.get()),
        0,
        "all 3 injected allocation failures must have been consumed by restarts"
    );

    assert!(index.tree_level() >= 1, "splits must have happened");
    for i in 0..2_000i32 {
        assert_eq!(index.lookup(&key(i)).unwrap(), Some(tid(i as u64)));
    }
    index.validate().unwrap();
}

/// Small-pool storm (Stage Q review, biggest test gap): a 16-frame buffer
/// pool forces CLOCK eviction to interleave with splits and crabbing — the
/// exact window where `split_copy`'s unpin→flush could see the right page
/// evicted (G1: flush must treat PageNotFound as already-durable, not fail
/// the insert). 4 writers + 1 scanner; asserts no deadlock (watchdog), no
/// lost keys, no spurious PageNotFound-flavoured errors (any would panic
/// the workers' unwraps), and a clean quiescent validate.
#[test]
fn concurrent_small_pool_split_eviction_storm() {
    const THREADS: usize = 4;
    const PER_THREAD: usize = 1_200; // 4800 keys ≈ 11 leaves + internals > 16 frames
    const FRAMES: usize = 16;

    let (_tmp, engine, meta_page) = setup_with_config(|cfg| {
        cfg.buffer_pool_size = FRAMES * pg_storage::types::PAGE_SIZE;
    });
    let committed: Arc<Mutex<BTreeSet<i32>>> = Arc::new(Mutex::new(BTreeSet::new()));
    let writers_done = Arc::new(AtomicUsize::new(0));

    let engine2 = Arc::clone(&engine);
    let committed2 = Arc::clone(&committed);
    let writers_done2 = Arc::clone(&writers_done);
    run_with_watchdog("small-pool storm", Duration::from_secs(300), move || {
        let mut handles = Vec::new();
        for t in 0..THREADS {
            let engine = Arc::clone(&engine2);
            let committed = Arc::clone(&committed2);
            let writers_done = Arc::clone(&writers_done2);
            handles.push(thread::spawn(move || {
                let mut index = open_handle(&engine, meta_page);
                let base = (t * PER_THREAD) as i32;
                for i in 0..PER_THREAD {
                    let k = base + i as i32;
                    index.insert(&key(k), tid(k as u64)).unwrap();
                    committed.lock().unwrap().insert(k);
                }
                writers_done.fetch_add(1, Ordering::SeqCst);
            }));
        }
        {
            let engine = Arc::clone(&engine2);
            let committed = Arc::clone(&committed2);
            let writers_done = Arc::clone(&writers_done2);
            handles.push(thread::spawn(move || {
                let index = open_handle(&engine, meta_page);
                let mut window = 0usize;
                while writers_done.load(Ordering::SeqCst) < THREADS {
                    let snapshot: Vec<i32> = committed.lock().unwrap().iter().copied().collect();
                    if snapshot.is_empty() {
                        thread::yield_now();
                        continue;
                    }
                    let w = window % THREADS;
                    window += 1;
                    let lo = (w * PER_THREAD) as i32;
                    let hi = lo + PER_THREAD as i32;
                    let rows = index.range_scan(Some(&key(lo)), Some(&key(hi))).unwrap();
                    let got: BTreeSet<i32> = rows
                        .iter()
                        .map(|(k, _)| decode_i32(k.clone().try_into().unwrap()))
                        .collect();
                    for k in snapshot.into_iter().filter(|k| *k >= lo && *k < hi) {
                        assert!(
                            got.contains(&k),
                            "scanner missed committed key {k} in range [{lo}, {hi})"
                        );
                    }
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    });

    let index = open_handle(&engine, meta_page);
    let rows = index.range_scan(None, None).unwrap();
    assert_eq!(rows.len(), THREADS * PER_THREAD, "no key may be lost");
    index.validate().unwrap();
}

/// Stage Q 1h soak: 100 connections, mixed INSERT + range scan, no miss
/// (coding-plan acceptance scenario, time-boxed version of
/// `concurrent_insert_and_range_scan_no_miss`). `#[ignore]`d by default —
/// see the module header for the 1h and smoke commands.
///
/// Writers insert ascending keys into their own disjoint 10M-wide key range
/// until the deadline; scanners repeatedly snapshot the committed set and
/// assert that a range scan over a writer's window never misses a committed
/// key. Final state: exact total count and a clean quiescent validate.
#[test]
#[ignore = "1h soak — run explicitly with --ignored (see module header)"]
fn soak_mixed_insert_scan_no_miss() {
    let threads = env_usize("BTREE_SOAK_THREADS", 100);
    let secs = env_usize("BTREE_SOAK_SECS", 3600);
    const SCANNERS: usize = 4;
    /// Per-writer disjoint key-range width: an hour of inserts stays well
    /// below 10M keys per connection at this engine's TPS.
    const RANGE: i32 = 10_000_000;

    let (_tmp, engine, meta_page) = setup();
    let committed: Arc<Mutex<BTreeSet<i32>>> = Arc::new(Mutex::new(BTreeSet::new()));
    let deadline = Instant::now() + Duration::from_secs(secs as u64);

    let engine2 = Arc::clone(&engine);
    let committed2 = Arc::clone(&committed);
    run_with_watchdog(
        "1h soak",
        Duration::from_secs(secs as u64 + 300),
        move || {
            let mut handles = Vec::new();
            for t in 0..threads {
                let engine = Arc::clone(&engine2);
                let committed = Arc::clone(&committed2);
                handles.push(thread::spawn(move || {
                    let mut index = open_handle(&engine, meta_page);
                    let base = (t as i32) * RANGE;
                    let mut i = 0i32;
                    while Instant::now() < deadline {
                        let k = base + i;
                        index.insert(&key(k), tid(k as u64)).unwrap();
                        // Publish only after the insert fully returned.
                        committed.lock().unwrap().insert(k);
                        i += 1;
                    }
                }));
            }
            for _ in 0..SCANNERS {
                let engine = Arc::clone(&engine2);
                let committed = Arc::clone(&committed2);
                handles.push(thread::spawn(move || {
                    let index = open_handle(&engine, meta_page);
                    let mut window = 0usize;
                    while Instant::now() < deadline {
                        // Snapshot first: the scan below must contain every
                        // snapshotted key in its range.
                        let snapshot: Vec<i32> =
                            committed.lock().unwrap().iter().copied().collect();
                        if snapshot.is_empty() {
                            thread::yield_now();
                            continue;
                        }
                        let w = window % threads;
                        window += 1;
                        let lo = (w as i32) * RANGE;
                        let hi = lo + RANGE;
                        let rows = index.range_scan(Some(&key(lo)), Some(&key(hi))).unwrap();
                        let got: BTreeSet<i32> = rows
                            .iter()
                            .map(|(k, _)| decode_i32(k.clone().try_into().unwrap()))
                            .collect();
                        for k in snapshot.into_iter().filter(|k| *k >= lo && *k < hi) {
                            assert!(
                                got.contains(&k),
                                "scanner missed committed key {k} in range [{lo}, {hi})"
                            );
                        }
                    }
                }));
            }
            for h in handles {
                h.join().unwrap();
            }
        },
    );

    let index = open_handle(&engine, meta_page);
    let rows = index.range_scan(None, None).unwrap();
    let expected = committed.lock().unwrap().len();
    assert_eq!(
        rows.len(),
        expected,
        "final row count must equal the number of completed inserts"
    );
    index.validate().unwrap();
}
