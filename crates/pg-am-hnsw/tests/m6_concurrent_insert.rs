//! M6 Stage B slice 2: concurrent-insert smoke tests on the shared
//! `&HnswIndex` handle (tech-selection §4.1/§4.2). They prove the critical
//! section protocol end to end: N threads insert disjoint vector streams
//! into ONE index; afterwards the audit must pass (all residues zero — no
//! crash, so no INITIALIZING/orphan/hidden), the NodeId stream must be
//! dense 0..N (the alloc critical section), every inserted vector must be
//! self-recallable at ef = N (flood), and a racing first-insert pair must
//! both land connected (the meta critical section's backfill path).
//!
//! The bitwise form is NOT pinned here (concurrent interleavings are a
//! legal-shape SET, §13: the pins are audit + recall, not a single
//! reference stream); the single-writer bitwise pin stays with the lib's
//! parity tests and the m5_insert_crash probe matrix.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use pg_am_hnsw::graph::{Metric, NeighborSelection};
use pg_am_hnsw::{HnswParams, NodeId};
use pg_storage::config::StorageConfig;
use pg_storage::engine::StorageEngine;

const DIM: u16 = 8;
const THREADS: usize = 8;
const PER_THREAD: usize = 50;

static DIR_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn fresh_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pg_am_hnsw_m6_concurrent-{}-{}-{tag}",
        std::process::id(),
        DIR_COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Deterministic disjoint vectors: thread t, seq i — distinct per (t, i).
fn vector_for(t: usize, i: usize) -> Vec<f32> {
    (0..DIM as usize)
        .map(|j| ((t * 131 + i * 17 + j * 7) % 113) as f32 / 16.0)
        .collect()
}

fn audit_and_recall_all(
    engine: &StorageEngine,
    index: &pg_am_hnsw::HnswIndex,
    vectors: &[(usize, usize)],
) {
    let n = vectors.len() as u64;
    assert_eq!(index.hwm(), n, "dense NodeIds from the alloc section");
    let report = index.audit(engine.buffer_pool()).unwrap();
    assert_eq!(report.node_count, n);
    assert_eq!(report.live_count, n);
    assert_eq!(report.initializing_count, 0);
    assert_eq!(report.orphan_entry_count, 0);
    assert_eq!(report.hidden_high_level_count, 0);
    assert_eq!(report.tombstoned_count, 0);
    let reach =
        pg_am_hnsw::audit::audit_reachability(engine.buffer_pool(), index.meta_page_id()).unwrap();
    assert!(
        reach.unreachable_live.is_empty(),
        "a fully-committed concurrent build leaves no hanging node: {:?}",
        reach.unreachable_live
    );
    // Recall sanity: every inserted vector recalls ITSELF at distance 0
    // under a full-width flood (ef = n).
    for &(t, i) in vectors {
        let q = vector_for(t, i);
        let hits = index
            .search(engine.buffer_pool(), &q, 1, Some(n as usize))
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(
            hits[0].1, 0.0,
            "vector ({t},{i}) must recall itself at distance 0 (got node {:?} at {})",
            hits[0].0, hits[0].1
        );
    }
}

/// N threads insert disjoint streams into ONE shared index handle. The
/// `&HnswIndex` sharing across `scope` threads IS the compile-time Sync
/// witness.
#[test]
fn concurrent_inserts_all_land_and_are_reachable() {
    let dir = fresh_dir("insert");
    let engine = StorageEngine::open(&dir, &StorageConfig::new(&dir)).unwrap();
    let index = pg_am_hnsw::index::create(
        engine.buffer_pool(),
        engine.wal_writer(),
        HnswParams::new(8, 16, 32, 8).unwrap(),
        DIM,
        Metric::L2,
        NeighborSelection::Heuristic,
        0xC0FFEE,
    )
    .unwrap();
    let index = Arc::new(index);

    let errors = Arc::new(AtomicUsize::new(0));
    std::thread::scope(|scope| {
        for t in 0..THREADS {
            let index = Arc::clone(&index);
            let errors = Arc::clone(&errors);
            let engine = &engine;
            scope.spawn(move || {
                for i in 0..PER_THREAD {
                    if index
                        .insert(engine.buffer_pool(), engine.wal_writer(), &vector_for(t, i))
                        .is_err()
                    {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
        }
    });

    assert_eq!(errors.load(Ordering::Relaxed), 0, "no insert may fail");
    let vectors: Vec<(usize, usize)> = (0..THREADS)
        .flat_map(|t| (0..PER_THREAD).map(move |i| (t, i)))
        .collect();
    audit_and_recall_all(&engine, &index, &vectors);

    engine.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Two racing inserts onto an EMPTY index: both must land and stay
/// connected — the loser's meta-race BACKFILL is what this pins (M6 §4.2
/// step 4). The race is repeated on the growing graph to squeeze the
/// interleaving window; the audit + recall sweep runs ONCE at the end
/// (per-round sweeps would be O(rounds²) — the debug build cannot afford
/// them).
#[test]
fn racing_first_inserts_backfill_connect() {
    let dir = fresh_dir("first-race");
    let engine = StorageEngine::open(&dir, &StorageConfig::new(&dir)).unwrap();
    let index = pg_am_hnsw::index::create(
        engine.buffer_pool(),
        engine.wal_writer(),
        HnswParams::new(4, 8, 16, 4).unwrap(),
        DIM,
        Metric::L2,
        NeighborSelection::Heuristic,
        0xBEEF,
    )
    .unwrap();
    let index = Arc::new(index);

    let mut vectors: Vec<(usize, usize)> = Vec::new();
    for attempt in 0..50usize {
        let va = vector_for(100 + attempt, 0);
        let vb = vector_for(200 + attempt, 0);
        vectors.push((100 + attempt, 0));
        vectors.push((200 + attempt, 0));
        let index = &index;
        let engine = &engine;
        std::thread::scope(|scope| {
            let a = scope.spawn(|| index.insert(engine.buffer_pool(), engine.wal_writer(), &va));
            let b = scope.spawn(|| index.insert(engine.buffer_pool(), engine.wal_writer(), &vb));
            a.join().expect("racer A panicked").unwrap();
            b.join().expect("racer B panicked").unwrap();
        });
    }
    audit_and_recall_all(&engine, &index, &vectors);

    engine.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Review P1-1 regression: a search CONCURRENT with an insert burst must
/// never index past its visited bitset — neighbor lists can name nodes
/// allocated after the search's hwm snapshot (a concurrent inserter's
/// backward edges); those nodes are invisible to the search, never a
/// panic. The burst crosses the 64-id bitset word boundary by design.
#[test]
fn search_concurrent_with_insert_burst_never_panics() {
    let dir = fresh_dir("search-burst");
    let engine = StorageEngine::open(&dir, &StorageConfig::new(&dir)).unwrap();
    let index = pg_am_hnsw::index::create(
        engine.buffer_pool(),
        engine.wal_writer(),
        HnswParams::new(4, 8, 16, 4).unwrap(),
        DIM,
        Metric::L2,
        NeighborSelection::Heuristic,
        0xCAFE,
    )
    .unwrap();
    let index = Arc::new(index);

    // Seed past one bitset word (64 ids) so a burst can push a neighbor
    // id past the reader's snapshot capacity.
    for i in 0..70usize {
        index
            .insert(
                engine.buffer_pool(),
                engine.wal_writer(),
                &vector_for(500, i),
            )
            .unwrap();
    }

    // Readers exit when both writers are DONE (the burst is over) — a flag
    // set after `scope` would deadlock (scope waits for the readers).
    let writers_done = Arc::new(AtomicUsize::new(0));
    std::thread::scope(|scope| {
        // 2 writers: 40 inserts each (the burst).
        for t in 0..2usize {
            let index = Arc::clone(&index);
            let writers_done = Arc::clone(&writers_done);
            let engine = &engine;
            scope.spawn(move || {
                for i in 0..40 {
                    index
                        .insert(
                            engine.buffer_pool(),
                            engine.wal_writer(),
                            &vector_for(600 + t, i),
                        )
                        .unwrap();
                }
                writers_done.fetch_add(1, Ordering::Relaxed);
            });
        }
        // 2 readers: search continuously during the burst.
        for _ in 0..2 {
            let index = Arc::clone(&index);
            let writers_done = Arc::clone(&writers_done);
            let engine = &engine;
            scope.spawn(move || {
                while writers_done.load(Ordering::Relaxed) < 2 {
                    let hits = index
                        .search(engine.buffer_pool(), &vector_for(500, 0), 3, Some(16))
                        .unwrap();
                    assert!(!hits.is_empty());
                }
            });
        }
    });

    // Post-burst: exact state — 70 seeded + 80 burst = 150, all LIVE,
    // audit clean, and a recall sweep over a sample recalls each at
    // distance 0.
    assert_eq!(index.hwm(), 150, "70 seeded + 80 burst");
    let report = index.audit(engine.buffer_pool()).unwrap();
    assert_eq!(report.node_count, 150);
    assert_eq!(report.live_count, 150);
    assert_eq!(report.initializing_count, 0);
    assert_eq!(report.orphan_entry_count, 0);
    assert_eq!(report.tombstoned_count, 0);
    for i in (0..70).step_by(17) {
        let q = vector_for(500, i);
        let hits = index
            .search(engine.buffer_pool(), &q, 1, Some(150))
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].1, 0.0);
    }

    engine.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// DirLink under concurrency (non-loom; M6 §12.1's "hwm critical section
/// in full incl. the DirLink branch" is covered HERE, not in loom — loom's
/// internal path counter overflows on an 812-insert pre-fill, registered
/// in stage_spec/tech-selection): the chain crosses a directory page
/// boundary while writers race and readers search. Asserts: exact hwm,
/// clean audit, no "maps past the directory chain" (the slice-3
/// self-healing covers a reader whose snapshot lags the DirLink).
#[test]
fn concurrent_insert_across_dir_page_boundary() {
    let dir = fresh_dir("dirlink-race");
    let engine = StorageEngine::open(&dir, &StorageConfig::new(&dir)).unwrap();
    let index = pg_am_hnsw::index::create(
        engine.buffer_pool(),
        engine.wal_writer(),
        HnswParams::new(4, 8, 16, 4).unwrap(),
        DIM,
        Metric::L2,
        NeighborSelection::Heuristic,
        0x1A1C,
    )
    .unwrap();
    let index = Arc::new(index);

    // Pre-fill to 810 single-threaded — the page boundary (813 entries)
    // falls INSIDE the concurrent burst below, so the DirLink for node 813
    // genuinely races the readers' stale snapshots (review round 3: at 815
    // the crossing happened in the single-threaded prefix and the race was
    // never exercised).
    for i in 0..810usize {
        index
            .insert(
                engine.buffer_pool(),
                engine.wal_writer(),
                &vector_for(700, i),
            )
            .unwrap();
    }
    assert_eq!(index.hwm(), 810);

    let errors = Arc::new(AtomicUsize::new(0));
    let writers_done = Arc::new(AtomicUsize::new(0));
    std::thread::scope(|scope| {
        for t in 0..2usize {
            let index = Arc::clone(&index);
            let errors = Arc::clone(&errors);
            let writers_done = Arc::clone(&writers_done);
            let engine = &engine;
            scope.spawn(move || {
                for i in 0..20 {
                    if index
                        .insert(
                            engine.buffer_pool(),
                            engine.wal_writer(),
                            &vector_for(800 + t, i),
                        )
                        .is_err()
                    {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
                writers_done.fetch_add(1, Ordering::Relaxed);
            });
        }
        // A reader searching continuously THROUGH the boundary crossings.
        let index = Arc::clone(&index);
        let writers_done = Arc::clone(&writers_done);
        let engine = &engine;
        scope.spawn(move || {
            while writers_done.load(Ordering::Relaxed) < 2 {
                let hits = index
                    .search(engine.buffer_pool(), &vector_for(700, 0), 3, Some(16))
                    .unwrap();
                assert!(!hits.is_empty());
            }
        });
    });
    assert_eq!(errors.load(Ordering::Relaxed), 0);
    assert_eq!(index.hwm(), 850);

    let report = index.audit(engine.buffer_pool()).unwrap();
    assert_eq!(report.node_count, 850);
    assert_eq!(report.live_count, 850);
    assert_eq!(report.initializing_count, 0);
    assert_eq!(report.orphan_entry_count, 0);

    engine.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Delete × insert concurrency (M6 §8.1): tombstones under the node latch
/// interleave with in-flight inserts; concurrent deletes of the SAME node
/// are idempotent (both Ok, one a no-op). Quiesce, then the audit counts
/// must match the exact delete set.
#[test]
fn concurrent_delete_and_insert() {
    let dir = fresh_dir("delete-insert");
    let engine = StorageEngine::open(&dir, &StorageConfig::new(&dir)).unwrap();
    let index = pg_am_hnsw::index::create(
        engine.buffer_pool(),
        engine.wal_writer(),
        HnswParams::new(4, 8, 16, 4).unwrap(),
        DIM,
        Metric::L2,
        NeighborSelection::Heuristic,
        0x5EED,
    )
    .unwrap();
    let index = Arc::new(index);

    // Seed 30 nodes single-threaded.
    for i in 0..30usize {
        index
            .insert(
                engine.buffer_pool(),
                engine.wal_writer(),
                &vector_for(300, i),
            )
            .unwrap();
    }

    // Concurrent: 2 writers insert 25 each; 2 deleters tombstone nodes
    // 0..20 (10 each); 2 more threads delete node 29 TWICE (idempotent
    // race).
    let errors = Arc::new(AtomicUsize::new(0));
    std::thread::scope(|scope| {
        for t in 0..2usize {
            let index = Arc::clone(&index);
            let errors = Arc::clone(&errors);
            let engine = &engine;
            scope.spawn(move || {
                for i in 0..25 {
                    if index
                        .insert(
                            engine.buffer_pool(),
                            engine.wal_writer(),
                            &vector_for(400 + t, i),
                        )
                        .is_err()
                    {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
        }
        for t in 0..2usize {
            let index = Arc::clone(&index);
            let errors = Arc::clone(&errors);
            let engine = &engine;
            scope.spawn(move || {
                for i in 0..10u32 {
                    let victim = NodeId(t as u32 * 10 + i);
                    if index
                        .delete(engine.buffer_pool(), engine.wal_writer(), victim)
                        .is_err()
                    {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
        }
        // The double-delete race on node 29.
        for _ in 0..2 {
            let index = Arc::clone(&index);
            let errors = Arc::clone(&errors);
            let engine = &engine;
            scope.spawn(move || {
                if index
                    .delete(engine.buffer_pool(), engine.wal_writer(), NodeId(29))
                    .is_err()
                {
                    errors.fetch_add(1, Ordering::Relaxed);
                }
            });
        }
    });
    assert_eq!(errors.load(Ordering::Relaxed), 0, "no op may fail");

    assert_eq!(index.hwm(), 80, "30 seeded + 50 concurrent inserts");
    let report = index.audit(engine.buffer_pool()).unwrap();
    assert_eq!(report.node_count, 80);
    assert_eq!(
        report.tombstoned_count, 21,
        "nodes 0..19 (the two deleter threads) plus 29 (double-deleted, idempotent)"
    );
    assert_eq!(report.initializing_count, 0);
    assert_eq!(report.orphan_entry_count, 0);

    engine.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}
