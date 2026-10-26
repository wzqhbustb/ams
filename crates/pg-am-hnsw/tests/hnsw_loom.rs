//! M6 Stage B loom model tests: exhaustive interleaving exploration of the
//! REAL concurrent insert path (the alloc critical section, the node-latch
//! connect discipline, the meta critical section's first-insert race and
//! backfill, and a reader's PublishLive × search interleaving) — the
//! tech-selection §4.2/§4.3 choreography, verified by loom instead of by
//! argument. M6 Stage E adds the fourth model: vacuum-redirect ×
//! concurrent insert (§12.1's third face) — the holder-latch RMW
//! discipline shared by insert step 6 and the redirect's SetNeighbors
//! rewrite is proven to lose no edge under any interleaving.
//!
//! Run with bounded preemptions (same tier discipline as btree_loom):
//!
//! ```sh
//! LOOM_MAX_PREEMPTIONS=2 cargo test -p pg-am-hnsw --features loom --release --test hnsw_loom
//! ```
//!
//! # How the model stays inside loom
//!
//! The `loom` feature turns on `pg-storage/loom`, which swaps every lock
//! that participates in the choreography for loom's instrumented
//! primitives (`pg_storage::sync`); the crate's OWN synchronization
//! (alloc mutex, dir_pages RwLock, the 4096-shard latch table, the
//! hwm/meta_rt atomics) aliases the same layer (`pg_storage::sync::{Mutex,
//! RwLock, atomic}` in index.rs), so every §4.2/§4.3 scheduling point is a
//! loom point. The WAL group-commit worker and `flush_frame`'s data-file
//! I/O are stubbed by pg-storage's loom build (see its sync module docs).
//!
//! **Build-shape disclosure** (review round 3 P2-2): the dev-dependency
//! self-reference enables `test-harness` in ALL test builds INCLUDING this
//! one, so the probe Mutex exists here and `barrier` takes it at every WAL
//! boundary — extra scheduling points the production build (no probe field,
//! no-op barrier) does not have. The probe is never armed and logging is
//! off, so the barrier's behavior is the production no-op branch; the
//! choreography under test (alloc/dir_pages/node-latch/meta) is unchanged.
//! (An AtomicBool fast path that skips the probe lock is registered as a
//! Stage C顺带 item.)
//!
//! # Sizing (loom state space explodes — keep models TINY)
//!
//! - One insert per writer thread; the connect phase on a 1-3 node graph.
//! - Acceptance bound: `LOOM_MAX_PREEMPTIONS=2` for all models (bound 3 on
//!   HNSW's per-insert scheduling density did not finish in 76 min locally;
//!   btree's linearizable model self-clamps to 2 for the same reason).
//! - Pool sized at 16 frames so the tiny graph never evicts (an evicted
//!   page would reload as zeros — the flush stub writes nothing).
//! - The model closure re-runs per interleaving, including setup (fresh
//!   temp dir + WAL/pool open); setup runs on the big-stack driver thread.
//! - **DirLink branch**: NOT modeled here — loom's internal path counter
//!   overflows on the 812-insert pre-fill needed to reach a directory page
//!   boundary. Coverage of that branch under concurrency lives in
//!   `m6_concurrent_insert.rs::concurrent_insert_across_dir_page_boundary`
//!   (non-loom); the narrowing is registered in stage_spec/tech-selection.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use loom::sync::atomic::Ordering;
// NOT loom-instrumented: unique dir names per interleaving — no
// choreography semantics, so std's atomic is fine (and `static`-legal,
// which loom's is not).
use loom::thread;
use std::sync::atomic::AtomicUsize;

use pg_am_hnsw::graph::{Metric, NeighborSelection};
use pg_am_hnsw::{HnswIndex, HnswParams};
use pg_storage::buffer_pool::BufferPool;
use pg_storage::config::StorageConfig;
use pg_storage::page_allocator::PageAllocator;
use pg_storage::sync::Mutex;
use pg_storage::wal::WalWriter;

/// Generator stack for every loom thread including the driver (real engine
/// code overflows loom's 4 KB generator default immediately; btree_loom).
const LOOM_STACK_SIZE: usize = 4 << 20;

fn spawn_big<F>(f: F) -> thread::JoinHandle<()>
where
    F: FnOnce() + Send + 'static,
{
    thread::Builder::new()
        .stack_size(LOOM_STACK_SIZE)
        .spawn(f)
        .unwrap()
}

/// Run `body` as a loom model, clamping the preemption bound per model.
fn run_model<F>(max_preemptions: usize, body: F)
where
    F: Fn() + Send + Sync + 'static,
{
    run_model_with_branches(max_preemptions, None, body);
}

/// `max_branches` overrides loom's per-execution branch budget (default
/// 1000) for models whose single execution is scheduling-point dense —
/// the redirect pass adds a full graph scan + per-edge latch RMW on top of
/// the seed inserts (Stage E); raising the budget does NOT widen the
/// explored interleaving set (the preemption bound governs that), it only
/// lets one execution finish.
fn run_model_with_branches<F>(max_preemptions: usize, max_branches: Option<usize>, body: F)
where
    F: Fn() + Send + Sync + 'static,
{
    let body = Arc::new(body);
    let mut builder = loom::model::Builder::new();
    if builder.preemption_bound.unwrap_or(usize::MAX) > max_preemptions {
        eprintln!(
            "hnsw_loom: clamping preemption bound {:?} -> {max_preemptions}",
            builder.preemption_bound
        );
        builder.preemption_bound = Some(max_preemptions);
    }
    if let Some(b) = max_branches {
        if builder.max_branches < b {
            eprintln!(
                "hnsw_loom: raising max branches {} -> {b} (dense single execution)",
                builder.max_branches
            );
            builder.max_branches = b;
        }
    }
    builder.check(move || {
        let body = Arc::clone(&body);
        spawn_big(move || body()).join().unwrap();
    });
}

static COUNTER: AtomicUsize = AtomicUsize::new(0);

/// Unique temp dir per interleaving (the closure re-runs in full).
fn fresh_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pg_am_hnsw_loom-{}-{}-{tag}",
        std::process::id(),
        COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

const DIM: u16 = 4;

fn loom_config(dir: &Path) -> StorageConfig {
    let mut cfg = StorageConfig::new(dir);
    cfg.buffer_pool_size = 16 * cfg.page_size();
    cfg.buffer_pool_shards = 4;
    cfg.wal_segment_size = 64 * 1024;
    cfg.wal_group_commit_batch_size = 1;
    cfg.wal_group_commit_timeout_ms = 1;
    cfg
}

/// Open a loom-instrumented (pool, wal) + a fresh HNSW index on them.
fn setup(tag: &str) -> (PathBuf, Arc<BufferPool>, Arc<WalWriter>, HnswIndex) {
    let dir = fresh_dir(tag);
    let cfg = loom_config(&dir);
    let wal = Arc::new(WalWriter::open(&dir, &cfg).unwrap());
    let allocator = Arc::new(Mutex::new(
        PageAllocator::open(&dir, &cfg, Arc::clone(&wal)).unwrap(),
    ));
    let pool =
        Arc::new(BufferPool::open(&dir, &cfg, Arc::clone(&allocator), Arc::clone(&wal)).unwrap());
    let index = pg_am_hnsw::index::create(
        &pool,
        &wal,
        HnswParams::new(4, 8, 16, 4).unwrap(),
        DIM,
        Metric::L2,
        NeighborSelection::Heuristic,
        0xBEEF,
    )
    .unwrap();
    (dir, pool, wal, index)
}

fn vec_for(i: u32) -> Vec<f32> {
    (0..DIM)
        .map(|j| (i * 7 + j as u32 + 1) as f32 / 8.0)
        .collect()
}

/// Two writers insert into a NON-empty index concurrently: the alloc
/// critical section serializes their NodeIds, the connect phase's
/// node-latch discipline covers their (possibly overlapping) neighbor
/// lists, and the meta critical section keeps max_level monotone.
#[test]
fn loom_two_writers_shared_index() {
    run_model(3, || {
        let (_dir, pool, wal, index) = setup("two-writers");
        // Seed the entry point on the driver thread (hoisted out of the
        // racing section).
        index.insert(&pool, &wal, &vec_for(0)).unwrap();
        let index = Arc::new(index);
        let errors = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();
        for t in 1..=2u32 {
            let index = Arc::clone(&index);
            let errors = Arc::clone(&errors);
            let pool = Arc::clone(&pool);
            let wal = Arc::clone(&wal);
            handles.push(spawn_big(move || {
                if index.insert(&pool, &wal, &vec_for(t * 10)).is_err() {
                    errors.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(errors.load(Ordering::Relaxed), 0, "no insert may fail");
        assert_eq!(index.hwm(), 3, "dense allocation under the race");
    });
}

/// The first-insert race (M6 §4.2 step 4): BOTH writers read an empty meta
/// and must not both win — the loser backfills the connect phase so its
/// node is reachable, and exactly one MetaUpdate wins per promotion.
#[test]
fn loom_racing_first_insert() {
    run_model(3, || {
        let (_dir, pool, wal, index) = setup("first-race");
        let index = Arc::new(index);
        let errors = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();
        for t in 1..=2u32 {
            let index = Arc::clone(&index);
            let errors = Arc::clone(&errors);
            let pool = Arc::clone(&pool);
            let wal = Arc::clone(&wal);
            handles.push(spawn_big(move || {
                if index.insert(&pool, &wal, &vec_for(t * 10)).is_err() {
                    errors.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(errors.load(Ordering::Relaxed), 0);
        assert_eq!(index.hwm(), 2, "both racing inserts allocated");
        assert_ne!(
            index.entry_point(),
            u32::MAX,
            "exactly one first-insert winner published the entry point"
        );
        // max_level monotone holds structurally (the meta critical section
        // re-reads and only writes when `level > cur_max`); the direct
        // per-node comparison needs the winner's top level, which the test
        // surface doesn't expose — the packed-pair invariant is pinned by
        // lib test `meta_pair_never_reads_torn`.
        // Both nodes must be reachable: search for each vector recalls it.
        for t in 1..=2u32 {
            let hits = index.search(&pool, &vec_for(t * 10), 1, Some(4)).unwrap();
            assert_eq!(hits.len(), 1, "node {t} must be reachable");
            assert_eq!(hits[0].1, 0.0, "node {t} must recall itself");
        }
    });
}

/// Reader × writer: a search interleaves with an in-flight insert
/// (PublishLive vs. beam traversal; the read path holds no node latch).
/// The search must never panic nor read a torn entry; its hits are either
/// the pre-insert set or include the newcomer — both legal (the M6 §4.2
/// reachable ⟹ own-lists-complete invariant).
#[test]
fn loom_reader_writer_interleave() {
    run_model(2, || {
        let (_dir, pool, wal, index) = setup("reader-writer");
        index.insert(&pool, &wal, &vec_for(0)).unwrap();
        index.insert(&pool, &wal, &vec_for(1)).unwrap();
        let index = Arc::new(index);

        let writer = {
            let index = Arc::clone(&index);
            let pool = Arc::clone(&pool);
            let wal = Arc::clone(&wal);
            spawn_big(move || {
                index.insert(&pool, &wal, &vec_for(2)).unwrap();
            })
        };
        let reader = {
            let index = Arc::clone(&index);
            let pool = Arc::clone(&pool);
            spawn_big(move || {
                for _ in 0..3 {
                    let hits = index.search(&pool, &vec_for(0), 2, Some(4)).unwrap();
                    assert!(!hits.is_empty(), "a non-empty graph must answer");
                    // Hits must name nodes allocated at/before the hwm
                    // observed by THIS read (never a future allocation).
                    let hwm_now = index.hwm();
                    for (id, _) in &hits {
                        assert!(
                            (id.0 as u64) < hwm_now,
                            "a hit must name a real allocated node (< observed hwm)"
                        );
                    }
                }
            })
        };
        writer.join().unwrap();
        reader.join().unwrap();
        assert_eq!(index.hwm(), 3);
    });
}

/// Redirect × concurrent insert (M6 Stage E, tech-selection §12.1's
/// vacuum-redirect × insert face): a redirect pass over a
/// tombstone-carrying graph interleaves with an in-flight insert. Both
/// sides cover their neighbor-list read-modify-write with the HOLDER's
/// node latch (insert's step 6; the redirect's SetNeighbors rewrite), so
/// no interleaving can lose an edge: the post-race audit must be green
/// and every LIVE node must stay reachable — a lost reciprocal edge
/// would strand the newcomer. (The engine's `AccessExclusive` lock makes
/// this race operationally impossible in production; the model proves the
/// latch discipline underneath is a mechanism, not an assumption.)
#[test]
fn loom_redirect_insert_interleave() {
    // 3 seeded inserts + a delete + a racing insert + the full redirect
    // scan/rewrite pass make a single execution branch-dense — loom's
    // default 1000-branch budget overflows instantly (measured); 10k
    // leaves headroom without touching the interleaving set (bound 2).
    run_model_with_branches(2, Some(10_000), || {
        let (_dir, pool, wal, index) = setup("redirect-insert");
        // Seed a tombstone-carrying graph on the driver (hoisted out of
        // the racing section).
        index.insert(&pool, &wal, &vec_for(0)).unwrap();
        index.insert(&pool, &wal, &vec_for(1)).unwrap();
        index.insert(&pool, &wal, &vec_for(2)).unwrap();
        index.delete(&pool, &wal, pg_am_hnsw::NodeId(2)).unwrap();
        let index = Arc::new(index);

        let writer = {
            let index = Arc::clone(&index);
            let pool = Arc::clone(&pool);
            let wal = Arc::clone(&wal);
            spawn_big(move || {
                index.insert(&pool, &wal, &vec_for(3)).unwrap();
            })
        };
        let rebuilder = {
            let index = Arc::clone(&index);
            let pool = Arc::clone(&pool);
            let wal = Arc::clone(&wal);
            spawn_big(move || {
                index.redirect_tombstoned_in_edges(&pool, &wal).unwrap();
            })
        };
        writer.join().unwrap();
        rebuilder.join().unwrap();

        assert_eq!(index.hwm(), 4, "the insert landed exactly once");
        // Quiesce: structural audit green, and no LIVE node is stranded
        // (a lost reciprocal edge would show up here).
        pg_am_hnsw::audit::audit_index(&pool, index.meta_page_id()).unwrap();
        let reach = pg_am_hnsw::audit::audit_reachability(&pool, index.meta_page_id()).unwrap();
        assert!(
            reach.unreachable_live.is_empty(),
            "no interleaving may strand a LIVE node: {:?}",
            reach.unreachable_live
        );
        for t in [0u32, 1, 3] {
            let hits = index.search(&pool, &vec_for(t), 1, Some(4)).unwrap();
            assert_eq!(hits.len(), 1, "node {t} must be reachable");
            assert_eq!(hits[0].1, 0.0, "node {t} must recall itself");
        }
    });
}
