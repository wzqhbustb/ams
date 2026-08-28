//! M2c Stage T acceptance bench: mixed read/write throughput at 50 and 100
//! concurrent connections against an INDEXED table, through the SQL `exec`
//! API (coding-plan Stage T acceptance command:
//! `cargo bench -p pg-engine --bench m2c_100_conn`).
//!
//! Each thread ("connection") owns a disjoint `id` range and runs a mixed
//! auto-commit workload — 50% INSERT (maintains the B+Tree: splits fire as
//! each range grows), 25% UPDATE (HOT: only the unindexed `v` changes),
//! 25% point SELECT — so the measurement covers the full Stage T
//! concurrency surface: heap + index maintenance + WAL + group-committed
//! fsync + row/table lock protocol + deadlock detector tick.
//!
//! Run with the (already optimized) bench profile — no extra flag:
//!
//! ```sh
//! cargo bench -p pg-engine --bench m2c_100_conn
//! # quick smoke:
//! M2C_BENCH_CONNS=50 M2C_BENCH_OPS=10 cargo bench -p pg-engine \
//!     --bench m2c_100_conn -- --measurement-time 3 --sample-size 10
//! ```
//!
//! The Stage T acceptance CONFIGURATIONS are the sustained stability runs
//! (50 conn x 100 txn/s x 30min floor, 100 conn x 100 txn/s x 60min
//! challenge); those are paced wall-clock runs driven by the
//! `m2c_stress` test (see `tests/m2c_stress.rs` header), not criterion
//! benches. This bench is the short criterion throughput sample recorded
//! in `docs/phase1-m2-benchmarks.md`.
//!
//! `M2C_BENCH_CONNS` (comma-separated, default `50,100`) and
//! `M2C_BENCH_OPS` (mixed ops per thread per timed iteration, default 30)
//! shrink the run for smoke testing.
//!
//! KNOWN ISSUE (same fsync bound as `m2c_btree_tps.rs`): on this hardware
//! the per-commit fsync caps group-committed auto-commit throughput at
//! ~6-12K TPS regardless of thread count; numbers are measured and
//! reported, not gated.

use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use pg_engine::{Engine, EngineConfig, QueryResult};

/// `id` range size per connection.
const RANGE: i32 = 1_000_000;
/// Rows preloaded per connection (one transaction total) so UPDATE/SELECT
/// have targets from the first op.
const PRELOAD: i32 = 8;

fn bench_conns() -> Vec<usize> {
    std::env::var("M2C_BENCH_CONNS")
        .ok()
        .map(|v| v.split(',').filter_map(|s| s.trim().parse().ok()).collect())
        .unwrap_or_else(|| vec![50, 100])
}

fn bench_ops() -> usize {
    std::env::var("M2C_BENCH_OPS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(30)
}

struct Fixture {
    _tmp: tempfile::TempDir,
    engine: Arc<Engine>,
}

fn setup(threads: usize) -> Fixture {
    let tmp = tempfile::TempDir::new().unwrap();
    let engine = Arc::new(Engine::open(tmp.path(), EngineConfig::new(tmp.path())).unwrap());
    engine.exec(None, "CREATE TABLE t (id INT, v INT)").unwrap();
    // Preload in ONE explicit transaction: one fsync total, not one per row.
    let preload = engine.begin_txn().unwrap();
    for conn in 0..threads {
        let base = conn as i32 * RANGE;
        for i in 0..PRELOAD {
            engine
                .exec(
                    Some(&preload),
                    &format!("INSERT INTO t VALUES ({}, 0)", base + i),
                )
                .unwrap();
        }
    }
    preload.commit().unwrap();
    // The indexed column: every timed insert/delete maintains the B+Tree.
    engine.create_index("t", "id").unwrap();
    Fixture { _tmp: tmp, engine }
}

/// Run `threads` x `ops` mixed auto-commit ops; returns the number of
/// INSERTs performed (for the final row-count assertion).
fn run_mixed(engine: &Arc<Engine>, threads: usize, ops: usize) -> usize {
    let inserts = std::sync::atomic::AtomicUsize::new(0);
    std::thread::scope(|s| {
        for t in 0..threads {
            let engine = Arc::clone(engine);
            let inserts = &inserts;
            s.spawn(move || {
                let base = t as i32 * RANGE;
                let mut next_id = base + PRELOAD;
                for i in 0..ops {
                    match i % 4 {
                        // 50% INSERT (index maintenance + splits).
                        0 | 1 => {
                            engine
                                .exec(
                                    None,
                                    &format!("INSERT INTO t VALUES ({next_id}, 0)"),
                                )
                                .unwrap();
                            next_id += 1;
                            inserts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                        // 25% UPDATE (HOT: only the unindexed `v` changes).
                        2 => {
                            let id = base + (i as i32 / 4) % PRELOAD;
                            engine
                                .exec(None, &format!("UPDATE t SET v = {i} WHERE id = {id}"))
                                .unwrap();
                        }
                        // 25% point SELECT.
                        _ => {
                            let id = base + (i as i32 / 4) % PRELOAD;
                            let res = engine
                                .exec(None, &format!("SELECT v FROM t WHERE id = {id}"))
                                .unwrap();
                            assert!(
                                matches!(res, QueryResult::Rows { ref rows, .. } if rows.len() == 1),
                                "point select must return exactly one row: {res:?}"
                            );
                        }
                    }
                }
            });
        }
    });
    inserts.load(std::sync::atomic::Ordering::Relaxed)
}

fn bench_mixed_conns(c: &mut Criterion) {
    let ops = bench_ops();
    let mut group = c.benchmark_group("m2c_100_conn");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(10);

    for conns in bench_conns() {
        group.throughput(Throughput::Elements((conns * ops) as u64));
        group.bench_with_input(
            BenchmarkId::new("engine_mixed_autocommit", format!("{conns}conn_x_{ops}ops")),
            &conns,
            |b, &conns| {
                // iter_custom: only the mixed run is timed; the final
                // bookkeeping SELECT/assert and the clean shutdown are
                // teardown and must not count toward throughput.
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let fixture = setup(conns);
                        let start = Instant::now();
                        let inserted = run_mixed(&fixture.engine, conns, ops);
                        total += start.elapsed();
                        // Untimed bookkeeping: preload + inserts, nothing lost.
                        let res = fixture.engine.exec(None, "SELECT * FROM t").unwrap();
                        let QueryResult::Rows { rows, .. } = res else {
                            panic!("final scan returned {res:?}");
                        };
                        assert_eq!(
                            rows.len(),
                            conns * PRELOAD as usize + inserted,
                            "row count mismatch after mixed run"
                        );
                        fixture.engine.shutdown();
                    }
                    total
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_mixed_conns);
criterion_main!(benches);
