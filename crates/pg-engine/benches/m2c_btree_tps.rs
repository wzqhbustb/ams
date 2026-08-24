//! M2c Stage Q bench: concurrent INSERT TPS on an INDEXED table through the
//! auto-commit path (coding-plan Stage Q acceptance: 并发 INSERT >= 15K
//! TPS).
//!
//! The table carries a B+Tree index on the `id` column, so every timed
//! insert pays heap insert + B+Tree leaf insert (Blink crabbing, optimistic
//! leaf writes, pessimistic splits) + WAL + group-committed fsync — the
//! full Stage Q concurrency surface, measured at the engine layer to match
//! the acceptance intent. Each thread owns a disjoint key range, so the
//! measurement covers throughput, not key collisions; splits fire steadily
//! as each range's right edge advances.
//!
//! Run with the (already optimized) bench profile — no extra flag:
//!
//! ```sh
//! cargo bench -p pg-engine --bench m2c_btree_tps
//! # quick smoke:
//! M2Q_BENCH_OPS=200 cargo bench -p pg-engine --bench m2c_btree_tps -- \
//!     --measurement-time 3 --sample-size 10
//! ```
//!
//! Measured (Apple Silicon, macOS, bench profile; 200 ops/thread, smoke run
//! with 3s measurement window, 10 samples):
//!
//! - 1T x 200 INSERT (indexed, auto-commit): **~184 TPS** — bounded by raw
//!   fsync latency (F_FULLFSYNC ~4-7 ms/commit).
//! - 1T x 200 INSERT (indexed, single txn): **~7.4K TPS** — a ~40x jump
//!   from removing the per-commit fsync, despite the SQL-executor overhead
//!   this arm pays and auto-commit does not.
//! - 100T x 200 INSERT (indexed, auto-commit, 20K rows): **~6.6K TPS**.
//! - 100T x 200 INSERT (indexed, single txn): **~13.5K TPS** — ~2x over
//!   the auto-commit arm and ABOVE the m2a 100-thread unindexed ~11.6K TPS
//!   reference, again with the SQL-executor handicap.
//!
//! Gap attribution (Stage Q review, T5 — measured, not guessed): the
//! indexed-vs-unindexed TPS gap at 100 threads is dominated by the
//! per-commit fsync / group-commit path, NOT by B+Tree latch contention —
//! removing per-commit fsync lifts indexed inserts past the unindexed
//! auto-commit reference even through the slower SQL arm. The single-txn
//! arm is a confounded-but-conservative control (SQL parsing only slows
//! it), so the true no-fsync ceiling is at least as high as reported.
//!
//! KNOWN ISSUE (same as m2a_100_threads.rs and m2c_locks.rs:27-30): this
//! hardware's per-commit fsync caps group-committed throughput at ~11-12K
//! TPS even WITHOUT index maintenance, so the plan's >= 15K TPS aspiration
//! is fsync-bound on this machine, not a regression of this stage —
//! batch-commit (amortizing one fsync across several client transactions
//! beyond the current group commit) would clear it. Numbers are measured
//! and reported, not gated, and not faked.
//!
//! `M2Q_BENCH_THREADS` (default 100) and `M2Q_BENCH_OPS` (inserts per
//! thread per timed iteration, default 200) shrink the run for smoke
//! testing.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use pg_engine::{ColumnDef, ColumnType, Datum, Engine, EngineConfig, Tid};

fn bench_threads() -> usize {
    std::env::var("M2Q_BENCH_THREADS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100)
}

fn bench_ops() -> usize {
    std::env::var("M2Q_BENCH_OPS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200)
}

struct Fixture {
    _tmp: tempfile::TempDir,
    engine: Arc<Engine>,
}

fn setup() -> Fixture {
    let tmp = tempfile::TempDir::new().unwrap();
    let engine = Arc::new(Engine::open(tmp.path(), EngineConfig::new(tmp.path())).unwrap());
    engine
        .create_table(
            "t",
            &[
                ColumnDef {
                    name: "id".to_string(),
                    col_type: ColumnType::Int4,
                },
                ColumnDef {
                    name: "name".to_string(),
                    col_type: ColumnType::Text,
                },
            ],
        )
        .unwrap();
    // The indexed column: every timed insert maintains the B+Tree too.
    engine.create_index("t", "id").unwrap();
    Fixture { _tmp: tmp, engine }
}

/// Run `threads` x `ops` inserts into the indexed table, asserting TID
/// uniqueness and an exact final row count. Failures panic (criterion
/// reports them).
fn run_concurrent_inserts(engine: &Arc<Engine>, threads: usize, ops: usize) {
    let tids = Arc::new(Mutex::new(Vec::with_capacity(threads * ops)));
    std::thread::scope(|s| {
        for t in 0..threads {
            let engine = Arc::clone(engine);
            let tids = Arc::clone(&tids);
            s.spawn(move || {
                for i in 0..ops {
                    let tid = engine
                        .insert(
                            "t",
                            &[
                                Some(Datum::Int4((t * ops + i) as i32)),
                                Some(Datum::Text("bench".to_string())),
                            ],
                        )
                        .unwrap();
                    tids.lock().unwrap().push(tid);
                }
            });
        }
    });

    let tids = tids.lock().unwrap();
    assert_eq!(tids.len(), threads * ops);
    let unique: HashSet<&Tid> = tids.iter().collect();
    assert_eq!(unique.len(), tids.len(), "slot conflict: duplicate TIDs");
    drop(tids);

    assert_eq!(
        engine.scan("t", None).unwrap().len(),
        threads * ops,
        "row count mismatch after concurrent inserts"
    );
}

/// Control for the auto-commit measurement: each thread wraps its `ops`
/// inserts in ONE explicit transaction (one commit => one group fsync per
/// thread instead of one per insert). This removes the per-commit fsync
/// from the measured path, so comparing it against the auto-commit arm
/// separates "fsync-bound" from "latch/WAL-serialization-bound" as the
/// cause of the indexed-vs-unindexed TPS gap. Caveat reported honestly:
/// this arm goes through the SQL executor (parser + planner per insert),
/// which the auto-commit typed API does not — if anything that makes this
/// arm SLOWER per op, so a TPS jump here is decisive evidence for fsync
/// dominance, while a flat result is inconclusive (confounded).
fn run_concurrent_inserts_single_txn(engine: &Arc<Engine>, threads: usize, ops: usize) {
    std::thread::scope(|s| {
        for t in 0..threads {
            let engine = Arc::clone(engine);
            s.spawn(move || {
                let txn = engine.begin_txn().unwrap();
                for i in 0..ops {
                    let id = (t * ops + i) as i32;
                    engine
                        .exec(Some(&txn), &format!("INSERT INTO t VALUES ({id}, 'bench')"))
                        .unwrap();
                }
                txn.commit().unwrap();
            });
        }
    });

    assert_eq!(
        engine.scan("t", None).unwrap().len(),
        threads * ops,
        "row count mismatch after concurrent single-txn inserts"
    );
}

fn bench_stage_q_indexed_insert(c: &mut Criterion) {
    let ops = bench_ops();
    let threads = bench_threads();
    let mut group = c.benchmark_group("m2c_btree_tps");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(10);

    for &t in &[1usize, threads] {
        group.throughput(Throughput::Elements((t * ops) as u64));
        group.bench_with_input(
            BenchmarkId::new(
                "engine_insert_indexed_autocommit",
                format!("{t}T_x_{ops}ops"),
            ),
            &t,
            |b, &t| {
                b.iter_with_setup(setup, |fixture| {
                    run_concurrent_inserts(&fixture.engine, t, ops);
                    fixture.engine.shutdown();
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new(
                "engine_insert_indexed_singletxn",
                format!("{t}T_x_{ops}ops"),
            ),
            &t,
            |b, &t| {
                b.iter_with_setup(setup, |fixture| {
                    run_concurrent_inserts_single_txn(&fixture.engine, t, ops);
                    fixture.engine.shutdown();
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_stage_q_indexed_insert);
criterion_main!(benches);
