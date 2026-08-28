//! M2c Stage P bench: uncontended single-row UPDATE TPS through the
//! auto-commit path (coding-plan Stage P acceptance: 无冲突 UPDATE
//! >= 30K TPS).
//!
//! Each thread owns a disjoint slice of preloaded rows and updates them
//! round-robin (`Engine::update`, one auto-commit transaction per update),
//! so no two threads ever touch the same row version: the row-lock
//! protocol always takes the §9.1 step-2 fast path, and the measurement
//! covers heap update + WAL + group-committed fsync, not contention.
//!
//! Run in RELEASE mode for meaningful numbers:
//!
//! ```sh
//! cargo bench -p pg-engine --bench m2c_locks --release
//! ```
//!
//! Measured (Apple Silicon, macOS, bench profile; 200 ops/thread):
//!
//! - 1T x 200 UPDATE: **~241 TPS** — bounded by raw fsync latency
//!   (F_FULLFSYNC ~4 ms/commit), matching the m2a insert reference point.
//! - 100T x 200 UPDATE (20K updates): **~11.2K TPS** — group commit
//!   amortizes fsync across concurrent committers. On par with the m2a
//!   100-thread INSERT number (~11.6K TPS): the §9.1 fast path (gate +
//!   stamp under one page latch, no waiting) adds no measurable cost on
//!   the uncontended path.
//!
//! The plan's >= 30K TPS aspiration is NOT reached on this hardware with
//! per-commit fsync semantics — the same physical ceiling the
//! m2a_100_threads bench header documents for Stage K, not a regression of
//! this stage. Numbers are reported, not gated.
//!
//! `M2C_BENCH_THREADS` (default 100), `M2C_BENCH_OPS` (updates per thread
//! per timed iteration, default 200), and `M2C_BENCH_ROWS_PER_THREAD`
//! (preloaded rows per thread slice, default 100) shrink the run for smoke
//! testing.

use std::sync::Arc;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use pg_engine::{ColumnDef, ColumnType, Datum, Engine, EngineConfig, Tid};

fn bench_threads() -> usize {
    std::env::var("M2C_BENCH_THREADS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100)
}

fn bench_ops() -> usize {
    std::env::var("M2C_BENCH_OPS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200)
}

fn rows_per_thread() -> usize {
    std::env::var("M2C_BENCH_ROWS_PER_THREAD")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100)
}

struct Fixture {
    _tmp: tempfile::TempDir,
    engine: Arc<Engine>,
    /// CURRENT row TIDs, laid out as `threads` contiguous slices of
    /// `rows_per_thread` each — thread `t` only ever updates slice `t`.
    /// An update creates a new version at a new TID, so every batch
    /// rewrites its slices with the TIDs it produced (the mutex is taken
    /// only at batch start/end, never per update).
    tids: std::sync::Mutex<Vec<Tid>>,
}

fn setup(threads: usize) -> Fixture {
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
                    name: "v".to_string(),
                    col_type: ColumnType::Int4,
                },
            ],
        )
        .unwrap();
    let total = threads * rows_per_thread();
    // Preload in multi-row INSERT statements (one fsync per statement, not
    // per row — 10K single-row auto-commit inserts would take ~40 s), then
    // recover the TIDs with a full scan.
    let mut loaded = 0usize;
    while loaded < total {
        let batch = (total - loaded).min(500);
        let values: Vec<String> = (loaded..loaded + batch)
            .map(|i| format!("({i}, 0)"))
            .collect();
        engine
            .exec(None, &format!("INSERT INTO t VALUES {}", values.join(", ")))
            .unwrap();
        loaded += batch;
    }
    let tids: Vec<Tid> = engine
        .scan("t", None)
        .unwrap()
        .into_iter()
        .map(|(tid, _)| tid)
        .collect();
    assert_eq!(tids.len(), total, "preload row count mismatch");
    Fixture {
        _tmp: tmp,
        engine,
        tids: std::sync::Mutex::new(tids),
    }
}

/// Run one timed batch: `threads` threads × `ops` uncontended updates.
///
/// Each thread works on a private copy of its TID slice and returns the
/// post-batch TIDs; the fixture's TID list is rewritten after the join so
/// the next criterion iteration starts from live row versions (updating a
/// stale TID is a committed-conflict error, §9.1 step 3).
fn run_update_batch(
    engine: &Arc<Engine>,
    tids: &std::sync::Mutex<Vec<Tid>>,
    threads: usize,
    ops: usize,
) {
    let snapshot = tids.lock().unwrap().clone();
    let per_slice = snapshot.len() / threads;
    let mut new_slices: Vec<Vec<Tid>> = Vec::with_capacity(threads);
    std::thread::scope(|s| {
        let mut handles = Vec::with_capacity(threads);
        for t in 0..threads {
            let engine = Arc::clone(engine);
            let slice = snapshot[t * per_slice..(t + 1) * per_slice].to_vec();
            handles.push(s.spawn(move || {
                let mut current = slice;
                for i in 0..ops {
                    let slot = i % current.len();
                    let new_tid = engine
                        .update(
                            "t",
                            current[slot],
                            &[Some(Datum::Int4(0)), Some(Datum::Int4(i as i32))],
                        )
                        .unwrap();
                    current[slot] = new_tid;
                }
                current
            }));
        }
        for h in handles {
            new_slices.push(h.join().unwrap());
        }
    });
    let mut guard = tids.lock().unwrap();
    guard.clear();
    guard.extend(new_slices.into_iter().flatten());
}

fn bench_uncontended_update_tps(c: &mut Criterion) {
    let ops = bench_ops();
    let mut group = c.benchmark_group("m2c_uncontended_update");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(10);

    let mut configs = vec![1usize, bench_threads()];
    configs.sort_unstable();
    configs.dedup();
    for &threads in &configs {
        let fixture = setup(threads);
        group.throughput(Throughput::Elements((threads * ops) as u64));
        group.bench_with_input(
            BenchmarkId::new("engine_update_autocommit", format!("{threads}T_x_{ops}ops")),
            &threads,
            |b, &t| {
                b.iter(|| run_update_batch(&fixture.engine, &fixture.tids, t, ops));
            },
        );
        fixture.engine.shutdown();
    }
    group.finish();
}

criterion_group!(benches, bench_uncontended_update_tps);
criterion_main!(benches);
