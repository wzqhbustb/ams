//! HNSW build/search throughput benchmark — Phase 2 M4 Stage E (coding plan
//! Stage E "criterion bench" row): synthetic 10k x 128d, guarding against
//! performance regressions (aligned with the pg-storage bench precedent:
//! criterion 0.5, `harness = false`).
//!
//! These numbers are for REGRESSION DETECTION (relative change between
//! runs/commits), not absolute performance claims — the absolute-口径
//! figures live in docs/phase2-m4-benchmarks.md, produced by
//! `examples/m4_recall_probe.rs` on real datasets (siftsmall/sift/gist).
//! Synthetic data only: benches must not depend on real datasets (CI
//! nightly smoke runs them dataset-less).
//!
//! Data generation reuses the crate's test-only `MixtureGen` via `#[path]`
//! (2026-09-08, Stage E: the least-invasive option — duplicating the
//! generator would violate the single-implementation discipline, and moving
//! it into src/ would ship test tooling in the public API; the module is
//! deterministic and `rand`-free, §4.1/§10).
//!
//! Parameters are the frozen acceptance set (coding plan Stage D): M=16,
//! m_max0=32, ef_construction=200, ef_search=64, fixed seed — same numbers
//! the recall gate uses, so a slowdown here maps 1:1 onto the gated config.

use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use pg_am_hnsw::{Hnsw, HnswParams, Metric};

#[path = "../tests/common/mod.rs"]
mod common;

const N: usize = 10_000;
const DIM: usize = 128;
const QUERIES: usize = 100;
const EF_SEARCH: usize = 64;

/// Frozen acceptance parameters (see module docs).
fn params() -> HnswParams {
    HnswParams::new(16, 32, 200, EF_SEARCH as u32).unwrap()
}

/// Generate the base and query sets once per bench run; generation time is
/// setup, never measured (Criterion excludes iter_with_setup's setup, and
/// bench 2 builds its graph outside the measured loop).
fn generate() -> (Vec<Vec<f32>>, Vec<Vec<f32>>) {
    let mut gen = common::MixtureGen::new(DIM, 8, 0xDA7A);
    let base = (0..N).map(|_| gen.next_vec()).collect();
    let queries = (0..QUERIES).map(|_| gen.next_vec()).collect();
    (base, queries)
}

/// Bench 1: build throughput — 10k inserts into a fresh graph per
/// iteration, reported as inserts/sec. sample_size(10) (Criterion's
/// minimum): one iteration costs seconds, so the default 100 samples would
/// blow the nightly smoke budget; regression detection does not need
/// tighter statistics than this.
fn build_throughput(c: &mut Criterion) {
    let (base, _queries) = generate();
    let mut group = c.benchmark_group("hnsw_build");
    group.throughput(Throughput::Elements(N as u64));
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(30));
    group.bench_function("insert_10k_128d", |b| {
        b.iter_with_setup(
            || Hnsw::new(DIM as u16, Metric::L2, params(), 42).unwrap(),
            |mut g| {
                for v in &base {
                    g.insert(v).unwrap();
                }
            },
        );
    });
    group.finish();
}

/// Bench 2: query throughput — 100 fixed queries at ef=64 against a graph
/// built ONCE outside the measured loop, reported as queries/sec.
fn search_throughput(c: &mut Criterion) {
    let (base, queries) = generate();
    let mut g = Hnsw::new(DIM as u16, Metric::L2, params(), 42).unwrap();
    for v in &base {
        g.insert(v).unwrap();
    }
    let mut group = c.benchmark_group("hnsw_search");
    group.throughput(Throughput::Elements(QUERIES as u64));
    group.bench_function("search_100_ef64", |b| {
        b.iter(|| {
            for q in &queries {
                let _ = g.search(q, 10, Some(EF_SEARCH)).unwrap();
            }
        });
    });
    group.finish();
}

criterion_group!(benches, build_throughput, search_throughput);
criterion_main!(benches);
