//! End-to-end single-thread INSERT throughput for the heap AM.
//!
//! Three benches decompose the cost:
//!
//! - `heap_insert_e2e` — default config: `HeapAM::insert` with the group-commit
//!   worker fsyncing every 2 ms / 64 records in the background. The worker
//!   holds the writer mutex across `sync_all` (~4 ms F_FULLFSYNC on macOS), so
//!   appends stall behind every background fsync; this bench includes those
//!   stalls, which dominate its per-op time.
//! - `heap_insert_no_fsync` — batch/timeout set out of reach: the pure insert
//!   path (page pin + WAL encode/append to OS cache + slotted write) with no
//!   fsync interference. This is the Stage I "pure heap AM path" number and the
//!   ceiling any fsync scheduling can approach.
//! - `heap_txn_insert_commit` — the real M2a unit of work: `begin_txn` →
//!   `insert` → `commit_txn` through `TxnManager`, where commit fsyncs
//!   (`flush_to`). Single-threaded this is bound by the device fsync latency;
//!   30K ops/s is only reachable by amortizing that fsync across many
//!   concurrent committers (group commit), not by a single thread.

use std::sync::Arc;
use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, Criterion};

use pg_am_heap::access_method::{AccessMethod, InsertContext, RelationDesc};
use pg_am_heap::tuple::{encode_tuple, ColumnType, Datum, TupleHeader};
use pg_am_heap::HeapAM;

use pg_storage::config::StorageConfig;
use pg_storage::engine::StorageEngine;
use pg_storage::types::{Oid, PageId, Tid, TxnId};

use pg_txn::{ClogAccessor, CommitWal, InMemoryClogAccessor, Snapshot, TxnManager};

use tempfile::TempDir;

const COLUMNS: [ColumnType; 2] = [ColumnType::Int4, ColumnType::Text];
const REL_OID: Oid = Oid(16_384);

/// Shared writer XID for the insert-only benches. Visibility is never
/// consulted on the insert path (no scan), so one constant is fine — but the
/// tuple header's `t_xmin` and the snapshot/WAL stamp must agree, otherwise
/// the bench silently measures a misconfigured writer.
const BENCH_XID: TxnId = TxnId(100);

fn encode_row(id: i32) -> Vec<u8> {
    let header = TupleHeader::new(
        BENCH_XID,
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
            Some(Datum::Text(format!("row-{id}"))),
        ],
    )
    .unwrap()
}

struct Harness {
    _tmp: TempDir,
    engine: StorageEngine,
    heap: HeapAM,
    first_page: PageId,
}

fn harness(config_tweak: impl FnOnce(&mut StorageConfig)) -> Harness {
    let tmp = TempDir::new().unwrap();
    let mut config = StorageConfig::new(tmp.path());
    config_tweak(&mut config);
    let engine = StorageEngine::open(tmp.path(), &config).unwrap();
    let heap = HeapAM::new(
        Arc::clone(engine.buffer_pool()),
        Arc::clone(engine.wal_writer()),
    );
    let first_page = heap.create_heap(REL_OID).unwrap();
    Harness {
        _tmp: tmp,
        engine,
        heap,
        first_page,
    }
}

fn insert_loop_bench(c: &mut Criterion, name: &str, h: &Harness) {
    let mut snap = Snapshot::everything();
    snap.set_current_xid(BENCH_XID);

    // Pre-encode a pool of rows so the timed loop measures the pure insert
    // path (page acquire + WAL append + slotted write), not tuple encoding.
    let pool: Vec<Vec<u8>> = (0..1024).map(encode_row).collect();
    let mut next = 0usize;

    // Cap the run: every iteration permanently grows the heap and the WAL, so
    // unbounded criterion iteration counts turn the bench into an I/O soak
    // test (tens of millions of rows) instead of a latency measurement.
    let mut group = c.benchmark_group("insert");
    group
        .sample_size(20)
        .measurement_time(Duration::from_secs(3))
        .warm_up_time(Duration::from_secs(1));
    group.bench_function(name, |b| {
        b.iter(|| {
            let tuple = &pool[next % pool.len()];
            next = next.wrapping_add(1);
            h.heap
                .insert(InsertContext {
                    rel: RelationDesc {
                        rel_oid: REL_OID,
                        first_page: h.first_page,
                        columns: &COLUMNS,
                    },
                    snapshot: &snap,
                    tuple: black_box(tuple),
                    out_tid: None,
                })
                .unwrap();
        })
    });
    group.finish();
}

/// Default config: background group-commit fsync every 2 ms / 64 records.
fn bench_heap_insert(c: &mut Criterion) {
    let h = harness(|_| {});
    insert_loop_bench(c, "heap_insert_e2e", &h);
}

/// Fsync pushed out of reach: isolates the append path (no fsync stalls).
/// The engine's Drop still flushes everything on shutdown.
fn bench_heap_insert_no_fsync(c: &mut Criterion) {
    let h = harness(|cfg| {
        cfg.wal_group_commit_batch_size = usize::MAX;
        cfg.wal_group_commit_timeout_ms = 3_600_000;
    });
    insert_loop_bench(c, "heap_insert_no_fsync", &h);
}

/// The real M2a transaction unit: begin → insert → commit (durable fsync).
/// Single-threaded, so each iteration pays one full device fsync.
fn bench_txn_insert_commit(c: &mut Criterion) {
    let h = harness(|_| {});
    let clog: Arc<dyn ClogAccessor> = Arc::new(InMemoryClogAccessor::new());
    let wal: Arc<dyn CommitWal> = Arc::clone(h.engine.wal_writer()) as Arc<dyn CommitWal>;
    let mgr = TxnManager::new(h.engine.txn_id_clock(), wal, clog);

    let pool: Vec<Vec<u8>> = (0..1024).map(encode_row).collect();
    let mut next = 0usize;

    let mut group = c.benchmark_group("txn");
    // Each iteration fsyncs (~ms on macOS F_FULLFSYNC); keep the run short.
    group
        .sample_size(10)
        .measurement_time(Duration::from_secs(8))
        .warm_up_time(Duration::from_secs(1));
    group.bench_function("txn_insert_commit_e2e", |b| {
        b.iter(|| {
            let xid = mgr.begin_txn();
            let mut snap = Snapshot::everything();
            snap.set_current_xid(xid);
            let tuple = &pool[next % pool.len()];
            next = next.wrapping_add(1);
            h.heap
                .insert(InsertContext {
                    rel: RelationDesc {
                        rel_oid: REL_OID,
                        first_page: h.first_page,
                        columns: &COLUMNS,
                    },
                    snapshot: &snap,
                    tuple: black_box(tuple),
                    out_tid: None,
                })
                .unwrap();
            mgr.commit_txn(xid).unwrap();
        })
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_heap_insert,
    bench_heap_insert_no_fsync,
    bench_txn_insert_commit
);
criterion_main!(benches);
