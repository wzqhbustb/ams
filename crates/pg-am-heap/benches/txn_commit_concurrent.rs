//! Concurrent commit throughput: the Stage J 30K ops/s acceptance.
//!
//! A single thread paying one fsync per commit is bounded by raw fsync
//! latency (macOS F_FULLFSYNC ≈ 4 ms — ~220 commits/s). The 30K target is
//! only meaningful as a CONCURRENT number: many committers share each fsync
//! via group commit. This bench measures `begin → insert 1 row → commit`
//! across 1 / 8 / 32 / 100 threads and reports end-to-end ops/s.
//!
//! Reference numbers (Apple Silicon, release): 1T ≈ 220 ops/s, 8T ≈ 1.1K,
//! 32T ≈ 4K, 100T ≈ 12K ops/s — near-linear wave batching after the WAL
//! writer stopped holding its mutex across fsync (≈ 1K ops/s at 100T before
//! that fix). 30K+ needs ~250 concurrent committers or a faster fsync path.

use std::sync::Arc;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use pg_am_heap::access_method::{AccessMethod, InsertContext, RelationDesc};
use pg_am_heap::tuple::{encode_tuple, ColumnType, Datum, TupleHeader};
use pg_am_heap::HeapAM;
use pg_storage::config::StorageConfig;
use pg_storage::engine::StorageEngine;
use pg_storage::types::{Oid, PageId, Tid, TxnId};
use pg_txn::{ClogAccessor, CommitWal, InMemoryClogAccessor, Snapshot, TxnManager};

const COLUMNS: [ColumnType; 2] = [ColumnType::Int4, ColumnType::Text];
const REL_OID: Oid = Oid(16_385);
const OPS_PER_THREAD: usize = 50;

fn rel(first_page: PageId) -> RelationDesc<'static> {
    RelationDesc {
        rel_oid: REL_OID,
        first_page,
        columns: &COLUMNS,
    }
}

fn encode_row(xid: TxnId, id: i32) -> Vec<u8> {
    let header = TupleHeader::new(
        xid,
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
            Some(Datum::Text("bench".to_string())),
        ],
    )
    .unwrap()
}

struct Fixture {
    _tmp: tempfile::TempDir,
    engine: StorageEngine,
    mgr: Arc<TxnManager>,
    heap: HeapAM,
    first_page: PageId,
}

fn setup(_threads: usize) -> Fixture {
    let tmp = tempfile::TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());
    let engine = StorageEngine::open(tmp.path(), &config).unwrap();
    let clog: Arc<dyn ClogAccessor> = Arc::new(InMemoryClogAccessor::new());
    let wal: Arc<dyn CommitWal> = Arc::clone(engine.wal_writer()) as Arc<dyn CommitWal>;
    let mgr = Arc::new(TxnManager::new(engine.txn_id_clock(), wal, clog));
    let heap = HeapAM::new(
        Arc::clone(engine.buffer_pool()),
        Arc::clone(engine.wal_writer()),
    );
    let first_page = heap.create_heap(REL_OID).unwrap();
    Fixture {
        _tmp: tmp,
        engine,
        mgr,
        heap,
        first_page,
    }
}

fn bench_concurrent_commit(c: &mut Criterion) {
    let mut group = c.benchmark_group("txn_commit_concurrent");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(10);

    for &threads in &[1usize, 8, 32, 100] {
        group.throughput(Throughput::Elements((threads * OPS_PER_THREAD) as u64));
        group.bench_with_input(
            BenchmarkId::new("insert_commit", threads),
            &threads,
            |b, &t| {
                b.iter_with_setup(
                    || setup(t),
                    |fixture| {
                        let Fixture {
                            engine,
                            mgr,
                            heap,
                            first_page,
                            ..
                        } = fixture;
                        std::thread::scope(|s| {
                            for _ in 0..t {
                                let (mgr, heap) = (&mgr, &heap);
                                s.spawn(move || {
                                    for i in 0..OPS_PER_THREAD {
                                        let xid = mgr.begin_txn();
                                        let mut snap = Snapshot::everything();
                                        snap.set_current_xid(xid);
                                        let tuple = encode_row(xid, i as i32);
                                        heap.insert(InsertContext {
                                            rel: rel(first_page),
                                            snapshot: &snap,
                                            tuple: &tuple,
                                            out_tid: None,
                                        })
                                        .unwrap();
                                        mgr.commit_txn(xid).unwrap();
                                    }
                                });
                            }
                        });
                        engine.shutdown();
                    },
                );
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_concurrent_commit);
criterion_main!(benches);
