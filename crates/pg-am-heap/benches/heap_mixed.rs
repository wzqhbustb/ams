//! Stage T benchmark inventory: heap INSERT-UPDATE-DELETE mixed throughput.
//!
//! The Stage T benchmark set (coding-plan Stage T, `docs/phase1-m2-benchmarks.md`)
//! calls for a heap mixed-DML bench alongside `heap_insert` (pure INSERT)
//! and `txn_commit_concurrent` (insert+commit). This bench's unit of work
//! is one transaction that INSERTs a row, UPDATEs it, and DELETEs it
//! (3 heap ops), committed once — begin → insert → update → delete →
//! commit through `TxnManager`, at 1 / 8 / 32 / 100 threads.
//!
//! Fixture and concurrency shape follow `txn_commit_concurrent.rs`: the
//! InMemoryClogAccessor keeps the AM-level liveness checks honest, commit
//! fsyncs via group commit, and per-op tuples are tracked through
//! `InsertContext::out_tid` so no scan is needed to address them.
//!
//! Run with: `cargo bench -p pg-am-heap --bench heap_mixed`
//! Smoke: `HEAP_MIXED_OPS=5 cargo bench -p pg-am-heap --bench heap_mixed -- \
//!     --measurement-time 2 --sample-size 10`

use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use pg_am_heap::access_method::{
    AccessMethod, DeleteContext, InsertContext, RelationDesc, UpdatableAM, UpdateContext,
};
use pg_am_heap::tuple::{encode_tuple, ColumnType, Datum, TupleHeader};
use pg_am_heap::HeapAM;
use pg_storage::config::StorageConfig;
use pg_storage::engine::StorageEngine;
use pg_storage::types::{Oid, PageId, Tid, TxnId};
use pg_txn::{ClogAccessor, CommitWal, InMemoryClogAccessor, Snapshot, TxnManager};

const COLUMNS: [ColumnType; 2] = [ColumnType::Int4, ColumnType::Text];
const REL_OID: Oid = Oid(16_386);

fn ops_per_thread() -> usize {
    std::env::var("HEAP_MIXED_OPS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(30)
}

fn rel(first_page: PageId) -> RelationDesc<'static> {
    RelationDesc {
        rel_oid: REL_OID,
        first_page,
        columns: &COLUMNS,
    }
}

fn encode_row(xid: TxnId, id: i32, tag: &str) -> Vec<u8> {
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
        &[Some(Datum::Int4(id)), Some(Datum::Text(tag.to_string()))],
    )
    .unwrap()
}

struct Fixture {
    _tmp: tempfile::TempDir,
    engine: StorageEngine,
    mgr: Arc<TxnManager>,
    clog: Arc<dyn ClogAccessor>,
    heap: HeapAM,
    first_page: PageId,
}

fn setup() -> Fixture {
    let tmp = tempfile::TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());
    let engine = StorageEngine::open(tmp.path(), &config).unwrap();
    // ONE shared clog: the txn manager records commit/abort states into it
    // and the AM liveness checks read the same instance.
    let clog: Arc<dyn ClogAccessor> = Arc::new(InMemoryClogAccessor::new());
    let wal: Arc<dyn CommitWal> = Arc::clone(engine.wal_writer()) as Arc<dyn CommitWal>;
    let mgr = Arc::new(TxnManager::new(
        engine.txn_id_clock(),
        wal,
        Arc::clone(&clog),
    ));
    let heap = HeapAM::new(
        Arc::clone(engine.buffer_pool()),
        Arc::clone(engine.wal_writer()),
    );
    let first_page = heap.create_heap(REL_OID).unwrap();
    Fixture {
        _tmp: tmp,
        engine,
        mgr,
        clog,
        heap,
        first_page,
    }
}

/// One mixed unit of work: insert + update + delete in ONE transaction
/// (one commit fsync), all addressing the row through its tracked TID.
fn mixed_unit(
    mgr: &TxnManager,
    clog: &dyn ClogAccessor,
    heap: &HeapAM,
    first_page: PageId,
    i: i32,
) {
    let xid = mgr.begin_txn();
    let mut snap = Snapshot::everything();
    snap.set_current_xid(xid);

    let mut tid = Tid {
        page_id: PageId::INVALID,
        slot_id: 0,
    };
    heap.insert(InsertContext {
        rel: rel(first_page),
        snapshot: &snap,
        tuple: &encode_row(xid, i, "ins"),
        out_tid: Some(&mut tid),
    })
    .unwrap();

    let mut new_tid = Tid {
        page_id: PageId::INVALID,
        slot_id: 0,
    };
    heap.update(UpdateContext {
        rel: rel(first_page),
        snapshot: &snap,
        old_tid: tid,
        new_tuple: &encode_row(xid, i, "upd"),
        out_tid: Some(&mut new_tid),
        clog,
        hot_eligible: true,
    })
    .unwrap();

    heap.delete(DeleteContext {
        rel: rel(first_page),
        snapshot: &snap,
        tid: new_tid,
        clog,
    })
    .unwrap();

    mgr.commit_txn(xid).unwrap();
}

fn bench_heap_mixed(c: &mut Criterion) {
    let ops = ops_per_thread();
    let mut group = c.benchmark_group("heap_mixed");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(10);

    for &threads in &[1usize, 8, 32, 100] {
        // Throughput counts HEAP OPERATIONS (3 per transaction unit).
        group.throughput(Throughput::Elements((threads * ops * 3) as u64));
        group.bench_with_input(
            BenchmarkId::new("insert_update_delete_commit", threads),
            &threads,
            |b, &t| {
                // iter_custom: only the concurrent mixed units are timed;
                // the clean shutdown is teardown and must not count toward
                // throughput.
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let fixture = setup();
                        let start = Instant::now();
                        {
                            let Fixture {
                                mgr,
                                clog,
                                heap,
                                first_page,
                                ..
                            } = &fixture;
                            std::thread::scope(|s| {
                                for _ in 0..t {
                                    let (mgr, clog, heap) = (mgr, clog, heap);
                                    s.spawn(move || {
                                        for i in 0..ops {
                                            mixed_unit(
                                                mgr,
                                                clog.as_ref(),
                                                heap,
                                                *first_page,
                                                i as i32,
                                            );
                                        }
                                    });
                                }
                            });
                        }
                        total += start.elapsed();
                        fixture.engine.shutdown();
                    }
                    total
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_heap_mixed);
criterion_main!(benches);
