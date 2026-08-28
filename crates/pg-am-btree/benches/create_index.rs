//! Stage M acceptance benchmark (coding-plan Stage M): single-threaded
//! **1,000,000 INSERT + blocking CREATE INDEX ≤ 30s**.
//!
//! Structure:
//!
//! 1. One-time setup (outside criterion timing): create a table and insert
//!    1M rows in a **single transaction** through the heap AM (one commit =
//!    one fsync; per-row auto-commit would fsync per row and measure commit
//!    latency, not insert throughput). The wall time is printed as
//!    `insert_1m_secs`.
//! 2. The timed routine: `Engine::open` + `Engine::create_index` on a fresh
//!    copy of that data directory (the setup clone is not measured; the
//!    engine checkpoint at setup keeps clones small and replay empty).
//!
//! Run with: `cargo bench -p pg-am-btree --bench create_index`.

use std::fs;
use std::path::Path;
use std::time::Instant;

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};

use pg_am_heap::access_method::{InsertContext, RelationDesc};
use pg_am_heap::tuple::{encode_tuple, ColumnType, Datum, TupleHeader};
use pg_am_heap::AccessMethod;
use pg_engine::{ColumnDef, Engine, EngineConfig};
use pg_storage::types::{PageId, Tid, TxnId};
use pg_txn::Snapshot;

use tempfile::TempDir;

const ROWS: i64 = 1_000_000;

/// Insert `ROWS` rows `(i, i)` in one transaction; return the wall seconds.
fn insert_1m(engine: &Engine) -> f64 {
    let entry = engine.describe_table("t").unwrap();
    let col_types = [ColumnType::Int8, ColumnType::Int8];
    let xid = engine.txn_manager().begin_txn();
    let mut snap = Snapshot::everything();
    snap.set_current_xid(xid);
    let start = Instant::now();
    for i in 0..ROWS {
        let tuple = encode_tuple(
            TupleHeader::new(
                TxnId::INVALID,
                TxnId::INVALID,
                0,
                [0; 16],
                Tid {
                    page_id: PageId::INVALID,
                    slot_id: 0,
                },
                0,
            ),
            &col_types,
            &[Some(Datum::Int8(i)), Some(Datum::Int8(i))],
        )
        .unwrap();
        engine
            .heap()
            .insert(InsertContext {
                rel: RelationDesc {
                    rel_oid: entry.oid,
                    first_page: entry.first_page,
                    columns: &col_types,
                },
                snapshot: &snap,
                tuple: &tuple,
                out_tid: None,
            })
            .unwrap();
    }
    engine.txn_manager().commit_txn(xid).unwrap();
    start.elapsed().as_secs_f64()
}

/// Recursively copy a data directory (plain files and subdirectories).
fn clone_dir(src: &Path, dst: &Path) {
    fs::create_dir_all(dst).unwrap();
    for entry in fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let to = dst.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            clone_dir(&entry.path(), &to);
        } else {
            fs::copy(entry.path(), &to).unwrap();
        }
    }
}

/// The one-time fixture: an engine checkpointed and shut down with 1M rows
/// in table `t`, plus the measured insert wall time.
fn fixture() -> (TempDir, f64) {
    let tmp = TempDir::new().unwrap();
    let engine = Engine::open(tmp.path(), EngineConfig::new(tmp.path())).unwrap();
    engine
        .create_table(
            "t",
            &[
                ColumnDef {
                    name: "k".to_string(),
                    col_type: ColumnType::Int8,
                },
                ColumnDef {
                    name: "v".to_string(),
                    col_type: ColumnType::Int8,
                },
            ],
        )
        .unwrap();
    let insert_secs = insert_1m(&engine);
    // Checkpoint so the clones are small and their open replays nothing.
    engine.checkpoint().unwrap();
    engine.shutdown();
    (tmp, insert_secs)
}

fn bench_create_index(c: &mut Criterion) {
    let (fixture_dir, insert_secs) = fixture();
    eprintln!("insert_1m_secs = {insert_secs:.3}");

    let mut group = c.benchmark_group("create_index");
    group.sample_size(10);
    group.bench_function("open_and_create_index_1m_rows", |b| {
        b.iter_batched(
            || {
                let clone = TempDir::new().unwrap();
                clone_dir(fixture_dir.path(), clone.path());
                clone
            },
            |clone: TempDir| {
                let engine = Engine::open(clone.path(), EngineConfig::new(clone.path())).unwrap();
                engine.create_index("t", "k").unwrap();
                engine.shutdown();
            },
            BatchSize::PerIteration,
        );
    });
    group.finish();

    // The criterion report prints per-iteration create_index stats; the
    // acceptance total is insert_1m_secs (above) + one iteration.
    drop(fixture_dir);
}

criterion_group!(benches, bench_create_index);
criterion_main!(benches);
