//! M6 Stage F (coding-plan 行 300): the siftsmall benchmark for the
//! `phase2-m6-benchmarks.md` comparison — recall@10, per-query latency,
//! and insert throughput of the M6 transactional HNSW path, measured on
//! the SAME dataset used by the pgvector harness (10k x 128d, 100
//! queries, ground-truth ivecs).
//!
//! Manual run (ignored by default so CI never pays the 10k build):
//!
//! ```sh
//! cargo test -p pg-engine --test m6_benchmark --release -- --ignored --nocapture
//! ```

use std::collections::HashMap;
use std::time::{Duration, Instant};

use pg_am_heap::ColumnType;
use pg_am_hnsw::dataset::{read_fvecs, read_ivecs, recall_at_k};
use pg_am_hnsw::graph::NeighborSelection;
use pg_am_hnsw::{HnswParams, Metric, NodeId};
use pg_engine::{ColumnDef, Engine, EngineConfig};
use pg_storage::types::{Oid, Tid};
use pg_txn::Snapshot;
use tempfile::TempDir;

const DIM: u16 = 128;

fn dataset_dir() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("datasets/siftsmall")
}

fn create_index(engine: &Engine, table: &str) -> Oid {
    let (oid, _meta) = engine
        .create_hnsw_index_on(
            table,
            "v",
            HnswParams::default(),
            DIM,
            Metric::L2,
            NeighborSelection::Heuristic,
            42,
        )
        .unwrap();
    oid
}

#[test]
#[ignore = "manual benchmark (10k build); run with --ignored"]
fn m6_benchmark_siftsmall() {
    let dir = dataset_dir();
    let base = read_fvecs(dir.join("siftsmall_base.fvecs")).unwrap();
    let queries = read_fvecs(dir.join("siftsmall_query.fvecs")).unwrap();
    let gt = read_ivecs(dir.join("siftsmall_groundtruth.ivecs")).unwrap();
    assert_eq!(base.len(), 10_000);
    assert_eq!(queries.len(), 100);

    let tmp = TempDir::new().unwrap();
    let engine = Engine::open(tmp.path(), EngineConfig::new(tmp.path())).unwrap();
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
                    col_type: ColumnType::vector(u32::from(DIM)).unwrap(),
                },
            ],
        )
        .unwrap();
    create_index(&engine, "t");

    // Bulk insert: all 10k in ONE explicit transaction (one fsync).
    let bulk_started = Instant::now();
    let txn = engine.begin_txn().unwrap();
    for (i, v) in base.iter().enumerate() {
        let values: String = v
            .iter()
            .map(|x| format!("{x}"))
            .collect::<Vec<_>>()
            .join(", ");
        engine
            .exec(
                Some(&txn),
                &format!("INSERT INTO t VALUES ({i}, [{values}])"),
            )
            .unwrap();
    }
    txn.commit().unwrap();
    let bulk_elapsed = bulk_started.elapsed();
    eprintln!(
        "M6-BENCH bulk_insert: {} rows in {:.2}s = {:.0} rows/s (single txn)",
        base.len(),
        bulk_elapsed.as_secs_f64(),
        base.len() as f64 / bulk_elapsed.as_secs_f64()
    );

    // tid -> id map for recall scoring.
    let rows = engine.scan("t", None).unwrap();
    let tid_to_id: HashMap<Tid, i32> = rows
        .into_iter()
        .map(|(tid, vals)| {
            let id = match &vals[0] {
                Some(pg_am_heap::Datum::Int4(i)) => *i,
                other => panic!("unexpected id datum: {other:?}"),
            };
            (tid, id)
        })
        .collect();

    let measure = |tag: &str| {
        let mut retrieved: Vec<Vec<NodeId>> = Vec::with_capacity(queries.len());
        let mut latencies: Vec<Duration> = Vec::with_capacity(queries.len());
        for q in &queries {
            let started = Instant::now();
            let hits = engine
                .hnsw_search_visible("t", q, 10, Some(64), &Snapshot::everything())
                .unwrap();
            latencies.push(started.elapsed());
            retrieved.push(
                hits.iter()
                    .map(|(tid, _)| NodeId(*tid_to_id.get(tid).unwrap() as u32))
                    .collect(),
            );
        }
        let recall = recall_at_k(&gt, &retrieved, 10, base.len() as u32).unwrap();
        let mut micros: Vec<f64> = latencies.iter().map(|d| d.as_secs_f64() * 1000.0).collect();
        micros.sort_by(|a, b| a.total_cmp(b));
        let mean = micros.iter().sum::<f64>() / micros.len() as f64;
        eprintln!(
            "M6-BENCH {tag}: recall@10={recall:.4}; query latency mean={mean:.3}ms p50={:.3}ms p95={:.3}ms max={:.3}ms (ef=64)",
            micros[micros.len() / 2],
            micros[(micros.len() as f64 * 0.95) as usize],
            micros[micros.len() - 1]
        );
    };

    measure("pre_rebuild");
    engine.hnsw_vacuum("t").unwrap();
    measure("post_rebuild");

    // Auto-commit insert throughput (AFTER the recall measurements, so
    // the recall corpus is exactly the 10k base).
    let auto_started = Instant::now();
    for i in 10_000..11_000i32 {
        let v = &base[i as usize - 10_000];
        let values: String = v
            .iter()
            .map(|x| format!("{x}"))
            .collect::<Vec<_>>()
            .join(", ");
        engine
            .exec(None, &format!("INSERT INTO t VALUES ({i}, [{values}])"))
            .unwrap();
    }
    let auto_elapsed = auto_started.elapsed();
    eprintln!(
        "M6-BENCH auto_commit_insert: 1000 rows in {:.2}s = {:.0} rows/s (group commit)",
        auto_elapsed.as_secs_f64(),
        1000.0 / auto_elapsed.as_secs_f64()
    );

    engine.shutdown();
}
