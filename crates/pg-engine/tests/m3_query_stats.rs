//! M3 Stage E (tech-selection §6.3) acceptance: the `QueryStats` ring
//! buffer wired into `Engine::exec`.
//!
//! Covered:
//!
//! - capacity overflow drops the OLDEST entries (§6.3 既定语义);
//! - the single `exec` probe covers BOTH transaction paths (auto-commit
//!   and explicit `TxnHandle`), success and failure, with rows
//!   affected/returned and the execution path recorded;
//! - the typed API (`Engine::insert` / `scan`) produces NO entries — it
//!   never passes through `exec` (§6.3 另注).
//!
//! Acceptance: `cargo test -p pg-engine --test m3_query_stats`

use pg_engine::{Engine, EngineConfig, ExecutionPath, QueryResult};
use tempfile::TempDir;

fn open_with_capacity(dir: &std::path::Path, capacity: usize) -> Engine {
    let mut config = EngineConfig::new(dir);
    config.query_stats_capacity = capacity;
    Engine::open(dir, config).unwrap()
}

#[test]
fn overflow_drops_oldest_entries() {
    let tmp = TempDir::new().unwrap();
    let engine = open_with_capacity(tmp.path(), 4);

    engine.exec(None, "CREATE TABLE t (id INT)").unwrap();
    // 1 (DDL) + 5 inserts = 6 statements into a capacity-4 ring.
    for i in 0..5 {
        engine
            .exec(None, &format!("INSERT INTO t VALUES ({i})"))
            .unwrap();
    }

    let stats = engine.query_stats();
    assert_eq!(stats.capacity(), 4);
    assert_eq!(stats.len(), 4);
    let entries = stats.entries();
    let queries: Vec<&str> = entries.iter().map(|e| e.query.as_str()).collect();
    // The DDL and the first insert are the evicted oldest entries.
    assert_eq!(
        queries,
        [
            "INSERT INTO t VALUES (1)",
            "INSERT INTO t VALUES (2)",
            "INSERT INTO t VALUES (3)",
            "INSERT INTO t VALUES (4)",
        ]
    );
}

#[test]
fn exec_probe_covers_both_txn_paths() {
    let tmp = TempDir::new().unwrap();
    let engine = open_with_capacity(tmp.path(), 1000);

    engine.exec(None, "CREATE TABLE t (id INT)").unwrap();
    // Auto-commit DML.
    engine.exec(None, "INSERT INTO t VALUES (1)").unwrap();
    engine.exec(None, "INSERT INTO t VALUES (2)").unwrap();
    // Explicit-transaction DML + read.
    let txn = engine.begin_txn().unwrap();
    engine.exec(Some(&txn), "INSERT INTO t VALUES (3)").unwrap();
    let result = engine.exec(Some(&txn), "SELECT * FROM t").unwrap();
    let QueryResult::Rows { rows, .. } = result else {
        panic!("expected rows, got {result:?}");
    };
    assert_eq!(rows.len(), 3);
    txn.commit().unwrap();

    let entries = engine.query_stats().entries();
    assert_eq!(entries.len(), 5);
    assert_eq!(entries[0].path, ExecutionPath::Ddl);
    assert_eq!(entries[0].rows, 0);
    assert_eq!(entries[1].path, ExecutionPath::Insert);
    assert_eq!(entries[1].rows, 1);
    assert_eq!(entries[3].path, ExecutionPath::Insert);
    assert_eq!(entries[3].query, "INSERT INTO t VALUES (3)");
    assert_eq!(entries[4].path, ExecutionPath::SeqScan);
    assert_eq!(entries[4].rows, 3);
    // Every entry carries a real timestamp (wall clock > epoch).
    for entry in &entries {
        assert!(entry.timestamp > std::time::SystemTime::UNIX_EPOCH);
    }
}

#[test]
fn failed_statements_are_recorded_with_zero_rows() {
    let tmp = TempDir::new().unwrap();
    let engine = open_with_capacity(tmp.path(), 1000);

    // Statement-level failure (unknown table): parsed, executed, failed —
    // one entry with rows = 0.
    assert!(engine.exec(None, "SELECT * FROM nope").is_err());
    // Transaction control via exec is rejected: recorded as TxnControl.
    assert!(engine.exec(None, "BEGIN").is_err());
    // Parse failure returns BEFORE the probe: no entry.
    assert!(engine.exec(None, "SELECT FROM WHERE").is_err());

    let entries = engine.query_stats().entries();
    assert_eq!(entries.len(), 2, "{entries:?}");
    assert_eq!(entries[0].path, ExecutionPath::SeqScan);
    assert_eq!(entries[0].rows, 0);
    assert_eq!(entries[1].path, ExecutionPath::TxnControl);
    assert_eq!(entries[1].rows, 0);
}

#[test]
fn typed_api_produces_no_entries() {
    let tmp = TempDir::new().unwrap();
    let engine = open_with_capacity(tmp.path(), 1000);

    engine.exec(None, "CREATE TABLE t (id INT)").unwrap();
    assert_eq!(engine.query_stats().len(), 1);

    // Typed API: insert / scan bypass `exec` entirely (§6.3).
    engine
        .insert("t", &[Some(pg_engine::Datum::Int4(7))])
        .unwrap();
    let rows = engine.scan("t", None).unwrap();
    assert_eq!(rows.len(), 1);

    assert_eq!(
        engine.query_stats().len(),
        1,
        "typed API calls must not produce query-stat entries"
    );
}

#[test]
fn capacity_zero_disables_recording() {
    let tmp = TempDir::new().unwrap();
    let engine = open_with_capacity(tmp.path(), 0);
    engine.exec(None, "CREATE TABLE t (id INT)").unwrap();
    engine.exec(None, "INSERT INTO t VALUES (1)").unwrap();
    assert!(engine.query_stats().is_empty());
}
