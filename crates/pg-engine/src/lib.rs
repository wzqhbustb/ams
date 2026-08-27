//! pg_rust engine — Phase 1 M2.
//!
//! This crate is the top-level assembly that wires together:
//! - `pg-storage` (page, WAL, buffer pool, checkpoint)
//! - `pg-txn` (XID, CLOG, snapshot, visibility, locks)
//! - `pg-catalog` (system tables, bootstrap, AM traits)
//! - `pg-am-heap` / `pg-am-btree` (access methods)
//!
//! It exposes the public `Engine` API and registers all redo handlers at startup.
//!
//! # M2b: disk-backed CLOG
//!
//! The engine's commit log is the disk SLRU `pg_txn::ClogBuffer`
//! (tech-selection §6.2–6.4): `Engine::open` opens it, injects it into
//! storage recovery and the `TxnManager`, and installs it as the
//! checkpointer's `ClogFlush` hook so every checkpoint fsyncs the dirty
//! CLOG frames between `CheckpointBegin` and `CheckpointEnd` (v2.3-21).
//! The M2a `clog-snapshot.bin` bridge (`clog_snapshot` module,
//! `TrackingClog`) is deleted — the disk CLOG closes the
//! "commit → checkpoint → crash" gap natively. A leftover
//! `clog-snapshot.bin` in an old data directory is ignored: open never
//! reads it and the engine never writes one.

#![warn(missing_docs)]
#![warn(rust_2018_idioms)]

pub mod clog_snapshot_migrate;
pub mod diag;
pub mod engine;
pub mod error;
pub mod query_stats;
pub mod sql;

pub use engine::{
    ColumnDef, Engine, EngineConfig, IndexEntry, Predicate, QueryResult, TableEntry, TxnHandle,
    VacuumStats, Value, DEFAULT_CLOG_BUFFER_FRAMES,
};
pub use error::{EngineError, Result};
pub use query_stats::{
    ExecutionPath, QueryStatEntry, QueryStats, DEFAULT_QUERY_STATS_CAPACITY, MAX_QUERY_TEXT_BYTES,
};

// API surface re-exports: callers of the programmatic API (tech-selection
// §21) should not need to name the lower crates for the basic types.
pub use pg_am_heap::tuple::{ColumnType, Datum};
// So callers/tests can match `EngineError::Heap(HeapError::TupleConcurrentlyUpdated)`
// (M2c Stage P) without depending on pg-am-heap directly.
pub use pg_am_heap::HeapError;
pub use pg_storage::types::{Oid, PageId, Tid};
