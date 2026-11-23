//! The assembled engine and its programmatic API (coding-plan Stage K;
//! tech-selection §21 "M2a API"; CLOG assembly updated for M2b Stage L).
//!
//! [`Engine`] wires the components together at open time, in this order:
//!
//! 1. `ClogBuffer::open` — the M2b disk-backed SLRU commit log
//!    (`pg_txn::ClogBuffer`, tech-selection §6.3), rooted at
//!    `{data_dir}/clog/`. It is opened **before** storage so WAL replay can
//!    record terminal states into it (the redo handlers' `set_state` calls
//!    are idempotent, so replaying a commit the CLOG already flushed is a
//!    no-op).
//! 2. `StorageEngine::open_with_redo_and_clog` — storage recovery with the
//!    heap AM's, the txn layer's, and the B+Tree AM's redo handlers injected
//!    (`heap_redo_handlers` + `txn_redo_handlers` + `btree_redo_handlers`,
//!    the last from Stage M wave 2) and the shared
//!    `ClogBuffer` as the `ClogAccessor` that both recovery and
//!    post-recovery visibility checks read.
//!    `checkpoint.set_next_txn_id_source` is already done by `pg-storage`
//!    itself (see `StorageEngine::create_new` / `recover_with_redo_handlers`).
//! 3. `storage.checkpoint().set_clog_flush(clog)` — installs the
//!    checkpoint-time CLOG flush hook (tech-selection §6.4, v2.3-21): every
//!    checkpoint writes back and fsyncs the dirty CLOG frames between
//!    `CheckpointBegin` and `CheckpointEnd`. That flush is the **only**
//!    CLOG fsync anywhere; commit/abort durability comes from the
//!    `TxnCommit`/`TxnAbort` WAL records until a checkpoint lands the bits.
//! 4. `Catalog::open` — bootstrap (if needed) + read-back of the system
//!    tables; owns the `OidAllocator` wired into checkpoints. The catalog is
//!    opened **once per Engine** and kept: re-opening after DDL would reset
//!    the OID allocator and reopen the crash-rollback window the startup
//!    correction in `Catalog::open` exists to close.
//! 5. `TxnManager::new` over `engine.txn_id_clock()`, the WAL writer as
//!    `Arc<dyn CommitWal>`, and the same CLOG. The manager is held in an
//!    `Arc` and also installed as the checkpoint coordinator's ATT snapshot
//!    source (`set_att_provider`, Stage N, tech-selection §11.4).
//! 6. `HeapAM::new` over the shared buffer pool / WAL writer, with the
//!    manager installed as its row-lock waiter (`set_row_waiter`, M2c
//!    Stage P: the §9.1 5-step `t_xmax` protocol).
//! 7. The in-memory **table registry** (`RwLock<HashMap<String, TableEntry>>`)
//!    is rebuilt from the catalog: `pg_class` rows with `relkind = 'r'` (last
//!    version per OID wins, `relkind = 'd'` marks a drop) joined with
//!    `pg_attribute` and `pg_rust_relpages`. Index relations
//!    (`relkind = 'i'`, Stage M wave 2) are **not** in the table registry —
//!    they carry no heap chain and must not be reachable through the DML
//!    API; they are rebuilt from the `pg_index` heap chain into a separate
//!    `Vec<IndexEntry>` keyed by (table, column).
//!
//! # M2a → M2b: the CLOG snapshot bridge is gone
//!
//! M2a kept commit state in memory and bridged the "commit → checkpoint →
//! crash" gap with a checkpoint-time dump of `{data_dir}/clog-snapshot.bin`
//! (the deleted `clog_snapshot` module). The disk CLOG closes that gap
//! natively: checkpointed bits are on disk, and bits newer than the last
//! checkpoint are rebuilt by WAL replay from the redo point. A leftover
//! `clog-snapshot.bin` / `clog-snapshot.tmp` in an old data directory is
//! **ignored** — open never reads it and the engine never writes one.
//!
//! # DDL crash atomicity (M2a limitation)
//!
//! Heap records are replayed unconditionally and there is no undo in M2a, so
//! a crash mid-DDL can leave a physical half-state. The ordering inside
//! `create_table` / `drop_table` is chosen so every half-state degrades to a
//! *leak*, never to corruption:
//!
//! - `create_table`: a crash can leave catalog rows for a table whose
//!   commit never landed. Registry rebuild skips entries without a complete
//!   `pg_class` + `pg_rust_relpages` pair (warn + leak the heap pages).
//! - `drop_table`: the `relkind = 'd'` version is written **before** the
//!   data pages are freed. A crash then leaks the pages of a table that
//!   reads as dropped; the reverse order could return a freed page to the
//!   allocator while the table still reads as live — a reused page under a
//!   live chain head is corruption, so this order is never used.
//!
//! # System-catalog capacity (M2a limitation)
//!
//! `Catalog::open` reads back only each system table's *first* page, so DDL
//! must never let `pg_class` / `pg_attribute` / `pg_rust_relpages` overflow
//! onto a second chain page — the overflow rows would vanish after a reopen.
//! Every catalog insert/update pre-checks the first page's free space and
//! fails with [`EngineError::CatalogFull`] instead of silently overflowing
//! (M2a expects the single page to be ample: a `pg_class` row is ~100
//! bytes).
//!
//! # Concurrency scope
//!
//! DML (`insert` / `scan` / `update` / `delete`) is safe to call from many
//! threads concurrently (Stage K 100-thread acceptance). M2c Stage P adds
//! the two-tier locking of tech-selection §9:
//!
//! - **Row locks** (§9.1): UPDATE/DELETE and `SELECT ... FOR UPDATE` run
//!   the 5-step `t_xmax` protocol in the heap AM — a row stamped by a
//!   still-active transaction is WAITED on (not errored), and a committed
//!   stamper surfaces as `HeapError::TupleConcurrentlyUpdated`.
//! - **Table locks** (§9.2): statements acquire `AccessShare` (SELECT),
//!   `RowExclusive` (INSERT/UPDATE/DELETE, FOR UPDATE), `Exclusive`
//!   (CREATE INDEX), or `AccessExclusive` (CREATE/DROP TABLE) after table
//!   resolution; locks are keyed by XID and released at commit/abort (2PL).
//!
//! DDL is additionally serialized by an internal lock. Deadlock detection
//! is M2c Stage R (tech-selection §9.3): [`Engine::open`] starts one
//! `DeadlockDetector` per engine (tick interval from
//! [`EngineConfig::deadlock_detector_interval`], default 100ms), which scans
//! the wait-for graph — row edges from `TxnManager::wait_edges`, table edges
//! from `LockManager::table_lock_states` — and breaks each cycle by marking
//! its youngest transaction (max XID) in a victim registry shared by both
//! wait loops. A victim parked in `wait_for` / `LockManager::acquire` wakes
//! with a deadlock error: the current statement fails, and the caller must
//! abort the transaction (PG semantics — there is no enforced "aborted
//! state" in M2c, the same caller discipline as the no-statement-level-
//! rollback rule). Because the detector's two wait-graph sources cannot be
//! snapshotted atomically, a cycle can dissolve between the detector's
//! re-verification and its mark, delivering a DeadlockVictim error to a
//! transaction whose deadlock no longer exists — rare, and consistent with
//! what PG can surface under `deadlock_timeout` races; the error is always
//! safe to retry as a fresh transaction after the abort. The snapshot-only
//! read APIs (`scan`, `index_lookup`) and
//! plain auto-commit SELECT take no table lock (they own no transaction),
//! so a `DROP TABLE` racing them can still produce `TableNotFound` —
//! unchanged from M2b.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use parking_lot::{Mutex, RwLock};

use pg_am_btree::{
    btree_redo_handlers, encode_key, is_supported_key_type, BTreeAM, BTreeError, BTreeUndoHandler,
};
use pg_am_heap::access_method::{
    DeleteContext, InsertContext, RelationDesc, ScanContext, UpdateContext, Vacuumable,
};
use pg_am_heap::heap_am::MAX_TUPLE_BYTES;
use pg_am_heap::line_pointer::LINE_POINTER_SIZE;
use pg_am_heap::toast::{inline_size, toastable, ToastRelation, TOAST_TUPLE_THRESHOLD};
use pg_am_heap::tuple::{decode_tuple, encode_tuple, ColumnType, Datum, TupleHeader};
use pg_am_heap::{
    heap_redo_handlers, AccessMethod, HeapAM, HeapUndoHandler, SlottedPage, UpdatableAM,
};
use pg_am_inverted::Analyzer as _;
use pg_catalog::builtin_types::{column_type_of_attr, BUILTIN_TYPES, FLOAT8_OID, VECTOR_OID};
use pg_catalog::system_tables::{
    SystemTableDef, BTREE_AM_OID, HEAP_AM_OID, HNSW_AM_OID, INVERTED_AM_OID, PG_ATTRIBUTE,
    PG_CLASS, PG_INDEX, PG_RELPAGES, RELKIND_INDEX, RELKIND_TABLE,
};
use pg_catalog::{Catalog, TableOid, TypeOid};
use pg_storage::buffer_pool::BufferPool;
use pg_storage::clog::ClogFlush;
use pg_storage::config::StorageConfig;
use pg_storage::engine::StorageEngine;
use pg_storage::recovery::{AttProvider, UndoHandler};
use pg_storage::types::{Lsn, Oid, PageId, Tid, TxnId, PAGE_SIZE};
use pg_storage::wal::reader::WalReader;
use pg_storage::wal::record::{HeapDeleteRecord, HeapUpdateRecord, WalRecord, WalRecordType};
use pg_storage::wal::WalWriter;
use pg_txn::{
    is_visible, txn_redo_handlers, ClogAccessor, ClogBuffer, CommitWal, DeadlockDetector,
    DeadlockVictims, LockManager, LockMode, RowWaiter, Snapshot, SnapshotGuard, TableLockState,
    TxnManager,
};

use crate::error::{EngineError, Result};
use crate::planner::{AmKind, InvertedIndexMetadata};
use crate::query_stats::{ExecutionPath, QueryStatEntry, QueryStats, DEFAULT_QUERY_STATS_CAPACITY};
use crate::sequence::{
    attrdefaults_table, sequence_class_row, sequences_table, SequenceManager,
    PG_RUST_ATTRDEFAULTS_NAME, PG_RUST_ATTRDEFAULTS_OID, PG_RUST_SEQUENCES_NAME,
    PG_RUST_SEQUENCES_OID, RELKIND_INTERNAL, RELKIND_SEQUENCE,
};
use crate::session::SqlSession;
use crate::sql::{
    self, Bm25ScoreExpr, CmpOp, Expr, Filter, Literal, LockClause, OrderByExpr, OrderByKind,
    SelectCols, SelectItem, SequenceFunction, Statement,
};

/// `pg_class.relkind` marker written by [`Engine::drop_table`].
///
/// Engine-private (PostgreSQL has no such relkind; it removes the row). The
/// row is kept — with its `relkind` flipped — so the drop is an ordinary
/// MVCC update through the heap AM (WAL-logged, redo-safe) instead of a
/// physical removal the M2a catalog read-back could not distinguish from
/// corruption.
const RELKIND_DROPPED: &str = "d";

/// Default [`EngineConfig::clog_buffer_frames`] (tech-selection §6.3,
/// v2.3-25): 8 frames = a 128K-XID window, covering 100 concurrent
/// transactions with headroom.
pub const DEFAULT_CLOG_BUFFER_FRAMES: usize = 8;

/// Process-wide engine identity source (Stage O review): every [`Engine`]
/// takes a unique instance ID at open, and [`TxnHandle`]s carry it so a
/// handle created by one engine cannot be executed against another.
static NEXT_ENGINE_INSTANCE_ID: AtomicU64 = AtomicU64::new(1);
static NEXT_SQL_SESSION_ID: AtomicU64 = AtomicU64::new(1);

/// What a transaction did to one index entry, for undo purposes (Stage O
/// review: index maintenance is not MVCC-covered by heap headers, so the
/// engine keeps an explicit per-transaction undo log).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IndexUndoOp {
    /// The transaction inserted `(key, tid)`; abort removes it.
    Inserted,
    /// The transaction deleted `(key, tid)`; abort re-inserts it.
    Deleted,
    /// The transaction inserted a graph node (M6 Stage C, tech-selection
    /// §6.2): abort tombstones it — the node id travels in `key` (4 LE
    /// bytes), and the entry locates through the index's own meta page.
    /// There is NO HnswDeleted arm: a delete never touches the graph
    /// (statement time is heap-xmax only), so there is nothing to undo.
    HnswInserted,
}

/// One reversible index maintenance op performed inside a transaction.
/// An UPDATE contributes two entries (delete old key + insert new key).
#[derive(Debug, Clone)]
struct IndexUndo {
    /// The index the op ran against (re-opened by OID + meta page on undo).
    index: IndexEntry,
    /// The encoded key bytes of the entry.
    key: Vec<u8>,
    /// The heap TID of the entry (the B+Tree delete API is (key, tid)
    /// exact, so duplicates of the same key are undone individually).
    tid: Tid,
    /// What the transaction did.
    op: IndexUndoOp,
}

/// Engine-level configuration.
///
/// A thin wrapper over [`StorageConfig`] plus the M2b CLOG cache size;
/// wrapping keeps [`Engine::open`]'s signature stable when later milestones
/// add their own knobs without breaking callers.
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// Storage-layer configuration (buffer pool, WAL, checkpoints).
    pub storage: StorageConfig,
    /// Number of clock-sweep frames in the disk CLOG's SLRU cache
    /// (`pg_txn::ClogBuffer`; M2b Stage L, tech-selection §6.3, v2.3-25).
    ///
    /// Frame-count rationale (§6.3): the default of 8 frames is a
    /// 128K-XID window, which covers 100 concurrent transactions with
    /// headroom; production TP (≥1K TPS × 60s transaction lifetimes plus
    /// cold lookbacks) should use 64 (1M XIDs); OLAP with hour-long scans
    /// should use 256 (4M XIDs) to avoid hot/cold thrash.
    ///
    /// Validation is delegated to [`ClogBuffer::open`], which panics on a
    /// value outside [4, 1024] — an invalid configuration must fail loudly
    /// at startup, not degrade at runtime.
    pub clog_buffer_frames: usize,
    /// Deadlock detector tick interval (M2c Stage R, tech-selection §9.3):
    /// the background scan of the wait-for graph runs this often. Default
    /// 100ms, which bounds detection latency at ~one tick (acceptance:
    /// ≤200ms). Tests may lower it to keep the suite fast; setting it too
    /// low wastes CPU on empty-graph scans (each tick is µs-scale, so even
    /// the 100ms default is far under the 1% CPU budget). Zero is clamped
    /// to a 1ms floor by `DeadlockDetector::start` — a free-running scan
    /// buys no observable latency and would busy-loop a core.
    pub deadlock_detector_interval: Duration,
    /// Query-statistics ring buffer capacity (M3 Stage E, tech-selection
    /// §6.3): how many recent `exec` statements the engine retains for
    /// diagnostics. Default 1000 ([`DEFAULT_QUERY_STATS_CAPACITY`]);
    /// 0 disables recording. Overflow drops the oldest entry (§6.3 既定语义).
    pub query_stats_capacity: usize,
    /// Whether an inverted query may read sealed segments when its WAL
    /// freshness watermark lags behind the query snapshot. Default `false`
    /// (`require_fresh`): stale reads fall back to SeqScan. This is an
    /// engine-level switch only; no SQL syntax is added.
    pub allow_stale_inverted: bool,
    /// Whether automatic inverted segment compaction is enabled. Disable only
    /// for benchmarks or when a deployment explicitly prefers more, smaller
    /// segments. Default `true`.
    pub inverted_merge_enabled: bool,
    /// Number of committed documents accumulated before a delta is sealed.
    /// Larger batches reduce fsync and segment churn; recovery remains WAL-driven.
    pub inverted_seal_batch_size: usize,
    /// Analyzer identity expected when recovering inverted segment metadata.
    /// Defaults to the built-in mixed analyzer; custom configurations are
    /// accepted only when the persisted identity matches exactly.
    pub inverted_analyzer: pg_am_inverted::AnalyzerConfig,
}

impl EngineConfig {
    /// Default configuration rooted at `data_dir`.
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            storage: StorageConfig::new(data_dir),
            clog_buffer_frames: DEFAULT_CLOG_BUFFER_FRAMES,
            deadlock_detector_interval: pg_txn::DEFAULT_DEADLOCK_INTERVAL,
            query_stats_capacity: DEFAULT_QUERY_STATS_CAPACITY,
            allow_stale_inverted: false,
            inverted_merge_enabled: true,
            inverted_seal_batch_size: 128,
            inverted_analyzer: pg_am_inverted::AnalyzerConfig::builtin(),
        }
    }
}

/// A column definition supplied to [`Engine::create_table`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDef {
    /// Column name (`pg_attribute.attname`).
    pub name: String,
    /// The heap tuple codec type; mapped to a built-in `pg_type` OID on DDL.
    pub col_type: ColumnType,
}

/// A single column value supplied to / returned by the DML API.
/// `None` encodes SQL NULL. This is exactly the heap codec's
/// `Option<Datum>` — re-exported, not re-wrapped, so no conversion layer
/// exists between the engine API and the AM.
pub type Value = Option<Datum>;

/// The outcome of an [`Engine::exec`] or [`Engine::exec_with_params`] call.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryResult {
    /// A SELECT result set.
    Rows {
        /// Column names in the result.
        columns: Vec<String>,
        /// Rows, each a vector of values matching `columns`.
        rows: Vec<Vec<Value>>,
    },
    /// The number of rows affected by an INSERT/UPDATE/DELETE.
    Affected(usize),
    /// DDL or txn-control statement succeeded.
    Ok,
}

/// A scan filter (§21): single-column comparison.
///
/// `None` on [`Engine::scan`] means a full scan.
#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    /// Keep rows whose column `col_index` equals `value`.
    Eq {
        /// 0-based column position in the table's schema.
        col_index: usize,
        /// The value to compare against (type must match the column).
        value: Datum,
    },
    /// Keep rows whose column `col_index` is less than `value`.
    Lt {
        /// 0-based column position in the table's schema.
        col_index: usize,
        /// The value to compare against (type must match the column).
        value: Datum,
    },
    /// Keep rows whose column `col_index` is greater than `value`.
    Gt {
        /// 0-based column position in the table's schema.
        col_index: usize,
        /// The value to compare against (type must match the column).
        value: Datum,
    },
    /// Keep rows whose TEXT column matches the analyzed full-text query.
    FullText {
        /// 0-based column position in the table's schema.
        col_index: usize,
        /// Query text stored as `Datum::Text` for the common predicate API.
        value: Datum,
        /// Analyzer used for both the query and the candidate document.
        analyzer: pg_am_inverted::AnalyzerConfig,
    },
}

impl Predicate {
    /// The 0-based column index this predicate filters on.
    pub fn col_index(&self) -> usize {
        match self {
            Predicate::Eq { col_index, .. }
            | Predicate::Lt { col_index, .. }
            | Predicate::Gt { col_index, .. }
            | Predicate::FullText { col_index, .. } => *col_index,
        }
    }

    /// The comparison value.
    pub fn value(&self) -> &Datum {
        match self {
            Predicate::Eq { value, .. }
            | Predicate::Lt { value, .. }
            | Predicate::Gt { value, .. }
            | Predicate::FullText { value, .. } => value,
        }
    }

    /// Whether `val` satisfies this predicate.
    pub fn matches(&self, val: &Value) -> bool {
        let Some(d) = val else {
            return false;
        };
        match self {
            Predicate::Eq { value, .. } => d == value,
            Predicate::Lt { value, .. } => d < value,
            Predicate::Gt { value, .. } => d > value,
            Predicate::FullText {
                value: Datum::Text(query),
                analyzer,
                ..
            } => {
                let Datum::Text(document) = d else {
                    return false;
                };
                fulltext_matches(analyzer, query, document)
            }
            Predicate::FullText { .. } => false,
        }
    }
}

fn fulltext_matches(
    analyzer_config: &pg_am_inverted::AnalyzerConfig,
    query: &str,
    document: &str,
) -> bool {
    let analyzer = pg_am_inverted::DefaultAnalyzer::from_config(analyzer_config.clone());
    let query = pg_am_inverted::parse_query(&analyzer, query);
    let document_terms = analyzer
        .analyze(document)
        .into_iter()
        .map(|token| token.text)
        .collect::<std::collections::BTreeSet<_>>();
    match query {
        pg_am_inverted::Query::And(query_terms) => {
            !query_terms.is_empty() && query_terms.iter().all(|term| document_terms.contains(term))
        }
        pg_am_inverted::Query::Or(groups) => groups.iter().any(|group| {
            !group.is_empty() && group.iter().all(|term| document_terms.contains(term))
        }),
    }
}

/// One entry of the engine's in-memory table registry.
#[derive(Debug, Clone)]
pub struct TableEntry {
    /// The table's OID (`pg_class.oid`).
    pub oid: Oid,
    /// Head of the table's on-disk page chain (`pg_rust_relpages.first_page`).
    pub first_page: PageId,
    /// Column schema in `attnum` order.
    pub columns: Vec<ColumnDef>,
    /// Optional sequence OID used as each column's default value.
    pub defaults: Vec<Option<Oid>>,
    /// Hidden chunk heap for TOASTable columns. Its first page is persisted
    /// in the main table's `pg_rust_relpages.last_page` slot; no catalog row
    /// is added for the hidden relation.
    pub toast: Option<ToastRelation>,
}

/// One entry of the engine's in-memory index registry (M2b Stage M wave 2).
///
/// Mirrors one `pg_index` row plus the index's `pg_rust_relpages` entry
/// (whose `first_page` is the B+Tree **meta page**) and its single
/// `pg_attribute` row (whose `attname` mirrors the indexed column, as in
/// PostgreSQL). Rebuilt at open by scanning the `pg_index` heap chain;
/// append-only in M2b (no `DROP INDEX`).
#[derive(Debug, Clone)]
pub struct IndexEntry {
    /// The access method that owns this index.
    pub kind: AmKind,
    /// Inverted analyzer/watermark metadata; `Some` exactly for inverted AMs.
    pub inverted: Option<InvertedIndexMetadata>,
    /// The index relation's OID (`pg_index.indexrelid`).
    pub index_oid: Oid,
    /// The indexed table's OID (`pg_index.indrelid`).
    pub table_oid: Oid,
    /// The indexed column's name (from the index's `pg_attribute` row).
    pub column: String,
    /// The indexed column's codec type (the B+Tree key type). An HNSW index
    /// is marked by `ColumnType::Vector(dim)` (M6 Stage C); an inverted index
    /// uses `ColumnType::Text` and never interprets it as a B+Tree key.
    pub key_type: ColumnType,
    /// The index's meta page (`pg_rust_relpages.first_page` of the index).
    pub meta_page: PageId,
    /// The sidecar NodeId→ctid map B+Tree's meta page (`last_page` of the
    /// relpages row; M6 Stage C). `None` for B+Tree indexes and for M5-shape
    /// HNSW registrations (`last == first`).
    pub map_page: Option<PageId>,
}

/// The outcome of one [`Engine::vacuum`] pass (M3 Stage D).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct VacuumStats {
    /// Dead TIDs `scan_dead_tuples` reported at the pass's horizon
    /// (includes partially-dead HOT chain members, which phases 3–5 then
    /// leave untouched — §4.2).
    pub dead_tuples: usize,
    /// `(tid, values)` work items `collect_index_keys` produced: standalone
    /// dead tuples plus the roots of fully-dead HOT chains.
    pub index_keys: usize,
    /// Index entries physically removed by this pass (`BTreeDelete` WAL;
    /// HNSW sidecar-map entries included — the map IS a btree, M6 Stage D).
    /// Under eager online index maintenance this is essentially only
    /// crash-loser dangling entries and watermark-deferred map residue
    /// (§4.3).
    pub index_entries_removed: usize,
    /// Phase-4 delete attempts that found the entry already gone
    /// (`EntryNotFound` → Ok at the vacuum call site): eager online index
    /// maintenance had removed them.
    pub index_entries_already_gone: usize,
    /// Physical TOAST chunk rows reclaimed after main-row reclamation. This
    /// directly proves that the pass performed out-of-line garbage collection.
    pub toast_chunks_reclaimed: usize,
}

/// The default leak-ratio threshold for [`Engine::hnsw_leak_report`] (M6
/// Stage E, tech-selection §9: "比例阈值（默认 10%，可配）触发局部重建建议").
pub const DEFAULT_HNSW_LEAK_THRESHOLD: f64 = 0.10;

/// The leak ledger for one table's HNSW index (M6 Stage E, tech-selection
/// §8.3/§9): the three-term residue accounting — tombstoned nodes +
/// crash-window residue (INITIALIZING/orphan entries) + orphan sidecar-map
/// entries — against the graph's node count, plus the reachability
/// diagnostics. Read-only; the §12.5 quiesce premise applies to the
/// reachability fields (a concurrent writer's legal intermediate states
/// would be misread).
#[derive(Debug, Clone, PartialEq)]
pub struct HnswLeakReport {
    /// Directory-mapped node count (the chain high-water mark).
    pub node_count: u64,
    /// Mapped entries with the LIVE bit set.
    pub live_count: u64,
    /// Mapped entries with the tombstone bit set.
    pub tombstoned_count: u64,
    /// Mapped entries still INITIALIZING (crash-window ghost stock).
    pub initializing_count: u64,
    /// Occupied node-entry slots no directory entry names (crash-window
    /// orphan stock; `audit_index`'s referenced-pages scope applies).
    pub orphan_entry_count: u64,
    /// Sidecar-map entries whose row is gone or will never be visible
    /// again (the horizon-free deadness rule: slot empty, inserter
    /// aborted, or deleter committed — in-flight rows are NOT orphans).
    pub orphan_map_entries: u64,
    /// LIVE, non-tombstoned nodes no legal search can reach (diagnostic;
    /// NOT part of the leak ratio — §14.1's verdict reads this).
    pub unreachable_live_count: u64,
    /// Unreachable INITIALIZING nodes (diagnostic).
    pub unreachable_initializing_count: u64,
    /// Unreachable tombstoned nodes (diagnostic).
    pub unreachable_tombstoned_count: u64,
    /// `tombstoned + initializing + orphan_entry + orphan_map_entries`
    /// divided by `node_count` (0.0 on an empty graph).
    pub leak_ratio: f64,
    /// The threshold in effect (default [`DEFAULT_HNSW_LEAK_THRESHOLD`]).
    pub threshold: f64,
    /// `leak_ratio > threshold` — the §9 trigger for a local-rebuild
    /// recommendation (the recommendation only; the rebuild itself is
    /// Stage E's `hnsw_vacuum` work).
    pub rebuild_recommended: bool,
}

/// The outcome of one [`Engine::hnsw_vacuum`] pass (M6 Stage E).
#[derive(Debug, Clone, PartialEq)]
pub struct HnswVacuumStats {
    /// The redirect pass's own counters.
    pub redirect: pg_am_hnsw::RedirectStats,
    /// The leak ledger re-taken after the pass.
    pub leak_after: HnswLeakReport,
}

/// One inverted-index change awaiting its transaction's WAL commit.
#[derive(Debug, Clone)]
enum InvertedWalAction {
    /// Replace a document idempotently using its WAL-encoded tuple bytes.
    Upsert { tid: Tid, tuple_bytes: Vec<u8> },
    /// Remove the document owned by a HOT chain root.
    Delete { tid: Tid },
    /// A TOAST group to validate at commit; `None` requires full validation
    /// because the deleted chunk's identity could not be recovered safely.
    ToastChanged { valueid: Option<u32> },
}

impl InvertedWalAction {
    fn approximate_bytes(&self) -> usize {
        match self {
            Self::Upsert { tuple_bytes, .. } => std::mem::size_of::<Self>() + tuple_bytes.len(),
            Self::Delete { .. } | Self::ToastChanged { .. } => std::mem::size_of::<Self>(),
        }
    }
}

type PendingInvertedLog = HashMap<Oid, HashMap<TxnId, Vec<InvertedWalAction>>>;

const INVERTED_PENDING_LIMIT: usize = 1024;
const INVERTED_PENDING_BYTES_LIMIT: usize = 16 * 1024 * 1024;
const INVERTED_PENDING_XID_LIMIT: usize = 64;

fn push_inverted_pending(
    pending: &mut HashMap<TxnId, Vec<InvertedWalAction>>,
    xid: TxnId,
    action: InvertedWalAction,
    limit: usize,
    byte_limit: usize,
) -> Result<()> {
    let current_bytes = pending.get(&xid).map_or(0usize, |actions| {
        actions
            .iter()
            .map(InvertedWalAction::approximate_bytes)
            .sum()
    });
    let action_bytes = action.approximate_bytes();
    if pending.len() >= INVERTED_PENDING_XID_LIMIT && !pending.contains_key(&xid) {
        return Err(EngineError::Unsupported(format!(
            "inverted pending exceeds {INVERTED_PENDING_XID_LIMIT} transactions"
        )));
    }
    let actions = pending.entry(xid).or_default();
    if actions.len() >= limit || current_bytes.saturating_add(action_bytes) > byte_limit {
        return Err(EngineError::Unsupported(format!(
            "inverted pending transaction exceeds {limit} actions or {byte_limit} bytes; commit or split the transaction"
        )));
    }
    actions.push(action);
    Ok(())
}

/// The assembled engine: storage + catalog + heap AM + txn manager + disk
/// CLOG + in-memory table registry. Assembly order is documented at the
/// module level.
///
/// All operations take `&self`; every field is internally synchronized, so a
/// single `Engine` (typically behind an `Arc`) is shared across threads.
pub struct Engine {
    storage: StorageEngine,
    catalog: Catalog,
    heap: HeapAM,
    /// Shared with the checkpoint coordinator as its ATT snapshot source
    /// (Stage N, §11.4), hence the `Arc`. The manager also OWNS the
    /// commit/checkpoint barrier (M2c Stage P): its `commit_txn`/`abort_txn`
    /// take the read guard internally, and the checkpoint coordinator takes
    /// the write guard via `set_commit_barrier` — so the engine no longer
    /// carries its own `commit_barrier` field (Stage L's arrangement).
    txn: Arc<TxnManager>,
    clog: Arc<ClogBuffer>,
    /// Table-level lock manager (M2c Stage P, tech-selection §9.2): SELECT
    /// takes `AccessShare`, DML and `SELECT ... FOR UPDATE` take
    /// `RowExclusive`, `CREATE INDEX` takes `Exclusive`, `CREATE`/`DROP
    /// TABLE take `AccessExclusive`. Locks key by XID and are held to
    /// transaction end (2PL — released only by `release_all` at
    /// commit/abort, never mid-transaction). Always the blocking `acquire`:
    /// there is no NOWAIT; a table-lock cycle is broken by the deadlock
    /// detector (Stage R), which shares the victim registry with this
    /// manager (wired at open).
    lock_manager: Arc<LockManager>,
    /// Deadlock detector (M2c Stage R, tech-selection §9.3): background
    /// thread scanning the wait-for graph every
    /// [`EngineConfig::deadlock_detector_interval`], aborting the youngest
    /// transaction of each cycle. One per engine; stopped by
    /// [`Engine::shutdown`] (and by `Drop`, so an engine dropped without
    /// shutdown never leaves an orphaned thread).
    deadlock_detector: DeadlockDetector,
    /// Name → table. Rebuilt from the catalog at open; kept in sync by DDL.
    registry: RwLock<HashMap<String, TableEntry>>,
    /// All live indexes (M2b Stage M wave 2). Rebuilt at open by scanning
    /// the `pg_index` heap chain; `create_index` appends. Indexes on dropped
    /// tables linger harmlessly: `index_lookup` resolves the table first.
    indexes: RwLock<Vec<IndexEntry>>,
    /// Live HNSW index handles, keyed by index OID (M6 Stage C): filled at
    /// `create_hnsw_index*`/`open_hnsw_index` time, lazily opened from the
    /// meta page on a DML-hook cache miss (self-evidencing parameters via
    /// [`pg_am_hnsw::index::expected_params_of`]). Shared `&self` handles
    /// (M6 Stage B), so one per index is enough for all writers.
    hnsw_handles: Arc<Mutex<std::collections::HashMap<Oid, Arc<pg_am_hnsw::HnswIndex>>>>,
    /// Post-commit graph actions deferred from statement time, keyed by
    /// the owning transaction (M6 Stage C, tech-selection §6.2/§8.1: the
    /// graph is never touched by an UNCOMMITTED delete — the row's xmax IS
    /// the delete until the commit is durable; tombstones and map removals
    /// run only after the commit record lands). Discarded untouched on
    /// abort; replayed idempotently on the commit success path.
    pending_hnsw: Arc<Mutex<PendingHnswLog>>,
    /// Inverted segment stores, keyed by index OID. Each handle owns the
    /// recovered manifest, analyzer, and WAL watermark for one full-text index.
    inverted_handles:
        Arc<Mutex<std::collections::HashMap<Oid, Arc<Mutex<pg_am_inverted::InvertedIndex>>>>>,
    /// Uncommitted WAL actions retained across synchronous catch-up calls.
    pending_inverted: Arc<Mutex<PendingInvertedLog>>,
    /// Per-index consumer lag and last-progress observations.
    inverted_lag: Arc<Mutex<HashMap<Oid, pg_am_inverted::tier2::LagMetrics>>>,
    /// Commit completion times used by the test-only visibility-latency hook.
    #[cfg(test)]
    inverted_commit_finished: Arc<Mutex<HashMap<TxnId, Instant>>>,
    /// Commit-to-visible latency samples used by the test-only hook.
    #[cfg(test)]
    inverted_visibility_samples: Arc<Mutex<HashMap<Oid, Vec<Duration>>>>,
    /// Serializes `create_table` / `drop_table` (see the module docs for the
    /// DDL-vs-DML concurrency scope).
    ddl_lock: Mutex<()>,
    /// This engine's identity (see `NEXT_ENGINE_INSTANCE_ID`).
    instance_id: u64,
    /// Per-transaction index undo log, keyed by XID (Stage O review: index
    /// modifications are not transactional). Every index insert/delete a
    /// transaction performs — explicit `TxnHandle` txns AND the auto-commit
    /// path — is recorded here; abort reverse-applies the entries, commit
    /// discards them. Without this, an aborted INSERT left a dangling
    /// `(key, tid)` entry pointing at a dead tuple, and an aborted DELETE
    /// lost the entry of a still-live row. Entries are always removed on
    /// commit AND abort, so the map never leaks committed transactions.
    index_undo: Arc<Mutex<HashMap<TxnId, Vec<IndexUndo>>>>,
    /// Query statistics ring buffer (M3 Stage E, tech-selection §6.3):
    /// [`Self::exec`] and [`Self::exec_with_params`] record one entry per
    /// statement — see [`Self::query_stats`].
    query_stats: QueryStats,
    /// Phase 3 inverted-query freshness policy, retained for the planner.
    allow_stale_inverted: bool,
    /// Number of committed documents required before sealing a delta.
    inverted_seal_batch_size: usize,
    /// Whether query-path automatic segment compaction is enabled.
    inverted_merge_enabled: bool,
    /// Analyzer identity used when creating new inverted segments.
    inverted_analyzer: pg_am_inverted::AnalyzerConfig,
    sequences: SequenceManager,
}

/// A handle to an explicit transaction (§21 M2b API).
///
/// Created by [`Engine::begin_txn`], consumed by [`TxnHandle::commit`] or
/// [`TxnHandle::abort`]. The `Snapshot` is taken at creation time (SI) and
/// `curcid` is advanced before each statement via [`Self::advance_curcid`].
///
/// A handle is bound to the [`Engine`] instance that created it (Stage O
/// review): passing it to another engine's `exec` fails with
/// [`EngineError::InvalidArgument`].
///
/// # No statement-level rollback (M2b)
///
/// There are no subtransactions in M2b: if a statement inside an explicit
/// transaction fails mid-way, the rows it already wrote REMAIN in the
/// transaction and the only safe operation is [`TxnHandle::abort`].
///
/// If dropped without calling `commit` or `abort`, the transaction is
/// automatically aborted (best-effort) to prevent XID leaks in the active
/// set — a leaked in-progress XID would make its writes invisible to every
/// future snapshot.
pub struct TxnHandle {
    txn: Arc<TxnManager>,
    xid: Option<TxnId>,
    snapshot: RefCell<Snapshot>,
    /// Keeps the snapshot's `xmin` registered in the vacuum-horizon
    /// registry for the handle's whole lifetime (M3 Stage A, §3.3): dropped
    /// — and thus unregistered — when the handle is consumed by
    /// `commit`/`abort` or auto-aborted by `Drop` (field drops run after
    /// `Drop::drop`, so the best-effort abort still sees the registration;
    /// either order would be correct). `Option` so `commit` can drop it
    /// EARLY — the post-commit cleanup's watermark read must not see this
    /// transaction's own registration (Stage D review: otherwise every
    /// explicit-transaction delete's map removal is deferred forever).
    _snapshot_guard: Option<SnapshotGuard>,
    /// Identity of the creating engine (see `NEXT_ENGINE_INSTANCE_ID`).
    instance_id: u64,
    /// Shared with the engine: table locks are keyed by XID, so commit /
    /// abort / Drop release them through this handle (2PL release point).
    lock_manager: Arc<LockManager>,
    /// Shared with the engine: this txn's index maintenance ops, reversed
    /// on abort and discarded on commit.
    index_undo: Arc<Mutex<HashMap<TxnId, Vec<IndexUndo>>>>,
    /// B+Tree construction pieces for applying the undo log.
    buffer_pool: Arc<BufferPool>,
    wal_writer: Arc<WalWriter>,
    /// Shared with the engine: live HNSW handles (M6 Stage C).
    hnsw_handles: Arc<Mutex<std::collections::HashMap<Oid, Arc<pg_am_hnsw::HnswIndex>>>>,
    /// Shared with the engine: post-commit graph actions, replayed on
    /// commit and DISCARDED on abort (the graph is never touched by an
    /// uncommitted delete, §6.2/§8.1).
    pending_hnsw: Arc<Mutex<PendingHnswLog>>,
    /// Shared commit completion times for the test-only visibility hook.
    #[cfg(test)]
    inverted_commit_finished: Arc<Mutex<HashMap<TxnId, Instant>>>,
}

impl TxnHandle {
    /// The transaction's XID.
    pub fn xid(&self) -> TxnId {
        self.xid.expect("xid accessed after commit/abort")
    }

    /// A copy of the transaction's SI snapshot (M6 Stage D review P2 — the
    /// visibility-filtered ANN path takes the caller's snapshot; this
    /// accessor lets a transaction supply its own).
    pub fn snapshot(&self) -> Snapshot {
        self.snapshot.borrow().clone()
    }

    /// Advance the command counter before executing a new SQL statement
    /// (Halloween protection, §7.2 / v2.3-Q4).
    pub fn advance_curcid(&self) {
        self.snapshot.borrow_mut().advance_curcid();
    }

    /// Commit this transaction. Consumes `self` so it cannot be reused.
    ///
    /// The transaction's index undo entries are discarded: its index
    /// maintenance becomes durable with the commit.
    ///
    /// The commit-barrier read guard is taken inside `TxnManager::commit_txn`
    /// itself (M2c Stage P), so this path — and every other commit path — is
    /// serialized against checkpoints by construction.
    pub fn commit(mut self) -> Result<()> {
        let xid = self.xid.take().expect("commit called twice");
        let result = self.txn.commit_txn(xid);
        if result.is_err() {
            // Commit failed (WAL/CLOG failure): without intervention the
            // XID would leak into the active set forever — `self.xid` was
            // taken, so `Drop` will NOT auto-abort — pinning the vacuum
            // horizon and every future snapshot's xmin. Best-effort abort
            // with the same discipline as `Drop`: replay the index undo
            // FIRST (so no snapshot sees a heap row whose index entry is
            // gone), then flip the CLOG bit. If the WAL is broken badly
            // enough that even the abort record cannot append, the process
            // is tearing down anyway; the failure must still be loud.
            apply_index_undo(
                &self.index_undo,
                &self.buffer_pool,
                &self.wal_writer,
                &self.hnsw_handles,
                xid,
            );
            if let Err(e) = self.txn.abort_txn(xid) {
                tracing::warn!(error = %e, xid = xid.0, "commit failure fallback abort failed");
            }
        }
        // 2PL release point (M2c Stage P): table locks go AFTER the CLOG
        // bit flips, so a woken row-lock waiter that then needs this
        // transaction's table locks never observes the reverse order.
        if result.is_ok() {
            #[cfg(test)]
            self.inverted_commit_finished
                .lock()
                .insert(xid, Instant::now());
            // Drop OUR snapshot registration BEFORE the watermark read —
            // the cleanup's `oldest_snapshot_xmin()` must not count this
            // transaction's own snapshot (otherwise it concludes "someone
            // older is still reading" and defers every map removal
            // forever). The guard lives in an Option exactly so it can be
            // dropped here (a Drop type can't be partially moved).
            drop(self._snapshot_guard.take());
            // M6 Stage C: the commit record is durable — NOW the deferred
            // graph actions (tombstone + map removal) run BEFORE the table
            // locks release (review round 3 P3-1: a drop_table/DROP INDEX in
            // the gap could otherwise change the cleanup's target structure;
            // run_pending_hnsw takes no lock-manager locks, so no deadlock).
            run_pending_hnsw(
                xid,
                &self.pending_hnsw,
                &self.hnsw_handles,
                &self.buffer_pool,
                &self.wal_writer,
                self.txn.oldest_snapshot_xmin(),
            );
            self.index_undo.lock().remove(&xid);
        } else {
            self.pending_hnsw.lock().remove(&xid);
        }
        // 2PL release point (M2c Stage P): table locks go AFTER the CLOG
        // bit flips AND after the deferred graph cleanup ran.
        self.lock_manager.release_all(xid);
        result?;
        Ok(())
    }

    /// Abort (roll back) this transaction. Consumes `self` so it cannot be
    /// reused.
    ///
    /// The transaction's index maintenance is reverse-applied from the undo
    /// log BEFORE the CLOG abort lands, so no snapshot can observe a heap
    /// row whose index entry is already gone (or vice versa). Deferred
    /// post-commit graph actions are DISCARDED untouched — the graph was
    /// never touched by this transaction's deletes (§6.2/§8.1).
    pub fn abort(mut self) -> Result<()> {
        let xid = self.xid.take().expect("abort called twice");
        apply_index_undo(
            &self.index_undo,
            &self.buffer_pool,
            &self.wal_writer,
            &self.hnsw_handles,
            xid,
        );
        let result = self.txn.abort_txn(xid);
        self.pending_hnsw.lock().remove(&xid);
        self.lock_manager.release_all(xid);
        result?;
        Ok(())
    }
}

impl Drop for TxnHandle {
    fn drop(&mut self) {
        if let Some(xid) = self.xid.take() {
            // Best-effort abort: the transaction was never explicitly
            // committed or aborted. Aborting prevents the XID from
            // lingering in the active set forever (which would make
            // its writes invisible to all future snapshots).
            //
            // No commit-barrier guard is needed here anymore (M2c Stage P):
            // `abort_txn` takes the read guard internally, so the abort's
            // `set_state` cannot race a checkpoint's CLOG flush.
            apply_index_undo(
                &self.index_undo,
                &self.buffer_pool,
                &self.wal_writer,
                &self.hnsw_handles,
                xid,
            );
            if let Err(e) = self.txn.abort_txn(xid) {
                // Same reporting level as the auto-commit error path: a
                // failed abort leaks the XID into the active set (its
                // writes stay invisible to all future snapshots), so it
                // must be loud even though Drop cannot propagate.
                tracing::warn!(error = %e, xid = xid.0, "txn handle drop auto-abort failed");
            }
            // Discard any deferred post-commit graph actions (M6 Stage C:
            // the graph was never touched by this transaction's deletes).
            self.pending_hnsw.lock().remove(&xid);
            // Release table locks even when the abort failed: a leaked
            // lock would wedge DDL on the table forever, and the XID is
            // gone from this handle either way.
            self.lock_manager.release_all(xid);
        }
    }
}

/// Reverse-apply the per-transaction index undo log for `xid`, in reverse
/// order of recording (Stage O review): `Inserted` entries are removed from
/// the index, `Deleted` entries are re-inserted. The log slot is always
/// removed, committed-or-not. Best-effort: a failed undo leaves the index
/// inconsistent with the heap, so it is logged loudly rather than silently
/// dropped, but abort itself must still proceed (heap MVCC via the CLOG is
/// the primary consistency mechanism).
///
/// WAL semantics: the `index.delete()` / `index.insert()` calls below
/// produce ordinary B+Tree WAL records stamped while transaction `xid` is
/// aborting — i.e. the records logically belong to a transaction whose
/// terminal state is ABORTED. That is harmless in M2b: btree redo is
/// physical (page-image level), so on restart these records replay exactly
/// like any committed transaction's, and the CLOG's aborted state for `xid`
/// only governs HEAP tuple visibility, never index entry replay. Abort
/// itself is not transactional (no undo-undo): if the process dies
/// mid-undo, recovery replays the WAL prefix that did land, which is
/// consistent because heap visibility never depends on index contents.
///
/// Restart budget (Stage Q review M3): the `Deleted` re-insert goes through
/// [`BTreeIndex::insert_with_budget`] with [`UNDO_INSERT_RESTART_BUDGET`]
/// instead of the online insert's 256. Undo runs on the abort path (no
/// client waiting), and a spurious budget exhaustion during a split storm
/// or a stale separator gap would permanently drop a LIVE row's index
/// entry (heap visibility only masks the opposite direction of
/// inconsistency). A genuine post-crash wedge still terminates — the
/// budget is large, not infinite — and is reported at ERROR level.
fn apply_index_undo(
    log: &Mutex<HashMap<TxnId, Vec<IndexUndo>>>,
    buffer_pool: &Arc<BufferPool>,
    wal_writer: &Arc<WalWriter>,
    hnsw_handles: &Mutex<std::collections::HashMap<Oid, Arc<pg_am_hnsw::HnswIndex>>>,
    xid: TxnId,
) {
    let entries = log.lock().remove(&xid).unwrap_or_default();
    let btree = BTreeAM::new(Arc::clone(buffer_pool), Arc::clone(wal_writer));
    for undo in entries.into_iter().rev() {
        let result = match undo.op {
            // M6 Stage C: an insert's graph node is tombstoned on abort —
            // the NodeId rides in `key` (4 LE bytes).
            IndexUndoOp::HnswInserted => {
                pending_hnsw_handle(hnsw_handles, &undo.index, buffer_pool, wal_writer)
                    .map_err(|e| e.to_string())
                    .and_then(|handle| {
                        let node_id = u32::from_le_bytes(
                            undo.key
                                .as_slice()
                                .try_into()
                                .expect("HnswInserted key is 4 bytes"),
                        );
                        handle
                            .delete(buffer_pool, wal_writer, pg_am_hnsw::NodeId(node_id))
                            .map_err(|e| format!("hnsw abort tombstone: {e}"))
                    })
            }
            IndexUndoOp::Inserted | IndexUndoOp::Deleted => btree
                .open_index(
                    undo.index.index_oid,
                    undo.index.meta_page,
                    undo.index.key_type,
                )
                .and_then(|mut index| match undo.op {
                    IndexUndoOp::Inserted => index.delete(&undo.key, undo.tid),
                    IndexUndoOp::Deleted => {
                        index.insert_with_budget(&undo.key, undo.tid, UNDO_INSERT_RESTART_BUDGET)
                    }
                    IndexUndoOp::HnswInserted => unreachable!(),
                })
                .map_err(|e| e.to_string()),
        };
        if let Err(e) = result {
            tracing::error!(
                error = %e,
                xid = xid.0,
                index_oid = undo.index.index_oid.0,
                "index undo failed after full retry budget; index is likely \
                 inconsistent with the heap (a live row may have lost its \
                 index entry) — rebuild the index"
            );
        }
    }
}

/// Restart budget for abort-time index-undo re-inserts (see
/// [`apply_index_undo`]): ~1M passes, versus the online insert's 256.
/// Each pass is a full descent (~µs), so even a pathological split storm
/// bounds the abort path at seconds, while a genuinely wedged tree still
/// terminates and is logged at ERROR level.
const UNDO_INSERT_RESTART_BUDGET: usize = 1 << 20;

/// The deferred-action log's entry type (clarity for the field
/// declarations; clippy type_complexity).
type PendingHnswLog = HashMap<TxnId, Vec<(IndexEntry, PendingHnswAction)>>;

/// A post-commit graph action deferred from statement time (M6 Stage C,
/// tech-selection §6.2/§8.1: the graph is never touched by an uncommitted
/// delete — the row's xmax IS the delete until the commit is durable).
/// Idempotent: replay after a crash is a no-op against the same state.
#[derive(Debug, Clone, Copy)]
enum PendingHnswAction {
    /// Tombstone the node AND remove its map entry (row delete, or a
    /// vector-column update where the old vector leaves the graph).
    Tombstone { node_id: u32, tid: Tid },
    /// Remove the map entry only (a non-HOT non-vector-column update: the
    /// node lives on under the new row version; only its ctid moves).
    MapRemove { node_id: u32, tid: Tid },
}

/// Replay the deferred post-commit graph actions of a committed
/// transaction (M6 Stage C): tombstones and map removals. Idempotent and
/// best-effort-after-durability: a failure leaves a residue the Stage E
/// dead-row channel reclaims (the row IS dead either way) and is loud in
/// the log, never silently swallowed.
fn run_pending_hnsw(
    xid: TxnId,
    pending_hnsw: &Mutex<PendingHnswLog>,
    hnsw_handles: &Mutex<std::collections::HashMap<Oid, Arc<pg_am_hnsw::HnswIndex>>>,
    buffer_pool: &Arc<BufferPool>,
    wal_writer: &Arc<WalWriter>,
    oldest_xmin: TxnId,
) {
    let actions = pending_hnsw.lock().remove(&xid).unwrap_or_default();
    for (idx, action) in actions {
        if let Err(e) = pending_hnsw_one(
            &idx,
            action,
            hnsw_handles,
            buffer_pool,
            wal_writer,
            oldest_xmin,
            xid,
        ) {
            tracing::warn!(error = %e, index_oid = idx.index_oid.0, node = ?action, "post-commit hnsw cleanup failed; the Stage E dead-row channel reclaims the residue");
        }
    }
}

fn pending_hnsw_one(
    idx: &IndexEntry,
    action: PendingHnswAction,
    hnsw_handles: &Mutex<std::collections::HashMap<Oid, Arc<pg_am_hnsw::HnswIndex>>>,
    buffer_pool: &Arc<BufferPool>,
    wal_writer: &Arc<WalWriter>,
    oldest_xmin: TxnId,
    xid: TxnId,
) -> Result<()> {
    // The map entry's removal waits for the snapshot watermark (Stage D
    // review P2): a reader holding a snapshot older than this delete must
    // still resolve the row through the map — deleting the entry early
    // would make a repeatable-read ANN query lose the row while the row
    // itself is still visible. The tombstone itself is harmless to land
    // early (the state bit is never a traversal predicate).
    // Deferred removals are reclaimed by the Stage E dead-row channel.
    let map_removable = oldest_xmin > xid;
    match action {
        PendingHnswAction::Tombstone { node_id, tid } => {
            // Graph tombstone first, then the map entry.
            let handle = pending_hnsw_handle(hnsw_handles, idx, buffer_pool, wal_writer)?;
            handle.delete(buffer_pool, wal_writer, pg_am_hnsw::NodeId(node_id))?;
            if map_removable {
                pending_map_remove(idx, node_id, tid, buffer_pool, wal_writer)?;
            }
        }
        PendingHnswAction::MapRemove { node_id, tid } => {
            if map_removable {
                pending_map_remove(idx, node_id, tid, buffer_pool, wal_writer)?;
            }
        }
    }
    Ok(())
}

fn pending_hnsw_handle(
    handles: &Mutex<std::collections::HashMap<Oid, Arc<pg_am_hnsw::HnswIndex>>>,
    idx: &IndexEntry,
    buffer_pool: &Arc<BufferPool>,
    wal_writer: &Arc<WalWriter>,
) -> Result<Arc<pg_am_hnsw::HnswIndex>> {
    if let Some(h) = handles.lock().get(&idx.index_oid) {
        return Ok(Arc::clone(h));
    }
    let expected = pg_am_hnsw::index::expected_params_of(buffer_pool, idx.meta_page)?;
    let outcome = pg_am_hnsw::index::open(buffer_pool, wal_writer, idx.meta_page, &expected)?;
    let h = Arc::new(outcome.index);
    handles.lock().insert(idx.index_oid, Arc::clone(&h));
    Ok(h)
}

fn pending_map_remove(
    idx: &IndexEntry,
    node_id: u32,
    tid: Tid,
    buffer_pool: &Arc<BufferPool>,
    wal_writer: &Arc<WalWriter>,
) -> Result<()> {
    let map_page = idx.map_page.expect("hnsw index carries map_page");
    let mut map = pg_am_btree::BTreeIndex::open(
        Arc::clone(buffer_pool),
        Arc::clone(wal_writer),
        idx.index_oid,
        map_page,
        ColumnType::Int8,
    )?;
    let key = encode_key(&Datum::Int8(i64::from(node_id)))?;
    map.delete(&key, tid)?;
    Ok(())
}

#[cfg(test)]
type CommitFinishedHook = fn(&Engine, TxnId);

#[cfg(test)]
thread_local! {
    static BEFORE_AUTO_COMMIT_HOOK: std::cell::Cell<Option<fn()>> =
        std::cell::Cell::new(None);
    static BEFORE_HNSW_PUBLISH_HOOK: std::cell::Cell<Option<fn()>> =
        std::cell::Cell::new(None);
    static BEFORE_DROP_CLEANUP_HOOK: std::cell::Cell<Option<fn()>> =
        std::cell::Cell::new(None);
    static COMMIT_FINISHED_HOOK: std::cell::Cell<Option<CommitFinishedHook>> =
        std::cell::Cell::new(None);
    static BEFORE_INVERTED_SEAL_HOOK: std::cell::Cell<Option<fn(&Engine)>> =
        std::cell::Cell::new(None);
}

impl Engine {
    /// Open (or create) a database at `data_dir`, assembling all layers.
    ///
    /// See the module docs for the assembly order and the redo-handler /
    /// CLOG wiring.
    ///
    /// # Panics
    ///
    /// Panics if `config.clog_buffer_frames` is outside [4, 1024]
    /// (validation lives in [`ClogBuffer::open`], tech-selection §6.3).
    pub fn open(data_dir: &Path, config: EngineConfig) -> Result<Self> {
        // 1. Disk CLOG first: WAL replay during storage recovery records
        //    terminal states into it (idempotently).
        let clog = Arc::new(ClogBuffer::open(data_dir, config.clog_buffer_frames)?);
        // 2. Storage recovery with the heap + txn + btree redo handlers and
        //    the CLOG (Stage M wave 2: btree records — index entries, the
        //    3-step split protocol, meta records — must replay too).
        //    Phase 2 M5 Stage C slice 1: the seven HNSW handlers (121–127)
        //    register through pg_am_hnsw::redo::hnsw_redo_handlers
        //    (tech-selection §2).
        let sequences = SequenceManager::default();
        let mut redo_handlers = heap_redo_handlers();
        redo_handlers.extend(txn_redo_handlers());
        redo_handlers.extend(btree_redo_handlers());
        redo_handlers.extend(pg_am_hnsw::redo::hnsw_redo_handlers());
        redo_handlers.push(Box::new(pg_am_inverted::segment::SegmentSealRedoHandler));
        redo_handlers.push(Box::new(pg_am_inverted::segment::SegmentMergeRedoHandler));
        redo_handlers.push(Box::new(sequences.redo_handler()));
        let undo_handlers: Vec<Box<dyn UndoHandler>> =
            vec![Box::new(HeapUndoHandler), Box::new(BTreeUndoHandler)];
        let storage = StorageEngine::open_with_redo_and_clog(
            data_dir,
            &config.storage,
            redo_handlers,
            undo_handlers,
            Arc::clone(&clog) as Arc<dyn ClogAccessor>,
        )?;
        // 3. Checkpoint-time CLOG flush hook (§6.4, v2.3-21). The engine
        //    never starts background checkpointing, so this cannot race a
        //    checkpoint already in flight.
        storage
            .checkpoint()
            .set_clog_flush(Arc::clone(&clog) as Arc<dyn ClogFlush>);

        // 3b. M2a → M2b migration: load any leftover `clog-snapshot.bin`
        //     (the M2a bridge for pre-checkpoint commit states, written by
        //     directories last opened before the disk CLOG existed) into the
        //     disk CLOG. Replay already covered the post-checkpoint WAL
        //     suffix; this covers the pre-checkpoint M2a-era states.
        //     Missing file = no-op; corrupt file = hard error (never silent).
        crate::clog_snapshot_migrate::migrate_legacy_clog_snapshot(data_dir, &clog)?;

        // 4. One catalog per engine (module docs: OID-allocator monotonicity).
        let catalog = Catalog::open(&storage)?;

        // 5./6. Transaction manager and heap AM over the shared components.
        //     The manager comes FIRST: the heap AM's §9.1 row-lock protocol
        //     (M2c Stage P) needs its wait capability installed before the
        //     AM is shared.
        //
        // The deadlock-victim registry (M2c Stage R) is created here so the
        // SAME Arc is shared by the TxnManager (row-lock waits), the
        // LockManager (table-lock waits), and the DeadlockDetector (which
        // marks). A victim flag set by the detector is thus visible to
        // whichever wait loop the victim is parked in.
        let victims = Arc::new(DeadlockVictims::new());
        let wal: Arc<dyn CommitWal> = Arc::clone(storage.wal_writer()) as Arc<dyn CommitWal>;
        let txn = Arc::new(
            TxnManager::new(
                storage.txn_id_clock(),
                wal,
                Arc::clone(&clog) as Arc<dyn ClogAccessor>,
            )
            .with_deadlock_victims(Arc::clone(&victims)),
        );
        let mut heap = HeapAM::new(
            Arc::clone(storage.buffer_pool()),
            Arc::clone(storage.wal_writer()),
        );
        // 5b. Row-lock waiter (M2c Stage P): with this installed, the heap
        //     AM runs the full §9.1 5-step protocol (wait on an in-progress
        //     t_xmax, `TupleConcurrentlyUpdated` on a committed one) instead
        //     of the legacy "second-writer-errors" behavior.
        heap.set_row_waiter(Arc::clone(&txn) as Arc<dyn RowWaiter>);
        // 5c. Page allocator (M3 Stage C): vacuum's `reclaim` returns fully
        //     emptied heap pages to the allocator after unlinking them from
        //     the chain.
        heap.set_page_allocator(Arc::clone(storage.page_allocator()));
        // 6b. Checkpoint ATT snapshot source (Stage N, §11.4): every
        //     checkpoint persists the manager's in-flight XIDs as the ATT
        //     snapshot file referenced by the v2 CheckpointEnd record. The
        //     engine never starts background checkpointing, so this cannot
        //     race a checkpoint already in flight (same argument as step 3).
        storage
            .checkpoint()
            .set_att_provider(Arc::clone(&txn) as Arc<dyn AttProvider>);

        // 6c. Commit/checkpoint barrier (M2c Stage P): the checkpoint
        //     coordinator takes the manager's barrier WRITE guard across its
        //     critical section while commit_txn/abort_txn hold READ guards,
        //     closing the "neither snapshot nor replay" window by
        //     construction (this replaces the engine-level `commit_barrier`
        //     field of Stage L). Same no-race argument as step 3.
        storage
            .checkpoint()
            .set_commit_barrier(txn.commit_barrier());

        // 7. Table registry from the catalog, index registry from the
        //    `pg_index` heap chain.
        let registry = Self::build_registry(&catalog, &heap, &txn, clog.as_ref(), &HashMap::new())?;
        let indexes = Self::build_index_registry(
            data_dir,
            &catalog,
            &heap,
            clog.as_ref(),
            &registry,
            &config.inverted_analyzer,
        )?;

        // 8. Lock manager (sharing the victim registry) and the deadlock
        //    detector thread (M2c Stage R, §9.3). Started LAST, once both
        //    managers exist; one detector per engine instance.
        let lock_manager = Arc::new(LockManager::new().with_deadlock_victims(Arc::clone(&victims)));
        let deadlock_detector = DeadlockDetector::start(
            Arc::clone(&txn),
            Arc::clone(&lock_manager),
            victims,
            config.deadlock_detector_interval,
        );

        let engine = Self {
            storage,
            catalog,
            heap,
            txn,
            clog,
            lock_manager,
            deadlock_detector,
            registry: RwLock::new(registry),
            indexes: RwLock::new(indexes),
            hnsw_handles: Arc::new(Mutex::new(std::collections::HashMap::new())),
            inverted_handles: Arc::new(Mutex::new(std::collections::HashMap::new())),
            pending_inverted: Arc::new(Mutex::new(HashMap::new())),
            inverted_lag: Arc::new(Mutex::new(HashMap::new())),
            #[cfg(test)]
            inverted_commit_finished: Arc::new(Mutex::new(HashMap::new())),
            #[cfg(test)]
            inverted_visibility_samples: Arc::new(Mutex::new(HashMap::new())),
            pending_hnsw: Arc::new(Mutex::new(HashMap::new())),
            ddl_lock: Mutex::new(()),
            instance_id: NEXT_ENGINE_INSTANCE_ID.fetch_add(1, Ordering::Relaxed),
            index_undo: Arc::new(Mutex::new(HashMap::new())),
            query_stats: QueryStats::new(config.query_stats_capacity),
            allow_stale_inverted: config.allow_stale_inverted,
            inverted_seal_batch_size: config.inverted_seal_batch_size,
            inverted_merge_enabled: config.inverted_merge_enabled,
            inverted_analyzer: config.inverted_analyzer,
            sequences,
        };
        engine.initialize_sequences()?;

        // 7b. Stage D migration for pre-TOAST directories. New tables already
        //     carry reltoastrelid; legacy TOASTable tables get the hidden heap
        //     and catalog metadata once, transactionally, before any DML.
        engine.install_missing_toast_relations()?;

        // 7c. Recovery-time index compensation for loser transactions
        //     (Stage T stress finding): index maintenance WAL records are
        //     physical (xid=0), so the CLOG/MVCC filtering that hides a
        //     loser transaction's heap tuples does NOT cover the index. A
        //     crash between a transaction's index maintenance and its
        //     commit/abort record leaves the index physically updated
        //     while the heap side stays invisible:
        //
        //     - loser-INSERTED entries point at invisible tuples — masked
        //       by `index_lookup`'s visibility filter, no action needed;
        //     - loser-DELETED entries (delete / non-HOT update old side)
        //       leave a still-visible row with NO index entry — not
        //       recoverable by filtering; compensated here by re-inserting
        //       `(key, tid)` recomputed from the row's on-page bytes.
        //
        //     Idempotent across repeated recoveries: each re-insert is
        //     preceded by an exact `(key, tid)` existence check.
        engine.compensate_loser_index_entries(data_dir)?;
        Ok(engine)
    }

    /// Rebuild the table registry through a fresh MVCC heap scan. Raw
    /// last-version-wins cannot distinguish an aborted DROP marker from a
    /// committed one, so recovery must use visibility rather than slot order.
    fn default_map(&self) -> Result<HashMap<(i64, i32), Oid>> {
        let Some(first_page) = self
            .sequences
            .relation_pages
            .lock()
            .get(&PG_RUST_ATTRDEFAULTS_OID)
            .copied()
        else {
            return Ok(HashMap::new());
        };
        let (snapshot, _guard) = self.txn.snapshot(TxnId::INVALID);
        let def = attrdefaults_table(first_page);
        let columns = def.column_types();
        let rows = self.heap.scan(ScanContext {
            rel: RelationDesc {
                rel_oid: def.oid.raw(),
                first_page,
                columns: &columns,
            },
            snapshot: &snapshot,
            clog: self.clog.as_ref(),
        })?;
        let class_columns = PG_CLASS.column_types();
        let class_rows = self.heap.scan(ScanContext {
            rel: RelationDesc {
                rel_oid: PG_CLASS.oid.raw(),
                first_page: PG_CLASS.first_page,
                columns: &class_columns,
            },
            snapshot: &snapshot,
            clog: self.clog.as_ref(),
        })?;
        let live_tables: HashSet<i64> = class_rows
            .into_iter()
            .filter_map(|(_, values)| match (values.first(), values.get(2)) {
                (Some(Some(Datum::Int8(oid))), Some(Some(Datum::Text(kind))))
                    if kind == RELKIND_TABLE =>
                {
                    Some(*oid)
                }
                _ => None,
            })
            .collect();
        let mut defaults = HashMap::new();
        for (_tid, values) in rows {
            let (
                Some(Some(Datum::Int8(rel_oid))),
                Some(Some(Datum::Int4(attnum))),
                Some(Some(Datum::Int8(sequence_oid))),
            ) = (values.first(), values.get(1), values.get(2))
            else {
                return Err(EngineError::Corrupted(
                    "malformed pg_rust_attrdefaults row".to_string(),
                ));
            };
            // A pre-fix DROP could leave SERIAL metadata behind. Orphaned
            // defaults are ignored just like orphaned index catalog rows.
            if !live_tables.contains(rel_oid) {
                continue;
            }
            if *attnum <= 0 {
                return Err(EngineError::Corrupted(format!(
                    "invalid default attnum {attnum} for relation {rel_oid}"
                )));
            }
            let sequence_oid = u64::try_from(*sequence_oid).map_err(|_| {
                EngineError::Corrupted(format!("negative default sequence OID {sequence_oid}"))
            })?;
            if !self
                .sequences
                .states
                .lock()
                .contains_key(&Oid(sequence_oid))
            {
                return Err(EngineError::Corrupted(format!(
                    "default for relation {rel_oid} references missing sequence {sequence_oid}"
                )));
            }
            if defaults
                .insert((*rel_oid, *attnum), Oid(sequence_oid))
                .is_some()
            {
                return Err(EngineError::Corrupted(format!(
                    "duplicate default for relation {rel_oid} attnum {attnum}"
                )));
            }
        }
        Ok(defaults)
    }

    fn build_registry(
        catalog: &Catalog,
        heap: &HeapAM,
        txn: &TxnManager,
        clog: &dyn ClogAccessor,
        default_map: &HashMap<(i64, i32), Oid>,
    ) -> Result<HashMap<String, TableEntry>> {
        let (snapshot, _guard) = txn.snapshot(TxnId::INVALID);
        let columns = PG_CLASS.column_types();
        let rows = heap.scan(ScanContext {
            rel: RelationDesc {
                rel_oid: PG_CLASS.oid.raw(),
                first_page: PG_CLASS.first_page,
                columns: &columns,
            },
            snapshot: &snapshot,
            clog,
        })?;
        let mut live: HashMap<i64, Vec<Value>> = HashMap::new();
        for (_, values) in rows {
            let (Some(Some(Datum::Int8(oid))), Some(Some(Datum::Text(kind)))) =
                (values.first(), values.get(2))
            else {
                continue;
            };
            if kind == RELKIND_DROPPED {
                live.remove(oid);
            } else {
                live.insert(*oid, values);
            }
        }

        let mut registry = HashMap::new();
        for (raw_oid, values) in live {
            let oid = u64::try_from(raw_oid)
                .map_err(|_| EngineError::Corrupted(format!("negative pg_class OID {raw_oid}")))?;
            if oid < Oid::FIRST_USER.0 {
                continue;
            }
            let kind = match values.get(2) {
                Some(Some(Datum::Text(kind))) => kind,
                _ => continue,
            };
            if kind != RELKIND_TABLE {
                continue;
            }
            let name = match values.get(1) {
                Some(Some(Datum::Text(name))) => name.clone(),
                _ => continue,
            };
            let natts = match values.get(3) {
                Some(Some(Datum::Int4(natts))) => *natts,
                _ => continue,
            };
            let table_oid = TableOid::new(Oid(oid));
            let Some(relpages) = catalog.relpages_of(table_oid) else {
                tracing::warn!(
                    table = %name,
                    oid = oid,
                    "pg_class row without pg_rust_relpages entry; skipping half-created table"
                );
                continue;
            };
            let mut columns = Vec::with_capacity(natts as usize);
            for attr in catalog.attributes_of(table_oid) {
                let col_type = column_type_of_attr(attr.type_oid, attr.len).ok_or_else(|| {
                    EngineError::Corrupted(format!(
                        "{}.{}: unknown type OID {:?} or malformed vector len {}",
                        name, attr.name, attr.type_oid, attr.len
                    ))
                })?;
                columns.push(ColumnDef {
                    name: attr.name.clone(),
                    col_type,
                });
            }
            let toast = match values.get(4) {
                Some(Some(Datum::Int8(raw_toast_oid))) if *raw_toast_oid != 0 => {
                    let toast_oid = u64::try_from(*raw_toast_oid).map_err(|_| {
                        EngineError::Corrupted(format!(
                            "negative reltoastrelid {raw_toast_oid} for table {name:?}"
                        ))
                    })?;
                    if toast_oid > u32::MAX as u64 {
                        return Err(EngineError::Corrupted(format!(
                            "reltoastrelid {toast_oid} exceeds the 20-byte pointer's u32 range"
                        )));
                    }
                    let toast = ToastRelation {
                        rel_oid: Oid(toast_oid),
                        first_page: relpages.last_page,
                    };
                    heap.register_toast_relation(Oid(oid), toast)?;
                    Some(toast)
                }
                Some(Some(Datum::Int8(_))) => None,
                _ => {
                    return Err(EngineError::Corrupted(format!(
                        "table {name:?} has malformed pg_class.reltoastrelid"
                    )))
                }
            };
            if registry
                .insert(
                    name.clone(),
                    TableEntry {
                        oid: Oid(oid),
                        first_page: relpages.first_page,
                        defaults: (0..columns.len())
                            .map(|index| default_map.get(&(raw_oid, index as i32 + 1)).copied())
                            .collect(),
                        columns,
                        toast,
                    },
                )
                .is_some()
            {
                return Err(EngineError::Corrupted(format!(
                    "duplicate live pg_class rows for table {:?}",
                    name
                )));
            }
        }
        Ok(registry)
    }

    /// Rebuild the index registry by scanning the `pg_index` heap chain
    /// (the catalog snapshot intentionally does not read `pg_index`, so the
    /// engine reads it through the heap AM; rows were committed through
    /// auto-commit, so ordinary visibility applies).
    ///
    /// Each row is joined with the catalog snapshot for the index's single
    /// `pg_attribute` row (attnum = 1, `attname` = indexed column) and its
    /// `pg_rust_relpages` entry (`first_page` = B+Tree meta page). A
    /// `pg_index` row missing either is a crash-half-written index build —
    /// leaked pages, skipped with a warning (same policy as half-created
    /// tables).
    fn build_index_registry(
        data_dir: &Path,
        catalog: &Catalog,
        heap: &HeapAM,
        clog: &dyn ClogAccessor,
        tables: &HashMap<String, TableEntry>,
        expected_analyzer: &pg_am_inverted::AnalyzerConfig,
    ) -> Result<Vec<IndexEntry>> {
        let columns = PG_INDEX.column_types();
        let rows = heap.scan(ScanContext {
            rel: RelationDesc {
                rel_oid: PG_INDEX.oid.raw(),
                first_page: PG_INDEX.first_page,
                columns: &columns,
            },
            snapshot: &Snapshot::everything(),
            clog,
        })?;
        let mut indexes = Vec::new();
        for (_tid, values) in rows {
            let (Some(Datum::Int8(indexrelid)), Some(Datum::Int8(indrelid))) =
                (&values[0], &values[1])
            else {
                return Err(EngineError::Corrupted(
                    "pg_index row with non-int8 identity columns".to_string(),
                ));
            };
            let index_oid = Oid(*indexrelid as u64);
            let table_oid = Oid(*indrelid as u64);
            if !tables.values().any(|entry| entry.oid == table_oid) {
                // DROP TABLE leaves index rows behind, but an index whose
                // parent is dropped is unreachable and must not be rebuilt.
                continue;
            }
            let attrs = catalog.attributes_of(pg_catalog::TableOid(index_oid));
            let relpages = catalog.relpages_of(pg_catalog::TableOid(index_oid));
            let (Some(attr), Some(relpages)) = (attrs.first(), relpages) else {
                tracing::warn!(
                    index_oid = index_oid.0,
                    "pg_index row without pg_attribute/pg_rust_relpages; skipping half-built index"
                );
                continue;
            };
            let key_type = column_type_of_attr(attr.type_oid, attr.len).ok_or_else(|| {
                EngineError::Corrupted(format!(
                    "index {index_oid}: unknown type OID {:?} or malformed vector len {}",
                    attr.type_oid, attr.len
                ))
            })?;
            let relation = catalog
                .relation(pg_catalog::TableOid(index_oid))
                .ok_or_else(|| {
                    EngineError::Corrupted(format!("index {index_oid}: missing pg_class row"))
                })?;
            let (kind, expected_am_name) = match relation.am {
                BTREE_AM_OID => (AmKind::BTree, "btree"),
                HNSW_AM_OID => (AmKind::Hnsw, "hnsw"),
                INVERTED_AM_OID => (AmKind::Inverted, "inverted"),
                other => {
                    return Err(EngineError::Corrupted(format!(
                        "index {index_oid}: unknown access method {other}"
                    )))
                }
            };
            let am = catalog
                .access_methods()
                .iter()
                .find(|am| am.oid == relation.am)
                .ok_or_else(|| {
                    EngineError::Corrupted(format!(
                        "index {index_oid}: access method {} has no pg_am row",
                        relation.am
                    ))
                })?;
            if am.name != expected_am_name {
                return Err(EngineError::Corrupted(format!(
                    "index {index_oid}: access method {} has amname {:?}, expected {expected_am_name:?}",
                    relation.am, am.name
                )));
            }
            let inverted = if matches!(kind, AmKind::Inverted) {
                if key_type != ColumnType::Text {
                    return Err(EngineError::Corrupted(format!(
                        "inverted index {index_oid} requires a TEXT column, got {key_type:?}"
                    )));
                }
                let store =
                    pg_am_inverted::SegmentStore::open(data_dir, index_oid, expected_analyzer)
                        .map_err(|error| {
                            EngineError::Corrupted(format!(
                                "cannot recover inverted index {index_oid}: {error}"
                            ))
                        })?;
                let metadata = InvertedIndexMetadata::from_store(&store);
                metadata.analyzer.validate().map_err(|error| {
                    EngineError::Corrupted(format!(
                        "inverted index {index_oid}: invalid analyzer metadata: {error}"
                    ))
                })?;
                Some(metadata)
            } else {
                None
            };
            indexes.push(IndexEntry {
                kind,
                inverted,
                index_oid,
                table_oid: Oid(*indrelid as u64),
                column: attr.name.clone(),
                key_type,
                meta_page: relpages.first_page,
                map_page: (relpages.last_page != relpages.first_page).then_some(relpages.last_page),
            });
        }
        Ok(indexes)
    }

    /// Flush all dirty pages, fsync the disk CLOG's dirty frames, persist
    /// the superblock (XID clock, OID counter, checkpoint LSN), and recycle
    /// WAL segments before the redo point.
    ///
    /// The CLOG flush runs **inside** the storage checkpoint — between
    /// `CheckpointBegin` and `CheckpointEnd`, via the `ClogFlush` hook
    /// installed at open (tech-selection §6.4, v2.3-21) — so there is no
    /// separate engine-level dump step: this is a pure
    /// `trigger_checkpoint` call. The commit-barrier write guard that Stage
    /// L took here is now taken inside the coordinator itself (M2c Stage P,
    /// wired at open via `set_commit_barrier`). This is the only supported
    /// checkpoint path; background checkpointing is never started.
    pub fn checkpoint(&self) -> Result<()> {
        let recycle_before = self
            .indexes
            .read()
            .iter()
            .filter_map(|index| {
                let lsn = index.inverted.as_ref()?.watermark.durable_applied_lsn;
                lsn.is_valid().then_some(lsn)
            })
            .min();
        self.storage
            .trigger_checkpoint_with_recycle_before(recycle_before)?;
        Ok(())
    }

    /// Gracefully shut down background threads (deadlock detector,
    /// checkpointer, WAL writer).
    pub fn shutdown(&self) {
        // Stop the detector FIRST (M2c Stage R): it only reads the two
        // managers, but stopping it here guarantees no tick can mark a
        // victim while storage is tearing down underneath a wait loop.
        self.deadlock_detector.stop();
        self.storage.shutdown();
    }

    fn initialize_sequences(&self) -> Result<()> {
        self.ensure_sequence_relation(
            PG_RUST_SEQUENCES_OID,
            PG_RUST_SEQUENCES_NAME,
            sequences_table,
        )?;
        self.ensure_sequence_relation(
            PG_RUST_ATTRDEFAULTS_OID,
            PG_RUST_ATTRDEFAULTS_NAME,
            attrdefaults_table,
        )?;
        self.recover_sequences()?;
        let defaults = self.default_map()?;
        let registry = Self::build_registry(
            &self.catalog,
            &self.heap,
            &self.txn,
            self.clog.as_ref(),
            &defaults,
        )?;
        *self.registry.write() = registry;
        Ok(())
    }

    fn live_sequence_relation_page(&self, oid: Oid, name: &str) -> Result<Option<PageId>> {
        let (snapshot, _guard) = self.txn.snapshot(TxnId::INVALID);
        let class_columns = PG_CLASS.column_types();
        let class_rows = self.heap.scan(ScanContext {
            rel: RelationDesc {
                rel_oid: PG_CLASS.oid.raw(),
                first_page: PG_CLASS.first_page,
                columns: &class_columns,
            },
            snapshot: &snapshot,
            clog: self.clog.as_ref(),
        })?;
        let mut live_class = false;
        for (_, values) in class_rows {
            let (
                Some(Some(Datum::Int8(row_oid))),
                Some(Some(Datum::Text(row_name))),
                Some(Some(Datum::Text(kind))),
            ) = (values.first(), values.get(1), values.get(2))
            else {
                continue;
            };
            if *row_oid == oid.0 as i64 {
                if row_name != name || kind != RELKIND_INTERNAL {
                    return Err(EngineError::Corrupted(format!(
                        "sequence catalog {name} has inconsistent pg_class metadata"
                    )));
                }
                live_class = true;
            }
        }
        let relpages_columns = PG_RELPAGES.column_types();
        let relpages_rows = self.heap.scan(ScanContext {
            rel: RelationDesc {
                rel_oid: PG_RELPAGES.oid.raw(),
                first_page: PG_RELPAGES.first_page,
                columns: &relpages_columns,
            },
            snapshot: &snapshot,
            clog: self.clog.as_ref(),
        })?;
        let first_page = relpages_rows.into_iter().find_map(|(_, values)| {
            if values.first() == Some(&Some(Datum::Int8(oid.0 as i64))) {
                let Some(Some(Datum::Int8(first_page))) = values.get(1) else {
                    return None;
                };
                return Some(PageId(*first_page as u64));
            }
            None
        });
        match (live_class, first_page) {
            (true, Some(first_page)) => Ok(Some(first_page)),
            (false, None) => Ok(None),
            _ => Err(EngineError::Corrupted(format!(
                "sequence catalog {name} is only partially present"
            ))),
        }
    }

    fn ensure_sequence_relation(
        &self,
        oid: Oid,
        name: &str,
        make_def: fn(PageId) -> SystemTableDef,
    ) -> Result<()> {
        if let Some(first_page) = self.live_sequence_relation_page(oid, name)? {
            self.sequences.relation_pages.lock().insert(oid, first_page);
            return Ok(());
        }

        let first_page = self.heap.create_heap(oid)?;
        let def = make_def(first_page);
        self.auto_commit_with_hooks(
            |snap| {
                self.lock_oid(snap.current_xid(), oid, LockMode::AccessExclusive)?;
                let class_row = vec![
                    Some(Datum::Int8(oid.0 as i64)),
                    Some(Datum::Text(name.to_string())),
                    Some(Datum::Text(RELKIND_INTERNAL.to_string())),
                    Some(Datum::Int4(def.columns.len() as i32)),
                    Some(Datum::Int8(0)),
                    Some(Datum::Int8(HEAP_AM_OID.0 as i64)),
                ];
                self.insert_catalog_row(snap, &PG_CLASS, &class_row)?;
                if crate::sequence::test_hooks::fail_internal_catalog_write() {
                    return Err(EngineError::InvalidArgument(
                        "injected internal sequence catalog write failure".to_string(),
                    ));
                }
                for (index, column) in def.columns.iter().enumerate() {
                    self.insert_catalog_row(
                        snap,
                        &PG_ATTRIBUTE,
                        &[
                            Some(Datum::Int8(oid.0 as i64)),
                            Some(Datum::Text(column.name.to_string())),
                            Some(Datum::Int8(column.type_oid.raw().0 as i64)),
                            Some(Datum::Int4(column.len)),
                            Some(Datum::Int4(index as i32 + 1)),
                            Some(Datum::Int4(0)),
                            Some(Datum::Int4(1)),
                        ],
                    )?;
                }
                self.insert_catalog_row(
                    snap,
                    &PG_RELPAGES,
                    &[
                        Some(Datum::Int8(oid.0 as i64)),
                        Some(Datum::Int8(first_page.0 as i64)),
                        Some(Datum::Int8(first_page.0 as i64)),
                        Some(Datum::Int8(1)),
                    ],
                )
            },
            || {
                self.sequences.relation_pages.lock().insert(oid, first_page);
                Ok(())
            },
            || {},
        )
    }

    fn sequence_relation(&self) -> Result<SystemTableDef> {
        let first_page = self
            .sequences
            .relation_pages
            .lock()
            .get(&PG_RUST_SEQUENCES_OID)
            .copied()
            .ok_or_else(|| {
                EngineError::Corrupted("pg_rust_sequences is not registered".to_string())
            })?;
        Ok(sequences_table(first_page))
    }

    fn attrdefaults_relation(&self) -> Result<SystemTableDef> {
        let first_page = self
            .sequences
            .relation_pages
            .lock()
            .get(&PG_RUST_ATTRDEFAULTS_OID)
            .copied()
            .ok_or_else(|| {
                EngineError::Corrupted("pg_rust_attrdefaults is not registered".to_string())
            })?;
        Ok(attrdefaults_table(first_page))
    }

    fn recover_sequences(&self) -> Result<()> {
        let (snapshot, _guard) = self.txn.snapshot(TxnId::INVALID);
        let scan = |def: &SystemTableDef| -> Result<Vec<(Tid, Vec<Value>)>> {
            let columns = def.column_types();
            Ok(self.heap.scan(ScanContext {
                rel: RelationDesc {
                    rel_oid: def.oid.raw(),
                    first_page: def.first_page,
                    columns: &columns,
                },
                snapshot: &snapshot,
                clog: self.clog.as_ref(),
            })?)
        };
        let rows = scan(&self.sequence_relation()?)?;
        let class_rows = scan(&PG_CLASS)?;
        let default_rows = scan(&self.attrdefaults_relation()?)?;
        let live_tables: HashSet<i64> = class_rows
            .iter()
            .filter_map(|(_, values)| match (values.first(), values.get(2)) {
                (Some(Some(Datum::Int8(oid))), Some(Some(Datum::Text(kind))))
                    if kind == RELKIND_TABLE =>
                {
                    Some(*oid)
                }
                _ => None,
            })
            .collect();
        let sequence_parents: HashMap<i64, i64> = default_rows
            .into_iter()
            .filter_map(|(_, values)| match (values.first(), values.get(2)) {
                (Some(Some(Datum::Int8(rel_oid))), Some(Some(Datum::Int8(sequence_oid)))) => {
                    Some((*sequence_oid, *rel_oid))
                }
                _ => None,
            })
            .collect();
        let live_classes: HashMap<i64, Vec<Value>> = class_rows
            .into_iter()
            .filter_map(|(_, values)| match values.first() {
                Some(Some(Datum::Int8(oid))) => Some((*oid, values)),
                _ => None,
            })
            .collect();
        let recovered = self.sequences.recovered_high_water();
        for (_tid, values) in rows {
            let (
                Some(Some(Datum::Int8(raw_oid))),
                Some(Some(Datum::Text(name))),
                Some(Some(Datum::Int8(high_water))),
            ) = (values.first(), values.get(1), values.get(2))
            else {
                return Err(EngineError::Corrupted(
                    "malformed pg_rust_sequences row".to_string(),
                ));
            };
            let oid = u64::try_from(*raw_oid)
                .map_err(|_| EngineError::Corrupted(format!("negative sequence OID {raw_oid}")))?;
            if *high_water < 0 {
                return Err(EngineError::Corrupted(format!(
                    "sequence {name} has negative high water {high_water}"
                )));
            }
            // Pre-fix DROP TABLE left the owned sequence and default rows
            // behind. They must not resurrect or block name reuse on reopen.
            if sequence_parents
                .get(raw_oid)
                .is_some_and(|parent| !live_tables.contains(parent))
            {
                continue;
            }
            let class = live_classes.get(raw_oid).ok_or_else(|| {
                EngineError::Corrupted(format!("sequence {name} has no live pg_class row"))
            })?;
            let class_kind = class.get(2);
            if matches!(class_kind, Some(Some(Datum::Text(kind))) if kind == RELKIND_DROPPED) {
                continue;
            }
            if class.get(1) != Some(&Some(Datum::Text(name.clone())))
                || !matches!(class_kind, Some(Some(Datum::Text(kind))) if kind == RELKIND_SEQUENCE)
            {
                return Err(EngineError::Corrupted(format!(
                    "sequence {name} has inconsistent pg_class metadata"
                )));
            }
            let oid = Oid(oid);
            self.sequences.states.lock().insert(
                oid,
                crate::sequence::SequenceState {
                    name: name.clone(),
                    high_water: *high_water,
                },
            );
            if let Some(recovered_high_water) = recovered.get(&oid).copied() {
                if recovered_high_water > *high_water {
                    self.advance_sequence_to(oid, recovered_high_water)?;
                }
            }
        }
        Ok(())
    }

    /// Create one sequence with an initial high-water mark of zero.
    pub fn create_sequence(&self, name: &str) -> Result<Oid> {
        if name.is_empty() {
            return Err(EngineError::InvalidArgument(
                "sequence name must not be empty".to_string(),
            ));
        }
        let _ddl = self.ddl_lock.lock();
        if self.relation_name_exists(name)?
            || self
                .sequences
                .states
                .lock()
                .values()
                .any(|state| state.name == name)
        {
            return Err(EngineError::InvalidArgument(format!(
                "sequence {name:?} already exists"
            )));
        }
        let oid = self.catalog.oid_allocator().alloc();
        let mut states = self.sequences.states.lock();
        self.auto_commit_with_hooks(
            |snap| {
                self.lock_oid(snap.current_xid(), oid, LockMode::AccessExclusive)?;
                self.insert_catalog_row(snap, &PG_CLASS, &sequence_class_row(oid, name))?;
                let def = self.sequence_relation()?;
                self.insert_catalog_row(
                    snap,
                    &def,
                    &[
                        Some(Datum::Int8(oid.0 as i64)),
                        Some(Datum::Text(name.to_string())),
                        Some(Datum::Int8(0)),
                    ],
                )
            },
            || {
                states.insert(
                    oid,
                    crate::sequence::SequenceState {
                        name: name.to_string(),
                        high_water: 0,
                    },
                );
                Ok(())
            },
            || {},
        )?;
        Ok(oid)
    }

    /// Return and record the next value of a named sequence in this session.
    pub fn nextval(&self, session: &mut SqlSession, name: &str) -> Result<i64> {
        let oid = self.sequence_oid(name)?;
        let value = self.nextval_oid(oid)?;
        session.record_nextval(oid, value);
        Ok(value)
    }

    /// Return this session's current value of a named sequence.
    pub fn currval(&self, session: &SqlSession, name: &str) -> Result<i64> {
        let oid = self.sequence_oid(name)?;
        session.currval(oid)
    }

    fn sequence_oid(&self, name: &str) -> Result<Oid> {
        self.sequences
            .states
            .lock()
            .iter()
            .find_map(|(oid, state)| (state.name == name).then_some(*oid))
            .ok_or_else(|| {
                EngineError::InvalidArgument(format!("sequence {name:?} does not exist"))
            })
    }

    fn nextval_oid(&self, oid: Oid) -> Result<i64> {
        let mut states = self.sequences.states.lock();
        let current = states
            .get(&oid)
            .ok_or_else(|| {
                EngineError::InvalidArgument(format!("sequence OID {oid} does not exist"))
            })?
            .high_water;
        let next = current.checked_add(1).ok_or_else(|| {
            EngineError::InvalidArgument(format!("sequence OID {oid} exhausted Int64 values"))
        })?;
        self.advance_sequence_to_locked(&mut states, oid, next)
    }

    fn advance_sequence_to(&self, oid: Oid, high_water: i64) -> Result<i64> {
        let mut states = self.sequences.states.lock();
        self.advance_sequence_to_locked(&mut states, oid, high_water)
    }

    fn advance_sequence_to_locked(
        &self,
        states: &mut HashMap<Oid, crate::sequence::SequenceState>,
        oid: Oid,
        high_water: i64,
    ) -> Result<i64> {
        if high_water < 0 {
            return Err(EngineError::InvalidArgument(
                "sequence high water must be non-negative".to_string(),
            ));
        }
        let current = states.get(&oid).ok_or_else(|| {
            EngineError::InvalidArgument(format!("sequence OID {oid} does not exist"))
        })?;
        if high_water <= current.high_water {
            return Ok(current.high_water);
        }
        let name = current.name.clone();
        self.auto_commit_with_hooks(
            |snap| {
                let record = WalRecord::sequence_advance(oid, high_water, snap.current_xid())?;
                let record_len = record.record_size() as u64;
                let lsn = self.storage.wal_writer().append(record)?;
                if crate::sequence::test_hooks::fail_nextval_flush() {
                    return Err(EngineError::InvalidArgument(
                        "injected sequence WAL flush failure".to_string(),
                    ));
                }
                self.storage
                    .wal_writer()
                    .flush_to(Lsn(lsn.0 + record_len))?;
                let def = self.sequence_relation()?;
                let needed = self.sequence_row_len(&def, oid, &name, high_water)?;
                if self.sequence_catalog_needs_compaction(&def, needed)? {
                    self.lock_oid(snap.current_xid(), def.oid.raw(), LockMode::AccessExclusive)?;
                    self.compact_sequence_catalog(&def)?;
                }
                let (tid, _) = self.find_sequence_row(snap, &def, oid)?;
                self.update_catalog_row(
                    snap,
                    &def,
                    tid,
                    &[
                        Some(Datum::Int8(oid.0 as i64)),
                        Some(Datum::Text(name.clone())),
                        Some(Datum::Int8(high_water)),
                    ],
                )
            },
            || {
                if let Some(state) = states.get_mut(&oid) {
                    state.high_water = high_water;
                }
                Ok(())
            },
            || {},
        )?;
        Ok(high_water)
    }

    fn sequence_row_len(
        &self,
        def: &SystemTableDef,
        oid: Oid,
        name: &str,
        high_water: i64,
    ) -> Result<usize> {
        let columns = def.column_types();
        let row = vec![
            Some(Datum::Int8(oid.0 as i64)),
            Some(Datum::Text(name.to_string())),
            Some(Datum::Int8(high_water)),
        ];
        Ok(encode_tuple(tuple_header(0), &columns, &row)?.len() + LINE_POINTER_SIZE)
    }

    fn sequence_catalog_needs_compaction(
        &self,
        def: &SystemTableDef,
        needed: usize,
    ) -> Result<bool> {
        let guard = self.storage.buffer_pool().pin(def.first_page)?;
        let page: &[u8; PAGE_SIZE] = guard
            .page()
            .try_into()
            .expect("buffer pool pages are PAGE_SIZE bytes");
        Ok(SlottedPage::free_space(page) < needed.max(1024))
    }

    fn compact_sequence_catalog(&self, def: &SystemTableDef) -> Result<()> {
        let columns = def.column_types();
        let dead = self.heap.scan_dead_tuples(
            RelationDesc {
                rel_oid: def.oid.raw(),
                first_page: def.first_page,
                columns: &columns,
            },
            TxnId(u64::MAX),
            self.clog.as_ref(),
        )?;
        if dead.is_empty() {
            return Ok(());
        }
        // Sequence high water is non-transactional metadata: only the latest
        // committed row is needed, so obsolete versions may be physically pruned.
        if crate::sequence::test_hooks::fail_compaction() {
            return Err(EngineError::InvalidArgument(
                "injected sequence catalog compaction failure".to_string(),
            ));
        }
        self.heap.reclaim(
            RelationDesc {
                rel_oid: def.oid.raw(),
                first_page: def.first_page,
                columns: &columns,
            },
            &dead,
        )?;
        Ok(())
    }

    fn find_sequence_row(
        &self,
        snap: &Snapshot,
        def: &SystemTableDef,
        oid: Oid,
    ) -> Result<(Tid, i64)> {
        let columns = def.column_types();
        let rows = self.heap.scan(ScanContext {
            rel: RelationDesc {
                rel_oid: def.oid.raw(),
                first_page: def.first_page,
                columns: &columns,
            },
            snapshot: snap,
            clog: self.clog.as_ref(),
        })?;
        for (tid, values) in rows {
            if values.first() == Some(&Some(Datum::Int8(oid.0 as i64))) {
                let Some(Some(Datum::Int8(high_water))) = values.get(2) else {
                    return Err(EngineError::Corrupted(
                        "malformed pg_rust_sequences high water".to_string(),
                    ));
                };
                return Ok((tid, *high_water));
            }
        }
        Err(EngineError::Corrupted(format!(
            "sequence OID {oid} has no committed high-water row"
        )))
    }

    /// Add hidden TOAST heaps to pre-Stage-D tables. Catalog updates and the
    /// in-memory publication use the same success/failure hooks as other DDL;
    /// a failed commit leaves only an unreachable, leaked empty heap.
    fn install_missing_toast_relations(&self) -> Result<()> {
        let pending: Vec<(String, TableEntry)> = self
            .registry
            .read()
            .iter()
            .filter(|(_, entry)| entry.toast.is_none())
            .filter(|(_, entry)| {
                entry
                    .columns
                    .iter()
                    .any(|column| toastable(column.col_type))
            })
            .map(|(name, entry)| (name.clone(), entry.clone()))
            .collect();
        for (name, entry) in pending {
            let toast_oid = self.catalog.oid_allocator().alloc();
            if toast_oid.0 > u32::MAX as u64 {
                return Err(EngineError::InvalidArgument(
                    "TOAST relation OID exceeds the frozen pointer's u32 range".to_string(),
                ));
            }
            let toast = ToastRelation {
                rel_oid: toast_oid,
                first_page: self.heap.create_heap(toast_oid)?,
            };
            self.heap.register_toast_relation(entry.oid, toast)?;
            self.auto_commit_with_hooks(
                |snap| {
                    self.lock_table(snap.current_xid(), &entry, LockMode::AccessExclusive)?;
                    self.write_toast_catalog_metadata(snap, &name, &entry, toast)
                },
                || {
                    let mut registry = self.registry.write();
                    let live = registry.get_mut(&name).ok_or_else(|| {
                        EngineError::Corrupted(format!(
                            "table {name:?} disappeared during TOAST migration"
                        ))
                    })?;
                    live.toast = Some(toast);
                    Ok(())
                },
                || {
                    self.heap.unregister_toast_relation(entry.oid);
                    self.heap.drop_relation(toast_oid);
                },
            )?;
        }
        Ok(())
    }

    /// Persist a legacy table's hidden relation in the two existing catalog
    /// rows. The hidden relation itself gets no pg_class/pg_attribute row.
    fn write_toast_catalog_metadata(
        &self,
        snap: &Snapshot,
        name: &str,
        entry: &TableEntry,
        toast: ToastRelation,
    ) -> Result<()> {
        let class_tid = self.find_live_pg_class_row(snap, entry.oid)?;
        let class_row = vec![
            Some(Datum::Int8(entry.oid.0 as i64)),
            Some(Datum::Text(name.to_string())),
            Some(Datum::Text(RELKIND_TABLE.to_string())),
            Some(Datum::Int4(entry.columns.len() as i32)),
            Some(Datum::Int8(toast.rel_oid.0 as i64)),
            Some(Datum::Int8(HEAP_AM_OID.0 as i64)),
        ];
        self.update_catalog_row(snap, &PG_CLASS, class_tid, &class_row)?;

        let relpages_tid = self.find_relpages_row(snap, entry.oid)?;
        let previous = self
            .catalog
            .relpages_of(TableOid::new(entry.oid))
            .ok_or_else(|| {
                EngineError::Corrupted(format!(
                    "table OID {} has no pg_rust_relpages row",
                    entry.oid.0
                ))
            })?;
        let relpages_row = vec![
            Some(Datum::Int8(entry.oid.0 as i64)),
            Some(Datum::Int8(entry.first_page.0 as i64)),
            Some(Datum::Int8(toast.first_page.0 as i64)),
            Some(Datum::Int8(previous.page_count as i64)),
        ];
        self.update_catalog_row(snap, &PG_RELPAGES, relpages_tid, &relpages_row)
    }

    fn update_catalog_row(
        &self,
        snap: &Snapshot,
        def: &SystemTableDef,
        old_tid: Tid,
        row: &[Value],
    ) -> Result<()> {
        let columns = def.column_types();
        let tuple = encode_tuple(tuple_header(snap.curcid()), &columns, row)?;
        self.ensure_catalog_room(def, tuple.len())?;
        self.heap.update(UpdateContext {
            rel: RelationDesc {
                rel_oid: def.oid.raw(),
                first_page: def.first_page,
                columns: &columns,
            },
            snapshot: snap,
            old_tid,
            new_tuple: &tuple,
            out_tid: None,
            clog: self.clog.as_ref(),
            hot_eligible: false,
        })?;
        Ok(())
    }

    fn delete_catalog_row(&self, snap: &Snapshot, def: &SystemTableDef, tid: Tid) -> Result<()> {
        let columns = def.column_types();
        self.heap.delete(DeleteContext {
            rel: RelationDesc {
                rel_oid: def.oid.raw(),
                first_page: def.first_page,
                columns: &columns,
            },
            snapshot: snap,
            tid,
            clog: self.clog.as_ref(),
        })?;
        Ok(())
    }

    /// Locate the relpages version visible to the current DDL transaction.
    fn find_relpages_row(&self, snap: &Snapshot, oid: Oid) -> Result<Tid> {
        let columns = PG_RELPAGES.column_types();
        let rows = self.heap.scan(ScanContext {
            rel: RelationDesc {
                rel_oid: PG_RELPAGES.oid.raw(),
                first_page: PG_RELPAGES.first_page,
                columns: &columns,
            },
            snapshot: snap,
            clog: self.clog.as_ref(),
        })?;
        for (tid, values) in rows {
            if matches!(values.first(), Some(Some(Datum::Int8(value))) if *value == oid.0 as i64) {
                return Ok(tid);
            }
        }
        Err(EngineError::Corrupted(format!(
            "pg_rust_relpages has no row for relation OID {}",
            oid.0
        )))
    }

    /// Create a table and register it in the system catalog.
    ///
    /// Runs as one auto-commit transaction: allocate an OID, allocate the
    /// first heap page, insert the `pg_class` / `pg_attribute` /
    /// `pg_rust_relpages` rows **through the heap AM**, register TOAST before
    /// commit, and publish the in-memory registry only after a durable commit.
    /// Registration or commit failure cleans up before table locks release.
    ///
    /// Fails with [`EngineError::TableExists`] if the name is taken, and
    /// with [`EngineError::CatalogFull`] if a system catalog's first page
    /// has no room (module-level M2a limitation).
    fn create_table_from_spec(
        &self,
        name: &str,
        columns: Vec<crate::sql::ColumnSpec>,
    ) -> Result<Oid> {
        let mut defaults = Vec::with_capacity(columns.len());
        let mut sequences = Vec::new();
        for column in &columns {
            if column.serial {
                let sequence_oid = self.catalog.oid_allocator().alloc();
                let sequence_name = format!("{name}_{}_seq", column.name);
                defaults.push(Some(sequence_oid));
                sequences.push((sequence_oid, sequence_name));
            } else {
                defaults.push(None);
            }
        }
        let schema = columns
            .into_iter()
            .map(|column| ColumnDef {
                name: column.name,
                col_type: column.col_type,
            })
            .collect::<Vec<_>>();
        self.create_table_with_defaults(name, &schema, &defaults, &sequences)
    }

    /// Create a table and persist its schema in the catalog.
    pub fn create_table(&self, name: &str, schema: &[ColumnDef]) -> Result<Oid> {
        self.create_table_with_defaults(name, schema, &vec![None; schema.len()], &[])
    }

    fn create_table_with_defaults(
        &self,
        name: &str,
        schema: &[ColumnDef],
        defaults: &[Option<Oid>],
        sequences: &[(Oid, String)],
    ) -> Result<Oid> {
        if defaults.len() != schema.len() {
            return Err(EngineError::InvalidArgument(
                "defaults must align with table columns".to_string(),
            ));
        }
        if name.is_empty() {
            return Err(EngineError::InvalidArgument(
                "table name must not be empty".to_string(),
            ));
        }
        if schema.is_empty() {
            return Err(EngineError::InvalidArgument(
                "cannot create a table with no columns".to_string(),
            ));
        }
        let _ddl = self.ddl_lock.lock();
        for (_, sequence_name) in sequences {
            if self.relation_name_exists(sequence_name)? {
                return Err(EngineError::InvalidArgument(format!(
                    "sequence {sequence_name:?} already exists"
                )));
            }
        }
        if self.registry.read().contains_key(name) || self.relation_name_exists(name)? {
            return Err(EngineError::TableExists(name.to_string()));
        }

        let oid = self.catalog.oid_allocator().alloc();
        let toast_oid = schema
            .iter()
            .any(|column| toastable(column.col_type))
            .then(|| self.catalog.oid_allocator().alloc());
        if toast_oid.is_some_and(|toast| toast.0 > u32::MAX as u64) {
            return Err(EngineError::InvalidArgument(
                "TOAST relation OID exceeds the frozen pointer's u32 range".to_string(),
            ));
        }
        // AccessExclusive on the fresh OID (M2c Stage P): ceremonial today
        // — nobody else can reference an OID that was just allocated — but
        // it fixes the DDL lock mode by construction and covers the
        // catalog-row writes like any other DDL. TOAST registration also runs
        // HERE, before commit: a registration error aborts the table instead
        // of returning an error after the table has already become durable.
        let created_metadata = RefCell::new(None);
        let result = self.auto_commit_with_hooks(
            |snap| {
                self.lock_oid(snap.current_xid(), oid, LockMode::AccessExclusive)?;
                let created = self.create_table_inner(snap, oid, toast_oid, name, schema)?;
                if let Some(toast) = created.1 {
                    self.heap.register_toast_relation(oid, toast)?;
                }
                *created_metadata.borrow_mut() = Some(created);
                let sequence_def = self.sequence_relation()?;
                let default_def = self.attrdefaults_relation()?;
                for (sequence_oid, sequence_name) in sequences {
                    self.insert_catalog_row(
                        snap,
                        &PG_CLASS,
                        &sequence_class_row(*sequence_oid, sequence_name),
                    )?;
                    self.insert_catalog_row(
                        snap,
                        &sequence_def,
                        &[
                            Some(Datum::Int8(sequence_oid.0 as i64)),
                            Some(Datum::Text(sequence_name.clone())),
                            Some(Datum::Int8(0)),
                        ],
                    )?;
                    let default_index = defaults
                        .iter()
                        .position(|candidate| candidate.as_ref() == Some(sequence_oid))
                        .ok_or_else(|| {
                            EngineError::Corrupted(
                                "SERIAL default has no sequence mapping".to_string(),
                            )
                        })?;
                    self.insert_catalog_row(
                        snap,
                        &default_def,
                        &[
                            Some(Datum::Int8(oid.0 as i64)),
                            Some(Datum::Int4(default_index as i32 + 1)),
                            Some(Datum::Int8(defaults[default_index].unwrap().0 as i64)),
                        ],
                    )?;
                }
                Ok(())
            },
            || {
                let (first_page, toast) = created_metadata
                    .borrow()
                    .expect("successful create_table installs metadata before commit");
                {
                    let mut states = self.sequences.states.lock();
                    for (sequence_oid, sequence_name) in sequences {
                        states.insert(
                            *sequence_oid,
                            crate::sequence::SequenceState {
                                name: sequence_name.clone(),
                                high_water: 0,
                            },
                        );
                    }
                }
                self.registry.write().insert(
                    name.to_string(),
                    TableEntry {
                        oid,
                        first_page,
                        defaults: defaults.to_vec(),
                        columns: schema.to_vec(),
                        toast,
                    },
                );
                Ok(())
            },
            || {
                // Runs before lock release on op or commit failure. The pages
                // themselves may leak after a crash, but no failed relation or
                // TOAST registration remains visible to later DML.
                self.heap.unregister_toast_relation(oid);
                self.heap.drop_relation(oid);
                if let Some(toast_oid) = toast_oid {
                    self.heap.drop_relation(toast_oid);
                }
            },
        );
        result.map(|()| oid)
    }

    /// The catalog-writing half of `create_table`, inside transaction `snap`.
    fn create_table_inner(
        &self,
        snap: &Snapshot,
        oid: Oid,
        toast_oid: Option<Oid>,
        name: &str,
        schema: &[ColumnDef],
    ) -> Result<(PageId, Option<ToastRelation>)> {
        ensure_float8_storage_supported(self.catalog.type_by_oid(FLOAT8_OID).is_some(), schema)?;
        let first_page = self.heap.create_heap(oid)?;
        let toast = toast_oid
            .map(|rel_oid| {
                let first_page = self.heap.create_heap(rel_oid)?;
                Ok::<ToastRelation, EngineError>(ToastRelation {
                    rel_oid,
                    first_page,
                })
            })
            .transpose()?;

        let class_row = vec![
            Some(Datum::Int8(oid.0 as i64)),
            Some(Datum::Text(name.to_string())),
            Some(Datum::Text(RELKIND_TABLE.to_string())),
            Some(Datum::Int4(schema.len() as i32)),
            Some(Datum::Int8(toast.map_or(0, |toast| toast.rel_oid.0) as i64)),
            Some(Datum::Int8(HEAP_AM_OID.0 as i64)),
        ];
        self.insert_catalog_row(snap, &PG_CLASS, &class_row)?;

        for (i, col) in schema.iter().enumerate() {
            let (type_oid, len) = type_oid_of(col.col_type)?;
            let attr_row = vec![
                Some(Datum::Int8(oid.0 as i64)),
                Some(Datum::Text(col.name.clone())),
                Some(Datum::Int8(type_oid.raw().0 as i64)),
                Some(Datum::Int4(len)),
                // attnum is 1-based.
                Some(Datum::Int4(i as i32 + 1)),
                // M2a ColumnDef has no nullability: nullable, not-null = 0.
                Some(Datum::Int4(0)),
                Some(Datum::Int4(1)),
            ];
            self.insert_catalog_row(snap, &PG_ATTRIBUTE, &attr_row)?;
        }

        // For ordinary heap tables `last_page` was advisory and unused. Stage D
        // freezes it as the hidden TOAST chain head when reltoastrelid != 0;
        // without TOAST it remains equal to first_page. The on-disk chain walk
        // stays authoritative for page membership.
        let relpages_row = vec![
            Some(Datum::Int8(oid.0 as i64)),
            Some(Datum::Int8(first_page.0 as i64)),
            Some(Datum::Int8(
                toast.map_or(first_page, |toast| toast.first_page).0 as i64,
            )),
            Some(Datum::Int8(1)),
        ];
        self.insert_catalog_row(snap, &PG_RELPAGES, &relpages_row)?;

        Ok((first_page, toast))
    }

    /// Drop a table: write `relkind = 'd'`, then perform physical cleanup
    /// only after the commit is durable.
    ///
    /// The marker is an MVCC **update through the heap AM**, not a physical
    /// row removal. Physical page freeing, heap/table/index registry removal
    /// and HNSW handle cleanup run in `auto_commit_with_hooks`'s
    /// `on_success` hook, before the AccessExclusive lock is released. A DML
    /// statement queued behind that lock therefore observes the completed
    /// drop, while a commit failure performs none of those destructive steps.
    /// The `pg_rust_relpages` row is left behind; it is keyed by the table's
    /// never-reused OID and ignored by the rebuild (leaked row, documented
    /// M2a simplification).
    ///
    /// Recovery rebuilds the table registry through a fresh MVCC scan, not a
    /// raw last-version-wins fold, so an aborted marker cannot hide the
    /// original table after restart.
    ///
    /// Fails with [`EngineError::TableNotFound`] if the table does not
    /// exist.
    pub fn drop_table(&self, name: &str) -> Result<()> {
        let _ddl = self.ddl_lock.lock();
        let entry = self
            .registry
            .read()
            .get(name)
            .cloned()
            .ok_or_else(|| EngineError::TableNotFound(name.to_string()))?;
        let sequence_states = RefCell::new(None);
        let serial_sequences = RefCell::new(Vec::new());

        self.auto_commit_with_hooks(
            |snap| {
                // Acquire the table lock BEFORE the sequence mutex, matching
                // SERIAL INSERT's RowExclusive -> nextval order. The mutex is
                // then held through commit and pre-unlock in-memory cleanup.
                self.lock_table(snap.current_xid(), &entry, LockMode::AccessExclusive)?;
                let states = self.sequences.states.lock();
                let sequences = entry
                    .defaults
                    .iter()
                    .enumerate()
                    .filter_map(|(index, sequence_oid)| {
                        sequence_oid.map(|sequence_oid| {
                            let state = states.get(&sequence_oid).ok_or_else(|| {
                                EngineError::Corrupted(format!(
                                    "SERIAL default references missing sequence {sequence_oid}"
                                ))
                            })?;
                            Ok((index as i32 + 1, sequence_oid, state.name.clone()))
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                self.drop_serial_sequences(snap, &entry, &sequences)?;
                *serial_sequences.borrow_mut() = sequences;
                *sequence_states.borrow_mut() = Some(states);
                self.drop_table_marker(snap, name, &entry)
            },
            || {
                self.drop_table_storage(name, &entry)?;
                let mut states = sequence_states.borrow_mut();
                let states = states
                    .as_mut()
                    .expect("successful DROP holds its sequence mutex");
                for (_, sequence_oid, _) in serial_sequences.borrow().iter() {
                    states.remove(sequence_oid);
                }
                Ok(())
            },
            || {},
        )
    }

    /// Delete SERIAL-owned metadata and mark each sequence dropped in the
    /// same transaction as the parent table. Standalone sequences are not
    /// listed in pg_rust_attrdefaults and are therefore unaffected.
    fn drop_serial_sequences(
        &self,
        snap: &Snapshot,
        entry: &TableEntry,
        serial_sequences: &[(i32, Oid, String)],
    ) -> Result<()> {
        if serial_sequences.is_empty() {
            return Ok(());
        }
        let default_def = self.attrdefaults_relation()?;
        let columns = default_def.column_types();
        let default_rows = self.heap.scan(ScanContext {
            rel: RelationDesc {
                rel_oid: default_def.oid.raw(),
                first_page: default_def.first_page,
                columns: &columns,
            },
            snapshot: snap,
            clog: self.clog.as_ref(),
        })?;
        let parent_rows = default_rows
            .iter()
            .filter(|(_, values)| values.first() == Some(&Some(Datum::Int8(entry.oid.0 as i64))))
            .collect::<Vec<_>>();
        if parent_rows.len() != serial_sequences.len() {
            return Err(EngineError::Corrupted(format!(
                "SERIAL default count mismatch for table OID {}",
                entry.oid.0
            )));
        }
        let sequence_def = self.sequence_relation()?;
        for (attnum, sequence_oid, sequence_name) in serial_sequences {
            let mut matching = parent_rows.iter().filter(|(_, values)| {
                values.get(1) == Some(&Some(Datum::Int4(*attnum)))
                    && values.get(2) == Some(&Some(Datum::Int8(sequence_oid.0 as i64)))
            });
            let (default_tid, _) = matching.next().ok_or_else(|| {
                EngineError::Corrupted(format!(
                    "missing SERIAL default for table OID {} attnum {attnum}",
                    entry.oid.0
                ))
            })?;
            if matching.next().is_some() {
                return Err(EngineError::Corrupted(format!(
                    "duplicate SERIAL default for table OID {} attnum {attnum}",
                    entry.oid.0
                )));
            }
            let (sequence_tid, _) = self.find_sequence_row(snap, &sequence_def, *sequence_oid)?;
            let class_tid = self.find_live_class_row(snap, *sequence_oid, RELKIND_SEQUENCE)?;
            let mut class_row = sequence_class_row(*sequence_oid, sequence_name);
            class_row[2] = Some(Datum::Text(RELKIND_DROPPED.to_string()));
            self.update_catalog_row(snap, &PG_CLASS, class_tid, &class_row)?;
            self.delete_catalog_row(snap, &sequence_def, sequence_tid)?;
            self.delete_catalog_row(snap, &default_def, *default_tid)?;
        }
        Ok(())
    }

    /// Write only the drop marker inside the transaction. Physical cleanup
    /// is deferred until the commit is durable, so a commit failure leaves
    /// the original table and every data page intact.
    fn drop_table_marker(&self, snap: &Snapshot, name: &str, entry: &TableEntry) -> Result<()> {
        let old_tid = self.find_live_pg_class_row(snap, entry.oid)?;
        let columns = PG_CLASS.column_types();
        let class_row = vec![
            Some(Datum::Int8(entry.oid.0 as i64)),
            Some(Datum::Text(name.to_string())),
            Some(Datum::Text(RELKIND_DROPPED.to_string())),
            Some(Datum::Int4(entry.columns.len() as i32)),
            Some(Datum::Int8(
                entry.toast.map_or(0, |toast| toast.rel_oid.0) as i64
            )),
            Some(Datum::Int8(HEAP_AM_OID.0 as i64)),
        ];
        let header = tuple_header(snap.curcid());
        let new_tuple = encode_tuple(header, &columns, &class_row)?;
        self.ensure_catalog_room(&PG_CLASS, new_tuple.len())?;
        self.heap.update(UpdateContext {
            rel: RelationDesc {
                rel_oid: PG_CLASS.oid.raw(),
                first_page: PG_CLASS.first_page,
                columns: &columns,
            },
            snapshot: snap,
            old_tid,
            new_tuple: &new_tuple,
            out_tid: None,
            clog: self.clog.as_ref(),
            hot_eligible: false,
        })?;
        Ok(())
    }

    /// Post-commit physical and in-memory cleanup, before the table lock is
    /// released. No part of this runs when the commit fails.
    fn drop_table_storage(&self, name: &str, entry: &TableEntry) -> Result<()> {
        // Remove every live entry point BEFORE freeing any page. If a later
        // page walk/free fails, the table is already unreachable and pages
        // merely leak; no DML can observe a live table with reclaimed pages.
        #[cfg(test)]
        Self::run_before_drop_cleanup_hook();
        let index_oids: HashSet<Oid> = self
            .indexes
            .read()
            .iter()
            .filter(|index| index.table_oid == entry.oid)
            .map(|index| index.index_oid)
            .collect();
        self.heap.drop_relation(entry.oid);
        self.heap.unregister_toast_relation(entry.oid);
        if let Some(toast) = entry.toast {
            self.heap.drop_relation(toast.rel_oid);
        }
        self.registry.write().remove(name);
        self.indexes
            .write()
            .retain(|index| index.table_oid != entry.oid);
        self.hnsw_handles
            .lock()
            .retain(|index_oid, _| !index_oids.contains(index_oid));
        self.inverted_handles
            .lock()
            .retain(|index_oid, _| !index_oids.contains(index_oid));
        self.pending_inverted
            .lock()
            .retain(|index_oid, _| !index_oids.contains(index_oid));
        self.inverted_lag
            .lock()
            .retain(|index_oid, _| !index_oids.contains(index_oid));
        let mut pages = match self.walk_chain(entry.first_page) {
            Ok(pages) => pages,
            Err(error) => {
                tracing::warn!(%error, table = %name, "post-commit DROP page walk failed; leaving pages leaked");
                Vec::new()
            }
        };
        if let Some(toast) = entry.toast {
            match self.walk_chain(toast.first_page) {
                Ok(toast_pages) => pages.extend(toast_pages),
                Err(error) => {
                    tracing::warn!(%error, table = %name, "post-commit DROP TOAST page walk failed; leaving pages leaked");
                }
            }
        }
        for page_id in pages {
            if let Err(error) = self.storage.page_allocator().lock().free_page(page_id) {
                tracing::warn!(%error, table = %name, ?page_id, "post-commit DROP page free failed; leaving page leaked");
            }
        }
        // The marker is already durable and every live entry is gone. These
        // cleanup failures are warnings, not a DROP failure: the table is
        // dropped, and any unfreed pages are leaks by design.
        Ok(())
    }

    /// Create a B+Tree index on `table(column)` (M2b Stage M wave 2,
    /// blocking build) and return the index relation's OID.
    ///
    /// Steps: scan the table through the heap AM (visible rows only),
    /// extract and encode the key column (SQL NULLs are **not indexed** —
    /// an M2b simplification, matching "single-column non-null keys" scope),
    /// bottom-up bulk-load the tree (one post-image FPI per page, see
    /// `pg_am_btree::BTreeAM::build_index`), and only then write the catalog
    /// rows in one auto-commit transaction:
    ///
    /// - `pg_class`: `(oid, "{table}_{column}_idx", relkind='i', natts=1,
    ///   toast=0, relam=BTREE_AM_OID)`;
    /// - `pg_attribute`: one row for the index (attnum=1, attname = the
    ///   indexed column's name, atttypid = its type — PostgreSQL's shape);
    /// - `pg_index`: `(indexrelid=oid, indrelid=table, indnatts=1,
    ///   indisunique=0, indisprimary=0)`;
    /// - `pg_rust_relpages`: `(oid, meta_page, meta_page, 1)` — the index's
    ///   B+Tree meta page location.
    ///
    /// Crash atomicity (same "leak, never corruption" policy as the other
    /// DDL): a crash before the catalog commit leaves only the bulk-loaded
    /// pages — unreachable, with no catalog rows pointing at them; a crash
    /// after it leaves a fully built index (every page is FPI-covered and
    /// the meta record is written last by the loader).
    ///
    /// Fails with [`EngineError::IndexExists`] if (table, column) already
    /// has an index (M2b: one index per column, no `DROP INDEX`).
    pub fn create_index(&self, table: &str, column: &str) -> Result<Oid> {
        let _ddl = self.ddl_lock.lock();
        let entry = self.table_entry(table)?;
        let col_index = entry
            .columns
            .iter()
            .position(|c| c.name == column)
            .ok_or_else(|| {
                EngineError::InvalidArgument(format!("table {table:?} has no column {column:?}"))
            })?;
        let key_type = entry.columns[col_index].col_type;
        if !is_supported_key_type(key_type) {
            return Err(EngineError::InvalidArgument(format!(
                "column {column:?} of type {key_type:?} is not an indexable M2b key type"
            )));
        }
        if self
            .indexes
            .read()
            .iter()
            .any(|e| e.table_oid == entry.oid && e.column == column)
        {
            return Err(EngineError::IndexExists(format!("{table}({column})")));
        }
        let index_name = format!("{}_{column}_idx", entry.oid.0);
        if self.relation_name_exists(&index_name)? {
            return Err(EngineError::IndexExists(index_name));
        }

        // Catalog-room pre-check BEFORE the expensive scan + bulk load
        // (~1s and ~20MB of WAL for a large table): the four catalog rows
        // written at the end must fit their system pages, so fail cheap and
        // early if they cannot. The rows are encoded exactly as
        // `create_index_catalog_rows` will encode them.
        let index_oid = self.catalog.oid_allocator().alloc();
        self.ensure_index_catalog_room(&entry, column, index_oid, BTREE_AM_OID)?;

        // The scan + bulk load + catalog write run as ONE auto-commit
        // transaction holding the table's Exclusive lock (M2c Stage P,
        // §9.2): Exclusive conflicts with RowExclusive, so concurrent
        // writers block for the duration of the build instead of racing
        // it, and the build's heap scan reads one consistent SI snapshot.
        // Catalog rows stay LAST inside the transaction (see the fn docs
        // for the crash-atomicity order); a failed build aborts the
        // transaction, leaving only unreachable pages — the documented
        // "leak, never corruption" policy.
        let col_types = column_types(&entry);
        let result = self.auto_commit_with_hooks(
            |snap| {
                let entry =
                    self.lock_table_entry(snap.current_xid(), table, LockMode::Exclusive)?;
                // Re-take the snapshot AFTER the lock wait (M2c Stage P
                // review): `auto_commit`'s snapshot was taken before we queued
                // on Exclusive, so a writer we blocked behind would be stuck in
                // its `xip` and its rows would silently never enter the index.
                // The lock guarantees no writer can be in flight now, so a
                // fresh snapshot sees exactly the committed contents the index
                // must cover. `_re_guard` keeps the re-snapshot registered
                // until this auto-commit closure ends (M3 Stage A, §3.3).
                let (mut snap, _re_guard) = self.txn.snapshot(snap.current_xid());
                snap.advance_curcid();

                // Collect (key_bytes, tid) from a full heap scan (M2b: simple
                // scan, no ordering assumption on the source).
                let rows = self.heap.scan(ScanContext {
                    rel: relation_desc(&entry, &col_types),
                    snapshot: &snap,
                    clog: self.clog.as_ref(),
                })?;
                let mut entries = Vec::with_capacity(rows.len());
                for (tid, values) in rows {
                    // NULL keys are not indexed (see the fn docs).
                    if let Some(datum) = &values[col_index] {
                        // Post-Stage-S review H2: the scan yields the VISIBLE
                        // version, which for a HOT chain is a `HEAP_ONLY_TUPLE`
                        // tail that never gets index entries of its own. Build
                        // the entry against the chain ROOT instead, so a later
                        // key-changing update or delete — which retires entries
                        // via `hot_chain_root` — finds it (`EntryNotFound`
                        // otherwise, and the index would leak the deleted row
                        // forever). Safe under the build's Exclusive table lock:
                        // no writer can be mutating the chains mid-scan.
                        let index_tid = self.hot_chain_root(tid)?;
                        entries.push((encode_key(datum)?, index_tid));
                    }
                }

                let btree = BTreeAM::new(
                    Arc::clone(self.storage.buffer_pool()),
                    Arc::clone(self.storage.wal_writer()),
                );
                let index = btree.build_index(index_oid, key_type, entries)?;
                let meta_page = index.meta_page();

                self.create_index_catalog_rows(
                    &snap,
                    &entry,
                    column,
                    index_oid,
                    meta_page,
                    BTREE_AM_OID,
                )?;
                // Registry push INSIDE the lock-holding transaction (round-4
                // P3-C): pushing after the lock released left a residual window
                // where a concurrent INSERT saw no index and wrote rows without
                // index maintenance. The registry is in-memory (crash-rebuilt
                // from the catalog), so the in-transaction push is safe.
                self.indexes.write().push(IndexEntry {
                    kind: AmKind::BTree,
                    inverted: None,
                    index_oid,
                    table_oid: entry.oid,
                    column: column.to_string(),
                    key_type,
                    meta_page,
                    map_page: None,
                });
                Ok(())
            },
            || Ok(()),
            || {
                // Registry publication is in-memory and OID-scoped: a failed
                // commit must not leave a usable ghost index behind.
                self.indexes
                    .write()
                    .retain(|index| index.index_oid != index_oid);
            },
        );
        result?;
        Ok(index_oid)
    }

    /// Create an inverted full-text index with a full backfill under
    /// `AccessExclusive`. Catalog rows and the in-memory registry are written
    /// inside the same transaction; failure cleans them before lock release.
    fn create_inverted_index_on_named(
        &self,
        table: &str,
        column: &str,
        index_name: Option<&str>,
    ) -> Result<Oid> {
        let _ddl = self.ddl_lock.lock();
        let entry = self.table_entry(table)?;
        let col_index = entry
            .columns
            .iter()
            .position(|c| c.name == column)
            .ok_or_else(|| {
                EngineError::InvalidArgument(format!("table {table:?} has no column {column:?}"))
            })?;
        if entry.columns[col_index].col_type != ColumnType::Text {
            return Err(EngineError::InvalidArgument(format!(
                "inverted index requires a TEXT column; {table}({column}) is {:?}",
                entry.columns[col_index].col_type
            )));
        }
        if self
            .indexes
            .read()
            .iter()
            .any(|index| index.table_oid == entry.oid && index.column == column)
        {
            return Err(EngineError::IndexExists(format!("{table}({column})")));
        }
        let index_name = index_name
            .map(str::to_owned)
            .unwrap_or_else(|| format!("{}_{column}_inverted_idx", entry.oid.0));
        if self.relation_name_exists(&index_name)? {
            return Err(EngineError::IndexExists(index_name));
        }
        let index_oid = self.catalog.oid_allocator().alloc();
        self.ensure_index_catalog_room(&entry, column, index_oid, INVERTED_AM_OID)?;
        let pending_store = Mutex::new(None);
        let result = self.auto_commit_with_hooks(
            |snap| {
                let _locked =
                    self.lock_table_entry(snap.current_xid(), table, LockMode::AccessExclusive)?;
                let (mut snap, _re_guard) = self.txn.snapshot(snap.current_xid());
                snap.advance_curcid();
                let col_types = column_types(&entry);
                let rows = self.heap.scan(ScanContext {
                    rel: relation_desc(&entry, &col_types),
                    snapshot: &snap,
                    clog: self.clog.as_ref(),
                })?;
                let mut documents = Vec::with_capacity(rows.len());
                for (tid, values) in rows {
                    let values = self.heap.detoast_values(
                        &relation_desc(&entry, &col_types),
                        &snap,
                        self.clog.as_ref(),
                        &values,
                    )?;
                    let Some(Datum::Text(text)) = &values[col_index] else {
                        continue;
                    };
                    let index_tid = self.hot_chain_root(tid)?;
                    documents.push((index_tid, text.clone()));
                }
                let mut store = pg_am_inverted::SegmentStore::create(
                    self.storage.data_dir(),
                    index_oid,
                    self.inverted_analyzer.clone(),
                    documents,
                    self.storage.wal_writer().synced_lsn(),
                )
                .map_err(|error| EngineError::Corrupted(error.to_string()))?;
                store.set_flush_threshold(self.inverted_seal_batch_size);
                store
                    .seal(
                        self.storage.data_dir(),
                        self.storage.wal_writer(),
                        snap.current_xid(),
                    )
                    .map_err(|error| EngineError::Corrupted(error.to_string()))?;
                self.create_index_catalog_rows(
                    &snap,
                    &entry,
                    column,
                    index_oid,
                    PageId::INVALID,
                    INVERTED_AM_OID,
                )?;
                *pending_store.lock() = Some(store);
                Ok(())
            },
            || {
                // Publish only after the catalog commit is durable, while the
                // AccessExclusive lock still excludes every concurrent writer.
                let mut store = pending_store.lock().take().ok_or_else(|| {
                    EngineError::Corrupted("missing inverted backfill store".to_string())
                })?;
                // No table DML can belong to (base_lsn, current frontier]:
                // AccessExclusive is still held. Advancing past our own
                // catalog commit cannot skip a document change.
                store.advance_consumed(self.storage.wal_writer().synced_lsn());
                let metadata = InvertedIndexMetadata::from_store(&store);
                self.inverted_handles.lock().insert(
                    index_oid,
                    Arc::new(Mutex::new(pg_am_inverted::InvertedIndex::from_store(store))),
                );
                self.indexes.write().push(IndexEntry {
                    kind: AmKind::Inverted,
                    inverted: Some(metadata),
                    index_oid,
                    table_oid: entry.oid,
                    column: column.to_string(),
                    key_type: ColumnType::Text,
                    meta_page: PageId::INVALID,
                    map_page: None,
                });
                Ok(())
            },
            || {
                pending_store.lock().take();
                self.inverted_handles.lock().remove(&index_oid);
                self.indexes
                    .write()
                    .retain(|index| index.index_oid != index_oid);
            },
        );
        result?;
        Ok(index_oid)
    }

    /// Create an HNSW index ON a table column (M6 Stage C, tech-selection
    /// §7/§11): the table-linked form of [`Self::create_hnsw_index`].
    /// Validates the column is `VECTOR(dim)` with the same dim, creates the
    /// graph and the sidecar NodeId→ctid map, registers the catalog rows
    /// (pg_class/pg_attribute/pg_index/relpages with `last_page` = map),
    /// and pushes the registry entry so DML hooks see the index.
    #[allow(clippy::too_many_arguments)]
    pub fn create_hnsw_index_on(
        &self,
        table: &str,
        column: &str,
        params: pg_am_hnsw::HnswParams,
        dim: u16,
        metric: pg_am_hnsw::Metric,
        selection: pg_am_hnsw::NeighborSelection,
        rng_seed: u64,
    ) -> Result<(Oid, PageId)> {
        self.create_hnsw_index_on_named(
            table, column, None, params, dim, metric, selection, rng_seed,
        )
    }

    /// Phase 4a Stage B named SQL form. The public M6 wrapper above keeps its
    /// signature and automatic name; SQL supplies `index_name` here.
    #[allow(clippy::too_many_arguments)]
    fn create_hnsw_index_on_named(
        &self,
        table: &str,
        column: &str,
        index_name: Option<&str>,
        params: pg_am_hnsw::HnswParams,
        dim: u16,
        metric: pg_am_hnsw::Metric,
        selection: pg_am_hnsw::NeighborSelection,
        rng_seed: u64,
    ) -> Result<(Oid, PageId)> {
        let _ddl = self.ddl_lock.lock();
        let entry = self.table_entry(table)?;
        let col_index = entry
            .columns
            .iter()
            .position(|c| c.name == column)
            .ok_or_else(|| {
                EngineError::InvalidArgument(format!("table {table:?} has no column {column:?}"))
            })?;
        let col_type = entry.columns[col_index].col_type;
        let ColumnType::Vector(col_dim) = col_type else {
            return Err(EngineError::InvalidArgument(format!(
                "hnsw index requires a VECTOR column; {table}({column}) is {col_type:?}"
            )));
        };
        if col_dim != u32::from(dim) {
            return Err(EngineError::InvalidArgument(format!(
                "hnsw index dim {dim} does not match column {table}({column}) VECTOR({col_dim})"
            )));
        }
        if self
            .indexes
            .read()
            .iter()
            .any(|e| e.table_oid == entry.oid && e.column == column)
        {
            return Err(EngineError::IndexExists(format!("{table}({column})")));
        }
        // M6 Stage C review P2-2: at most ONE HNSW index per table — the
        // row carries a single trailing NodeId (§7 (b)), so a second
        // vector index would never be maintained by the DML hooks (they
        // take the first match). Refuse loudly.
        if self
            .indexes
            .read()
            .iter()
            .any(|e| e.table_oid == entry.oid && matches!(e.key_type, ColumnType::Vector(_)))
        {
            return Err(EngineError::InvalidArgument(format!(
                "table {table:?} already has an HNSW index (one per table: the row carries a single NodeId)"
            )));
        }
        let index_name = index_name
            .map(str::to_owned)
            .unwrap_or_else(|| format!("{}_{column}_hnsw_idx", entry.oid.0));
        if self.relation_name_exists(&index_name)? {
            return Err(EngineError::IndexExists(index_name));
        }
        // M6 Stage C review P2-1 (+ round-3 P2-1): index creation does NOT
        // backfill existing rows — non-empty tables are refused loudly. The
        // WHOLE creation (emptiness check + graph + map + catalog rows) runs
        // in ONE auto-commit holding the table's Exclusive lock (btree
        // create_index's exact precedent): without this, a concurrent insert
        // in the window between the check and the registry push would
        // encode rows with no NodeId, permanently invisible to ANN search.
        // The registry is published inside that lock-holding transaction.
        let index_oid = self.catalog.oid_allocator().alloc();
        self.ensure_hnsw_index_catalog_room(&entry, column, &index_name, index_oid, dim)?;
        let result = self.auto_commit_with_hooks(|snap| {
            let _locked = self.lock_table_entry(snap.current_xid(), table, LockMode::Exclusive)?;
            // Re-take the snapshot AFTER the lock wait (the auto-commit
            // snapshot predates it — btree create_index's precedent).
            let (mut snap, _re_guard) = self.txn.snapshot(snap.current_xid());
            snap.advance_curcid();
            if !self.scan_inner(&snap, table, None)?.is_empty() {
                return Err(EngineError::InvalidArgument(format!(
                    "create_hnsw_index_on: table {table:?} is not empty (M6 creates indexes on empty tables only — no backfill)"
                )));
            }
            let index = pg_am_hnsw::index::create(
                self.storage.buffer_pool(),
                self.storage.wal_writer(),
                params,
                dim,
                metric,
                selection,
                rng_seed,
            )?;
            let meta_page = index.meta_page_id();
            let map = pg_am_btree::BTreeIndex::create(
                std::sync::Arc::clone(self.storage.buffer_pool()),
                std::sync::Arc::clone(self.storage.wal_writer()),
                self.catalog.oid_allocator().alloc(),
                ColumnType::Int8,
            )?;
            let map_page = map.meta_page();
            self.create_hnsw_index_catalog_rows(
                &snap,
                &entry,
                column,
                &index_name,
                index_oid,
                meta_page,
                map_page,
                dim,
            )?;
            // Registry push + handle cache INSIDE the lock-holding
            // transaction (round-4 P3-C): pushing outside left a residual
            // window where a concurrent INSERT wrote rows with no NodeId,
            // permanently invisible to ANN search. `meta_page` is captured
            // before the handle moves into the cache.
            let meta_page = index.meta_page_id();
            self.hnsw_handles.lock().insert(index_oid, Arc::new(index));
            self.indexes.write().push(IndexEntry {
                kind: AmKind::Hnsw,
            inverted: None,
                index_oid,
                table_oid: entry.oid,
                column: column.to_string(),
                key_type: ColumnType::Vector(u32::from(dim)),
                meta_page,
                map_page: Some(map_page),
            });
            Ok(meta_page)
        }, || Ok(()), || {
            self.hnsw_handles.lock().remove(&index_oid);
            self.indexes
                .write()
                .retain(|index| index.index_oid != index_oid);
        });
        let meta_page = result?;
        Ok((index_oid, meta_page))
    }

    /// Whether a committed live table, index, or sequence already owns `name`.
    /// `Catalog`'s open-time snapshot cannot see relations created later in
    /// this process, so name validation reads `pg_class` through a fresh
    /// MVCC snapshot. Drop markers (`relkind='d'`) and relations whose parent
    /// table is dropped do not block reuse.
    fn relation_name_exists(&self, name: &str) -> Result<bool> {
        // Bootstrap catalog rows use a synthetic XID that an ordinary MVCC
        // heap scan cannot see. Only system relations use the open-time
        // snapshot: its user rows may be stale after DROP, while system
        // relations are immutable for this stage.
        if self.catalog.relation_by_name(name).is_some_and(|row| {
            row.oid.raw().is_system()
                && (row.kind == RELKIND_TABLE
                    || row.kind == RELKIND_INDEX
                    || row.kind == RELKIND_SEQUENCE
                    || row.kind == RELKIND_INTERNAL)
        }) {
            return Ok(true);
        }
        let (snapshot, _guard) = self.txn.snapshot(TxnId::INVALID);
        let scan = |def: &SystemTableDef| -> Result<Vec<(Tid, Vec<Value>)>> {
            let columns = def.column_types();
            Ok(self.heap.scan(ScanContext {
                rel: RelationDesc {
                    rel_oid: def.oid.raw(),
                    first_page: def.first_page,
                    columns: &columns,
                },
                snapshot: &snapshot,
                clog: self.clog.as_ref(),
            })?)
        };
        let class_rows = scan(&PG_CLASS)?;
        let index_rows = scan(&PG_INDEX)?;
        let default_rows = scan(&self.attrdefaults_relation()?)?;
        let index_parents: HashMap<i64, i64> = index_rows
            .into_iter()
            .filter_map(|(_, values)| match (values.first(), values.get(1)) {
                (Some(Some(Datum::Int8(index_oid))), Some(Some(Datum::Int8(parent_oid)))) => {
                    Some((*index_oid, *parent_oid))
                }
                _ => None,
            })
            .collect();
        let sequence_parents: HashMap<i64, i64> = default_rows
            .into_iter()
            .filter_map(|(_, values)| match (values.first(), values.get(2)) {
                (Some(Some(Datum::Int8(rel_oid))), Some(Some(Datum::Int8(sequence_oid)))) => {
                    Some((*sequence_oid, *rel_oid))
                }
                _ => None,
            })
            .collect();
        let live_tables: HashSet<i64> = class_rows
            .iter()
            .filter_map(|(_, values)| match (values.first(), values.get(2)) {
                (Some(Some(Datum::Int8(oid))), Some(Some(Datum::Text(kind))))
                    if kind == RELKIND_TABLE =>
                {
                    Some(*oid)
                }
                _ => None,
            })
            .collect();
        Ok(class_rows.into_iter().any(|(_, values)| {
            let (Some(Some(Datum::Text(existing))), Some(Some(Datum::Text(kind)))) =
                (values.get(1), values.get(2))
            else {
                return false;
            };
            if existing != name {
                return false;
            }
            if kind == RELKIND_TABLE || kind == RELKIND_INTERNAL {
                return true;
            }
            if kind == RELKIND_SEQUENCE {
                // A SERIAL sequence has the same lifetime as its parent
                // table. Pre-fix orphaned rows must not block name reuse.
                let Some(Some(Datum::Int8(sequence_oid))) = values.first() else {
                    return false;
                };
                return sequence_parents
                    .get(sequence_oid)
                    .is_none_or(|parent_oid| live_tables.contains(parent_oid));
            }
            if kind != RELKIND_INDEX {
                return false;
            }
            // An index relation is live only while its parent table is live.
            // DROP TABLE deliberately leaves index catalog pages/rows behind,
            // but those orphans must not block a name's reuse.
            let Some(Some(Datum::Int8(index_oid))) = values.first() else {
                return false;
            };
            index_parents
                .get(index_oid)
                .is_some_and(|parent_oid| live_tables.contains(parent_oid))
        }))
    }

    /// Write the four catalog rows of a table-linked HNSW index inside the
    /// caller's auto-commit. Their shape is shared with the build-time room
    /// precheck by [`Self::hnsw_index_catalog_rows`].
    #[allow(clippy::too_many_arguments)]
    fn create_hnsw_index_catalog_rows(
        &self,
        snap: &Snapshot,
        entry: &TableEntry,
        column: &str,
        index_name: &str,
        index_oid: Oid,
        meta_page: PageId,
        map_page: PageId,
        dim: u16,
    ) -> Result<()> {
        for (def, row) in Self::hnsw_index_catalog_rows(
            entry, column, index_name, index_oid, meta_page, map_page, dim,
        ) {
            self.insert_catalog_row(snap, def, &row)?;
        }
        Ok(())
    }

    /// Fail before allocating any graph/map pages when the catalog rows
    /// cannot fit. Page ids are zero placeholders: they have the same
    /// fixed-width encoding as the real meta/map pages.
    fn ensure_hnsw_index_catalog_room(
        &self,
        entry: &TableEntry,
        column: &str,
        index_name: &str,
        index_oid: Oid,
        dim: u16,
    ) -> Result<()> {
        let rows = Self::hnsw_index_catalog_rows(
            entry,
            column,
            index_name,
            index_oid,
            PageId(0),
            PageId(0),
            dim,
        );
        for (def, row) in &rows {
            let columns = def.column_types();
            let tuple = encode_tuple(tuple_header(0), &columns, row)?;
            self.ensure_catalog_room(def, tuple.len())?;
        }
        Ok(())
    }

    /// The single source of HNSW catalog-row shape for writes and prechecks.
    fn hnsw_index_catalog_rows(
        entry: &TableEntry,
        column: &str,
        index_name: &str,
        index_oid: Oid,
        meta_page: PageId,
        map_page: PageId,
        dim: u16,
    ) -> [(&'static SystemTableDef, Vec<Value>); 4] {
        [
            (
                &PG_CLASS,
                vec![
                    Some(Datum::Int8(index_oid.0 as i64)),
                    Some(Datum::Text(index_name.to_string())),
                    Some(Datum::Text(RELKIND_INDEX.to_string())),
                    Some(Datum::Int4(1)),
                    Some(Datum::Int8(0)),
                    Some(Datum::Int8(pg_catalog::system_tables::HNSW_AM_OID.0 as i64)),
                ],
            ),
            (
                &PG_ATTRIBUTE,
                vec![
                    Some(Datum::Int8(index_oid.0 as i64)),
                    Some(Datum::Text(column.to_string())),
                    Some(Datum::Int8(VECTOR_OID.raw().0 as i64)),
                    Some(Datum::Int4(dim as i32 * 4)),
                    Some(Datum::Int4(1)),
                    Some(Datum::Int4(0)),
                    Some(Datum::Int4(1)),
                ],
            ),
            (
                &PG_INDEX,
                vec![
                    Some(Datum::Int8(index_oid.0 as i64)),
                    Some(Datum::Int8(entry.oid.0 as i64)),
                    Some(Datum::Int4(1)),
                    Some(Datum::Int4(0)),
                    Some(Datum::Int4(0)),
                ],
            ),
            (
                &PG_RELPAGES,
                vec![
                    Some(Datum::Int8(index_oid.0 as i64)),
                    Some(Datum::Int8(meta_page.0 as i64)),
                    Some(Datum::Int8(map_page.0 as i64)),
                    Some(Datum::Int8(1)),
                ],
            ),
        ]
    }

    /// Create an HNSW vector index and register its meta page in
    /// `pg_rust_relpages` (Phase 2 M5 Stage B slice 3b, tech-selection
    /// §10.3 step 3 — the engine-side MINIMAL registration).
    ///
    /// Sequence (creation is a utility operation, §8.1 — no transactional
    /// DML): `pg_am_hnsw`'s create protocol allocates the directory-chain
    /// head and the meta page (each made durable by its own post-image FPI,
    /// the A1 contract), then ONE `pg_rust_relpages` row `(oid, meta_page,
    /// meta_page, 1)` is written through the heap AM inside an auto-commit
    /// — the exact same write path as the B+Tree index registration
    /// (`create_index_catalog_rows`), WAL-logged and redo-recoverable.
    ///
    /// **Minimal-registration scope** (§10.3's range reconciliation): only
    /// the `pg_rust_relpages` row. No `pg_class` / `pg_attribute` /
    /// `pg_index` rows — M5 has no SQL surface, and the lone row is
    /// harmless: the registry rebuild iterates `pg_class` / `pg_index`
    /// rows and joins OUT to `pg_rust_relpages`, so it never encounters a
    /// relpages row that has no catalog companion (2026-09-16 review nano
    /// — result equivalent to "skipped", mechanism stated precisely). The
    /// SQL/DDL user surface is Phase 4's deliverable.
    ///
    /// **Crash windows** (§10.3): a crash between the pg-am-hnsw create and
    /// the catalog row leaves orphan directory/meta pages (accounted, M6
    /// cleanup; creation is a utility operation — fail and recreate). A
    /// crash after the row is durable has the full registration: redo
    /// replays the heap-AM catalog write and `hnsw_index_first_page`
    /// locates the index again.
    ///
    /// Returns `(index_oid, meta_page_id)`.
    pub fn create_hnsw_index(
        &self,
        params: pg_am_hnsw::HnswParams,
        dim: u16,
        metric: pg_am_hnsw::Metric,
        selection: pg_am_hnsw::NeighborSelection,
        rng_seed: u64,
    ) -> Result<(Oid, PageId)> {
        let _ddl = self.ddl_lock.lock();
        let index = pg_am_hnsw::index::create(
            self.storage.buffer_pool(),
            self.storage.wal_writer(),
            params,
            dim,
            metric,
            selection,
            rng_seed,
        )?;
        let meta_page_id = index.meta_page_id();
        let index_oid = self.catalog.oid_allocator().alloc();
        // M6 Stage C: the sidecar NodeId→ctid map B+Tree is created with the
        // index (tech-selection §7 (c)). Its meta page is recorded in the
        // relpages row's `last_page` (the row's `first_page` stays the HNSW
        // meta page; `last_page == first_page` means "no map" — the M5
        // registration shape).
        let map = pg_am_btree::BTreeIndex::create(
            std::sync::Arc::clone(self.storage.buffer_pool()),
            std::sync::Arc::clone(self.storage.wal_writer()),
            self.catalog.oid_allocator().alloc(),
            pg_am_heap::tuple::ColumnType::Int8,
        )?;
        let map_page = map.meta_page();
        self.auto_commit(|snap| {
            let relpages_row = vec![
                Some(Datum::Int8(index_oid.0 as i64)),
                Some(Datum::Int8(meta_page_id.0 as i64)),
                Some(Datum::Int8(map_page.0 as i64)),
                Some(Datum::Int8(1)),
            ];
            self.insert_catalog_row(snap, &PG_RELPAGES, &relpages_row)
        })?;
        Ok((index_oid, meta_page_id))
    }
}

/// Which relpages field an HNSW lookup reads (M6 Stage C: `last_page`
/// carries the sidecar map B+Tree's meta page; `last == first` means
/// "no map", the M5 registration shape).
enum RelpagesField {
    FirstPage,
    MapPage,
}

impl Engine {
    /// `pg_rust_relpages.first_page` of the minimal registration). Returns
    /// `Ok(None)` when no such row exists. The lookup SCANS `pg_rust_relpages`
    /// through the heap AM (same read path the registry rebuild uses at
    /// :910) — the in-memory `Catalog` is loaded once at open and does not
    /// observe rows written since (2026-09-16: the first cut of this
    /// function used the cache and returned `None` right after
    /// `create_hnsw_index`; the heap scan is always fresh).
    pub fn hnsw_index_first_page(&self, oid: Oid) -> Result<Option<PageId>> {
        self.hnsw_relpages_field(oid, RelpagesField::FirstPage)
    }

    /// The sidecar map B+Tree's meta page for an HNSW index (M6 Stage C):
    /// the relpages row's `last_page`. Returns `Ok(None)` when no row
    /// exists OR the row is an M5-shape registration (last == first = the
    /// HNSW meta page = "no sidecar map").
    pub fn hnsw_index_map_page(&self, oid: Oid) -> Result<Option<PageId>> {
        self.hnsw_relpages_field(oid, RelpagesField::MapPage)
    }

    fn hnsw_relpages_field(&self, oid: Oid, field: RelpagesField) -> Result<Option<PageId>> {
        let columns = PG_RELPAGES.column_types();
        let rows = self.heap.scan(ScanContext {
            rel: RelationDesc {
                rel_oid: PG_RELPAGES.oid.raw(),
                first_page: PG_RELPAGES.first_page,
                columns: &columns,
            },
            snapshot: &Snapshot::everything(),
            clog: self.clog.as_ref(),
        })?;
        for (_tid, values) in rows {
            let Some(Datum::Int8(rel_oid)) = &values[0] else {
                continue;
            };
            if *rel_oid as u64 == oid.0 {
                let Some(Datum::Int8(first_page)) = &values[1] else {
                    return Err(EngineError::Corrupted(
                        "pg_rust_relpages row with non-int8 first_page".to_string(),
                    ));
                };
                let Some(Datum::Int8(last_page)) = &values[2] else {
                    return Err(EngineError::Corrupted(
                        "pg_rust_relpages row with non-int8 last_page".to_string(),
                    ));
                };
                return Ok(match field {
                    RelpagesField::FirstPage => Some(PageId(*first_page as u64)),
                    RelpagesField::MapPage => {
                        if *last_page == *first_page {
                            None
                        } else {
                            Some(PageId(*last_page as u64))
                        }
                    }
                });
            }
        }
        Ok(None)
    }

    /// Put a NodeId → ctid entry into the index's sidecar map (M6 Stage C
    /// §7). The B+Tree's IndexUndo pair carries transactional semantics
    /// (insert-abort removes the entry); the map lives and dies with the
    /// index's relpages registration.
    pub fn hnsw_map_put(&self, index_oid: Oid, node_id: u32, ctid: Tid) -> Result<()> {
        let mut map = self.open_hnsw_map(index_oid)?.ok_or_else(|| {
            EngineError::InvalidArgument(format!("hnsw index {index_oid:?} has no sidecar map"))
        })?;
        let key = pg_am_btree::encode_key(&Datum::Int8(i64::from(node_id)))?;
        Ok(map.insert(&key, ctid)?)
    }

    /// Point-lookup a NodeId in the sidecar map.
    pub fn hnsw_map_get(&self, index_oid: Oid, node_id: u32) -> Result<Option<Tid>> {
        let Some(map) = self.open_hnsw_map(index_oid)? else {
            return Ok(None);
        };
        let key = pg_am_btree::encode_key(&Datum::Int8(i64::from(node_id)))?;
        Ok(map.lookup(&key)?)
    }

    /// Remove a NodeId → ctid entry (post-commit delete cleanup).
    pub fn hnsw_map_remove(&self, index_oid: Oid, node_id: u32, ctid: Tid) -> Result<()> {
        let mut map = self.open_hnsw_map(index_oid)?.ok_or_else(|| {
            EngineError::InvalidArgument(format!("hnsw index {index_oid:?} has no sidecar map"))
        })?;
        let key = pg_am_btree::encode_key(&Datum::Int8(i64::from(node_id)))?;
        Ok(map.delete(&key, ctid)?)
    }

    /// Put a map entry into an HNSW index's sidecar map AND record the
    /// insert-undo pair (M6 Stage C). The map B+Tree shares the index
    /// family's rel_oid (WAL-record identity only).
    fn hnsw_map_put_txn(&self, xid: TxnId, idx: &IndexEntry, node_id: u32, tid: Tid) -> Result<()> {
        let map_page = idx.map_page.expect("hnsw index carries map_page");
        let mut map = pg_am_btree::BTreeIndex::open(
            Arc::clone(self.storage.buffer_pool()),
            Arc::clone(self.storage.wal_writer()),
            idx.index_oid,
            map_page,
            ColumnType::Int8,
        )?;
        let key = encode_key(&Datum::Int8(i64::from(node_id)))?;
        map.insert(&key, tid)?;
        let map_entry = IndexEntry {
            kind: AmKind::Hnsw,
            inverted: None,
            index_oid: idx.index_oid,
            table_oid: idx.table_oid,
            column: idx.column.clone(),
            key_type: ColumnType::Int8,
            meta_page: map_page,
            map_page: None,
        };
        self.record_index_undo(xid, &map_entry, key, tid, IndexUndoOp::Inserted);
        Ok(())
    }

    fn open_hnsw_map(&self, index_oid: Oid) -> Result<Option<pg_am_btree::BTreeIndex>> {
        let Some(meta_page) = self.hnsw_index_map_page(index_oid)? else {
            return Ok(None);
        };
        Ok(Some(pg_am_btree::BTreeIndex::open(
            std::sync::Arc::clone(self.storage.buffer_pool()),
            std::sync::Arc::clone(self.storage.wal_writer()),
            index_oid,
            meta_page,
            pg_am_heap::tuple::ColumnType::Int8,
        )?))
    }

    /// The shared HNSW handle for an index OID (M6 Stage C): cache-hit
    /// returns it; a miss opens the index from its meta page with
    /// self-evidencing parameters ([`pg_am_hnsw::index::expected_params_of`])
    /// and caches the handle. Registry read locks cover open + publish, so a
    /// concurrent DROP cannot clear the registry and then see a stale insert.
    #[cfg(test)]
    fn run_before_hnsw_publish_hook() {
        BEFORE_HNSW_PUBLISH_HOOK.with(|hook| {
            if let Some(hook) = hook.get() {
                hook();
            }
        });
    }

    #[cfg(test)]
    fn run_before_drop_cleanup_hook() {
        BEFORE_DROP_CLEANUP_HOOK.with(|hook| {
            if let Some(hook) = hook.get() {
                hook();
            }
        });
    }

    pub(crate) fn hnsw_handle(&self, idx: &IndexEntry) -> Result<Arc<pg_am_hnsw::HnswIndex>> {
        let table_registry = self.registry.read();
        let indexes = self.indexes.read();
        let live = || {
            table_registry
                .values()
                .any(|entry| entry.oid == idx.table_oid)
                && indexes.iter().any(|entry| entry.index_oid == idx.index_oid)
        };
        if !live() {
            return Err(EngineError::IndexNotFound(format!(
                "index {} is no longer live",
                idx.index_oid.0
            )));
        }
        if let Some(h) = self.hnsw_handles.lock().get(&idx.index_oid) {
            return Ok(Arc::clone(h));
        }
        let expected =
            pg_am_hnsw::index::expected_params_of(self.storage.buffer_pool(), idx.meta_page)?;
        let outcome = pg_am_hnsw::index::open(
            self.storage.buffer_pool(),
            self.storage.wal_writer(),
            idx.meta_page,
            &expected,
        )?;
        #[cfg(test)]
        Self::run_before_hnsw_publish_hook();
        if !live() {
            return Err(EngineError::IndexNotFound(format!(
                "index {} is no longer live",
                idx.index_oid.0
            )));
        }
        let handle = Arc::new(outcome.index);
        self.hnsw_handles
            .lock()
            .insert(idx.index_oid, Arc::clone(&handle));
        Ok(handle)
    }

    /// Open an HNSW index by its meta page (thin wrapper over the
    /// pg-am-hnsw open protocol, tech-selection §10.3): structural meta
    /// validation, expectation check, directory-chain walk with open-time
    /// repair, and rng skip-ahead. Warnings (currently the
    /// ef_search_default performance-knob mismatch, v1.4 nano) are passed
    /// through in [`pg_am_hnsw::OpenOutcome::warnings`].
    pub fn open_hnsw_index(
        &self,
        first_page: PageId,
        expected: &pg_am_hnsw::ExpectedParams,
    ) -> Result<pg_am_hnsw::OpenOutcome> {
        Ok(pg_am_hnsw::index::open(
            self.storage.buffer_pool(),
            self.storage.wal_writer(),
            first_page,
            expected,
        )?)
    }

    /// Insert a vector into an open HNSW index (thin wrapper over
    /// [`pg_am_hnsw::HnswIndex::insert`], tech-selection §8.1; M5 Stage C
    /// slice 3).
    ///
    /// Utility/auto-commit shape, aligned with [`Self::create_hnsw_index`] /
    /// [`Self::open_hnsw_index`]: the handle is a SHARED `&` reference (M6
    /// Stage B — the crate-internal latch protocol serializes concurrent
    /// writers), and the §8.1 boundary-① success flush (`flush_to` the
    /// PublishLive LSN) is already executed inside the crate before `Ok`
    /// escapes — the caller owes no further flush. A mid-sequence `Err` or
    /// crash leaves a §8.2 crash-window residue that slice-1 redo replays
    /// and the open protocol repairs.
    ///
    /// `index` must be a handle opened against THIS engine's storage
    /// (handles do not cross engines: the page ids inside name frames of
    /// the originating data directory — same premise as `BTreeIndex`).
    pub fn hnsw_insert(
        &self,
        index: &pg_am_hnsw::HnswIndex,
        vector: &[f32],
    ) -> Result<pg_am_hnsw::NodeId> {
        Ok(index.insert(
            self.storage.buffer_pool(),
            self.storage.wal_writer(),
            vector,
        )?)
    }

    /// k-nearest-neighbor search on an open HNSW index (thin wrapper over
    /// [`pg_am_hnsw::HnswIndex::search`], tech-selection §4.4/§8.1; M5
    /// Stage C slice 4).
    ///
    /// Read-only: no WAL records, no page mutations, no flush owed by the
    /// caller. `ef = None` uses the meta-pinned `ef_search_default`; the
    /// only per-query invariant is `ef >= k`. Same fail-stop corruption
    /// premise as the crate-level search (paged.rs module header); the
    /// handle is a shared `&` reference (M6 Stage B). The INITIALIZING
    /// state bit is deliberately NOT a visibility filter (§8.1③): a
    /// crash-window residue node is recallable. Transactional visibility
    /// filtering is [`Self::hnsw_search_visible`] (M6 Stage D).
    ///
    /// `index` must be a handle opened against THIS engine's storage —
    /// same premise as [`Self::hnsw_insert`].
    pub fn hnsw_search(
        &self,
        index: &pg_am_hnsw::HnswIndex,
        query: &[f32],
        k: usize,
        ef: Option<usize>,
    ) -> Result<Vec<(pg_am_hnsw::NodeId, f64)>> {
        Ok(index.search(self.storage.buffer_pool(), query, k, ef)?)
    }

    /// Visibility-filtered ANN search (M6 Stage D, tech-selection §7): the
    /// transactional `hnsw_search`. Five-step pipeline, all in pg-engine
    /// (pg-am-hnsw stays MVCC-free):
    ///
    /// 1. Candidate pull at 2k (initial 2x factor; ef grows on retries).
    /// 2. Sidecar map forward lookup (NodeId → ctid) — a miss means the
    ///    node was deleted (the tombstone path) or never committed.
    /// 3. Heap visibility check (`heap_tuple_visible`, HOT-chain aware,
    ///    read-only, no row locks).
    /// 4. In-row NodeId re-check — the row MUST carry the candidate's
    ///    NodeId; a mismatch means slot reuse (discard, never a wrong row).
    /// 5. Fewer than k survivors → ef doubles and re-search, at most 3
    ///    rounds; the visible subset is returned as-is (fewer than k is a
    ///    legal answer).
    ///
    /// `snap` is the caller's transaction snapshot (visibility is judged
    /// against it). Returns `(tid, distance)` of visible rows, ascending.
    #[allow(clippy::too_many_arguments)] // public M6 parameters + ScanSpec's two policy knobs
    pub(crate) fn hnsw_search_visible_with_policy(
        &self,
        table: &str,
        query: &[f32],
        k: usize,
        ef: Option<usize>,
        candidates: usize,
        max_retry_rounds: u8,
        snap: &Snapshot,
    ) -> Result<Vec<(Tid, f64)>> {
        let entry = self.table_entry(table)?;
        let col_types = column_types(&entry);
        let (idx, _col_index) = self
            .indexes_of(&entry)
            .into_iter()
            .find(|(i, _)| matches!(i.key_type, ColumnType::Vector(_)))
            .ok_or_else(|| EngineError::IndexNotFound(format!("{table}: no HNSW index")))?;
        let handle = self.hnsw_handle(&idx)?;
        // Loud Err (not panic) on a corrupted registration: an M5-shape or
        // hand-forged relpages row (last == first) means no sidecar map.
        let map = self.open_hnsw_map(idx.index_oid)?.ok_or_else(|| {
            EngineError::Corrupted(format!(
                "hnsw index {:?} has no sidecar map (relpages row is M5-shape or corrupt)",
                idx.index_oid
            ))
        })?;

        if k == 0 {
            return Ok(Vec::new());
        }
        if max_retry_rounds == 0 {
            return Err(EngineError::InvalidArgument(
                "HNSW search requires at least one candidate round".to_string(),
            ));
        }
        let hwm = handle.hwm();
        if hwm == 0 {
            return Ok(Vec::new());
        }
        let hwm = usize::try_from(hwm).unwrap_or(usize::MAX);
        let result_capacity = k.min(hwm);
        let ef_base = ef.unwrap_or(handle.params().ef_search_default as usize);
        for round in 0..u32::from(max_retry_rounds) {
            // Candidate counts are capped by the graph's actual node count,
            // so a user-controlled huge k cannot force a capacity overflow
            // or an oversized allocation. `ef` still satisfies ef >= want.
            let want = candidates
                .saturating_mul(2usize.saturating_pow(round))
                .min(hwm);
            let ef_round = ef_base.max(want);
            let candidates =
                handle.search(self.storage.buffer_pool(), query, want, Some(ef_round))?;
            let mut results: Vec<(Tid, f64)> = Vec::with_capacity(result_capacity);
            for (node_id, dist) in candidates {
                // 2. Map forward lookup — a miss IS the tombstone path.
                //    lookup_ALL (round-7 P1): a non-HOT non-vector update
                //    can leave TWO entries for one node while an old
                //    snapshot is alive (the old entry waits out the
                //    watermark): the old tid is dead for a fresh snapshot
                //    and a single `lookup` would return it and discard the
                //    node deterministically. Judge each returned ctid for
                //    visibility; at most one version is visible per
                //    snapshot (no duplicates).
                let key = pg_am_btree::encode_key(&Datum::Int8(i64::from(node_id.0)))?;
                let ctids = map.lookup_all(&key)?;
                let mut found: Option<Tid> = None;
                for ctid in ctids {
                    // 3. Heap visibility (HOT-chain aware).
                    let Some(vis_tid) = self.heap_tuple_visible(snap, ctid)? else {
                        continue;
                    };
                    // 4. In-row NodeId re-check — slot reuse ⇒ discard.
                    let (_values, row_nid) =
                        self.read_row_by_tid_with_node_id(&entry, &col_types, vis_tid)?;
                    if row_nid != Some(node_id.0) {
                        continue;
                    }
                    found = Some(vis_tid);
                    break;
                }
                let Some(vis_tid) = found else {
                    continue;
                };
                results.push((vis_tid, dist));
                if results.len() == k {
                    break;
                }
            }
            if results.len() == k || round + 1 == u32::from(max_retry_rounds) {
                return Ok(results);
            }
        }
        unreachable!("the loop returns on the last round")
    }

    /// Visibility-filtered ANN search with the M6 default policy (2k initial
    /// candidates and at most three rounds). This is the public compatibility
    /// wrapper; Phase 4a's ScanSpec supplies the same values explicitly.
    pub fn hnsw_search_visible(
        &self,
        table: &str,
        query: &[f32],
        k: usize,
        ef: Option<usize>,
        snap: &Snapshot,
    ) -> Result<Vec<(Tid, f64)>> {
        self.hnsw_search_visible_with_policy(table, query, k, ef, k.saturating_mul(2), 3, snap)
    }

    /// The leak ledger for the table's HNSW index (M6 Stage E,
    /// tech-selection §8.3/§9 监控): read-only accounting, never mutates.
    /// Combines the graph-side audit counters (`audit_index` +
    /// `audit_reachability` — the §12.5 quiesce premise applies to the
    /// reachability fields) with the engine-side orphan-map count (every
    /// sidecar entry range-scanned and probed against its heap tuple).
    ///
    /// `threshold` overrides [`DEFAULT_HNSW_LEAK_THRESHOLD`]; it must be
    /// finite and within `[0, 1]`. Fails with
    /// [`EngineError::IndexNotFound`] when the table has no HNSW index.
    pub fn hnsw_leak_report(&self, table: &str, threshold: Option<f64>) -> Result<HnswLeakReport> {
        let threshold = threshold.unwrap_or(DEFAULT_HNSW_LEAK_THRESHOLD);
        if !threshold.is_finite() || !(0.0..=1.0).contains(&threshold) {
            return Err(EngineError::InvalidArgument(format!(
                "hnsw leak threshold {threshold} is not finite or outside [0, 1]"
            )));
        }
        let entry = self.table_entry(table)?;
        let (idx, _) = self
            .indexes_of(&entry)
            .into_iter()
            .find(|(i, _)| matches!(i.key_type, ColumnType::Vector(_)))
            .ok_or_else(|| EngineError::IndexNotFound(format!("{table}: no HNSW index")))?;
        let handle = self.hnsw_handle(&idx)?;
        let audit = handle.audit(self.storage.buffer_pool())?;
        let reach =
            pg_am_hnsw::audit::audit_reachability(self.storage.buffer_pool(), idx.meta_page)?;
        let orphan_map_entries = match self.open_hnsw_map(idx.index_oid)? {
            Some(map) => self.count_orphan_map_entries(&map)?,
            None => 0,
        };
        let residue = audit.tombstoned_count
            + audit.initializing_count
            + audit.orphan_entry_count
            + orphan_map_entries;
        let leak_ratio = if audit.node_count == 0 {
            0.0
        } else {
            residue as f64 / audit.node_count as f64
        };
        Ok(HnswLeakReport {
            node_count: audit.node_count,
            live_count: audit.live_count,
            tombstoned_count: audit.tombstoned_count,
            initializing_count: audit.initializing_count,
            orphan_entry_count: audit.orphan_entry_count,
            orphan_map_entries,
            unreachable_live_count: reach.unreachable_live.len() as u64,
            unreachable_initializing_count: reach.unreachable_initializing_count,
            unreachable_tombstoned_count: reach.unreachable_tombstoned_count,
            leak_ratio,
            threshold,
            rebuild_recommended: leak_ratio > threshold,
        })
    }

    /// The local rebuild for the table's HNSW index (M6 Stage E,
    /// tech-selection §9 局部重建): one redirect pass over the LIVE nodes'
    /// in-edges that point at tombstoned nodes, then a fresh leak ledger.
    ///
    /// **独占执行 (§9)**: the pass runs under the table's
    /// `AccessExclusive` lock (the `vacuum` precedent) — concurrent DML
    /// queues; lock-free SEARCHES proceed (the read path takes no table
    /// lock; a reader sees either the old or the rewritten neighbor list,
    /// both valid graphs). Relationship to heap vacuum (§9 各管各的): the
    /// two channels are independent — neither touches the other's
    /// internals; taking the same table lock only serializes them.
    ///
    /// **What it does NOT do** (the §9 format-freeze boundary): no
    /// physical reclamation — tombstoned nodes stay mapped
    /// (`leak_after.tombstoned_count` is unchanged by the pass), no slot
    /// reuse, no page freeing. The edge semantics are approximate by
    /// design ("次近邻") with the recall gate as the backstop.
    ///
    /// Drive it from [`Self::hnsw_leak_report`]'s `rebuild_recommended`.
    /// Fails with [`EngineError::IndexNotFound`] when the table has no
    /// HNSW index.
    pub fn hnsw_vacuum(&self, table: &str) -> Result<HnswVacuumStats> {
        let redirect = self.auto_commit(|snap| {
            let entry =
                self.lock_table_entry(snap.current_xid(), table, LockMode::AccessExclusive)?;
            let (idx, _) = self
                .indexes_of(&entry)
                .into_iter()
                .find(|(i, _)| matches!(i.key_type, ColumnType::Vector(_)))
                .ok_or_else(|| EngineError::IndexNotFound(format!("{table}: no HNSW index")))?;
            let handle = self.hnsw_handle(&idx)?;
            Ok(handle.redirect_tombstoned_in_edges(
                self.storage.buffer_pool(),
                self.storage.wal_writer(),
            )?)
        })?;
        let leak_after = self.hnsw_leak_report(table, None)?;
        Ok(HnswVacuumStats {
            redirect,
            leak_after,
        })
    }

    /// Every sidecar-map entry whose row is gone or will never be visible
    /// again, counted by a full range scan + per-entry heap probe (cold
    /// path — the §9 leak accounting, not a query path).
    fn count_orphan_map_entries(&self, map: &pg_am_btree::BTreeIndex) -> Result<u64> {
        let mut orphans = 0u64;
        for (_key, tid) in map.range_scan(None, None)? {
            if self.map_entry_is_orphan(tid)? {
                orphans += 1;
            }
        }
        Ok(orphans)
    }

    /// The horizon-free deadness rule for the leak ledger (M6 Stage E):
    /// an entry is an orphan iff its row is gone or will never be visible
    /// again — slot empty, inserter aborted, or deleter committed.
    /// In-flight rows (xmin/xmax still InProgress) are NOT orphans: their
    /// transaction will decide, and counting them would false-positive on
    /// ordinary concurrent traffic.
    fn map_entry_is_orphan(&self, tid: Tid) -> Result<bool> {
        let guard = self.storage.buffer_pool().pin(tid.page_id)?;
        let page: &[u8; PAGE_SIZE] = guard.page().try_into().expect("frame is PAGE_SIZE");
        let Some(bytes) = SlottedPage::tuple(page, tid.slot_id)? else {
            return Ok(true);
        };
        let header = TupleHeader::read_from(&bytes[..pg_am_heap::tuple::TUPLE_HEADER_SIZE])?;
        if self.clog.get_state(header.t_xmin) == pg_storage::clog::TxnState::Aborted {
            return Ok(true);
        }
        // A HEAP_XMAX_LOCK_ONLY stamp is a row lock, not a delete (§9.1).
        let xmax = if header.t_infomask & pg_am_heap::tuple::HEAP_XMAX_LOCK_ONLY != 0 {
            TxnId::INVALID
        } else {
            header.t_xmax
        };
        Ok(xmax != TxnId::INVALID
            && self.clog.get_state(xmax) == pg_storage::clog::TxnState::Committed)
    }

    /// The catalog-writing half of `create_index`, inside transaction `snap`.
    fn create_index_catalog_rows(
        &self,
        snap: &Snapshot,
        entry: &TableEntry,
        column: &str,
        index_oid: Oid,
        meta_page: PageId,
        access_method_oid: Oid,
    ) -> Result<()> {
        let col_index = entry
            .columns
            .iter()
            .position(|c| c.name == column)
            .expect("create_index validated the column");
        let (type_oid, len) = type_oid_of(entry.columns[col_index].col_type)?;

        let class_row = vec![
            Some(Datum::Int8(index_oid.0 as i64)),
            Some(Datum::Text(format!("{}_{column}_idx", entry.oid.0))),
            Some(Datum::Text(RELKIND_INDEX.to_string())),
            Some(Datum::Int4(1)),
            Some(Datum::Int8(0)),
            Some(Datum::Int8(access_method_oid.0 as i64)),
        ];
        self.insert_catalog_row(snap, &PG_CLASS, &class_row)?;

        let attr_row = vec![
            Some(Datum::Int8(index_oid.0 as i64)),
            Some(Datum::Text(column.to_string())),
            Some(Datum::Int8(type_oid.raw().0 as i64)),
            Some(Datum::Int4(len)),
            Some(Datum::Int4(1)),
            Some(Datum::Int4(0)),
            Some(Datum::Int4(1)),
        ];
        self.insert_catalog_row(snap, &PG_ATTRIBUTE, &attr_row)?;

        let index_row = vec![
            Some(Datum::Int8(index_oid.0 as i64)),
            Some(Datum::Int8(entry.oid.0 as i64)),
            Some(Datum::Int4(1)),
            // indisunique / indisprimary: M2b builds non-unique indexes only.
            Some(Datum::Int4(0)),
            Some(Datum::Int4(0)),
        ];
        self.insert_catalog_row(snap, &PG_INDEX, &index_row)?;

        let relpages_row = vec![
            Some(Datum::Int8(index_oid.0 as i64)),
            Some(Datum::Int8(meta_page.0 as i64)),
            Some(Datum::Int8(meta_page.0 as i64)),
            Some(Datum::Int8(1)),
        ];
        self.insert_catalog_row(snap, &PG_RELPAGES, &relpages_row)?;
        Ok(())
    }

    /// Pre-check that the four catalog rows `create_index` will write fit
    /// their system pages, run BEFORE the expensive heap scan + bulk load
    /// (P3 review): failing here costs nothing, failing after the build
    /// would waste ~1s and ~20MB of WAL on a large table. The rows are
    /// encoded exactly as `create_index_catalog_rows` encodes them, so the
    /// check is authoritative, not an estimate.
    fn ensure_index_catalog_room(
        &self,
        entry: &TableEntry,
        column: &str,
        index_oid: Oid,
        access_method_oid: Oid,
    ) -> Result<()> {
        let col_index = entry
            .columns
            .iter()
            .position(|c| c.name == column)
            .expect("create_index validated the column");
        let (type_oid, len) = type_oid_of(entry.columns[col_index].col_type)?;
        let rows: [(&SystemTableDef, Vec<Value>); 4] = [
            (
                &PG_CLASS,
                vec![
                    Some(Datum::Int8(index_oid.0 as i64)),
                    Some(Datum::Text(format!("{}_{column}_idx", entry.oid.0))),
                    Some(Datum::Text(RELKIND_INDEX.to_string())),
                    Some(Datum::Int4(1)),
                    Some(Datum::Int8(0)),
                    Some(Datum::Int8(access_method_oid.0 as i64)),
                ],
            ),
            (
                &PG_ATTRIBUTE,
                vec![
                    Some(Datum::Int8(index_oid.0 as i64)),
                    Some(Datum::Text(column.to_string())),
                    Some(Datum::Int8(type_oid.raw().0 as i64)),
                    Some(Datum::Int4(len)),
                    Some(Datum::Int4(1)),
                    Some(Datum::Int4(0)),
                    Some(Datum::Int4(1)),
                ],
            ),
            (
                &PG_INDEX,
                vec![
                    Some(Datum::Int8(index_oid.0 as i64)),
                    Some(Datum::Int8(entry.oid.0 as i64)),
                    Some(Datum::Int4(1)),
                    Some(Datum::Int4(0)),
                    Some(Datum::Int4(0)),
                ],
            ),
            (
                &PG_RELPAGES,
                vec![
                    Some(Datum::Int8(index_oid.0 as i64)),
                    Some(Datum::Int8(0)), // meta page unknown yet; same 8-byte width
                    Some(Datum::Int8(0)),
                    Some(Datum::Int8(1)),
                ],
            ),
        ];
        for (def, row) in &rows {
            let columns = def.column_types();
            let tuple = encode_tuple(tuple_header(0), &columns, row)?;
            self.ensure_catalog_room(def, tuple.len())?;
        }
        Ok(())
    }

    /// Point lookup through the index on `table(column)` (M2b Stage M wave
    /// 2): resolve the index in the registry, open its B+Tree from the meta
    /// page, encode `key`, and return the heap TID of the first matching
    /// entry whose heap tuple is VISIBLE under a fresh snapshot (or `None`).
    ///
    /// Visibility mask (Stage O review): B+Tree entries carry no MVCC
    /// metadata, so a raw TID can point at an aborted or deleted heap
    /// version (e.g. an entry whose undo was skipped, or a duplicate key
    /// whose first version is dead). Every candidate TID is re-checked
    /// against the heap tuple header through the §7.2 oracle; M2b indexes
    /// are non-unique, so all duplicates of the key are walked and the
    /// first visible one wins. The snapshot is auto-commit style: a fresh
    /// SI snapshot per call, same as [`Engine::scan`].
    ///
    /// # WARNING
    ///
    /// Every call takes a NEW snapshot: a call made inside an explicit
    /// transaction does NOT see that transaction's own uncommitted writes
    /// (no read-your-writes), and two calls in the same transaction can
    /// observe different database states. Callers building higher-level
    /// transactional logic should NOT compose this API — use
    /// [`Engine::exec`] with SQL instead, which routes SELECT through
    /// `scan_inner` under the transaction snapshot.
    ///
    /// Takes no table lock, for the same reason as [`Engine::scan`] (no
    /// owning transaction; M2c Stage P).
    pub fn index_lookup(&self, table: &str, column: &str, key: &Datum) -> Result<Option<Tid>> {
        // `_guard`: registered for this call frame (M3 Stage A, §3.3).
        let (mut snap, _guard) = self.txn.snapshot(TxnId::INVALID);
        snap.advance_curcid();
        let index = self.btree_index(table, column)?;
        let key_bytes = encode_key(key)?;
        for tid in index.lookup_all(&key_bytes)? {
            if let Some(visible_tid) = self.heap_tuple_visible(&snap, tid)? {
                return Ok(Some(visible_tid));
            }
        }
        Ok(None)
    }

    /// Whether the heap tuple at `tid` is visible under `snap` (the
    /// `index_lookup` visibility mask): reads the tuple header directly
    /// from its page and runs the §7.2 judgment against the engine's CLOG.
    /// A slot that no longer holds a tuple reads as invisible.
    ///
    /// This checks visibility only, not that the tuple's key matches the
    /// queried key. That stays sound under vacuum's slot reuse (M3 Stage
    /// D): a dead slot becomes reusable ONLY through `reclaim`, and the
    /// §4.1 phase order guarantees every index entry pointing at it was
    /// removed (WAL-durable) earlier in the same vacuum pass — or, after a
    /// crash in window ①, by the next pass before any reuse, since the
    /// still-present dead tuple is re-collected and its delete tolerated
    /// as `EntryNotFound`. A stale entry resolving to an unrelated row in
    /// a recycled slot therefore requires a phase-ordering violation, not
    /// just bad luck.
    pub(crate) fn heap_tuple_visible(&self, snap: &Snapshot, tid: Tid) -> Result<Option<Tid>> {
        let guard = self.storage.buffer_pool().pin(tid.page_id)?;
        let page: &[u8; PAGE_SIZE] = guard.page().try_into().expect("frame is PAGE_SIZE");
        let Some(bytes) = SlottedPage::tuple(page, tid.slot_id)? else {
            return Ok(None);
        };
        let header = TupleHeader::read_from(&bytes[..pg_am_heap::tuple::TUPLE_HEADER_SIZE])?;
        // A HEAP_XMAX_LOCK_ONLY stamp is a row lock, not a delete: mask it
        // off so a FOR-UPDATE-locked row stays visible (§9.1, M2c Stage P).
        let xmax = if header.t_infomask & pg_am_heap::tuple::HEAP_XMAX_LOCK_ONLY != 0 {
            TxnId::INVALID
        } else {
            header.t_xmax
        };
        if is_visible(header.t_xmin, xmax, header.t_cid, snap, self.clog.as_ref()) {
            return Ok(Some(tid));
        }
        // HOT chain: old version is invisible but t_ctid may point to a
        // newer version visible to this snapshot (Stage S). The walk reads the
        // page pinned above (HOT chains never leave their page; re-pinning
        // here would be a recursive read latch, which deadlocks as soon as a
        // writer queues) and runs to the chain end, slot-count bounded — the
        // shared `follow_hot_chain` helper (post-Stage-S review H1; the
        // pre-H1 8-hop cap made >8-hop chains silently vanish from
        // index_lookup).
        if header.t_infomask2 & pg_am_heap::tuple::HEAP_HOT_UPDATED != 0 && header.t_ctid != tid {
            return Ok(pg_am_heap::follow_hot_chain(
                page,
                tid.page_id,
                header.t_ctid,
                snap,
                self.clog.as_ref(),
            )?);
        }
        Ok(None)
    }

    /// The TID that owns `tid`'s index entries.
    ///
    /// A `HEAP_ONLY_TUPLE` version was created without index entries of its
    /// own — that is the whole point of HOT — so index maintenance for such a
    /// version must act on its chain root instead. Thin wrapper over the
    /// shared [`pg_am_heap::hot_chain_root`] helper (post-Stage-S review H1,
    /// which also folded the reverse walk's per-hop O(slots) scan into one
    /// `t_ctid → slot` map, B6): pin the page, walk `t_ctid` links backwards
    /// to the first version that is not `HEAP_ONLY_TUPLE`. Every version in
    /// a HOT chain shares the same indexed columns, so the root's entry
    /// carries exactly the key the caller read back from `tid`.
    pub(crate) fn hot_chain_root(&self, tid: Tid) -> Result<Tid> {
        let guard = self.storage.buffer_pool().pin(tid.page_id)?;
        let page: &[u8; PAGE_SIZE] = guard.page().try_into().expect("frame is PAGE_SIZE");
        Ok(pg_am_heap::hot_chain_root(page, tid.page_id, tid)?)
    }

    /// Open the B+Tree handle for the index on `table(column)` (testing /
    /// advanced use — e.g. native `range_scan` / `validate`).
    pub fn btree_index(&self, table: &str, column: &str) -> Result<pg_am_btree::BTreeIndex> {
        let entry = self.table_entry(table)?;
        let idx = self
            .indexes
            .read()
            .iter()
            .find(|e| {
                matches!(e.kind, AmKind::BTree)
                    && e.table_oid == entry.oid
                    && e.column.eq_ignore_ascii_case(column)
            })
            .cloned()
            .ok_or_else(|| EngineError::IndexNotFound(format!("{table}({column})")))?;
        let btree = BTreeAM::new(
            Arc::clone(self.storage.buffer_pool()),
            Arc::clone(self.storage.wal_writer()),
        );
        Ok(btree.open_index(idx.index_oid, idx.meta_page, idx.key_type)?)
    }

    /// The live index registry entries (testing / diagnostics).
    pub fn indexes(&self) -> Vec<IndexEntry> {
        self.indexes.read().clone()
    }

    /// Whether the inverted planner may intentionally use a stale index.
    pub(crate) fn allow_stale_inverted(&self) -> bool {
        self.allow_stale_inverted
    }

    /// Open an inverted handle lazily. DDL serialization prevents a concurrent
    /// DROP from cleaning the registry before an old handle is republished.
    pub(crate) fn inverted_handle(
        &self,
        index_oid: Oid,
    ) -> Result<Arc<Mutex<pg_am_inverted::InvertedIndex>>> {
        let _ddl = self.ddl_lock.lock();
        if let Some(handle) = self.inverted_handles.lock().get(&index_oid) {
            return Ok(Arc::clone(handle));
        }
        let live = self
            .indexes
            .read()
            .iter()
            .any(|index| index.index_oid == index_oid && index.kind == AmKind::Inverted);
        if !live {
            return Err(EngineError::IndexNotFound(format!(
                "inverted index {index_oid}"
            )));
        }
        let mut store = pg_am_inverted::SegmentStore::open(
            self.storage.data_dir(),
            index_oid,
            &self.inverted_analyzer,
        )
        .map_err(|error| EngineError::Corrupted(error.to_string()))?;
        store.set_flush_threshold(self.inverted_seal_batch_size);
        let handle = Arc::new(Mutex::new(pg_am_inverted::InvertedIndex::from_store(store)));
        self.inverted_handles
            .lock()
            .insert(index_oid, Arc::clone(&handle));
        Ok(handle)
    }

    /// Select the indexed analyzer for an exact heap fallback, or the default
    /// analyzer when no inverted index exists on this table column.
    pub(crate) fn inverted_analyzer_for(
        &self,
        table_oid: Oid,
        column: &str,
    ) -> pg_am_inverted::AnalyzerConfig {
        self.indexes
            .read()
            .iter()
            .find(|index| {
                index.kind == AmKind::Inverted
                    && index.table_oid == table_oid
                    && index.column.eq_ignore_ascii_case(column)
            })
            .and_then(|index| index.inverted.as_ref())
            .map_or_else(
                || self.inverted_analyzer.clone(),
                |metadata| metadata.analyzer.clone(),
            )
    }

    /// Whether an inverted scan is complete for this snapshot. Read-only
    /// implicit transactions may use the index; any older snapshot, other
    /// in-flight writer, or write by the snapshot's own XID forces the heap.
    pub(crate) fn inverted_snapshot_allows(&self, index_oid: Oid, snap: &Snapshot) -> bool {
        if snap.xmax() != self.txn.current_xid()
            || snap.xip().iter().any(|xid| *xid != snap.current_xid())
        {
            return false;
        }
        !self
            .pending_inverted
            .lock()
            .get(&index_oid)
            .is_some_and(|pending| pending.contains_key(&snap.current_xid()))
    }

    fn seal_inverted(
        &self,
        index_oid: Oid,
        index: &mut pg_am_inverted::InvertedIndex,
    ) -> Result<()> {
        if index.seal_capacity_blocked() {
            return Ok(());
        }
        match index.seal(
            self.storage.data_dir(),
            self.storage.wal_writer(),
            TxnId::INVALID,
        ) {
            Ok(_) => Ok(()),
            Err(pg_am_inverted::InvertedError::Capacity(reason)) => {
                index.mark_seal_capacity_blocked();
                let watermark = index.watermark();
                tracing::warn!(
                    index_oid = index_oid.0,
                    consumed_lsn = watermark.consumed_lsn.0,
                    durable_applied_lsn = watermark.durable_applied_lsn.0,
                    %reason,
                    "inverted seal exceeded the frame capacity; serving the in-memory generation and keeping durable replay behind"
                );
                Ok(())
            }
            Err(error) => Err(EngineError::Corrupted(error.to_string())),
        }
    }

    fn merge_inverted_if_due(
        &self,
        index_oid: Oid,
        index: &mut pg_am_inverted::InvertedIndex,
    ) -> Result<()> {
        if !self.inverted_merge_enabled
            || index.active_segment_count() < 4
            || index.merge_capacity_blocked()
            || index.seal_capacity_blocked()
            || index.has_unsealed_changes()
        {
            return Ok(());
        }
        match index.merge(
            self.storage.data_dir(),
            self.storage.wal_writer(),
            TxnId::INVALID,
        ) {
            Ok(_) => Ok(()),
            Err(pg_am_inverted::InvertedError::Capacity(reason)) => {
                index.mark_merge_capacity_blocked();
                tracing::warn!(
                    index_oid = index_oid.0,
                    active_segments = index.active_segment_count(),
                    %reason,
                    "inverted merge exceeded the frame capacity; continuing with multiple segments"
                );
                Ok(())
            }
            Err(error) => Err(EngineError::Corrupted(error.to_string())),
        }
    }

    #[cfg(test)]
    fn run_before_inverted_seal_hook(&self) {
        BEFORE_INVERTED_SEAL_HOOK.with(|hook| {
            if let Some(hook) = hook.take() {
                hook(self);
            }
        });
    }

    /// Catch one inverted index up to the durable WAL frontier. Heap changes
    /// remain private per XID until Commit; Abort discards them. The consumed
    /// frontier is an end-LSN prefix, so replay starts at it directly (or at
    /// `Lsn::FIRST` for an empty WAL), never one record beyond it.
    pub(crate) fn refresh_inverted(
        &self,
        index_entry: &IndexEntry,
        entry: &TableEntry,
    ) -> Result<InvertedIndexMetadata> {
        let handle = self.inverted_handle(index_entry.index_oid)?;
        let mut index = handle.lock();
        let start = index.watermark().consumed_lsn;
        let current = self.storage.wal_writer().current_lsn();
        let frontier = if start == current {
            // No new records need replay or fsync. Most read-only queries take
            // this path instead of paying a full group-commit round.
            start
        } else {
            // Include a caller's own unflushed writes in pending, so the snapshot
            // check above cannot accidentally treat its read-write transaction
            // as read-only.
            self.storage.wal_writer().flush()?;
            self.storage.wal_writer().synced_lsn()
        };
        if start > frontier {
            return Err(EngineError::Corrupted(format!(
                "inverted consumed LSN {start} exceeds synced frontier {frontier}"
            )));
        }
        if start == frontier {
            #[cfg(test)]
            self.run_before_inverted_seal_hook();
            // Seal/merge markers are appended after any concurrently committed
            // records. Their end LSN must not consume those records unseen;
            // the next replay pass reads the markers and every preceding DML.
            self.seal_inverted(index_entry.index_oid, &mut index)?;
            self.merge_inverted_if_due(index_entry.index_oid, &mut index)?;
            self.record_inverted_lag(
                index_entry.index_oid,
                start,
                index.watermark().consumed_lsn,
                frontier,
            );
            let metadata = InvertedIndexMetadata {
                analyzer: index.analyzer_config().clone(),
                watermark: index.watermark(),
            };
            drop(index);
            self.publish_inverted_watermark(index_entry.index_oid, &metadata);
            return Ok(metadata);
        }

        let col_index = entry
            .columns
            .iter()
            .position(|column| column.name.eq_ignore_ascii_case(&index_entry.column))
            .ok_or_else(|| {
                EngineError::Corrupted(format!(
                    "inverted index column {:?} is missing from table {}",
                    index_entry.column, entry.oid.0
                ))
            })?;
        let col_types = column_types(entry);
        let rel = relation_desc(entry, &col_types);
        let pages = self
            .heap
            .relation_pages(&rel)?
            .into_iter()
            .collect::<HashSet<_>>();
        let toast_pages = if let Some(toast) = entry.toast {
            self.heap
                .relation_pages(&RelationDesc {
                    rel_oid: toast.rel_oid,
                    first_page: toast.first_page,
                    columns: &[ColumnType::Int8, ColumnType::Int4, ColumnType::Bytea],
                })?
                .into_iter()
                .collect::<HashSet<_>>()
        } else {
            HashSet::new()
        };
        let replay_start = if start.is_valid() { start } else { Lsn::FIRST };
        let tail = pg_am_inverted::tier2::FlushedWalTail::new(
            self.storage.data_dir().join("wal"),
            self.storage.config().wal_segment_size,
            Arc::clone(self.storage.wal_writer()),
        );
        let records = pg_storage::tier2::WalTailReader::tail_from(&tail, replay_start);
        let mut all_pending = self.pending_inverted.lock();
        let pending = all_pending.entry(index_entry.index_oid).or_default();
        for record in records {
            let record = record?;
            let record_end = record.lsn.advance(record.record_size() as u64);
            if record_end > frontier {
                break;
            }
            match record.record_type {
                WalRecordType::HeapInsert => {
                    let decoded =
                        pg_storage::wal::record::HeapInsertRecord::decode(&record.payload)?;
                    if toast_pages.contains(&decoded.page_id) {
                        push_inverted_pending(
                            pending,
                            record.txn_id,
                            InvertedWalAction::ToastChanged {
                                valueid: Some(self.toast_valueid_from_tuple(&decoded.tuple_bytes)?),
                            },
                            INVERTED_PENDING_LIMIT,
                            INVERTED_PENDING_BYTES_LIMIT,
                        )?;
                    }
                    if pages.contains(&decoded.page_id) {
                        let tid = Tid {
                            page_id: decoded.page_id,
                            slot_id: decoded.slot_id,
                        };
                        push_inverted_pending(
                            pending,
                            record.txn_id,
                            InvertedWalAction::Upsert {
                                tid,
                                tuple_bytes: decoded.tuple_bytes,
                            },
                            INVERTED_PENDING_LIMIT,
                            INVERTED_PENDING_BYTES_LIMIT,
                        )?;
                    }
                }
                WalRecordType::HeapUpdate => {
                    let decoded = HeapUpdateRecord::decode(&record.payload)?;
                    if toast_pages.contains(&decoded.old_tid.page_id)
                        || toast_pages.contains(&decoded.new_tid.page_id)
                    {
                        push_inverted_pending(
                            pending,
                            record.txn_id,
                            InvertedWalAction::ToastChanged {
                                valueid: Some(
                                    self.toast_valueid_from_tuple(&decoded.new_tuple_bytes)?,
                                ),
                            },
                            INVERTED_PENDING_LIMIT,
                            INVERTED_PENDING_BYTES_LIMIT,
                        )?;
                    }
                    if pages.contains(&decoded.new_tid.page_id) {
                        let old_tid = self.hot_chain_root(decoded.old_tid)?;
                        // Update order is part of the correctness contract:
                        // old postings disappear before the new version is added.
                        push_inverted_pending(
                            pending,
                            record.txn_id,
                            InvertedWalAction::Delete { tid: old_tid },
                            INVERTED_PENDING_LIMIT,
                            INVERTED_PENDING_BYTES_LIMIT,
                        )?;
                        push_inverted_pending(
                            pending,
                            record.txn_id,
                            InvertedWalAction::Upsert {
                                tid: decoded.new_tid,
                                tuple_bytes: decoded.new_tuple_bytes,
                            },
                            INVERTED_PENDING_LIMIT,
                            INVERTED_PENDING_BYTES_LIMIT,
                        )?;
                    }
                }
                WalRecordType::HeapDelete => {
                    let decoded = HeapDeleteRecord::decode(&record.payload)?;
                    if toast_pages.contains(&decoded.tid.page_id) {
                        push_inverted_pending(
                            pending,
                            record.txn_id,
                            InvertedWalAction::ToastChanged {
                                valueid: self.toast_valueid_at(decoded.tid, decoded.xmax)?,
                            },
                            INVERTED_PENDING_LIMIT,
                            INVERTED_PENDING_BYTES_LIMIT,
                        )?;
                    }
                    if pages.contains(&decoded.tid.page_id) {
                        let tid = self.hot_chain_root(decoded.tid)?;
                        push_inverted_pending(
                            pending,
                            record.txn_id,
                            InvertedWalAction::Delete { tid },
                            INVERTED_PENDING_LIMIT,
                            INVERTED_PENDING_BYTES_LIMIT,
                        )?;
                    }
                }
                WalRecordType::TxnCommit => {
                    let actions = pending.get(&record.txn_id).cloned().unwrap_or_default();
                    if actions.is_empty() {
                        pending.remove(&record.txn_id);
                        #[cfg(test)]
                        self.record_inverted_visible(index_entry.index_oid, record.txn_id);
                        index.advance_consumed(record_end);
                        continue;
                    }
                    let valueids = actions
                        .iter()
                        .filter_map(|action| match action {
                            InvertedWalAction::ToastChanged {
                                valueid: Some(valueid),
                            } => Some(*valueid),
                            _ => None,
                        })
                        .collect::<HashSet<_>>();
                    let validate_all = actions.iter().any(|action| {
                        matches!(action, InvertedWalAction::ToastChanged { valueid: None })
                    });
                    let (commit_snap, _commit_guard) =
                        self.inverted_commit_snapshot(record.txn_id)?;
                    self.validate_toast_groups(&commit_snap, &rel, &valueids, validate_all)?;
                    for action in actions {
                        match action {
                            InvertedWalAction::ToastChanged { .. } => {}
                            InvertedWalAction::Upsert { tid, tuple_bytes } => {
                                let text = if self.inverted_version_is_current(
                                    &commit_snap,
                                    &col_types,
                                    tid,
                                    record.txn_id,
                                )? {
                                    self.decode_inverted_text(
                                        &commit_snap,
                                        &rel,
                                        &col_types,
                                        col_index,
                                        &tuple_bytes,
                                    )?
                                } else {
                                    None
                                };
                                index
                                    .delete_document(tid)
                                    .map_err(|error| EngineError::Corrupted(error.to_string()))?;
                                if let Some(text) = text {
                                    index.insert_document(tid, &text).map_err(|error| {
                                        EngineError::Corrupted(error.to_string())
                                    })?;
                                }
                            }
                            InvertedWalAction::Delete { tid } => {
                                index
                                    .delete_document(tid)
                                    .map_err(|error| EngineError::Corrupted(error.to_string()))?;
                            }
                        }
                    }
                    pending.remove(&record.txn_id);
                    #[cfg(test)]
                    self.record_inverted_visible(index_entry.index_oid, record.txn_id);
                }
                WalRecordType::TxnAbort => {
                    pending.remove(&record.txn_id);
                }
                _ => {}
            }
            index.advance_consumed(record_end);
        }
        #[cfg(test)]
        self.run_before_inverted_seal_hook();
        // Do not jump consumed_lsn to either administrative marker's end:
        // another session may have committed DML between our replay frontier
        // and the marker. The next refresh replays that DML before the marker.
        self.seal_inverted(index_entry.index_oid, &mut index)?;
        self.merge_inverted_if_due(index_entry.index_oid, &mut index)?;
        self.record_inverted_lag(
            index_entry.index_oid,
            start,
            index.watermark().consumed_lsn,
            frontier,
        );
        let metadata = InvertedIndexMetadata {
            analyzer: index.analyzer_config().clone(),
            watermark: index.watermark(),
        };
        drop(all_pending);
        drop(index);
        self.publish_inverted_watermark(index_entry.index_oid, &metadata);
        Ok(metadata)
    }

    /// Return whether `tid` is still the visible version written by `xid`.
    /// Superseded versions must not be detoasted: their chunks may have been
    /// legally reclaimed, and later WAL records will converge to the new row.
    fn inverted_version_is_current(
        &self,
        snap: &Snapshot,
        col_types: &[ColumnType],
        tid: Tid,
        xid: TxnId,
    ) -> Result<bool> {
        if self.heap_tuple_visible(snap, tid)? != Some(tid) {
            return Ok(false);
        }
        let guard = self.storage.buffer_pool().pin(tid.page_id)?;
        let page: &[u8; PAGE_SIZE] = guard.page().try_into().expect("frame is PAGE_SIZE");
        let Some(bytes) = SlottedPage::tuple(page, tid.slot_id)? else {
            return Ok(false);
        };
        let (header, _) = decode_tuple(bytes, col_types)?;
        Ok(header.t_xmin == xid)
    }

    fn decode_inverted_text(
        &self,
        snap: &Snapshot,
        rel: &RelationDesc<'_>,
        col_types: &[ColumnType],
        col_index: usize,
        tuple_bytes: &[u8],
    ) -> Result<Option<String>> {
        let (_, values) = decode_tuple(tuple_bytes, col_types)?;
        let values = self
            .heap
            .detoast_values(rel, snap, self.clog.as_ref(), &values)?;
        match &values[col_index] {
            Some(Datum::Text(text)) => Ok(Some(text.clone())),
            None => Ok(None),
            other => Err(EngineError::Corrupted(format!(
                "inverted TEXT column decoded as {other:?}"
            ))),
        }
    }

    fn inverted_commit_snapshot(&self, xid: TxnId) -> Result<(Snapshot, SnapshotGuard)> {
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            let (mut snap, guard) = self.txn.snapshot(TxnId::INVALID);
            // CLOG may flip before the XID leaves the active set. Only a
            // snapshot that excludes this XID can see its newly written chunks.
            if !snap.xip().contains(&xid) {
                snap.advance_curcid();
                return Ok((snap, guard));
            }
            if Instant::now() >= deadline {
                return Err(EngineError::Corrupted(format!(
                    "commit record for XID {} is visible while it remains in the active set",
                    xid.0
                )));
            }
            drop(guard);
            std::thread::yield_now();
        }
    }

    fn validate_toast_groups(
        &self,
        snap: &Snapshot,
        rel: &RelationDesc<'_>,
        valueids: &HashSet<u32>,
        validate_all: bool,
    ) -> Result<()> {
        if valueids.is_empty() && !validate_all {
            return Ok(());
        }
        // One main-relation scan per commit, and detoast only rows that point
        // to a changed group. This avoids a full detoast pass per chunk record.
        let rows = self.heap.scan(ScanContext {
            rel: *rel,
            snapshot: snap,
            clog: self.clog.as_ref(),
        })?;
        for (_, values) in rows {
            let changed = validate_all
                || values.iter().any(|value| match value {
                    Some(Datum::External(pointer)) => valueids.contains(&pointer.va_valueid),
                    _ => false,
                });
            if changed {
                self.heap
                    .detoast_values(rel, snap, self.clog.as_ref(), &values)?;
            }
        }
        Ok(())
    }

    fn toast_valueid_from_tuple(&self, tuple_bytes: &[u8]) -> Result<u32> {
        let (_, values) = decode_tuple(
            tuple_bytes,
            &[ColumnType::Int8, ColumnType::Int4, ColumnType::Bytea],
        )?;
        match values.first() {
            Some(Some(Datum::Int8(valueid))) => u32::try_from(*valueid)
                .map_err(|_| EngineError::Corrupted(format!("invalid TOAST valueid {valueid}"))),
            other => Err(EngineError::Corrupted(format!(
                "malformed TOAST chunk identity: {other:?}"
            ))),
        }
    }

    fn toast_valueid_at(&self, tid: Tid, xmax: TxnId) -> Result<Option<u32>> {
        let guard = self.storage.buffer_pool().pin(tid.page_id)?;
        let page: &[u8; PAGE_SIZE] = guard.page().try_into().expect("frame is PAGE_SIZE");
        let Some(bytes) = SlottedPage::tuple(page, tid.slot_id)? else {
            // Vacuum may have reclaimed the physical tuple. Validate all
            // referenced groups at commit instead of trusting a missing slot.
            return Ok(None);
        };
        let (header, _) = decode_tuple(
            bytes,
            &[ColumnType::Int8, ColumnType::Int4, ColumnType::Bytea],
        )?;
        if header.t_xmax != xmax {
            // A reused slot belongs to a different valueid. Full validation is
            // safer than validating the wrong group.
            return Ok(None);
        }
        Ok(Some(self.toast_valueid_from_tuple(bytes)?))
    }

    fn publish_inverted_watermark(&self, index_oid: Oid, metadata: &InvertedIndexMetadata) {
        if let Some(entry) = self
            .indexes
            .write()
            .iter_mut()
            .find(|entry| entry.index_oid == index_oid)
        {
            entry.inverted = Some(metadata.clone());
        }
    }

    /// Insert one row (single auto-commit transaction) and return its TID.
    ///
    /// `values` must match the table's schema in count and types. The
    /// tuple's `t_xmin` is stamped by the AM with the transaction's own XID
    /// (Stage K), so callers cannot mislabel the writer.
    ///
    /// Index maintenance (Stage M wave 3): every index registered on the
    /// table gains a `(key, tid)` entry in the SAME transaction — the heap
    /// insert runs first (it allocates the TID), then each index is updated
    /// (NULL keys are skipped, matching `create_index`).
    pub fn insert(&self, table: &str, values: &[Value]) -> Result<Tid> {
        self.auto_commit(|snap| {
            let entry = self.lock_table_entry(snap.current_xid(), table, LockMode::RowExclusive)?;
            self.insert_inner(snap, &entry, values)
        })
    }

    /// Reuse an unchanged old pointer or write a fresh chunk group for one
    /// attribute. The old pointer is only accepted after its detoasted value
    /// matches the new value under the current snapshot.
    fn externalize_or_reuse(
        &self,
        snap: &Snapshot,
        rel: &RelationDesc<'_>,
        index: usize,
        column: ColumnType,
        new_datum: &Datum,
        old: (Option<&[Value]>, Option<&[Value]>),
    ) -> Result<Datum> {
        let old_pointer = match old.0.and_then(|old| old.get(index)) {
            Some(Some(Datum::External(pointer))) => Some(pointer),
            _ => None,
        };
        if let Some(pointer) = old_pointer {
            let unchanged = match old.1.and_then(|old| old.get(index)) {
                Some(old_value) => old_value.as_ref().is_some_and(|old| old == new_datum),
                None => false,
            };
            if unchanged {
                return Ok(Datum::External(*pointer));
            }
        }
        self.heap
            .externalize_value(rel, snap, column, new_datum)
            .map_err(EngineError::from)
    }

    /// The tuple codec requires all non-NULL VECTOR attributes to use the
    /// same representation. Once one is external, normalize every remaining
    /// inline VECTOR before the next encode so no payload tag inspection is
    /// ever needed to distinguish them.
    fn externalize_inline_vectors(
        &self,
        snap: &Snapshot,
        rel: &RelationDesc<'_>,
        col_types: &[ColumnType],
        working: &mut [Value],
        old_values: Option<&[Value]>,
        old_detoasted: Option<&[Value]>,
    ) -> Result<()> {
        let has_external = col_types.iter().zip(working.iter()).any(|(column, value)| {
            matches!(column, ColumnType::Vector(_)) && matches!(value, Some(Datum::External(_)))
        });
        if !has_external {
            return Ok(());
        }
        for (index, column) in col_types.iter().enumerate() {
            if !matches!(column, ColumnType::Vector(_)) {
                continue;
            }
            let datum = match working.get(index) {
                Some(Some(datum)) if !matches!(datum, Datum::External(_)) => datum.clone(),
                _ => continue,
            };
            let external = self.externalize_or_reuse(
                snap,
                rel,
                index,
                *column,
                &datum,
                (old_values, old_detoasted),
            )?;
            working[index] = Some(external);
        }
        Ok(())
    }

    /// Encode a main row, writing oversized attributes as chunk groups first.
    /// Every above-threshold value is externalized even when the row already
    /// fits; below-threshold values are then selected largest-first until the
    /// main row fits a heap page. An unchanged old pointer may be shared after
    /// its bytes are compared under the current query snapshot.
    fn encode_row_with_toast(
        &self,
        snap: &Snapshot,
        entry: &TableEntry,
        col_types: &[ColumnType],
        values: &[Value],
        node_id: Option<u32>,
        old_values: Option<&[Value]>,
    ) -> Result<Vec<u8>> {
        if values.len() != entry.columns.len() {
            return Err(EngineError::InvalidArgument(format!(
                "table has {} columns but {} values given",
                entry.columns.len(),
                values.len()
            )));
        }
        let rel = relation_desc(entry, col_types);
        let old_detoasted = old_values
            .map(|old| {
                self.heap
                    .detoast_values(&rel, snap, self.clog.as_ref(), old)
            })
            .transpose()?;
        let encode = |values: &[Value]| {
            match node_id {
                Some(node_id) => pg_am_heap::tuple::encode_tuple_with_node_id(
                    tuple_header(snap.curcid()),
                    col_types,
                    values,
                    node_id,
                ),
                None => encode_tuple(tuple_header(snap.curcid()), col_types, values),
            }
            .map_err(EngineError::from)
        };

        let mut working = values.to_vec();
        self.externalize_inline_vectors(
            snap,
            &rel,
            col_types,
            &mut working,
            old_values,
            old_detoasted.as_deref(),
        )?;
        let mut tuple = encode(&working)?;
        loop {
            let must_fit = tuple.len() > MAX_TUPLE_BYTES;
            let mut candidate: Option<(usize, usize)> = None;
            for (index, (column, value)) in col_types.iter().zip(&working).enumerate() {
                let Some(datum) = value else { continue };
                if !toastable(*column) || matches!(datum, Datum::External(_)) {
                    continue;
                }
                let size = inline_size(*column, datum)?;
                if size <= TOAST_TUPLE_THRESHOLD && !must_fit {
                    continue;
                }
                if candidate.is_none_or(|(_, best)| size > best) {
                    candidate = Some((index, size));
                }
            }
            let Some((index, _)) = candidate else {
                // No externalizable attribute remains. If this still exceeds
                // the page bound, HeapAM's existing TupleTooLarge guard is the
                // authoritative error and no chunk was written for this row.
                return Ok(tuple);
            };

            let Some(Some(datum)) = working.get(index).cloned() else {
                return Err(EngineError::Corrupted(format!(
                    "TOAST candidate column {index} changed while encoding"
                )));
            };
            let external = self.externalize_or_reuse(
                snap,
                &rel,
                index,
                col_types[index],
                &datum,
                (old_values, old_detoasted.as_deref()),
            )?;
            working[index] = Some(external);
            self.externalize_inline_vectors(
                snap,
                &rel,
                col_types,
                &mut working,
                old_values,
                old_detoasted.as_deref(),
            )?;
            tuple = encode(&working)?;
        }
    }

    /// The shared insert path. Takes the registry `entry` from the caller
    /// (Stage O review): SQL exec already holds it for
    /// `build_insert_values`, so looking it up again here would be a
    /// redundant recursive `registry.read()` per row.
    fn insert_inner(&self, snap: &Snapshot, entry: &TableEntry, values: &[Value]) -> Result<Tid> {
        let col_types = column_types(entry);
        let indexes = self.indexes_of(entry);

        // M6 Stage C (tech-selection §7/§11): at most ONE hnsw index per
        // table (the row carries a single trailing NodeId, §7 (b)). The
        // graph insert runs BEFORE the row encoding so the NodeId travels
        // inside the tuple (encode_tuple_with_node_id); a NULL vector
        // column skips the graph entirely (no node, no trailing id, no map
        // entry).
        let hnsw = indexes
            .iter()
            .find(|(idx, _)| matches!(idx.key_type, ColumnType::Vector(_)));
        let mut node_id: Option<u32> = None;
        if let Some((idx, col_index)) = hnsw {
            // `get` (not a raw index) mirrors update_inner: a short `values`
            // slice must fall through to encode_row's length check with a
            // loud InvalidArgument, never panic (round-4 P3-A).
            if let Some(Some(Datum::Vector(v))) = values.get(*col_index) {
                let handle = self.hnsw_handle(idx)?;
                // Transactional form (M6 §6.2/§10): NO per-insert flush —
                // the commit record's flush covers this sequence (WAL
                // prefix property). The utility form (`hnsw_insert`)
                // keeps the M5 success boundary.
                let id = handle.insert_unflushed(
                    self.storage.buffer_pool(),
                    self.storage.wal_writer(),
                    v,
                )?;
                node_id = Some(id.0);
                // Abort pairing (M6 Stage C §6.2): if this transaction
                // aborts, the node is tombstoned — the NodeId is the key.
                self.index_undo
                    .lock()
                    .entry(snap.current_xid())
                    .or_default()
                    .push(IndexUndo {
                        index: (*idx).clone(),
                        key: id.0.to_le_bytes().to_vec(),
                        tid: Tid {
                            page_id: PageId::INVALID,
                            slot_id: 0,
                        },
                        op: IndexUndoOp::HnswInserted,
                    });
            }
        }

        let tuple = self.encode_row_with_toast(snap, entry, &col_types, values, node_id, None)?;
        let mut out_tid = Tid {
            page_id: PageId::INVALID,
            slot_id: 0,
        };
        self.heap.insert(InsertContext {
            rel: relation_desc(entry, &col_types),
            snapshot: snap,
            tuple: &tuple,
            out_tid: Some(&mut out_tid),
        })?;

        // B+Tree index maintenance (skip HNSW and inverted: their graph /
        // Tier 2 WAL consumer owns maintenance, never this B+Tree loop).
        for (idx, col_index) in &indexes {
            if matches!(idx.kind, AmKind::Inverted) {
                continue;
            }
            if matches!(idx.key_type, ColumnType::Vector(_)) {
                continue;
            }
            if let Some(datum) = &values[*col_index] {
                let key = encode_key(datum)?;
                self.open_btree(idx)?.insert(&key, out_tid)?;
                self.record_index_undo(
                    snap.current_xid(),
                    idx,
                    key,
                    out_tid,
                    IndexUndoOp::Inserted,
                );
            }
        }

        // The sidecar map entry, transactional via the map B+Tree's own
        // IndexUndo pair (insert-abort removes it).
        if let (Some((idx, _)), Some(nid)) = (hnsw, node_id) {
            self.hnsw_map_put_txn(snap.current_xid(), idx, nid, out_tid)?;
        }
        Ok(out_tid)
    }

    /// Return every visible row of `table` as `(tid, values)`.
    ///
    /// Scans with a real SI snapshot against the engine's CLOG: committed
    /// rows are visible, aborted or in-progress writers are not.
    /// `predicate` applies a single-column filter after visibility.
    ///
    /// Takes NO table lock (M2c Stage P): this snapshot-only read owns no
    /// transaction, so there is no XID to key a lock on or a commit to
    /// release it at. A `DROP TABLE` racing this scan is the pre-existing
    /// documented DDL-vs-DML gap; SQL SELECT inside an explicit transaction
    /// (`exec`) DOES take `AccessShare`.
    pub fn scan(
        &self,
        table: &str,
        predicate: Option<Predicate>,
    ) -> Result<Vec<(Tid, Vec<Value>)>> {
        // `_guard`: registered for this call frame (M3 Stage A, §3.3).
        let (mut snap, _guard) = self.txn.snapshot(TxnId::INVALID);
        snap.advance_curcid();
        self.scan_inner(&snap, table, predicate.as_ref())
    }

    pub(crate) fn scan_inner(
        &self,
        snap: &Snapshot,
        table: &str,
        predicate: Option<&Predicate>,
    ) -> Result<Vec<(Tid, Vec<Value>)>> {
        let entry = self.table_entry(table)?;
        let col_types = column_types(&entry);
        validate_predicate(table, &entry, predicate)?;
        let mut rows = self.heap.scan(ScanContext {
            rel: relation_desc(&entry, &col_types),
            snapshot: snap,
            clog: self.clog.as_ref(),
        })?;
        apply_predicate(&mut rows, predicate);
        Ok(rows)
    }

    /// Replace the row at `tid` with `values` (single auto-commit
    /// transaction) and return the new version's TID.
    ///
    /// Index maintenance (Stage M wave 3): an update is delete-old +
    /// insert-new per index — the old key is read back from the heap row
    /// BEFORE the heap update, then `(old_key, old_tid)` is removed and
    /// `(new_key, new_tid)` inserted, all in the same transaction. NULL keys
    /// are skipped on both sides.
    pub fn update(&self, table: &str, tid: Tid, values: &[Value]) -> Result<Tid> {
        self.auto_commit(|snap| {
            self.lock_table_entry(snap.current_xid(), table, LockMode::RowExclusive)?;
            self.update_inner(snap, table, tid, values)
        })
    }

    fn update_inner(
        &self,
        snap: &Snapshot,
        table: &str,
        tid: Tid,
        values: &[Value],
    ) -> Result<Tid> {
        let entry = self.table_entry(table)?;
        let col_types = column_types(&entry);
        let indexes = self.indexes_of(&entry);

        // Read both forms: raw pointers may be reused by the new version,
        // while index maintenance and change detection need detoasted values.
        let hnsw = indexes
            .iter()
            .find(|(idx, _)| matches!(idx.key_type, ColumnType::Vector(_)));
        let (old_raw_values, old_node_id) =
            self.read_row_by_tid_with_node_id(&entry, &col_types, tid)?;
        let old_values = self.heap.detoast_values(
            &relation_desc(&entry, &col_types),
            snap,
            self.clog.as_ref(),
            &old_raw_values,
        )?;

        // Did the vector column change? (HOT eligibility below compares
        // values per indexed column; the vector column participates.)
        let vector_changed = match hnsw {
            Some((_, ci)) => values.get(*ci) != old_values.get(*ci),
            None => false,
        };

        // The new row's NodeId: a vector-column change draws a fresh graph
        // node; an unchanged vector keeps the old one (and NULL on either
        // side carries none).
        let mut new_node_id: Option<u32> = None;
        if let Some((idx, ci)) = hnsw {
            match (vector_changed, values.get(*ci)) {
                (true, Some(Some(Datum::Vector(v)))) => {
                    let handle = self.hnsw_handle(idx)?;
                    let id = handle
                        .insert_unflushed(self.storage.buffer_pool(), self.storage.wal_writer(), v)?
                        .0;
                    new_node_id = Some(id);
                    // Abort pairing (review round 3 P1-1): the vector-change
                    // arm's fresh graph node is an insert-direction write —
                    // without this undo, every aborted vector update leaks
                    // a LIVE graph node (linear in aborts, unbounded).
                    self.index_undo
                        .lock()
                        .entry(snap.current_xid())
                        .or_default()
                        .push(IndexUndo {
                            index: (*idx).clone(),
                            key: id.to_le_bytes().to_vec(),
                            tid: Tid {
                                page_id: PageId::INVALID,
                                slot_id: 0,
                            },
                            op: IndexUndoOp::HnswInserted,
                        });
                }
                (true, _) => {} // became NULL: no node
                (false, _) => new_node_id = old_node_id,
            }
        }

        let tuple = self.encode_row_with_toast(
            snap,
            &entry,
            &col_types,
            values,
            new_node_id,
            Some(&old_raw_values),
        )?;
        let hot_eligible = !indexes.is_empty()
            && indexes
                .iter()
                .all(|(_, col_idx)| old_values.get(*col_idx) == values.get(*col_idx));
        let mut out_tid = Tid {
            page_id: PageId::INVALID,
            slot_id: 0,
        };
        self.heap.update(UpdateContext {
            rel: relation_desc(&entry, &col_types),
            snapshot: snap,
            old_tid: tid,
            new_tuple: &tuple,
            out_tid: Some(&mut out_tid),
            clog: self.clog.as_ref(),
            hot_eligible,
        })?;
        // Skip index maintenance when HOT was actually applied (same-page
        // update with unchanged key columns): the old index entry still
        // points to the chain root, and scan follows t_ctid to the new
        // version. For HNSW the same holds: the row keeps its NodeId, the
        // map entry keeps pointing at the chain root. Cross-page fallback
        // reverts to the non-HOT path.
        let hot_applied = hot_eligible && out_tid.page_id == tid.page_id;
        if !hot_applied {
            // The entry to remove belongs to the chain root: `tid` itself may
            // be a HOT descendant, which never got an entry of its own.
            let index_tid = if indexes.is_empty() {
                tid
            } else {
                self.hot_chain_root(tid)?
            };
            for (idx, col_index) in &indexes {
                if matches!(idx.kind, AmKind::Inverted) {
                    continue;
                }
                if matches!(idx.key_type, ColumnType::Vector(_)) {
                    continue;
                }
                let mut index = self.open_btree(idx)?;
                if let Some(old_datum) = &old_values[*col_index] {
                    let key = encode_key(old_datum)?;
                    index.delete(&key, index_tid)?;
                    self.record_index_undo(
                        snap.current_xid(),
                        idx,
                        key,
                        index_tid,
                        IndexUndoOp::Deleted,
                    );
                }
                if let Some(new_datum) = &values[*col_index] {
                    let key = encode_key(new_datum)?;
                    index.insert(&key, out_tid)?;
                    self.record_index_undo(
                        snap.current_xid(),
                        idx,
                        key,
                        out_tid,
                        IndexUndoOp::Inserted,
                    );
                }
            }
            // HNSW maintenance (M6 Stage C):
            // - The NEW map entry goes in-transaction (insert undo pair).
            // - The OLD node's removal is DEFERRED to post-commit
            //   (pending_hnsw): tombstone for a vector change, map-only for
            //   a plain ctid move.
            if let Some((idx, _)) = hnsw {
                if let Some(nid) = new_node_id {
                    self.hnsw_map_put_txn(snap.current_xid(), idx, nid, out_tid)?;
                }
                if let Some(old_nid) = old_node_id {
                    let action = if vector_changed {
                        PendingHnswAction::Tombstone {
                            node_id: old_nid,
                            tid: index_tid,
                        }
                    } else {
                        PendingHnswAction::MapRemove {
                            node_id: old_nid,
                            tid: index_tid,
                        }
                    };
                    self.pending_hnsw
                        .lock()
                        .entry(snap.current_xid())
                        .or_default()
                        .push(((*idx).clone(), action));
                }
            }
        }
        Ok(out_tid)
    }

    /// Delete the row at `tid` (logical delete: stamps `t_xmax`; single
    /// auto-commit transaction).
    ///
    /// Index maintenance (Stage M wave 3): the row's key is read back BEFORE
    /// the heap delete, and `(key, tid)` is removed from every registered
    /// index afterwards — heap first because its liveness check is the
    /// authoritative validation; an index delete afterwards cannot fail on
    /// a consistent index (its `EntryNotFound` would mean the index and the
    /// table already disagreed). NULL keys are skipped.
    pub fn delete(&self, table: &str, tid: Tid) -> Result<()> {
        self.auto_commit(|snap| {
            self.lock_table_entry(snap.current_xid(), table, LockMode::RowExclusive)?;
            self.delete_inner(snap, table, tid)
        })
    }

    fn delete_inner(&self, snap: &Snapshot, table: &str, tid: Tid) -> Result<()> {
        let entry = self.table_entry(table)?;
        let col_types = column_types(&entry);
        let indexes = self.indexes_of(&entry);
        let hnsw = indexes
            .iter()
            .find(|(idx, _)| matches!(idx.key_type, ColumnType::Vector(_)));
        // Index keys need values, while the trailing NodeId is read from the
        // same physical row version before its xmax is stamped.
        let (old_raw_values, old_node_id) =
            self.read_row_by_tid_with_node_id(&entry, &col_types, tid)?;
        let old_values = self.heap.detoast_values(
            &relation_desc(&entry, &col_types),
            snap,
            self.clog.as_ref(),
            &old_raw_values,
        )?;
        // The entry to remove belongs to the chain root: `tid` itself may be a
        // HOT descendant, which never got an entry of its own.
        let index_tid = if indexes.is_empty() {
            tid
        } else {
            self.hot_chain_root(tid)?
        };
        self.heap.delete(DeleteContext {
            rel: relation_desc(&entry, &col_types),
            snapshot: snap,
            tid,
            clog: self.clog.as_ref(),
        })?;
        for (idx, col_index) in &indexes {
            if matches!(idx.kind, AmKind::Inverted) {
                continue;
            }
            if matches!(idx.key_type, ColumnType::Vector(_)) {
                continue;
            }
            if let Some(old_datum) = &old_values[*col_index] {
                let key = encode_key(old_datum)?;
                self.open_btree(idx)?.delete(&key, index_tid)?;
                self.record_index_undo(
                    snap.current_xid(),
                    idx,
                    key,
                    index_tid,
                    IndexUndoOp::Deleted,
                );
            }
        }
        // M6 Stage C: the graph is NEVER touched at statement time (the
        // row's xmax IS the delete until the commit is durable). The
        // tombstone + map removal are deferred to post-commit.
        if let (Some((idx, _)), Some(old_nid)) = (hnsw, old_node_id) {
            self.pending_hnsw
                .lock()
                .entry(snap.current_xid())
                .or_default()
                .push((
                    (*idx).clone(),
                    PendingHnswAction::Tombstone {
                        node_id: old_nid,
                        tid: index_tid,
                    },
                ));
        }
        Ok(())
    }

    /// Vacuum `table`: reclaim dead-tuple space end to end (M3 Stage D,
    /// tech-selection §4.1). Returns per-pass [`VacuumStats`].
    ///
    /// Five phases, in the §4.1 invariant order — never reorder them
    /// (a TID-invalidating heap record must become WAL-durable only AFTER
    /// that TID's last `BTreeDelete`):
    ///
    /// 1. Take the table's `AccessExclusive` lock under a short-lived
    ///    **maintenance XID**, auto-commit style — the `create_table` /
    ///    `drop_table` precedent: `Self::auto_commit` allocates the XID,
    ///    enters the active set, and routes both the success and the
    ///    failure path through `release_all(xid)`, so the lock's lifetime
    ///    is the maintenance transaction's. Then take the vacuum horizon
    ///    ONCE via [`Self::oldest_snapshot_xmin`] and use it for the whole
    ///    pass (§3.3: taken after the lock wait, so every lock-holding
    ///    transaction has ended; lock-free readers in flight are covered
    ///    through the snapshot registry, and XID monotonicity keeps every
    ///    FUTURE snapshot's xmin ≥ this horizon, so nothing taken later
    ///    can observe a version reclaimed below it).
    /// 2. `scan_dead_tuples(horizon)` — the dead-TID list.
    /// 3. `collect_index_keys` — READ-ONLY key extraction. Must precede
    ///    phase 5: once a page is compacted the keys are unreadable.
    /// 4. Push-mode index cleanup: per `(tid, values)` × per registered
    ///    index on the table, encode the indexed column (NULL skipped, the
    ///    `delete_inner` convention) and `BTreeIndex::delete(key, tid)`
    ///    (the Stage Q online path, `BTreeDelete` WAL included). An HNSW
    ///    index is not opened as a btree — its graph is untouched here —
    ///    but its sidecar NodeId→ctid map IS cleaned in this phase: each
    ///    dead chain root's trailing NodeId keys the map entry to delete
    ///    (M6 Stage D round-8 P1: the map must not outlive the page its
    ///    entries point at — phase 5 hands freed pages to ANY relation,
    ///    and a cross-tenant row with a colliding NodeId passes the search
    ///    pipeline's in-row re-check).
    ///    **`EntryNotFound` is Ok here and only here** (§4.3): eager
    ///    online maintenance already removed the entries of ordinary
    ///    committed deletes/updates, so the entries this phase actually
    ///    removes are essentially crash-loser dangling ones (plus
    ///    watermark-deferred map residue). The btree's own delete contract
    ///    is NOT weakened. Vacuum records no index
    ///    undo: its btree deletes are physical, idempotent
    ///    (EntryNotFound-tolerant) maintenance, never to be reverse-applied
    ///    even when a later phase fails.
    /// 5. `reclaim` — page compaction (`HeapCleanup` WAL) + empty-page
    ///    unlink + `free_page` (`PageFree` WAL). `reclaim` re-derives the
    ///    kill set from `dead` internally (Stage C), so partially-dead HOT
    ///    chains are structurally untouched.
    ///
    /// # Deadlock freedom (S3)
    ///
    /// While WAITING for `AccessExclusive` vacuum holds no lock at all —
    /// the waiting node has in-degree 0 in the wait-for graph (nobody waits
    /// on a lock vacuum has not yet been granted), so no cycle can pass
    /// through it; the detector sees an ordinary wait. Once granted, vacuum
    /// acquires no second table lock (phases 2–5 take only short-lived page
    /// latches).
    ///
    /// Fails with [`EngineError::TableNotFound`] if `table` does not exist
    /// (including when it was dropped while vacuum queued on the lock —
    /// the `lock_table_entry` post-lock re-check).
    pub fn vacuum(&self, table: &str) -> Result<VacuumStats> {
        self.auto_commit(|snap| {
            // Phase 1: AccessExclusive under the maintenance XID, then the
            // single horizon for the whole pass.
            let entry =
                self.lock_table_entry(snap.current_xid(), table, LockMode::AccessExclusive)?;
            let horizon = self.oldest_snapshot_xmin();
            let col_types = column_types(&entry);
            let rel = relation_desc(&entry, &col_types);

            // Phase 2: collect the dead-TID list at the pass's horizon.
            let dead = self
                .heap
                .scan_dead_tuples(rel, horizon, self.clog.as_ref())?;
            let mut stats = VacuumStats {
                dead_tuples: dead.len(),
                ..VacuumStats::default()
            };
            if dead.is_empty() {
                // A crash may leave chunks whose main-row WAL never landed.
                // They still need the reference-driven TOAST pass.
                stats.toast_chunks_reclaimed = self.heap.vacuum_toast(&rel)?;
                return Ok(stats);
            }

            // Phase 3: read-only key extraction, BEFORE any physical rewrite.
            // Dead/loser rows cannot use query-snapshot detoast; their unique
            // valueids are resolved by the physical maintenance path instead.
            let keys = self
                .heap
                .collect_index_keys(rel, &dead)?
                .into_iter()
                .map(|(tid, values)| {
                    self.heap
                        .detoast_values_physical(&rel, &values)
                        .map(|values| (tid, values))
                        .map_err(EngineError::from)
                })
                .collect::<Result<Vec<_>>>()?;
            stats.index_keys = keys.len();

            // Phase 4: push-mode index cleanup, one descent per (key, tid)
            // per index (batch interfaces are Phase 5b, §4.3).
            for (idx, col_index) in &self.indexes_of(&entry) {
                // Inverted indexes are reclaimed by segment merge, never by
                // opening their metadata as a B+Tree.
                if matches!(idx.kind, AmKind::Inverted) {
                    continue;
                }
                // M6 Stage C (round-5 P1): an HNSW index is not a B+Tree —
                // opening its meta page as one yields a fake "no root
                // record" corruption. The graph's orphan-node reclamation
                // is Stage E's audit-driven channel, not this loop.
                if matches!(idx.key_type, ColumnType::Vector(_)) {
                    // M6 Stage D (round-8 P1): the sidecar NodeId→ctid map
                    // obeys the SAME §4.1 phase order as every btree index
                    // — each dead chain root still carries its trailing
                    // NodeId, so its map entry is deleted HERE, before
                    // phase 5 frees the page. A residual entry outliving
                    // its page resolves to whatever row the global freelist
                    // hands the page to next — possibly ANOTHER TABLE's row
                    // with a colliding NodeId, which the in-row NodeId
                    // re-check cannot tell apart (round-8 probe: table a's
                    // ANN search returned table b's row). What remains for
                    // this pass is watermark-deferred or crash-skipped
                    // residue — eager post-commit cleanup took the ordinary
                    // entries — so `EntryNotFound` is tolerated exactly like
                    // the btree path below (§4.3).
                    //
                    // M6 Stage E (slice E-2, §9 死行驱动补刀): the tombstone
                    // half of the same channel — commit-crash / failure-
                    // skipped post-commit cleanup converges here. The
                    // discrimination (round-8's registered discipline): a
                    // non-HOT non-vector UPDATE reuses the SAME NodeId for
                    // the live new version, so tombstone ONLY when no map
                    // entry names a version live at this pass's horizon.
                    // Order mirrors post-commit (tombstone first, then the
                    // map entry): a crash between them converges on the
                    // next pass — both operations are idempotent /
                    // EntryNotFound-tolerant.
                    if let Some(mut map) = self.open_hnsw_map(idx.index_oid)? {
                        let handle = self.hnsw_handle(idx)?;
                        for (tid, _) in &keys {
                            // The chain root still lies unread on its page
                            // (phase 5 has not run); re-read it for just
                            // the trailing NodeId. `None` (no system column,
                            // e.g. a NULL vector at insert) means no map
                            // entry exists.
                            let Some(node_id) = self.read_node_id_by_tid(&col_types, *tid)? else {
                                continue;
                            };
                            let key = encode_key(&Datum::Int8(i64::from(node_id)))?;
                            let mut live_version = false;
                            for ctid in map.lookup_all(&key)? {
                                if ctid == *tid {
                                    continue; // the dead chain root itself
                                }
                                if !self.tuple_dead_at_horizon(ctid, horizon)? {
                                    live_version = true;
                                    break;
                                }
                            }
                            if !live_version {
                                match handle.delete(
                                    self.storage.buffer_pool(),
                                    self.storage.wal_writer(),
                                    pg_am_hnsw::NodeId(node_id),
                                ) {
                                    Ok(()) => {}
                                    // INITIALIZING crash residue — `delete`
                                    // refuses it by contract; the §8.3
                                    // orphan accounting owns it, not this
                                    // pass. Never loud.
                                    Err(pg_am_hnsw::HnswError::InvalidOperation(_)) => {}
                                    Err(e) => return Err(e.into()),
                                }
                            }
                            match map.delete(&key, *tid) {
                                Ok(()) => stats.index_entries_removed += 1,
                                // §4.3: eager maintenance removed it long ago.
                                Err(BTreeError::EntryNotFound) => {
                                    stats.index_entries_already_gone += 1;
                                }
                                Err(e) => return Err(e.into()),
                            }
                        }
                    }
                    continue;
                }
                let mut index = self.open_btree(idx)?;
                for (tid, values) in &keys {
                    // NULL keys carry no entry (the online convention).
                    let Some(datum) = &values[*col_index] else {
                        continue;
                    };
                    let key = encode_key(datum)?;
                    match index.delete(&key, *tid) {
                        Ok(()) => stats.index_entries_removed += 1,
                        // §4.3: eager maintenance removed it long ago.
                        Err(BTreeError::EntryNotFound) => {
                            stats.index_entries_already_gone += 1;
                        }
                        Err(e) => return Err(e.into()),
                    }
                }
            }

            // Phase 5: compaction + unlink + free (kill set re-derived
            // from `dead` inside reclaim — do not pre-filter it here).
            self.heap.reclaim(rel, &dead)?;
            // TOAST follows main-row reclamation: any pointer still present
            // in a physical old/HOT version conservatively pins its group.
            stats.toast_chunks_reclaimed = self.heap.vacuum_toast(&rel)?;
            Ok(stats)
        })
    }

    /// Every index registered on `entry`'s table, joined with the position
    /// of its indexed column in the table's schema.
    pub(crate) fn indexes_of(&self, entry: &TableEntry) -> Vec<(IndexEntry, usize)> {
        self.indexes
            .read()
            .iter()
            .filter(|e| e.table_oid == entry.oid)
            .filter_map(|e| {
                entry
                    .columns
                    .iter()
                    .position(|c| c.name == e.column)
                    .map(|pos| (e.clone(), pos))
            })
            .collect()
    }

    /// Open the B+Tree handle for a registry index entry.
    fn open_btree(&self, idx: &IndexEntry) -> Result<pg_am_btree::BTreeIndex> {
        if !matches!(idx.kind, AmKind::BTree) {
            return Err(EngineError::InvalidArgument(format!(
                "index {:?} is {:?}, not a B+Tree",
                idx.index_oid, idx.kind
            )));
        }
        let btree = BTreeAM::new(
            Arc::clone(self.storage.buffer_pool()),
            Arc::clone(self.storage.wal_writer()),
        );
        Ok(btree.open_index(idx.index_oid, idx.meta_page, idx.key_type)?)
    }

    /// Append one index maintenance op to the per-transaction undo log
    /// (Stage O review; see the `index_undo` field docs). Keyed by the
    /// snapshot's own XID, which is the auto-commit XID or the explicit
    /// transaction's XID depending on the caller — both paths route their
    /// abort through the same log.
    fn record_index_undo(
        &self,
        xid: TxnId,
        idx: &IndexEntry,
        key: Vec<u8>,
        tid: Tid,
        op: IndexUndoOp,
    ) {
        self.index_undo
            .lock()
            .entry(xid)
            .or_default()
            .push(IndexUndo {
                index: idx.clone(),
                key,
                tid,
                op,
            });
    }

    /// Read and detoast the addressed row version under the caller's snapshot.
    /// Visibility of the main row is judged by the caller's scan/search path;
    /// this helper resolves only the pointers carried by that exact version.
    pub(crate) fn read_row_by_tid(
        &self,
        snap: &Snapshot,
        entry: &TableEntry,
        col_types: &[ColumnType],
        tid: Tid,
    ) -> Result<Vec<Value>> {
        let (values, _) = self.read_row_by_tid_with_node_id(entry, col_types, tid)?;
        Ok(self.heap.detoast_values(
            &relation_desc(entry, col_types),
            snap,
            self.clog.as_ref(),
            &values,
        )?)
    }

    /// Physical detoast for recovery's loser-index compensation. Aborted
    /// chunks cannot be read through a query snapshot, but their exact valueid
    /// is still needed to restore an index delete performed by that loser.
    fn read_row_by_tid_physical(
        &self,
        entry: &TableEntry,
        col_types: &[ColumnType],
        tid: Tid,
    ) -> Result<Vec<Value>> {
        let (values, _) = self.read_row_by_tid_with_node_id(entry, col_types, tid)?;
        self.heap
            .detoast_values_physical(&relation_desc(entry, col_types), &values)
            .map_err(EngineError::from)
    }

    /// Read back a row's values AND its trailing NodeId (M6 Stage C):
    /// `None` for a row without the system column (no HNSW index, or a
    /// NULL vector at insert time).
    fn read_row_by_tid_with_node_id(
        &self,
        entry: &TableEntry,
        col_types: &[ColumnType],
        tid: Tid,
    ) -> Result<(Vec<Value>, Option<u32>)> {
        let guard = self.storage.buffer_pool().pin(tid.page_id)?;
        let page: &[u8; PAGE_SIZE] = guard.page().try_into().expect("frame is PAGE_SIZE");
        let bytes = SlottedPage::tuple(page, tid.slot_id)?.ok_or_else(|| {
            EngineError::Corrupted(format!("no tuple at {tid} for index key readback"))
        })?;
        let (header, values) = decode_tuple(bytes, col_types)?;
        // `decode_tuple` already validated the exact-4-byte tail — read the
        // NodeId directly rather than paying a SECOND full decode through
        // `tuple_node_id` (round-8 nano; this sits on the search hot path).
        let node_id = if header.t_infomask2 & pg_am_heap::tuple::HEAP_HAS_NODEID != 0 {
            let n = bytes.len();
            Some(u32::from_le_bytes(
                bytes[n - 4..n]
                    .try_into()
                    .expect("decode_tuple validated the 4-byte tail"),
            ))
        } else {
            None
        };
        debug_assert_eq!(values.len(), entry.columns.len());
        Ok((values, node_id))
    }

    /// Read ONLY the trailing NodeId of the row at `tid` — vacuum's dead-row
    /// channel (M6 Stage D round-8): phase 3's `collect_index_keys` decoded
    /// the tuple's VALUES for the btree path, but the sidecar map's key is
    /// the system column, so the dead chain root is re-read once here. Runs
    /// in vacuum phase 4, strictly before `reclaim` makes the bytes
    /// unreadable.
    fn read_node_id_by_tid(&self, col_types: &[ColumnType], tid: Tid) -> Result<Option<u32>> {
        let guard = self.storage.buffer_pool().pin(tid.page_id)?;
        let page: &[u8; PAGE_SIZE] = guard.page().try_into().expect("frame is PAGE_SIZE");
        let bytes = SlottedPage::tuple(page, tid.slot_id)?.ok_or_else(|| {
            EngineError::Corrupted(format!("no tuple at {tid} for hnsw map key readback"))
        })?;
        Ok(pg_am_heap::tuple::tuple_node_id(bytes, col_types)?)
    }

    /// Whether the row at `tid` is reclaimable-dead at `horizon` — the
    /// exact rule `scan_dead_tuples` applies (M3 §3.2): an aborted
    /// inserter, or a committed deleter older than the horizon (the
    /// `HEAP_XMAX_LOCK_ONLY` mask included — a row lock is not a delete).
    /// Vacuum's HNSW dead-row channel uses it for the Stage E tombstone
    /// discrimination: "no live version carries this NodeId" vs "an UPDATE
    /// reused the NodeId for a live version". An in-flight row reads as
    /// NOT dead — its transaction will decide, and this pass must not
    /// tombstone under it.
    fn tuple_dead_at_horizon(&self, tid: Tid, horizon: TxnId) -> Result<bool> {
        let guard = self.storage.buffer_pool().pin(tid.page_id)?;
        let page: &[u8; PAGE_SIZE] = guard.page().try_into().expect("frame is PAGE_SIZE");
        // A reclaimed/empty slot is as dead as it gets.
        let Some(bytes) = SlottedPage::tuple(page, tid.slot_id)? else {
            return Ok(true);
        };
        let header = TupleHeader::read_from(&bytes[..pg_am_heap::tuple::TUPLE_HEADER_SIZE])?;
        if self.clog.get_state(header.t_xmin) == pg_storage::clog::TxnState::Aborted {
            return Ok(true);
        }
        let xmax = if header.t_infomask & pg_am_heap::tuple::HEAP_XMAX_LOCK_ONLY != 0 {
            TxnId::INVALID
        } else {
            header.t_xmax
        };
        Ok(xmax != TxnId::INVALID
            && xmax.0 < horizon.0
            && self.clog.get_state(xmax) == pg_storage::clog::TxnState::Committed)
    }

    /// Find the first WAL record whose start lies inside segment `segment_id`
    /// by probing 8-byte-aligned offsets from the segment start with a
    /// CRC-checked decode (WAL records may straddle segment boundaries, so a
    /// segment start is NOT itself a record boundary). Used by the loser index
    /// compensation to extend its scan to the front of the retained segment.
    /// Returns `None` when nothing decodes within one maximum record length of
    /// the segment start, or when the first record starts at/after
    /// `upper_bound` (nothing to gain by rewinding).
    fn first_record_lsn_in_segment(
        wal_dir: &Path,
        segment_size: u64,
        segment_id: u64,
        upper_bound: Lsn,
    ) -> Option<Lsn> {
        use std::io::{Read, Seek, SeekFrom};
        // pg-storage wal/segment.rs filename layout: wal-{segment_id + 1:08}.log.
        let path = wal_dir.join(format!("wal-{:08}.log", segment_id + 1));
        let mut file = std::fs::File::open(path).ok()?;
        const HEADER: u64 = 32; // wal/record.rs fixed record header
        const MAX_RECORD: u64 = HEADER + u16::MAX as u64 + 8; // length prefix is u16
                                                              // The segment FILE holds the segment's bytes at file offset 0 (file
                                                              // offset = global LSN - segment start).
        let seg_start = segment_id * segment_size;
        file.seek(SeekFrom::Start(0)).ok()?;
        // Probe window: one max record for a straddling record's tail, plus one
        // max record so the candidate itself can be decoded fully.
        let window_len = (2 * MAX_RECORD).min(segment_size);
        let mut window = Vec::new();
        file.by_ref()
            .take(window_len)
            .read_to_end(&mut window)
            .ok()?;
        // WAL records are 8-byte aligned (pg-storage LSN_ALIGNMENT); the
        // resync probe steps by it.
        const PROBE_STEP: usize = 8;
        let mut off = 0usize;
        while off as u64 <= MAX_RECORD && off + HEADER as usize <= window.len() {
            let payload_len =
                u16::from_le_bytes(window[off + 26..off + 28].try_into().unwrap()) as u64;
            let total = (HEADER + payload_len).next_multiple_of(8);
            let end = off + total as usize;
            if end <= window.len() {
                if let Ok((rec, _)) = WalRecord::decode(&window[off..end]) {
                    // CRC passed; the position check pins the candidate to its
                    // header LSN, ruling out a coincidental decode of tail bytes.
                    if rec.lsn.0 == seg_start + off as u64 {
                        let found = Lsn(seg_start + off as u64);
                        return (found < upper_bound).then_some(found);
                    }
                }
            }
            off += PROBE_STEP;
        }
        None
    }

    /// Recovery-time index compensation for loser transactions (see the
    /// `Engine::open` step 7c note). Scans the post-redo-point WAL for
    /// `HeapDelete` / non-HOT `HeapUpdate` records of transactions that
    /// never reached a terminal record, and re-inserts the index entries
    /// those records physically removed. The removal is replayed
    /// unconditionally by redo (index records carry xid=0), while the heap
    /// side stays visible through the CLOG — without this step a committed,
    /// visible row can lose its index entry whenever a crash lands inside
    /// an in-flight delete/update.
    ///
    /// Idempotent: an entry is re-inserted only if the exact `(key, tid)`
    /// pair is absent, so a crash mid-compensation re-runs cleanly, and a
    /// second recovery after a clean one is a no-op.
    ///
    /// Boundary (documented): a loser whose delete/update record sits in a
    /// WAL segment that checkpoint recycling has already dropped is not
    /// compensable from the retained WAL. The scan rewinds to the first
    /// record starting inside the redo start's segment (CRC-checked
    /// resync), so the record must predate every segment that survived the
    /// last checkpoint — only reachable with hand-held multi-checkpoint
    /// transactions.
    fn compensate_loser_index_entries(&self, data_dir: &Path) -> Result<()> {
        let losers: HashSet<TxnId> = self
            .storage
            .recovered_active_xids()
            .iter()
            .copied()
            .collect();
        if losers.is_empty() {
            return Ok(());
        }

        // Collect the loser transactions' victim row versions: each of
        // these records physically removed the version's index entries.
        //
        // Scan-start choice (post-Stage-T review): the loser's delete may
        // predate `recovered_redo_start` — the redo start is pulled back
        // only for pages still DIRTY at the checkpoint's DPT sample, so a
        // victim page flushed earlier (e.g. evicted under pool pressure)
        // anchors nothing, and its delete record can sit in the same
        // retained segment but BEFORE the redo start. WAL records may
        // straddle segments, so a bare segment start is NOT a usable
        // boundary; instead the scan rewinds to the first record starting
        // inside the redo start's segment, found by a CRC-checked resync
        // probe (`first_record_lsn_in_segment`). Once recycling has dropped
        // the segment holding a loser's delete, the shape is no longer
        // compensable from the retained WAL — the documented boundary (see
        // fn doc).
        let seg = self.storage.config().wal_segment_size;
        let redo_start = self.storage.recovered_redo_start();
        let wal_dir = data_dir.join("wal");
        // A redo start in segment 0 scans from Lsn::FIRST: records begin at
        // offset 8 there, not at the segment's byte 0.
        let scan_start = if redo_start.segment_id(seg) == 0 {
            Lsn::FIRST
        } else {
            redo_start
        };
        // Recovery's own replay opened at redo_start successfully, so this
        // segment necessarily survives recycling.
        let mut reader = WalReader::open_at(&wal_dir, seg, scan_start)?;
        if scan_start != Lsn::FIRST {
            if let Some(first) = Self::first_record_lsn_in_segment(
                &wal_dir,
                seg,
                scan_start.segment_id(seg),
                scan_start,
            ) {
                reader = WalReader::open_at(&wal_dir, seg, first)?;
            }
        }
        let mut victim_tids: Vec<Tid> = Vec::new();
        while let Some(rec) = reader.next_record()? {
            if !losers.contains(&rec.txn_id) {
                continue;
            }
            match rec.record_type {
                WalRecordType::HeapDelete => {
                    victim_tids.push(HeapDeleteRecord::decode(&rec.payload)?.tid);
                }
                WalRecordType::HeapUpdate => {
                    // HeapHotUpdate never touches the index (key columns
                    // unchanged by definition); a non-HOT HeapUpdate is
                    // always paired with index delete+insert online.
                    victim_tids.push(HeapUpdateRecord::decode(&rec.payload)?.old_tid);
                }
                _ => {}
            }
        }
        if victim_tids.is_empty() {
            return Ok(());
        }
        victim_tids.sort_by_key(|t| (t.page_id.0, t.slot_id));
        victim_tids.dedup();

        // page → owning table map (chain walk per live table).
        let mut tables: Vec<(TableEntry, Vec<ColumnType>, HashSet<PageId>)> = Vec::new();
        {
            let registry = self.registry.read();
            for entry in registry.values() {
                let col_types = column_types(entry);
                let pages: HashSet<PageId> = self
                    .heap
                    .relation_pages(&relation_desc(entry, &col_types))?
                    .into_iter()
                    .collect();
                tables.push((entry.clone(), col_types, pages));
            }
        }

        let mut compensated = 0usize;
        for tid in victim_tids {
            let Some((entry, col_types, _)) = tables
                .iter()
                .find(|(_, _, pages)| pages.contains(&tid.page_id))
            else {
                // The page belongs to no live table (dropped-table remnant
                // or freelist reuse) — nothing live can reference the row.
                tracing::warn!(
                    page = tid.page_id.0,
                    slot = tid.slot_id,
                    "loser index compensation: no live table owns this page; skipping"
                );
                continue;
            };
            let indexes = self.indexes_of(entry);
            if indexes.is_empty() {
                continue;
            }
            // The row version's bytes are intact on its page (redo at most
            // stamped the loser's xmax). Same chain-root rule as the online
            // delete/update paths: HOT descendants never own entries.
            let old_values = self.read_row_by_tid_physical(entry, col_types, tid)?;
            let index_tid = self.hot_chain_root(tid)?;
            for (idx, col_index) in &indexes {
                // HNSW and inverted have their own recovery channels; never
                // compensate them through this B+Tree-only loop.
                if matches!(idx.kind, AmKind::Inverted) {
                    continue;
                }
                if matches!(idx.key_type, ColumnType::Vector(_)) {
                    continue;
                }
                let Some(old_datum) = &old_values[*col_index] else {
                    continue; // NULL keys carry no entry (online rule)
                };
                let key = encode_key(old_datum)?;
                let mut index = self.open_btree(idx)?;
                if index.lookup_all(&key)?.contains(&index_tid) {
                    continue; // idempotent re-run
                }
                index.insert_with_budget(&key, index_tid, UNDO_INSERT_RESTART_BUDGET)?;
                compensated += 1;
            }
        }
        if compensated > 0 {
            tracing::warn!(
                compensated,
                losers = losers.len(),
                "recovery compensated loser transactions' index deletes \
                 (crash inside an in-flight delete/update)"
            );
        }
        Ok(())
    }

    /// Begin an explicit transaction, returning a [`TxnHandle`] for
    /// commit/abort control (§21 M2b API). The snapshot is taken at this
    /// point (SI isolation); `curcid` starts at 0 and is advanced by the
    /// executor before each SQL statement.
    ///
    /// No commit-barrier guard is taken at begin (M2c Stage P): the barrier
    /// protects the commit/abort hard order against the checkpoint's CLOG
    /// flush, and `TxnManager::commit_txn`/`abort_txn` now guard themselves.
    /// Begin + snapshot only read the active set, which checkpoints never
    /// mutate. Begin also takes NO table locks: the first statement touching
    /// a table acquires what it needs (`lock_table`), and commit/abort/Drop
    /// release everything (`LockManager::release_all`).
    pub fn begin_txn(&self) -> Result<TxnHandle> {
        let xid = self.txn.begin_txn();
        let (snapshot, snapshot_guard) = self.txn.snapshot(xid);
        Ok(TxnHandle {
            txn: Arc::clone(&self.txn),
            xid: Some(xid),
            snapshot: RefCell::new(snapshot),
            _snapshot_guard: Some(snapshot_guard),
            instance_id: self.instance_id,
            lock_manager: Arc::clone(&self.lock_manager),
            index_undo: Arc::clone(&self.index_undo),
            buffer_pool: Arc::clone(self.storage.buffer_pool()),
            wal_writer: Arc::clone(self.storage.wal_writer()),
            hnsw_handles: Arc::clone(&self.hnsw_handles),
            pending_hnsw: Arc::clone(&self.pending_hnsw),
            #[cfg(test)]
            inverted_commit_finished: Arc::clone(&self.inverted_commit_finished),
        })
    }

    /// Execute a SQL statement (§21 M2b API).
    ///
    /// `txn = None` runs in auto-commit mode; `txn = Some(h)` runs inside an
    /// explicit transaction (use [`Engine::begin_txn`] to create one).
    /// BEGIN/COMMIT/ROLLBACK as SQL text are rejected: transaction control
    /// is programmatic only ([`Engine::begin_txn`], [`TxnHandle::commit`],
    /// [`TxnHandle::abort`]).
    ///
    /// # No statement-level rollback (M2b)
    ///
    /// There are no subtransactions: if a statement inside an explicit
    /// transaction fails mid-way, the rows it already wrote REMAIN in the
    /// transaction, and the only safe follow-up is [`TxnHandle::abort`].
    ///
    /// # Row locks: `SELECT ... FOR UPDATE` (M2c Stage P)
    ///
    /// A SELECT with a `FOR UPDATE` clause takes the table's `RowExclusive`
    /// lock and stamps every result row with a lock-only `t_xmax` (§9.1);
    /// concurrent writers/lockers of those rows block until this
    /// transaction ends. In auto-commit mode the locks are stamped with the
    /// statement's own short-lived transaction and released when it
    /// commits — allowed, but only meaningful inside an explicit
    /// transaction (same as PG). `FOR SHARE` (M2c Stage S "multixact
    /// lite") likewise takes `RowExclusive` and stamps each result row
    /// with a shared row lock (`HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_IS_SHARE`);
    /// the row stays visible to all snapshots.
    ///
    /// # Query statistics (M3 Stage E, tech-selection §6.3)
    ///
    /// Every successfully parsed statement records one
    /// [`QueryStatEntry`] into
    /// [`Self::query_stats`] — auto-commit and explicit-transaction paths
    /// alike, success or failure — making this the single instrumentation
    /// point for the SQL surface. Parse failures return before the probe
    /// and are not recorded. The typed API (`scan` / `insert` / `update` /
    /// `delete` / `index_lookup`) never passes through here and produces
    /// NO entries (§6.3 另注).
    pub fn exec(&self, txn: Option<&TxnHandle>, sql: &str) -> Result<QueryResult> {
        let mut session = SqlSession::new(NEXT_SQL_SESSION_ID.fetch_add(1, Ordering::Relaxed));
        self.exec_in_session(&mut session, txn, sql)
    }

    /// Execute SQL with a persistent per-connection sequence context.
    pub fn exec_in_session(
        &self,
        session: &mut SqlSession,
        txn: Option<&TxnHandle>,
        sql: &str,
    ) -> Result<QueryResult> {
        let started = Instant::now();
        let stmt = sql::parse(sql)?;
        self.exec_parsed(session, txn, sql, started, stmt, None)
    }

    /// Execute an AP query synchronously without nesting a runtime on the current thread.
    #[cfg(feature = "datafusion")]
    pub fn exec_datafusion(
        self: &Arc<Self>,
        txn: Option<&TxnHandle>,
        sql: &str,
    ) -> Result<QueryResult> {
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => match handle.runtime_flavor() {
                tokio::runtime::RuntimeFlavor::MultiThread => tokio::task::block_in_place(|| {
                    handle.block_on(self.exec_datafusion_inner(txn, sql))
                }),
                tokio::runtime::RuntimeFlavor::CurrentThread => {
                    Err(EngineError::Unsupported(
                        "synchronous DataFusion execution inside a current-thread Tokio runtime is not supported; use exec_datafusion_async".to_string(),
                    ))
                }
                _ => Err(EngineError::Unsupported(
                    "synchronous DataFusion execution is not supported in this Tokio runtime".to_string(),
                )),
            },
            Err(_) => {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|error| {
                        EngineError::InvalidArgument(format!(
                            "cannot create DataFusion runtime: {error}"
                        ))
                    })?;
                runtime.block_on(self.exec_datafusion_inner(txn, sql))
            }
        }
    }

    /// Execute an AP query directly in the current async task.
    #[cfg(feature = "datafusion")]
    pub async fn exec_datafusion_async(
        self: &Arc<Self>,
        txn: Option<&TxnHandle>,
        sql: &str,
    ) -> Result<QueryResult> {
        self.exec_datafusion_inner(txn, sql).await
    }

    #[cfg(feature = "datafusion")]
    async fn exec_datafusion_inner(
        self: &Arc<Self>,
        txn: Option<&TxnHandle>,
        sql: &str,
    ) -> Result<QueryResult> {
        if let Some(handle) = txn {
            if handle.instance_id != self.instance_id {
                return Err(EngineError::InvalidArgument(
                    "transaction handle belongs to a different Engine instance".to_string(),
                ));
            }
        }
        let started = Instant::now();
        let stmt = match sql::parse(sql) {
            Ok(statement) => statement,
            Err(_) => Statement::Select {
                columns: SelectCols::All,
                table: String::new(),
                filter: None,
                extra_filter: None,
                order_by: None,
                limit: None,
                lock: None,
            },
        };
        let result = crate::datafusion_provider::execute_datafusion(self, txn, sql).await;
        if self.query_stats.capacity() > 0 {
            let rows = match &result {
                Ok(QueryResult::Rows { rows, .. }) => rows.len(),
                Ok(QueryResult::Affected(n)) => *n,
                _ => 0,
            };
            self.query_stats.record(QueryStatEntry {
                query: sql.to_string(),
                latency: started.elapsed(),
                rows,
                path: ExecutionPath::of(&stmt),
                timestamp: SystemTime::now(),
            });
        }
        result
    }

    /// DataFusion AP is opt-in. Without the feature the request is rejected.
    #[cfg(not(feature = "datafusion"))]
    pub fn exec_datafusion(
        self: &Arc<Self>,
        _txn: Option<&TxnHandle>,
        _sql: &str,
    ) -> Result<QueryResult> {
        let _ = (self, _txn, _sql);
        Err(EngineError::Unsupported(
            "DataFusion AP path requires the 'datafusion' feature".to_string(),
        ))
    }

    /// Execute a SQL statement with typed `$1..$n` bind parameters.
    ///
    /// Parameters replace [`Literal::Param`] nodes in the parsed AST; they are
    /// never interpolated into SQL text. Int4, Int8, Float8, Text, Vector, and
    /// NULL values are supported. Types must match the inferred column type
    /// exactly, including a vector's dimension; in particular, Text is not
    /// implicitly decoded as Vector. The parameter slice length must equal the
    /// greatest referenced index (unused gaps remain valid, as in PostgreSQL).
    /// Query statistics retain the original SQL text, including placeholders.
    pub fn exec_with_params(
        &self,
        txn: Option<&TxnHandle>,
        sql: &str,
        params: &[Value],
    ) -> Result<QueryResult> {
        let mut session = SqlSession::new(NEXT_SQL_SESSION_ID.fetch_add(1, Ordering::Relaxed));
        self.exec_with_params_in_session(&mut session, txn, sql, params)
    }

    /// Execute parameterized SQL with a persistent per-connection sequence context.
    pub fn exec_with_params_in_session(
        &self,
        session: &mut SqlSession,
        txn: Option<&TxnHandle>,
        sql: &str,
        params: &[Value],
    ) -> Result<QueryResult> {
        let started = Instant::now();
        let stmt = sql::parse(sql)?;
        self.exec_parsed(session, txn, sql, started, stmt, Some(params))
    }

    /// Infer the schema type of every parameter slot in `$1..$max` order.
    ///
    /// A gap or an otherwise uninferable position is returned as `None`.
    /// Repeated parameters must have exactly compatible types; vector types
    /// include their dimension. Statements without parameters return an empty
    /// vector without requiring schema lookup.
    pub fn parameter_types(&self, stmt: &Statement) -> Result<Vec<Option<ColumnType>>> {
        let max_index = sql::max_parameter_index(stmt);
        if max_index == 0 {
            return Ok(Vec::new());
        }
        let mut inference = ParameterInference::new(max_index);
        infer_statement_parameters(self, stmt, &mut inference)?;
        Ok(inference.slots)
    }

    fn exec_parsed(
        &self,
        session: &mut SqlSession,
        txn: Option<&TxnHandle>,
        sql: &str,
        started: Instant,
        stmt: Statement,
        params: Option<&[Value]>,
    ) -> Result<QueryResult> {
        let mut path = ExecutionPath::of(&stmt);
        let result = match params {
            Some(params) => self
                .bind_parameters(stmt, params)
                .and_then(|stmt| match txn {
                    None => self.exec_auto(session, stmt, &mut path),
                    Some(handle) => self.exec_txn(session, handle, stmt, &mut path),
                }),
            None => match txn {
                None => self.exec_auto(session, stmt, &mut path),
                Some(handle) => self.exec_txn(session, handle, stmt, &mut path),
            },
        };
        // F2 (Stage E review): capacity 0 means "stats off" — skip the
        // per-statement String allocation entirely.
        if self.query_stats.capacity() > 0 {
            let rows = match &result {
                Ok(QueryResult::Rows { rows, .. }) => rows.len(),
                Ok(QueryResult::Affected(n)) => *n,
                _ => 0,
            };
            self.query_stats.record(QueryStatEntry {
                query: sql.to_string(),
                latency: started.elapsed(),
                rows,
                path,
                timestamp: SystemTime::now(),
            });
        }
        result
    }

    fn bind_parameters(&self, mut stmt: Statement, params: &[Value]) -> Result<Statement> {
        let inferred = self.parameter_types(&stmt)?;
        if params.len() != inferred.len() {
            let kind = if params.len() < inferred.len() {
                "missing"
            } else {
                "extra"
            };
            return Err(EngineError::InvalidArgument(format!(
                "{kind} parameter values: statement requires {} value(s), but {} were supplied",
                inferred.len(),
                params.len()
            )));
        }
        let literals = params
            .iter()
            .zip(&inferred)
            .enumerate()
            .map(|(index, (value, expected))| parameter_to_literal(index + 1, *expected, value))
            .collect::<Result<Vec<_>>>()?;
        replace_parameters(&mut stmt, &literals)?;
        Ok(stmt)
    }

    fn sequence_query(
        &self,
        session: &mut SqlSession,
        function: SequenceFunction,
        sequence: &str,
    ) -> Result<QueryResult> {
        let (name, value) = match function {
            SequenceFunction::NextVal => ("nextval", self.nextval(session, sequence)?),
            SequenceFunction::CurrVal => ("currval", self.currval(session, sequence)?),
        };
        Ok(QueryResult::Rows {
            columns: vec![name.to_string()],
            rows: vec![vec![Some(Datum::Int8(value))]],
        })
    }

    fn exec_auto(
        &self,
        session: &mut SqlSession,
        stmt: Statement,
        path: &mut ExecutionPath,
    ) -> Result<QueryResult> {
        match stmt {
            // Transaction control is programmatic only (Stage O review):
            // auto-commit mode has no transaction to begin or end, so these
            // must fail loudly instead of silently returning Ok.
            Statement::Begin => Err(EngineError::InvalidArgument(
                "BEGIN via exec is not supported; use Engine::begin_txn()".to_string(),
            )),
            Statement::Commit | Statement::Rollback => Err(EngineError::InvalidArgument(
                "no transaction in progress".to_string(),
            )),
            Statement::CreateSequence { name } => {
                self.create_sequence(&name).map(|_| QueryResult::Ok)
            }
            Statement::SequenceQuery { function, sequence } => {
                self.sequence_query(session, function, &sequence)
            }
            Statement::Explain(inner) => {
                let Statement::Select {
                    table,
                    columns,
                    filter,
                    extra_filter,
                    order_by,
                    limit,
                    ..
                } = *inner
                else {
                    return Err(EngineError::Unsupported(
                        "EXPLAIN currently supports SELECT only".to_string(),
                    ));
                };
                // EXPLAIN plans and estimates only: no inner B+Tree/ANN
                // execution and no FOR UPDATE row-lock stamping. The local
                // guard covers the statistics scan.
                let (mut snap, _guard) = self.txn.snapshot(TxnId::INVALID);
                snap.advance_curcid();
                let entry = self.table_entry(&table)?;
                crate::executor::explain(
                    self,
                    &snap,
                    &table,
                    &entry,
                    &columns,
                    &filter,
                    &extra_filter,
                    &order_by,
                    &limit,
                )
            }
            Statement::CreateTable { name, columns } => {
                self.create_table_from_spec(&name, columns)?;
                Ok(QueryResult::Ok)
            }
            Statement::CreateIndex {
                table,
                column,
                name,
                method,
                opclass,
                fulltext_index,
            } => {
                if method == Some(sql::IndexMethod::Inverted) {
                    if fulltext_index.as_deref().unwrap_or("bm25") != "bm25" {
                        return Err(EngineError::Unsupported(
                            "only fulltext_index='bm25' is supported".to_string(),
                        ));
                    }
                    self.create_inverted_index_on_named(&table, &column, name.as_deref())?;
                    return Ok(QueryResult::Ok);
                }
                if method == Some(sql::IndexMethod::Hnsw) {
                    let opclass = opclass.ok_or_else(|| {
                        EngineError::InvalidArgument(
                            "USING hnsw requires an explicit vector opclass".to_string(),
                        )
                    })?;
                    let metric = match opclass {
                        sql::VectorOpclass::VectorL2Ops => pg_am_hnsw::Metric::L2,
                        sql::VectorOpclass::VectorCosineOps => pg_am_hnsw::Metric::Cosine,
                        sql::VectorOpclass::VectorIpOps => pg_am_hnsw::Metric::InnerProduct,
                    };
                    let entry = self.table_entry(&table)?;
                    let column_def =
                        entry
                            .columns
                            .iter()
                            .find(|c| c.name == column)
                            .ok_or_else(|| {
                                EngineError::InvalidArgument(format!(
                                    "table {table:?} has no column {column:?}"
                                ))
                            })?;
                    let ColumnType::Vector(dim) = column_def.col_type else {
                        return Err(EngineError::InvalidArgument(format!(
                            "hnsw index requires a VECTOR column; {table}({column}) is {:?}",
                            column_def.col_type
                        )));
                    };
                    let dim = u16::try_from(dim).map_err(|_| {
                        EngineError::InvalidArgument(format!(
                            "VECTOR dim {dim} exceeds the HNSW u16 parameter range"
                        ))
                    })?;
                    self.create_hnsw_index_on_named(
                        &table,
                        &column,
                        name.as_deref(),
                        pg_am_hnsw::HnswParams::default(),
                        dim,
                        metric,
                        pg_am_hnsw::NeighborSelection::Heuristic,
                        42,
                    )?;
                    return Ok(QueryResult::Ok);
                }
                if name.is_some() {
                    return Err(EngineError::InvalidArgument(
                        "named BTree index DDL is not yet supported in this execution path"
                            .to_string(),
                    ));
                }
                if opclass.is_some() {
                    return Err(EngineError::InvalidArgument(
                        "a vector opclass is only valid with USING hnsw".to_string(),
                    ));
                }
                self.create_index(&table, &column)?;
                Ok(QueryResult::Ok)
            }
            Statement::Insert {
                table,
                columns,
                rows,
            } => {
                let count = self.auto_commit(|snap| {
                    let entry =
                        self.lock_table_entry(snap.current_xid(), &table, LockMode::RowExclusive)?;
                    let mut count = 0;
                    for row in &rows {
                        let values = self.build_insert_values(session, &entry, &columns, row)?;
                        self.insert_inner(snap, &entry, &values)?;
                        count += 1;
                    }
                    Ok(count)
                })?;
                Ok(QueryResult::Affected(count))
            }
            Statement::Select {
                columns,
                table,
                filter,
                extra_filter,
                order_by,
                limit,
                lock,
            } => {
                match lock {
                    // Plain auto-commit SELECT owns no transaction, so it
                    // takes no table lock — same shape as `Engine::scan`
                    // (see its doc). MVCC makes the read consistent; a
                    // racing DROP TABLE is the documented DDL-vs-DML gap.
                    None => {
                        // `_guard` registers this lock-free reader's
                        // snapshot for the duration of the statement
                        // (M3 Stage A, §3.1/§3.3 — the exact hole the
                        // registry closes).
                        let (mut snap, _guard) = self.txn.snapshot(TxnId::INVALID);
                        snap.advance_curcid();
                        self.exec_select(
                            &snap,
                            &columns,
                            &table,
                            &filter,
                            &extra_filter,
                            &order_by,
                            &limit,
                            false,
                            false,
                            path,
                        )
                    }
                    // FOR UPDATE (M2c Stage P) needs a real transaction:
                    // the row locks are stamped with its XID and the table
                    // lock keys on it; both are released when the
                    // auto-commit transaction ends (matching PG, where a
                    // statement-level FOR UPDATE's locks die with the
                    // statement).
                    Some(LockClause::ForUpdate) => self.auto_commit(|snap| {
                        self.lock_table_entry(snap.current_xid(), &table, LockMode::RowExclusive)?;
                        self.exec_select(
                            snap,
                            &columns,
                            &table,
                            &filter,
                            &extra_filter,
                            &order_by,
                            &limit,
                            true,
                            false,
                            path,
                        )
                    }),
                    // FOR SHARE (Stage S multixact lite): stamps a shared
                    // row lock (HEAP_XMAX_LOCK_ONLY + HEAP_XMAX_IS_SHARE).
                    // The row stays visible to all snapshots.
                    Some(LockClause::ForShare) => self.auto_commit(|snap| {
                        self.lock_table_entry(snap.current_xid(), &table, LockMode::RowExclusive)?;
                        self.exec_select(
                            snap,
                            &columns,
                            &table,
                            &filter,
                            &extra_filter,
                            &order_by,
                            &limit,
                            true,
                            true,
                            path,
                        )
                    }),
                }
            }
            Statement::Update {
                table,
                sets,
                filter,
            } => {
                let count = self.auto_commit(|snap| {
                    let entry =
                        self.lock_table_entry(snap.current_xid(), &table, LockMode::RowExclusive)?;
                    let pred = filter
                        .as_ref()
                        .map(|f| {
                            filter_to_predicate_with_analyzer(
                                &entry,
                                f,
                                &self.inverted_analyzer_for(entry.oid, &f.column),
                            )
                        })
                        .transpose()?;
                    let rows = self.scan_inner(snap, &table, pred.as_ref())?;
                    let mut count = 0;
                    for (tid, old_values) in rows {
                        let new_values = apply_sets(&entry, &old_values, &sets)?;
                        self.update_inner(snap, &table, tid, &new_values)?;
                        count += 1;
                    }
                    Ok(count)
                })?;
                Ok(QueryResult::Affected(count))
            }
            Statement::Delete { table, filter } => {
                let count = self.auto_commit(|snap| {
                    let entry =
                        self.lock_table_entry(snap.current_xid(), &table, LockMode::RowExclusive)?;
                    let pred = filter
                        .as_ref()
                        .map(|f| {
                            filter_to_predicate_with_analyzer(
                                &entry,
                                f,
                                &self.inverted_analyzer_for(entry.oid, &f.column),
                            )
                        })
                        .transpose()?;
                    let rows = self.scan_inner(snap, &table, pred.as_ref())?;
                    let mut count = 0;
                    for (tid, _) in rows {
                        self.delete_inner(snap, &table, tid)?;
                        count += 1;
                    }
                    Ok(count)
                })?;
                Ok(QueryResult::Affected(count))
            }
        }
    }

    fn exec_txn(
        &self,
        session: &mut SqlSession,
        handle: &TxnHandle,
        stmt: Statement,
        path: &mut ExecutionPath,
    ) -> Result<QueryResult> {
        // A handle is bound to its creating engine (Stage O review): using
        // it against another instance would run statements against the wrong
        // registry while writing through the original engine's txn manager.
        if handle.instance_id != self.instance_id {
            return Err(EngineError::InvalidArgument(
                "transaction handle belongs to a different Engine instance".to_string(),
            ));
        }
        handle.advance_curcid();
        let snap = handle.snapshot.borrow();
        match stmt {
            Statement::Begin => Err(EngineError::InvalidArgument(
                "nested BEGIN is not supported; already in an explicit transaction".to_string(),
            )),
            Statement::Commit => Err(EngineError::InvalidArgument(
                "COMMIT via exec is not supported; call TxnHandle::commit() instead".to_string(),
            )),
            Statement::Rollback => Err(EngineError::InvalidArgument(
                "ROLLBACK via exec is not supported; call TxnHandle::abort() instead".to_string(),
            )),
            Statement::SequenceQuery { function, sequence } => {
                self.sequence_query(session, function, &sequence)
            }
            Statement::Explain(inner) => {
                let Statement::Select {
                    table,
                    columns,
                    filter,
                    extra_filter,
                    order_by,
                    limit,
                    ..
                } = *inner
                else {
                    return Err(EngineError::Unsupported(
                        "EXPLAIN currently supports SELECT only".to_string(),
                    ));
                };
                let entry = self.lock_table_entry(handle.xid(), &table, LockMode::AccessShare)?;
                crate::executor::explain(
                    self,
                    &snap,
                    &table,
                    &entry,
                    &columns,
                    &filter,
                    &extra_filter,
                    &order_by,
                    &limit,
                )
            }
            Statement::CreateTable { .. }
            | Statement::CreateIndex { .. }
            | Statement::CreateSequence { .. } => {
                drop(snap);
                Err(EngineError::InvalidArgument(
                    "DDL inside explicit transactions is not supported in M2b; run DDL in auto-commit mode (exec(None, ...))".to_string(),
                ))
            }
            Statement::Insert {
                table,
                columns,
                rows,
            } => {
                let entry = self.lock_table_entry(handle.xid(), &table, LockMode::RowExclusive)?;
                let mut count = 0;
                for row in &rows {
                    let values = self.build_insert_values(session, &entry, &columns, row)?;
                    self.insert_inner(&snap, &entry, &values)?;
                    count += 1;
                }
                Ok(QueryResult::Affected(count))
            }
            Statement::Select {
                columns,
                table,
                filter,
                extra_filter,
                order_by,
                limit,
                lock,
            } => match lock {
                None => {
                    self.lock_table_entry(handle.xid(), &table, LockMode::AccessShare)?;
                    self.exec_select(
                        &snap,
                        &columns,
                        &table,
                        &filter,
                        &extra_filter,
                        &order_by,
                        &limit,
                        false,
                        false,
                        path,
                    )
                }
                Some(LockClause::ForUpdate) => {
                    self.lock_table_entry(handle.xid(), &table, LockMode::RowExclusive)?;
                    self.exec_select(
                        &snap,
                        &columns,
                        &table,
                        &filter,
                        &extra_filter,
                        &order_by,
                        &limit,
                        true,
                        false,
                        path,
                    )
                }
                Some(LockClause::ForShare) => {
                    self.lock_table_entry(handle.xid(), &table, LockMode::RowExclusive)?;
                    self.exec_select(
                        &snap,
                        &columns,
                        &table,
                        &filter,
                        &extra_filter,
                        &order_by,
                        &limit,
                        true,
                        true,
                        path,
                    )
                }
            },
            Statement::Update {
                table,
                sets,
                filter,
            } => {
                let entry = self.lock_table_entry(handle.xid(), &table, LockMode::RowExclusive)?;
                let pred = filter
                    .as_ref()
                    .map(|f| {
                        filter_to_predicate_with_analyzer(
                            &entry,
                            f,
                            &self.inverted_analyzer_for(entry.oid, &f.column),
                        )
                    })
                    .transpose()?;
                let rows = self.scan_inner(&snap, &table, pred.as_ref())?;
                let mut count = 0;
                for (tid, old_values) in rows {
                    let new_values = apply_sets(&entry, &old_values, &sets)?;
                    self.update_inner(&snap, &table, tid, &new_values)?;
                    count += 1;
                }
                Ok(QueryResult::Affected(count))
            }
            Statement::Delete { table, filter } => {
                let entry = self.lock_table_entry(handle.xid(), &table, LockMode::RowExclusive)?;
                let pred = filter
                    .as_ref()
                    .map(|f| {
                        filter_to_predicate_with_analyzer(
                            &entry,
                            f,
                            &self.inverted_analyzer_for(entry.oid, &f.column),
                        )
                    })
                    .transpose()?;
                let rows = self.scan_inner(&snap, &table, pred.as_ref())?;
                let mut count = 0;
                for (tid, _) in rows {
                    self.delete_inner(&snap, &table, tid)?;
                    count += 1;
                }
                Ok(QueryResult::Affected(count))
            }
        }
    }

    /// Validate the Stage A/B SELECT shape without doing heap I/O. EXPLAIN and
    /// normal execution share this gate so they cannot disagree on supported ANN
    /// syntax, projection expressions, or ORDER BY columns.
    pub(crate) fn validate_select_shape(
        entry: &TableEntry,
        columns: &SelectCols,
        order_by: &Option<OrderByKind>,
    ) -> Result<()> {
        if let Some(OrderByKind::Bm25Score(expr)) = order_by {
            crate::executor::validate_bm25(entry, expr)?;
        }
        if let Some(OrderByKind::Column(ob)) = order_by {
            let column = entry
                .columns
                .iter()
                .find(|c| c.name.eq_ignore_ascii_case(&ob.column))
                .ok_or_else(|| {
                    EngineError::InvalidPredicate(format!("no column {:?} in table", ob.column))
                })?;
            if matches!(column.col_type, ColumnType::Vector(_)) {
                return Err(EngineError::InvalidArgument(format!(
                    "ORDER BY VECTOR column {:?} is not supported; use a distance operator instead",
                    ob.column
                )));
            }
        }
        if let SelectCols::Cols(items) = columns {
            for item in items {
                match item {
                    SelectItem::Column(name) => {
                        if !entry
                            .columns
                            .iter()
                            .any(|c| c.name.eq_ignore_ascii_case(name))
                        {
                            return Err(EngineError::InvalidPredicate(format!(
                                "no column {name:?} in table"
                            )));
                        }
                    }
                    SelectItem::Distance(expr) => {
                        crate::executor::prepare_distance(entry, expr)?;
                    }
                    SelectItem::Bm25Score(expr) => {
                        crate::executor::validate_bm25(entry, expr)?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Shared SELECT execution. `lock_rows` is the M2c `FOR UPDATE` mode
    /// (§9.1): after the scan/filter/sort/LIMIT, every surviving row is
    /// stamped with a lock-only `t_xmax` via [`HeapAM::lock_tuple`] BEFORE
    /// projection — locking after LIMIT matches PG (only returned rows are
    /// locked). The caller must have already taken the statement's table
    /// lock, and `snap.current_xid()` must be a real transaction XID (auto-
    /// commit FOR UPDATE runs inside `auto_commit` for exactly this
    /// reason); the locks are released when that transaction ends.
    ///
    /// A row deleted or updated-and-committed between the scan and the
    /// lock surfaces as `HeapError::TupleConcurrentlyUpdated` (SI write
    /// conflict; PG would re-check via EvalPlanQual — M2c reports the
    /// error instead, and the caller may retry with a fresh snapshot).
    ///
    /// Locks are taken in the final RESULT order (i.e. ORDER BY's value
    /// order when sorting, not storage/scan order). Two transactions
    /// locking overlapping row sets in different value orders can deadlock;
    /// Stage R's detector breaks such cycles (youngest victim aborted).
    #[allow(clippy::too_many_arguments)] // mirrors the parsed Select AST fields
    fn exec_select(
        &self,
        snap: &Snapshot,
        columns: &SelectCols,
        table: &str,
        filter: &Option<Filter>,
        extra_filter: &Option<Filter>,
        order_by: &Option<OrderByKind>,
        limit: &Option<usize>,
        lock_rows: bool,
        shared: bool,
        path: &mut ExecutionPath,
    ) -> Result<QueryResult> {
        let entry = self.table_entry(table)?;
        Self::validate_select_shape(&entry, columns, order_by)?;
        let output = crate::executor::execute(
            self,
            snap,
            table,
            &entry,
            filter,
            extra_filter,
            order_by,
            limit,
        )?;
        let is_hnsw = matches!(&output.plan.spec, crate::planner::ScanSpec::Hnsw(_));
        let use_inverted = matches!(&output.plan.spec, crate::planner::ScanSpec::Inverted(_));
        let mut rows = output.rows;
        *path = match &output.plan.spec {
            crate::planner::ScanSpec::BTree(_)
            | crate::planner::ScanSpec::Hnsw(_)
            | crate::planner::ScanSpec::Inverted(_) => ExecutionPath::IndexLookup,
            crate::planner::ScanSpec::Seq(_) => ExecutionPath::SeqScan,
        };
        if let Some(OrderByKind::Column(ob)) = order_by {
            let idx = entry
                .columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(&ob.column))
                .ok_or_else(|| {
                    EngineError::InvalidPredicate(format!("no column {:?} in table", ob.column))
                })?;
            rows.sort_by(|a, b| {
                let av = a.1.get(idx).and_then(|value| value.as_ref());
                let bv = b.1.get(idx).and_then(|value| value.as_ref());
                // PostgreSQL defaults: ASC NULLS LAST, DESC NULLS FIRST.
                let cmp = match (av, bv) {
                    (Some(a), Some(b)) => a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal),
                    (None, None) => std::cmp::Ordering::Equal,
                    (None, Some(_)) => std::cmp::Ordering::Greater,
                    (Some(_), None) => std::cmp::Ordering::Less,
                };
                if ob.desc {
                    cmp.reverse()
                } else {
                    cmp
                }
            });
        }
        // hnsw_search_visible already returns the exact M6 order. Only an
        // exact SeqScan fallback needs a SQL-distance sort here.
        if !is_hnsw {
            if let Some(OrderByKind::Expr(expr)) = order_by {
                rows = crate::executor::sort_by_distance(&entry, rows, expr)?;
            }
        }
        if let Some(OrderByKind::Bm25Score(expr)) = order_by {
            let scores =
                crate::executor::bm25_scores(self, snap, table, &entry, expr, use_inverted)?;
            rows.sort_by(|a, b| {
                let a_score = scores.get(&a.0).copied().unwrap_or(0.0);
                let b_score = scores.get(&b.0).copied().unwrap_or(0.0);
                let ordering = a_score.total_cmp(&b_score);
                let ordering = if expr.desc {
                    ordering.reverse()
                } else {
                    ordering
                };
                ordering.then_with(|| a.0.cmp(&b.0))
            });
        }
        if let Some(n) = limit {
            rows.truncate(*n);
        }
        if lock_rows {
            for (tid, _) in &rows {
                if shared {
                    self.heap
                        .lock_tuple_shared(*tid, snap, self.clog.as_ref())?;
                } else {
                    self.heap.lock_tuple(*tid, snap, self.clog.as_ref())?;
                }
            }
        }
        let (col_names, projected) = match columns {
            SelectCols::All => {
                let names: Vec<String> = entry.columns.iter().map(|c| c.name.clone()).collect();
                let rows: Vec<Vec<Value>> = rows.into_iter().map(|(_, v)| v).collect();
                (names, rows)
            }
            SelectCols::Cols(items) => {
                enum Projection {
                    Column(usize),
                    Distance(crate::executor::DistanceEvaluator),
                    Bm25Score(std::collections::HashMap<Tid, f64>),
                }
                let names: Vec<String> = items
                    .iter()
                    .map(|item| match item {
                        SelectItem::Column(name) => name.clone(),
                        SelectItem::Distance(_) => "?column?".to_string(),
                        SelectItem::Bm25Score(expr) => {
                            expr.alias.clone().unwrap_or_else(|| "?column?".to_string())
                        }
                    })
                    .collect();
                let projections = items
                    .iter()
                    .map(|item| match item {
                        SelectItem::Column(name) => {
                            let index = entry
                                .columns
                                .iter()
                                .position(|c| c.name.eq_ignore_ascii_case(name))
                                .ok_or_else(|| {
                                    EngineError::InvalidPredicate(format!(
                                        "no column {name:?} in table"
                                    ))
                                })?;
                            Ok(Projection::Column(index))
                        }
                        SelectItem::Distance(expr) => Ok(Projection::Distance(
                            crate::executor::prepare_distance(&entry, expr)?,
                        )),
                        SelectItem::Bm25Score(expr) => {
                            Ok(Projection::Bm25Score(crate::executor::bm25_scores(
                                self,
                                snap,
                                table,
                                &entry,
                                expr,
                                use_inverted,
                            )?))
                        }
                    })
                    .collect::<Result<Vec<_>>>()?;
                let projected = rows
                    .into_iter()
                    .map(|(tid, values)| {
                        projections
                            .iter()
                            .map(|projection| match projection {
                                Projection::Column(index) => Ok(values[*index].clone()),
                                Projection::Distance(evaluator) => evaluator
                                    .evaluate(&values)
                                    .map(|distance| distance.map(Datum::Float8)),
                                Projection::Bm25Score(scores) => Ok(Some(Datum::Float8(
                                    scores.get(&tid).copied().unwrap_or(0.0),
                                ))),
                            })
                            .collect::<Result<Vec<_>>>()
                    })
                    .collect::<Result<Vec<_>>>()?;
                (names, projected)
            }
        };
        Ok(QueryResult::Rows {
            columns: col_names,
            rows: projected,
        })
    }

    /// Run `op` as a single auto-commit transaction (§21): begin, take a
    /// real SI snapshot, advance curcid (Halloween protection), run, commit.
    /// On error the transaction's index maintenance is reverse-applied from
    /// the undo log (Stage O review) and the transaction is aborted
    /// best-effort; the *original* error is returned.
    ///
    /// The commit/abort barrier guard lives inside `TxnManager` (M2c Stage
    /// P), so this wrapper no longer takes one itself. Table locks acquired
    /// by `op` are released by `release_all` at the end of BOTH the success
    /// and the failure path (2PL release point, M2c Stage P).
    ///
    /// # Panic policy
    ///
    /// A panic inside `op` skips `release_all` (and the abort path): the
    /// transaction's XID and table locks leak. That is deliberate — a
    /// panic mid-transaction leaves in-memory state no one can vouch for,
    /// and the process-level failure policy (same as a WAL-fatal error)
    /// applies rather than a best-effort cleanup that might make it worse.
    #[cfg(test)]
    fn run_before_auto_commit_hook() {
        BEFORE_AUTO_COMMIT_HOOK.with(|hook| {
            if let Some(hook) = hook.get() {
                hook();
            }
        });
    }

    #[cfg(test)]
    fn run_commit_finished_hook(&self, xid: TxnId) {
        COMMIT_FINISHED_HOOK.with(|hook| {
            if let Some(hook) = hook.get() {
                hook(self, xid);
            }
        });
    }

    #[cfg(test)]
    fn record_inverted_visible(&self, index_oid: Oid, xid: TxnId) {
        if let Some(finished_at) = self.inverted_commit_finished.lock().get(&xid).copied() {
            self.inverted_visibility_samples
                .lock()
                .entry(index_oid)
                .or_default()
                .push(finished_at.elapsed());
        }
    }

    fn record_inverted_lag(&self, index_oid: Oid, previous: Lsn, applied: Lsn, synced: Lsn) {
        let now = Instant::now();
        let last_progress_at = if applied > previous {
            now
        } else {
            self.inverted_lag
                .lock()
                .get(&index_oid)
                .map_or(now, |metrics| metrics.last_progress_at)
        };
        self.inverted_lag.lock().insert(
            index_oid,
            pg_am_inverted::tier2::LagMetrics {
                wal_lag_records: 0,
                wal_lag_bytes: synced.0.saturating_sub(applied.0),
                applied_lsn: applied,
                synced_lsn: synced,
                last_progress_at,
                watermark_lag_seconds: 0.0,
            },
        );
    }

    /// Read a point-in-time lag snapshot for one inverted index.
    pub fn inverted_lag_metrics(
        &self,
        index_oid: Oid,
    ) -> Result<Option<pg_am_inverted::tier2::LagMetrics>> {
        let Some(mut metrics) = self.inverted_lag.lock().get(&index_oid).cloned() else {
            return Ok(None);
        };
        let synced = metrics
            .synced_lsn
            .max(self.storage.wal_writer().synced_lsn());
        metrics.synced_lsn = synced;
        if synced > metrics.applied_lsn {
            let tail = pg_am_inverted::tier2::FlushedWalTail::new(
                self.storage.data_dir().join("wal"),
                self.storage.config().wal_segment_size,
                Arc::clone(self.storage.wal_writer()),
            );
            let records = pg_storage::tier2::WalTailReader::tail_from(&tail, metrics.applied_lsn);
            let mut count = 0u64;
            let mut bytes = 0u64;
            for record in records {
                let record = record?;
                if !matches!(
                    record.record_type,
                    WalRecordType::SegmentSeal | WalRecordType::SegmentMerge
                ) {
                    count += 1;
                    bytes = bytes.saturating_add(record.record_size() as u64);
                }
            }
            metrics.wal_lag_records = count;
            metrics.wal_lag_bytes = bytes;
        } else {
            metrics.wal_lag_records = 0;
            metrics.wal_lag_bytes = 0;
        }
        metrics.watermark_lag_seconds = metrics.last_progress_at.elapsed().as_secs_f64();
        Ok(Some(metrics))
    }

    /// Commit-to-visible samples collected by the test-only commit hook.
    #[cfg(test)]
    pub fn inverted_visibility_samples(&self, index_oid: Oid) -> Vec<Duration> {
        self.inverted_visibility_samples
            .lock()
            .get(&index_oid)
            .cloned()
            .unwrap_or_default()
    }

    fn auto_commit<T>(&self, op: impl FnOnce(&Snapshot) -> Result<T>) -> Result<T> {
        self.auto_commit_with_hooks(op, || Ok(()), || {})
    }

    /// `on_success` runs after a durable commit but before table locks are
    /// released; `on_error` runs before lock release on either failure path.
    fn auto_commit_with_hooks<T>(
        &self,
        op: impl FnOnce(&Snapshot) -> Result<T>,
        on_success: impl FnOnce() -> Result<()>,
        on_error: impl FnOnce(),
    ) -> Result<T> {
        let xid = self.txn.begin_txn();
        // The snapshot's registry entry is dropped when `op` returns — the
        // post-commit cleanup's watermark read (oldest_snapshot_xmin) must
        // NOT see this transaction's own snapshot, or it would conclude
        // "someone older is still reading" and defer every map removal
        // forever (Stage D review P2).
        let op_result = {
            let (mut snap, _snap_guard) = self.txn.snapshot(xid);
            snap.advance_curcid();
            op(&snap)
        };
        match op_result {
            Ok(v) => {
                #[cfg(test)]
                Self::run_before_auto_commit_hook();
                let result = self.txn.commit_txn(xid);
                if result.is_err() {
                    // Same fallback as TxnHandle::commit (F7): a failed
                    // commit would otherwise leak the XID into the active
                    // set forever, pinning the vacuum horizon. Replay the
                    // index undo FIRST (same discipline as abort), then
                    // flip the CLOG bit; both best-effort — a WAL broken
                    // badly enough to fail the abort record means the
                    // process is tearing down anyway, but it must be loud.
                    apply_index_undo(
                        &self.index_undo,
                        self.storage.buffer_pool(),
                        self.storage.wal_writer(),
                        &self.hnsw_handles,
                        xid,
                    );
                    if let Err(abort_err) = self.txn.abort_txn(xid) {
                        tracing::warn!(error = %abort_err, xid = xid.0, "auto-commit commit-failure fallback abort failed");
                    }
                }
                let hook_result = if result.is_ok() {
                    #[cfg(test)]
                    self.run_commit_finished_hook(xid);
                    // M6 Stage C: deferred post-commit graph actions run
                    // after the commit is durable AND before the table
                    // locks release (round-3 P3-1).
                    run_pending_hnsw(
                        xid,
                        &self.pending_hnsw,
                        &self.hnsw_handles,
                        self.storage.buffer_pool(),
                        self.storage.wal_writer(),
                        self.txn.oldest_snapshot_xmin(),
                    );
                    self.index_undo.lock().remove(&xid);
                    on_success()
                } else {
                    self.pending_hnsw.lock().remove(&xid);
                    on_error();
                    Ok(())
                };
                self.lock_manager.release_all(xid);
                hook_result?;
                result?;
                Ok(v)
            }
            Err(e) => {
                apply_index_undo(
                    &self.index_undo,
                    self.storage.buffer_pool(),
                    self.storage.wal_writer(),
                    &self.hnsw_handles,
                    xid,
                );
                if let Err(abort_err) = self.txn.abort_txn(xid) {
                    tracing::warn!(error = %abort_err, "auto-commit abort failed");
                }
                // Deferred graph actions belong to a now-aborted XID and
                // must never accumulate across failed statements.
                self.pending_hnsw.lock().remove(&xid);
                on_error();
                self.lock_manager.release_all(xid);
                Err(e)
            }
        }
    }

    /// Insert one row into a system catalog through the heap AM, so the
    /// write is WAL-logged and redo-recoverable (Stage K: system pages use
    /// the standard 16-byte special geometry, so the AM's chain machinery
    /// applies to them unchanged).
    fn insert_catalog_row(
        &self,
        snap: &Snapshot,
        def: &SystemTableDef,
        row: &[Value],
    ) -> Result<()> {
        let columns = def.column_types();
        let tuple = encode_tuple(tuple_header(snap.curcid()), &columns, row)?;
        self.ensure_catalog_room(def, tuple.len())?;
        self.heap.insert(InsertContext {
            rel: RelationDesc {
                rel_oid: def.oid.raw(),
                first_page: def.first_page,
                columns: &columns,
            },
            snapshot: snap,
            tuple: &tuple,
            out_tid: None,
        })?;
        Ok(())
    }

    /// Enforce the module-level M2a limitation: catalog rows must fit on
    /// the system table's first page (the only page `Catalog::open` reads
    /// back).
    fn ensure_catalog_room(&self, def: &SystemTableDef, tuple_len: usize) -> Result<()> {
        let guard = self.storage.buffer_pool().pin(def.first_page)?;
        let page: &[u8; PAGE_SIZE] = guard.page().try_into().expect("frame is PAGE_SIZE");
        let free = SlottedPage::free_space(page);
        let needed = tuple_len + LINE_POINTER_SIZE;
        if free < needed {
            return Err(EngineError::CatalogFull(format!(
                "{}: row needs {needed} bytes but the first page has {free}",
                def.name
            )));
        }
        Ok(())
    }

    /// Locate the live (`relkind = 'r'`) `pg_class` version visible to the
    /// current DDL transaction. TOAST migration and DROP both update this
    /// catalog row, so a raw first-slot match can address an already-dead
    /// version and spuriously fail with `TupleConcurrentlyUpdated`.
    fn find_live_pg_class_row(&self, snap: &Snapshot, oid: Oid) -> Result<Tid> {
        self.find_live_class_row(snap, oid, RELKIND_TABLE)
    }

    fn find_live_class_row(&self, snap: &Snapshot, oid: Oid, kind: &str) -> Result<Tid> {
        let columns = PG_CLASS.column_types();
        let rows = self.heap.scan(ScanContext {
            rel: RelationDesc {
                rel_oid: PG_CLASS.oid.raw(),
                first_page: PG_CLASS.first_page,
                columns: &columns,
            },
            snapshot: snap,
            clog: self.clog.as_ref(),
        })?;
        for (tid, values) in rows {
            let matches = matches!(&values[0], Some(Datum::Int8(v)) if *v == oid.0 as i64)
                && matches!(&values[2], Some(Datum::Text(k)) if k == kind);
            if matches {
                return Ok(tid);
            }
        }
        if kind == RELKIND_TABLE {
            Err(EngineError::Corrupted(format!(
                "pg_class has no live row for table OID {}",
                oid.0
            )))
        } else {
            Err(EngineError::Corrupted(format!(
                "pg_class has no live {kind} row for relation OID {}",
                oid.0
            )))
        }
    }

    /// Walk the on-disk page chain from `first_page` (same rules as the
    /// heap AM's chain seeding: a fresh all-zero page ends the walk, a cycle
    /// is corruption).
    fn walk_chain(&self, first_page: PageId) -> Result<Vec<PageId>> {
        let mut pages = vec![first_page];
        let mut seen = HashSet::from([first_page]);
        loop {
            let current = *pages.last().expect("chain starts non-empty");
            let guard = self.storage.buffer_pool().pin(current)?;
            let page: &[u8; PAGE_SIZE] = guard.page().try_into().expect("frame is PAGE_SIZE");
            if SlottedPage::header(page).pd_upper == 0 {
                break;
            }
            let Some(next) = SlottedPage::next_page(page)? else {
                break;
            };
            if !seen.insert(next) {
                return Err(EngineError::Corrupted(format!(
                    "page chain cycle detected at page {next} (head {first_page})"
                )));
            }
            pages.push(next);
        }
        Ok(pages)
    }

    /// The registry entry of `table`, or [`EngineError::TableNotFound`].
    fn table_entry(&self, table: &str) -> Result<TableEntry> {
        self.registry
            .read()
            .get(table)
            .cloned()
            .ok_or_else(|| EngineError::TableNotFound(table.to_string()))
    }

    /// Acquire the statement's table lock (M2c Stage P, §9.2) — called
    /// after the TableEntry is resolved, before any data is touched. Locks
    /// key by XID and are held to transaction end (`release_all` at
    /// commit/abort; 2PL, no downgrade). Re-acquisition upgrades in place,
    /// so calling this per statement is cheap and idempotent.
    ///
    /// Always the blocking `acquire`: there is no NOWAIT; a table-lock
    /// cycle is broken by the Stage R deadlock detector, which aborts the
    /// youngest participant with `LockError::DeadlockVictim`.
    fn lock_table(&self, xid: TxnId, entry: &TableEntry, mode: LockMode) -> Result<()> {
        self.lock_oid(xid, entry.oid, mode)
    }

    /// [`Self::lock_table`] by OID, for DDL that acts on a table before /
    /// without a registry entry (e.g. `create_table` locks its fresh OID).
    fn lock_oid(&self, xid: TxnId, table: Oid, mode: LockMode) -> Result<()> {
        Ok(self.lock_manager.acquire(xid, table, mode)?)
    }

    /// Resolve `table` and acquire `mode` on it, closing the
    /// resolution→acquisition TOCTOU (M2c Stage P review): without the
    /// post-lock re-check, a `drop_table` could complete in the window —
    /// freeing the table's pages for reuse — while the statement goes on
    /// to read/write them through the stale entry. The re-check is
    /// authoritative because `drop_table` removes the registry entry
    /// BEFORE releasing its AccessExclusive lock: once our lock is
    /// granted, a missing (or rebound-to-another-OID) name means the table
    /// we resolved was dropped while we waited — fail with
    /// [`EngineError::TableNotFound`]; the lock we took on the dead OID is
    /// released by the caller's normal commit/abort path.
    pub(crate) fn lock_table_entry(
        &self,
        xid: TxnId,
        table: &str,
        mode: LockMode,
    ) -> Result<TableEntry> {
        let entry = self.table_entry(table)?;
        self.lock_table(xid, &entry, mode)?;
        let current = self
            .registry
            .read()
            .get(table)
            .cloned()
            .ok_or_else(|| EngineError::TableNotFound(table.to_string()))?;
        if current.oid != entry.oid {
            return Err(EngineError::TableNotFound(table.to_string()));
        }
        Ok(current)
    }

    /// All live table names and entries, used by the optional AP router.
    pub fn table_entries(&self) -> Vec<(String, TableEntry)> {
        self.registry
            .read()
            .iter()
            .map(|(name, entry)| (name.clone(), entry.clone()))
            .collect()
    }

    /// A cloned registry entry for `table` (testing / advanced use, e.g.
    /// building a [`RelationDesc`] to drive the heap AM directly).
    pub fn describe_table(&self, table: &str) -> Option<TableEntry> {
        self.registry.read().get(table).cloned()
    }

    /// The storage engine handle (testing / advanced use).
    pub fn storage(&self) -> &StorageEngine {
        &self.storage
    }

    /// The heap access method (testing / advanced use).
    pub fn heap(&self) -> &HeapAM {
        &self.heap
    }

    /// The transaction manager (testing / advanced use, e.g. driving an
    /// explicit abort through the engine's own CLOG).
    ///
    /// Direct `commit_txn` / `abort_txn` through this handle is safe with
    /// respect to checkpoints (M2c Stage P): the manager takes its own
    /// commit-barrier read guard internally, and the checkpoint coordinator
    /// holds the matching write guard — the Stage L caveat about racing
    /// [`Engine::checkpoint`] through this back door is resolved by
    /// construction.
    ///
    /// # WARNING: table locks
    ///
    /// A raw `commit_txn` / `abort_txn` does NOT release the transaction's
    /// table locks (the `LockManager` lives at the engine layer). Pair any
    /// back-door commit/abort with `lock_manager().release_all(xid)`, or
    /// the transaction's `RowExclusive` etc. grants linger forever and
    /// later DDL on those tables wedges.
    pub fn txn_manager(&self) -> &TxnManager {
        &self.txn
    }

    /// The vacuum horizon: smallest `xmin` among all live registered
    /// snapshots, or the XID clock's current value when no snapshot is
    /// registered (M3 Stage A, tech-selection §3.3). Passthrough to
    /// [`TxnManager::oldest_snapshot_xmin`] for §6.2 introspection and the
    /// upcoming `Engine::vacuum`.
    pub fn oldest_snapshot_xmin(&self) -> TxnId {
        self.txn.oldest_snapshot_xmin()
    }

    /// Snapshot of the currently active XIDs, sorted (M3 Stage E
    /// introspection, tech-selection §6.2). Pure passthrough to
    /// [`TxnManager::active_xids`].
    pub fn active_xids(&self) -> Vec<TxnId> {
        self.txn.active_xids()
    }

    /// The complete wait-for graph `(waiter, holder)` (M3 Stage E
    /// introspection, tech-selection §6.2): row-lock edges from
    /// [`TxnManager::wait_edges`] merged with table-lock edges derived from
    /// [`LockManager::table_lock_states`]. This is exactly the graph the
    /// deadlock detector consumes — both sides call the same
    /// [`pg_txn::wait_for_edges`], so the diagnostic view can never drift
    /// from detection.
    pub fn wait_edges(&self) -> Vec<(TxnId, TxnId)> {
        pg_txn::wait_for_edges(&self.txn, &self.lock_manager)
    }

    /// Snapshot of one table's granted set and wait queue (M3 Stage E
    /// introspection, tech-selection §6.2); `None` when the table has no
    /// lock state at all. Passthrough to [`LockManager::table_lock_state`].
    pub fn table_lock_state(&self, table: Oid) -> Option<TableLockState> {
        self.lock_manager.table_lock_state(table)
    }

    /// Disk-CLOG SLRU hit rate since open (M3 Stage E introspection,
    /// tech-selection §6.2): `hits / (hits + misses)`, `0.0` before any
    /// lookup. Passthrough to [`ClogBuffer::hit_rate`].
    pub fn clog_hit_rate(&self) -> f64 {
        self.clog.hit_rate()
    }

    /// Buffer-pool hit rate since open (M3 Stage E introspection,
    /// tech-selection §6.2): `hits / (hits + misses)`, `0.0` before any pin.
    /// Passthrough to [`BufferPool::hit_rate`].
    pub fn buffer_pool_hit_rate(&self) -> f64 {
        self.storage.buffer_pool().hit_rate()
    }

    /// The table lock manager (testing / observability, M2c Stage P):
    /// `is_granted` / `held_by` / `table_lock_state` let tests observe
    /// grants and wait queues; `release_all` pairs with back-door
    /// commits through [`Self::txn_manager`] (see its doc).
    pub fn lock_manager(&self) -> &LockManager {
        &self.lock_manager
    }

    /// The engine's disk CLOG (testing / advanced use).
    pub fn clog(&self) -> &Arc<ClogBuffer> {
        &self.clog
    }

    /// The query statistics ring buffer (M3 Stage E, tech-selection §6.3):
    /// one entry per [`Self::exec`] or [`Self::exec_with_params`] call, oldest
    /// dropped on overflow. The typed API (`scan` / `insert` / `update` /
    /// `delete` / `index_lookup`) does NOT produce entries — it never passes
    /// through either SQL execution entry point.
    pub fn query_stats(&self) -> &QueryStats {
        &self.query_stats
    }
}

/// A tuple header for engine-encoded rows: every identity field is a
/// placeholder — the AM stamps `t_xmin` with the writer's XID (Stage K),
/// `t_xmax` starts INVALID, `t_ctid` is not maintained by the AM
/// (INVALID-ish self-reference placeholder), and `t_cid` is set to the
/// statement's `curcid` (Stage O: Halloween protection, §7.2 / v2.3-Q4).
fn tuple_header(curcid: u32) -> TupleHeader {
    TupleHeader::new(
        TxnId::INVALID,
        TxnId::INVALID,
        0,
        [0; 16],
        Tid {
            page_id: PageId::INVALID,
            slot_id: 0,
        },
        curcid,
    )
}

/// The codec column types of a registry entry, in schema order.
pub(crate) fn column_types(entry: &TableEntry) -> Vec<ColumnType> {
    entry.columns.iter().map(|c| c.col_type).collect()
}

/// Build the AM's relation descriptor for a registry entry.
fn relation_desc<'a>(entry: &TableEntry, col_types: &'a [ColumnType]) -> RelationDesc<'a> {
    RelationDesc {
        rel_oid: entry.oid,
        first_page: entry.first_page,
        columns: col_types,
    }
}

/// Check that a predicate's column index is in range for the table.
fn validate_predicate(
    table: &str,
    entry: &TableEntry,
    predicate: Option<&Predicate>,
) -> Result<()> {
    if let Some(p) = predicate {
        if p.col_index() >= entry.columns.len() {
            return Err(EngineError::InvalidPredicate(format!(
                "table {table:?} has {} columns, predicate references column {}",
                entry.columns.len(),
                p.col_index()
            )));
        }
    }
    Ok(())
}

/// Filter `rows` in place by the predicate (single-column comparison).
fn apply_predicate(rows: &mut Vec<(Tid, Vec<Value>)>, predicate: Option<&Predicate>) {
    if let Some(p) = predicate {
        let col_index = p.col_index();
        rows.retain(|(_, vals)| vals.get(col_index).is_some_and(|v| p.matches(v)));
    }
}

/// Refuse to persist a FLOAT8 attribute into a legacy catalog that cannot
/// describe OID 701. Computed distance results do not go through this gate.
fn ensure_float8_storage_supported(has_float8_type: bool, schema: &[ColumnDef]) -> Result<()> {
    if !has_float8_type
        && schema
            .iter()
            .any(|column| column.col_type == ColumnType::Float8)
    {
        return Err(EngineError::Unsupported(
            "legacy catalog has no FLOAT8 pg_type row; FLOAT8 column storage requires a catalog migration (distance results remain supported)".to_string(),
        ));
    }
    Ok(())
}

/// Map a codec column type to its built-in `pg_type` OID and `attlen`
/// (§5.1; the built-in set has exactly one type per codec type).
fn type_oid_of(col_type: ColumnType) -> Result<(TypeOid, i32)> {
    // M6 Stage C: a vector column's type OID is the dedicated VECTOR_OID
    // and its per-column dim travels in pg_attribute.len (= dim × 4, the
    // inline width) — the dynamic dim cannot ride the static builtin table.
    if let ColumnType::Vector(dim) = col_type {
        return Ok((VECTOR_OID, dim as i32 * 4));
    }
    BUILTIN_TYPES
        .iter()
        .find(|t| t.column_type == col_type)
        .map(|t| (t.oid, t.len))
        .ok_or_else(|| EngineError::InvalidArgument(format!("no builtin type for {col_type:?}")))
}

fn ensure_finite_vector_components(vector: &[f32], description: &str) -> Result<()> {
    if let Some((index, value)) = vector
        .iter()
        .enumerate()
        .find(|(_, value)| !value.is_finite())
    {
        return Err(EngineError::InvalidArgument(format!(
            "{description} component {index} is not finite ({value})"
        )));
    }
    Ok(())
}

/// Convert a SQL `Literal` to a heap `Value` (`Option<Datum>`) given the
/// target column type.
fn literal_to_value(lit: &Literal, col_type: ColumnType) -> Result<Value> {
    match (lit, col_type) {
        (Literal::Int(n), ColumnType::Int4) => {
            Ok(Some(Datum::Int4(i32::try_from(*n).map_err(|_| {
                EngineError::InvalidArgument("integer literal out of range for INT4".to_string())
            })?)))
        }
        (Literal::Int(n), ColumnType::Int8) => Ok(Some(Datum::Int8(*n))),
        (Literal::Int(n), ColumnType::Timestamptz) => Ok(Some(Datum::Timestamptz(*n))),
        (Literal::Float(f), ColumnType::Float8) => Ok(Some(Datum::Float8(*f))),
        (Literal::Str(s), ColumnType::Text) => Ok(Some(Datum::Text(s.clone()))),
        (Literal::Str(s), ColumnType::Bytea) => Ok(Some(Datum::Bytea(s.clone().into_bytes()))),
        // M6 Stage C: a vector literal must match the column's dim exactly
        // (encode_tuple's Vector arm re-checks, but the dim error is
        // clearer here).
        (Literal::Vector(v), ColumnType::Vector(dim)) => {
            if v.len() != dim as usize {
                return Err(EngineError::InvalidArgument(format!(
                    "vector literal has {} components, column is VECTOR({dim})",
                    v.len()
                )));
            }
            ensure_finite_vector_components(v, "vector literal")?;
            Ok(Some(Datum::Vector(v.clone())))
        }
        (Literal::Null, _) => Ok(None),
        (l, t) => Err(EngineError::InvalidArgument(format!(
            "literal {l:?} is not compatible with column type {t:?}"
        ))),
    }
}

impl Engine {
    fn build_insert_values(
        &self,
        session: &mut SqlSession,
        entry: &TableEntry,
        columns: &Option<Vec<String>>,
        row: &[Literal],
    ) -> Result<Vec<Value>> {
        let n = entry.columns.len();
        let mut values = match columns {
            Some(cols) => {
                if cols.len() != row.len() {
                    return Err(EngineError::InvalidArgument(format!(
                        "column list has {} entries but {} values given",
                        cols.len(),
                        row.len()
                    )));
                }
                let mut v = vec![None; n];
                for (col_name, lit) in cols.iter().zip(row.iter()) {
                    let idx = entry
                        .columns
                        .iter()
                        .position(|c| c.name.eq_ignore_ascii_case(col_name))
                        .ok_or_else(|| {
                            EngineError::InvalidArgument(format!("no column {col_name:?} in table"))
                        })?;
                    v[idx] = if matches!(lit, Literal::Default) {
                        None
                    } else {
                        literal_to_value(lit, entry.columns[idx].col_type)?
                    };
                }
                v
            }
            None => {
                if row.len() != n {
                    return Err(EngineError::InvalidArgument(format!(
                        "table has {n} columns but {} values given",
                        row.len()
                    )));
                }
                row.iter()
                    .zip(entry.columns.iter())
                    .map(|(lit, col)| {
                        if matches!(lit, Literal::Default) {
                            Ok(None)
                        } else {
                            literal_to_value(lit, col.col_type)
                        }
                    })
                    .collect::<Result<Vec<_>>>()?
            }
        };

        for (index, default) in entry.defaults.iter().enumerate() {
            let is_default = match columns {
                Some(cols) => {
                    let provided = cols
                        .iter()
                        .position(|name| name.eq_ignore_ascii_case(&entry.columns[index].name));
                    provided.is_none()
                        || matches!(row.get(provided.unwrap()), Some(Literal::Default))
                }
                None => matches!(row.get(index), Some(Literal::Default)),
            };
            if !is_default {
                continue;
            }
            let sequence_oid = default.ok_or_else(|| {
                EngineError::InvalidArgument(format!(
                    "column {:?} has no DEFAULT",
                    entry.columns[index].name
                ))
            })?;
            let sequence = self.sequence_name(sequence_oid)?;
            let next = self.nextval(session, &sequence)?;
            let value = i32::try_from(next).map_err(|_| {
                EngineError::InvalidArgument(format!(
                    "sequence {sequence:?} value {next} is out of range for INT4"
                ))
            })?;
            values[index] = Some(Datum::Int4(value));
        }
        Ok(values)
    }

    fn sequence_name(&self, oid: Oid) -> Result<String> {
        self.sequences
            .states
            .lock()
            .get(&oid)
            .map(|state| state.name.clone())
            .ok_or_else(|| {
                EngineError::InvalidArgument(format!("sequence OID {oid} does not exist"))
            })
    }
}

/// Build a `Predicate` from a parsed `Filter`, resolving the column name
/// to a 0-based index and converting the literal.
pub(crate) fn filter_to_predicate(entry: &TableEntry, filter: &Filter) -> Result<Predicate> {
    filter_to_predicate_with_analyzer(entry, filter, &pg_am_inverted::AnalyzerConfig::builtin())
}

/// Build a predicate with the analyzer belonging to a particular inverted index.
pub(crate) fn filter_to_predicate_with_analyzer(
    entry: &TableEntry,
    filter: &Filter,
    analyzer: &pg_am_inverted::AnalyzerConfig,
) -> Result<Predicate> {
    let col_index = entry
        .columns
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case(&filter.column))
        .ok_or_else(|| {
            EngineError::InvalidPredicate(format!("no column {:?} in table", filter.column))
        })?;
    let col_type = entry.columns[col_index].col_type;
    let value = match &filter.value {
        // M6 Stage C: vectors carry no ordering — a WHERE comparison on a
        // vector literal is rejected loudly (ANN predicates are Phase 4b).
        Literal::Vector(_) => {
            return Err(EngineError::InvalidPredicate(format!(
                "column {:?}: vector literals are not comparable in WHERE",
                filter.column
            )))
        }
        Literal::Int(n) => match col_type {
            ColumnType::Int4 => Datum::Int4(i32::try_from(*n).map_err(|_| {
                EngineError::InvalidArgument("integer literal out of range for INT4".to_string())
            })?),
            ColumnType::Int8 => Datum::Int8(*n),
            ColumnType::Timestamptz => Datum::Timestamptz(*n),
            _ => {
                return Err(EngineError::InvalidPredicate(format!(
                    "column {:?} is not numeric",
                    filter.column
                )))
            }
        },
        Literal::Float(f) => match col_type {
            ColumnType::Float8 => Datum::Float8(*f),
            _ => {
                return Err(EngineError::InvalidPredicate(format!(
                    "column {:?} is not FLOAT8",
                    filter.column
                )))
            }
        },
        Literal::Str(s) => match col_type {
            ColumnType::Text => Datum::Text(s.clone()),
            _ => {
                return Err(EngineError::InvalidPredicate(format!(
                    "column {:?} is not text",
                    filter.column
                )))
            }
        },
        Literal::Null => {
            return Err(EngineError::InvalidPredicate(
                "NULL in WHERE is not supported".to_string(),
            ))
        }
        Literal::Default => {
            return Err(EngineError::InvalidPredicate(
                "DEFAULT is only valid in INSERT VALUES".to_string(),
            ))
        }
        Literal::Param(index) => {
            return Err(EngineError::InvalidArgument(format!(
                "unbound parameter ${index}; use Engine::exec_with_params"
            )))
        }
    };
    if filter.op == CmpOp::FullText && col_type != ColumnType::Text {
        return Err(EngineError::InvalidPredicate(format!(
            "full-text left operand {:?} is not a TEXT column",
            filter.column
        )));
    }
    Ok(match filter.op {
        CmpOp::Eq => Predicate::Eq { col_index, value },
        CmpOp::Lt => Predicate::Lt { col_index, value },
        CmpOp::Gt => Predicate::Gt { col_index, value },
        CmpOp::FullText => Predicate::FullText {
            col_index,
            value,
            analyzer: analyzer.clone(),
        },
    })
}

struct ParameterInference {
    slots: Vec<Option<ColumnType>>,
}

impl ParameterInference {
    fn new(max_index: usize) -> Self {
        Self {
            slots: vec![None; max_index],
        }
    }

    fn infer(&mut self, literal: &Literal, expected: ColumnType) -> Result<()> {
        let Literal::Param(index) = literal else {
            return Ok(());
        };
        let Some(slot) = index
            .checked_sub(1)
            .and_then(|index| self.slots.get_mut(index))
        else {
            return Err(EngineError::InvalidArgument(format!(
                "invalid parameter index ${index}"
            )));
        };
        match slot {
            Some(existing) if *existing != expected => {
                return Err(EngineError::InvalidArgument(format!(
                    "parameter ${index} has incompatible inferred types {existing:?} and {expected:?}"
                )))
            }
            Some(_) => {}
            None => *slot = Some(expected),
        }
        Ok(())
    }
}

fn infer_statement_parameters(
    engine: &Engine,
    stmt: &Statement,
    inference: &mut ParameterInference,
) -> Result<()> {
    match stmt {
        Statement::Explain(inner) => infer_statement_parameters(engine, inner, inference)?,
        Statement::Insert {
            table,
            columns,
            rows,
        } => {
            let entry = engine.table_entry(table)?;
            let targets = match columns {
                Some(columns) => columns
                    .iter()
                    .map(|column| required_column_type(&entry, column, false))
                    .collect::<Result<Vec<_>>>()?,
                None => column_types(&entry),
            };
            for row in rows {
                if row.len() != targets.len() {
                    let message = if columns.is_some() {
                        format!(
                            "column list has {} entries but {} values given",
                            targets.len(),
                            row.len()
                        )
                    } else {
                        format!(
                            "table has {} columns but {} values given",
                            targets.len(),
                            row.len()
                        )
                    };
                    return Err(EngineError::InvalidArgument(message));
                }
                for (literal, expected) in row.iter().zip(&targets) {
                    inference.infer(literal, *expected)?;
                }
            }
        }
        Statement::Select {
            columns,
            table,
            filter,
            extra_filter,
            order_by,
            ..
        } => {
            let entry = engine.table_entry(table)?;
            if let SelectCols::Cols(items) = columns {
                for item in items {
                    if let SelectItem::Distance(expr) = item {
                        infer_distance_parameter(&entry, expr, inference)?;
                    }
                    if let SelectItem::Bm25Score(expr) = item {
                        infer_bm25_parameter(&entry, expr, inference)?;
                    }
                }
            }
            infer_filter_parameter(&entry, filter, inference)?;
            infer_filter_parameter(&entry, extra_filter, inference)?;
            if let Some(OrderByKind::Expr(expr)) = order_by {
                infer_distance_parameter(&entry, expr, inference)?;
            }
            if let Some(OrderByKind::Bm25Score(expr)) = order_by {
                infer_bm25_parameter(&entry, expr, inference)?;
            }
        }
        Statement::Update {
            table,
            sets,
            filter,
        } => {
            let entry = engine.table_entry(table)?;
            for (column, literal) in sets {
                let expected = required_column_type(&entry, column, false)?;
                inference.infer(literal, expected)?;
            }
            infer_filter_parameter(&entry, filter, inference)?;
        }
        Statement::Delete { table, filter } => {
            let entry = engine.table_entry(table)?;
            infer_filter_parameter(&entry, filter, inference)?;
        }
        Statement::Begin
        | Statement::Commit
        | Statement::Rollback
        | Statement::CreateTable { .. }
        | Statement::CreateIndex { .. }
        | Statement::CreateSequence { .. }
        | Statement::SequenceQuery { .. } => {}
    }
    Ok(())
}

fn required_column_type(entry: &TableEntry, column: &str, predicate: bool) -> Result<ColumnType> {
    entry
        .columns
        .iter()
        .find(|candidate| candidate.name.eq_ignore_ascii_case(column))
        .map(|column| column.col_type)
        .ok_or_else(|| {
            let message = format!("no column {column:?} in table");
            if predicate {
                EngineError::InvalidPredicate(message)
            } else {
                EngineError::InvalidArgument(message)
            }
        })
}

fn infer_filter_parameter(
    entry: &TableEntry,
    filter: &Option<Filter>,
    inference: &mut ParameterInference,
) -> Result<()> {
    if let Some(filter) = filter {
        let column_type = required_column_type(entry, &filter.column, true)?;
        let expected = if filter.op == CmpOp::FullText {
            if column_type != ColumnType::Text {
                return Err(EngineError::InvalidArgument(format!(
                    "full-text left operand {:?} is not a TEXT column",
                    filter.column
                )));
            }
            ColumnType::Text
        } else {
            column_type
        };
        inference.infer(&filter.value, expected)?;
    }
    Ok(())
}

fn infer_distance_parameter(
    entry: &TableEntry,
    expr: &OrderByExpr,
    inference: &mut ParameterInference,
) -> Result<()> {
    if !matches!(expr.right, Expr::Literal(Literal::Param(_))) {
        return Ok(());
    }
    let Expr::Column(column) = &expr.left else {
        return Ok(());
    };
    let expected = required_column_type(entry, column, true)?;
    let ColumnType::Vector(_) = expected else {
        return Err(EngineError::InvalidArgument(format!(
            "distance left operand {column:?} is not a VECTOR column"
        )));
    };
    let Expr::Literal(literal) = &expr.right else {
        unreachable!()
    };
    inference.infer(literal, expected)
}

fn infer_bm25_parameter(
    entry: &TableEntry,
    expr: &Bm25ScoreExpr,
    inference: &mut ParameterInference,
) -> Result<()> {
    if !matches!(expr.query, Literal::Param(_)) {
        return Ok(());
    }
    let expected = required_column_type(entry, &expr.column, true)?;
    if expected != ColumnType::Text {
        return Err(EngineError::InvalidArgument(format!(
            "bm25_score left operand {:?} is not a TEXT column",
            expr.column
        )));
    }
    inference.infer(&expr.query, expected)
}

fn parameter_to_literal(
    index: usize,
    expected: Option<ColumnType>,
    value: &Value,
) -> Result<Literal> {
    let Some(datum) = value else {
        return Ok(Literal::Null);
    };
    let (actual, literal) = match datum {
        Datum::Int4(value) => (ColumnType::Int4, Literal::Int(i64::from(*value))),
        Datum::Int8(value) => (ColumnType::Int8, Literal::Int(*value)),
        Datum::Float8(value) => (ColumnType::Float8, Literal::Float(*value)),
        Datum::Text(value) => (ColumnType::Text, Literal::Str(value.clone())),
        Datum::Vector(value) => {
            ensure_finite_vector_components(value, &format!("parameter ${index} vector"))?;
            let dim = u32::try_from(value.len()).map_err(|_| {
                EngineError::InvalidArgument(format!(
                    "parameter ${index} vector dimension exceeds u32"
                ))
            })?;
            (ColumnType::Vector(dim), Literal::Vector(value.clone()))
        }
        other => {
            return Err(EngineError::InvalidArgument(format!(
                "parameter ${index} uses unsupported datum type {other:?}"
            )))
        }
    };
    if let Some(expected) = expected {
        if expected != actual {
            return Err(EngineError::InvalidArgument(format!(
                "parameter ${index} expects {expected:?}, got {actual:?}"
            )));
        }
    }
    Ok(literal)
}

fn replace_parameters(stmt: &mut Statement, literals: &[Literal]) -> Result<()> {
    match stmt {
        Statement::Explain(inner) => replace_parameters(inner, literals)?,
        Statement::Insert { rows, .. } => {
            for literal in rows.iter_mut().flatten() {
                replace_parameter(literal, literals)?;
            }
        }
        Statement::Select {
            columns,
            filter,
            extra_filter,
            order_by,
            ..
        } => {
            if let SelectCols::Cols(items) = columns {
                for item in items {
                    if let SelectItem::Distance(expr) = item {
                        replace_expr_parameters(expr, literals)?;
                    }
                    if let SelectItem::Bm25Score(expr) = item {
                        replace_bm25_parameters(expr, literals)?;
                    }
                }
            }
            if let Some(filter) = filter {
                replace_parameter(&mut filter.value, literals)?;
            }
            if let Some(filter) = extra_filter {
                replace_parameter(&mut filter.value, literals)?;
            }
            if let Some(OrderByKind::Expr(expr)) = order_by {
                replace_expr_parameters(expr, literals)?;
            }
            if let Some(OrderByKind::Bm25Score(expr)) = order_by {
                replace_bm25_parameters(expr, literals)?;
            }
        }
        Statement::Update { sets, filter, .. } => {
            for (_, literal) in sets {
                replace_parameter(literal, literals)?;
            }
            if let Some(filter) = filter {
                replace_parameter(&mut filter.value, literals)?;
            }
        }
        Statement::Delete { filter, .. } => {
            if let Some(filter) = filter {
                replace_parameter(&mut filter.value, literals)?;
            }
        }
        Statement::Begin
        | Statement::Commit
        | Statement::Rollback
        | Statement::CreateTable { .. }
        | Statement::CreateIndex { .. }
        | Statement::CreateSequence { .. }
        | Statement::SequenceQuery { .. } => {}
    }
    Ok(())
}

fn replace_expr_parameters(expr: &mut OrderByExpr, literals: &[Literal]) -> Result<()> {
    for operand in [&mut expr.left, &mut expr.right] {
        if let Expr::Literal(literal) = operand {
            replace_parameter(literal, literals)?;
        }
    }
    Ok(())
}

fn replace_bm25_parameters(expr: &mut Bm25ScoreExpr, literals: &[Literal]) -> Result<()> {
    replace_parameter(&mut expr.query, literals)
}

fn replace_parameter(literal: &mut Literal, literals: &[Literal]) -> Result<()> {
    let index = match literal {
        Literal::Param(index) => *index,
        _ => return Ok(()),
    };
    let Some(replacement) = index.checked_sub(1).and_then(|index| literals.get(index)) else {
        return Err(EngineError::InvalidArgument(format!(
            "missing value for parameter ${index}"
        )));
    };
    *literal = replacement.clone();
    Ok(())
}

/// Apply UPDATE SET assignments to an existing row, producing the full new
/// values vector.
fn apply_sets(
    entry: &TableEntry,
    old_values: &[Value],
    sets: &[(String, Literal)],
) -> Result<Vec<Value>> {
    let mut new_values = old_values.to_vec();
    for (col_name, lit) in sets {
        let idx = entry
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(col_name))
            .ok_or_else(|| {
                EngineError::InvalidArgument(format!("no column {col_name:?} in table"))
            })?;
        new_values[idx] = literal_to_value(lit, entry.columns[idx].col_type)?;
    }
    Ok(new_values)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_catalog_rejects_only_float8_column_storage() {
        let float8 = [ColumnDef {
            name: "distance".to_string(),
            col_type: ColumnType::Float8,
        }];
        let err = ensure_float8_storage_supported(false, &float8).unwrap_err();
        assert!(matches!(err, EngineError::Unsupported(_)));
        ensure_float8_storage_supported(true, &float8).unwrap();

        let ordinary = [ColumnDef {
            name: "id".to_string(),
            col_type: ColumnType::Int4,
        }];
        ensure_float8_storage_supported(false, &ordinary).unwrap();
    }

    use tempfile::TempDir;

    fn open(dir: &std::path::Path) -> Engine {
        let mut config = EngineConfig::new(dir);
        config.inverted_seal_batch_size = 1;
        Engine::open(dir, config).unwrap()
    }

    fn create_l2_table(engine: &Engine) {
        engine
            .exec(None, "CREATE TABLE t (id INT4, v VECTOR(2))")
            .unwrap();
        engine
            .exec(
                None,
                "CREATE INDEX ann_idx ON t USING hnsw (v vector_l2_ops)",
            )
            .unwrap();
    }

    fn set_index_am_for_test(engine: &Engine, index_oid: Oid, am_oid: Oid) {
        let columns = PG_CLASS.column_types();
        debug_assert_eq!(PG_CLASS.columns.last().unwrap().name, "relam");
        let mut guard = engine
            .storage
            .buffer_pool()
            .pin_mut(PG_CLASS.first_page)
            .unwrap();
        let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().unwrap();
        let mut found = false;
        for slot in 0..SlottedPage::slot_count(page) as u16 {
            let (offset, len, is_target) = {
                let Some(bytes) = SlottedPage::tuple(page, slot).unwrap() else {
                    continue;
                };
                let (_, values) = decode_tuple(bytes, &columns).unwrap();
                (
                    SlottedPage::line_pointer(page, slot).unwrap().off() as usize,
                    bytes.len(),
                    matches!(values.first(), Some(Some(Datum::Int8(oid))) if *oid == index_oid.0 as i64),
                )
            };
            if !is_target {
                continue;
            }
            // `relam` is the final fixed-width attribute. Change the original
            // physical row rather than adding an MVCC version because the
            // legacy catalog snapshot reads that original row directly.
            page[offset + len - 8..offset + len].copy_from_slice(&am_oid.0.to_le_bytes());
            found = true;
            break;
        }
        assert!(found, "test index {index_oid} has no pg_class row");
        drop(guard);
        engine.checkpoint().unwrap();
    }

    fn delete_am_row_for_test(engine: &Engine, am_oid: Oid) {
        let def = &pg_catalog::system_tables::PG_AM;
        let columns = def.column_types();
        let guard = engine.storage.buffer_pool().pin(def.first_page).unwrap();
        let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
        let mut tid = None;
        for slot in 0..SlottedPage::slot_count(page) as u16 {
            let Some(bytes) = SlottedPage::tuple(page, slot).unwrap() else {
                continue;
            };
            let (_, values) = decode_tuple(bytes, &columns).unwrap();
            if matches!(values.first(), Some(Some(Datum::Int8(oid))) if *oid == am_oid.0 as i64) {
                tid = Some(Tid {
                    page_id: def.first_page,
                    slot_id: slot,
                });
                break;
            }
        }
        drop(guard);
        let tid = tid.expect("fresh catalog contains the AM row");
        let mut guard = engine.storage.buffer_pool().pin_mut(tid.page_id).unwrap();
        let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().unwrap();
        SlottedPage::delete_tuple(page, tid.slot_id).unwrap();
        drop(guard);
        engine.checkpoint().unwrap();
    }

    fn rename_am_for_test(engine: &Engine, am_oid: Oid, new_name: &str) {
        let def = &pg_catalog::system_tables::PG_AM;
        let columns = def.column_types();
        let mut guard = engine
            .storage
            .buffer_pool()
            .pin_mut(def.first_page)
            .unwrap();
        let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().unwrap();
        let mut found = false;
        for slot in 0..SlottedPage::slot_count(page) as u16 {
            let (offset, len, old_name_len, is_target) = {
                let Some(bytes) = SlottedPage::tuple(page, slot).unwrap() else {
                    continue;
                };
                let (_, values) = decode_tuple(bytes, &columns).unwrap();
                let old_name_len = match values.get(1) {
                    Some(Some(Datum::Text(name))) => name.len(),
                    other => panic!("invalid pg_am name: {other:?}"),
                };
                (
                    SlottedPage::line_pointer(page, slot).unwrap().off() as usize,
                    bytes.len(),
                    old_name_len,
                    matches!(values.first(), Some(Some(Datum::Int8(oid))) if *oid == am_oid.0 as i64),
                )
            };
            if !is_target {
                continue;
            }
            assert_eq!(
                old_name_len,
                new_name.len(),
                "test rename must not resize the tuple"
            );
            page[offset + len - old_name_len..offset + len].copy_from_slice(new_name.as_bytes());
            found = true;
            break;
        }
        assert!(found, "test AM {am_oid} has no pg_am row");
        drop(guard);
        engine.checkpoint().unwrap();
    }

    fn write_inverted_metadata_for_test(data_dir: &Path, index_oid: Oid, applied_lsn: Lsn) {
        write_inverted_metadata_with_analyzer_for_test(
            data_dir,
            index_oid,
            applied_lsn,
            pg_am_inverted::AnalyzerConfig::builtin(),
        );
    }

    fn write_inverted_metadata_with_analyzer_for_test(
        data_dir: &Path,
        index_oid: Oid,
        applied_lsn: Lsn,
        analyzer: pg_am_inverted::AnalyzerConfig,
    ) {
        let segment_id = pg_storage::segment::SegmentId(7);
        let manifest = pg_am_inverted::SegmentManifest {
            generation: 1,
            active_segment_ids: vec![segment_id],
            applied_lsn,
        };
        let manifest_path = pg_am_inverted::SegmentStore::manifest_path(data_dir, index_oid);
        std::fs::create_dir_all(manifest_path.parent().unwrap()).unwrap();
        std::fs::write(manifest_path, manifest.encode().unwrap()).unwrap();

        let metadata = pg_am_inverted::SegmentMeta {
            format_version: pg_am_inverted::posting::SEGMENT_FORMAT_VERSION,
            index_oid,
            segment_id,
            analyzer,
            state: pg_storage::segment::SegmentState::Sealed,
            doc_count: 0,
            total_tokens: 0,
            term_df: Default::default(),
            created_from_lsn: Lsn::INVALID,
            applied_lsn,
            row_count: 0,
            posting_count: 0,
            byte_count: 0,
        };
        let metadata_path =
            pg_am_inverted::SegmentStore::segment_metadata_path(data_dir, index_oid, segment_id);
        std::fs::create_dir_all(metadata_path.parent().unwrap()).unwrap();
        std::fs::write(metadata_path, metadata.encode().unwrap()).unwrap();
        let postings: std::collections::BTreeMap<
            String,
            std::collections::BTreeMap<pg_storage::types::Tid, pg_am_inverted::posting::Posting>,
        > = Default::default();
        let postings_path =
            pg_am_inverted::SegmentStore::postings_path(data_dir, index_oid, segment_id);
        std::fs::create_dir_all(postings_path.parent().unwrap()).unwrap();
        std::fs::write(
            postings_path,
            pg_am_inverted::recovery::encode(&postings).unwrap(),
        )
        .unwrap();
        let doc_lengths: Vec<pg_am_inverted::DocLength> = Vec::new();
        let doc_lengths_path =
            pg_am_inverted::SegmentStore::doc_lengths_path(data_dir, index_oid, segment_id);
        std::fs::create_dir_all(doc_lengths_path.parent().unwrap()).unwrap();
        std::fs::write(
            doc_lengths_path,
            pg_am_inverted::recovery::encode(&doc_lengths).unwrap(),
        )
        .unwrap();
    }

    fn raw_hnsw_map_page(engine: &Engine, index_oid: Oid) -> PageId {
        let def = &PG_RELPAGES;
        let columns = def.column_types();
        let guard = engine.storage.buffer_pool().pin(def.first_page).unwrap();
        let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
        for slot in 0..SlottedPage::slot_count(page) as u16 {
            let Some(bytes) = SlottedPage::tuple(page, slot).unwrap() else {
                continue;
            };
            let (_, values) = decode_tuple(bytes, &columns).unwrap();
            if !matches!(values.first(), Some(Some(Datum::Int8(oid))) if *oid == index_oid.0 as i64)
            {
                continue;
            }
            return match values.get(2) {
                Some(Some(Datum::Int8(page))) => PageId(*page as u64),
                other => panic!("invalid relpages map page: {other:?}"),
            };
        }
        panic!("no physical relpages row for failed index {index_oid:?}")
    }

    static COMMIT_FAIL_HOOK_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    static WRITER_READY: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
    static CREATE_LOCKED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
    static ANN_STARTED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
    static ANN_RELEASE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
    static DROP_CLEANUP_STARTED: std::sync::atomic::AtomicBool =
        std::sync::atomic::AtomicBool::new(false);

    fn wait_for_drop() {
        ANN_STARTED.store(true, Ordering::SeqCst);
        while !ANN_RELEASE.load(Ordering::SeqCst) {
            std::thread::yield_now();
        }
    }

    fn signal_drop_cleanup() {
        DROP_CLEANUP_STARTED.store(true, Ordering::SeqCst);
    }

    fn wait_for_writer_ready() {
        CREATE_LOCKED.store(true, Ordering::SeqCst);
        while !WRITER_READY.load(Ordering::SeqCst) {
            std::thread::yield_now();
        }
    }

    static INVERTED_WRITER_READY: std::sync::atomic::AtomicBool =
        std::sync::atomic::AtomicBool::new(false);
    static INVERTED_CREATE_LOCKED: std::sync::atomic::AtomicBool =
        std::sync::atomic::AtomicBool::new(false);

    fn wait_for_inverted_writer_ready() {
        INVERTED_CREATE_LOCKED.store(true, Ordering::SeqCst);
        while !INVERTED_WRITER_READY.load(Ordering::SeqCst) {
            std::thread::yield_now();
        }
    }

    #[expect(dead_code, reason = "the guard is held only for its Drop")]
    struct CommitFailGuard(std::sync::MutexGuard<'static, ()>);

    impl CommitFailGuard {
        fn arm() -> Self {
            let guard = COMMIT_FAIL_HOOK_LOCK
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            pg_txn::manager::test_hooks::set_commit_txn_force_fail(true);
            Self(guard)
        }
    }

    impl Drop for CommitFailGuard {
        fn drop(&mut self) {
            pg_txn::manager::test_hooks::set_commit_txn_force_fail(false);
        }
    }

    #[test]
    fn auto_commit_discards_pending_hnsw_after_a_later_error() {
        let tmp = TempDir::new().unwrap();
        let engine = open(tmp.path());
        create_l2_table(&engine);
        engine
            .exec(None, "INSERT INTO t VALUES (1, [1,0])")
            .unwrap();
        let tid = engine.scan("t", None).unwrap()[0].0;

        let result = engine.auto_commit(|snap| {
            let _entry =
                engine.lock_table_entry(snap.current_xid(), "t", LockMode::RowExclusive)?;
            engine.delete_inner(snap, "t", tid)?;
            Err::<(), _>(EngineError::InvalidArgument(
                "injected failure after the first deferred HNSW action".to_string(),
            ))
        });
        assert!(result.is_err());
        assert!(
            engine.pending_hnsw.lock().is_empty(),
            "aborted XID left deferred HNSW actions behind"
        );
        assert_eq!(engine.scan("t", None).unwrap().len(), 1);
        engine.shutdown();
    }

    #[test]
    fn hnsw_create_commit_failure_rolls_back_registry_and_handle() {
        let tmp = TempDir::new().unwrap();
        let engine = open(tmp.path());
        engine
            .exec(None, "CREATE TABLE t (id INT4, v VECTOR(2))")
            .unwrap();
        let hook = CommitFailGuard::arm();
        let result = engine.create_hnsw_index_on(
            "t",
            "v",
            pg_am_hnsw::HnswParams::default(),
            2,
            pg_am_hnsw::Metric::L2,
            pg_am_hnsw::NeighborSelection::Heuristic,
            42,
        );
        drop(hook);
        assert!(result.is_err());
        assert!(engine.indexes().is_empty());
        assert!(engine.hnsw_handles.lock().is_empty());

        engine
            .create_hnsw_index_on(
                "t",
                "v",
                pg_am_hnsw::HnswParams::default(),
                2,
                pg_am_hnsw::Metric::L2,
                pg_am_hnsw::NeighborSelection::Heuristic,
                42,
            )
            .unwrap();
        assert_eq!(engine.indexes().len(), 1);
        assert_eq!(engine.hnsw_handles.lock().len(), 1);
        engine.shutdown();
    }

    #[test]
    fn btree_create_commit_failure_rolls_back_registry() {
        let tmp = TempDir::new().unwrap();
        let engine = open(tmp.path());
        engine.exec(None, "CREATE TABLE t (id INT4)").unwrap();
        let hook = CommitFailGuard::arm();
        let result = engine.create_index("t", "id");
        drop(hook);
        assert!(result.is_err());
        assert!(engine.indexes().is_empty());

        engine.create_index("t", "id").unwrap();
        assert_eq!(engine.indexes().len(), 1);
        engine.shutdown();
    }

    #[test]
    fn seqscan_fallbacks_do_not_open_hnsw_handles() {
        let tmp = TempDir::new().unwrap();
        let engine = open(tmp.path());
        engine
            .exec(
                None,
                "CREATE TABLE t (id INT4, v VECTOR(2), other VECTOR(2))",
            )
            .unwrap();
        engine
            .exec(
                None,
                "CREATE INDEX ann_idx ON t USING hnsw (v vector_l2_ops)",
            )
            .unwrap();
        engine
            .exec(None, "INSERT INTO t VALUES (1, [1,0], [0,1])")
            .unwrap();

        for query in [
            "SELECT * FROM t ORDER BY v <-> '[1,0]' DESC LIMIT 1",
            "SELECT * FROM t ORDER BY v <-> '[1,0]' ASC",
            "SELECT * FROM t WHERE id > 0 ORDER BY v <-> '[1,0]' ASC LIMIT 1",
            "SELECT * FROM t ORDER BY other <-> '[1,0]' ASC LIMIT 1",
        ] {
            engine.hnsw_handles.lock().clear();
            engine.exec(None, query).unwrap();
            assert!(
                engine.hnsw_handles.lock().is_empty(),
                "fallback opened an HNSW handle: {query}"
            );
        }

        engine.hnsw_handles.lock().clear();
        engine
            .exec(None, "SELECT * FROM t ORDER BY v <-> '[1,0]' ASC LIMIT 1")
            .unwrap();
        assert_eq!(engine.hnsw_handles.lock().len(), 1);
        engine.shutdown();
    }

    #[test]
    fn real_legacy_directory_rejects_float8_storage_before_heap_creation() {
        let tmp = TempDir::new().unwrap();
        let engine = open(tmp.path());
        let def = &pg_catalog::system_tables::PG_TYPE;
        let columns = def.column_types();
        let guard = engine.storage.buffer_pool().pin(def.first_page).unwrap();
        let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
        let mut tid = None;
        for slot in 0..SlottedPage::slot_count(page) as u16 {
            let Some(bytes) = SlottedPage::tuple(page, slot).unwrap() else {
                continue;
            };
            let (_, values) = decode_tuple(bytes, &columns).unwrap();
            if matches!(values.first(), Some(Some(Datum::Int8(701)))) {
                tid = Some(Tid {
                    page_id: def.first_page,
                    slot_id: slot,
                });
                break;
            }
        }
        drop(guard);
        let tid = tid.expect("fresh catalog contains FLOAT8");
        let mut guard = engine.storage.buffer_pool().pin_mut(tid.page_id).unwrap();
        let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().unwrap();
        SlottedPage::delete_tuple(page, tid.slot_id).unwrap();
        drop(guard);
        engine.checkpoint().unwrap();
        engine.shutdown();

        let engine = open(tmp.path());
        assert!(
            engine.catalog.type_by_oid(FLOAT8_OID).is_none(),
            "legacy fixture still contains FLOAT8"
        );
        let before = engine.storage().page_allocator().lock().next_page_id();
        let err = engine
            .create_table(
                "legacy_float8",
                &[ColumnDef {
                    name: "distance".to_string(),
                    col_type: ColumnType::Float8,
                }],
            )
            .unwrap_err();
        assert!(matches!(err, EngineError::Unsupported(_)), "{err}");
        let after = engine.storage().page_allocator().lock().next_page_id();
        assert_eq!(before, after);
        engine.shutdown();
    }

    #[test]
    fn drop_cleanup_error_never_resurrects_a_live_table() {
        let tmp = TempDir::new().unwrap();
        let engine = open(tmp.path());
        create_l2_table(&engine);
        let entry = engine.describe_table("t").unwrap();
        // Force the post-commit page-free path to fail after the committed
        // marker. In-memory cleanup must already have removed every entry.
        engine
            .storage
            .page_allocator()
            .lock()
            .free_page(entry.first_page)
            .unwrap();
        let result = engine.drop_table("t");
        assert!(result.is_ok(), "post-commit cleanup is only a warning");
        assert!(matches!(
            engine.scan("t", None),
            Err(EngineError::TableNotFound(_))
        ));
        assert!(engine.indexes().is_empty());
        assert!(engine.hnsw_handles.lock().is_empty());
        engine.shutdown();

        let engine = open(tmp.path());
        assert!(matches!(
            engine.scan("t", None),
            Err(EngineError::TableNotFound(_))
        ));
        assert!(engine.indexes().is_empty());
        assert!(engine.hnsw_handles.lock().is_empty());
        engine.shutdown();
    }

    #[test]
    fn drop_page_walk_error_after_commit_is_only_a_warning() {
        let tmp = TempDir::new().unwrap();
        let engine = open(tmp.path());
        create_l2_table(&engine);
        let entry = engine.describe_table("t").unwrap();
        // A self-link makes the post-commit chain walk fail before any page
        // can be freed. The durable marker and in-memory cleanup still win.
        let mut guard = engine
            .storage
            .buffer_pool()
            .pin_mut(entry.first_page)
            .unwrap();
        let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().unwrap();
        SlottedPage::set_next_page(page, Some(entry.first_page)).unwrap();
        drop(guard);

        let result = engine.drop_table("t");
        assert!(result.is_ok(), "post-commit page walk is only a warning");
        assert!(matches!(
            engine.scan("t", None),
            Err(EngineError::TableNotFound(_))
        ));
        assert!(engine.indexes().is_empty());
        assert!(engine.hnsw_handles.lock().is_empty());
        engine.shutdown();

        let engine = open(tmp.path());
        assert!(matches!(
            engine.scan("t", None),
            Err(EngineError::TableNotFound(_))
        ));
        assert!(engine.indexes().is_empty());
        assert!(engine.hnsw_handles.lock().is_empty());
        engine.shutdown();
    }

    #[test]
    fn drop_commit_failure_preserves_table_and_allows_retry() {
        let tmp = TempDir::new().unwrap();
        let engine = open(tmp.path());
        create_l2_table(&engine);
        engine
            .exec(None, "INSERT INTO t VALUES (1, [1,0])")
            .unwrap();
        let hook = CommitFailGuard::arm();
        let result = engine.drop_table("t");
        drop(hook);
        assert!(result.is_err());
        assert_eq!(engine.scan("t", None).unwrap().len(), 1);
        assert!(matches!(
            engine.exec(None, "CREATE TABLE t (id INT4)"),
            Err(EngineError::TableExists(_))
        ));
        engine.shutdown();

        let engine = open(tmp.path());
        assert_eq!(engine.scan("t", None).unwrap().len(), 1);
        assert!(matches!(
            engine.exec(None, "CREATE TABLE t (id INT4)"),
            Err(EngineError::TableExists(_))
        ));

        engine.drop_table("t").unwrap();
        assert!(matches!(
            engine.scan("t", None),
            Err(EngineError::TableNotFound(_))
        ));
        assert!(engine.indexes().is_empty());
        assert!(engine.hnsw_handles.lock().is_empty());
        engine.shutdown();

        let engine = open(tmp.path());
        assert!(engine.indexes().is_empty());
        assert!(engine.hnsw_handles.lock().is_empty());
        create_l2_table(&engine);
        assert_eq!(engine.indexes().len(), 1);
        engine.shutdown();
    }

    #[test]
    fn legacy_orphaned_serial_metadata_does_not_block_recreate() {
        let tmp = TempDir::new().unwrap();
        let engine = open(tmp.path());
        engine.exec(None, "CREATE TABLE t (id SERIAL)").unwrap();
        let entry = engine.describe_table("t").unwrap();
        // Reproduce the pre-Stage-G DROP shape: only the table was marked and
        // cleaned, leaving its SERIAL sequence/default metadata behind.
        engine
            .auto_commit_with_hooks(
                |snap| {
                    engine.lock_table(snap.current_xid(), &entry, LockMode::AccessExclusive)?;
                    engine.drop_table_marker(snap, "t", &entry)
                },
                || engine.drop_table_storage("t", &entry),
                || {},
            )
            .unwrap();
        engine.shutdown();

        let engine = open(tmp.path());
        engine.exec(None, "CREATE TABLE t (id SERIAL)").unwrap();
        let mut session = SqlSession::new(1);
        engine
            .exec_in_session(&mut session, None, "INSERT INTO t VALUES (DEFAULT)")
            .unwrap();
        assert_eq!(
            engine.scan("t", None).unwrap()[0].1[0],
            Some(Datum::Int4(1))
        );
        engine.shutdown();
    }

    #[test]
    fn create_index_commit_failure_rolls_back_before_waiters_run() {
        let tmp = TempDir::new().unwrap();
        let engine = Arc::new(open(tmp.path()));
        engine
            .exec(None, "CREATE TABLE t (id INT4, v VECTOR(2))")
            .unwrap();
        let table_oid = engine.describe_table("t").unwrap().oid;
        struct ResetHook;
        impl Drop for ResetHook {
            fn drop(&mut self) {
                BEFORE_AUTO_COMMIT_HOOK.with(|hook| hook.set(None));
                WRITER_READY.store(false, Ordering::SeqCst);
                CREATE_LOCKED.store(false, Ordering::SeqCst);
            }
        }
        let _reset = ResetHook;
        WRITER_READY.store(false, Ordering::SeqCst);
        CREATE_LOCKED.store(false, Ordering::SeqCst);

        let creator_engine = Arc::clone(&engine);
        let creator = std::thread::spawn(move || {
            let _hook = CommitFailGuard::arm();
            BEFORE_AUTO_COMMIT_HOOK.with(|hook| hook.set(Some(wait_for_writer_ready)));
            creator_engine.create_hnsw_index_on(
                "t",
                "v",
                pg_am_hnsw::HnswParams::default(),
                2,
                pg_am_hnsw::Metric::L2,
                pg_am_hnsw::NeighborSelection::Heuristic,
                42,
            )
        });
        let deadline = Instant::now() + Duration::from_secs(30);
        while !CREATE_LOCKED.load(Ordering::SeqCst) {
            assert!(
                Instant::now() < deadline,
                "CREATE INDEX never reached its pre-commit hook"
            );
            std::thread::sleep(Duration::from_millis(1));
        }

        let writer_engine = Arc::clone(&engine);
        let writer =
            std::thread::spawn(move || writer_engine.exec(None, "INSERT INTO t VALUES (1, [1,0])"));
        let mut waiter_queued = false;
        while !waiter_queued {
            assert!(
                Instant::now() < deadline,
                "writer never queued behind the index build lock"
            );
            if writer.is_finished() {
                let result = writer.join().unwrap();
                panic!("writer finished before queuing behind the index build lock: {result:?}");
            }
            if engine.table_lock_state(table_oid).is_some_and(|state| {
                state
                    .waiters
                    .iter()
                    .any(|(_, mode)| *mode == LockMode::RowExclusive)
            }) {
                waiter_queued = true;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(waiter_queued, "writer never queued behind Exclusive");
        WRITER_READY.store(true, Ordering::SeqCst);
        let create_result = creator.join().unwrap();
        let write_result = writer.join().unwrap();

        assert!(create_result.is_err());
        assert!(write_result.is_ok(), "{write_result:?}");
        assert!(engine.indexes().is_empty());
        assert!(engine.hnsw_handles.lock().is_empty());
        // The failed index OID follows the table OID and its optional Stage D
        // TOAST OID. Read the physical sidecar directly: MVCC would hide the
        // aborted relpages row and produce a vacuous lookup result.
        let has_toast = engine.describe_table("t").unwrap().toast.is_some();
        let failed_index_oid = Oid(table_oid.0 + 1 + u64::from(has_toast));
        let map_page = raw_hnsw_map_page(&engine, failed_index_oid);
        let map = pg_am_btree::BTreeIndex::open(
            Arc::clone(engine.storage.buffer_pool()),
            Arc::clone(engine.storage.wal_writer()),
            failed_index_oid,
            map_page,
            ColumnType::Int8,
        )
        .unwrap();
        let key = encode_key(&Datum::Int8(0)).unwrap();
        assert!(map.lookup(&key).unwrap().is_none());
        engine.shutdown();
    }

    #[test]
    fn inverted_create_commit_failure_rolls_back_before_waiters_run() {
        let tmp = TempDir::new().unwrap();
        let engine = Arc::new(open(tmp.path()));
        engine
            .exec(None, "CREATE TABLE t (id INT4, content TEXT)")
            .unwrap();
        engine
            .exec(None, "INSERT INTO t VALUES (0, 'before')")
            .unwrap();
        let table_oid = engine.describe_table("t").unwrap().oid;
        struct ResetHook;
        impl Drop for ResetHook {
            fn drop(&mut self) {
                BEFORE_AUTO_COMMIT_HOOK.with(|hook| hook.set(None));
                INVERTED_WRITER_READY.store(false, Ordering::SeqCst);
                INVERTED_CREATE_LOCKED.store(false, Ordering::SeqCst);
            }
        }
        let _reset = ResetHook;
        INVERTED_WRITER_READY.store(false, Ordering::SeqCst);
        INVERTED_CREATE_LOCKED.store(false, Ordering::SeqCst);

        let creator_engine = Arc::clone(&engine);
        let creator = std::thread::spawn(move || {
            let _hook = CommitFailGuard::arm();
            BEFORE_AUTO_COMMIT_HOOK.with(|hook| hook.set(Some(wait_for_inverted_writer_ready)));
            creator_engine.exec(
                None,
                "CREATE INDEX content_idx ON t USING inverted (content)",
            )
        });
        let deadline = Instant::now() + Duration::from_secs(30);
        while !INVERTED_CREATE_LOCKED.load(Ordering::SeqCst) {
            assert!(
                Instant::now() < deadline,
                "inverted CREATE INDEX never reached its pre-commit hook"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        let writer_engine = Arc::clone(&engine);
        let writer = std::thread::spawn(move || {
            writer_engine.exec(None, "INSERT INTO t VALUES (1, 'after')")
        });
        let mut waiter_queued = false;
        while !waiter_queued {
            assert!(
                Instant::now() < deadline,
                "writer never queued behind the index build lock"
            );
            if writer.is_finished() {
                let result = writer.join().unwrap();
                panic!("writer finished before queuing behind the index build lock: {result:?}");
            }
            if engine.table_lock_state(table_oid).is_some_and(|state| {
                state
                    .waiters
                    .iter()
                    .any(|(_, mode)| *mode == LockMode::RowExclusive)
            }) {
                waiter_queued = true;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(waiter_queued, "writer never queued behind AccessExclusive");
        INVERTED_WRITER_READY.store(true, Ordering::SeqCst);
        let create_result = creator.join().unwrap();
        let write_result = writer.join().unwrap();
        assert!(create_result.is_err());
        assert!(write_result.is_ok(), "{write_result:?}");
        assert!(engine.indexes().is_empty());
        assert!(engine.inverted_handles.lock().is_empty());
        assert!(engine.pending_inverted.lock().is_empty());

        engine
            .exec(
                None,
                "CREATE INDEX content_idx ON t USING inverted (content)",
            )
            .unwrap();
        let result = engine
            .exec(
                None,
                "SELECT id FROM t WHERE content @@ 'after' ORDER BY id",
            )
            .unwrap();
        assert!(matches!(result, QueryResult::Rows { rows, .. } if rows.len() == 1));
        assert_eq!(
            engine.query_stats().entries().last().unwrap().path,
            ExecutionPath::IndexLookup
        );
        engine.shutdown();
    }

    #[test]
    fn lazy_open_cannot_reinsert_a_handle_after_drop() {
        let tmp = TempDir::new().unwrap();
        let engine = Arc::new(open(tmp.path()));
        create_l2_table(&engine);
        engine
            .exec(None, "INSERT INTO t VALUES (1, [1,0])")
            .unwrap();
        engine.hnsw_handles.lock().clear();

        struct ResetHook;
        impl Drop for ResetHook {
            fn drop(&mut self) {
                BEFORE_HNSW_PUBLISH_HOOK.with(|hook| hook.set(None));
                BEFORE_DROP_CLEANUP_HOOK.with(|hook| hook.set(None));
                ANN_RELEASE.store(true, Ordering::SeqCst);
            }
        }
        let _reset = ResetHook;
        ANN_STARTED.store(false, Ordering::SeqCst);
        ANN_RELEASE.store(false, Ordering::SeqCst);
        DROP_CLEANUP_STARTED.store(false, Ordering::SeqCst);

        let ann_engine = Arc::clone(&engine);
        let ann = std::thread::spawn(move || {
            BEFORE_HNSW_PUBLISH_HOOK.with(|hook| hook.set(Some(wait_for_drop)));
            ann_engine.hnsw_search_visible("t", &[1.0, 0.0], 1, Some(64), &Snapshot::everything())
        });
        let deadline = Instant::now() + Duration::from_secs(30);
        while !ANN_STARTED.load(Ordering::SeqCst) {
            assert!(
                Instant::now() < deadline,
                "ANN never reached its publish hook"
            );
            std::thread::sleep(Duration::from_millis(1));
        }

        let drop_engine = Arc::clone(&engine);
        let drop_thread = std::thread::spawn(move || {
            BEFORE_DROP_CLEANUP_HOOK.with(|hook| hook.set(Some(signal_drop_cleanup)));
            drop_engine.drop_table("t")
        });
        while !DROP_CLEANUP_STARTED.load(Ordering::SeqCst) {
            assert!(
                Instant::now() < deadline,
                "DROP never reached post-commit cleanup"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        // DROP is committed and inside cleanup while ANN is paused at publish.
        // Prove the guards directly: an unsynchronized open/publish would let
        // either try_write succeed here, regardless of thread scheduling.
        assert!(
            engine.registry.try_write().is_none(),
            "table registry is not read-locked across HNSW publish"
        );
        assert!(
            engine.indexes.try_write().is_none(),
            "index registry is not read-locked across HNSW publish"
        );
        ANN_RELEASE.store(true, Ordering::SeqCst);
        let ann_result = ann.join().unwrap();
        let drop_result = drop_thread.join().unwrap();

        assert!(ann_result.is_ok(), "{ann_result:?}");
        assert!(drop_result.is_ok(), "{drop_result:?}");
        assert!(engine.indexes().is_empty());
        assert!(engine.hnsw_handles.lock().is_empty());
        engine.shutdown();
    }

    #[test]
    fn inverted_pending_limit_is_loud_and_bounded() {
        let mut pending = HashMap::new();
        let xid = TxnId(7);
        let action = InvertedWalAction::Delete {
            tid: Tid {
                page_id: PageId(1),
                slot_id: 0,
            },
        };
        push_inverted_pending(&mut pending, xid, action.clone(), 2, usize::MAX).unwrap();
        push_inverted_pending(&mut pending, xid, action.clone(), 2, usize::MAX).unwrap();
        let error = push_inverted_pending(&mut pending, xid, action, 2, usize::MAX).unwrap_err();
        assert!(matches!(error, EngineError::Unsupported(_)));
        assert_eq!(pending.get(&xid).unwrap().len(), 2);

        let mut pending = HashMap::new();
        let action = InvertedWalAction::Upsert {
            tid: Tid {
                page_id: PageId(1),
                slot_id: 0,
            },
            tuple_bytes: vec![0; 128],
        };
        push_inverted_pending(&mut pending, xid, action.clone(), 2, 256).unwrap();
        let error = push_inverted_pending(&mut pending, xid, action, 2, 256).unwrap_err();
        assert!(matches!(error, EngineError::Unsupported(_)));

        let mut pending = HashMap::new();
        let action = InvertedWalAction::Delete {
            tid: Tid {
                page_id: PageId(1),
                slot_id: 0,
            },
        };
        for xid in 0..INVERTED_PENDING_XID_LIMIT as u64 {
            push_inverted_pending(&mut pending, TxnId(xid + 1), action.clone(), 1, usize::MAX)
                .unwrap();
        }
        let error = push_inverted_pending(
            &mut pending,
            TxnId(INVERTED_PENDING_XID_LIMIT as u64 + 1),
            action,
            1,
            usize::MAX,
        )
        .unwrap_err();
        assert!(matches!(error, EngineError::Unsupported(_)));
    }

    #[test]
    fn inverted_lag_and_commit_visibility_latency_are_observed() {
        let tmp = TempDir::new().unwrap();
        let engine = open(tmp.path());
        fn record_commit(engine: &Engine, xid: TxnId) {
            engine
                .inverted_commit_finished
                .lock()
                .insert(xid, Instant::now());
        }
        struct ResetHook;
        impl Drop for ResetHook {
            fn drop(&mut self) {
                COMMIT_FINISHED_HOOK.with(|hook| hook.set(None));
            }
        }
        let _reset = ResetHook;
        COMMIT_FINISHED_HOOK.with(|hook| hook.set(Some(record_commit)));
        engine
            .exec(None, "CREATE TABLE t (id INT4, content TEXT)")
            .unwrap();
        engine
            .exec(
                None,
                "CREATE INDEX content_idx ON t USING inverted (content)",
            )
            .unwrap();
        engine
            .exec(None, "CREATE TABLE other (id INT4, content TEXT)")
            .unwrap();
        engine
            .exec(
                None,
                "CREATE INDEX other_idx ON other USING inverted (content)",
            )
            .unwrap();
        engine.inverted_visibility_samples.lock().clear();
        engine.inverted_commit_finished.lock().clear();
        for id in 0..8 {
            engine
                .exec(
                    None,
                    &format!(
                        "INSERT INTO t VALUES ({id}, '{} contract')",
                        "x".repeat(9_000)
                    ),
                )
                .unwrap();
        }
        engine
            .exec(None, "SELECT id FROM t WHERE content @@ 'contract'")
            .unwrap();
        let index_oid = engine.indexes()[0].index_oid;
        let mut samples = engine.inverted_visibility_samples(index_oid);
        assert_eq!(samples.len(), 8);
        samples.sort();
        let p95 = samples[samples.len() * 95 / 100];
        assert!(p95 < Duration::from_secs(1), "p95 was {p95:?}");
        let metrics = engine.inverted_lag_metrics(index_oid).unwrap().unwrap();
        assert_eq!(metrics.wal_lag_records, 0);
        assert_eq!(metrics.wal_lag_bytes, 0);
        assert_eq!(
            metrics.applied_lsn,
            engine.indexes()[0]
                .inverted
                .as_ref()
                .unwrap()
                .watermark
                .consumed_lsn
        );
        assert!(metrics.synced_lsn >= metrics.applied_lsn);
        assert!(metrics.watermark_lag_seconds >= 0.0);

        engine
            .exec(None, "SELECT id FROM other WHERE content @@ 'contract'")
            .unwrap();
        let other_oid = engine.indexes()[1].index_oid;
        assert_eq!(engine.inverted_visibility_samples(other_oid).len(), 8);

        engine
            .exec(None, "INSERT INTO t VALUES (8, 'contract')")
            .unwrap();
        let lagging = engine.inverted_lag_metrics(index_oid).unwrap().unwrap();
        assert!(lagging.wal_lag_records > 0);
        assert!(lagging.wal_lag_bytes > 0);
        engine
            .exec(None, "SELECT id FROM t WHERE content @@ 'contract'")
            .unwrap();
        // Make the administrative seal marker durable before reading lag:
        // only real data records may contribute to these counters.
        engine.storage.wal_writer().flush().unwrap();
        let caught_up = engine.inverted_lag_metrics(index_oid).unwrap().unwrap();
        assert_eq!(caught_up.wal_lag_records, 0);
        assert_eq!(caught_up.wal_lag_bytes, 0);
        engine.shutdown();
    }

    #[test]
    fn inverted_allow_stale_reads_sealed_generation() {
        let tmp = TempDir::new().unwrap();
        let mut config = EngineConfig::new(tmp.path());
        config.allow_stale_inverted = true;
        let engine = Engine::open(tmp.path(), config).unwrap();
        engine
            .exec(None, "CREATE TABLE t (id INT4, content TEXT)")
            .unwrap();
        engine
            .exec(
                None,
                "CREATE INDEX content_idx ON t USING inverted (content)",
            )
            .unwrap();
        engine
            .exec(None, "INSERT INTO t VALUES (1, 'contract')")
            .unwrap();
        let result = engine
            .exec(None, "SELECT id FROM t WHERE content @@ 'contract'")
            .unwrap();
        assert!(matches!(result, QueryResult::Rows { rows, .. } if rows.is_empty()));
        let explain = engine
            .exec(None, "EXPLAIN SELECT id FROM t WHERE content @@ 'contract'")
            .unwrap();
        assert!(matches!(
            explain,
            QueryResult::Rows { rows, .. }
                if matches!(&rows[0][0], Some(Datum::Text(plan)) if plan.contains("stale_index=true"))
        ));
        engine.shutdown();

        let engine = open(tmp.path());
        let result = engine
            .exec(None, "SELECT id FROM t WHERE content @@ 'contract'")
            .unwrap();
        assert!(matches!(result, QueryResult::Rows { rows, .. } if rows.len() == 1));
        engine.shutdown();
    }

    #[test]
    fn inverted_cross_segment_checkpoint_and_reopen_preserve_documents() {
        let tmp = TempDir::new().unwrap();
        let mut config = EngineConfig::new(tmp.path());
        config.storage.wal_segment_size = 16384;
        let engine = Engine::open(tmp.path(), config.clone()).unwrap();
        engine
            .exec(None, "CREATE TABLE t (id INT4, content TEXT)")
            .unwrap();
        engine
            .exec(
                None,
                "CREATE INDEX content_idx ON t USING inverted (content)",
            )
            .unwrap();
        let query_count = |engine: &Engine| {
            let result = engine
                .exec(None, "SELECT id FROM t WHERE content @@ 'contract'")
                .unwrap();
            let QueryResult::Rows { rows, .. } = result else {
                panic!("expected full-text rows")
            };
            rows.len()
        };
        for id in 0..12 {
            engine
                .exec(
                    None,
                    &format!(
                        "INSERT INTO t VALUES ({id}, '{} contract')",
                        "x".repeat(300)
                    ),
                )
                .unwrap();
        }
        assert_eq!(query_count(&engine), 12);
        engine.checkpoint().unwrap();
        for id in 12..24 {
            engine
                .exec(
                    None,
                    &format!(
                        "INSERT INTO t VALUES ({id}, '{} contract')",
                        "x".repeat(300)
                    ),
                )
                .unwrap();
        }
        assert_eq!(query_count(&engine), 24);
        let before = engine.indexes()[0].inverted.as_ref().unwrap().watermark;
        assert!(before.consumed_lsn >= before.durable_applied_lsn);
        engine.shutdown();

        let engine = Engine::open(tmp.path(), config).unwrap();
        engine
            .exec(None, "INSERT INTO t VALUES (24, 'reopened contract')")
            .unwrap();
        assert_eq!(query_count(&engine), 25);
        assert_eq!(
            engine.query_stats().entries().last().unwrap().path,
            ExecutionPath::IndexLookup
        );
        engine.shutdown();
    }

    #[test]
    fn inverted_wand_top_k_repairs_visibility_filtered_candidates() {
        let tmp = TempDir::new().unwrap();
        let engine = open(tmp.path());
        engine
            .exec(None, "CREATE TABLE t (id INT4, content TEXT)")
            .unwrap();
        engine
            .exec(
                None,
                "CREATE INDEX content_idx ON t USING inverted (content)",
            )
            .unwrap();
        for (id, text) in [
            (1, "shared shared shared"),
            (2, "shared shared"),
            (3, "shared"),
            (4, "shared"),
        ] {
            engine
                .exec(None, &format!("INSERT INTO t VALUES ({id}, '{text}')"))
                .unwrap();
        }
        let query_ids = |engine: &Engine| {
            let result = engine
                .exec(
                    None,
                    "SELECT id FROM t WHERE content @@ 'shared' ORDER BY bm25_score(content, 'shared') DESC LIMIT 2",
                )
                .unwrap();
            let QueryResult::Rows { rows, .. } = result else {
                panic!("expected WAND rows")
            };
            rows.into_iter()
                .map(|row| match row[0] {
                    Some(Datum::Int4(id)) => id,
                    ref other => panic!("unexpected id {other:?}"),
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(query_ids(&engine), vec![1, 2]);
        engine.shutdown();
        let mut config = EngineConfig::new(tmp.path());
        config.allow_stale_inverted = true;
        let engine = Engine::open(tmp.path(), config).unwrap();
        engine.exec(None, "DELETE FROM t WHERE id = 1").unwrap();
        // The stale WAND shortlist contains the deleted leader. MVCC filtering
        // must trigger a full DAAT repair rather than returning a short page.
        assert_eq!(query_ids(&engine), vec![2, 3]);
        assert_eq!(
            engine.query_stats().entries().last().unwrap().path,
            ExecutionPath::IndexLookup
        );
        engine.shutdown();
    }

    #[test]
    fn inverted_seal_capacity_failure_degrades_and_recovers_by_replay() {
        let tmp = TempDir::new().unwrap();
        let engine = open(tmp.path());
        engine
            .exec(None, "CREATE TABLE t (id INT4, content TEXT)")
            .unwrap();
        engine
            .exec(
                None,
                "CREATE INDEX content_idx ON t USING inverted (content)",
            )
            .unwrap();
        let index_oid = engine.indexes()[0].index_oid;
        let query_count = |engine: &Engine| {
            let result = engine
                .exec(None, "SELECT id FROM t WHERE content @@ 'shared'")
                .unwrap();
            let QueryResult::Rows { rows, .. } = result else {
                panic!("expected full-text rows")
            };
            rows.len()
        };
        for id in 1..=3 {
            engine
                .exec(None, &format!("INSERT INTO t VALUES ({id}, 'shared')"))
                .unwrap();
            assert_eq!(query_count(&engine), id);
        }
        let handle = engine.inverted_handle(index_oid).unwrap();
        let attempts_before = handle.lock().seal_attempts_for_test();
        handle.lock().inject_seal_capacity_error();
        engine
            .exec(None, "INSERT INTO t VALUES (4, 'shared')")
            .unwrap();
        assert_eq!(query_count(&engine), 4);
        assert_eq!(handle.lock().seal_attempts_for_test(), attempts_before + 1);
        assert!(handle.lock().seal_capacity_blocked());
        let watermark = handle.lock().watermark();
        assert!(watermark.durable_applied_lsn < watermark.consumed_lsn);

        engine
            .exec(None, "INSERT INTO t VALUES (5, 'shared')")
            .unwrap();
        assert_eq!(query_count(&engine), 5);
        assert_eq!(handle.lock().seal_attempts_for_test(), attempts_before + 1);
        assert_eq!(handle.lock().active_segment_count(), 3);
        engine.shutdown();

        let engine = open(tmp.path());
        let handle = engine.inverted_handle(index_oid).unwrap();
        handle.lock().inject_seal_capacity_error();
        assert_eq!(query_count(&engine), 5);
        assert_eq!(handle.lock().seal_attempts_for_test(), 1);
        assert!(handle.lock().seal_capacity_blocked());
        engine
            .exec(None, "INSERT INTO t VALUES (6, 'shared')")
            .unwrap();
        assert_eq!(query_count(&engine), 6);
        assert_eq!(handle.lock().seal_attempts_for_test(), 1);
        assert_eq!(handle.lock().active_segment_count(), 3);
        engine.shutdown();
    }

    #[test]
    fn inverted_capacity_failure_degrades_to_multi_segment_reads() {
        let tmp = TempDir::new().unwrap();
        let engine = open(tmp.path());
        engine
            .exec(None, "CREATE TABLE t (id INT4, content TEXT)")
            .unwrap();
        engine
            .exec(
                None,
                "CREATE INDEX content_idx ON t USING inverted (content)",
            )
            .unwrap();
        let index_oid = engine.indexes()[0].index_oid;
        let query_count = |engine: &Engine| {
            let result = engine
                .exec(None, "SELECT id FROM t WHERE content @@ 'shared'")
                .unwrap();
            let QueryResult::Rows { rows, .. } = result else {
                panic!("expected full-text rows")
            };
            rows.len()
        };
        for id in 1..=3 {
            engine
                .exec(None, &format!("INSERT INTO t VALUES ({id}, 'shared')"))
                .unwrap();
            assert_eq!(query_count(&engine), id);
        }
        let handle = engine.inverted_handle(index_oid).unwrap();
        handle.lock().inject_merge_capacity_error();
        engine
            .exec(None, "INSERT INTO t VALUES (4, 'shared')")
            .unwrap();
        assert_eq!(query_count(&engine), 4);
        assert_eq!(handle.lock().merge_attempts_for_test(), 1);
        assert!(handle.lock().merge_capacity_blocked());

        engine
            .exec(None, "INSERT INTO t VALUES (5, 'shared')")
            .unwrap();
        assert_eq!(query_count(&engine), 5);
        assert_eq!(handle.lock().merge_attempts_for_test(), 1);
        assert_eq!(handle.lock().active_segment_count(), 5);
        engine.shutdown();
    }

    #[test]
    fn inverted_drop_and_recreate_does_not_resurrect_old_documents() {
        let tmp = TempDir::new().unwrap();
        let engine = open(tmp.path());
        engine
            .exec(None, "CREATE TABLE t (id INT4, content TEXT)")
            .unwrap();
        engine
            .exec(
                None,
                "CREATE INDEX content_idx ON t USING inverted (content)",
            )
            .unwrap();
        let old_oid = engine.indexes()[0].index_oid;
        engine
            .exec(None, "INSERT INTO t VALUES (1, 'oldterm')")
            .unwrap();
        engine.drop_table("t").unwrap();
        engine
            .exec(None, "CREATE TABLE t (id INT4, content TEXT)")
            .unwrap();
        engine
            .exec(
                None,
                "CREATE INDEX content_idx ON t USING inverted (content)",
            )
            .unwrap();
        let new_oid = engine.indexes()[0].index_oid;
        assert_ne!(old_oid, new_oid);
        engine
            .exec(None, "INSERT INTO t VALUES (2, 'newterm')")
            .unwrap();
        let count = |engine: &Engine, term: &str| {
            let result = engine
                .exec(None, &format!("SELECT id FROM t WHERE content @@ '{term}'"))
                .unwrap();
            let QueryResult::Rows { rows, .. } = result else {
                panic!("expected full-text rows")
            };
            rows.len()
        };
        assert_eq!(count(&engine, "oldterm"), 0);
        assert_eq!(count(&engine, "newterm"), 1);
        engine.shutdown();

        let engine = open(tmp.path());
        assert_eq!(count(&engine, "oldterm"), 0);
        assert_eq!(count(&engine, "newterm"), 1);
        engine.shutdown();
    }

    #[test]
    fn inverted_auto_merge_preserves_updates_deletes_toast_and_reopen() {
        let tmp = TempDir::new().unwrap();
        let engine = open(tmp.path());
        engine
            .exec(None, "CREATE TABLE t (id INT4, content TEXT)")
            .unwrap();
        engine
            .exec(
                None,
                "CREATE INDEX content_idx ON t USING inverted (content)",
            )
            .unwrap();
        let index_oid = engine.indexes()[0].index_oid;
        let query_ids = |engine: &Engine, term: &str| {
            let result = engine
                .exec(None, &format!("SELECT id FROM t WHERE content @@ '{term}'"))
                .unwrap();
            let QueryResult::Rows { rows, .. } = result else {
                panic!("expected full-text rows")
            };
            let mut ids = rows
                .into_iter()
                .map(|row| match row[0] {
                    Some(Datum::Int4(id)) => id,
                    ref other => panic!("unexpected id {other:?}"),
                })
                .collect::<Vec<_>>();
            ids.sort_unstable();
            ids
        };
        for id in 1..=4 {
            let text = if id == 4 {
                format!("shared {}", "toast ".repeat(2000))
            } else {
                format!("shared unique{id}")
            };
            engine
                .exec(None, &format!("INSERT INTO t VALUES ({id}, '{text}')"))
                .unwrap();
            assert_eq!(query_ids(&engine, "shared").len(), id as usize);
        }
        assert_eq!(
            engine
                .inverted_handle(index_oid)
                .unwrap()
                .lock()
                .active_segment_count(),
            1
        );
        engine
            .exec(None, "UPDATE t SET content = 'changed shared' WHERE id = 1")
            .unwrap();
        assert_eq!(query_ids(&engine, "changed"), vec![1]);
        engine.exec(None, "DELETE FROM t WHERE id = 2").unwrap();
        assert_eq!(query_ids(&engine, "shared"), vec![1, 3, 4]);
        assert!(query_ids(&engine, "unique1").is_empty());
        assert!(query_ids(&engine, "unique2").is_empty());
        engine.shutdown();

        let engine = open(tmp.path());
        assert_eq!(query_ids(&engine, "shared"), vec![1, 3, 4]);
        engine
            .exec(None, "INSERT INTO t VALUES (5, 'shared reopened')")
            .unwrap();
        assert_eq!(query_ids(&engine, "shared"), vec![1, 3, 4, 5]);
        assert_eq!(
            engine
                .inverted_handle(index_oid)
                .unwrap()
                .lock()
                .active_segment_count(),
            1
        );
        assert_eq!(
            engine.query_stats().entries().last().unwrap().path,
            ExecutionPath::IndexLookup
        );
        engine.shutdown();

        let engine = open(tmp.path());
        assert_eq!(query_ids(&engine, "shared"), vec![1, 3, 4, 5]);
        assert_eq!(query_ids(&engine, "toast"), vec![4]);
        engine.shutdown();
    }

    #[test]
    fn inverted_internal_markers_do_not_skip_concurrent_commits() {
        fn inject_commit(engine: &Engine) {
            engine
                .exec(None, "INSERT INTO t VALUES (5, 'shared injected')")
                .unwrap();
        }
        struct ResetHook;
        impl Drop for ResetHook {
            fn drop(&mut self) {
                BEFORE_INVERTED_SEAL_HOOK.with(|hook| hook.set(None));
            }
        }

        let tmp = TempDir::new().unwrap();
        let engine = open(tmp.path());
        let _reset = ResetHook;
        engine
            .exec(None, "CREATE TABLE t (id INT4, content TEXT)")
            .unwrap();
        engine
            .exec(
                None,
                "CREATE INDEX content_idx ON t USING inverted (content)",
            )
            .unwrap();
        let query_count = |engine: &Engine| {
            let result = engine
                .exec(None, "SELECT id FROM t WHERE content @@ 'shared'")
                .unwrap();
            let QueryResult::Rows { rows, .. } = result else {
                panic!("expected full-text rows")
            };
            rows.len()
        };
        for id in 1..=3 {
            engine
                .exec(None, &format!("INSERT INTO t VALUES ({id}, 'shared')"))
                .unwrap();
            assert_eq!(query_count(&engine), id);
        }
        engine
            .exec(None, "INSERT INTO t VALUES (4, 'shared')")
            .unwrap();
        BEFORE_INVERTED_SEAL_HOOK.with(|hook| hook.set(Some(inject_commit)));
        // The injected commit lands after the replay frontier but before the
        // seal/merge markers. This snapshot predates it, so four rows are visible.
        assert_eq!(query_count(&engine), 4);
        // The next refresh must consume the injected row rather than jumping
        // to the administrative marker's end LSN.
        assert_eq!(query_count(&engine), 5);
        let index_oid = engine.indexes()[0].index_oid;
        assert_eq!(
            engine
                .inverted_handle(index_oid)
                .unwrap()
                .lock()
                .active_segment_count(),
            2 // merged generation plus the injected commit's delta
        );
        assert_eq!(
            engine.query_stats().entries().last().unwrap().path,
            ExecutionPath::IndexLookup
        );
        engine.shutdown();

        let engine = open(tmp.path());
        assert_eq!(query_count(&engine), 5);
        engine.shutdown();
    }

    #[test]
    fn inverted_backfill_and_catalog_survive_reopen() {
        let tmp = TempDir::new().unwrap();
        assert!(!EngineConfig::new(tmp.path()).allow_stale_inverted);
        let engine = open(tmp.path());
        assert!(!engine.allow_stale_inverted());
        assert!(engine
            .catalog
            .access_methods()
            .iter()
            .any(|am| am.oid == INVERTED_AM_OID && am.name == "inverted"));
        engine
            .exec(None, "CREATE TABLE agent_memory (id INT4, content TEXT)")
            .unwrap();
        engine
            .exec(
                None,
                "INSERT INTO agent_memory VALUES (1, 'rust contract'), (2, 'other document')",
            )
            .unwrap();
        engine
            .insert(
                "agent_memory",
                &[
                    Some(Datum::Int4(9)),
                    Some(Datum::Text(format!("{} toasted", "x".repeat(9000)))),
                ],
            )
            .unwrap();
        engine
            .exec(
                None,
                "CREATE INDEX content_idx ON agent_memory USING inverted (content) WITH (fulltext_index='bm25')",
            )
            .unwrap();
        let result = engine
            .exec(
                None,
                "SELECT id FROM agent_memory WHERE content @@ 'contract'",
            )
            .unwrap();
        let QueryResult::Rows { rows, .. } = result else {
            panic!("expected full-text rows")
        };
        assert_eq!(rows, vec![vec![Some(Datum::Int4(1))]]);
        assert_eq!(
            engine.query_stats().entries().last().unwrap().path,
            ExecutionPath::IndexLookup
        );
        let result = engine
            .exec(
                None,
                "SELECT id, bm25_score(content, 'contract') AS score FROM agent_memory WHERE content @@ 'contract' ORDER BY bm25_score(content, 'contract') DESC",
            )
            .unwrap();
        let QueryResult::Rows { columns, rows } = result else {
            panic!("expected BM25 projection")
        };
        assert_eq!(columns, vec!["id", "score"]);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Some(Datum::Int4(1)));
        assert!(matches!(rows[0][1], Some(Datum::Float8(score)) if score > 0.0));

        let result = engine
            .exec(
                None,
                "SELECT id, bm25_score(content, 'contract') AS score FROM agent_memory WHERE content @@ 'contract' AND id > 0 ORDER BY bm25_score(content, 'contract') DESC",
            )
            .unwrap();
        let QueryResult::Rows { rows, .. } = result else {
            panic!("expected exact ExtraWhere BM25 projection")
        };
        assert_eq!(rows.len(), 1);
        assert!(matches!(rows[0][1], Some(Datum::Float8(score)) if score > 0.0));
        assert_eq!(
            engine.query_stats().entries().last().unwrap().path,
            ExecutionPath::SeqScan
        );
        let explain = engine
            .exec(
                None,
                "EXPLAIN SELECT id FROM agent_memory WHERE content @@ 'contract'",
            )
            .unwrap();
        assert!(matches!(
            explain,
            QueryResult::Rows { rows, .. }
                if matches!(&rows[0][0], Some(Datum::Text(plan)) if plan.contains("InvertedScan"))
        ));
        let query_ids = |engine: &Engine, sql: &str| {
            let result = engine.exec(None, sql).unwrap();
            let QueryResult::Rows { rows, .. } = result else {
                panic!("expected ID rows")
            };
            rows.into_iter()
                .map(|row| match &row[0] {
                    Some(Datum::Int4(id)) => i64::from(*id),
                    other => panic!("unexpected ID value {other:?}"),
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(
            query_ids(
                &engine,
                "SELECT id FROM agent_memory WHERE content @@ 'toasted'"
            ),
            vec![9]
        );
        engine
            .insert(
                "agent_memory",
                &[
                    Some(Datum::Int4(10)),
                    Some(Datum::Text(format!("{} aftertoast", "y".repeat(9000)))),
                ],
            )
            .unwrap();
        assert_eq!(
            query_ids(
                &engine,
                "SELECT id FROM agent_memory WHERE content @@ 'aftertoast'"
            ),
            vec![10]
        );
        assert_eq!(
            engine.query_stats().entries().last().unwrap().path,
            ExecutionPath::IndexLookup
        );
        engine
            .exec(
                None,
                "INSERT INTO agent_memory VALUES (3, 'fresh contract')",
            )
            .unwrap();
        assert_eq!(
            query_ids(
                &engine,
                "SELECT id FROM agent_memory WHERE content @@ 'contract' ORDER BY id"
            ),
            vec![1, 3]
        );
        assert_eq!(
            engine.query_stats().entries().last().unwrap().path,
            ExecutionPath::IndexLookup
        );
        engine
            .exec(
                None,
                "UPDATE agent_memory SET content = 'changed text' WHERE id = 3",
            )
            .unwrap();
        assert_eq!(
            query_ids(
                &engine,
                "SELECT id FROM agent_memory WHERE content @@ 'contract'"
            ),
            vec![1]
        );
        assert_eq!(
            query_ids(
                &engine,
                "SELECT id FROM agent_memory WHERE content @@ 'changed'"
            ),
            vec![3]
        );
        engine
            .exec(None, "DELETE FROM agent_memory WHERE id = 3")
            .unwrap();
        assert_eq!(
            query_ids(
                &engine,
                "SELECT id FROM agent_memory WHERE content @@ 'changed'"
            ),
            Vec::<i64>::new()
        );
        let txn = engine.begin_txn().unwrap();
        engine
            .exec(
                Some(&txn),
                "INSERT INTO agent_memory VALUES (11, 'own contract')",
            )
            .unwrap();
        let own = engine
            .exec(
                Some(&txn),
                "SELECT id FROM agent_memory WHERE content @@ 'own'",
            )
            .unwrap();
        assert!(matches!(own, QueryResult::Rows { rows, .. } if rows.len() == 1));
        assert_eq!(
            engine.query_stats().entries().last().unwrap().path,
            ExecutionPath::SeqScan
        );
        txn.commit().unwrap();
        assert_eq!(
            query_ids(
                &engine,
                "SELECT id FROM agent_memory WHERE content @@ 'own'"
            ),
            vec![11]
        );
        assert_eq!(
            engine.query_stats().entries().last().unwrap().path,
            ExecutionPath::IndexLookup
        );
        engine
            .exec(None, "DELETE FROM agent_memory WHERE id = 11")
            .unwrap();

        let txn = engine.begin_txn().unwrap();
        engine
            .exec(
                Some(&txn),
                "INSERT INTO agent_memory VALUES (4, 'aborted contract')",
            )
            .unwrap();
        txn.abort().unwrap();
        assert_eq!(
            query_ids(
                &engine,
                "SELECT id FROM agent_memory WHERE content @@ 'aborted'"
            ),
            Vec::<i64>::new()
        );
        engine.shutdown();

        let mut config = EngineConfig::new(tmp.path());
        config.allow_stale_inverted = true;
        let engine = Engine::open(tmp.path(), config).unwrap();
        assert!(engine.allow_stale_inverted());
        let indexes = engine.indexes();
        assert_eq!(indexes.len(), 1);
        assert_eq!(indexes[0].kind, AmKind::Inverted);
        assert_eq!(
            indexes[0].inverted.as_ref().unwrap().analyzer,
            pg_am_inverted::AnalyzerConfig::builtin()
        );
        assert_eq!(
            query_ids(
                &engine,
                "SELECT id FROM agent_memory WHERE content @@ 'contract'"
            ),
            vec![1]
        );
        assert_eq!(
            engine.query_stats().entries().last().unwrap().path,
            ExecutionPath::IndexLookup
        );
        engine.shutdown();
    }

    #[test]
    fn empty_inverted_index_rejects_analyzer_mismatch() {
        let tmp = TempDir::new().unwrap();
        let mut config = EngineConfig::new(tmp.path());
        config.inverted_analyzer = pg_am_inverted::AnalyzerConfig::english();
        let engine = Engine::open(tmp.path(), config.clone()).unwrap();
        engine.exec(None, "CREATE TABLE t (content TEXT)").unwrap();
        engine
            .exec(
                None,
                "CREATE INDEX content_idx ON t USING inverted (content)",
            )
            .unwrap();
        engine.shutdown();

        config.inverted_analyzer = pg_am_inverted::AnalyzerConfig::chinese();
        let Err(error) = Engine::open(tmp.path(), config) else {
            panic!("empty index accepted a different analyzer")
        };
        assert!(error.to_string().contains("analyzer"), "{error}");
    }

    #[test]
    fn english_and_chinese_analyzers_reopen_with_matching_config() {
        for analyzer in [
            pg_am_inverted::AnalyzerConfig::english(),
            pg_am_inverted::AnalyzerConfig::chinese(),
        ] {
            let tmp = TempDir::new().unwrap();
            let mut config = EngineConfig::new(tmp.path());
            config.inverted_analyzer = analyzer.clone();
            let engine = Engine::open(tmp.path(), config.clone()).unwrap();
            engine.exec(None, "CREATE TABLE t (content TEXT)").unwrap();
            engine.create_index("t", "content").unwrap();
            let index_oid = engine.indexes()[0].index_oid;
            set_index_am_for_test(&engine, index_oid, INVERTED_AM_OID);
            write_inverted_metadata_with_analyzer_for_test(
                tmp.path(),
                index_oid,
                Lsn(128),
                analyzer.clone(),
            );
            engine.shutdown();

            let engine = Engine::open(tmp.path(), config).unwrap();
            let entries = engine.indexes();
            assert_eq!(entries.len(), 1);
            let metadata = entries[0].inverted.as_ref().unwrap();
            assert_eq!(metadata.analyzer, analyzer);
            assert_eq!(metadata.watermark.consumed_lsn, Lsn(128));
            engine.shutdown();
        }
    }

    #[test]
    fn legacy_directory_without_inverted_am_reopens() {
        let tmp = TempDir::new().unwrap();
        let engine = open(tmp.path());
        delete_am_row_for_test(&engine, INVERTED_AM_OID);
        engine.shutdown();

        let engine = open(tmp.path());
        assert!(engine
            .catalog
            .access_methods()
            .iter()
            .all(|am| am.oid != INVERTED_AM_OID));
        engine.shutdown();
    }

    #[test]
    fn inverted_registry_recovers_analyzer_and_watermark_contract() {
        let tmp = TempDir::new().unwrap();
        let engine = open(tmp.path());
        engine.exec(None, "CREATE TABLE t (content TEXT)").unwrap();
        engine.create_index("t", "content").unwrap();
        let index_oid = engine.indexes()[0].index_oid;
        set_index_am_for_test(&engine, index_oid, INVERTED_AM_OID);
        write_inverted_metadata_for_test(tmp.path(), index_oid, Lsn(128));
        engine.shutdown();

        let engine = open(tmp.path());
        let entries = engine.indexes();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].kind, AmKind::Inverted);
        let metadata = entries[0].inverted.as_ref().unwrap();
        metadata.analyzer.validate().unwrap();
        assert_eq!(metadata.watermark.consumed_lsn, Lsn(128));
        assert_eq!(metadata.watermark.durable_applied_lsn, Lsn(128));
        engine.shutdown();
    }

    #[test]
    fn inverted_scan_request_preserves_index_oid_and_recovered_metadata() {
        let tmp = TempDir::new().unwrap();
        let engine = open(tmp.path());
        engine.exec(None, "CREATE TABLE t (content TEXT)").unwrap();
        engine.create_index("t", "content").unwrap();
        let index_oid = engine.indexes()[0].index_oid;
        set_index_am_for_test(&engine, index_oid, INVERTED_AM_OID);
        write_inverted_metadata_for_test(tmp.path(), index_oid, Lsn(128));
        engine.shutdown();

        let engine = open(tmp.path());
        let entry = engine.table_entry("t").unwrap();
        let filter = Some(Filter {
            column: "content".to_string(),
            op: CmpOp::FullText,
            value: Literal::Str("contract".to_string()),
        });
        let request =
            crate::executor::build_request(&engine, "t", &entry, &filter, &None, &Some(10))
                .unwrap();
        assert_eq!(request.indexes.len(), 1);
        assert_eq!(request.indexes[0].index_oid, index_oid);
        let metadata = request.indexes[0].inverted.as_ref().unwrap();
        assert_eq!(metadata.analyzer, pg_am_inverted::AnalyzerConfig::builtin());
        assert_eq!(metadata.watermark.consumed_lsn, request.snapshot_frontier);
        assert!(metadata.watermark.consumed_lsn >= Lsn(128));
        assert_eq!(metadata.watermark.durable_applied_lsn, Lsn(128));
        engine.shutdown();
    }

    #[test]
    fn inverted_registry_requires_real_segment_metadata() {
        let tmp = TempDir::new().unwrap();
        let engine = open(tmp.path());
        engine.exec(None, "CREATE TABLE t (content TEXT)").unwrap();
        engine.create_index("t", "content").unwrap();
        let index_oid = engine.indexes()[0].index_oid;
        set_index_am_for_test(&engine, index_oid, INVERTED_AM_OID);
        engine.shutdown();

        let Err(error) = Engine::open(tmp.path(), EngineConfig::new(tmp.path())) else {
            panic!("missing segment metadata must not be replaced with defaults")
        };
        assert!(
            error.to_string().contains("cannot recover inverted index"),
            "{error}"
        );
    }

    #[test]
    fn inverted_registry_rejects_analyzer_hash_mismatch() {
        let tmp = TempDir::new().unwrap();
        let engine = open(tmp.path());
        engine.exec(None, "CREATE TABLE t (content TEXT)").unwrap();
        engine.create_index("t", "content").unwrap();
        let index_oid = engine.indexes()[0].index_oid;
        set_index_am_for_test(&engine, index_oid, INVERTED_AM_OID);
        write_inverted_metadata_for_test(tmp.path(), index_oid, Lsn(128));
        let segment_id = pg_storage::segment::SegmentId(7);
        let path =
            pg_am_inverted::SegmentStore::segment_metadata_path(tmp.path(), index_oid, segment_id);
        let mut metadata =
            pg_am_inverted::SegmentMeta::decode(&std::fs::read(&path).unwrap()).unwrap();
        metadata.analyzer.config_hash = [1; 32];
        std::fs::write(path, metadata.encode().unwrap()).unwrap();
        engine.shutdown();

        let Err(error) = Engine::open(tmp.path(), EngineConfig::new(tmp.path())) else {
            panic!("a nonzero but wrong analyzer hash must be rejected")
        };
        assert!(
            error
                .to_string()
                .contains("analyzer configuration mismatch"),
            "{error}"
        );
    }

    #[test]
    fn inverted_index_without_pg_am_row_is_not_treated_as_btree() {
        let tmp = TempDir::new().unwrap();
        let engine = open(tmp.path());
        engine.exec(None, "CREATE TABLE t (id INT4)").unwrap();
        engine.create_index("t", "id").unwrap();
        let index_oid = engine.indexes()[0].index_oid;
        set_index_am_for_test(&engine, index_oid, INVERTED_AM_OID);
        delete_am_row_for_test(&engine, INVERTED_AM_OID);
        engine.shutdown();

        let Err(error) = Engine::open(tmp.path(), EngineConfig::new(tmp.path())) else {
            panic!("missing pg_am row must reject the inverted index")
        };
        assert!(error.to_string().contains("no pg_am row"), "{error}");
    }

    #[test]
    fn inverted_registry_rejects_wrong_pg_am_name() {
        let tmp = TempDir::new().unwrap();
        let engine = open(tmp.path());
        engine.exec(None, "CREATE TABLE t (content TEXT)").unwrap();
        engine.create_index("t", "content").unwrap();
        let index_oid = engine.indexes()[0].index_oid;
        set_index_am_for_test(&engine, index_oid, INVERTED_AM_OID);
        write_inverted_metadata_for_test(tmp.path(), index_oid, Lsn(128));
        rename_am_for_test(&engine, INVERTED_AM_OID, "btree___");
        engine.shutdown();

        let Err(error) = Engine::open(tmp.path(), EngineConfig::new(tmp.path())) else {
            panic!("the inverted OID with a B+Tree name must be rejected")
        };
        assert!(error.to_string().contains("amname"), "{error}");
    }

    #[test]
    fn inverted_registry_rejects_non_text_columns() {
        let tmp = TempDir::new().unwrap();
        let engine = open(tmp.path());
        engine.exec(None, "CREATE TABLE t (id INT4)").unwrap();
        engine.create_index("t", "id").unwrap();
        let index_oid = engine.indexes()[0].index_oid;
        set_index_am_for_test(&engine, index_oid, INVERTED_AM_OID);
        engine.shutdown();

        let Err(error) = Engine::open(tmp.path(), EngineConfig::new(tmp.path())) else {
            panic!("INT4 inverted index must fail during registry rebuild")
        };
        assert!(
            error.to_string().contains("requires a TEXT column"),
            "{error}"
        );
    }

    #[test]
    fn inverted_dml_never_uses_btree_maintenance_or_lookup() {
        let tmp = TempDir::new().unwrap();
        let engine = open(tmp.path());
        engine.exec(None, "CREATE TABLE t (content TEXT)").unwrap();
        engine.create_index("t", "content").unwrap();
        engine
            .insert("t", &[Some(Datum::Text("before".to_string()))])
            .unwrap();
        let index_oid = engine.indexes()[0].index_oid;
        set_index_am_for_test(&engine, index_oid, INVERTED_AM_OID);
        write_inverted_metadata_for_test(tmp.path(), index_oid, Lsn(128));
        engine.shutdown();

        let engine = open(tmp.path());
        engine
            .insert("t", &[Some(Datum::Text("contract".to_string()))])
            .unwrap();
        assert!(matches!(
            engine.index_lookup("t", "content", &Datum::Text("contract".to_string())),
            Err(EngineError::IndexNotFound(_))
        ));
        assert!(matches!(
            engine.btree_index("t", "content"),
            Err(EngineError::IndexNotFound(_))
        ));
        let result = engine
            .exec(None, "UPDATE t SET content = 'updated'")
            .unwrap();
        assert!(matches!(result, QueryResult::Affected(2)));
        let result = engine
            .exec(None, "DELETE FROM t WHERE content = 'updated'")
            .unwrap();
        assert!(matches!(result, QueryResult::Affected(2)));
        assert!(engine.scan("t", None).unwrap().is_empty());
        engine.shutdown();
    }

    #[test]
    fn fulltext_parameters_are_typed_and_never_become_equality() {
        let tmp = TempDir::new().unwrap();
        let engine = open(tmp.path());
        engine
            .exec(None, "CREATE TABLE agent_memory (id INT4, content TEXT)")
            .unwrap();
        let stmt = sql::parse("SELECT * FROM agent_memory WHERE content @@ $1").unwrap();
        assert_eq!(
            engine.parameter_types(&stmt).unwrap(),
            vec![Some(ColumnType::Text)]
        );
        let non_text = sql::parse("SELECT * FROM agent_memory WHERE id @@ $1").unwrap();
        assert!(matches!(
            engine.parameter_types(&non_text),
            Err(EngineError::InvalidArgument(_))
        ));
        assert!(engine
            .exec(None, "SELECT * FROM agent_memory WHERE id @@ 'contract'")
            .is_err());
        engine
            .exec(
                None,
                "INSERT INTO agent_memory VALUES (1, 'rust contract'), (2, 'other document')",
            )
            .unwrap();
        let result = engine
            .exec_with_params(
                None,
                "SELECT * FROM agent_memory WHERE content @@ $1",
                &[Some(Datum::Text("contract".to_string()))],
            )
            .unwrap();
        assert!(matches!(result, QueryResult::Rows { rows, .. } if rows.len() == 1));

        let stmt =
            sql::parse("SELECT * FROM agent_memory WHERE content @@ $1 AND id > $2").unwrap();
        assert_eq!(
            engine.parameter_types(&stmt).unwrap(),
            vec![Some(ColumnType::Text), Some(ColumnType::Int4)]
        );
        let result = engine
            .exec_with_params(
                None,
                "SELECT * FROM agent_memory WHERE content @@ $1 AND id > $2",
                &[
                    Some(Datum::Text("contract".to_string())),
                    Some(Datum::Int4(0)),
                ],
            )
            .unwrap();
        assert!(matches!(result, QueryResult::Rows { rows, .. } if rows.len() == 1));
        assert_eq!(
            engine.query_stats().entries().last().unwrap().path,
            ExecutionPath::SeqScan
        );
        engine.shutdown();
    }

    #[test]
    fn plain_bm25_score_column_still_executes_normally() {
        let tmp = TempDir::new().unwrap();
        let engine = open(tmp.path());
        engine
            .exec(None, "CREATE TABLE t (bm25_score FLOAT8)")
            .unwrap();
        engine
            .exec(None, "INSERT INTO t VALUES (2.5), (1.5)")
            .unwrap();
        let result = engine
            .exec(None, "SELECT bm25_score FROM t ORDER BY bm25_score")
            .unwrap();
        let QueryResult::Rows { columns, rows } = result else {
            panic!("expected rows")
        };
        assert_eq!(columns, vec!["bm25_score"]);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], Some(Datum::Float8(1.5)));
        assert_eq!(rows[1][0], Some(Datum::Float8(2.5)));
        engine.shutdown();
    }

    #[test]
    fn vacuum_skips_inverted_metadata_instead_of_opening_btree() {
        let tmp = TempDir::new().unwrap();
        let engine = open(tmp.path());
        engine
            .exec(None, "CREATE TABLE t (id INT4, content TEXT)")
            .unwrap();
        engine
            .exec(None, "INSERT INTO t VALUES (1, 'contract')")
            .unwrap();
        engine.exec(None, "DELETE FROM t WHERE id = 1").unwrap();
        let table_oid = engine.table_entry("t").unwrap().oid;
        engine.indexes.write().push(IndexEntry {
            kind: AmKind::Inverted,
            inverted: Some(InvertedIndexMetadata::initial()),
            index_oid: Oid(9879),
            table_oid,
            column: "content".to_string(),
            key_type: ColumnType::Text,
            // This is not a B+Tree page: any accidental B+Tree open fails.
            meta_page: PageId(0),
            map_page: None,
        });
        let stats = engine.vacuum("t").unwrap();
        assert_eq!(stats.index_entries_removed, 0);
        engine.shutdown();
    }
}
