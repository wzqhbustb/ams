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
use pg_am_heap::line_pointer::LINE_POINTER_SIZE;
use pg_am_heap::tuple::{decode_tuple, encode_tuple, ColumnType, Datum, TupleHeader};
use pg_am_heap::{
    heap_redo_handlers, AccessMethod, HeapAM, HeapUndoHandler, SlottedPage, UpdatableAM,
};
use pg_catalog::builtin_types::{builtin_type, BUILTIN_TYPES};
use pg_catalog::system_tables::{
    SystemTableDef, BTREE_AM_OID, HEAP_AM_OID, PG_ATTRIBUTE, PG_CLASS, PG_INDEX, PG_RELPAGES,
    RELKIND_INDEX, RELKIND_TABLE,
};
use pg_catalog::{Catalog, RelationRow, TypeOid};
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
use crate::query_stats::{ExecutionPath, QueryStatEntry, QueryStats, DEFAULT_QUERY_STATS_CAPACITY};
use crate::sql::{self, CmpOp, Filter, Literal, LockClause, OrderBy, SelectCols, Statement};

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

/// What a transaction did to one index entry, for undo purposes (Stage O
/// review: index maintenance is not MVCC-covered by heap headers, so the
/// engine keeps an explicit per-transaction undo log).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IndexUndoOp {
    /// The transaction inserted `(key, tid)`; abort removes it.
    Inserted,
    /// The transaction deleted `(key, tid)`; abort re-inserts it.
    Deleted,
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
}

impl EngineConfig {
    /// Default configuration rooted at `data_dir`.
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            storage: StorageConfig::new(data_dir),
            clog_buffer_frames: DEFAULT_CLOG_BUFFER_FRAMES,
            deadlock_detector_interval: pg_txn::DEFAULT_DEADLOCK_INTERVAL,
            query_stats_capacity: DEFAULT_QUERY_STATS_CAPACITY,
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

/// The outcome of an [`Engine::exec`] call.
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
}

impl Predicate {
    /// The 0-based column index this predicate filters on.
    pub fn col_index(&self) -> usize {
        match self {
            Predicate::Eq { col_index, .. }
            | Predicate::Lt { col_index, .. }
            | Predicate::Gt { col_index, .. } => *col_index,
        }
    }

    /// The comparison value.
    pub fn value(&self) -> &Datum {
        match self {
            Predicate::Eq { value, .. }
            | Predicate::Lt { value, .. }
            | Predicate::Gt { value, .. } => value,
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
        }
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
    /// The index relation's OID (`pg_index.indexrelid`).
    pub index_oid: Oid,
    /// The indexed table's OID (`pg_index.indrelid`).
    pub table_oid: Oid,
    /// The indexed column's name (from the index's `pg_attribute` row).
    pub column: String,
    /// The indexed column's codec type (the B+Tree key type).
    pub key_type: ColumnType,
    /// The index's meta page (`pg_rust_relpages.first_page` of the index).
    pub meta_page: PageId,
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
    /// Index entries physically removed by this pass (`BTreeDelete` WAL).
    /// Under eager online index maintenance this is essentially only
    /// crash-loser dangling entries (§4.3).
    pub index_entries_removed: usize,
    /// Phase-4 delete attempts that found the entry already gone
    /// (`EntryNotFound` → Ok at the vacuum call site): eager online index
    /// maintenance had removed them.
    pub index_entries_already_gone: usize,
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
    /// [`Self::exec`] records one entry per statement — see
    /// [`Self::query_stats`].
    query_stats: QueryStats,
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
    /// either order would be correct). Never read — RAII only.
    _snapshot_guard: SnapshotGuard,
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
}

impl TxnHandle {
    /// The transaction's XID.
    pub fn xid(&self) -> TxnId {
        self.xid.expect("xid accessed after commit/abort")
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
            apply_index_undo(&self.index_undo, &self.buffer_pool, &self.wal_writer, xid);
            if let Err(e) = self.txn.abort_txn(xid) {
                tracing::warn!(error = %e, xid = xid.0, "commit failure fallback abort failed");
            }
        }
        // 2PL release point (M2c Stage P): table locks go AFTER the CLOG
        // bit flips, so a woken row-lock waiter that then needs this
        // transaction's table locks never observes the reverse order.
        self.lock_manager.release_all(xid);
        // Discard the undo log on success only: on failure it was replayed
        // above. On success the entries are durable with the commit.
        if result.is_ok() {
            self.index_undo.lock().remove(&xid);
        }
        result?;
        Ok(())
    }

    /// Abort (roll back) this transaction. Consumes `self` so it cannot be
    /// reused.
    ///
    /// The transaction's index maintenance is reverse-applied from the undo
    /// log BEFORE the CLOG abort lands, so no snapshot can observe a heap
    /// row whose index entry is already gone (or vice versa).
    pub fn abort(mut self) -> Result<()> {
        let xid = self.xid.take().expect("abort called twice");
        apply_index_undo(&self.index_undo, &self.buffer_pool, &self.wal_writer, xid);
        let result = self.txn.abort_txn(xid);
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
            apply_index_undo(&self.index_undo, &self.buffer_pool, &self.wal_writer, xid);
            if let Err(e) = self.txn.abort_txn(xid) {
                // Same reporting level as the auto-commit error path: a
                // failed abort leaks the XID into the active set (its
                // writes stay invisible to all future snapshots), so it
                // must be loud even though Drop cannot propagate.
                tracing::warn!(error = %e, xid = xid.0, "txn handle drop auto-abort failed");
            }
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
    xid: TxnId,
) {
    let entries = log.lock().remove(&xid).unwrap_or_default();
    let btree = BTreeAM::new(Arc::clone(buffer_pool), Arc::clone(wal_writer));
    for undo in entries.into_iter().rev() {
        let result = btree
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
            });
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
        let mut redo_handlers = heap_redo_handlers();
        redo_handlers.extend(txn_redo_handlers());
        redo_handlers.extend(btree_redo_handlers());
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
        let registry = Self::build_registry(&catalog)?;
        let indexes = Self::build_index_registry(&catalog, &heap, clog.as_ref())?;

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
            ddl_lock: Mutex::new(()),
            instance_id: NEXT_ENGINE_INSTANCE_ID.fetch_add(1, Ordering::Relaxed),
            index_undo: Arc::new(Mutex::new(HashMap::new())),
            query_stats: QueryStats::new(config.query_stats_capacity),
        };

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

    /// Rebuild the table registry from the catalog snapshot.
    ///
    /// `pg_class` rows are folded **last-version-wins per OID** in slot
    /// order: `drop_table` appends a `relkind = 'd'` version after the live
    /// one, and a fresh insert always appends, so the last row for an OID is
    /// its current state. System catalogs (OID < `FIRST_USER`) are not user
    /// tables and are skipped.
    fn build_registry(catalog: &Catalog) -> Result<HashMap<String, TableEntry>> {
        let mut live: HashMap<u64, &RelationRow> = HashMap::new();
        for row in catalog.relations() {
            if row.kind == RELKIND_DROPPED {
                live.remove(&row.oid.raw().0);
            } else {
                live.insert(row.oid.raw().0, row);
            }
        }

        let mut registry = HashMap::new();
        for row in live.into_values() {
            let oid = row.oid.raw();
            if oid.0 < Oid::FIRST_USER.0 {
                continue;
            }
            if row.kind != RELKIND_TABLE {
                // Index relations (`relkind = 'i'`, Stage M wave 2) are NOT
                // tables: they carry no heap chain and must never be reached
                // through the DML API. They live in the index registry
                // (`build_index_registry`), keyed by (table, column).
                continue;
            }
            let Some(relpages) = catalog.relpages_of(row.oid) else {
                // Half-created table from a crash mid-create_table (module
                // docs, "DDL crash atomicity"): leak the pages, skip the row.
                tracing::warn!(
                    table = %row.name,
                    oid = oid.0,
                    "pg_class row without pg_rust_relpages entry; skipping half-created table"
                );
                continue;
            };
            let mut columns = Vec::with_capacity(row.natts as usize);
            for attr in catalog.attributes_of(row.oid) {
                let ty = builtin_type(attr.type_oid).ok_or_else(|| {
                    EngineError::Corrupted(format!(
                        "{}.{}: unknown type OID {:?}",
                        row.name, attr.name, attr.type_oid
                    ))
                })?;
                columns.push(ColumnDef {
                    name: attr.name.clone(),
                    col_type: ty.column_type,
                });
            }
            if registry
                .insert(
                    row.name.clone(),
                    TableEntry {
                        oid,
                        first_page: relpages.first_page,
                        columns,
                    },
                )
                .is_some()
            {
                return Err(EngineError::Corrupted(format!(
                    "duplicate live pg_class rows for table {:?}",
                    row.name
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
        catalog: &Catalog,
        heap: &HeapAM,
        clog: &dyn ClogAccessor,
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
            let attrs = catalog.attributes_of(pg_catalog::TableOid(index_oid));
            let relpages = catalog.relpages_of(pg_catalog::TableOid(index_oid));
            let (Some(attr), Some(relpages)) = (attrs.first(), relpages) else {
                tracing::warn!(
                    index_oid = index_oid.0,
                    "pg_index row without pg_attribute/pg_rust_relpages; skipping half-built index"
                );
                continue;
            };
            let ty = builtin_type(attr.type_oid).ok_or_else(|| {
                EngineError::Corrupted(format!(
                    "index {index_oid}: unknown type OID {:?}",
                    attr.type_oid
                ))
            })?;
            indexes.push(IndexEntry {
                index_oid,
                table_oid: Oid(*indrelid as u64),
                column: attr.name.clone(),
                key_type: ty.column_type,
                meta_page: relpages.first_page,
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
        self.storage.trigger_checkpoint()?;
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

    /// Create a table and register it in the system catalog.
    ///
    /// Runs as one auto-commit transaction: allocate an OID, allocate the
    /// first heap page, insert the `pg_class` / `pg_attribute` /
    /// `pg_rust_relpages` rows **through the heap AM** (WAL-logged, so the
    /// catalog changes are redo-recoverable), commit, then update the
    /// in-memory registry.
    ///
    /// Fails with [`EngineError::TableExists`] if the name is taken, and
    /// with [`EngineError::CatalogFull`] if a system catalog's first page
    /// has no room (module-level M2a limitation).
    pub fn create_table(&self, name: &str, schema: &[ColumnDef]) -> Result<Oid> {
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
        if self.registry.read().contains_key(name) {
            return Err(EngineError::TableExists(name.to_string()));
        }

        let oid = self.catalog.oid_allocator().alloc();
        // AccessExclusive on the fresh OID (M2c Stage P): ceremonial today
        // — nobody else can reference an OID that was just allocated — but
        // it fixes the DDL lock mode by construction and covers the
        // catalog-row writes like any other DDL.
        let first_page = self.auto_commit(|snap| {
            self.lock_oid(snap.current_xid(), oid, LockMode::AccessExclusive)?;
            self.create_table_inner(snap, oid, name, schema)
        });
        match first_page {
            Ok(first_page) => {
                self.registry.write().insert(
                    name.to_string(),
                    TableEntry {
                        oid,
                        first_page,
                        columns: schema.to_vec(),
                    },
                );
                Ok(oid)
            }
            Err(e) => {
                // The heap page may have been allocated and tracked before
                // the failure; forget it so the AM cache cannot hand the
                // (never-committed) page to a later relation of this OID.
                self.heap.drop_relation(oid);
                Err(e)
            }
        }
    }

    /// The catalog-writing half of `create_table`, inside transaction `snap`.
    fn create_table_inner(
        &self,
        snap: &Snapshot,
        oid: Oid,
        name: &str,
        schema: &[ColumnDef],
    ) -> Result<PageId> {
        let first_page = self.heap.create_heap(oid)?;

        let class_row = vec![
            Some(Datum::Int8(oid.0 as i64)),
            Some(Datum::Text(name.to_string())),
            Some(Datum::Text(RELKIND_TABLE.to_string())),
            Some(Datum::Int4(schema.len() as i32)),
            // No TOAST table (M2a).
            Some(Datum::Int8(0)),
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

        // The chain currently has exactly one page; `last_page` /
        // `page_count` are advisory only (they go stale as the chain grows)
        // — the on-disk chain walk is authoritative (Stage K wave 1).
        let relpages_row = vec![
            Some(Datum::Int8(oid.0 as i64)),
            Some(Datum::Int8(first_page.0 as i64)),
            Some(Datum::Int8(first_page.0 as i64)),
            Some(Datum::Int8(1)),
        ];
        self.insert_catalog_row(snap, &PG_RELPAGES, &relpages_row)?;

        Ok(first_page)
    }

    /// Drop a table: mark its `pg_class` row `relkind = 'd'`, free its data
    /// pages, and remove it from the registry.
    ///
    /// The marker is an MVCC **update through the heap AM**, not a physical
    /// row removal: the old version stays in the page (invisible to scans
    /// once the drop commits), and the registry rebuild's last-version-wins
    /// fold reads the drop. A physical removal would need either slot
    /// recycling (breaks TID stability) or a raw in-place rewrite that
    /// reaches past the AM into tuple internals — both worse than one extra
    /// dead version per dropped table in M2a.
    ///
    /// Ordering inside the transaction (module docs, "DDL crash
    /// atomicity"): the `'d'` marker is written **first**, then the data
    /// pages are walked and freed (`PageFree` WAL + redo, Stage E), then
    /// the heap AM's cached page list is dropped. The `pg_rust_relpages`
    /// row is left behind; it is keyed by the table's never-reused OID and
    /// ignored by the rebuild (leaked row, documented M2a simplification).
    ///
    /// The registry entry is removed INSIDE the transaction, before commit
    /// releases the AccessExclusive lock (M2c Stage P review): a DML
    /// statement that resolved the table before the drop and queued behind
    /// its lock must observe the removal the moment its own lock is
    /// granted — `lock_table_entry`'s post-lock registry re-check relies on
    /// this ordering. If the commit itself fails (WAL-fatal, process
    /// teardown territory) the in-memory removal has already happened while
    /// the durable pg_class row is not marked 'd': the two diverge until
    /// restart, when the rebuild resurrects the table consistent with the
    /// durable state.
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

        self.auto_commit(|snap| {
            // AccessExclusive (§9.2): blocks — and is blocked by — every
            // other lock mode on this table, so a DROP waits for in-flight
            // readers/writers and new ones queue behind it (M2c Stage P).
            self.lock_table(snap.current_xid(), &entry, LockMode::AccessExclusive)?;
            self.drop_table_inner(snap, name, &entry)?;
            // Remove BEFORE commit releases the lock (see the fn docs).
            self.registry.write().remove(name);
            Ok(())
        })
    }

    /// The page-freeing half of `drop_table`, inside transaction `snap`.
    fn drop_table_inner(&self, snap: &Snapshot, name: &str, entry: &TableEntry) -> Result<()> {
        // 1. Mark relkind = 'd' (before freeing pages — see the fn docs).
        let old_tid = self.find_live_pg_class_row(entry.oid)?;
        let columns = PG_CLASS.column_types();
        let class_row = vec![
            Some(Datum::Int8(entry.oid.0 as i64)),
            // The name is repeated so the row stays self-describing.
            Some(Datum::Text(name.to_string())),
            Some(Datum::Text(RELKIND_DROPPED.to_string())),
            Some(Datum::Int4(entry.columns.len() as i32)),
            Some(Datum::Int8(0)),
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

        // 2. Free the data pages along the chain.
        for page_id in self.walk_chain(entry.first_page)? {
            self.storage.page_allocator().lock().free_page(page_id)?;
        }

        // 3. Forget the relation in the AM cache: the freed page IDs can be
        //    handed out again and must never be reached through this OID.
        self.heap.drop_relation(entry.oid);
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

        // Catalog-room pre-check BEFORE the expensive scan + bulk load
        // (~1s and ~20MB of WAL for a large table): the four catalog rows
        // written at the end must fit their system pages, so fail cheap and
        // early if they cannot. The rows are encoded exactly as
        // `create_index_catalog_rows` will encode them.
        let index_oid = self.catalog.oid_allocator().alloc();
        self.ensure_index_catalog_room(&entry, column, index_oid)?;

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
        let meta_page = self.auto_commit(|snap| {
            let entry = self.lock_table_entry(snap.current_xid(), table, LockMode::Exclusive)?;
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

            self.create_index_catalog_rows(&snap, &entry, column, index_oid, meta_page)?;
            Ok(meta_page)
        })?;
        self.indexes.write().push(IndexEntry {
            index_oid,
            table_oid: entry.oid,
            column: column.to_string(),
            key_type,
            meta_page,
        });
        Ok(index_oid)
    }

    /// The catalog-writing half of `create_index`, inside transaction `snap`.
    fn create_index_catalog_rows(
        &self,
        snap: &Snapshot,
        entry: &TableEntry,
        column: &str,
        index_oid: Oid,
        meta_page: PageId,
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
            Some(Datum::Int8(BTREE_AM_OID.0 as i64)),
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
                    Some(Datum::Int8(BTREE_AM_OID.0 as i64)),
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
    fn heap_tuple_visible(&self, snap: &Snapshot, tid: Tid) -> Result<Option<Tid>> {
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
    fn hot_chain_root(&self, tid: Tid) -> Result<Tid> {
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
            .find(|e| e.table_oid == entry.oid && e.column == column)
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

    /// The shared insert path. Takes the registry `entry` from the caller
    /// (Stage O review): SQL exec already holds it for
    /// `build_insert_values`, so looking it up again here would be a
    /// redundant recursive `registry.read()` per row.
    fn insert_inner(&self, snap: &Snapshot, entry: &TableEntry, values: &[Value]) -> Result<Tid> {
        let col_types = column_types(entry);
        let tuple = encode_row(entry, &col_types, values, snap.curcid())?;
        let indexes = self.indexes_of(entry);
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
        for (idx, col_index) in &indexes {
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

    fn scan_inner(
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
        let tuple = encode_row(&entry, &col_types, values, snap.curcid())?;
        let indexes = self.indexes_of(&entry);
        let old_values = if indexes.is_empty() {
            Vec::new()
        } else {
            self.read_row_by_tid(&entry, &col_types, tid)?
        };
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
        // version. Cross-page fallback reverts to the non-HOT path.
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
        let old_values = if indexes.is_empty() {
            Vec::new()
        } else {
            self.read_row_by_tid(&entry, &col_types, tid)?
        };
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
    ///    (the Stage Q online path, `BTreeDelete` WAL included).
    ///    **`EntryNotFound` is Ok here and only here** (§4.3): eager
    ///    online maintenance already removed the entries of ordinary
    ///    committed deletes/updates, so the entries this phase actually
    ///    removes are essentially crash-loser dangling ones. The btree's
    ///    own delete contract is NOT weakened. Vacuum records no index
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
                return Ok(stats);
            }

            // Phase 3: read-only key extraction, BEFORE any physical rewrite.
            let keys = self.heap.collect_index_keys(rel, &dead)?;
            stats.index_keys = keys.len();

            // Phase 4: push-mode index cleanup, one descent per (key, tid)
            // per index (batch interfaces are Phase 5b, §4.3).
            for (idx, col_index) in &self.indexes_of(&entry) {
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
            Ok(stats)
        })
    }

    /// Every index registered on `entry`'s table, joined with the position
    /// of its indexed column in the table's schema.
    fn indexes_of(&self, entry: &TableEntry) -> Vec<(IndexEntry, usize)> {
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

    /// Read back the decoded values of the row at `tid` directly from its
    /// heap page (raw read, no visibility filter: DML uses this to recover
    /// the OLD key of a row the caller already addressed by TID, before the
    /// heap mutation runs in the same statement).
    fn read_row_by_tid(
        &self,
        entry: &TableEntry,
        col_types: &[ColumnType],
        tid: Tid,
    ) -> Result<Vec<Value>> {
        let guard = self.storage.buffer_pool().pin(tid.page_id)?;
        let page: &[u8; PAGE_SIZE] = guard.page().try_into().expect("frame is PAGE_SIZE");
        let bytes = SlottedPage::tuple(page, tid.slot_id)?.ok_or_else(|| {
            EngineError::Corrupted(format!("no tuple at {tid} for index key readback"))
        })?;
        let (_header, values) = decode_tuple(bytes, col_types)?;
        debug_assert_eq!(values.len(), entry.columns.len());
        Ok(values)
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
            let old_values = self.read_row_by_tid(entry, col_types, tid)?;
            let index_tid = self.hot_chain_root(tid)?;
            for (idx, col_index) in &indexes {
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
            _snapshot_guard: snapshot_guard,
            instance_id: self.instance_id,
            lock_manager: Arc::clone(&self.lock_manager),
            index_undo: Arc::clone(&self.index_undo),
            buffer_pool: Arc::clone(self.storage.buffer_pool()),
            wal_writer: Arc::clone(self.storage.wal_writer()),
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
        let started = Instant::now();
        let stmt = sql::parse(sql)?;
        let path = ExecutionPath::of(&stmt);
        let result = match txn {
            None => self.exec_auto(stmt),
            Some(h) => self.exec_txn(h, stmt),
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

    fn exec_auto(&self, stmt: Statement) -> Result<QueryResult> {
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
            Statement::CreateTable { name, columns } => {
                let defs: Vec<ColumnDef> = columns
                    .into_iter()
                    .map(|c| ColumnDef {
                        name: c.name,
                        col_type: c.col_type,
                    })
                    .collect();
                self.create_table(&name, &defs)?;
                Ok(QueryResult::Ok)
            }
            Statement::CreateIndex { table, column } => {
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
                        let values = build_insert_values(&entry, &columns, row)?;
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
                            &snap, &columns, &table, &filter, &order_by, &limit, false, false,
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
                            snap, &columns, &table, &filter, &order_by, &limit, true, false,
                        )
                    }),
                    // FOR SHARE (Stage S multixact lite): stamps a shared
                    // row lock (HEAP_XMAX_LOCK_ONLY + HEAP_XMAX_IS_SHARE).
                    // The row stays visible to all snapshots.
                    Some(LockClause::ForShare) => self.auto_commit(|snap| {
                        self.lock_table_entry(snap.current_xid(), &table, LockMode::RowExclusive)?;
                        self.exec_select(
                            snap, &columns, &table, &filter, &order_by, &limit, true, true,
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
                        .map(|f| filter_to_predicate(&entry, f))
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
                        .map(|f| filter_to_predicate(&entry, f))
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

    fn exec_txn(&self, handle: &TxnHandle, stmt: Statement) -> Result<QueryResult> {
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
            Statement::CreateTable { .. } | Statement::CreateIndex { .. } => {
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
                    let values = build_insert_values(&entry, &columns, row)?;
                    self.insert_inner(&snap, &entry, &values)?;
                    count += 1;
                }
                Ok(QueryResult::Affected(count))
            }
            Statement::Select {
                columns,
                table,
                filter,
                order_by,
                limit,
                lock,
            } => match lock {
                None => {
                    self.lock_table_entry(handle.xid(), &table, LockMode::AccessShare)?;
                    self.exec_select(
                        &snap, &columns, &table, &filter, &order_by, &limit, false, false,
                    )
                }
                Some(LockClause::ForUpdate) => {
                    self.lock_table_entry(handle.xid(), &table, LockMode::RowExclusive)?;
                    self.exec_select(
                        &snap, &columns, &table, &filter, &order_by, &limit, true, false,
                    )
                }
                Some(LockClause::ForShare) => {
                    self.lock_table_entry(handle.xid(), &table, LockMode::RowExclusive)?;
                    self.exec_select(
                        &snap, &columns, &table, &filter, &order_by, &limit, true, true,
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
                    .map(|f| filter_to_predicate(&entry, f))
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
                    .map(|f| filter_to_predicate(&entry, f))
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
        order_by: &Option<OrderBy>,
        limit: &Option<usize>,
        lock_rows: bool,
        shared: bool,
    ) -> Result<QueryResult> {
        let entry = self.table_entry(table)?;
        let pred = filter
            .as_ref()
            .map(|f| filter_to_predicate(&entry, f))
            .transpose()?;
        let mut rows = self.scan_inner(snap, table, pred.as_ref())?;
        if let Some(ob) = order_by {
            let idx = entry
                .columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(&ob.column))
                .ok_or_else(|| {
                    EngineError::InvalidPredicate(format!("no column {:?} in table", ob.column))
                })?;
            rows.sort_by(|a, b| {
                let av = a.1.get(idx);
                let bv = b.1.get(idx);
                let cmp = av.partial_cmp(&bv).unwrap_or(std::cmp::Ordering::Equal);
                if ob.desc {
                    cmp.reverse()
                } else {
                    cmp
                }
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
            SelectCols::Cols(cols) => {
                let indices: Vec<usize> = cols
                    .iter()
                    .map(|name| {
                        entry
                            .columns
                            .iter()
                            .position(|c| c.name.eq_ignore_ascii_case(name))
                            .ok_or_else(|| {
                                EngineError::InvalidPredicate(format!(
                                    "no column {name:?} in table"
                                ))
                            })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let rows: Vec<Vec<Value>> = rows
                    .into_iter()
                    .map(|(_, v)| indices.iter().map(|&i| v[i].clone()).collect())
                    .collect();
                (cols.clone(), rows)
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
    fn auto_commit<T>(&self, op: impl FnOnce(&Snapshot) -> Result<T>) -> Result<T> {
        let xid = self.txn.begin_txn();
        // `_snap_guard` unregisters the snapshot at the end of this frame
        // on BOTH the success and the failure path (M3 Stage A, §3.3). A
        // panic in `op` still runs the guard's Drop during unwinding (the
        // default panic policy), so the snapshot unregisters normally; only
        // panic=abort or mem::forget would skip it — the documented O1
        // semantics (horizon pinned low, vacuum degrades to no-reclaim,
        // safe), matching this function's existing panic policy below.
        let (mut snap, _snap_guard) = self.txn.snapshot(xid);
        snap.advance_curcid();
        match op(&snap) {
            Ok(v) => {
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
                        xid,
                    );
                    if let Err(abort_err) = self.txn.abort_txn(xid) {
                        tracing::warn!(error = %abort_err, xid = xid.0, "auto-commit commit-failure fallback abort failed");
                    }
                }
                self.lock_manager.release_all(xid);
                // Discard the undo log on success only — on failure it was
                // replayed above (see TxnHandle::commit).
                if result.is_ok() {
                    self.index_undo.lock().remove(&xid);
                }
                result?;
                Ok(v)
            }
            Err(e) => {
                apply_index_undo(
                    &self.index_undo,
                    self.storage.buffer_pool(),
                    self.storage.wal_writer(),
                    xid,
                );
                if let Err(abort_err) = self.txn.abort_txn(xid) {
                    tracing::warn!(error = %abort_err, "auto-commit abort failed");
                }
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

    /// Locate the live (`relkind = 'r'`) `pg_class` row of `oid` by a raw
    /// slot scan of the catalog's first page.
    ///
    /// Raw, not `HeapAM::scan`: catalog rows written by bootstrap carry the
    /// synthetic bootstrap XID, which the CLOG has never heard of — a
    /// visibility-filtered AM scan would see nothing. The registry only
    /// tracks live tables, so a live row must exist.
    fn find_live_pg_class_row(&self, oid: Oid) -> Result<Tid> {
        let guard = self.storage.buffer_pool().pin(PG_CLASS.first_page)?;
        let page: &[u8; PAGE_SIZE] = guard.page().try_into().expect("frame is PAGE_SIZE");
        let columns = PG_CLASS.column_types();
        for slot in 0..SlottedPage::slot_count(page) as u16 {
            let Some(bytes) = SlottedPage::tuple(page, slot)? else {
                continue;
            };
            let (_header, values) = decode_tuple(bytes, &columns)?;
            let matches = matches!(&values[0], Some(Datum::Int8(v)) if *v == oid.0 as i64)
                && matches!(&values[2], Some(Datum::Text(k)) if k == RELKIND_TABLE);
            if matches {
                return Ok(Tid {
                    page_id: PG_CLASS.first_page,
                    slot_id: slot,
                });
            }
        }
        Err(EngineError::Corrupted(format!(
            "pg_class has no live row for table OID {}",
            oid.0
        )))
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
    fn lock_table_entry(&self, xid: TxnId, table: &str, mode: LockMode) -> Result<TableEntry> {
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
    /// one entry per [`Self::exec`] call, oldest dropped on overflow. The
    /// typed API (`scan` / `insert` / `update` / `delete` /
    /// `index_lookup`) does NOT produce entries — it never passes through
    /// `exec`.
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
fn column_types(entry: &TableEntry) -> Vec<ColumnType> {
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

/// Encode `values` as a heap tuple against the entry's schema.
fn encode_row(
    entry: &TableEntry,
    col_types: &[ColumnType],
    values: &[Value],
    curcid: u32,
) -> Result<Vec<u8>> {
    if values.len() != entry.columns.len() {
        return Err(EngineError::InvalidArgument(format!(
            "table has {} columns but {} values given",
            entry.columns.len(),
            values.len()
        )));
    }
    Ok(encode_tuple(tuple_header(curcid), col_types, values)?)
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

/// Map a codec column type to its built-in `pg_type` OID and `attlen`
/// (§5.1; the built-in set has exactly one type per codec type).
fn type_oid_of(col_type: ColumnType) -> Result<(TypeOid, i32)> {
    BUILTIN_TYPES
        .iter()
        .find(|t| t.column_type == col_type)
        .map(|t| (t.oid, t.len))
        .ok_or_else(|| EngineError::InvalidArgument(format!("no builtin type for {col_type:?}")))
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
        (Literal::Str(s), ColumnType::Text) => Ok(Some(Datum::Text(s.clone()))),
        (Literal::Str(s), ColumnType::Bytea) => Ok(Some(Datum::Bytea(s.clone().into_bytes()))),
        (Literal::Null, _) => Ok(None),
        (l, t) => Err(EngineError::InvalidArgument(format!(
            "literal {l:?} is not compatible with column type {t:?}"
        ))),
    }
}

/// Build the full `values` vector for an INSERT, matching the table schema.
fn build_insert_values(
    entry: &TableEntry,
    columns: &Option<Vec<String>>,
    row: &[Literal],
) -> Result<Vec<Value>> {
    let n = entry.columns.len();
    let values = match columns {
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
                v[idx] = literal_to_value(lit, entry.columns[idx].col_type)?;
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
                .map(|(lit, col)| literal_to_value(lit, col.col_type))
                .collect::<Result<Vec<_>>>()?
        }
    };
    Ok(values)
}

/// Build a `Predicate` from a parsed `Filter`, resolving the column name
/// to a 0-based index and converting the literal.
fn filter_to_predicate(entry: &TableEntry, filter: &Filter) -> Result<Predicate> {
    let col_index = entry
        .columns
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case(&filter.column))
        .ok_or_else(|| {
            EngineError::InvalidPredicate(format!("no column {:?} in table", filter.column))
        })?;
    let col_type = entry.columns[col_index].col_type;
    let value = match &filter.value {
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
    };
    Ok(match filter.op {
        CmpOp::Eq => Predicate::Eq { col_index, value },
        CmpOp::Lt => Predicate::Lt { col_index, value },
        CmpOp::Gt => Predicate::Gt { col_index, value },
    })
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
