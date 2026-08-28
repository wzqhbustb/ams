//! pg_rust transaction layer — Phase 1 M2.
//!
//! This crate implements transaction management, MVCC visibility, and locking:
//! - XID allocation (`TxnIdClock`)
//! - CLOG (transaction status log) with `ClogBuffer` SLRU cache
//! - Snapshot and `VisibilityOracle`
//! - Lock Manager (row-level via tuple.xmax + table-level 4-mode locks)
//!
//! It depends only on `pg-storage` for physical types and primitives.
//!
//! # M2a scope (Stage I–J)
//!
//! Stage I added the minimal [`Snapshot`] + [`is_visible`] surface for heap
//! scan. Stage J adds the [`manager::TxnManager`] (XID allocation + durable
//! commit/abort), the [`clog_mem::InMemoryClogAccessor`] (a real CLOG that
//! records aborts), and the [`redo`] handlers that rebuild the CLOG from the
//! WAL on recovery.
//!
//! # M2b scope (Stage K–L)
//!
//! Stage K/L add the disk-backed CLOG: [`clog_file`] segment files and the
//! [`ClogBuffer`] SLRU cache, which implements the same [`ClogAccessor`] trait
//! (and the checkpoint flush hook `pg_storage::clog::ClogFlush`) so call sites
//! do not change. Stage L also completes the MVCC surface: [`Snapshot`] gains
//! its full §7.1 field set (`xip: SmallVec<[TxnId; 32]>`, `curcid`),
//! [`TxnManager::snapshot`] produces real SI snapshots, and
//! [`visibility::PgVisibilityOracle`] implements the complete §7.2 textbook
//! judgment including the `t_cid`/`curcid` self-command branches.
//!
//! # M2c scope (Stage P)
//!
//! Stage P adds the lock surface: [`lock_manager::LockManager`] provides the
//! four table-level lock modes (tech-selection §9.2) with FIFO fair waiting,
//! and [`TxnManager`] grows the row-lock wait protocol (§9.1): the
//! `row_wait_registry`, `register_row_wait` / `wait_for` / `wait_edges`, and
//! the `end_txn` wakeup broadcast. The heap AM consumes that protocol
//! through the narrow [`manager::RowWaiter`] trait (register + wait), which
//! [`TxnManager`] implements. The commit/checkpoint barrier is sunk into
//! [`TxnManager`] itself, so commits are serialized against checkpoint CLOG
//! flushes by construction.
//!
//! # M2c scope (Stage R)
//!
//! Stage R adds deadlock detection (tech-selection §9.3):
//! [`deadlock::DeadlockDetector`] runs a 100ms background scan over the
//! wait-for graph (row edges from `TxnManager::wait_edges`, table edges from
//! `LockManager::table_lock_states`), picks the youngest transaction of each
//! cycle as victim, and marks it in the shared [`deadlock::DeadlockVictims`]
//! registry. Both wait loops — `TxnManager::wait_for` and
//! `LockManager::acquire` — consume the mark and fail with
//! [`TxnError::DeadlockVictim`] / [`LockError::DeadlockVictim`], which the
//! caller's abort path turns into a full rollback.
//!
//! # M3 scope (Stage A)
//!
//! Stage A adds the snapshot registry + vacuum horizon (tech-selection
//! §3.3): [`TxnManager::snapshot`] registers each snapshot's `xmin`
//! atomically with its construction and returns a [`SnapshotGuard`] that
//! unregisters on `Drop`; [`TxnManager::oldest_snapshot_xmin`] exposes the
//! horizon. [`Snapshot`] fields are crate-private (anti-enumeration
//! guardrail: `snapshot()` is the only registered construction point;
//! [`Snapshot::everything`] is the explicit never-registered special case).

#![warn(missing_docs)]
#![warn(rust_2018_idioms)]

pub mod clog_buffer;
pub mod clog_file;
pub mod clog_mem;
pub mod deadlock;
pub mod lock_manager;
pub mod manager;
pub mod redo;
pub mod snapshot;
pub mod visibility;

pub use clog_buffer::ClogBuffer;
pub use clog_mem::InMemoryClogAccessor;
pub use deadlock::{wait_for_edges, DeadlockDetector, DeadlockVictims, DEFAULT_DEADLOCK_INTERVAL};
pub use lock_manager::{LockError, LockManager, LockMode, TableLockState};
pub use manager::{CommitWal, RowWaiter, SnapshotGuard, TxnError, TxnManager};
pub use pg_storage::clog::{ClogAccessor, TxnState};
pub use redo::txn_redo_handlers;
pub use snapshot::Snapshot;
pub use visibility::{is_visible, HintBit, PgVisibilityOracle, Visibility, VisibilityOracle};
