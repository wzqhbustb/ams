//! Minimal transaction manager (M2a Stage J).
//!
//! [`TxnManager`] gives M2a a single real transaction per SQL statement: each
//! `begin_txn` allocates one XID from the shared [`TxnIdClock`], and
//! `commit_txn` / `abort_txn` make the outcome durable in the WAL and then
//! record it in the commit log. M2a runs in auto-commit, so callers pair one
//! `begin_txn` with exactly one `commit_txn`/`abort_txn`.
//!
//! # Commit hard-order (§3 P1-5)
//!
//! Commit performs four steps in a fixed order so recovery can rebuild the
//! CLOG authoritatively from the WAL:
//!
//! 1. `wal.append(TxnCommit)` — stage the record.
//! 2. `wal.flush_to(lsn)` — fsync it (the commit is durable here).
//! 3. `clog.set_state(xid, Committed)` — flip the in-memory bit.
//! 4. `remove_active(xid)` — drop the XID from the active set.
//!
//! If step 2 fails the CLOG bit is never flipped (step 3 is unreachable), so a
//! transaction whose commit record did not reach disk is treated as aborted on
//! recovery — never as committed. Abort follows the same shape with
//! `TxnAbort` and `TxnState::Aborted`.
//!
//! # Group-commit batching (coding-plan Stage J `page_alloc flush 攒批`)
//!
//! `PageAllocator::alloc_page` / `free_page` are append-only: they write their
//! WAL record to the segment file but do **not** fsync. The commit's single
//! `flush_to(lsn)` at step 2 therefore amortizes every allocation fsync
//! accumulated during the transaction into one syscall — a `CREATE TABLE` that
//! extends many pages pays for one fsync at commit instead of one per page.
//! This is safe because `flush_to(commit_lsn)` fsyncs the whole WAL prefix up
//! to the commit record, and the LSN clock is monotonic, so every earlier
//! `PageAlloc`/`PageFree` LSN is covered.
//!
//! # Snapshot registry + vacuum horizon (M3 Stage A, tech-selection §3.3)
//!
//! [`TxnManager::snapshot`] registers every snapshot's `xmin` in
//! `snapshot_xmins` (xmin → refcount) in the SAME critical section that
//! reads the active set and XID clock (B1 atomicity; caller-side
//! registration wrappers are forbidden — see the `snapshot` doc). The
//! returned [`SnapshotGuard`] unregisters on `Drop`.
//! [`TxnManager::oldest_snapshot_xmin`] is the vacuum horizon: the
//! registry's smallest key, or the XID clock's current value when empty.
//! Panic semantics (§11 O1, decided): under the default unwind policy the
//! guard's `Drop` DOES run during unwinding, so the snapshot unregisters
//! normally and the horizon is unaffected. Only `panic=abort` (process is
//! dead anyway) or `mem::forget` skips the `Drop` and pins the horizon low
//! forever — vacuum degrades to no-reclaim, which is safe, matching
//! `auto_commit`'s existing panic cost model.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Test-only failure injection (F7 regression): when armed on the current
/// thread, `commit_txn` fails at entry with a synthetic error, before any
/// WAL work. Compiled in always (doc-hidden) so pg-engine integration tests
/// can arm it without feature plumbing; production never sets it.
#[doc(hidden)]
pub mod test_hooks {
    use std::cell::Cell;
    use std::sync::atomic::{AtomicBool, Ordering};

    thread_local! {
        static COMMIT_TXN_FORCE_FAIL: Cell<bool> = const { Cell::new(false) };
    }
    static ARMED: AtomicBool = AtomicBool::new(false);

    /// Arm/disarm the hook for the current thread, with a process-global
    /// claim so two tests can't arm it concurrently.
    pub fn set_commit_txn_force_fail(on: bool) {
        if on {
            assert!(
                !ARMED.swap(true, Ordering::SeqCst),
                "commit_txn failure hook already armed by another test"
            );
        } else {
            ARMED.store(false, Ordering::SeqCst);
        }
        COMMIT_TXN_FORCE_FAIL.with(|c| c.set(on));
    }

    pub(crate) fn commit_txn_should_fail() -> bool {
        COMMIT_TXN_FORCE_FAIL.with(Cell::get)
    }
}

use parking_lot::{Condvar, Mutex};
use smallvec::SmallVec;
use thiserror::Error;

use pg_storage::clog::{ClogAccessor, TxnState};
// The barrier crosses the crate boundary into pg-storage's checkpoint
// coordinator, so it must be the aliased type: identical to
// `parking_lot::RwLock` in production builds, loom-instrumented under
// `--cfg loom` (Stage Q; see pg_storage::sync).
use pg_storage::error::Result;
use pg_storage::sync::RwLock;
use pg_storage::txn_id::TxnIdClock;
use pg_storage::types::{Lsn, TxnId};
use pg_storage::wal::record::WalRecord;
use pg_storage::wal::writer::WalWriter;

use crate::deadlock::DeadlockVictims;
use crate::snapshot::Snapshot;

/// The two WAL operations the commit path needs: stage a record and fsync it.
///
/// [`WalWriter`] is the production implementation. The trait exists so the
/// commit hard-order (§3 P1-5) can be tested by injecting a WAL whose
/// `flush_to` fails — proving the CLOG bit is never flipped when the commit
/// record did not reach disk. It is intentionally tiny (append + flush).
pub trait CommitWal: std::fmt::Debug + Send + Sync {
    /// Append `record`, returning the LSN it was assigned.
    fn append(&self, record: WalRecord) -> Result<Lsn>;
    /// Flush (fsync) the WAL up to and including `lsn`.
    fn flush_to(&self, lsn: Lsn) -> Result<()>;
}

impl CommitWal for WalWriter {
    fn append(&self, record: WalRecord) -> Result<Lsn> {
        WalWriter::append(self, record)
    }

    fn flush_to(&self, lsn: Lsn) -> Result<()> {
        WalWriter::flush_to(self, lsn)
    }
}

/// Errors from transaction wait operations (M2c Stage P).
#[derive(Debug, Error, PartialEq, Eq)]
pub enum TxnError {
    /// A transaction asked to wait on itself. That is always a caller bug:
    /// the row-lock protocol (§9.1) only ever waits on a *different* XID
    /// read from a tuple's `t_xmax`.
    #[error("transaction {0} cannot wait on itself")]
    SelfWait(TxnId),
    /// The deadlock detector (M2c Stage R, §9.3) chose this transaction as
    /// the victim of a wait-for cycle and interrupted its row-lock wait.
    /// The waiter's registry edge is already cleared; the caller's current
    /// statement fails and the transaction must be aborted (PG semantics:
    /// the error is retryable as a whole, the statement is not).
    #[error("deadlock detected: transaction {0} chosen as victim")]
    DeadlockVictim(TxnId),
}

/// The row-lock wait capability the heap AM's §9.1 5-step protocol needs
/// (M2c Stage P).
///
/// Implemented by [`TxnManager`]; declared as a separate trait so
/// `pg-am-heap` depends on the narrow register/wait surface instead of the
/// concrete manager type, and so AM tests can inject a fake. The protocol's
/// ordering requirements are on the methods.
pub trait RowWaiter: std::fmt::Debug + Send + Sync {
    /// Register the wait edge `self_xid → blocking_xid` (§9.1 step 5a).
    ///
    /// The caller MUST invoke this while still holding the page latch under
    /// which it read `blocking_xid` from the tuple's `t_xmax`, and MUST NOT
    /// release that latch before the call returns — registration before
    /// latch release is what makes the subsequent [`RowWaiter::wait_for`]
    /// wakeup unmissable.
    fn register_row_wait(&self, self_xid: TxnId, blocking_xid: TxnId);

    /// Drop `self_xid`'s wait edge without waiting (caller-side cleanup on
    /// paths that leave the protocol without a completed
    /// [`RowWaiter::wait_for`], which otherwise clears the edge itself).
    fn unregister_row_wait(&self, self_xid: TxnId);

    /// Block until `blocking_xid` commits or aborts (§9.1 step 5c). The
    /// caller must hold NO page latch while blocked. Clears the wait edge
    /// on success.
    fn wait_for(&self, self_xid: TxnId, blocking_xid: TxnId) -> std::result::Result<(), TxnError>;

    /// Is `xid` a live, in-flight transaction?
    ///
    /// The gate needs this to distinguish a genuinely active stamper (wait
    /// on it) from one whose CLOG entry reads `InProgress` but whose XID is
    /// gone from the active set. That combination has TWO causes, and the
    /// caller must tell them apart by RE-READING the CLOG after a `false`
    /// return:
    ///
    /// - the stamper ended between the caller's CLOG read and this check
    ///   (normal race): `end_txn` flips the CLOG bit BEFORE removing the
    ///   XID, so observing not-active orders the caller after the terminal
    ///   write and the CLOG re-read yields the terminal state;
    /// - the stamper CRASHED (post-recovery; recovery-end ATT abort
    ///   marking is still open, §11.3): the re-read still says
    ///   `InProgress`, and — WAL replay having rebuilt every durable
    ///   commit's bit — the stamp must be treated as aborted, never waited
    ///   on (waiting would return instantly and the gate would spin).
    fn is_active(&self, xid: TxnId) -> bool;
}

/// Lock-shared transaction state (M3 Stage A, tech-selection §3.3 v1.3):
/// the active set and the snapshot registry live under ONE mutex so that
/// [`TxnManager::snapshot`] reads the active set + XID clock AND registers
/// the new snapshot's `xmin` in a single critical section (B1 atomicity —
/// see the `snapshot` doc for why a caller-side wrapper is unsound).
#[derive(Debug, Default)]
struct TxnShared {
    /// XIDs that have begun but not yet committed or aborted.
    active: HashSet<TxnId>,
    /// Snapshot registry: `xmin` → refcount of live snapshots taken at that
    /// xmin (multiple snapshots may share one xmin). The vacuum horizon is
    /// its smallest key; an empty map means "no readers anywhere", and the
    /// horizon falls back to the XID clock's current value.
    snapshot_xmins: BTreeMap<TxnId, usize>,
}

/// RAII token that unregisters one snapshot from the vacuum-horizon
/// registry (M3 Stage A, tech-selection §3.3).
///
/// Obtained from [`TxnManager::snapshot`] together with the [`Snapshot`];
/// `Drop` decrements the refcount of the snapshot's `xmin` and removes the
/// key when it reaches zero. The guard is intentionally independent of the
/// `Snapshot` value's lifetime: callers may clone or store the snapshot
/// wherever they like as long as the guard outlives its use (the engine
/// keeps the guard in `TxnHandle` / on the `auto_commit` frame).
///
/// # Panic semantics (tech-selection §11 O1 — decided, no guardrail)
///
/// Under the default unwind policy a panic DOES run this guard's `Drop`
/// during unwinding (e.g. through `Engine::auto_commit`), so the registry
/// entry is unregistered normally and the horizon is unaffected. Only
/// `panic=abort` (process dead anyway) or `mem::forget` skips the `Drop`,
/// leaving the entry behind: the horizon then stays pinned low forever and
/// vacuum degrades to no-reclaim — SAFE (nothing visible is ever
/// collected), merely ineffective. This matches the existing panic cost
/// model, so no process-level watchdog is added.
#[derive(Debug)]
pub struct SnapshotGuard {
    shared: Arc<Mutex<TxnShared>>,
    live_snapshots: Arc<AtomicUsize>,
    xmin: TxnId,
}

impl SnapshotGuard {
    /// The registered `xmin` this guard pins in the horizon registry.
    pub fn xmin(&self) -> TxnId {
        self.xmin
    }
}

impl Drop for SnapshotGuard {
    fn drop(&mut self) {
        let mut shared = self.shared.lock();
        if let Some(count) = shared.snapshot_xmins.get_mut(&self.xmin) {
            *count -= 1;
            if *count == 0 {
                shared.snapshot_xmins.remove(&self.xmin);
            }
        }
        self.live_snapshots.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Coordinates XID allocation and durable commit/abort for M2a.
///
/// Cheap to clone conceptually via `Arc`; hold a single instance per engine
/// and share it. All fields are `Arc`/interior-mutable so `&self` methods
/// are safe to call concurrently.
#[derive(Debug)]
pub struct TxnManager {
    txn_id_clock: TxnIdClock,
    wal: Arc<dyn CommitWal>,
    clog: Arc<dyn ClogAccessor>,
    /// Active set + snapshot registry under ONE lock (M3 Stage A, B1):
    /// `snapshot()` must observe active set and clock and register the new
    /// snapshot's xmin atomically, so the two cannot live behind separate
    /// mutexes. `Arc` because [`SnapshotGuard`] unlocks through it on Drop.
    shared: Arc<Mutex<TxnShared>>,
    /// Number of live registered snapshots (== sum of
    /// `TxnShared::snapshot_xmins` refcounts). Mirrored as an atomic so the
    /// registry count assertion (M3 Stage A guardrail) can cross-check the
    /// registry without locking. Not a correctness mechanism —
    /// observability only.
    live_snapshots: Arc<AtomicUsize>,
    /// Commit/checkpoint barrier (M2c Stage P: sunk down from pg-engine,
    /// where it was introduced in Stage L). `commit_txn` / `abort_txn` hold
    /// a READ guard for their whole hard order; the checkpoint coordinator
    /// in pg-storage takes the WRITE guard (via `set_commit_barrier`).
    /// This closes the "neither snapshot nor replay" window by construction:
    /// a commit durable before the checkpoint's `begin_lsn` has finished its
    /// `clog.set_state` before the checkpoint's CLOG flush runs (its bit is
    /// fsynced), and a commit starting after lands past `begin_lsn` (replay
    /// rebuilds it).
    ///
    /// Note on scope: the coordinator currently holds the WRITE guard
    /// across the WHOLE checkpoint (all dirty-page flushes), not merely the
    /// ATT-sampling + CLOG-flush critical section — correct but
    /// conservative, since commits stall for the full flush duration on
    /// this hardware. Narrowing the guard to the critical section is a
    /// planned Phase 7b optimization; the implementation is intentionally
    /// left as-is for now.
    commit_barrier: Arc<RwLock<()>>,
    /// Row-lock wait registry (§9.1 step 5a): waiter XID → the XID it is
    /// blocked on. Paired with `row_wait_cv`; waiters clear their own entry
    /// when their wait completes ([`Self::wait_for`]).
    row_wait_registry: Mutex<HashMap<TxnId, TxnId>>,
    /// Broadcast on every commit/abort (see [`Self::end_txn`]) so row-lock
    /// waiters re-check whether their blocking XID left the active set.
    row_wait_cv: Condvar,
    /// Deadlock-victim flags (M2c Stage R): shared with the engine's
    /// `LockManager` and the `DeadlockDetector` via
    /// [`Self::with_deadlock_victims`]. A manager built without one gets a
    /// private, never-marked registry, which preserves the Stage P
    /// "waits are never interrupted" behavior. Leaf lock — see the
    /// lock-order note on [`Self::wait_for`].
    deadlock_victims: Arc<DeadlockVictims>,
}

impl TxnManager {
    /// Create a transaction manager over the engine's shared components.
    pub fn new(
        txn_id_clock: TxnIdClock,
        wal: Arc<dyn CommitWal>,
        clog: Arc<dyn ClogAccessor>,
    ) -> Self {
        Self {
            txn_id_clock,
            wal,
            clog,
            shared: Arc::new(Mutex::new(TxnShared::default())),
            live_snapshots: Arc::new(AtomicUsize::new(0)),
            commit_barrier: Arc::new(RwLock::new(())),
            row_wait_registry: Mutex::new(HashMap::new()),
            row_wait_cv: Condvar::new(),
            deadlock_victims: Arc::new(DeadlockVictims::new()),
        }
    }

    /// Install the shared deadlock-victim registry (M2c Stage R). Builder
    /// style: call before the manager is wrapped in an `Arc` and shared.
    /// The engine passes the SAME registry to the `LockManager` and the
    /// `DeadlockDetector`, so a mark by the detector is visible to both
    /// wait loops.
    pub fn with_deadlock_victims(mut self, victims: Arc<DeadlockVictims>) -> Self {
        self.deadlock_victims = victims;
        self
    }

    /// The victim registry this manager checks in [`Self::wait_for`].
    pub fn deadlock_victims(&self) -> Arc<DeadlockVictims> {
        Arc::clone(&self.deadlock_victims)
    }

    /// Broadcast to row-lock waiters WITHOUT a state change (M2c Stage R):
    /// the deadlock detector calls this after marking a victim so a waiter
    /// parked in [`Self::wait_for`] re-checks its victim flag. The notify
    /// is delivered under the registry mutex, matching `end_txn`'s wakeup
    /// discipline — a waiter checks its predicates and sleeps atomically
    /// with respect to this mutex, so the mark cannot be missed.
    pub fn notify_row_waiters(&self) {
        let _registry = self.row_wait_registry.lock();
        self.row_wait_cv.notify_all();
    }

    /// The commit/checkpoint barrier (M2c Stage P). The engine installs
    /// this into the pg-storage checkpoint coordinator
    /// (`CheckpointCoordinator::set_commit_barrier`) at open time so every
    /// checkpoint's ATT sampling + CLOG flush runs under the write guard
    /// while commits/aborts run under read guards.
    pub fn commit_barrier(&self) -> Arc<RwLock<()>> {
        Arc::clone(&self.commit_barrier)
    }

    /// Begin a transaction: allocate a fresh XID and mark it active.
    ///
    /// The XID's CLOG entry is left implicit (`InProgress`) until commit/abort
    /// records the terminal state.
    ///
    /// The clock alloc and the active-set insert happen under the SAME lock
    /// (PostgreSQL: "store the new XID into the shared ProcArray before
    /// releasing XidGenLock"). Splitting them — a wait-free `alloc` followed
    /// by a locked `insert` — opens a window where a concurrent
    /// [`Self::snapshot`] can read `xmax = X+1` while `X` is not yet in the
    /// active set: `X < xmax`, `X ∉ xip`, and once `X` commits the snapshot
    /// sees its writes — a snapshot-isolation violation, because `X`
    /// linearized *after* the snapshot was taken. Holding the lock across
    /// both steps makes the pair atomic: any `xid < snapshot.xmax` is either
    /// in `xip` or already terminal.
    pub fn begin_txn(&self) -> TxnId {
        let mut shared = self.shared.lock();
        let xid = self.txn_id_clock.alloc();
        shared.active.insert(xid);
        xid
    }

    /// Commit `xid` following the four-step hard order (§3 P1-5).
    ///
    /// Returns an error (leaving the CLOG bit unflipped) if the WAL append or
    /// fsync fails, so a non-durable commit is never observable as committed.
    ///
    /// # Table locks are NOT released here
    ///
    /// The manager knows nothing about the table `LockManager` (it lives in
    /// pg-engine, keyed by XID). Callers that acquired table locks through
    /// the engine MUST pair this with `LockManager::release_all(xid)` — the
    /// engine's `TxnHandle::commit` / `auto_commit` do so; a raw
    /// `commit_txn` through `Engine::txn_manager()` leaves the transaction's
    /// table locks behind, which later DDL (`AccessExclusive`) wedges on.
    ///
    /// # Failure semantics of the active set
    ///
    /// If step 1 or step 2 fails, `xid` is left in the active set on purpose.
    /// The commit is not durable, so the transaction is still logically
    /// in-progress from every reader's point of view; keeping it active
    /// reflects that. M2a runs auto-commit (one caller owns the XID and will
    /// not retry after an `Err`), so the stale entry is harmless — the process
    /// tears down on a WAL error anyway (the writer marks itself shut down on
    /// fsync failure). A future multi-statement layer that retries commits must
    /// treat the active entry as authoritative and re-drive the same four steps.
    ///
    /// # Step 3/4 ordering
    ///
    /// The CLOG bit is the source of truth for visibility and is flipped
    /// (step 3) *before* the XID leaves the active set (step 4). A concurrent
    /// reader that observes the XID still active will consult the CLOG and may
    /// already see `Committed`; that is correct — the commit is durable by
    /// step 2, so treating it as committed the instant the bit flips is sound.
    /// The reverse order (remove-then-set) would open a window where the XID is
    /// neither active nor yet Committed, i.e. momentarily invisible as either.
    pub fn commit_txn(&self, xid: TxnId) -> Result<()> {
        // Commit-barrier read guard for the WHOLE hard order (M2c Stage P):
        // a checkpoint holds the write guard across its ATT sampling + CLOG
        // flush, so this commit's `set_state` can never land in the window
        // where it would be present in neither the fsynced CLOG nor the
        // replay. The guard is taken here, inside the manager, so every
        // caller — engine auto-commit, explicit TxnHandle, or direct
        // `txn_manager()` access — is covered by construction.
        let _barrier = self.commit_barrier.read();
        // Test-only failure injection (F7 regression): fail before any WAL
        // work, simulating a WAL-fatal commit.
        if test_hooks::commit_txn_should_fail() {
            return Err(pg_storage::error::StorageError::WalWriteFailed(format!(
                "injected commit failure for xid {}",
                xid.0
            )));
        }
        // 1. Append the commit record.
        let lsn = self.wal.append(WalRecord::txn_commit(xid)?)?;
        // 2. fsync it — the commit becomes durable here.
        self.wal.flush_to(lsn)?;
        // 3. Flip the in-memory CLOG bit (only after the record is durable).
        // 4. Drop the XID from the active set and wake row-lock waiters.
        self.end_txn(xid, TxnState::Committed);
        Ok(())
    }

    /// Abort `xid`, recording a durable `TxnAbort` before the CLOG bit.
    ///
    /// ABORTED entries are never garbage-collected (v2.3-2), so recovery can
    /// always distinguish an aborted XID from one that never ran.
    ///
    /// Failure and ordering semantics mirror [`Self::commit_txn`]: on a WAL
    /// error `xid` stays active (the abort is not durable), and the CLOG bit is
    /// set before the active-set removal. The same table-lock caveat applies:
    /// raw callers must pair this with `LockManager::release_all(xid)` (see
    /// [`Self::commit_txn`]).
    pub fn abort_txn(&self, xid: TxnId) -> Result<()> {
        // Same commit-barrier read guard as commit_txn (see there).
        let _barrier = self.commit_barrier.read();
        let lsn = self.wal.append(WalRecord::txn_abort(xid)?)?;
        self.wal.flush_to(lsn)?;
        self.end_txn(xid, TxnState::Aborted);
        Ok(())
    }

    /// The shared tail of commit/abort: flip the CLOG bit, drop the XID
    /// from the active set, then broadcast to row-lock waiters (§9.1 step
    /// 5d, M2c Stage P).
    ///
    /// # Broadcast ordering requirement
    ///
    /// The `notify_all` runs strictly AFTER `clog.set_state` (and after the
    /// active-set removal): a woken waiter that re-reads the CLOG must see
    /// the terminal state, never a stale `InProgress` that would send it
    /// back to sleep on a condvar nobody will signal again. The broadcast
    /// itself is delivered under the registry mutex — `wait_for` checks its
    /// predicate and sleeps atomically with respect to that mutex, so the
    /// removal-then-notify sequence cannot be missed (a waiter either sees
    /// the XID gone before sleeping, or is woken after it).
    ///
    /// Registry *entries* are not touched here: each waiter clears its own
    /// edge on wake (`wait_for` removes it), which keeps the wait-for graph
    /// free of stale edges without end_txn having to scan for them.
    ///
    /// # Lock order
    ///
    /// All mutexes here are taken SEQUENTIALLY, never nested; the only
    /// nested directions anywhere in this manager are registry → shared
    /// (active set + snapshot registry, M3 Stage A: one lock) and registry
    /// → deadlock-victims (both inside [`Self::wait_for`]). The
    /// victims mutex is a LEAF (taken alone above, and never held while
    /// acquiring anything else anywhere), so no inversion is possible.
    fn end_txn(&self, xid: TxnId, state: TxnState) {
        // Stage R hygiene: drop a stale victim flag for the ending XID
        // (the detector may have marked it after its last wait completed).
        // Leaf mutex taken alone — no lock-order interaction. A mark that
        // lands AFTER this clear is pruned by the detector's next tick
        // (the XID is no longer active).
        self.deadlock_victims.clear(xid);
        self.clog.set_state(xid, state);
        // Active-set removal AFTER the CLOG bit; see the commit_txn doc on
        // the step 3/4 ordering argument.
        self.shared.lock().active.remove(&xid);
        // Wake row-lock waiters (§9.1 5d) AFTER the terminal state is
        // visible; see the ordering doc above.
        let _registry = self.row_wait_registry.lock();
        self.row_wait_cv.notify_all();
    }

    /// Register a row-lock wait edge (§9.1 step 5a): `self_xid` is about to
    /// block on `blocking_xid`. Idempotent — re-registering the same waiter
    /// overwrites its edge, matching the protocol's restart-from-step-1
    /// loop where a waiter may re-block on a *different* XID.
    pub fn register_row_wait(&self, self_xid: TxnId, blocking_xid: TxnId) {
        self.row_wait_registry.lock().insert(self_xid, blocking_xid);
    }

    /// Drop `self_xid`'s wait edge, if any. Called on paths that leave the
    /// wait protocol without going through [`Self::wait_for`] (e.g. the
    /// caller aborts); `wait_for` itself clears the edge on completion.
    pub fn unregister_row_wait(&self, self_xid: TxnId) {
        self.row_wait_registry.lock().remove(&self_xid);
    }

    /// Snapshot of all row-lock wait edges `(waiter, waiting_on)` — the
    /// row-lock half of Stage R's wait-for graph (the table-lock half comes
    /// from `LockManager::table_lock_state`).
    pub fn wait_edges(&self) -> Vec<(TxnId, TxnId)> {
        let mut edges: Vec<(TxnId, TxnId)> = self
            .row_wait_registry
            .lock()
            .iter()
            .map(|(&w, &b)| (w, b))
            .collect();
        edges.sort_unstable();
        edges
    }

    /// Block until `blocking_xid` leaves the active set (§9.1 step 5c).
    ///
    /// Returns immediately when `blocking_xid` is already terminated
    /// (committed/aborted XIDs are removed from the active set by
    /// (`Self::end_txn`). Spurious wakeups are handled by looping on the
    /// predicate; the wakeup sources are `end_txn`'s broadcast and the
    /// deadlock detector's [`Self::notify_row_waiters`] (Stage R).
    ///
    /// While blocked this holds NO `TxnManager` lock except the registry
    /// mutex the condvar releases — the active set, CLOG, and WAL stay
    /// available to the blocking transaction's own commit/abort, which is
    /// exactly the progress the waiter is sleeping on.
    ///
    /// # Lock order
    ///
    /// The only legal nestings are registry → shared (active set + snapshot
    /// registry) and registry → deadlock-victims (this function holds the
    /// registry mutex and takes those two inside it). The victims mutex is
    /// a LEAF: nothing is ever
    /// acquired while holding it, and the detector never holds it while
    /// taking the registry mutex (mark first, then lock-and-notify), so no
    /// inversion is possible. `end_txn` takes its locks sequentially,
    /// never nested.
    ///
    /// # Interruption by the deadlock detector (M2c Stage R)
    ///
    /// The victim flag is checked FIRST on every iteration, under the
    /// registry mutex: the detector marks the flag and then notifies under
    /// the same mutex, so a mark can never slip between the check and the
    /// sleep. On a hit the waiter consumes the flag (`take`), clears its
    /// own registry edge (same cleanup as a normal wake), and returns
    /// [`TxnError::DeadlockVictim`]. If the blocking XID terminated in the
    /// same instant the mark landed, the victim error wins — semantically
    /// safe (retryable), and it keeps the flag from leaking into the
    /// transaction's NEXT wait.
    ///
    /// A wait that is never ended by commit/abort NOR interrupted by the
    /// detector still never returns (e.g. the blocking transaction's WAL
    /// fsync failed and the process is tearing down — a process-level
    /// failure policy, not a liveness bug).
    ///
    /// On success the waiter's registry edge is cleared (§9.1: waiters
    /// clear their own entries on wake).
    ///
    /// # Errors
    ///
    /// - [`TxnError::SelfWait`] if `self_xid == blocking_xid` — a
    ///   transaction waiting on itself is a caller bug, not a schedulable
    ///   state.
    /// - [`TxnError::DeadlockVictim`] if the detector chose this
    ///   transaction to break a wait-for cycle.
    pub fn wait_for(
        &self,
        self_xid: TxnId,
        blocking_xid: TxnId,
    ) -> std::result::Result<(), TxnError> {
        if self_xid == blocking_xid {
            return Err(TxnError::SelfWait(self_xid));
        }
        let mut registry = self.row_wait_registry.lock();
        loop {
            // Victim check FIRST, under the registry mutex (see the doc
            // above for the missed-wakeup argument).
            if self.deadlock_victims.take(self_xid) {
                registry.remove(&self_xid);
                return Err(TxnError::DeadlockVictim(self_xid));
            }
            // Predicate: the blocking XID has left the active set. Checked
            // while holding the registry mutex; `end_txn` notifies under
            // the same mutex after removing the XID, so no wakeup is lost.
            if !self.shared.lock().active.contains(&blocking_xid) {
                registry.remove(&self_xid);
                return Ok(());
            }
            self.row_wait_cv.wait(&mut registry);
        }
    }

    /// Snapshot of the currently active XIDs (test/observability helper).
    pub fn active_xids(&self) -> Vec<TxnId> {
        let mut v: Vec<TxnId> = self.shared.lock().active.iter().copied().collect();
        v.sort_unstable();
        v
    }

    /// Is `xid` a live, in-flight transaction? See [`RowWaiter::is_active`]
    /// for how the row-lock gate uses this to tell a crashed stamper from
    /// an active one.
    pub fn is_active(&self, xid: TxnId) -> bool {
        self.shared.lock().active.contains(&xid)
    }
    /// Take a real Snapshot-Isolation snapshot for `current_xid`
    /// (tech-selection §7.1), registering its `xmin` in the vacuum-horizon
    /// registry ATOMICALLY with its construction (M3 Stage A, §3.3 v1.3 B1).
    ///
    /// The snapshot reads `xmax` from the XID clock and `xip` from the active
    /// set; `xmin` is the smallest active XID (or `xmax` when the active set
    /// is empty), and `curcid` starts at 0 (the executor advances it per
    /// statement, §7.1 Q4). The caller's own XID may appear in `xip` when it
    /// is still active; the oracle's `xmin == self_xid` branch is checked
    /// before `xip`, so this is harmless and matches PG, which also records
    /// the snapshot taker among the running XIDs.
    ///
    /// The returned [`SnapshotGuard`] unregisters the snapshot on `Drop`;
    /// the caller keeps it alive for exactly the snapshot's use lifetime
    /// (transaction handle, auto-commit frame, or reader call frame — the
    /// six engine call sites enumerated in §3.2). This is the ONLY
    /// registered construction point in the system: `Snapshot` fields are
    /// pg-txn-private (see `snapshot.rs` module docs), and
    /// [`Snapshot::everything`] is the explicit never-registered special
    /// case.
    ///
    /// # Why registration is sunk into this critical section (B1)
    ///
    /// A "construct first, let the caller register afterwards" wrapper is
    /// FORBIDDEN: the window between construction and registration lets
    /// vacuum compute a horizon that misses an in-flight snapshot.
    /// Counterexample (coding-plan Stage A): U begins with xid=15, takes a
    /// snapshot but has not registered it yet; vacuum sees an empty
    /// registry and takes `horizon = clock.current()`; a deleter D commits;
    /// the row with `xmax = 18` (committed) that U's snapshot must still
    /// see is reclaimed under it. `AccessExclusive` cannot close this
    /// window because taking the snapshot precedes ANY lock acquisition.
    /// Registering inside the SAME critical section that reads active set +
    /// clock makes "snapshot exists ⇒ its xmin is in the registry"
    /// hold from the moment this function returns, and
    /// `oldest_snapshot_xmin` reads the registry under the same lock, so a
    /// horizon can never skip past a returned-but-alive snapshot's xmin.
    ///
    /// # Atomicity argument
    ///
    /// The shared-state mutex is the single serialization point for
    /// membership changes: `begin_txn` inserts, `commit_txn`/`abort_txn`
    /// remove, `snapshot` registers, all under this lock. `snapshot` holds
    /// the lock while reading **both** the clock and the set, which defines
    /// the logical instant — `xip` and `xmax` are mutually consistent by
    /// construction:
    ///
    /// - Every XID in the set was allocated before its insert, and the insert
    ///   happened-before our lock acquisition, so every `xip` entry is
    ///   strictly below the `xmax` we read inside the same critical section
    ///   (the invariant `xmin <= xip[i] < xmax` holds).
    /// - XID allocation and active-set insertion happen together inside this
    ///   same lock (`begin_txn`), so no "allocated but not yet inserted" XID
    ///   can interleave with our read; any concurrent begin either completed
    ///   before our critical section (visible in the set) or starts after
    ///   (its XID lands at or above our `xmax`, judged "future" — invisible
    ///   — which is correct: that begin is not yet observable to anyone).
    /// - A concurrent commit between its CLOG-bit flip (step 3) and its
    ///   active-set removal (step 4) leaves the XID in our `xip` with
    ///   CLOG = Committed; the oracle consults `xip` before the CLOG, so the
    ///   transaction stays invisible — correct, because at the snapshot's
    ///   logical instant the commit had not completed (removal is the
    ///   completion signal).
    pub fn snapshot(&self, current_xid: TxnId) -> (Snapshot, SnapshotGuard) {
        let mut shared = self.shared.lock();
        let xmax = self.txn_id_clock.current();
        let mut xip: SmallVec<[TxnId; 32]> = shared.active.iter().copied().collect();
        xip.sort_unstable();
        let xmin = xip.first().copied().unwrap_or(xmax);
        // Register in the SAME critical section (B1, see the doc above).
        *shared.snapshot_xmins.entry(xmin).or_insert(0) += 1;
        self.live_snapshots.fetch_add(1, Ordering::Relaxed);
        let guard = SnapshotGuard {
            shared: Arc::clone(&self.shared),
            live_snapshots: Arc::clone(&self.live_snapshots),
            xmin,
        };
        (
            Snapshot {
                xmin,
                xmax,
                xip,
                current_xid,
                curcid: 0,
            },
            guard,
        )
    }

    /// The vacuum horizon: the smallest `xmin` among all live registered
    /// snapshots (tech-selection §3.3). Empty registry ⇒ fall back to the
    /// smallest ACTIVE xid (not straight to the clock): there is a window
    /// between `begin_txn` and `snapshot()` during which a transaction is
    /// active but has not registered yet, and its eventual snapshot's xmin
    /// can be as low as the smallest xid currently active (PG's OldestXmin
    /// likewise counts both backend xids and snapshot xmins). Only with an
    /// empty registry AND an empty active set does the horizon become the
    /// XID clock's current value — truly no readers. Vacuum samples this
    /// ONCE at start and uses that single horizon for the whole pass; the
    /// §3.3 XID monotonicity argument (`new_snapshot.xmin >= horizon`)
    /// makes snapshots taken mid-vacuum safe.
    pub fn oldest_snapshot_xmin(&self) -> TxnId {
        let shared = self.shared.lock();
        match shared.snapshot_xmins.keys().next() {
            Some(&xmin) => xmin,
            None => shared
                .active
                .iter()
                .min()
                .copied()
                .unwrap_or_else(|| self.txn_id_clock.current()),
        }
    }

    /// Copy of the snapshot registry (`xmin` → refcount), for tests and
    /// observability (M3 Stage A). `Snapshot::everything()` never appears
    /// here — it is the explicit unregistered special case.
    #[doc(hidden)]
    pub fn snapshot_xmin_registry(&self) -> BTreeMap<TxnId, usize> {
        self.shared.lock().snapshot_xmins.clone()
    }

    /// Number of live registered snapshots (== sum of the registry's
    /// refcounts; the count-assertion guardrail cross-checks this).
    #[doc(hidden)]
    pub fn live_registered_snapshots(&self) -> usize {
        self.live_snapshots.load(Ordering::Relaxed)
    }
}

/// M2b Stage N wiring (tech-selection §11.4): the checkpoint coordinator in
/// `pg-storage` captures the ATT snapshot through this trait, keeping the
/// dependency direction `pg-txn` → `pg-storage` (same pattern as
/// `ClogFlush`). The engine installs the manager at open time via
/// `CheckpointCoordinator::set_att_provider`.
impl pg_storage::recovery::AttProvider for TxnManager {
    fn active_xids(&self) -> Vec<TxnId> {
        // Delegates to the inherent method (sorted), so the ATT snapshot
        // file is deterministic.
        TxnManager::active_xids(self)
    }
}

/// The heap AM's row-lock protocol (§9.1 step 5) drives the manager through
/// this narrow surface; every method delegates to the inherent implementation
/// documented there.
impl RowWaiter for TxnManager {
    fn register_row_wait(&self, self_xid: TxnId, blocking_xid: TxnId) {
        TxnManager::register_row_wait(self, self_xid, blocking_xid);
    }

    fn unregister_row_wait(&self, self_xid: TxnId) {
        TxnManager::unregister_row_wait(self, self_xid);
    }

    fn wait_for(&self, self_xid: TxnId, blocking_xid: TxnId) -> std::result::Result<(), TxnError> {
        TxnManager::wait_for(self, self_xid, blocking_xid)
    }

    fn is_active(&self, xid: TxnId) -> bool {
        TxnManager::is_active(self, xid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::InMemoryClogAccessor;

    /// A no-op WAL: append/flush always succeed, so the manager can be driven
    /// without touching disk.
    #[derive(Debug, Default)]
    struct OkWal;

    impl CommitWal for OkWal {
        fn append(&self, _record: WalRecord) -> Result<Lsn> {
            Ok(Lsn::FIRST)
        }

        fn flush_to(&self, _lsn: Lsn) -> Result<()> {
            Ok(())
        }
    }

    fn manager() -> TxnManager {
        TxnManager::new(
            TxnIdClock::new(TxnId::FIRST),
            Arc::new(OkWal),
            Arc::new(InMemoryClogAccessor::new()),
        )
    }

    /// M2b Stage N (§11.4): the manager doubles as the checkpoint
    /// coordinator's ATT snapshot source — begun-but-not-committed XIDs show
    /// up, committed/aborted ones do not.
    #[test]
    fn att_provider_reports_in_flight_xids() {
        use pg_storage::recovery::AttProvider;

        let mgr = manager();
        let t1 = mgr.begin_txn();
        let t2 = mgr.begin_txn();
        let t3 = mgr.begin_txn();
        mgr.commit_txn(t2).unwrap();
        assert_eq!(AttProvider::active_xids(&mgr), vec![t1, t3]);
        mgr.abort_txn(t1).unwrap();
        assert_eq!(AttProvider::active_xids(&mgr), vec![t3]);
    }

    #[test]
    fn snapshot_with_empty_active_set() {
        let mgr = manager();
        let (snap, _guard) = mgr.snapshot(TxnId::INVALID);
        assert!(snap.xip.is_empty());
        // Empty active set: xmin collapses to xmax = next unallocated XID.
        assert_eq!(snap.xmax, TxnId::FIRST);
        assert_eq!(snap.xmin, snap.xmax);
        assert_eq!(snap.curcid, 0);
    }

    #[test]
    fn snapshot_captures_active_set_contents() {
        let mgr = manager();
        let t1 = mgr.begin_txn();
        let t2 = mgr.begin_txn();
        let t3 = mgr.begin_txn();

        let (snap, _guard) = mgr.snapshot(t2);
        assert_eq!(snap.xip.as_slice(), &[t1, t2, t3], "sorted full copy");
        assert_eq!(snap.xmin, t1, "xmin = smallest active XID");
        assert_eq!(snap.xmax, TxnId(4), "xmax = next unallocated XID");
        assert_eq!(snap.current_xid, t2);
        assert_eq!(snap.curcid, 0);
        for &xid in snap.xip.iter() {
            assert!(snap.xmin <= xid && xid < snap.xmax);
        }
    }

    #[test]
    fn snapshot_xmin_xmax_boundaries_track_commit() {
        let mgr = manager();
        let t1 = mgr.begin_txn();
        let t2 = mgr.begin_txn();
        mgr.commit_txn(t1).unwrap();

        let (snap, _guard) = mgr.snapshot(t2);
        assert_eq!(snap.xip.as_slice(), &[t2], "committed XID leaves xip");
        assert_eq!(snap.xmin, t2, "xmin advances past the committed XID");
        assert_eq!(snap.xmax, TxnId(3));

        mgr.commit_txn(t2).unwrap();
        let (snap, _guard2) = mgr.snapshot(TxnId::INVALID);
        assert!(snap.xip.is_empty());
        assert_eq!(snap.xmin, snap.xmax);
    }

    #[test]
    fn snapshot_is_consistent_under_concurrent_begin_commit() {
        // Hammer the manager from multiple threads; every snapshot must
        // satisfy the structural invariants (sorted xip, xmin <= xip < xmax).
        let mgr = Arc::new(manager());
        let mut handles = Vec::new();
        for _ in 0..8 {
            let mgr = Arc::clone(&mgr);
            handles.push(std::thread::spawn(move || {
                for _ in 0..50 {
                    let xid = mgr.begin_txn();
                    let (snap, _guard) = mgr.snapshot(xid);
                    for w in snap.xip.windows(2) {
                        assert!(w[0] < w[1], "xip sorted");
                    }
                    for &entry in snap.xip.iter() {
                        assert!(snap.xmin <= entry && entry < snap.xmax);
                    }
                    mgr.commit_txn(xid).unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert!(mgr.active_xids().is_empty());
    }
}

#[cfg(test)]
mod begin_atomicity_tests {
    //! Regression for the Stage L review P1: `begin_txn` used to split the
    //! clock alloc (wait-free) from the active-set insert (locked). A
    //! concurrent `snapshot()` could then read `xmax = X+1` while `X` was
    //! not yet registered — `X < xmax`, `X ∉ xip`, and after X committed
    //! the snapshot saw its writes: an SI violation (PG avoids this by
    //! registering the XID in ProcArray before releasing XidGenLock).
    use super::*;
    use crate::InMemoryClogAccessor;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    #[derive(Debug)]
    struct NoWal;
    impl CommitWal for NoWal {
        fn append(&self, _record: WalRecord) -> Result<Lsn> {
            Ok(Lsn::FIRST)
        }
        fn flush_to(&self, _lsn: Lsn) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn snapshot_never_sees_alloc_without_insert() {
        let mgr = Arc::new(TxnManager::new(
            TxnIdClock::new(TxnId::FIRST),
            Arc::new(NoWal),
            Arc::new(InMemoryClogAccessor::new()),
        ));
        let mgr2 = Arc::clone(&mgr);

        // A "slow begin": hold the active lock, alloc, sleep, then insert —
        // exactly the old implementation's interleaving. With the fix,
        // `snapshot()` blocks on the same lock until the insert completes.
        let slow = thread::spawn(move || {
            let mut shared = mgr2.shared.lock();
            let xid = mgr2.txn_id_clock.alloc();
            thread::sleep(Duration::from_millis(50));
            shared.active.insert(xid);
            drop(shared);
            xid
        });

        // While the slow begin sleeps, take a snapshot.
        let (snap, _guard) = mgr.snapshot(TxnId::INVALID);
        let xid = slow.join().unwrap();

        // The snapshot must either predate the alloc (xid >= xmax) or have
        // the xid registered in xip. The middle state — xmax above xid while
        // xid is absent from xip — is the SI violation and must not occur.
        assert!(
            xid.0 >= snap.xmax.0 || snap.xip.contains(&xid),
            "snapshot saw alloc-without-insert: xid={xid:?}, xmax={:?}, xip={:?}",
            snap.xmax,
            snap.xip
        );
    }
}
