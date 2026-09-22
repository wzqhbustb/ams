//! Checkpoint coordinator.
//!
//! The checkpoint coordinator performs fuzzy checkpoints: it flushes dirty
//! buffer pool pages to disk while concurrent readers and writers continue to
//! access other pages. It writes `CheckpointBegin` / `CheckpointEnd` WAL records
//! and updates the superblock so that recovery knows where to start replay.
//!
//! Both manual checkpoints (`trigger_checkpoint`) and background periodic
//! checkpoints are supported. Automatic checkpoints run on a dedicated thread
//! until `shutdown` is called or the coordinator is dropped.

use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::sync::atomic::{AtomicBool, Ordering};
use crate::sync::{Mutex, RwLock};
use tracing::{debug, error, info, warn};

use crate::buffer_pool::BufferPool;
use crate::config::StorageConfig;
use crate::error::{Result, StorageError};
use crate::freelist_meta::FreelistMeta;
use crate::oid::OidCounter;
use crate::page_allocator::PageAllocator;
use crate::superblock::Superblock;
use crate::txn_id::TxnIdClock;
use crate::types::Lsn;
use crate::wal::record::WalRecord;
use crate::wal::writer::WalWriter;

/// Number of recent checkpoints whose ATT/DPT snapshot files are retained
/// under `meta/` (tech-selection §11.4 P2-7). Older files are deleted
/// synchronously at checkpoint completion. Only the superblock's current
/// checkpoint group is usable for recovery — older snapshots' WAL segments
/// have already been recycled — but retaining a small window guards against
/// a crash that corrupts the very latest snapshot file.
const RETAINED_SNAPSHOT_CHECKPOINTS: usize = 3;

/// Coordinates fuzzy checkpoints and persists the resulting anchor state.
#[derive(Debug)]
pub struct CheckpointCoordinator {
    data_dir: std::path::PathBuf,
    config: StorageConfig,
    superblock: Arc<Mutex<Superblock>>,
    buffer_pool: Arc<BufferPool>,
    page_allocator: Arc<Mutex<PageAllocator>>,
    wal_writer: Arc<WalWriter>,
    /// Serializes checkpoint execution so that manual and background checkpoints
    /// do not interleave.
    checkpoint_lock: Arc<Mutex<()>>,
    /// Source of the next OID to allocate, written into the v2 superblock
    /// on every checkpoint (Stage H). Initialized from the superblock's
    /// persisted `next_oid`, so checkpoints never roll the value back — not
    /// even to `Oid::FIRST_USER` — before the catalog wires its allocator
    /// via [`set_next_oid_source`](Self::set_next_oid_source).
    ///
    /// The counter holds the *next OID to hand out* (same semantics as
    /// `Superblock::next_oid`). Since Stage N the v2 CheckpointEnd WAL record
    /// carries the same value, so the superblock and the WAL agree on
    /// `next_oid` across checkpoints.
    ///
    /// Wrapped in `Arc<Mutex<..>>` and shared with the background thread's
    /// clone so that a [`set_next_oid_source`](Self::set_next_oid_source)
    /// call made *after* the background checkpointer starts (the catalog
    /// wires its allocator only once `Catalog::open` runs, which may follow
    /// [`start_background_checkpointing`](Self::start_background_checkpointing))
    /// is still observed by background checkpoints.
    next_oid_source: Arc<Mutex<OidCounter>>,
    /// Source of the next transaction ID to allocate, written into the
    /// superblock on every checkpoint (Stage J). Mirrors `next_oid_source`:
    /// seeded from the superblock's persisted `next_txn_id`, replaced by the
    /// `TxnManager`'s clock via [`set_next_txn_id_source`](Self::set_next_txn_id_source),
    /// and read once per checkpoint. Persisting the live clock (instead of the
    /// old hardcoded `TxnId::FIRST`) lets recovery reseed the XID clock so
    /// post-restart transactions never reuse a committed XID.
    ///
    /// Shared with the background thread's clone for the same reason as
    /// `next_oid_source`.
    next_txn_id_source: Arc<Mutex<TxnIdClock>>,
    /// Flush hook for the disk CLOG (M2b Stage L), invoked between
    /// `CheckpointBegin` and `CheckpointEnd` — the single authoritative CLOG
    /// flush point (tech-selection §6.4, v2.3-21). `None` until the engine
    /// layer (pg-engine) installs its `ClogBuffer` via
    /// [`set_clog_flush`](Self::set_clog_flush); M1/M2a configurations (no
    /// disk CLOG) leave it unset and checkpoints skip the CLOG flush
    /// entirely.
    ///
    /// Wrapped in `Arc<Mutex<Option<..>>>` and shared with the background
    /// thread's clone for the same reason as `next_oid_source`: the engine
    /// may install the hook after background checkpointing has started.
    clog_flush: Arc<Mutex<Option<Arc<dyn crate::clog::ClogFlush>>>>,
    /// Source of the ATT snapshot written at every checkpoint (M2b Stage N,
    /// tech-selection §11.4). `None` until the engine layer wires its
    /// `TxnManager` via [`set_att_provider`](Self::set_att_provider);
    /// configurations without a provider snapshot an empty ATT, which the
    /// analysis phase reads as "no snapshot — rebuild by a full WAL scan
    /// from the checkpoint LSN".
    ///
    /// Wrapped in `Arc<Mutex<Option<..>>>` and shared with the background
    /// thread's clone for the same reason as `clog_flush`.
    att_provider: Arc<Mutex<Option<Arc<dyn crate::recovery::AttProvider>>>>,
    /// Commit/checkpoint barrier (M2c Stage P): when installed,
    /// [`trigger_checkpoint`](Self::trigger_checkpoint) holds the WRITE guard
    /// across the whole checkpoint critical section — from `begin_lsn`
    /// capture through ATT/DPT sampling and the CLOG flush to WAL
    /// recycling — while `TxnManager::commit_txn`/`abort_txn` hold READ
    /// guards for their hard order. This is the barrier the `AttProvider`
    /// contract has always required (see
    /// [`set_att_provider`](Self::set_att_provider)), sunk into the
    /// storage/txn layers so it is enforced by construction instead of by
    /// caller discipline. `None` keeps M1/M2a-style standalone
    /// configurations (no `TxnManager`) working unchanged.
    ///
    /// Same `Arc<Mutex<Option<..>>>` slot pattern as `clog_flush` /
    /// `att_provider`: the engine may install it after background
    /// checkpointing has started and the next checkpoint still picks it up.
    commit_barrier: Arc<Mutex<Option<Arc<RwLock<()>>>>>,
    shutdown: Arc<AtomicBool>,
    background_handle: Mutex<Option<JoinHandle<()>>>,
}

impl CheckpointCoordinator {
    /// Create a new checkpoint coordinator.
    ///
    /// The caller must supply already-opened storage components. Background
    /// checkpointing is not started automatically; call
    /// [`start_background_checkpointing`](Self::start_background_checkpointing)
    /// to enable it.
    pub fn new(
        data_dir: impl Into<std::path::PathBuf>,
        config: &StorageConfig,
        superblock: Arc<Mutex<Superblock>>,
        buffer_pool: Arc<BufferPool>,
        page_allocator: Arc<Mutex<PageAllocator>>,
        wal_writer: Arc<WalWriter>,
    ) -> Self {
        // Seed the OID source from the persisted superblock value so every
        // checkpoint — including ones fired before the catalog wires its
        // allocator — persists a monotone `next_oid`.
        let next_oid = superblock.lock().next_oid;
        let next_txn_id = superblock.lock().next_txn_id;
        Self {
            data_dir: data_dir.into(),
            config: config.clone(),
            superblock,
            buffer_pool,
            page_allocator,
            wal_writer,
            checkpoint_lock: Arc::new(Mutex::new(())),
            next_oid_source: Arc::new(Mutex::new(OidCounter::new(next_oid))),
            next_txn_id_source: Arc::new(Mutex::new(TxnIdClock::new(next_txn_id))),
            clog_flush: Arc::new(Mutex::new(None)),
            att_provider: Arc::new(Mutex::new(None)),
            commit_barrier: Arc::new(Mutex::new(None)),
            shutdown: Arc::new(AtomicBool::new(false)),
            background_handle: Mutex::new(None),
        }
    }

    /// Install the source of `next_oid` values persisted by checkpoints
    /// (Stage H wiring).
    ///
    /// `source` must hold the next OID to hand out; each
    /// [`trigger_checkpoint`](Self::trigger_checkpoint) reads it and writes the
    /// value into the v2 superblock. The coordinator starts with a counter
    /// seeded from the persisted superblock value; installing the catalog's
    /// allocator replaces it. The source is read once per checkpoint, so it
    /// may be replaced between checkpoints.
    ///
    /// The slot is shared (via `Arc`) with the background checkpoint thread's
    /// clone, so installing a source *after*
    /// [`start_background_checkpointing`](Self::start_background_checkpointing)
    /// still takes effect on the next background checkpoint.
    pub fn set_next_oid_source(&self, source: OidCounter) {
        *self.next_oid_source.lock() = source;
    }

    /// Install the source of `next_txn_id` values persisted by checkpoints
    /// (Stage J wiring).
    ///
    /// `source` is the `TxnManager`'s live [`TxnIdClock`]; each checkpoint
    /// reads its `current()` and writes it into the superblock's `next_txn_id`,
    /// so recovery reseeds the clock past every XID that could already be
    /// referenced by committed data. Mirrors
    /// [`set_next_oid_source`](Self::set_next_oid_source) — shared with the
    /// background thread, read once per checkpoint, replaceable between
    /// checkpoints.
    pub fn set_next_txn_id_source(&self, source: TxnIdClock) {
        *self.next_txn_id_source.lock() = source;
    }

    /// Install the disk CLOG's flush hook (M2b Stage L wiring).
    ///
    /// Each [`trigger_checkpoint`](Self::trigger_checkpoint) calls
    /// [`ClogFlush::flush_dirty`](crate::clog::ClogFlush::flush_dirty) after
    /// emitting `CheckpointBegin` and before emitting `CheckpointEnd` — the
    /// single authoritative CLOG flush point (tech-selection §6.4, v2.3-21).
    /// Until a hook is installed the step is skipped, which keeps M1/M2a
    /// configurations (no disk CLOG) working unchanged.
    ///
    /// The slot is shared (via `Arc`) with the background checkpoint thread's
    /// clone, so installing the hook *after*
    /// [`start_background_checkpointing`](Self::start_background_checkpointing)
    /// still takes effect on the next background checkpoint — same pattern as
    /// [`set_next_oid_source`](Self::set_next_oid_source).
    pub fn set_clog_flush(&self, source: Arc<dyn crate::clog::ClogFlush>) {
        *self.clog_flush.lock() = Some(source);
    }

    /// Install the ATT snapshot source (M2b Stage N wiring; tech-selection
    /// §11.4).
    ///
    /// Each [`trigger_checkpoint`](Self::trigger_checkpoint) calls
    /// [`AttProvider::active_xids`](crate::recovery::AttProvider::active_xids)
    /// once and persists the result as the ATT snapshot file referenced by
    /// the v2 `CheckpointEnd` record. Until a provider is installed the ATT
    /// snapshot is empty — the analysis phase treats an empty ATT snapshot
    /// like a v1 record's empty `att_file`: rebuild from the checkpoint LSN
    /// by a full WAL scan.
    ///
    /// # Commit barrier required for correctness
    ///
    /// The ATT snapshot's correctness depends on serializing checkpoints
    /// against in-flight commits. Without a commit barrier, the following
    /// interleaving is possible:
    ///
    /// 1. A commit's WAL record is appended before the checkpoint's
    ///    `CheckpointBegin`, but its `active.remove` has not yet run.
    /// 2. Phase 2a samples the XID into the ATT snapshot (it is still
    ///    "active").
    /// 3. Phase 4b flushes the CLOG — but `set_state(COMMITTED)` has not
    ///    yet run (it runs after `flush_to` in the commit hard order), so
    ///    the CLOG does NOT carry the Committed bit.
    /// 4. The `CheckpointEnd` is emitted with a snapshot that lists an
    ///    already-committed XID, and the CLOG has no record of its commit.
    ///
    /// After recovery the post-redo ATT filter (engine.rs step 5b) cannot
    /// drop this XID, and it remains permanently "active" — a committed
    /// transaction whose tuples are visible as InProgress forever.
    ///
    /// Since M2c Stage P the barrier is built in: `TxnManager` owns an
    /// internal `RwLock` whose READ guard covers the commit/abort hard
    /// order, and pg-engine installs it here via
    /// [`set_commit_barrier`](Self::set_commit_barrier) so every checkpoint
    /// runs under the WRITE guard. Configurations that drive checkpoints
    /// WITHOUT a `TxnManager`-backed barrier installed (storage-only
    /// setups) remain **unsafe** in the sense above — such callers must
    /// supply their own serialization, or accept the corruption risk.
    ///
    /// The slot is shared (via `Arc`) with the background checkpoint thread's
    /// clone, so installing a provider *after*
    /// [`start_background_checkpointing`](Self::start_background_checkpointing)
    /// still takes effect on the next background checkpoint — same pattern as
    /// [`set_clog_flush`](Self::set_clog_flush).
    pub fn set_att_provider(&self, provider: Arc<dyn crate::recovery::AttProvider>) {
        *self.att_provider.lock() = Some(provider);
    }

    /// Install the commit/checkpoint barrier (M2c Stage P).
    ///
    /// Once installed, every
    /// [`trigger_checkpoint`](Self::trigger_checkpoint) holds the barrier's
    /// WRITE guard across the whole checkpoint critical section — from
    /// `begin_lsn` capture through ATT/DPT sampling and the CLOG flush to
    /// WAL recycling — while `TxnManager::commit_txn`/`abort_txn` hold READ
    /// guards for their hard order. This is the serialization the
    /// `AttProvider` contract (above) has always required, enforced by
    /// construction rather than by caller discipline. pg-engine wires
    /// `TxnManager::commit_barrier()` in at open; storage-only
    /// configurations leave this unset and behave exactly as before.
    ///
    /// Same shared-slot pattern as
    /// [`set_att_provider`](Self::set_att_provider): an install after
    /// background checkpointing has started takes effect on the next
    /// checkpoint.
    pub fn set_commit_barrier(&self, barrier: Arc<RwLock<()>>) {
        *self.commit_barrier.lock() = Some(barrier);
    }

    /// Start a background thread that triggers checkpoints periodically.
    ///
    /// If `config.checkpoint_interval_ms` is 0, this method returns immediately
    /// without starting a thread.
    ///
    /// # Errors
    ///
    /// Returns an error if background checkpointing is already running.
    pub fn start_background_checkpointing(&self) -> Result<()> {
        let interval_ms = self.config.checkpoint_interval_ms;
        if interval_ms == 0 {
            info!("automatic checkpoints are disabled (checkpoint_interval_ms=0)");
            return Ok(());
        }

        let mut handle = self.background_handle.lock();
        if handle.is_some() {
            return Err(StorageError::InvalidConfig(
                "background checkpointing already started".to_string(),
            ));
        }

        let shutdown = Arc::clone(&self.shutdown);
        let coordinator = self.clone_for_background_thread();
        let interval = Duration::from_millis(interval_ms);

        let join_handle = thread::Builder::new()
            .name("pg-checkpoint".to_string())
            .spawn(move || {
                info!(?interval, "background checkpoint thread started");
                loop {
                    // Sleep in small chunks so shutdown is responsive even with
                    // a long checkpoint interval.
                    const TICK_MS: u64 = 100;
                    let mut elapsed = 0u64;
                    while elapsed < interval_ms {
                        if shutdown.load(Ordering::Relaxed) {
                            debug!("background checkpoint thread shutting down");
                            return;
                        }
                        thread::sleep(Duration::from_millis(TICK_MS.min(interval_ms - elapsed)));
                        elapsed += TICK_MS.min(interval_ms - elapsed);
                    }

                    if shutdown.load(Ordering::Relaxed) {
                        debug!("background checkpoint thread shutting down");
                        return;
                    }

                    if let Err(e) = coordinator.trigger_checkpoint() {
                        // Background checkpoints log the error and keep going;
                        // the next checkpoint may succeed.
                        error!(error = %e, "background checkpoint failed");
                    }
                }
            })
            .map_err(StorageError::Io)?;

        *handle = Some(join_handle);
        Ok(())
    }

    /// Perform a checkpoint synchronously and return the `CheckpointBegin` LSN.
    ///
    /// The checkpoint is fuzzy: readers and writers on other pages proceed
    /// concurrently. Flushing an individual page briefly blocks writers on that
    /// page.
    ///
    /// # FPI race fix (Stage B)
    ///
    /// M1 had a race window between `wal_writer.append(begin_record)` returning
    /// and `set_checkpoint_lsn(begin_lsn)`: a `pin_mut` in that window could
    /// skip its FPI because `checkpoint_lsn` was still the old value. Stage B
    /// eliminates the window by pre-reserving the LSN and publishing it before
    /// emitting the record.
    ///
    /// # Freelist snapshot atomicity (Stage E)
    ///
    /// `reserve_lsn`, `set_checkpoint_lsn`, and `freelist.snapshot` are
    /// performed atomically under the `page_allocator` lock. This prevents a
    /// concurrent `free_page` from interleaving between the LSN reservation and
    /// the snapshot: such an interleaving would place the freed page in both
    /// the snapshot (in-memory push) and WAL replay (post-begin_lsn record),
    /// producing a duplicate freelist entry on recovery.
    pub fn trigger_checkpoint(&self) -> Result<Lsn> {
        // Serialize checkpoints so that manual and background checkpoints do not
        // interleave and produce redundant or overlapping checkpoint records.
        let _lock = self.checkpoint_lock.lock();

        // -- Phase 0: commit barrier (M2c Stage P) --------------------------
        //
        // Hold the commit barrier's WRITE guard across the whole checkpoint
        // (when a barrier is installed — pg-engine wires
        // `TxnManager::commit_barrier()` in at open). Commits/aborts hold
        // READ guards for their hard order, so while this checkpoint runs:
        //
        // - a commit durable before `begin_lsn` has already finished its
        //   `clog.set_state` — its bit is inside the Phase 4b CLOG flush;
        // - a commit starting after blocks until the checkpoint completes
        //   and then lands past `begin_lsn` — replay rebuilds it.
        //
        // No commit can sit in the "neither the fsynced CLOG nor the
        // replay" window. See `set_att_provider` for the failure shape this
        // closes. Lock order: checkpoint_lock → commit_barrier → WAL/page
        // locks; the commit side takes commit_barrier (read) before the WAL
        // lock, so the orders compose without a cycle.
        let barrier = self.commit_barrier.lock().clone();
        let _barrier = barrier.as_ref().map(|b| b.write());

        // -- Phase 1: CheckpointBegin ----------------------------------------
        //
        // 1. Atomically reserve the checkpoint LSN, write the
        //    CheckpointBegin record, publish the LSN to the buffer pool, and
        //    snapshot the freelist — all under the page_allocator lock.
        //    `reserve_and_append` holds the WAL lock for the full reserve +
        //    write, eliminating the zero-filled hole between `reserve_lsn` and
        //    `append_at` that a crash would interpret as end-of-WAL (Stage N
        //    review, P0-3).
        //
        //    This is the core correctness invariant for concurrent free_page
        //    during a fuzzy checkpoint:
        //
        //    - A free_page that completes BEFORE this critical section has its
        //      page in the snapshot AND its WAL record at LSN < begin_lsn (not
        //      replayed). No duplicate.
        //    - A free_page that runs AFTER this critical section has its page
        //      NOT in the snapshot AND its WAL record at LSN > begin_lsn
        //      (replayed exactly once). No duplicate.
        //    - No free_page can run DURING this critical section (blocked on
        //      the page_allocator lock), so there is no window where a page
        //      is both in the snapshot and has a post-begin_lsn WAL record.
        //
        //    Lock order is page_allocator → WAL inner (reserve_and_append),
        //    which matches free_page's lock order — no deadlock.
        let (begin_lsn, freelist_snap) = {
            let pa = self.page_allocator.lock();
            let begin_lsn = self
                .wal_writer
                .reserve_and_append(WalRecord::checkpoint_begin())?;
            // Publish the checkpoint LSN immediately so any pin_mut from this
            // point on writes an FPI. Must be inside the lock to preserve the
            // Stage B FPI race fix (no window between reserve and publish).
            self.buffer_pool.set_checkpoint_lsn(begin_lsn);
            let snap = pa.snapshot(begin_lsn);
            (begin_lsn, snap)
        };
        debug!(%begin_lsn, "checkpoint begin (LSN reserved, record emitted, freelist snapshot taken)");

        // -- Phase 2a: ATT/DPT snapshot files (Stage N, §11.4) ---------------
        //
        // 2a. Capture the ARIES tables as of the checkpoint begin and persist
        //     them as snapshot files BEFORE flushing any dirty pages:
        //
        //     - ATT: the provider's in-flight XIDs (empty when no provider is
        //       installed — analysis then rebuilds by a full WAL scan, same
        //       as for a v1 CheckpointEnd).
        //     - DPT: `(page_id, rec_lsn)` per dirty frame. This must be
        //       sampled before the Phase 2 flush below, which would drain
        //       the very dirty set the DPT describes.
        //
        //     This is the first leg of the three-step hard order (§11.4,
        //     same style as the §3 P1-5 commit hard order):
        //
        //       fsync(att/dpt snapshot files)     <- here, via write_atomic
        //         -> wal.append(CheckpointEnd v2) <- step 6
        //         -> wal.flush_to(ckpt_end_lsn)   <- step 7
        //
        //     `write_atomic` fsyncs the file, renames, and fsyncs the
        //     directory, so by the time the v2 CheckpointEnd names these
        //     files they are durable. A snapshot write failure aborts the
        //     checkpoint — same discipline as a failed page flush: emitting a
        //     v2 record that references missing snapshot files would send
        //     recovery after dangling paths.
        let att: Vec<crate::types::TxnId> = self
            .att_provider
            .lock()
            .as_ref()
            .map(|p| p.active_xids())
            .unwrap_or_default();
        let dpt = self.buffer_pool.dirty_page_snapshot();
        let att_file = format!("meta/att-{:016}.snapshot", begin_lsn.0);
        let dpt_file = format!("meta/dpt-{:016}.snapshot", begin_lsn.0);
        self.write_snapshot_file(&att_file, &att)?;
        self.write_snapshot_file(&dpt_file, &dpt)?;
        debug!(
            att = att.len(),
            dpt = dpt.len(),
            "wrote ATT/DPT snapshot files"
        );

        // -- Phase 2: Flush dirty pages --------------------------------------

        // 3. Collect dirty pages. This is a snapshot; pages may become clean or
        //    dirty while we iterate.
        let dirty_pages = self.buffer_pool.dirty_page_ids();
        debug!(count = dirty_pages.len(), "collected dirty pages");

        // 4. Flush each dirty page. flush() enforces WAL-before-data internally.
        //    M1 is conservative: any flush failure aborts the checkpoint so that
        //    the superblock is never updated to a checkpoint that did not
        //    actually complete.
        //
        //    PageNotFound is harmless: the page was evicted and flushed between
        //    dirty_page_ids() and our flush() call, so it is already durable.
        let mut flushed = 0usize;
        for page_id in dirty_pages {
            match self.buffer_pool.flush(page_id) {
                Ok(()) => flushed += 1,
                Err(StorageError::PageNotFound(_)) => {
                    debug!(%page_id, "page was evicted and flushed before checkpoint reached it");
                }
                Err(e) => {
                    return Err(StorageError::CheckpointFailed(format!(
                        "failed to flush page {page_id} during checkpoint: {e}"
                    )));
                }
            }
        }
        debug!(%flushed, "flushed dirty pages");

        // 4b. Flush the disk CLOG's dirty frames (writeback + fsync) — the
        //     single authoritative CLOG flush point (tech-selection §6.4,
        //     v2.3-21): after CheckpointBegin is emitted, before
        //     CheckpointEnd. Nowhere else flushes the CLOG; bits not yet
        //     fsynced at a crash are rebuilt from TxnCommit/TxnAbort WAL
        //     records by redo. Skipped when no hook is installed (M1/M2a).
        //     As with page flushes, a failure aborts the checkpoint so the
        //     superblock never advances past work that did not complete.
        let clog_flush = self.clog_flush.lock().clone();
        if let Some(clog_flush) = clog_flush {
            clog_flush.flush_dirty().map_err(|e| {
                StorageError::CheckpointFailed(format!(
                    "failed to flush CLOG during checkpoint: {e}"
                ))
            })?;
            debug!("flushed dirty CLOG frames");
        }

        // -- Phase 3: CheckpointEnd and superblock update ---------------------

        // 5. Capture allocator state for the checkpoint end record. `next_oid`
        //    is read once here so the v2 CheckpointEnd record and the
        //    superblock persist the same value.
        let next_page_id = self.page_allocator.lock().next_page_id();
        let next_txn_id = self.next_txn_id_source.lock().current();
        let next_oid = self.next_oid_source.lock().current();

        // 6. Write CheckpointEnd (v2 payload, flags version nibble = 1; §11.4).
        //    The snapshot files are already fsynced (Phase 2a), satisfying the
        //    first leg of the three-step hard order. This marks the point at
        //    which the superblock can be safely updated.
        let end_record = WalRecord::checkpoint_end(
            begin_lsn,
            next_page_id,
            next_txn_id,
            next_oid.0,
            att_file,
            dpt_file,
        )?;
        let end_lsn = self.wal_writer.append(end_record)?;
        debug!(%end_lsn, "checkpoint end");

        // 7. Ensure CheckpointEnd and everything before it is fsynced before the
        //    freelist snapshot or superblock is written. (append() no longer
        //    fsyncs implicitly; this explicit flush is required.) Flush through
        //    the record's END boundary: flush_to's prefix semantics early-exit
        //    on the record's own start LSN whenever a group-commit wave (or a
        //    reopen) already reached it, leaving CheckpointEnd itself unsynced
        //    (see flush_to's rustdoc).
        let end_boundary = self.wal_writer.current_lsn();
        self.wal_writer.flush_to(end_boundary)?;

        // 8. Write the freelist snapshot BEFORE the superblock. This is an
        //    acceleration hint for recovery: if present and valid, recovery
        //    seeds the allocator freelist from it and only replays
        //    post-checkpoint WAL records. If the snapshot is lost or
        //    corrupted, WAL replay rebuilds the freelist from scratch — so a
        //    failure here is non-fatal.
        //
        //    Ordering rationale: the snapshot must be written before the
        //    superblock so that a crash between the two leaves the superblock
        //    with the OLD checkpoint_lsn. Recovery then replays from the old
        //    LSN and sees all WAL records, correctly rebuilding the freelist.
        //    If the superblock were written first, a crash before the snapshot
        //    would leave recovery with the new LSN but a stale/missing
        //    snapshot, losing pre-checkpoint frees.
        if let Err(e) = freelist_snap.write(&FreelistMeta::path(&self.data_dir)) {
            warn!(error = %e, "failed to write freelist snapshot; recovery will rebuild from WAL");
        }

        // 9. Update the superblock. The redo LSN is the CheckpointBegin LSN.
        //    next_oid rides along in the v2 superblock; since Stage N the v2
        //    CheckpointEnd WAL record carries the same value (read once at
        //    step 5), so WAL and superblock never disagree across a crash.
        {
            let mut sb = self.superblock.lock();
            sb.checkpoint_lsn = begin_lsn;
            sb.next_page_id = next_page_id;
            sb.next_txn_id = next_txn_id;
            sb.next_oid = next_oid;
            let sb_path = Superblock::path(&self.data_dir);
            sb.write(&sb_path)?;
        }
        info!(%begin_lsn, %flushed, "checkpoint completed");

        // 10. Recycle WAL segments that are no longer needed for recovery.
        self.wal_writer.recycle_before(begin_lsn)?;

        // 11. Prune old ATT/DPT snapshot files, keeping the most recent
        //     checkpoints' files (tech-selection §11.4 P2-7). Synchronous,
        //     at checkpoint completion — no background thread. Non-fatal:
        //     a prune failure leaves extra files behind, never missing ones.
        self.prune_snapshot_files();

        Ok(begin_lsn)
    }

    /// Serialize a checkpoint snapshot (ATT `Vec<TxnId>` or DPT
    /// `Vec<(PageId, Lsn)>`) as bincode and atomically write it to
    /// `{data_dir}/{rel_path}` (Stage N, §11.4).
    ///
    /// [`crate::io::write_atomic`] fsyncs the temp file, renames it over the
    /// target, and fsyncs the parent directory, so once this returns the file
    /// named by the v2 `CheckpointEnd` record is durable — the first leg of
    /// the three-step hard order.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::CheckpointFailed`] on any I/O or serialization
    /// failure so the caller aborts the checkpoint rather than emitting a
    /// `CheckpointEnd` that references a missing snapshot file.
    fn write_snapshot_file<T: serde::Serialize>(&self, rel_path: &str, snapshot: &T) -> Result<()> {
        let body = bincode::serde::encode_to_vec(snapshot, crate::wal::record::bincode_config())
            .map_err(|e| StorageError::Serialize(e.to_string()))?;
        // Prepend a CRC32 over the body (same format as FreelistMeta) so that
        // bit-rot in a snapshot file is detected rather than silently producing
        // a "valid but wrong" ATT/DPT baseline (Stage N review, P1).
        let crc = crc32fast::hash(&body);
        let mut bytes = Vec::with_capacity(4 + body.len());
        bytes.extend_from_slice(&crc.to_le_bytes());
        bytes.extend_from_slice(&body);
        crate::io::write_atomic(&self.data_dir.join(rel_path), &bytes).map_err(|e| {
            StorageError::CheckpointFailed(format!("failed to write snapshot file {rel_path}: {e}"))
        })
    }

    /// Delete `meta/att-*.snapshot` / `meta/dpt-*.snapshot` files belonging to
    /// checkpoints older than the [`RETAINED_SNAPSHOT_CHECKPOINTS`] most
    /// recent ones (tech-selection §11.4 P2-7).
    ///
    /// Runs synchronously at checkpoint completion — no background thread.
    /// All failures are logged and swallowed: leaving extra files behind is
    /// harmless, while failing a completed checkpoint over cleanup would be
    /// wrong.
    fn prune_snapshot_files(&self) {
        let meta_dir = self.data_dir.join("meta");
        let entries = match std::fs::read_dir(&meta_dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return,
            Err(e) => {
                warn!(error = %e, "failed to list snapshot directory; skipping prune");
                return;
            }
        };

        // Parse the embedded checkpoint LSN out of each file name so stray
        // files (leftover write_atomic temp files, foreign names) are ignored
        // rather than accidentally kept or deleted.
        let mut snapshots: Vec<(u64, std::path::PathBuf)> = Vec::new();
        for entry in entries {
            let Ok(entry) = entry else { continue };
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            let Some(lsn) = Self::parse_snapshot_file_lsn(name) else {
                continue;
            };
            snapshots.push((lsn, entry.path()));
        }

        // Unconditionally retain the superblock's current checkpoint group:
        // if ≥3 consecutive checkpoints abort after writing their snapshot
        // files but before updating the superblock, the orphan groups sort
        // newer than the valid one and push it out of the retention window
        // (Stage N review, P1 — orphan snapshots are dead weight, not
        // fallback targets). The current group is the ONLY one that
        // corresponds to a completed checkpoint; losing it removes the only
        // valid recovery baseline.
        let current_checkpoint_lsn = self.superblock.lock().checkpoint_lsn.0;

        // Keep the newest RETAINED_SNAPSHOT_CHECKPOINTS distinct checkpoint
        // LSNs (each has an att- and a dpt- file), PLUS the superblock's
        // current checkpoint LSN; delete everything older.
        let mut lsns: Vec<u64> = snapshots.iter().map(|(lsn, _)| *lsn).collect();
        lsns.sort_unstable_by(|a, b| b.cmp(a));
        lsns.dedup();
        let mut keep: std::collections::HashSet<u64> = lsns
            .into_iter()
            .take(RETAINED_SNAPSHOT_CHECKPOINTS)
            .collect();
        keep.insert(current_checkpoint_lsn);

        let mut removed = 0usize;
        for (lsn, path) in snapshots {
            if keep.contains(&lsn) {
                continue;
            }
            match std::fs::remove_file(&path) {
                Ok(()) => {
                    removed += 1;
                    debug!(path = %path.display(), "pruned old checkpoint snapshot file");
                }
                Err(e) => {
                    warn!(path = %path.display(), error = %e, "failed to prune old snapshot file");
                }
            }
        }
        if removed > 0 {
            if let Err(e) = crate::io::sync_dir(&meta_dir) {
                warn!(error = %e, "failed to fsync snapshot directory after prune");
            }
        }
    }

    /// Parse the embedded checkpoint LSN out of an
    /// `att-{lsn}.snapshot` / `dpt-{lsn}.snapshot` file name.
    ///
    /// The writer zero-pads to 16 digits (`{lsn:016}`) so names sort
    /// lexicographically in LSN order; the PARSER accepts any non-empty
    /// digit run (Stage N review, P3-2): once LSNs reach 10^16 the padded
    /// form exceeds 16 digits, and an exact-width check would reject every
    /// file and silently stop pruning forever. A digit run too large for
    /// `u64` fails the final `parse` and is ignored — safe, since real LSNs
    /// are `u64`.
    fn parse_snapshot_file_lsn(name: &str) -> Option<u64> {
        let rest = name
            .strip_prefix("att-")
            .or_else(|| name.strip_prefix("dpt-"))?;
        let digits = rest.strip_suffix(".snapshot")?;
        if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        digits.parse().ok()
    }

    /// Stop the background checkpoint thread if it is running.
    ///
    /// This is called automatically on drop, but may be called explicitly to
    /// wait for the thread to finish (e.g., before tests assert on file state).
    pub fn shutdown(&self) {
        if self.shutdown.swap(true, Ordering::Relaxed) {
            return; // already shutting down
        }

        let mut handle = self.background_handle.lock();
        if let Some(h) = handle.take() {
            if let Err(e) = h.join() {
                warn!(error = ?e, "background checkpoint thread panicked");
            }
        }
    }

    /// Clone the state needed by the background checkpoint thread.
    fn clone_for_background_thread(&self) -> Self {
        Self {
            data_dir: self.data_dir.clone(),
            config: self.config.clone(),
            superblock: Arc::clone(&self.superblock),
            buffer_pool: Arc::clone(&self.buffer_pool),
            page_allocator: Arc::clone(&self.page_allocator),
            wal_writer: Arc::clone(&self.wal_writer),
            // Share the checkpoint lock with the foreground coordinator so that
            // manual and background checkpoints are mutually exclusive.
            checkpoint_lock: Arc::clone(&self.checkpoint_lock),
            // Share the next_oid source slot so a source installed after the
            // background thread starts (e.g. by Catalog::open) is still seen
            // by background checkpoints.
            next_oid_source: Arc::clone(&self.next_oid_source),
            // Share the next_txn_id source slot for the same reason as
            // next_oid_source: a source installed after the background thread
            // starts (by the TxnManager) must still be seen.
            next_txn_id_source: Arc::clone(&self.next_txn_id_source),
            // Share the CLOG flush hook slot for the same reason: a hook
            // installed after the background thread starts must still be
            // invoked by background checkpoints.
            clog_flush: Arc::clone(&self.clog_flush),
            // Share the ATT provider slot for the same reason as clog_flush.
            att_provider: Arc::clone(&self.att_provider),
            // Share the commit-barrier slot for the same reason: a barrier
            // installed after the background thread starts must still guard
            // background checkpoints.
            commit_barrier: Arc::clone(&self.commit_barrier),
            shutdown: Arc::clone(&self.shutdown),
            background_handle: Mutex::new(None),
        }
    }
}

impl Drop for CheckpointCoordinator {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer_pool::BufferPool;
    use crate::config::StorageConfig;
    use crate::page::PAGE_HEADER_SIZE;
    use crate::page_allocator::PageAllocator;
    use crate::superblock::Superblock;
    use crate::types::Oid;
    use crate::wal::writer::WalWriter;
    use std::path::PathBuf;
    use tempfile::TempDir;

    type TestComponents = (
        PathBuf,
        StorageConfig,
        Arc<Mutex<Superblock>>,
        Arc<BufferPool>,
        Arc<Mutex<PageAllocator>>,
        Arc<WalWriter>,
    );

    fn setup(tmp: &TempDir) -> TestComponents {
        let data_dir = tmp.path().to_path_buf();
        let config = {
            let mut cfg = StorageConfig::new(&data_dir);
            cfg.buffer_pool_size = 8 * 1024 * 1024; // 8 MB => 1024 frames
            cfg
        };
        config.validate().unwrap();

        Superblock::create(&Superblock::path(&data_dir), config.page_size() as u32).unwrap();
        let superblock = Arc::new(Mutex::new(
            Superblock::read(&Superblock::path(&data_dir)).unwrap(),
        ));

        let wal_writer = Arc::new(WalWriter::open(&data_dir, &config).unwrap());
        let page_allocator = Arc::new(Mutex::new(
            PageAllocator::open(&data_dir, &config, Arc::clone(&wal_writer)).unwrap(),
        ));
        let buffer_pool = Arc::new(
            BufferPool::open(
                &data_dir,
                &config,
                Arc::clone(&page_allocator),
                Arc::clone(&wal_writer),
            )
            .unwrap(),
        );

        (
            data_dir,
            config,
            superblock,
            buffer_pool,
            page_allocator,
            wal_writer,
        )
    }

    #[test]
    fn trigger_checkpoint_writes_begin_and_end() {
        let tmp = TempDir::new().unwrap();
        let (data_dir, config, superblock, buffer_pool, page_allocator, wal_writer) = setup(&tmp);

        // Allocate and modify a page so there is dirty work to checkpoint.
        {
            let mut guard = buffer_pool.new_page().unwrap();
            guard.page_mut()[PAGE_HEADER_SIZE] = 42;
        }

        let coordinator = CheckpointCoordinator::new(
            &data_dir,
            &config,
            superblock,
            buffer_pool,
            page_allocator,
            wal_writer.clone(),
        );

        let begin_lsn = coordinator.trigger_checkpoint().unwrap();
        assert!(begin_lsn.is_valid());

        // WAL should contain CheckpointBegin, FullPageImage (from new_page -> pin_mut),
        // and CheckpointEnd.
        let mut reader = crate::wal::reader::WalReader::open_at(
            data_dir.join("wal"),
            config.wal_segment_size,
            begin_lsn,
        )
        .unwrap();
        let mut saw_begin = false;
        let mut saw_end = false;
        while let Some(rec) = reader.next_record().unwrap() {
            match rec.record_type {
                crate::wal::record::WalRecordType::CheckpointBegin => saw_begin = true,
                crate::wal::record::WalRecordType::CheckpointEnd => {
                    // Stage N: emitted records are v2 (version nibble = 1).
                    assert_eq!(rec.flags, crate::wal::record::CHECKPOINT_END_V2_FLAGS);
                    let decoded =
                        crate::wal::record::CheckpointEndRecord::decode(&rec.payload, rec.flags)
                            .unwrap();
                    assert_eq!(decoded.checkpoint_lsn, begin_lsn);
                    saw_end = true;
                }
                _ => {}
            }
        }
        assert!(saw_begin);
        assert!(saw_end);
    }

    #[test]
    fn checkpoint_updates_superblock() {
        let tmp = TempDir::new().unwrap();
        let (data_dir, config, superblock, buffer_pool, page_allocator, wal_writer) = setup(&tmp);

        // Allocate a few pages.
        for _ in 0..5 {
            drop(buffer_pool.new_page().unwrap());
        }

        let coordinator = CheckpointCoordinator::new(
            &data_dir,
            &config,
            superblock,
            buffer_pool,
            page_allocator,
            wal_writer,
        );

        let begin_lsn = coordinator.trigger_checkpoint().unwrap();

        let sb = Superblock::read(&Superblock::path(&data_dir)).unwrap();
        assert_eq!(sb.checkpoint_lsn, begin_lsn);
        assert!(sb.next_page_id.0 > 1);
        // Stage H: the OID source is seeded from the superblock at creation,
        // so on a fresh data directory checkpoints persist FIRST_USER — not
        // as a placeholder fallback, but because that is the persisted value.
        assert_eq!(sb.next_oid, crate::types::Oid::FIRST_USER);
    }

    #[test]
    fn checkpoint_persists_next_oid_from_source() {
        let tmp = TempDir::new().unwrap();
        let (data_dir, config, superblock, buffer_pool, page_allocator, wal_writer) = setup(&tmp);

        let coordinator = CheckpointCoordinator::new(
            &data_dir,
            &config,
            superblock,
            buffer_pool,
            page_allocator,
            wal_writer,
        );

        // Stage H wiring: the catalog installs its OID allocator's counter as
        // the next_oid source; checkpoints persist its current value into the
        // v2 superblock.
        let source = OidCounter::new(Oid(20_000));
        coordinator.set_next_oid_source(source.clone());

        coordinator.trigger_checkpoint().unwrap();
        let sb = Superblock::read(&Superblock::path(&data_dir)).unwrap();
        assert_eq!(sb.next_oid, crate::types::Oid(20_000));

        // The source is read on every checkpoint, so allocations that advance
        // the counter are picked up by the next checkpoint.
        assert_eq!(source.alloc(), Oid(20_000));
        coordinator.trigger_checkpoint().unwrap();
        let sb = Superblock::read(&Superblock::path(&data_dir)).unwrap();
        assert_eq!(sb.next_oid, crate::types::Oid(20_001));
    }

    /// Regression: a next_oid source installed *after* the background thread's
    /// clone is created must still be observed by that clone. This guards the
    /// shared-`Arc` slot — the catalog wires its allocator in `Catalog::open`,
    /// which may run after `start_background_checkpointing`.
    #[test]
    fn background_clone_observes_source_installed_after_clone() {
        let tmp = TempDir::new().unwrap();
        let (data_dir, config, superblock, buffer_pool, page_allocator, wal_writer) = setup(&tmp);

        let coordinator = CheckpointCoordinator::new(
            &data_dir,
            &config,
            superblock,
            buffer_pool,
            page_allocator,
            wal_writer,
        );

        // Clone for the background thread FIRST (source still the default
        // seeded from the superblock), then wire the source on the foreground
        // coordinator — the order a real startup hits when open() spawns the
        // checkpointer before Catalog::open.
        let background = coordinator.clone_for_background_thread();
        coordinator.set_next_oid_source(OidCounter::new(Oid(30_000)));

        // The background clone shares the slot, so its checkpoint persists the
        // late-installed source rather than the default it was cloned with.
        background.trigger_checkpoint().unwrap();
        let sb = Superblock::read(&Superblock::path(&data_dir)).unwrap();
        assert_eq!(sb.next_oid, crate::types::Oid(30_000));
    }

    #[test]
    fn background_checkpoint_runs_and_stops() {
        let tmp = TempDir::new().unwrap();
        let (data_dir, mut config, superblock, buffer_pool, page_allocator, wal_writer) =
            setup(&tmp);
        // Use a short interval so the test does not take long.
        config.checkpoint_interval_ms = 50;

        // Allocate a few dirty pages so the background checkpoint has work to
        // flush and advances the superblock state.
        for _ in 0..3 {
            let mut guard = buffer_pool.new_page().unwrap();
            guard.page_mut()[PAGE_HEADER_SIZE] = 0xCD;
        }

        let initial_lsn = superblock.lock().checkpoint_lsn;

        let coordinator = CheckpointCoordinator::new(
            &data_dir,
            &config,
            superblock,
            buffer_pool,
            page_allocator,
            wal_writer,
        );

        coordinator.start_background_checkpointing().unwrap();

        // Wait long enough for at least one background checkpoint to fire.
        thread::sleep(Duration::from_millis(300));

        coordinator.shutdown();

        let sb = Superblock::read(&Superblock::path(&data_dir)).unwrap();
        assert!(sb.checkpoint_lsn.is_valid());
        assert!(
            sb.checkpoint_lsn > initial_lsn,
            "background checkpoint did not advance checkpoint_lsn"
        );
        assert!(sb.next_page_id.0 > 1);
    }

    #[test]
    fn checkpoint_lsn_controls_fpi_in_next_cycle() {
        let tmp = TempDir::new().unwrap();
        let (data_dir, config, superblock, buffer_pool, page_allocator, wal_writer) = setup(&tmp);

        // Allocate and modify page 1.
        let page_id = {
            let mut guard = buffer_pool.new_page().unwrap();
            guard.page_mut()[PAGE_HEADER_SIZE] = 1;
            guard.page_id()
        };
        wal_writer.flush().unwrap();
        buffer_pool.flush(page_id).unwrap();

        let coordinator = CheckpointCoordinator::new(
            &data_dir,
            &config,
            superblock,
            buffer_pool.clone(),
            page_allocator,
            wal_writer,
        );

        let first_lsn = coordinator.trigger_checkpoint().unwrap();

        // Evict the page and reload it. The next pin_mut should write an FPI
        // because the page was last modified before the checkpoint.
        let frame_count = buffer_pool.frame_count();
        for _ in 0..frame_count + 10 {
            drop(buffer_pool.new_page().unwrap());
        }

        let mut saw_fpi = false;
        {
            let mut guard = buffer_pool.pin_mut(page_id).unwrap();
            guard.page_mut()[PAGE_HEADER_SIZE] = 2;

            // Read the WAL from the first checkpoint begin to find the FPI.
            let mut reader = crate::wal::reader::WalReader::open_at(
                data_dir.join("wal"),
                config.wal_segment_size,
                first_lsn,
            )
            .unwrap();
            while let Some(rec) = reader.next_record().unwrap() {
                if rec.record_type == crate::wal::record::WalRecordType::FullPageImage {
                    let decoded: crate::wal::record::FullPageImageRecord =
                        bincode::serde::decode_from_slice(
                            &rec.payload,
                            crate::wal::record::bincode_config(),
                        )
                        .unwrap()
                        .0;
                    if decoded.page_id == page_id {
                        saw_fpi = true;
                    }
                }
            }
        }
        assert!(
            saw_fpi,
            "expected FPI after checkpoint for page modified before checkpoint"
        );
    }

    #[test]
    fn checkpoint_fpi_race_window_is_eliminated() {
        let tmp = TempDir::new().unwrap();
        let (data_dir, config, superblock, buffer_pool, page_allocator, wal_writer) = setup(&tmp);

        // Allocate and dirty a page so the next checkpoint has work to do.
        let page_id = {
            let mut guard = buffer_pool.new_page().unwrap();
            guard.page_mut()[PAGE_HEADER_SIZE] = 0xAA;
            guard.page_id()
        };

        let coordinator = CheckpointCoordinator::new(
            &data_dir,
            &config,
            superblock,
            buffer_pool.clone(),
            page_allocator,
            wal_writer.clone(),
        );

        // Trigger checkpoint. With the Stage B fix, set_checkpoint_lsn is
        // called BEFORE the CheckpointBegin record is emitted, so any pin_mut
        // that starts after trigger_checkpoint returns will see the new LSN.
        let begin_lsn = coordinator.trigger_checkpoint().unwrap();

        // Invariant 1: buffer_pool's checkpoint_lsn must equal the reserved LSN.
        // We can't read it directly, but we can verify via behavior: a pin_mut
        // on a page modified before the checkpoint must write an FPI.
        //
        // Invariant 2: the CheckpointBegin record must exist at exactly
        // begin_lsn in the WAL.
        let mut reader = crate::wal::reader::WalReader::open_at(
            data_dir.join("wal"),
            config.wal_segment_size,
            begin_lsn,
        )
        .unwrap();
        let first_rec = reader
            .next_record()
            .unwrap()
            .expect("WAL must contain CheckpointBegin");
        assert_eq!(
            first_rec.record_type,
            crate::wal::record::WalRecordType::CheckpointBegin
        );
        assert_eq!(first_rec.lsn, begin_lsn);

        // Invariant 3: a subsequent pin_mut on a pre-checkpoint page must
        // produce an FPI because checkpoint_lsn was already visible.
        buffer_pool.flush(page_id).unwrap(); // ensure page is clean first
        let frame_count = buffer_pool.frame_count();
        for _ in 0..frame_count + 10 {
            drop(buffer_pool.new_page().unwrap());
        }

        let mut saw_fpi = false;
        {
            let mut guard = buffer_pool.pin_mut(page_id).unwrap();
            guard.page_mut()[PAGE_HEADER_SIZE] = 0xBB;
            let mut reader2 = crate::wal::reader::WalReader::open_at(
                data_dir.join("wal"),
                config.wal_segment_size,
                begin_lsn,
            )
            .unwrap();
            while let Some(rec) = reader2.next_record().unwrap() {
                if rec.record_type == crate::wal::record::WalRecordType::FullPageImage {
                    let decoded: crate::wal::record::FullPageImageRecord =
                        bincode::serde::decode_from_slice(
                            &rec.payload,
                            crate::wal::record::bincode_config(),
                        )
                        .unwrap()
                        .0;
                    if decoded.page_id == page_id {
                        saw_fpi = true;
                    }
                }
            }
        }
        assert!(
            saw_fpi,
            "pin_mut after checkpoint must write FPI (checkpoint_lsn was visible)"
        );
    }

    #[test]
    fn checkpoint_succeeds_with_concurrent_wal_appends() {
        let tmp = TempDir::new().unwrap();
        let (data_dir, config, superblock, buffer_pool, page_allocator, wal_writer) = setup(&tmp);

        // Dirty a page so the checkpoint has flush work to do.
        {
            let mut guard = buffer_pool.new_page().unwrap();
            guard.page_mut()[PAGE_HEADER_SIZE] = 0x5A;
        }

        let coordinator = CheckpointCoordinator::new(
            &data_dir,
            &config,
            superblock,
            buffer_pool,
            page_allocator,
            wal_writer.clone(),
        );

        // Continuously append WAL records while checkpoints run. An append
        // landing between reserve_lsn(CheckpointBegin) and append_at advances
        // the clock past the reserved range; the relaxed append_at check must
        // tolerate that (it used to fail with "not at the reservation
        // boundary", failing the whole checkpoint).
        let stop = Arc::new(AtomicBool::new(false));
        let stop_flag = Arc::clone(&stop);
        let wal = Arc::clone(&wal_writer);
        let appender = thread::spawn(move || {
            let mut count = 0u64;
            while !stop_flag.load(Ordering::Relaxed) {
                wal.append(WalRecord::page_alloc(crate::types::PageId(1_000_000 + count)).unwrap())
                    .unwrap();
                count += 1;
            }
            count
        });

        // Run many checkpoints so appends land inside the reserve → emit
        // window with high probability.
        for _ in 0..20 {
            coordinator.trigger_checkpoint().unwrap();
        }

        stop.store(true, Ordering::Relaxed);
        let appended = appender.join().unwrap();
        assert!(appended > 0, "appender thread made no progress");
    }

    /// M2b Stage L: an installed `ClogFlush` hook is invoked once per
    /// checkpoint (between CheckpointBegin and CheckpointEnd), and a hook
    /// failure aborts the checkpoint like any other flush failure.
    #[test]
    fn checkpoint_invokes_clog_flush_hook() {
        use std::sync::atomic::AtomicUsize;

        #[derive(Debug)]
        struct MockFlush {
            calls: AtomicUsize,
            fail: AtomicBool,
        }
        impl crate::clog::ClogFlush for MockFlush {
            fn flush_dirty(&self) -> Result<()> {
                self.calls.fetch_add(1, Ordering::Relaxed);
                if self.fail.load(Ordering::Relaxed) {
                    return Err(StorageError::CheckpointFailed("boom".to_string()));
                }
                Ok(())
            }
        }

        let tmp = TempDir::new().unwrap();
        let (data_dir, config, superblock, buffer_pool, page_allocator, wal_writer) = setup(&tmp);

        let coordinator = CheckpointCoordinator::new(
            &data_dir,
            &config,
            superblock,
            buffer_pool,
            page_allocator,
            wal_writer,
        );

        let hook = Arc::new(MockFlush {
            calls: AtomicUsize::new(0),
            fail: AtomicBool::new(false),
        });
        coordinator.set_clog_flush(hook.clone());

        coordinator.trigger_checkpoint().unwrap();
        assert_eq!(hook.calls.load(Ordering::Relaxed), 1);

        // A failing hook aborts the checkpoint (same discipline as a failed
        // page flush) instead of silently advancing the superblock.
        hook.fail.store(true, Ordering::Relaxed);
        assert!(coordinator.trigger_checkpoint().is_err());
        assert_eq!(hook.calls.load(Ordering::Relaxed), 2);
    }

    /// Read back a bincode snapshot file written by a checkpoint (Stage N).
    /// The file format is `crc32(4) + body` — skip the CRC prefix.
    fn read_snapshot<T: serde::de::DeserializeOwned>(
        data_dir: &std::path::Path,
        rel_path: &str,
    ) -> T {
        let bytes = std::fs::read(data_dir.join(rel_path)).unwrap();
        bincode::serde::decode_from_slice(&bytes[4..], crate::wal::record::bincode_config())
            .unwrap()
            .0
    }

    fn att_snapshot_path(lsn: Lsn) -> String {
        format!("meta/att-{:016}.snapshot", lsn.0)
    }

    fn dpt_snapshot_path(lsn: Lsn) -> String {
        format!("meta/dpt-{:016}.snapshot", lsn.0)
    }

    #[derive(Debug)]
    struct MockAttProvider {
        xids: Mutex<Vec<crate::types::TxnId>>,
    }

    impl crate::recovery::AttProvider for MockAttProvider {
        fn active_xids(&self) -> Vec<crate::types::TxnId> {
            self.xids.lock().clone()
        }
    }

    /// Find and decode the first `CheckpointEnd` record at or after `from`.
    fn scan_checkpoint_end(
        data_dir: &std::path::Path,
        config: &StorageConfig,
        from: Lsn,
    ) -> (Lsn, u8, crate::wal::record::CheckpointEndRecord) {
        let mut reader = crate::wal::reader::WalReader::open_at(
            data_dir.join("wal"),
            config.wal_segment_size,
            from,
        )
        .unwrap();
        while let Some(rec) = reader.next_record().unwrap() {
            if rec.record_type == crate::wal::record::WalRecordType::CheckpointEnd {
                let decoded =
                    crate::wal::record::CheckpointEndRecord::decode(&rec.payload, rec.flags)
                        .unwrap();
                return (rec.lsn, rec.flags, decoded);
            }
        }
        panic!("no CheckpointEnd record found at or after {from:?}");
    }

    /// Stage N (§11.4): a checkpoint captures the ATT from the installed
    /// provider and the DPT from the buffer pool, persists both as fsynced
    /// snapshot files, and emits a v2 `CheckpointEnd` that names them —
    /// in the hard order snapshot-fsync → append → flush_to.
    #[test]
    fn checkpoint_captures_att_and_dpt_snapshots() {
        use crate::types::{PageId, TxnId};

        let tmp = TempDir::new().unwrap();
        let (data_dir, config, superblock, buffer_pool, page_allocator, wal_writer) = setup(&tmp);

        // A page that survives checkpoint #1 and is re-dirtied afterwards
        // through the FPI path, giving it a known rec_lsn anchor.
        let page_id = {
            let mut guard = buffer_pool.new_page().unwrap();
            guard.page_mut()[PAGE_HEADER_SIZE] = 42;
            guard.page_id()
        };

        let coordinator = CheckpointCoordinator::new(
            &data_dir,
            &config,
            superblock,
            buffer_pool.clone(),
            page_allocator,
            wal_writer,
        );
        let provider = Arc::new(MockAttProvider {
            xids: Mutex::new(vec![TxnId(5), TxnId(9)]),
        });
        coordinator.set_att_provider(provider.clone());

        // Checkpoint #1: captures the ATT; the DPT is empty because the raw
        // page write above never stamped a WAL LSN (first_dirty_lsn INVALID
        // is filtered out of the snapshot).
        let begin1 = coordinator.trigger_checkpoint().unwrap();
        let att1: Vec<TxnId> = read_snapshot(&data_dir, &att_snapshot_path(begin1));
        assert_eq!(att1, vec![TxnId(5), TxnId(9)]);
        let dpt1: Vec<(PageId, Lsn)> = read_snapshot(&data_dir, &dpt_snapshot_path(begin1));
        assert!(dpt1.is_empty());

        // Re-dirty the page via pin_mut: the page is older than checkpoint
        // #1, so the FPI path fires and the FPI LSN becomes the rec_lsn.
        {
            let mut guard = buffer_pool.pin_mut(page_id).unwrap();
            guard.page_mut()[PAGE_HEADER_SIZE] = 43;
        }
        let expected_dpt = buffer_pool.dirty_page_snapshot();
        assert_eq!(expected_dpt.len(), 1);
        assert_eq!(expected_dpt[0].0, page_id);
        assert!(expected_dpt[0].1.is_valid());

        // One of the active transactions committed meanwhile.
        *provider.xids.lock() = vec![TxnId(9)];

        // Checkpoint #2: the DPT snapshot must hold the page as of begin2
        // (captured before the flush phase drains the dirty set).
        let begin2 = coordinator.trigger_checkpoint().unwrap();
        let att2: Vec<TxnId> = read_snapshot(&data_dir, &att_snapshot_path(begin2));
        assert_eq!(att2, vec![TxnId(9)]);
        let dpt2: Vec<(PageId, Lsn)> = read_snapshot(&data_dir, &dpt_snapshot_path(begin2));
        assert_eq!(dpt2, expected_dpt);

        // The v2 CheckpointEnd names both snapshot files; the files exist
        // (they were fsynced before the record was appended — step 1 of the
        // hard order) and the record's own LSN is past the begin LSN embedded
        // in their names (step 2/3 ordering).
        let (end_lsn, flags, end) = scan_checkpoint_end(&data_dir, &config, begin2);
        assert_eq!(flags, crate::wal::record::CHECKPOINT_END_V2_FLAGS);
        assert_eq!(end.checkpoint_lsn, begin2);
        assert_eq!(end.att_file, att_snapshot_path(begin2));
        assert_eq!(end.dpt_file, dpt_snapshot_path(begin2));
        assert!(end_lsn > begin2);
        assert!(data_dir.join(&end.att_file).exists());
        assert!(data_dir.join(&end.dpt_file).exists());
        // next_oid rides in the v2 payload: seeded from the superblock here
        // (no catalog wired in this test), so FIRST_USER.
        assert_eq!(end.next_oid, crate::types::Oid::FIRST_USER.0);
    }

    /// With no ATT provider installed (M1/M2a configuration) the checkpoint
    /// still writes a v2 record, but the ATT snapshot is empty — which
    /// analysis reads as "rebuild by a full WAL scan from the checkpoint
    /// LSN", the same semantics as a v1 record's empty `att_file`.
    #[test]
    fn checkpoint_without_att_provider_writes_empty_att_snapshot() {
        use crate::types::{PageId, TxnId};

        let tmp = TempDir::new().unwrap();
        let (data_dir, config, superblock, buffer_pool, page_allocator, wal_writer) = setup(&tmp);

        let coordinator = CheckpointCoordinator::new(
            &data_dir,
            &config,
            superblock,
            buffer_pool,
            page_allocator,
            wal_writer,
        );

        let begin = coordinator.trigger_checkpoint().unwrap();

        let att: Vec<TxnId> = read_snapshot(&data_dir, &att_snapshot_path(begin));
        assert!(att.is_empty());
        let dpt: Vec<(PageId, Lsn)> = read_snapshot(&data_dir, &dpt_snapshot_path(begin));
        assert!(dpt.is_empty());

        let (_end_lsn, flags, end) = scan_checkpoint_end(&data_dir, &config, begin);
        assert_eq!(flags, crate::wal::record::CHECKPOINT_END_V2_FLAGS);
        assert_eq!(end.att_file, att_snapshot_path(begin));
        assert_eq!(end.dpt_file, dpt_snapshot_path(begin));
        assert!(data_dir.join(&end.att_file).exists());
        assert!(data_dir.join(&end.dpt_file).exists());
    }

    #[test]
    fn parse_snapshot_file_lsn_accepts_any_digit_run() {
        assert_eq!(
            CheckpointCoordinator::parse_snapshot_file_lsn("att-0000000000000128.snapshot"),
            Some(128)
        );
        assert_eq!(
            CheckpointCoordinator::parse_snapshot_file_lsn("dpt-0000000000012345.snapshot"),
            Some(12345)
        );
        // Non-padded and beyond-16-digit forms (LSN >= 10^16) parse too:
        // the writer's zero-padding is for sort order, not a parse contract
        // (P3-2 — an exact-width check would stop pruning forever once
        // LSNs outgrow 16 digits).
        assert_eq!(
            CheckpointCoordinator::parse_snapshot_file_lsn("att-128.snapshot"),
            Some(128)
        );
        assert_eq!(
            CheckpointCoordinator::parse_snapshot_file_lsn(
                "att-10000000000000000.snapshot" // 10^16, 17 digits
            ),
            Some(10_000_000_000_000_000)
        );
        // Empty / non-numeric infixes, unknown prefixes, leftover
        // write_atomic temp files, and digit runs beyond u64 are ignored.
        assert_eq!(
            CheckpointCoordinator::parse_snapshot_file_lsn("att-.snapshot"),
            None
        );
        assert_eq!(
            CheckpointCoordinator::parse_snapshot_file_lsn(
                "att-0000000000000128.snapshot.pg_rust_tmp"
            ),
            None
        );
        assert_eq!(
            CheckpointCoordinator::parse_snapshot_file_lsn("freelist-0000000000000128.snapshot"),
            None
        );
        assert_eq!(
            CheckpointCoordinator::parse_snapshot_file_lsn("att-0000000000000abc.snapshot"),
            None
        );
        assert_eq!(
            CheckpointCoordinator::parse_snapshot_file_lsn(
                "att-99999999999999999999999999.snapshot"
            ),
            None
        );
    }

    /// §11.4 P2-7: checkpoint completion prunes `meta/` down to the
    /// ATT/DPT snapshots of the most recent three checkpoints.
    #[test]
    fn prune_snapshot_files_keeps_latest_three_checkpoints() {
        let tmp = TempDir::new().unwrap();
        let (data_dir, config, superblock, buffer_pool, page_allocator, wal_writer) = setup(&tmp);

        let coordinator = CheckpointCoordinator::new(
            &data_dir,
            &config,
            superblock,
            buffer_pool.clone(),
            page_allocator,
            wal_writer,
        );

        let meta_dir = data_dir.join("meta");
        let snapshot_names = |begin: Lsn| -> [String; 2] {
            [
                format!("att-{:016}.snapshot", begin.0),
                format!("dpt-{:016}.snapshot", begin.0),
            ]
        };
        let list_meta = || -> Vec<String> {
            let mut names: Vec<String> = std::fs::read_dir(&meta_dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .filter_map(|e| e.file_name().into_string().ok())
                // Snapshot files and crash-leftover temp files; skips
                // freelist.meta, which shares the directory.
                .filter(|name| name.contains(".snapshot"))
                .collect();
            names.sort_unstable();
            names
        };

        // Seed two fake old checkpoint snapshot groups, then run five real
        // checkpoints. After each of the last three, the retained set must
        // shrink to the latest three groups.
        std::fs::create_dir_all(&meta_dir).unwrap();
        for fake_lsn in [8u64, 16] {
            for name in snapshot_names(Lsn(fake_lsn)) {
                std::fs::write(meta_dir.join(name), b"fake").unwrap();
            }
        }

        let mut begins = Vec::new();
        for i in 0..5u8 {
            {
                let mut guard = buffer_pool.new_page().unwrap();
                guard.page_mut()[PAGE_HEADER_SIZE] = i;
            }
            begins.push(coordinator.trigger_checkpoint().unwrap());
        }

        let kept = list_meta();
        let mut expected: Vec<String> = begins[2..5]
            .iter()
            .flat_map(|begin| snapshot_names(*begin))
            .collect();
        expected.sort_unstable();
        assert_eq!(
            kept, expected,
            "meta/ must hold exactly the att/dpt snapshots of the latest 3 checkpoints"
        );

        // A crash leftover (write_atomic temp file) is neither pruned nor
        // counted as a snapshot group.
        std::fs::write(
            meta_dir.join("att-0000000000000064.snapshot.pg_rust_tmp"),
            b"partial",
        )
        .unwrap();
        coordinator.prune_snapshot_files();
        assert!(meta_dir
            .join("att-0000000000000064.snapshot.pg_rust_tmp")
            .exists());
        assert_eq!(list_meta().len(), expected.len() + 1);
    }

    /// `prune_snapshot_files` must be a no-op (not an error) when the
    /// `meta/` directory does not exist — e.g. a directory where `meta/`
    /// was lost or never created. The `read_dir → ErrorKind::NotFound`
    /// early-return path.
    #[test]
    fn prune_snapshot_files_handles_missing_meta_dir() {
        let tmp = TempDir::new().unwrap();
        let (data_dir, config, superblock, buffer_pool, page_allocator, wal_writer) = setup(&tmp);

        let coordinator = CheckpointCoordinator::new(
            &data_dir,
            &config,
            superblock,
            buffer_pool.clone(),
            page_allocator,
            wal_writer,
        );

        // `setup` calls `ensure_data_dir`, which creates `meta/`. Remove
        // it to simulate a directory that lost its `meta/` subdir.
        std::fs::remove_dir_all(data_dir.join("meta")).unwrap();
        assert!(!data_dir.join("meta").exists());

        // Prune must not panic or error.
        coordinator.prune_snapshot_files();

        // Still no meta/ — prune did not recreate it.
        assert!(!data_dir.join("meta").exists());
    }
}
