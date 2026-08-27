//! Top-level storage engine.
//!
//! `StorageEngine` owns and wires together all M1 storage components:
//! superblock, page allocator, WAL writer, buffer pool, and checkpoint
//! coordinator. It also provides the canonical crash-recovery entry point
//! [`StorageEngine::recover`].

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::sync::Mutex;
use tracing::{debug, info, warn};

use crate::analysis;
use crate::buffer_pool::BufferPool;
use crate::checkpoint::CheckpointCoordinator;
use crate::clog::{ClogAccessor, NoOpClogAccessor, TxnState};
use crate::config::StorageConfig;
use crate::data_dir_lock::DataDirLock;
use crate::error::{Result, StorageError};
use crate::freelist_meta::FreelistMeta;
use crate::io::ensure_data_dir;
use crate::page_allocator::PageAllocator;
use crate::positioned_file::PositionedFile;
use crate::recovery::{
    ActiveXactTable, DirtyPageTable, FullPageImageRedoHandler, IncompleteSplitTracker,
    NoOpRedoHandler, PageAllocRedoHandler, PageFreeRedoHandler, RedoContext, RedoHandler,
    RedoRegistry, UndoContext, UndoHandler,
};
use crate::superblock::Superblock;
use crate::txn_id::TxnIdClock;
use crate::types::{Lsn, PageId, TxnId};
use crate::wal::reader::WalReader;
use crate::wal::record::{CheckpointEndRecord, WalRecordType};
use crate::wal::writer::WalWriter;

/// Owning handle for a recovered or newly created storage engine.
#[derive(Debug)]
pub struct StorageEngine {
    data_dir: PathBuf,
    config: StorageConfig,
    superblock: Arc<Mutex<Superblock>>,
    page_allocator: Arc<Mutex<PageAllocator>>,
    wal_writer: Arc<WalWriter>,
    buffer_pool: Arc<BufferPool>,
    checkpoint: CheckpointCoordinator,
    /// XID clock seeded from `superblock.next_txn_id`, shared with the
    /// checkpoint coordinator (which persists its `current()`) and handed to
    /// the `pg-txn` `TxnManager` via [`Self::txn_id_clock`]. Advancing it past
    /// every persisted XID on recovery guarantees restarted transactions never
    /// reuse a committed XID.
    txn_id_clock: TxnIdClock,
    /// CLOG used by recovery redo handlers to rebuild transaction state. The
    /// engine holds it so the same instance the caller injected is visible for
    /// post-recovery queries. Defaults to `NoOpClogAccessor` when the caller
    /// opens without one (M1 / heap-only paths).
    clog: Arc<dyn ClogAccessor>,
    /// XIDs that were active (neither committed nor aborted) when the
    /// previous instance stopped, rebuilt by the analysis phase (M2b Stage N;
    /// tech-selection §11.1). Empty on a freshly created database.
    recovered_att: Vec<TxnId>,
    /// The WAL position this recovery's redo scan started from
    /// (`AnalysisResult::redo_start`): a guaranteed record boundary in a
    /// retained segment. [`Lsn::FIRST`] for a freshly created database.
    /// pg-engine's loser-transaction index compensation scans from here.
    recovered_redo_start: Lsn,
    /// Exclusive data-directory lock (M3 Stage E review F1): created at
    /// open, held until the engine value is dropped. Declared LAST so the
    /// lock is released only after every subsystem that writes through the
    /// directory (fields drop in declaration order).
    _data_dir_lock: DataDirLock,
}

impl StorageEngine {
    /// Open or create a storage engine at `data_dir`.
    ///
    /// If a superblock already exists, this calls [`Self::recover`]. Otherwise
    /// it initializes a fresh database and returns an engine ready for use.
    ///
    /// Background checkpointing is not started automatically; call
    /// [`Self::start_background_checkpointing`] to enable it.
    ///
    /// # Exclusive directory lock (M3 Stage E review F1)
    ///
    /// Opening takes an exclusive lock on the data directory (a
    /// `{data_dir}/lock` file, `create_new`) held until the engine value is
    /// dropped; a second PROCESS opening the same directory fails with
    /// [`StorageError::InvalidOperation`] ("already in use"). A stale lock
    /// left by a crashed process must be removed by hand (the error says
    /// so); a same-pid lock — the in-process `mem::forget` crash-test
    /// idiom — is reclaimed automatically. See the `data_dir_lock` module
    /// docs for the design and its limits.
    pub fn open(data_dir: impl AsRef<Path>, config: &StorageConfig) -> Result<Self> {
        Self::open_with_redo_handlers(data_dir, config, Vec::new(), Vec::new())
    }

    /// Open or create a storage engine, injecting extra redo handlers.
    ///
    /// `extra_redo_handlers` are registered into the recovery [`RedoRegistry`]
    /// alongside the built-in storage handlers before WAL replay. This is how
    /// upper layers (e.g. the heap AM) supply redo handlers for their own
    /// record types without `pg-storage` depending on them. On a fresh
    /// database the handlers are unused (nothing to replay).
    pub fn open_with_redo_handlers(
        data_dir: impl AsRef<Path>,
        config: &StorageConfig,
        extra_redo_handlers: Vec<Box<dyn RedoHandler>>,
        extra_undo_handlers: Vec<Box<dyn UndoHandler>>,
    ) -> Result<Self> {
        Self::open_with_redo_and_clog(
            data_dir,
            config,
            extra_redo_handlers,
            extra_undo_handlers,
            Arc::new(NoOpClogAccessor),
        )
    }

    /// Open or create a storage engine, injecting extra redo handlers and a
    /// CLOG (Stage J).
    ///
    /// The `clog` is used by transaction redo handlers during WAL replay to
    /// rebuild committed/aborted state, and is retained on the engine so the
    /// same instance backs post-recovery visibility checks. `pg-storage` never
    /// constructs a real CLOG itself (that lives in `pg-txn`); callers running
    /// transactions pass `pg_txn::ClogBuffer` (M2b disk-backed) or, in M2a
    /// configurations, `pg_txn::InMemoryClogAccessor` here. Callers with no
    /// transactions use [`Self::open_with_redo_handlers`], which supplies a
    /// [`NoOpClogAccessor`].
    pub fn open_with_redo_and_clog(
        data_dir: impl AsRef<Path>,
        config: &StorageConfig,
        extra_redo_handlers: Vec<Box<dyn RedoHandler>>,
        extra_undo_handlers: Vec<Box<dyn UndoHandler>>,
        clog: Arc<dyn ClogAccessor>,
    ) -> Result<Self> {
        config.validate()?;
        let data_dir = data_dir.as_ref().to_path_buf();
        ensure_data_dir(&data_dir)?;
        // F1: take the exclusive directory lock BEFORE any file is read or
        // written (superblock probe included), so a second process can
        // never interleave with this one's recovery/writes.
        let data_dir_lock = DataDirLock::acquire(&data_dir)?;

        let sb_path = Superblock::path(&data_dir);
        if sb_path.exists() {
            Self::recover_inner(
                data_dir,
                config,
                extra_redo_handlers,
                extra_undo_handlers,
                clog,
                data_dir_lock,
            )
        } else {
            Self::create_new(data_dir, config, clog, data_dir_lock)
        }
    }

    /// Create a brand-new database.
    fn create_new(
        data_dir: PathBuf,
        config: &StorageConfig,
        clog: Arc<dyn ClogAccessor>,
        data_dir_lock: DataDirLock,
    ) -> Result<Self> {
        info!(data_dir = %data_dir.display(), "creating new storage engine");

        let sb_path = Superblock::path(&data_dir);
        let superblock = Superblock::create(&sb_path, config.page_size() as u32)?;
        let next_txn_id = superblock.next_txn_id;
        let superblock = Arc::new(Mutex::new(superblock));

        let wal_writer = Arc::new(WalWriter::open(&data_dir, config)?);
        let page_allocator = Arc::new(Mutex::new(PageAllocator::open(
            &data_dir,
            config,
            Arc::clone(&wal_writer),
        )?));
        let buffer_pool = Arc::new(BufferPool::open(
            &data_dir,
            config,
            Arc::clone(&page_allocator),
            Arc::clone(&wal_writer),
        )?);

        // A fresh database has no freelist snapshot; the first checkpoint
        // will write one. No load needed here.

        let checkpoint = CheckpointCoordinator::new(
            &data_dir,
            config,
            Arc::clone(&superblock),
            Arc::clone(&buffer_pool),
            Arc::clone(&page_allocator),
            Arc::clone(&wal_writer),
        );

        // Seed the XID clock from the (fresh) superblock and share it with the
        // checkpoint coordinator so checkpoints persist the live value.
        let txn_id_clock = TxnIdClock::new(next_txn_id);
        checkpoint.set_next_txn_id_source(txn_id_clock.clone());

        Ok(Self {
            data_dir,
            config: config.clone(),
            superblock,
            page_allocator,
            wal_writer,
            buffer_pool,
            checkpoint,
            txn_id_clock,
            clog,
            recovered_att: Vec::new(),
            recovered_redo_start: Lsn::FIRST,
            _data_dir_lock: data_dir_lock,
        })
    }

    /// Recover a storage engine from disk after a crash or clean shutdown.
    ///
    /// This is the convenience entry without extra redo handlers; see
    /// [`Self::recover_with_redo_handlers`] for the full (Stage I) procedure.
    /// Callers opening a data directory that may contain heap WAL records
    /// must use [`Self::open_with_redo_handlers`] with the heap handlers
    /// injected, or replay will fail on the unregistered record types.
    pub fn recover(data_dir: PathBuf, config: &StorageConfig) -> Result<Self> {
        Self::recover_with_redo_handlers(
            data_dir,
            config,
            Vec::new(),
            Vec::new(),
            Arc::new(NoOpClogAccessor),
        )
    }

    /// Recover, injecting `extra_redo_handlers` into the redo registry.
    ///
    /// From Stage I recovery opens the buffer pool **before** replay so that
    /// redo handlers can pin/dirty pages through it (`RedoContext.buffer_pool`
    /// is `Some`). Since Stage N the replay start comes from the ARIES
    /// analysis phase (tech-selection §11.1). Order: superblock → WAL writer
    /// → page allocator → freelist snapshot → buffer pool → **analysis** →
    /// replay (redo) → flush dirty pages → checkpoint coordinator.
    pub fn recover_with_redo_handlers(
        data_dir: PathBuf,
        config: &StorageConfig,
        extra_redo_handlers: Vec<Box<dyn RedoHandler>>,
        extra_undo_handlers: Vec<Box<dyn UndoHandler>>,
        clog: Arc<dyn ClogAccessor>,
    ) -> Result<Self> {
        // F1: same exclusive directory lock as the open path, taken before
        // any file is read (the superblock included).
        let data_dir_lock = DataDirLock::acquire(&data_dir)?;
        Self::recover_inner(
            data_dir,
            config,
            extra_redo_handlers,
            extra_undo_handlers,
            clog,
            data_dir_lock,
        )
    }

    /// The recovery body, with the directory lock already held by the
    /// caller (see [`Self::open_with_redo_and_clog`] /
    /// [`Self::recover_with_redo_handlers`]).
    fn recover_inner(
        data_dir: PathBuf,
        config: &StorageConfig,
        extra_redo_handlers: Vec<Box<dyn RedoHandler>>,
        extra_undo_handlers: Vec<Box<dyn UndoHandler>>,
        clog: Arc<dyn ClogAccessor>,
        data_dir_lock: DataDirLock,
    ) -> Result<Self> {
        info!(data_dir = %data_dir.display(), "recovering storage engine");

        let sb_path = Superblock::path(&data_dir);
        let superblock = Superblock::read(&sb_path)?;
        let checkpoint_lsn = superblock.checkpoint_lsn;
        let next_txn_id = superblock.next_txn_id;
        info!(%checkpoint_lsn, "loaded superblock");

        // 2. Open the WAL writer once. WalWriter::open_with_scan_start scans
        //    the durable WAL from the checkpoint redo point (a guaranteed
        //    record boundary inside the oldest retained segment — the oldest
        //    segment's boundary itself may cut through a record) and resumes
        //    appending after the last complete record, so it is consistent
        //    with the on-disk state. Replay itself writes no WAL (redo is
        //    idempotent and, because the buffer pool's checkpoint_lsn stays
        //    invalid during recovery, pin_mut emits no FPI), so opening it
        //    up front is safe and lets the allocator + buffer pool share it.
        let wal_writer = Arc::new(WalWriter::open_with_scan_start(
            &data_dir,
            config,
            checkpoint_lsn,
        )?);
        let page_allocator = Arc::new(Mutex::new(PageAllocator::open_at(
            &data_dir,
            config,
            Arc::clone(&wal_writer),
            superblock.next_page_id,
        )?));

        // 3. Load freelist snapshot (acceleration; WAL is authoritative).
        //    If the snapshot is corrupted (CRC mismatch), warn and skip — WAL
        //    replay will rebuild the freelist from scratch. If the snapshot's
        //    checkpoint_lsn matches the superblock's, seed the allocator so
        //    replay only needs to apply post-checkpoint records.
        //
        //    If the snapshot's checkpoint_lsn does NOT match the superblock's,
        //    pre-checkpoint frees are permanently leaked: WAL segments before
        //    the superblock's checkpoint_lsn have already been recycled, so
        //    they cannot be replayed, and the stale snapshot is the only other
        //    source. This is a leak, not corruption — the pages remain
        //    allocated and are simply never reused. We warn so operators can
        //    investigate the mismatch.
        match FreelistMeta::read(&FreelistMeta::path(&data_dir)) {
            Ok(snap) => {
                if snap.checkpoint_lsn == checkpoint_lsn {
                    debug!(%checkpoint_lsn, count = snap.page_ids.len(), "seeding freelist from snapshot");
                    page_allocator.lock().seed_freelist(&snap.page_ids);
                } else {
                    warn!(
                        snapshot_lsn = %snap.checkpoint_lsn,
                        superblock_lsn = %checkpoint_lsn,
                        "freelist snapshot LSN does not match superblock; \
                         skipping seed — pre-checkpoint frees are leaked (not corruption)"
                    );
                }
            }
            Err(StorageError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
                debug!("no freelist.meta found; rebuilding from WAL");
            }
            Err(StorageError::MetadataCorrupted(msg)) => {
                warn!(error = %msg, "freelist.meta corrupted; rebuilding freelist from WAL");
            }
            Err(e) => return Err(e),
        }

        // 4. Open the buffer pool before replay so redo handlers can route
        //    through it. Its checkpoint_lsn starts invalid, which keeps pin_mut
        //    from emitting FPIs during redo (no WAL recursion).
        let buffer_pool = Arc::new(BufferPool::open(
            &data_dir,
            config,
            Arc::clone(&page_allocator),
            Arc::clone(&wal_writer),
        )?);

        // 5. Analysis phase (M2b Stage N; tech-selection §11.1): locate the
        //    latest *completed* CheckpointEnd, rebuild the ATT/DPT from its
        //    snapshot files plus the WAL tail, and derive the redo start LSN.
        //    A CheckpointBegin without a matching CheckpointEnd (crash
        //    mid-checkpoint) is invisible here: the superblock still points
        //    at the previous completed checkpoint, whose CheckpointEnd is
        //    the latest one the scan finds.
        let analysis_result = Self::run_analysis(&data_dir, config, checkpoint_lsn)?;
        let replay_start = analysis_result.redo_start;
        let recovered_att = analysis_result.att;
        let (replayed_max_txn_id, incomplete_splits) = Self::replay_wal(
            data_dir.clone(),
            config,
            replay_start,
            &page_allocator,
            &buffer_pool,
            extra_redo_handlers,
            clog.as_ref(),
        )?;
        page_allocator.lock().mark_recovery_complete();

        // 5b. Simplified undo, filter half (§11.3; see the analysis module
        //     docs): now that redo has rebuilt the CLOG, drop ATT members
        //     the CLOG already knows as Committed/Aborted — they are NOT in
        //     flight. This closes the §11.4 ATT-snapshot race where a
        //     commit's WAL record predates the checkpoint begin (invisible
        //     to the analysis scan) while the racy snapshot still lists the
        //     XID. The survivors are genuinely uncommitted; Stage N writes
        //     no ABORTED for them (M2c work). NOTE: with the NoOp CLOG
        //     (M1/heap-only configurations) every XID reads Committed, so
        //     the recovered ATT comes back empty — correct there, since a
        //     configuration without a real CLOG has no visibility decisions
        //     to inform.
        let recovered_att: Vec<TxnId> = recovered_att
            .into_iter()
            .filter(|xid| {
                !matches!(
                    clog.get_state(*xid),
                    TxnState::Committed | TxnState::Aborted
                )
            })
            .collect();

        // Undo phase (Stage S, §11.3): finish incomplete B+Tree splits and
        // stamp aborted XIDs in the CLOG. Runs after redo + ATT filter, BEFORE
        // the checkpoint_lsn seed — deliberately (post-Stage-S review H4):
        // seeding earlier would make `pin_mut` emit FPIs during undo, and an
        // FPI stamps the page's pd_lsn with the FPI's own LSN, which is
        // NEWER than the already-appended CLR — the CLR apply's per-page
        // idempotency guards (page_pd_lsn < clr_lsn) would then skip the
        // very changes the CLR owes (observed: `btree_undo_clr` H3 tests
        // fail with "left page is past the CLR but right page never received
        // the moved entries"). The torn-write hole this ordering leaves —
        // a crash mid-undo-flush producing a garbage pd_lsn ≥ CLR lsn that
        // the next recovery would skip — is instead closed by explicit
        // pre-image FullPageImage records appended BEFORE each CLR
        // (`emit_and_apply_clr` in pg-am-btree): FPI replay restores the
        // pre-image unconditionally, then re-applies the CLR on top.
        //
        // The handlers run UNCONDITIONALLY — even
        // with an empty tracker and ATT — because the B+Tree undo handler
        // also scans pages for `SPLIT_INCOMPLETE` flags that redo could not
        // see (post-Stage-S review H3: a split whose Prepare predates the
        // checkpoint's redo start leaves no record in the replay window, but
        // its flag is durable on the page).
        //
        // The heap undo handler's `Aborted` CLOG stamps are NOT made durable
        // here (post-Stage-S review B3): they are `set_state` calls with no
        // WAL record, flushed only by the first post-recovery checkpoint's
        // `ClogFlush` hook. A crash before that checkpoint loses them
        // harmlessly — this recovery path then re-derives the same ATT and
        // re-stamps (the marks are idempotent), and a missing CLOG entry
        // reads `InProgress`, already MVCC-invisible (Stage N "no explicit
        // heap undo" decision; see the analysis module docs and
        // `UndoContext::clog`).
        {
            let mut undo_ctx = UndoContext {
                buffer_pool: &buffer_pool,
                wal_writer: &wal_writer,
                clog: clog.as_ref(),
                att: &recovered_att,
                incomplete_splits: &incomplete_splits,
                page_allocator: &page_allocator,
            };
            for handler in &extra_undo_handlers {
                handler.undo(&mut undo_ctx)?;
            }
            // Make CLR records durable before flushing data pages
            // (WAL-before-data invariant for undo-generated records).
            wal_writer.flush()?;
            // Flush dirty pages from undo.
            for page_id in buffer_pool.dirty_page_ids() {
                buffer_pool.flush(page_id)?;
            }
        }

        // Seed the buffer pool's checkpoint_lsn from the superblock. Without
        // this it stays INVALID until the first new checkpoint, and
        // `pin_mut`'s FPI gate (checkpoint_lsn.is_valid()) means every page
        // modification in that window goes without FPI protection — while
        // the pages involved DO have on-disk images after a restart. A torn
        // write there would leave a garbage pd_lsn that heap redo's
        // idempotency check (page_pd_lsn >= record.lsn) reads as "already
        // applied", skipping the record — silent corruption. Seeding the
        // last checkpoint's LSN restores PG's post-recovery rule: first
        // touch of any page whose pd_lsn predates that LSN writes an FPI.
        buffer_pool.set_checkpoint_lsn(checkpoint_lsn);

        let superblock = Arc::new(Mutex::new(superblock));
        let checkpoint = CheckpointCoordinator::new(
            &data_dir,
            config,
            Arc::clone(&superblock),
            Arc::clone(&buffer_pool),
            Arc::clone(&page_allocator),
            Arc::clone(&wal_writer),
        );

        // Seed the XID clock and share it with the checkpoint coordinator so
        // checkpoints persist the live value. The catalog/txn layer may later
        // replace the source via `set_next_txn_id_source` once it wires its
        // own allocator.
        //
        // The seed is the maximum of two lower bounds:
        //   * `next_txn_id` from the superblock — the value persisted at the
        //     last checkpoint;
        //   * `replayed_max_txn_id + 1` — one past the highest XID stamped on
        //     any replayed WAL record.
        //
        // The WAL bound is essential: transactions committed *after* the last
        // checkpoint advanced no superblock value, so seeding from the
        // superblock alone would hand a fresh transaction an XID that a
        // replayed `TxnCommit` already marked committed in the CLOG — the new
        // transaction's tuples would be instantly visible (XID reuse). Taking
        // the WAL high-water mark restores PG's rule of advancing `nextXid`
        // past every XID observed during redo.
        let recovered_next_txn_id = if replayed_max_txn_id != TxnId::INVALID {
            // saturating_add: a record carrying u64::MAX must not wrap the
            // clock back to 0 — that would silently restart XID reuse, the
            // exact failure this seeding exists to prevent.
            std::cmp::max(next_txn_id, TxnId(replayed_max_txn_id.0.saturating_add(1)))
        } else {
            next_txn_id
        };
        let txn_id_clock = TxnIdClock::new(recovered_next_txn_id);
        checkpoint.set_next_txn_id_source(txn_id_clock.clone());

        info!("recovery complete");
        Ok(Self {
            data_dir,
            config: config.clone(),
            superblock,
            page_allocator,
            wal_writer,
            buffer_pool,
            checkpoint,
            txn_id_clock,
            clog,
            recovered_att,
            recovered_redo_start: replay_start,
            _data_dir_lock: data_dir_lock,
        })
    }

    /// Run the ARIES analysis phase (M2b Stage N; tech-selection §11.1).
    ///
    /// `checkpoint_lsn` is the superblock's redo point. The WAL scan starts
    /// there (a guaranteed record boundary) and looks for the latest
    /// completed `CheckpointEnd`:
    ///
    /// - found, with the same `checkpoint_lsn` — the normal case: analyze
    ///   from that record, consuming its ATT/DPT snapshot files (v2) or
    ///   rebuilding with an empty baseline (v1);
    /// - found, but NEWER than the superblock — the instance crashed after
    ///   `flush_to(end_lsn)` but before the superblock write (checkpoint.rs
    ///   steps 7→9). The superblock's older anchors (`next_page_id`,
    ///   `next_txn_id`) still seed the allocator and XID clock, so redo must
    ///   cover the records between the two redo points: analyze from the
    ///   superblock's point with an empty baseline, exactly like a v1
    ///   record;
    /// - not found — either no checkpoint ever completed (`checkpoint_lsn`
    ///   invalid, analyze from [`Lsn::FIRST`]) or the WAL lost the record
    ///   (warn; analyze from the superblock's point with an empty
    ///   baseline).
    fn run_analysis(
        data_dir: &Path,
        config: &StorageConfig,
        checkpoint_lsn: Lsn,
    ) -> Result<analysis::AnalysisResult> {
        let scan_start = if checkpoint_lsn.is_valid() {
            checkpoint_lsn
        } else {
            warn!("checkpoint_lsn is invalid; analyzing WAL from the beginning");
            Lsn::FIRST
        };
        let end_record = match analysis::find_latest_checkpoint_end(
            &data_dir.join("wal"),
            config.wal_segment_size,
            scan_start,
        )? {
            Some((end, _end_lsn)) if end.checkpoint_lsn == checkpoint_lsn => end,
            Some((end, end_lsn)) => {
                warn!(
                    superblock_checkpoint_lsn = %checkpoint_lsn,
                    end_checkpoint_lsn = %end.checkpoint_lsn,
                    %end_lsn,
                    "WAL holds a CheckpointEnd newer than the superblock \
                     (crash between WAL flush and superblock write); \
                     rebuilding analysis baseline from the superblock redo point"
                );
                Self::synthesized_checkpoint_end(scan_start)
            }
            None => {
                if checkpoint_lsn.is_valid() {
                    warn!(
                        %checkpoint_lsn,
                        "no CheckpointEnd record found for the superblock's \
                         checkpoint; rebuilding analysis baseline from the \
                         superblock redo point"
                    );
                }
                Self::synthesized_checkpoint_end(scan_start)
            }
        };
        analysis::run_analysis(data_dir, config.wal_segment_size, &end_record)
    }

    /// Build an in-memory v1-equivalent `CheckpointEnd` anchor: the empty
    /// snapshot file references make analysis rebuild the ATT/DPT by a full
    /// WAL scan from `checkpoint_lsn` (§11.4 empty-`att_file` semantics).
    /// Only `checkpoint_lsn` is consumed by analysis; the remaining fields
    /// are placeholders.
    fn synthesized_checkpoint_end(checkpoint_lsn: Lsn) -> CheckpointEndRecord {
        CheckpointEndRecord {
            checkpoint_lsn,
            next_page_id: PageId::INVALID,
            next_txn_id: TxnId::INVALID,
            next_oid: 0,
            att_file: String::new(),
            dpt_file: String::new(),
        }
    }

    /// Replay WAL from `replay_start` (the analysis phase's redo point) and
    /// return the highest `txn_id` stamped on any replayed record.
    ///
    /// The returned XID is the recovery high-water mark: every record that a
    /// transaction wrote carries its XID (`WalRecord::txn_id`), so the largest
    /// one seen bounds all XIDs that were ever handed out before the crash.
    /// The caller advances the [`TxnIdClock`] past it so post-recovery
    /// transactions never reuse a committed/aborted XID — the equivalent of
    /// PG advancing `nextXid` during redo. Returns [`TxnId::INVALID`] if no
    /// record carried an XID (e.g. a pure-DDL or empty WAL).
    fn replay_wal(
        data_dir: PathBuf,
        config: &StorageConfig,
        replay_start: Lsn,
        page_allocator: &Arc<Mutex<PageAllocator>>,
        buffer_pool: &BufferPool,
        extra_redo_handlers: Vec<Box<dyn RedoHandler>>,
        clog: &dyn ClogAccessor,
    ) -> Result<(TxnId, IncompleteSplitTracker)> {
        let mut reader =
            WalReader::open_at(data_dir.join("wal"), config.wal_segment_size, replay_start)?;

        // The FPI handler routes through the buffer pool when present (it is,
        // here), but still needs a data-file handle for its no-pool fallback.
        // This is a replay-scoped temporary handle that must NOT be reused for
        // normal operation.
        let data_file_path = crate::io::data_file_path(&data_dir);
        let data_file = Arc::new(PositionedFile::open(&data_file_path)?);

        // Stage D: replay dispatches through the RedoRegistry. Every record
        // type that can appear in the WAL has exactly one registered handler;
        // anything else is a hard failure (UnknownRecord) instead of being
        // silently skipped. Stage I injects heap handlers via
        // `extra_redo_handlers`.
        let mut registry = RedoRegistry::new();
        registry.register(Box::new(PageAllocRedoHandler));
        registry.register(Box::new(PageFreeRedoHandler));
        registry.register(Box::new(FullPageImageRedoHandler::new(Arc::clone(
            &data_file,
        ))));
        registry.register(Box::new(NoOpRedoHandler::new(
            WalRecordType::CheckpointBegin,
        )));
        registry.register(Box::new(NoOpRedoHandler::new(WalRecordType::CheckpointEnd)));
        for handler in extra_redo_handlers {
            registry.register(handler);
        }

        let mut att = ActiveXactTable::new();
        let mut dpt = DirtyPageTable::new();
        let mut incomplete_splits = IncompleteSplitTracker::new();
        let mut ctx = RedoContext {
            buffer_pool: Some(buffer_pool),
            page_allocator,
            clog,
            att: &mut att,
            dpt: &mut dpt,
            incomplete_splits: &mut incomplete_splits,
        };
        let mut records_replayed = 0usize;
        let mut max_txn_id = TxnId::INVALID;

        loop {
            match reader.next_record() {
                Ok(Some(record)) => {
                    if record.txn_id > max_txn_id {
                        max_txn_id = record.txn_id;
                    }
                    registry.apply(&record, &mut ctx)?;
                    records_replayed += 1;
                }
                Ok(None) => break,
                Err(e) => {
                    // Propagate hard errors (hole detection, corrupt
                    // metadata) — they are not end-of-WAL. Genuine tail
                    // truncation (WalCorrupted) is safe to treat as
                    // end-of-WAL: records before it are already applied.
                    if analysis::is_hard_error(&e) {
                        return Err(e);
                    }
                    warn!(error = %e, "WAL replay stopped at truncated/final record");
                    break;
                }
            }
        }

        // Flush all pages dirtied by redo through the buffer pool, which makes
        // them durable (WAL-before-data + fsync). This replaces the M1
        // direct-write + data_file.sync_all() path.
        for page_id in buffer_pool.dirty_page_ids() {
            buffer_pool.flush(page_id)?;
        }

        info!(records_replayed, "WAL replay complete");
        Ok((max_txn_id, incomplete_splits))
    }

    /// Return the data directory.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Return the storage configuration.
    pub fn config(&self) -> &StorageConfig {
        &self.config
    }

    /// Return a reference to the superblock.
    pub fn superblock(&self) -> &Arc<Mutex<Superblock>> {
        &self.superblock
    }

    /// Return a reference to the buffer pool.
    pub fn buffer_pool(&self) -> &Arc<BufferPool> {
        &self.buffer_pool
    }

    /// Return a reference to the page allocator.
    pub fn page_allocator(&self) -> &Arc<Mutex<PageAllocator>> {
        &self.page_allocator
    }

    /// Return a reference to the WAL writer.
    pub fn wal_writer(&self) -> &Arc<WalWriter> {
        &self.wal_writer
    }

    /// Return a reference to the checkpoint coordinator.
    pub fn checkpoint(&self) -> &CheckpointCoordinator {
        &self.checkpoint
    }

    /// Return a clone of the transaction-id clock.
    ///
    /// The clock is seeded from the superblock's `next_txn_id` at open and
    /// shared (via `Arc`) with the checkpoint coordinator, which persists the
    /// live value on each checkpoint. The txn layer allocates XIDs from it.
    pub fn txn_id_clock(&self) -> TxnIdClock {
        self.txn_id_clock.clone()
    }

    /// Return the commit-log accessor injected at open.
    pub fn clog(&self) -> &Arc<dyn ClogAccessor> {
        &self.clog
    }

    /// XIDs that were active (neither committed nor aborted) when the
    /// previous instance stopped, rebuilt by the analysis phase (M2b
    /// Stage N; tech-selection §11.1) and filtered through the rebuilt
    /// CLOG (members the CLOG knows as Committed/Aborted are dropped — see
    /// the [`crate::analysis`] module docs). Empty on a freshly created
    /// database, when every transaction reached a terminal record before
    /// the crash, or under the `NoOp` CLOG (which reads every XID as
    /// Committed).
    ///
    /// Stage N runs no explicit undo for these XIDs: they have no terminal
    /// record in the durable WAL, so the rebuilt CLOG reads them as
    /// InProgress and MVCC visibility already hides their tuples. See the
    /// [`crate::analysis`] module docs for the §11.3 alignment of this
    /// decision.
    pub fn recovered_active_xids(&self) -> &[TxnId] {
        &self.recovered_att
    }

    /// The WAL position the just-completed recovery's redo scan started
    /// from (a guaranteed record boundary in a retained segment).
    /// [`Lsn::FIRST`] on a freshly created database. pg-engine uses this to
    /// bound its loser-transaction WAL scan (index-entry compensation).
    pub fn recovered_redo_start(&self) -> Lsn {
        self.recovered_redo_start
    }

    /// Return the `next_oid` currently recorded in the superblock.
    ///
    /// Stage H (catalog bootstrap) uses this to initialize the OID allocator:
    /// the superblock is the authoritative source of `next_oid` across
    /// checkpoints until CheckpointEnd WAL records switch to v2 (Stage N).
    pub fn next_oid(&self) -> crate::types::Oid {
        self.superblock.lock().next_oid
    }

    /// Install the source of `next_oid` values persisted by checkpoints
    /// (Stage H wiring). Forwards to
    /// [`CheckpointCoordinator::set_next_oid_source`]; the catalog calls this
    /// once its OID allocator exists.
    pub fn set_next_oid_source(&self, source: crate::oid::OidCounter) {
        self.checkpoint.set_next_oid_source(source);
    }

    /// Manually trigger a checkpoint.
    pub fn trigger_checkpoint(&self) -> Result<Lsn> {
        self.checkpoint.trigger_checkpoint()
    }

    /// Start automatic background checkpoints.
    pub fn start_background_checkpointing(&self) -> Result<()> {
        self.checkpoint.start_background_checkpointing()
    }

    /// Gracefully shut down background threads.
    pub fn shutdown(&self) {
        self.checkpoint.shutdown();
        // WalWriter's Drop handles its own worker shutdown.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::PAGE_HEADER_SIZE;
    use crate::types::PageId;

    #[test]
    fn create_and_recover_empty_engine() {
        let tmp = tempfile::TempDir::new().unwrap();
        let config = StorageConfig::new(tmp.path());

        {
            let engine = StorageEngine::open(tmp.path(), &config).unwrap();
            engine.trigger_checkpoint().unwrap();
        }

        {
            let engine = StorageEngine::open(tmp.path(), &config).unwrap();
            assert!(engine.superblock.lock().checkpoint_lsn.is_valid());
        }
    }

    #[test]
    fn write_and_recover_data_after_checkpoint() {
        let tmp = tempfile::TempDir::new().unwrap();
        let config = StorageConfig::new(tmp.path());

        let page_id = {
            let engine = StorageEngine::open(tmp.path(), &config).unwrap();
            let mut guard = engine.buffer_pool().new_page().unwrap();
            let id = guard.page_id();
            guard.page_mut()[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + 4].copy_from_slice(&[1, 2, 3, 4]);
            drop(guard);
            engine.trigger_checkpoint().unwrap();
            id
        };

        {
            let engine = StorageEngine::open(tmp.path(), &config).unwrap();
            let guard = engine.buffer_pool().pin(page_id).unwrap();
            assert_eq!(
                &guard.page()[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + 4],
                &[1, 2, 3, 4]
            );
        }
    }

    #[test]
    fn recover_without_checkpoint_replays_page_allocs() {
        let tmp = tempfile::TempDir::new().unwrap();
        let config = StorageConfig::new(tmp.path());

        let page_id = {
            let engine = StorageEngine::open(tmp.path(), &config).unwrap();
            let guard = engine.buffer_pool().new_page().unwrap();
            let id = guard.page_id();
            drop(guard);
            // Intentionally do NOT checkpoint. The next open must replay the
            // PageAlloc WAL record so that it does not hand out the same id.
            id
        };

        {
            let engine = StorageEngine::open(tmp.path(), &config).unwrap();
            let guard = engine.buffer_pool().new_page().unwrap();
            assert_ne!(guard.page_id(), page_id, "PageAlloc WAL was not replayed");
            assert!(guard.page_id().0 > page_id.0);
        }
    }

    #[test]
    fn checkpoint_recycles_old_wal_segments() {
        let tmp = tempfile::TempDir::new().unwrap();
        let mut config = StorageConfig::new(tmp.path());
        // Use a tiny segment size so that a modest number of allocations spans
        // several segments and checkpointing has something to recycle.
        config.wal_segment_size = 1024;

        let engine = StorageEngine::open(tmp.path(), &config).unwrap();
        // Allocate and modify enough pages to span multiple WAL segments.
        for _ in 0..64 {
            let mut guard = engine.buffer_pool().new_page().unwrap();
            guard.page_mut()[PAGE_HEADER_SIZE] = 0xAB;
        }

        let wal_dir = tmp.path().join("wal");
        let segments_before = std::fs::read_dir(&wal_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "log"))
            .count();
        assert!(
            segments_before > 1,
            "test precondition failed: expected multiple WAL segments, got {segments_before}"
        );

        engine.trigger_checkpoint().unwrap();

        // After checkpoint, the number of retained segments should be reduced.
        let segments_after = std::fs::read_dir(&wal_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "log"))
            .count();
        assert!(
            segments_after < segments_before,
            "old WAL segments were not recycled: before={segments_before}, after={segments_after}"
        );
    }

    #[test]
    fn recover_repairs_torn_page_after_checkpoint() {
        use std::io::{Seek, SeekFrom, Write};
        use std::mem;

        let tmp = tempfile::TempDir::new().unwrap();
        let mut config = StorageConfig::new(tmp.path());
        // Use a tiny buffer pool so that the original page is evicted quickly.
        config.buffer_pool_size = 256 * 1024; // 32 frames

        let page_id = {
            let engine = StorageEngine::open(tmp.path(), &config).unwrap();

            // 1. Allocate and modify a page (past the 32-byte page header so
            //    pd_lsn is not clobbered).
            let mut guard = engine.buffer_pool().new_page().unwrap();
            let id = guard.page_id();
            guard.page_mut()[PAGE_HEADER_SIZE..].fill(0xCD);
            drop(guard);

            // 2. First checkpoint: establishes checkpoint_lsn and flushes the page.
            engine.trigger_checkpoint().unwrap();

            // 3. Evict the page so that the next pin_mut sees it as "old" and
            //    writes a FullPageImage (because page_lsn < checkpoint_lsn).
            let frame_count = engine.buffer_pool().frame_count();
            for _ in 0..frame_count + 4 {
                drop(engine.buffer_pool().new_page().unwrap());
            }

            // 4. Reload and modify the page. The first pin_mut writes an FPI.
            {
                let mut guard = engine.buffer_pool().pin_mut(id).unwrap();
                guard.page_mut()[PAGE_HEADER_SIZE..].fill(0xCD);
            }

            // 5. Ensure the FPI is durable in the WAL. We do not need a second
            //    checkpoint; the recovery replay will apply the FPI directly.
            engine.wal_writer().flush().unwrap();

            // Simulate kill -9: do not run Drop / graceful shutdown. This leaks
            // the WalWriter background thread, but the process exits shortly and
            // the OS reaps it. A more realistic crash is exercised by the
            // fork+kill integration tests in tests/crash_recovery.rs; this unit
            // test keeps the torn-page repair path fast and self-contained.
            mem::forget(engine);
            id
        };

        // Corrupt the first half of the page in the data file (torn write).
        let data_file_path = crate::io::data_file_path(tmp.path());
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(&data_file_path)
            .unwrap();
        let offset = (page_id.0 - 1) * crate::types::PAGE_SIZE as u64;
        file.seek(SeekFrom::Start(offset)).unwrap();
        let half = vec![0xFFu8; crate::types::PAGE_SIZE / 2];
        file.write_all(&half).unwrap();
        file.sync_all().unwrap();
        drop(file);

        // Recovery replays the FPI, repairing the torn page. (Only the payload
        // region past the page header is compared: the FPI replay patches
        // pd_lsn at page[0..8] to the record's own LSN.)
        let engine = StorageEngine::open(tmp.path(), &config).unwrap();
        let guard = engine.buffer_pool().pin(page_id).unwrap();
        assert!(
            guard.page()[PAGE_HEADER_SIZE..].iter().all(|&b| b == 0xCD),
            "FPI did not repair the torn page"
        );
    }

    #[test]
    fn background_checkpoint_flushes_dirty_pages() {
        use std::time::Duration;

        let tmp = tempfile::TempDir::new().unwrap();
        let mut config = StorageConfig::new(tmp.path());
        // Short interval so the test does not have to wait long.
        config.checkpoint_interval_ms = 200;
        config.wal_group_commit_timeout_ms = 1;
        config.wal_group_commit_batch_size = 1;

        let page_id = {
            let engine = StorageEngine::open(tmp.path(), &config).unwrap();
            engine.start_background_checkpointing().unwrap();

            let mut guard = engine.buffer_pool().new_page().unwrap();
            let id = guard.page_id();
            guard.page_mut()[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + 8].copy_from_slice(b"bgckpt01");
            drop(guard);

            // Wait for at least one background checkpoint to run.
            std::thread::sleep(Duration::from_millis(600));

            id
        };

        // Reopen without an explicit manual checkpoint: the background thread
        // should have persisted the page.
        let engine = StorageEngine::open(tmp.path(), &config).unwrap();
        let guard = engine.buffer_pool().pin(page_id).unwrap();
        assert_eq!(
            &guard.page()[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + 8],
            b"bgckpt01"
        );
    }

    #[test]
    fn concurrent_new_page_and_pin_are_safe() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::thread;

        let tmp = tempfile::TempDir::new().unwrap();
        let mut config = StorageConfig::new(tmp.path());
        // Small buffer pool to force eviction pressure.
        config.buffer_pool_size = 256 * 1024; // 32 frames
        config.wal_group_commit_timeout_ms = 1;
        config.wal_group_commit_batch_size = 8;

        let engine = StorageEngine::open(tmp.path(), &config).unwrap();
        let engine = Arc::new(engine);

        let successes = Arc::new(AtomicUsize::new(0));
        let all_ids: Arc<Mutex<Vec<PageId>>> = Arc::new(Mutex::new(Vec::new()));
        let mut handles = Vec::new();

        for _ in 0..64 {
            let e = Arc::clone(&engine);
            let s = Arc::clone(&successes);
            let ids = Arc::clone(&all_ids);
            handles.push(thread::spawn(move || {
                for _ in 0..25 {
                    if let Ok(mut g) = e.buffer_pool().new_page() {
                        g.page_mut()[PAGE_HEADER_SIZE] = 0xAB;
                        ids.lock().push(g.page_id());
                        s.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let ids = all_ids.lock();
        assert_eq!(ids.len(), 64 * 25);
        let mut sorted = ids.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), ids.len(), "duplicate page IDs detected");

        // All successfully allocated pages must be durable after checkpoint.
        let checkpoint_lsn = engine.trigger_checkpoint().unwrap();
        assert!(checkpoint_lsn.is_valid());
    }

    #[test]
    fn multiple_checkpoints_keep_data_consistent() {
        let tmp = tempfile::TempDir::new().unwrap();
        let mut config = StorageConfig::new(tmp.path());
        // Use the default WAL segment size: modifying a resident page after a
        // checkpoint now correctly emits a full-page image (~8 KiB), which does
        // not fit the tiny segments some other tests use. Segment recycling is
        // covered by `checkpoint_recycles_old_wal_segments`.
        config.wal_group_commit_timeout_ms = 1;
        config.wal_group_commit_batch_size = 1;

        let ids = {
            let engine = StorageEngine::open(tmp.path(), &config).unwrap();
            let mut ids = Vec::new();
            for i in 0..16u8 {
                let mut guard = engine.buffer_pool().new_page().unwrap();
                guard.page_mut()[PAGE_HEADER_SIZE] = i;
                ids.push(guard.page_id());
            }

            // First checkpoint.
            engine.trigger_checkpoint().unwrap();

            // Modify a subset after the first checkpoint.
            for (idx, id) in ids.iter().enumerate() {
                if idx % 2 == 0 {
                    let mut guard = engine.buffer_pool().pin_mut(*id).unwrap();
                    guard.page_mut()[PAGE_HEADER_SIZE + 1] = 0xCC;
                }
            }

            // Second checkpoint; WAL recycling is verified by the dedicated
            // `checkpoint_recycles_old_wal_segments` test above.
            engine.trigger_checkpoint().unwrap();
            ids
        };

        let engine = StorageEngine::open(tmp.path(), &config).unwrap();
        for (idx, id) in ids.iter().enumerate() {
            let guard = engine.buffer_pool().pin(*id).unwrap();
            assert_eq!(guard.page()[PAGE_HEADER_SIZE], idx as u8);
            if idx % 2 == 0 {
                assert_eq!(guard.page()[PAGE_HEADER_SIZE + 1], 0xCC);
            }
        }
    }

    /// Regression for the P0 bug where `WalWriter::open`'s resume scan
    /// started at `Lsn::FIRST`: once WAL spanned more than one segment and a
    /// checkpoint recycled the older ones, reopening the engine failed with
    /// "No such file or directory" — the database could not start.
    ///
    /// The second phase additionally guards the follow-up bug: the resume
    /// scan must not start at the oldest retained segment's *boundary*,
    /// because that boundary can cut through a record (records span
    /// segments). With 40-byte PageAlloc records and 4096-byte segments,
    /// segment 1's boundary (byte 4096) falls in the middle of record #102
    /// (bytes 4088..4128); a boundary-start scan would misread the orphan
    /// tail as the end of the WAL and truncate every record after it.
    #[test]
    fn reopen_after_wal_segment_recycle() {
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let tmp = tmp_dir.path();
        let mut config = StorageConfig::new(tmp);
        config.wal_segment_size = 4096; // tiny segments to force recycling

        let written;
        let begin_lsn;
        {
            let engine = StorageEngine::open(tmp, &config).unwrap();
            // 150 PageAlloc records (~40 B each) put the checkpoint begin_lsn
            // into segment 1, whose boundary cuts through record #102.
            for _ in 0..150 {
                let mut guard = engine.buffer_pool().new_page().unwrap();
                guard.page_mut()[PAGE_HEADER_SIZE] = 0xAB;
            }
            written = engine.page_allocator().lock().next_page_id();
            // Checkpoint recycles every segment before its begin_lsn.
            begin_lsn = engine.trigger_checkpoint().unwrap();
            engine.shutdown();
        }

        // Segment 0 must be gone, or this test exercises nothing.
        assert!(
            !tmp.join("wal").join("wal-00000001.log").exists(),
            "segment 0 was not recycled; test is vacuous"
        );

        // Reopen must succeed and the data must be intact.
        let engine = StorageEngine::open(tmp, &config).unwrap();
        assert_eq!(engine.page_allocator().lock().next_page_id(), written);
        let guard = engine.buffer_pool().pin(PageId(1)).unwrap();
        assert_eq!(guard.page()[PAGE_HEADER_SIZE], 0xAB);

        // The resume scan must have found the true end of the WAL: a new
        // append belongs AFTER the checkpoint records, not at the recycled
        // segment's boundary (which would overwrite them).
        let appended = engine
            .wal_writer()
            .append(crate::wal::record::WalRecord::checkpoint_begin())
            .unwrap();
        assert!(
            appended > begin_lsn,
            "resume scan truncated the WAL: appended at {appended}, checkpoint began at {begin_lsn}"
        );
    }

    /// P1-1 regression: after a restart, the buffer pool's checkpoint_lsn
    /// must be seeded from the superblock, so the first modification of an
    /// on-disk page writes an FPI. Before the fix, checkpoint_lsn stayed
    /// INVALID until the first new checkpoint and post-recovery
    /// modifications went without torn-write protection.
    #[test]
    fn recover_seeds_buffer_pool_checkpoint_lsn() {
        let tmp = tempfile::TempDir::new().unwrap();
        let config = StorageConfig::new(tmp.path());

        // Create a page, checkpoint (page is durable on disk), shut down.
        {
            let engine = StorageEngine::open(tmp.path(), &config).unwrap();
            {
                let mut guard = engine.buffer_pool().new_page().unwrap();
                guard.page_mut()[PAGE_HEADER_SIZE] = 0x5A;
                // The guard must drop BEFORE the checkpoint: it holds the
                // frame's content write lock, and flush_frame would block on
                // content.read forever (self-deadlock).
            }
            engine.trigger_checkpoint().unwrap();
            engine.shutdown();
        }

        // Reopen and modify the on-disk page: with the seeded checkpoint_lsn
        // this must write an FPI.
        let engine = StorageEngine::open(tmp.path(), &config).unwrap();
        {
            let mut guard = engine.buffer_pool().pin_mut(PageId(1)).unwrap();
            guard.page_mut()[PAGE_HEADER_SIZE + 1] = 0x01;
        }

        // Exactly one FullPageImage record in the WAL (nothing before the
        // reopen could have produced one: new pages skip FPI, checkpoints do
        // not write them).
        let mut reader =
            crate::wal::reader::WalReader::open(tmp.path().join("wal"), config.wal_segment_size)
                .unwrap();
        let mut fpi_count = 0;
        while let Some(record) = reader.next_record().unwrap() {
            if record.record_type == crate::wal::record::WalRecordType::FullPageImage {
                fpi_count += 1;
            }
        }
        assert_eq!(
            fpi_count, 1,
            "first post-recovery modification must write an FPI (got {fpi_count})"
        );
    }

    #[test]
    fn concurrent_grow_and_read_through_separate_fds() {
        use std::sync::atomic::{AtomicBool, Ordering as AOrdering};
        use std::thread;
        use std::time::Duration;

        let tmp = tempfile::TempDir::new().unwrap();
        let mut config = StorageConfig::new(tmp.path());
        config.buffer_pool_size = 4 * 1024 * 1024;
        config.wal_group_commit_timeout_ms = 1;
        config.wal_group_commit_batch_size = 8;

        let engine = Arc::new(StorageEngine::open(tmp.path(), &config).unwrap());
        let stop = Arc::new(AtomicBool::new(false));
        let allocated: Arc<Mutex<Vec<(PageId, u8)>>> = Arc::new(Mutex::new(Vec::new()));

        let alloc_engine = Arc::clone(&engine);
        let alloc_ids = Arc::clone(&allocated);
        let alloc_stop = Arc::clone(&stop);
        let allocator = thread::spawn(move || {
            let mut count = 0u64;
            while !alloc_stop.load(AOrdering::Relaxed) && count < 512 {
                let mut guard = alloc_engine.buffer_pool().new_page().unwrap();
                let tag = (count & 0xFF) as u8;
                guard.page_mut()[PAGE_HEADER_SIZE] = tag;
                guard.page_mut()[PAGE_HEADER_SIZE + 1] = !tag;
                alloc_ids.lock().push((guard.page_id(), tag));
                count += 1;
            }
            count
        });

        let read_engine = Arc::clone(&engine);
        let read_ids = Arc::clone(&allocated);
        let read_stop = Arc::clone(&stop);
        let reader = thread::spawn(move || {
            let mut reads = 0u64;
            while !read_stop.load(AOrdering::Relaxed) {
                let snapshot: Vec<(PageId, u8)> = read_ids.lock().clone();
                for &(id, expected_tag) in &snapshot {
                    let guard = read_engine.buffer_pool().pin(id).unwrap();
                    let page = guard.page();
                    assert_eq!(
                        page[PAGE_HEADER_SIZE], expected_tag,
                        "page {:?} content mismatch after concurrent grow",
                        id
                    );
                    assert_eq!(
                        page[PAGE_HEADER_SIZE + 1],
                        !expected_tag,
                        "page {:?} second byte mismatch after concurrent grow",
                        id
                    );
                    reads += 1;
                }
                if snapshot.is_empty() {
                    thread::sleep(Duration::from_micros(100));
                }
            }
            reads
        });

        thread::sleep(Duration::from_millis(500));
        stop.store(true, AOrdering::Relaxed);

        let alloc_count = allocator.join().unwrap();
        let read_count = reader.join().unwrap();
        assert!(alloc_count > 0, "allocator thread produced no pages");
        assert!(read_count > 0, "reader thread completed no reads");
    }

    #[test]
    fn redo_context_pool_is_some_during_recovery() {
        use crate::recovery::RedoContext;
        use crate::wal::record::{WalRecord, WalRecordType};
        use std::sync::atomic::{AtomicBool, Ordering};

        // A probe handler for HeapInsert records that records whether the
        // buffer pool was present in the RedoContext when it ran.
        struct ProbeHandler {
            saw_pool: Arc<AtomicBool>,
            applied: Arc<AtomicBool>,
        }

        impl RedoHandler for ProbeHandler {
            fn kind(&self) -> WalRecordType {
                WalRecordType::HeapInsert
            }

            fn apply(&self, _record: &WalRecord, ctx: &mut RedoContext<'_>) -> Result<()> {
                self.applied.store(true, Ordering::Relaxed);
                self.saw_pool
                    .store(ctx.buffer_pool.is_some(), Ordering::Relaxed);
                Ok(())
            }
        }

        let tmp = tempfile::TempDir::new().unwrap();
        let config = StorageConfig::new(tmp.path());

        // Create an engine and append a HeapInsert record to the WAL, then make
        // it durable and abandon the engine (simulate crash).
        {
            let engine = StorageEngine::open(tmp.path(), &config).unwrap();
            let record =
                WalRecord::heap_insert(PageId(1), 0, vec![1, 2, 3, 4], TxnId(100)).unwrap();
            engine.wal_writer().append(record).unwrap();
            engine.wal_writer().flush().unwrap();
            std::mem::forget(engine);
        }

        let saw_pool = Arc::new(AtomicBool::new(false));
        let applied = Arc::new(AtomicBool::new(false));
        let handlers: Vec<Box<dyn RedoHandler>> = vec![Box::new(ProbeHandler {
            saw_pool: Arc::clone(&saw_pool),
            applied: Arc::clone(&applied),
        })];

        // Recovery must replay the HeapInsert record through the probe handler,
        // and the buffer pool must exist at that point.
        let _engine =
            StorageEngine::open_with_redo_handlers(tmp.path(), &config, handlers, Vec::new())
                .unwrap();
        assert!(
            applied.load(Ordering::Relaxed),
            "probe handler was never invoked during recovery"
        );
        assert!(
            saw_pool.load(Ordering::Relaxed),
            "RedoContext.buffer_pool was None during recovery replay"
        );
    }
}
