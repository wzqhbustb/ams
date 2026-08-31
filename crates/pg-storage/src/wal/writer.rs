//! WAL writer with group-commit.
//!
//! The writer owns the LSN clock and the WAL segment manager. It serializes
//! records, writes them to the current WAL segment, and fsyncs them in batches
//! using a dedicated background thread.

use std::path::Path;
use std::sync::Arc;
#[cfg(not(loom))]
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::sync::{Condvar, Mutex};

use crate::config::StorageConfig;
use crate::error::{Result, StorageError};
use crate::lsn_clock::LsnClock;
use crate::types::{Lsn, LSN_ALIGNMENT};
use crate::wal::reader::WalReader;
use crate::wal::record::WalRecord;
use crate::wal::segment::WalSegmentManager;

/// A thread-safe WAL writer.
#[derive(Debug)]
pub struct WalWriter {
    inner: Arc<Mutex<WriterState>>,
    cond: Arc<Condvar>,
    config: StorageConfig,
    handle: Option<JoinHandle<()>>,
}

#[derive(Debug)]
struct WriterState {
    lsn_clock: LsnClock,
    segment_manager: WalSegmentManager,
    synced_lsn: Lsn,
    pending: usize,
    last_flush: Instant,
    /// Set by `flush_to` when a caller is blocked waiting for durability; makes
    /// the worker fsync immediately instead of waiting out the group-commit
    /// batch/timeout thresholds (which would add up to `timeout_ms` of latency
    /// to every synchronous commit — or hang forever if the thresholds are
    /// configured out of reach).
    #[cfg_attr(loom, allow(dead_code))] // only read by the worker / real flush_to
    flush_requested: bool,
    shutdown: bool,
    last_error: Option<String>,
}

impl WalWriter {
    /// Open or create the WAL writer in `{data_dir}/wal`, starting at
    /// [`Lsn::FIRST`].
    ///
    /// If WAL segment files already exist (for example after a crash or a
    /// previous run), the writer scans the durable log and resumes appending
    /// from the byte position immediately after the last complete record. This
    /// prevents recovery code from accidentally overwriting existing WAL.
    ///
    /// # Recycled segments
    ///
    /// Checkpoints may have recycled segments older than the redo point; the
    /// resume scan therefore starts at the oldest segment still on disk (see
    /// `Self::discover_resume_lsn`), so a `wal` directory whose numbering
    /// does not start at segment 0 is handled correctly. Callers that know
    /// the checkpoint redo LSN should prefer
    /// [`Self::open_with_scan_start`]: the oldest retained segment can begin
    /// with the tail bytes of a record that started in a recycled segment,
    /// and only a guaranteed record boundary is a safe scan start.
    pub fn open(data_dir: impl AsRef<Path>, config: &StorageConfig) -> Result<Self> {
        Self::open_with_scan_start(data_dir, config, Lsn::INVALID)
    }

    /// Open like [`Self::open`], but start the resume scan at
    /// `scan_start_hint` when it is valid (e.g. the superblock's checkpoint
    /// redo LSN — a guaranteed record boundary that lies inside the oldest
    /// retained segment). Pass [`Lsn::INVALID`] to fall back to scanning
    /// from the oldest segment on disk.
    pub fn open_with_scan_start(
        data_dir: impl AsRef<Path>,
        config: &StorageConfig,
        scan_start_hint: Lsn,
    ) -> Result<Self> {
        let wal_dir = data_dir.as_ref().join("wal");
        let mut segment_manager = WalSegmentManager::open(&wal_dir, config.wal_segment_size)?;

        let has_existing_records = segment_manager.current_segment_id() > 0
            || segment_manager
                .current_file()
                .metadata()
                .map_err(StorageError::Io)?
                .len()
                > 0;

        let start_lsn = if has_existing_records {
            Self::discover_resume_lsn(&wal_dir, config.wal_segment_size, scan_start_hint)?
        } else {
            Lsn::FIRST
        };

        // A resumed WAL is durable up to `start_lsn`; a WAL with no records
        // (start_lsn still at FIRST — the segment file is preallocated so its
        // length alone does not imply records) has nothing synced yet.
        let initial_synced_lsn = if start_lsn > Lsn::FIRST {
            start_lsn
        } else {
            Lsn::INVALID
        };

        Self::open_with_segment_manager(segment_manager, config, start_lsn, initial_synced_lsn)
    }

    /// Open or create the WAL writer in `{data_dir}/wal`, starting at
    /// `start_lsn`.
    ///
    /// The caller (Stage I recovery) is responsible for ensuring that the WAL
    /// segment files on disk are consistent with `start_lsn`: the latest
    /// existing segment must be the one that contains `start_lsn`. If a later
    /// segment exists, this method returns an error so that recovery can trim
    /// or recycle it first.
    ///
    /// For normal restart, prefer [`Self::open`] which resumes from the end of
    /// the existing WAL automatically.
    pub fn open_at(
        data_dir: impl AsRef<Path>,
        config: &StorageConfig,
        start_lsn: Lsn,
    ) -> Result<Self> {
        if !start_lsn.is_valid() || start_lsn.0 % LSN_ALIGNMENT != 0 {
            return Err(StorageError::InvalidConfig(format!(
                "invalid WAL start LSN {start_lsn}"
            )));
        }

        let wal_dir = data_dir.as_ref().join("wal");
        let segment_manager = WalSegmentManager::open(wal_dir, config.wal_segment_size)?;

        let expected_segment = start_lsn.segment_id(config.wal_segment_size);
        if expected_segment != segment_manager.current_segment_id() {
            return Err(StorageError::InvalidConfig(format!(
                "start_lsn {start_lsn} is in segment {expected_segment} but latest WAL segment is {}; recovery must trim or recycle segments first",
                segment_manager.current_segment_id()
            )));
        }

        Self::open_with_segment_manager(segment_manager, config, start_lsn, start_lsn)
    }

    fn open_with_segment_manager(
        segment_manager: WalSegmentManager,
        config: &StorageConfig,
        start_lsn: Lsn,
        initial_synced_lsn: Lsn,
    ) -> Result<Self> {
        let lsn_clock = LsnClock::new(start_lsn);

        let inner = Arc::new(Mutex::new(WriterState {
            lsn_clock,
            segment_manager,
            // For a resumed WAL, everything on disk up to `start_lsn` is durable
            // by definition (open scans past the last complete record), so the
            // synced position starts there. This lets `flush_to` for a record
            // that predates this open return immediately instead of waiting on
            // the worker — which only advances `synced_lsn` when there are
            // freshly appended `pending` bytes. Recovery relies on this when
            // flushing replayed pages (WAL-before-data) without appending any
            // new WAL. A brand-new (empty) WAL has nothing durable yet, so it
            // passes `Lsn::INVALID`.
            synced_lsn: initial_synced_lsn,
            pending: 0,
            flush_requested: false,
            last_flush: Instant::now(),
            shutdown: false,
            last_error: None,
        }));
        let cond = Arc::new(Condvar::new());

        // The background group-commit worker cannot run inside a loom model
        // (loom only schedules threads it spawned); under `cfg(loom)` there
        // is no worker and `flush_to` completes synchronously inline.
        #[cfg(not(loom))]
        let handle = {
            let inner = Arc::clone(&inner);
            let cond = Arc::clone(&cond);
            let config = config.clone();
            Some(thread::spawn(move || Self::worker(inner, cond, config)))
        };
        #[cfg(loom)]
        let handle: Option<JoinHandle<()>> = None;

        Ok(Self {
            inner,
            cond,
            config: config.clone(),
            handle,
        })
    }

    /// Scan the existing WAL and return the LSN immediately after the last
    /// complete record. Torn or partial records at the tail are discarded: the
    /// returned LSN points to the start of the torn record, allowing the next
    /// append to overwrite it.
    ///
    /// Scan start selection (records may span segment boundaries):
    ///
    /// - `scan_start_hint` valid (the checkpoint redo LSN): use it. It is a
    ///   guaranteed record boundary and lies inside the oldest retained
    ///   segment (`recycle_before` keeps the segment containing it).
    /// - Otherwise: the oldest segment still on disk. Checkpoints recycle
    ///   segments older than the redo point, so segment 0 may be gone;
    ///   starting at [`Lsn::FIRST`] would fail to open the missing file.
    ///
    /// Starting at the oldest segment's *boundary* when a hint is available
    /// is NOT safe: the retained segment can begin with the tail bytes of a
    /// record whose head lived in a recycled segment, and decoding those
    /// orphan bytes either fails or — worse — is mistaken for the torn tail,
    /// truncating every record that follows.
    fn discover_resume_lsn(wal_dir: &Path, segment_size: u64, scan_start_hint: Lsn) -> Result<Lsn> {
        let oldest = WalSegmentManager::discover_oldest_segment_id(wal_dir)?;
        // Segment 0 begins at Lsn::FIRST (Lsn(0) is INVALID); later segments
        // begin at their segment boundary.
        let oldest_start = if oldest == 0 {
            Lsn::FIRST
        } else {
            Lsn(oldest * segment_size)
        };
        let scan_start = if scan_start_hint.is_valid() && scan_start_hint > oldest_start {
            scan_start_hint
        } else {
            oldest_start
        };
        let mut reader = WalReader::open_at(wal_dir, segment_size, scan_start)?;
        while reader.next_record()?.is_some() {}
        Ok(reader.current_lsn())
    }

    /// Append a record to the WAL.
    ///
    /// The record is assigned an LSN and written to the current segment file.
    /// **This method does not fsync**: the record is only guaranteed to be
    /// durable after the caller explicitly invokes [`Self::flush_to`] or
    /// [`Self::flush`]. Concurrent callers are batched into a single fsync by
    /// the background worker.
    ///
    /// Callers that need WAL-before-data ordering (e.g. before flushing a dirty
    /// page) must call `flush_to(lsn)` explicitly.
    pub fn append(&self, mut record: WalRecord) -> Result<Lsn> {
        let mut state = self.inner.lock();
        Self::check_error(&state)?;

        // Validate before touching the clock: a payload exceeding u16::MAX
        // must not leave an unfilled reservation.
        if record.payload.len() > u16::MAX as usize {
            return Err(StorageError::Serialize(format!(
                "payload length {} exceeds u16::MAX",
                record.payload.len()
            )));
        }

        let record_size = record.record_size() as u64;
        let lsn = state.lsn_clock.next(record_size);
        record.lsn = lsn;

        let buf = record.encode()?;
        let cond = Arc::clone(&self.cond);
        write_record_to_segment(&mut state.segment_manager, &buf, lsn).inspect_err(|e| {
            state.last_error = Some(format!("append: segment write failed at {lsn}: {e}"));
            state.shutdown = true;
            cond.notify_all();
        })?;

        state.pending += 1;
        let timeout = Duration::from_millis(self.config.wal_group_commit_timeout_ms);
        let should_wake = state.pending >= self.config.wal_group_commit_batch_size
            || state.last_flush.elapsed() >= timeout;
        if should_wake {
            self.cond.notify_one();
        }
        Ok(lsn)
    }

    /// Append a record at a specific pre-reserved LSN.
    ///
    /// The caller must have already reserved the LSN range via
    /// [`Self::reserve_lsn`]. This method writes the record at exactly `lsn`
    /// without allocating a new one. It is used by the checkpoint coordinator
    /// to emit `CheckpointBegin` after `set_checkpoint_lsn` has already been
    /// called, eliminating the FPI race window.
    ///
    /// Like [`Self::append`], this method does not fsync; the caller must
    /// invoke [`Self::flush_to`] when durability is required.
    ///
    /// # Concurrency
    ///
    /// Other threads may keep appending or reserving LSNs between
    /// [`Self::reserve_lsn`] and this call — this is expected while a fuzzy
    /// checkpoint is in progress (e.g. FPI records from `pin_mut`). That is
    /// safe: `LsnClock` hands out non-overlapping ranges via `fetch_add`, so
    /// the reserved range stays exclusively owned by this caller no matter how
    /// far the clock advances past it.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `lsn` is invalid or unaligned;
    /// - the reserved range `[lsn, lsn + reserved_size)` extends beyond the
    ///   current clock (i.e. it was never reserved);
    /// - the record's encoded size does not match `reserved_size`.
    pub fn append_at(&self, mut record: WalRecord, lsn: Lsn, reserved_size: u64) -> Result<Lsn> {
        if !lsn.is_valid() || lsn.0 % LSN_ALIGNMENT != 0 {
            return Err(StorageError::InvalidConfig(format!(
                "append_at LSN {lsn} is invalid or unaligned"
            )));
        }

        let mut state = self.inner.lock();
        Self::check_error(&state)?;

        // The reserved range [lsn, lsn + reserved_size) must lie entirely
        // within the allocated LSN space. The clock hands out non-overlapping
        // ranges via fetch_add, so any range inside the allocated space cannot
        // overlap a record written by another thread. Concurrent appends may
        // have advanced the clock beyond this range since reserve_lsn; that is
        // expected during a fuzzy checkpoint and must not fail the append.
        if lsn.0 + reserved_size > state.lsn_clock.current().0 {
            return Err(StorageError::LsnNotAvailable(lsn));
        }

        // Validate before encode: a payload exceeding u16::MAX must be caught
        // early so the caller can decide how to handle the reserved LSN gap.
        if record.payload.len() > u16::MAX as usize {
            return Err(StorageError::Serialize(format!(
                "payload length {} exceeds u16::MAX",
                record.payload.len()
            )));
        }

        record.lsn = lsn;
        let buf = record.encode()?;
        if buf.len() as u64 != reserved_size {
            return Err(StorageError::WalWriteFailed(format!(
                "append_at record size {} does not match reserved size {}",
                buf.len(),
                reserved_size
            )));
        }

        let cond = Arc::clone(&self.cond);
        write_record_to_segment(&mut state.segment_manager, &buf, lsn).inspect_err(|e| {
            state.last_error = Some(format!("append_at: segment write failed at {lsn}: {e}"));
            state.shutdown = true;
            cond.notify_all();
        })?;

        state.pending += 1;
        let timeout = Duration::from_millis(self.config.wal_group_commit_timeout_ms);
        let should_wake = state.pending >= self.config.wal_group_commit_batch_size
            || state.last_flush.elapsed() >= timeout;
        if should_wake {
            self.cond.notify_one();
        }
        Ok(lsn)
    }

    /// Flush all records that have been appended up to this point.
    ///
    /// If no records are pending this returns immediately.
    pub fn flush(&self) -> Result<()> {
        let target = {
            let state = self.inner.lock();
            Self::check_error(&state)?;
            if state.pending == 0 {
                return Ok(());
            }
            // synced_lsn is advanced to lsn_clock.current() (the byte
            // immediately following the last written record) on each flush,
            // so waiting for current itself covers every pending byte.
            state.lsn_clock.current()
        };
        self.flush_to(target)
    }

    /// Block until all records with LSN `<= lsn` have been fsynced.
    #[cfg(not(loom))]
    pub fn flush_to(&self, lsn: Lsn) -> Result<()> {
        let mut state = self.inner.lock();
        Self::check_error(&state)?;

        if lsn > state.lsn_clock.current() {
            return Err(StorageError::LsnNotAvailable(lsn));
        }
        if state.synced_lsn >= lsn {
            return Ok(());
        }

        // Ask the worker to fsync immediately rather than waiting for the
        // group-commit batch/timeout thresholds: a blocked flush_to caller is
        // a synchronous durability request (e.g. a commit).
        state.flush_requested = true;
        self.cond.notify_one();

        while state.synced_lsn < lsn {
            Self::check_error(&state)?;
            if state.shutdown {
                return Err(StorageError::WalWriteFailed(
                    "wal writer shut down".to_string(),
                ));
            }
            // Re-assert the request on every iteration: the worker clears
            // `flush_requested` after each fsync wave, and this caller may not
            // have been covered by that wave (e.g. its record landed in a
            // segment rotated past mid-fsync). Without re-asserting, the
            // waiter could sleep until the next group-commit timeout.
            state.flush_requested = true;
            self.cond.notify_one();
            self.cond.wait(&mut state);
        }
        Ok(())
    }

    /// `cfg(loom)` variant of [`Self::flush_to`]: with no background worker
    /// inside a loom model, durability is marked **inline and without any
    /// fsync** — the record bytes are already in the segment file (append
    /// writes them synchronously), and loom models check latch choreography,
    /// not crash durability. This is the stub called out in the `crate::sync`
    /// module docs.
    #[cfg(loom)]
    pub fn flush_to(&self, lsn: Lsn) -> Result<()> {
        let mut state = self.inner.lock();
        Self::check_error(&state)?;
        if lsn > state.lsn_clock.current() {
            return Err(StorageError::LsnNotAvailable(lsn));
        }
        state.synced_lsn = state.synced_lsn.max(lsn);
        state.pending = 0;
        Ok(())
    }

    /// Return the latest LSN that has been fsynced to disk.
    pub fn synced_lsn(&self) -> Lsn {
        self.inner.lock().synced_lsn
    }

    /// Return the current end-of-WAL LSN: the byte immediately following the
    /// last record handed out by the clock, whether or not it is fsynced yet.
    ///
    /// Unlike [`Self::synced_lsn`], this reflects appended-but-not-yet-durable
    /// records. It is the right bound for `checkpoint_lsn` (which gates FPIs on
    /// "modified since the last checkpoint" and does not require durability of
    /// the boundary itself).
    pub fn current_lsn(&self) -> Lsn {
        self.inner.lock().lsn_clock.current()
    }

    /// Reserve a contiguous chunk of LSN space without writing any record.
    ///
    /// This is a convenience wrapper around [`LsnClock::reserve`] that locks
    /// the writer state. The caller must later emit a record at the reserved
    /// LSN via [`Self::append_at`].
    ///
    /// Used by the checkpoint coordinator to pre-allocate the `CheckpointBegin`
    /// LSN so that `set_checkpoint_lsn` can be called before the record is
    /// written, eliminating the FPI race window.
    ///
    /// # Crash-safety note
    ///
    /// If the process crashes between `reserve_lsn` and `append_at`, the
    /// reserved range appears as zeros to recovery, which treats it as
    /// end-of-WAL. Any records written by other threads at higher LSNs after
    /// the reservation are therefore lost. This is acceptable for the
    /// checkpoint use case (in-flight records during checkpoint are not
    /// guaranteed durable) but callers should minimize the reserve→emit
    /// window.
    pub fn reserve_lsn(&self, record_size: u64) -> Result<Lsn> {
        let state = self.inner.lock();
        Self::check_error(&state)?;
        Ok(state.lsn_clock.reserve(record_size))
    }

    /// Reserve an LSN and immediately write a record at that position —
    /// atomically under one lock hold, leaving no zero-filled hole between
    /// reserve and write (the window where a crash would silently truncate
    /// every record after the hole).
    ///
    /// This is the preferred method for callers that need to know the LSN
    /// before emission (e.g. a checkpoint that publishes `begin_lsn` before
    /// the `CheckpointBegin` record is durable). Callers that do not need
    /// the LSN before writing should use [`Self::append`].
    ///
    /// # Poisoning
    ///
    /// If the segment write fails, the writer is **poisoned**: `last_error`
    /// is set so all future operations return an error immediately. This
    /// prevents a process from continuing with a hole in the WAL that would
    /// silently truncate the log on the next recovery.
    pub fn reserve_and_append(&self, mut record: WalRecord) -> Result<Lsn> {
        let mut state = self.inner.lock();
        Self::check_error(&state)?;

        // Validate before touching the clock: a payload exceeding u16::MAX
        // must not leave an unfilled reservation.
        if record.payload.len() > u16::MAX as usize {
            return Err(StorageError::Serialize(format!(
                "payload length {} exceeds u16::MAX",
                record.payload.len()
            )));
        }

        let record_size = record.record_size() as u64;
        let lsn = state.lsn_clock.next(record_size);
        record.lsn = lsn;
        let buf = record.encode()?;
        let cond = Arc::clone(&self.cond);
        write_record_to_segment(&mut state.segment_manager, &buf, lsn).inspect_err(|e| {
            state.last_error = Some(format!(
                "reserve_and_append: segment write failed at {lsn}: {e}"
            ));
            state.shutdown = true;
            cond.notify_all();
        })?;
        state.pending += 1;
        let timeout = Duration::from_millis(self.config.wal_group_commit_timeout_ms);
        let should_wake = state.pending >= self.config.wal_group_commit_batch_size
            || state.last_flush.elapsed() >= timeout;
        if should_wake {
            self.cond.notify_one();
        }
        Ok(lsn)
    }

    /// Recycle WAL segment files whose contents are all before `lsn`.
    ///
    /// The segment that contains `lsn` itself is preserved.
    pub fn recycle_before(&self, lsn: Lsn) -> Result<()> {
        let mut state = self.inner.lock();
        Self::check_error(&state)?;
        if state.shutdown {
            return Err(StorageError::WalWriteFailed(
                "wal writer shut down".to_string(),
            ));
        }
        state.segment_manager.recycle_before(lsn)?;
        Ok(())
    }

    fn check_error(state: &WriterState) -> Result<()> {
        if let Some(ref msg) = state.last_error {
            return Err(StorageError::WalWriteFailed(msg.clone()));
        }
        Ok(())
    }

    #[cfg(not(loom))]
    fn worker(inner: Arc<Mutex<WriterState>>, cond: Arc<Condvar>, config: StorageConfig) {
        let timeout = Duration::from_millis(config.wal_group_commit_timeout_ms);

        loop {
            let mut state = inner.lock();

            if state.shutdown && state.pending == 0 {
                break;
            }

            let should_flush = state.pending > 0
                && (state.shutdown
                    || state.flush_requested
                    || state.pending >= config.wal_group_commit_batch_size
                    || state.last_flush.elapsed() >= timeout);

            if !should_flush && !state.shutdown {
                cond.wait_for(&mut state, timeout);
                continue;
            }

            if state.pending == 0 {
                // Timeout with nothing to do, or woken spuriously.
                if state.shutdown {
                    break;
                }
                continue;
            }

            // Capture what this fsync wave covers, dup the current segment
            // file, then RELEASE the state lock before fsyncing: the fsync
            // (milliseconds on consumer SSDs / macOS F_FULLFSYNC) must not
            // serialize appends behind it — group-commit throughput comes
            // precisely from appends flowing while one fsync covers a whole
            // wave of waiters.
            let cover_lsn = state.lsn_clock.current();
            let cover_segment = state.segment_manager.current_segment_id();
            let cover_pending = state.pending;
            let sync_file = match state.segment_manager.current_file().try_clone() {
                Ok(f) => f,
                Err(e) => {
                    state.last_error = Some(format!("wal dup-for-fsync failed: {e}"));
                    state.shutdown = true;
                    cond.notify_all();
                    break;
                }
            };
            drop(state);

            // Perform the fsync WITHOUT holding the lock. fsync operates on
            // the file (inode), not the fd, so the dup'd handle covers every
            // byte appended through the original before the call. On failure
            // record the error, mark the writer as shut down, and wake all
            // waiters. The worker exits so that it does not retry fsync on a
            // potentially corrupted state; subsequent appends will fail with
            // the recorded error.
            //
            // TODO(M2+): there is no recovery path today. Future work could
            // reopen/rotate the segment or surface the error to the caller for
            // a higher-level decision.
            let fsync_result = sync_file.sync_all();

            let mut state = inner.lock();
            if let Err(e) = fsync_result {
                state.last_error = Some(format!("wal fsync failed: {e}"));
                state.shutdown = true;
                cond.notify_all();
                break;
            }

            // Fsync succeeded: clear any transient error and advance
            // synced_lsn. If appends rotated to a newer segment while we were
            // fsyncing lock-free, only the end of the fsynced segment is
            // provably durable (rotation itself fsyncs the old segment's
            // tail, so that end point is solid); otherwise everything up to
            // `cover_lsn` is durable.
            let durable = if state.segment_manager.current_segment_id() == cover_segment {
                cover_lsn
            } else {
                Lsn((cover_segment + 1) * state.segment_manager.segment_size())
            };
            // Only clear a transient fsync error; never clear a poison set by
            // an append-path failure (which also set shutdown). Clearing it
            // would revive the writer and leave a permanent WAL hole.
            if !state.shutdown {
                state.last_error = None;
            }
            state.synced_lsn = state.synced_lsn.max(durable);
            state.pending -= cover_pending;
            state.flush_requested = false;
            state.last_flush = Instant::now();
            cond.notify_all();
        }
    }
}

impl Drop for WalWriter {
    fn drop(&mut self) {
        {
            let mut state = self.inner.lock();
            state.shutdown = true;
        }
        self.cond.notify_one();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

/// Write a serialized record to the WAL segment(s), handling cross-segment
/// records by splitting the write at the segment boundary.
fn write_record_to_segment(
    segment_manager: &mut WalSegmentManager,
    buf: &[u8],
    lsn: Lsn,
) -> Result<()> {
    use std::io::{Seek, SeekFrom, Write};

    let segment_size = segment_manager.segment_size();
    if buf.len() as u64 > segment_size {
        return Err(StorageError::WalWriteFailed(format!(
            "record size {} exceeds segment size {}",
            buf.len(),
            segment_size
        )));
    }

    let mut written = 0;
    while written < buf.len() {
        let pos = Lsn(lsn.0 + written as u64);
        let offset = pos.segment_offset(segment_size);
        let remaining_in_segment = (segment_size - offset) as usize;
        let chunk = std::cmp::min(remaining_in_segment, buf.len() - written);

        let file = segment_manager.ensure_for_write(pos)?;
        file.seek(SeekFrom::Start(offset))
            .map_err(StorageError::Io)?;
        file.write_all(&buf[written..written + chunk])
            .map_err(StorageError::Io)?;

        written += chunk;

        // If this chunk filled the segment and there is more data, fsync the
        // current segment before switching to the next one.
        if written < buf.len() && offset + chunk as u64 == segment_size {
            file.sync_all().map_err(StorageError::Io)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Read;
    use std::sync::Arc;
    use std::thread;

    use crate::types::PageId;
    use crate::wal::record::WalRecordType;
    use tempfile::TempDir;

    fn writer_config(tmp: &TempDir) -> StorageConfig {
        let mut cfg = StorageConfig::new(tmp.path());
        cfg.wal_group_commit_timeout_ms = 5;
        cfg.wal_group_commit_batch_size = 4;
        cfg.wal_segment_size = 1024;
        cfg
    }

    #[test]
    fn append_and_read_single_record() {
        let tmp = TempDir::new().unwrap();
        let cfg = writer_config(&tmp);
        let writer = WalWriter::open(tmp.path(), &cfg).unwrap();

        let lsn = writer
            .append(WalRecord::page_alloc(PageId(42)).unwrap())
            .unwrap();
        assert!(lsn.is_valid());
        // append() no longer fsyncs; explicit flush_to is required.
        writer.flush_to(lsn).unwrap();
        assert!(writer.synced_lsn() >= lsn);

        // Read the record back from the WAL segment.
        let path = tmp.path().join("wal").join("wal-00000001.log");
        let mut file = fs::File::open(&path).unwrap();
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).unwrap();

        let (decoded, _) =
            WalRecord::decode(&buf[lsn.segment_offset(cfg.wal_segment_size) as usize..]).unwrap();
        assert_eq!(decoded.record_type, WalRecordType::PageAlloc);
        assert_eq!(decoded.lsn, lsn);
    }

    #[test]
    fn append_many_records_preserves_order() {
        let tmp = TempDir::new().unwrap();
        let cfg = writer_config(&tmp);
        let writer = WalWriter::open(tmp.path(), &cfg).unwrap();

        let count = 100;
        let mut lsns = Vec::new();
        for i in 0..count {
            let lsn = writer
                .append(WalRecord::page_alloc(PageId(i + 1)).unwrap())
                .unwrap();
            lsns.push(lsn);
        }

        for window in lsns.windows(2) {
            assert!(window[1] > window[0]);
        }
        // Explicit flush required after batch of appends.
        writer.flush().unwrap();
        assert!(writer.synced_lsn() >= lsns[lsns.len() - 1]);
    }

    #[test]
    fn concurrent_appends_are_monotonic() {
        let tmp = TempDir::new().unwrap();
        let cfg = writer_config(&tmp);
        let writer = Arc::new(WalWriter::open(tmp.path(), &cfg).unwrap());

        let mut handles = Vec::new();
        for i in 0..8 {
            let w = Arc::clone(&writer);
            handles.push(thread::spawn(move || {
                let mut lsns = Vec::new();
                for j in 0..50 {
                    let lsn = w
                        .append(WalRecord::page_alloc(PageId(i * 50 + j + 1)).unwrap())
                        .unwrap();
                    lsns.push(lsn);
                }
                lsns
            }));
        }

        let mut all = Vec::new();
        for h in handles {
            all.extend(h.join().unwrap());
        }

        all.sort_unstable();
        for window in all.windows(2) {
            assert!(window[1] > window[0]);
        }
        // Flush all pending records after concurrent appends.
        writer.flush().unwrap();
    }

    #[test]
    fn cross_segment_write() {
        let tmp = TempDir::new().unwrap();
        let mut cfg = writer_config(&tmp);
        cfg.wal_segment_size = 256;
        cfg.wal_group_commit_batch_size = 1;
        cfg.wal_group_commit_timeout_ms = 1; // flush quickly, but avoid a 0 timeout busy-loop
        let writer = WalWriter::open(tmp.path(), &cfg).unwrap();

        // Build a record that is larger than the remaining space at the end of
        // a segment, so it must be split across two segment files.
        let fpi = WalRecord::full_page_image(PageId(99), vec![0xCD; 32]).unwrap();
        let fpi_size = fpi.record_size() as u64;
        assert!(fpi_size <= cfg.wal_segment_size);

        let small = WalRecord::page_alloc(PageId(1)).unwrap();
        let small_size = small.record_size() as u64;

        // Append small records until the next record would start close enough to
        // the segment boundary that the FPI crosses over.
        let mut last_lsn = writer.append(small.clone()).unwrap();
        loop {
            let offset = last_lsn.segment_offset(cfg.wal_segment_size);
            if offset + small_size + fpi_size > cfg.wal_segment_size
                && offset + fpi_size > cfg.wal_segment_size
            {
                break;
            }
            last_lsn = writer.append(small.clone()).unwrap();
        }

        let fpi_lsn = writer.append(fpi).unwrap();
        // The FPI starts in the same segment as the last small record but its
        // tail crosses the segment boundary, so a second segment file must exist.
        assert_eq!(
            fpi_lsn.segment_id(cfg.wal_segment_size),
            last_lsn.segment_id(cfg.wal_segment_size)
        );
        let fpi_end_segment = Lsn(fpi_lsn.0 + fpi_size - 1).segment_id(cfg.wal_segment_size);
        assert!(fpi_end_segment > fpi_lsn.segment_id(cfg.wal_segment_size));
        assert!(tmp.path().join("wal").join("wal-00000001.log").exists());
        assert!(tmp.path().join("wal").join("wal-00000002.log").exists());
    }

    #[test]
    fn flush_on_empty_writer_returns_immediately() {
        let tmp = TempDir::new().unwrap();
        let cfg = writer_config(&tmp);
        let writer = WalWriter::open(tmp.path(), &cfg).unwrap();
        writer.flush().unwrap();
    }

    #[test]
    fn append_wakes_worker_for_sync() {
        let tmp = TempDir::new().unwrap();
        let mut cfg = writer_config(&tmp);
        cfg.wal_group_commit_timeout_ms = 1000; // long timeout
        cfg.wal_group_commit_batch_size = 100;
        let writer = Arc::new(WalWriter::open(tmp.path(), &cfg).unwrap());

        let w = Arc::clone(&writer);
        let handle = thread::spawn(move || {
            let lsn = w.append(WalRecord::page_alloc(PageId(1)).unwrap()).unwrap();
            w.flush_to(lsn).unwrap();
            lsn
        });

        // flush_to should wake the worker and return once the background fsync
        // completes.
        let lsn = handle.join().unwrap();
        assert!(writer.synced_lsn() >= lsn);
    }

    #[test]
    fn flush_to_is_idempotent_after_sync() {
        let tmp = TempDir::new().unwrap();
        let cfg = writer_config(&tmp);
        let writer = WalWriter::open(tmp.path(), &cfg).unwrap();

        let lsn = writer
            .append(WalRecord::page_alloc(PageId(1)).unwrap())
            .unwrap();
        writer.flush_to(lsn).unwrap();
        // Calling flush_to directly on an already-synced LSN must return
        // immediately without error.
        writer.flush_to(lsn).unwrap();
        assert!(writer.synced_lsn() >= lsn);
    }

    #[test]
    fn open_at_rejects_invalid_and_unaligned_lsn() {
        let tmp = TempDir::new().unwrap();
        let cfg = writer_config(&tmp);
        assert!(WalWriter::open_at(tmp.path(), &cfg, Lsn::INVALID).is_err());
        assert!(WalWriter::open_at(tmp.path(), &cfg, Lsn(12)).is_err());
    }

    #[test]
    fn open_at_rejects_lsn_in_older_segment() {
        let tmp = TempDir::new().unwrap();
        let mut cfg = writer_config(&tmp);
        cfg.wal_segment_size = 64;

        // Pre-create segment 1 so the segment manager discovers it as latest.
        let wal_dir = tmp.path().join("wal");
        fs::create_dir(&wal_dir).unwrap();
        fs::File::create(wal_dir.join("wal-00000002.log")).unwrap();

        // Lsn::FIRST lives in segment 0, but the latest segment is 1.
        assert!(WalWriter::open_at(tmp.path(), &cfg, Lsn::FIRST).is_err());
    }

    #[test]
    fn open_at_accepts_lsn_in_latest_segment() {
        let tmp = TempDir::new().unwrap();
        let mut cfg = writer_config(&tmp);
        cfg.wal_segment_size = 64;
        cfg.wal_group_commit_batch_size = 1;
        cfg.wal_group_commit_timeout_ms = 1;

        // Pre-create segment 1 so the segment manager discovers it as latest.
        let wal_dir = tmp.path().join("wal");
        fs::create_dir(&wal_dir).unwrap();
        fs::File::create(wal_dir.join("wal-00000002.log")).unwrap();

        let writer = WalWriter::open_at(tmp.path(), &cfg, Lsn(64)).unwrap();
        let lsn = writer
            .append(WalRecord::page_alloc(PageId(1)).unwrap())
            .unwrap();
        writer.flush_to(lsn).unwrap();
        assert!(lsn >= Lsn(64));
    }

    #[test]
    fn append_at_writes_to_reserved_lsn() {
        let tmp = TempDir::new().unwrap();
        let cfg = writer_config(&tmp);
        let writer = WalWriter::open(tmp.path(), &cfg).unwrap();

        // Reserve an LSN range for a PageAlloc record. PageAlloc encodes to
        // 40 bytes (32B header + 8B payload), not 32.
        let record = WalRecord::page_alloc(PageId(42)).unwrap();
        let record_size = record.record_size() as u64;
        let reserved = writer.reserve_lsn(record_size).unwrap();
        assert_eq!(reserved, Lsn(8));

        // Emit the record at the reserved LSN.
        let lsn = writer.append_at(record, reserved, record_size).unwrap();
        assert_eq!(lsn, reserved);

        // Explicit flush required.
        writer.flush_to(lsn).unwrap();
        assert!(writer.synced_lsn() >= lsn);

        // The next append should continue from after the reserved range.
        let next = writer
            .append(WalRecord::page_alloc(PageId(43)).unwrap())
            .unwrap();
        assert_eq!(next, Lsn(48));
    }

    #[test]
    fn append_at_rejects_unaligned_lsn() {
        let tmp = TempDir::new().unwrap();
        let cfg = writer_config(&tmp);
        let writer = WalWriter::open(tmp.path(), &cfg).unwrap();

        let record = WalRecord::page_alloc(PageId(1)).unwrap();
        assert!(writer.append_at(record, Lsn(12), 32).is_err());
    }

    #[test]
    fn append_at_rejects_unreserved_future_lsn() {
        let tmp = TempDir::new().unwrap();
        let cfg = writer_config(&tmp);
        let writer = WalWriter::open(tmp.path(), &cfg).unwrap();

        // Lsn(64) is beyond the current clock (Lsn(8)), so it cannot have been
        // reserved.
        let record = WalRecord::page_alloc(PageId(1)).unwrap();
        assert!(writer.append_at(record, Lsn(64), 32).is_err());
    }

    #[test]
    fn append_at_rejects_wrong_size_record() {
        let tmp = TempDir::new().unwrap();
        let cfg = writer_config(&tmp);
        let writer = WalWriter::open(tmp.path(), &cfg).unwrap();

        // Reserve the size of a PageAlloc record but try to write a larger FPI.
        let page_alloc_size = WalRecord::page_alloc(PageId(1)).unwrap().record_size() as u64;
        let reserved = writer.reserve_lsn(page_alloc_size).unwrap();
        let fpi = WalRecord::full_page_image(PageId(1), vec![0xAB; 64]).unwrap();
        assert!(writer.append_at(fpi, reserved, page_alloc_size).is_err());
    }

    #[test]
    fn append_at_succeeds_after_clock_advances() {
        let tmp = TempDir::new().unwrap();
        let cfg = writer_config(&tmp);
        let writer = WalWriter::open(tmp.path(), &cfg).unwrap();

        // Reserve a range, then let the clock advance via further reservations
        // and concurrent-style appends. This mirrors a fuzzy checkpoint:
        // reserve_lsn(CheckpointBegin), FPI/PageAlloc appends from other
        // threads, then append_at on the reserved LSN.
        let record_size = WalRecord::page_alloc(PageId(1)).unwrap().record_size() as u64;
        let reserved = writer.reserve_lsn(record_size).unwrap();
        let _r2 = writer.reserve_lsn(record_size).unwrap();
        let _r3 = writer
            .append(WalRecord::page_alloc(PageId(2)).unwrap())
            .unwrap();

        // Emitting at the originally reserved LSN must still succeed: the
        // range was handed out exclusively and cannot have been overwritten.
        let record = WalRecord::page_alloc(PageId(1)).unwrap();
        let lsn = writer.append_at(record, reserved, record_size).unwrap();
        assert_eq!(lsn, reserved);

        // The record must be readable back at exactly the reserved LSN.
        writer.flush_to(lsn).unwrap();
        let mut reader =
            WalReader::open_at(tmp.path().join("wal"), cfg.wal_segment_size, reserved).unwrap();
        let first = reader
            .next_record()
            .unwrap()
            .expect("record at reserved LSN");
        assert_eq!(first.lsn, reserved);
    }

    #[test]
    fn append_at_cross_segment_write() {
        let tmp = TempDir::new().unwrap();
        let mut cfg = writer_config(&tmp);
        // 96-byte segments: after one 40B PageAlloc (offset 48), a 64B FPI
        // starting at 48 extends to 111, crossing into segment 1.
        cfg.wal_segment_size = 96;
        cfg.wal_group_commit_batch_size = 1;
        cfg.wal_group_commit_timeout_ms = 1;

        let writer = WalWriter::open(tmp.path(), &cfg).unwrap();

        // Write a small record to advance the clock close to the segment
        // boundary.
        let small = WalRecord::page_alloc(PageId(1)).unwrap();
        let last_lsn = writer.append(small.clone()).unwrap();
        writer.flush_to(last_lsn).unwrap();

        // Reserve a range that will cross the segment boundary.
        let fpi = WalRecord::full_page_image(PageId(99), vec![0xCD; 32]).unwrap();
        let fpi_size = fpi.record_size() as u64;
        assert!(fpi_size <= cfg.wal_segment_size);
        let reserved = writer.reserve_lsn(fpi_size).unwrap();
        // The reserved range starts in the current segment but extends into the
        // next one.
        assert_eq!(
            reserved.segment_id(cfg.wal_segment_size),
            last_lsn.segment_id(cfg.wal_segment_size)
        );
        let reserved_end = Lsn(reserved.0 + fpi_size - 1);
        assert!(
            reserved_end.segment_id(cfg.wal_segment_size)
                > reserved.segment_id(cfg.wal_segment_size)
        );

        // append_at must handle the cross-segment write correctly.
        let lsn = writer.append_at(fpi, reserved, fpi_size).unwrap();
        writer.flush_to(lsn).unwrap();
        assert_eq!(lsn, reserved);
        assert!(tmp.path().join("wal").join("wal-00000001.log").exists());
        assert!(tmp.path().join("wal").join("wal-00000002.log").exists());
    }
}
