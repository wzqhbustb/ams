//! In-memory buffer pool for database pages.
//!
//! The buffer pool caches data pages in a fixed-size array of frames. Pages are
//! located through a sharded page table and evicted using the CLOCK algorithm.
//!
//! # Concurrency model
//!
//! - Lookups on the sharded `page_table` can proceed in parallel for different
//!   shards.
//! - Allocating a frame (for a miss or a new page) requires the global
//!   `allocation_lock`, which serializes eviction and page loading. This keeps
//!   the eviction invariant simple and correct for M1.
//! - Once a page is resident, `pin` / `pin_mut` only touch the frame's own
//!   metadata and content locks.
//! - `pin_mut` waits for exclusive access by acquiring the frame content write
//!   lock. The first mutable access after a page is loaded writes a
//!   `FullPageImage` WAL record before returning the guard.
//!
//! # Lock ordering
//!
//! To avoid deadlocks, locks are acquired in one of two compatible orders:
//!
//! - **Hit path**: `page_table[shard]` → `try_lock(Frame::meta)`. If the frame
//!   metadata is locked by an evictor, the hit falls back to the allocation path
//!   instead of blocking.
//! - **Eviction / allocation path**: `allocation_lock` → `try_lock(Frame::meta)`
//!   → (dirty victim: the flush path's `Frame::meta` → `Frame::content.read`)
//!   → `page_table[shard]` (mapping removed only after the victim is durable).
//! - **Flush path**: `Frame::meta` (clear dirty) → `Frame::content.read` →
//!   (on I/O error only) `Frame::meta` (restore dirty). This follows the same
//!   meta-before-content order as eviction. The nested re-acquisition of
//!   `Frame::meta` on the error path is safe because `content.read` is a
//!   shared lock that no other path holds exclusively while waiting for
//!   `Frame::meta`, so no cycle can form.
//!
//! The data file itself is accessed through a lock-free `PositionedFile`
//! (pread/pwrite), so it does not participate in the lock ordering above.
//!
//! The use of `try_lock` on `Frame::meta` from both directions prevents the
//! classic page-table / frame-meta lock-order reversal deadlock.
//!
//! `alloc_frame` is the one place that reads `Frame::content` before taking
//! `Frame::meta` (to initialize the `pd_lsn` cache). This is safe because
//! the frame has just been evicted and is not yet inserted into the page
//! table: no other thread can locate or contend for it. Everywhere else the
//! two locks are either held in one of the orders above or never nested.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use crate::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use crate::sync::{Condvar, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::config::StorageConfig;
use crate::error::{Result, StorageError};
use crate::page::{page_pd_lsn, set_page_pd_lsn};
use crate::page_allocator::PageAllocator;
use crate::positioned_file::PositionedFile;
use crate::types::{FrameId, Lsn, PageId, PAGE_SIZE};
use crate::wal::record::WalRecord;
use crate::wal::writer::WalWriter;

/// Metadata for a buffer pool frame.
#[derive(Debug)]
struct FrameMeta {
    /// The page currently stored in this frame, or [`PageId::INVALID`] if empty.
    page_id: PageId,
    /// Number of active pins (read or write) on this frame.
    pin_count: u32,
    /// Whether the frame has been modified since it was read from disk.
    dirty: bool,
    /// CLOCK reference bit.
    reference: bool,
    /// Read-only cache of the page's `pd_lsn` (`page[0..8]`), kept in sync by
    /// the FPI path in `pin_mut` and refreshed from page content on load. The
    /// authoritative value lives in the page itself; readers that need
    /// correctness (e.g. `flush_frame`) must read `page[0..8]` directly.
    cached_lsn: Lsn,
    /// ARIES recovery LSN (`rec_lsn`, tech-selection §11.1): the LSN at which
    /// this page was first dirtied since it was last flushed. [`Lsn::INVALID`]
    /// iff the frame is clean or the first-dirty LSN is unknown (a freshly
    /// allocated page whose caller never stamped a WAL LSN). Set by every
    /// path that transitions the frame from clean to dirty — the FPI branch
    /// of `pin_mut` (the FPI LSN *is* the cycle's first modification) and
    /// `PageGuardMut::drop` (the page's `pd_lsn` at drop; see there for the
    /// approximation argument) — and reset to `INVALID` by `flush_frame`
    /// atomically with the dirty flag, so the checkpoint's DPT snapshot
    /// ([`BufferPool::dirty_page_snapshot`]) never pairs a dirty page with a
    /// stale rec_lsn.
    first_dirty_lsn: Lsn,
    /// True if the page has an on-disk image that a torn write could corrupt,
    /// so the next modification must be preceded by a `FullPageImage` record.
    /// Set on load-from-disk and after any successful `flush_frame` (a resident
    /// page flushed in place at a checkpoint now has an on-disk version);
    /// cleared only for freshly allocated pages that have never been flushed.
    /// Whether an FPI is actually written is additionally gated in `pin_mut` by
    /// `pd_lsn < checkpoint_lsn` (i.e. not yet modified in this checkpoint cycle).
    needs_fpi: bool,
    /// True if this frame is in the process of being evicted. New pins must
    /// reject the frame even if `page_id` still matches.
    evicting: bool,
    /// True while a `flush_frame` call is between claiming the dirty epoch
    /// (dirty cleared) and completing the durability decision (fsync done,
    /// or the group-fsync coalescing check confirmed coverage). A second
    /// flush caller that observes a CLEAN page while this is set must NOT
    /// return early: the first flush's write/fsync may still be in flight,
    /// and a caller with a durability contract (B+Tree `split_copy`'s
    /// right-page flush) would otherwise release its left-page latch before
    /// the right page is truly durable (Stage Q review H1). Waiters block
    /// on [`BufferPool::flush_done`].
    flushing: bool,
}

impl Default for FrameMeta {
    fn default() -> Self {
        Self {
            page_id: PageId::INVALID,
            pin_count: 0,
            dirty: false,
            reference: false,
            cached_lsn: Lsn::INVALID,
            first_dirty_lsn: Lsn::INVALID,
            needs_fpi: false,
            evicting: false,
            flushing: false,
        }
    }
}

/// A single slot in the buffer pool.
#[derive(Debug)]
pub struct Frame {
    /// Mutable frame metadata.
    meta: Mutex<FrameMeta>,
    /// Page content. Write access is exclusive; read access may be shared.
    content: RwLock<[u8; PAGE_SIZE]>,
}

impl Default for Frame {
    fn default() -> Self {
        Self {
            meta: Mutex::new(FrameMeta::default()),
            content: RwLock::new([0u8; PAGE_SIZE]),
        }
    }
}

/// In-memory cache of database pages.
#[derive(Debug)]
pub struct BufferPool {
    config: StorageConfig,
    data_file: PositionedFile,
    page_allocator: Arc<Mutex<PageAllocator>>,
    wal_writer: Arc<WalWriter>,
    page_table: Vec<Mutex<HashMap<PageId, FrameId>>>,
    frames: Vec<Frame>,
    clock_hand: AtomicUsize,
    /// Serializes frame allocation / eviction.
    allocation_lock: Mutex<()>,
    /// LSN of the most recent checkpoint begin. Used to decide whether a page
    /// needs a Full Page Image before its first modification in the current
    /// checkpoint cycle.
    checkpoint_lsn: AtomicU64,
    /// Monotonically increasing generation bumped after each `write_all_at` in
    /// `flush_frame`. Used together with `synced_gen` for group-fsync coalescing.
    #[cfg_attr(loom, allow(dead_code))] // only read by the real `flush_frame`
    flush_gen: AtomicU64,
    /// Generation value as of the most recent completed `sync_all`. Writers
    /// whose `flush_gen` ≤ `synced_gen` can skip their own fsync because a
    /// later sync already covered their write.
    #[cfg_attr(loom, allow(dead_code))] // only read by the real `flush_frame`
    synced_gen: AtomicU64,
    /// Signalled when a frame's `flushing` flag clears (see
    /// [`FrameMeta::flushing`]): concurrent flush callers with a durability
    /// contract wait here for the in-flight flush to complete.
    #[cfg_attr(loom, allow(dead_code))] // only waited on by the real `flush_frame`
    flush_done: Condvar,
}

impl BufferPool {
    /// Open or create the buffer pool.
    ///
    /// `page_allocator` and `wal_writer` are shared with the caller. The buffer
    /// pool opens its own file descriptor to the data file for reads and writes.
    pub fn open(
        data_dir: impl AsRef<Path>,
        config: &StorageConfig,
        page_allocator: Arc<Mutex<PageAllocator>>,
        wal_writer: Arc<WalWriter>,
    ) -> Result<Self> {
        config.validate()?;

        crate::io::ensure_data_dir(data_dir.as_ref())?;
        let data_file_path = crate::io::data_file_path(data_dir.as_ref());
        let data_file = PositionedFile::open(&data_file_path)?;

        let frame_count = config.buffer_pool_size / config.page_size();
        let frames: Vec<Frame> = (0..frame_count).map(|_| Frame::default()).collect();

        let shards = config.buffer_pool_shards;
        let page_table: Vec<Mutex<HashMap<PageId, FrameId>>> =
            (0..shards).map(|_| Mutex::new(HashMap::new())).collect();

        Ok(Self {
            config: config.clone(),
            data_file,
            page_allocator,
            wal_writer,
            page_table,
            frames,
            clock_hand: AtomicUsize::new(0),
            allocation_lock: Mutex::new(()),
            checkpoint_lsn: AtomicU64::new(Lsn::INVALID.0),
            flush_gen: AtomicU64::new(0),
            synced_gen: AtomicU64::new(0),
            flush_done: Condvar::new(),
        })
    }

    /// Update the checkpoint LSN used to decide FPI requirements.
    ///
    /// Called by `CheckpointCoordinator` immediately after writing a
    /// `CheckpointBegin` record.
    pub fn set_checkpoint_lsn(&self, lsn: Lsn) {
        self.checkpoint_lsn.store(lsn.0, Ordering::Release);
    }

    /// Return the current checkpoint LSN.
    pub fn checkpoint_lsn(&self) -> Lsn {
        Lsn(self.checkpoint_lsn.load(Ordering::Acquire))
    }

    /// Pin a page for read access.
    ///
    /// If the page is not resident, it is read from disk into a frame. The
    /// returned guard keeps the page pinned until it is dropped.
    pub fn pin(&self, page_id: PageId) -> Result<PageGuard<'_>> {
        if page_id == PageId::INVALID {
            return Err(StorageError::InvalidConfig(
                "cannot pin PageId::INVALID".to_string(),
            ));
        }

        // `locate_or_load` returns a frame that is already pinned and referenced.
        let frame_id = self.locate_or_load(page_id)?;

        let content_guard = self.frames[frame_id.0].content.read();

        Ok(PageGuard {
            frame_id,
            page_id,
            content_guard: Some(content_guard),
            pool: self,
        })
    }

    /// Pin a page for write access.
    ///
    /// The first mutable access after a page is loaded writes a
    /// `FullPageImage` WAL record before the guard is returned. Subsequent
    /// modifications within the same residency do not write additional FPIs.
    pub fn pin_mut(&self, page_id: PageId) -> Result<PageGuardMut<'_>> {
        let mut guard = self.pin_mut_without_fpi(page_id)?;
        self.ensure_fpi(&mut guard)?;
        Ok(guard)
    }

    /// Emit the page's per-checkpoint-cycle `FullPageImage` under an
    /// already-held write guard, if the FPI gate says one is due.
    ///
    /// This is the FPI block of [`Self::pin_mut`], split out so a caller can
    /// inspect the page under the write latch BEFORE the FPI is emitted.
    /// The B+Tree leaf write path uses this to SKIP the FPI for a page
    /// whose split Commit is in flight: an FPI emitted for a
    /// `SPLIT_INCOMPLETE` page in the (Commit append, Commit apply) window
    /// would capture a pre-commit image at a WAL position after the Commit
    /// record, and recovery's unconditional FPI replay would roll the page
    /// back past the Commit (Stage T P0; see the pg-am-btree module doc).
    /// The skip is redo-safe by the same argument as
    /// [`Self::pin_mut_without_fpi`].
    ///
    /// Must be called before the caller's first modification (and before its
    /// WAL record is appended): the image must reflect the exact state just
    /// before this modification. The gate is the same one `pin_mut` uses, so
    /// a second call in the same checkpoint cycle is a no-op.
    pub fn ensure_fpi(&self, guard: &mut PageGuardMut<'_>) -> Result<()> {
        let frame_id = guard.frame_id;
        let content_guard = guard.content_guard.as_mut().expect("guard is active");

        // Same gate as `pin_mut` (see there for the full rationale):
        // `needs_fpi` means the page has an on-disk image, and `pd_lsn <
        // checkpoint_lsn` means it has not yet been modified in the current
        // checkpoint cycle.
        let needs_fpi = { self.frames[frame_id.0].meta.lock().needs_fpi };
        let checkpoint_lsn = self.checkpoint_lsn();
        let page_lsn = page_pd_lsn(&content_guard[..]);
        if !(needs_fpi && checkpoint_lsn.is_valid() && page_lsn < checkpoint_lsn) {
            return Ok(());
        }

        let image = content_guard.to_vec();
        let fpi_record = WalRecord::full_page_image(guard.page_id, image)?;
        let fpi_lsn = self.wal_writer.append(fpi_record)?;

        // Publish the FPI LSN into the page itself (authoritative) and mirror
        // it into the frame cache. The FPI image keeps the *old* pd_lsn;
        // recovery patches it to the record's own LSN.
        set_page_pd_lsn(&mut content_guard[..], fpi_lsn);
        let mut meta = self.frames[frame_id.0].meta.lock();
        meta.cached_lsn = fpi_lsn;
        // DPT anchor (§11.1): the FPI LSN is this checkpoint cycle's first
        // modification of the page. Only set it when no first-dirty LSN is
        // recorded yet: a page re-dirtied after a mid-cycle flush keeps the
        // (older, conservative) anchor of its current dirty epoch.
        if meta.first_dirty_lsn == Lsn::INVALID {
            meta.first_dirty_lsn = fpi_lsn;
        }
        meta.dirty = true;
        Ok(())
    }

    /// Pin a page for write access WITHOUT emitting a `FullPageImage`, even
    /// if the per-checkpoint-cycle FPI gate says one is due.
    ///
    /// # Contract (FPI-before-commit, Stage T P0)
    ///
    /// The caller MUST guarantee one of:
    ///
    /// - the page's cycle FPI — if one was due — was already emitted earlier
    ///   in this checkpoint cycle at a WAL position preceding every record
    ///   this modification belongs to (the B+Tree split Commit pre-touches
    ///   the page with a regular `pin_mut` BEFORE the `BTreeSplitCommit`
    ///   record's WAL position is fixed, then re-pins here to apply the
    ///   commit's page effects; a regular `pin_mut` at re-pin time could
    ///   fire a SECOND FPI if a checkpoint published in the pre-touch →
    ///   re-pin window, capturing a pre-commit image after the Commit
    ///   record); or
    /// - the caller will run [`Self::ensure_fpi`] under the guard before
    ///   modifying the page (the B+Tree leaf write path checks
    ///   `SPLIT_INCOMPLETE` between the two and skips the FPI for a page
    ///   whose split Commit is in flight — its in-window modification is
    ///   redo-safe per the argument below); or
    /// - the caller establishes by AM-level means that no cycle FPI is owed
    ///   for this hold AND no stale image can result (same argument).
    ///
    /// Suppressing a due FPI is redo-safe in these shapes because the
    /// modifying record's LSN exceeds any intervening checkpoint's begin
    /// LSN, so replay from that checkpoint re-applies it under the usual
    /// `pd_lsn` guard. The residual exposure is torn-write-only — and for
    /// the split-Commit re-pin, the same class as the accepted Stage B
    /// publication window; kill -9 testing cannot tear a page-cache pwrite.
    pub fn pin_mut_without_fpi(&self, page_id: PageId) -> Result<PageGuardMut<'_>> {
        if page_id == PageId::INVALID {
            return Err(StorageError::InvalidConfig(
                "cannot pin_mut PageId::INVALID".to_string(),
            ));
        }

        // `locate_or_load` returns a frame that is already pinned and referenced.
        let frame_id = self.locate_or_load(page_id)?;

        // Acquire the write lock before any FPI/modification so the page
        // state observed reflects exactly this hold.
        let content_guard = self.frames[frame_id.0].content.write();

        // Mark the frame dirty NOW, at pin time — not at guard drop.
        // pin_mut means write intent, and a fuzzy checkpoint that collects
        // `dirty_page_ids()` while this guard is still held MUST see the
        // page: its WAL record may already sit before the checkpoint's
        // begin_lsn while the dirty flag only appears when the guard drops
        // after the collection — the page would be neither flushed nor in
        // the DPT snapshot, and its pre-begin record would fall behind the
        // redo point, silently losing the update on crash. A false positive
        // (guard dropped unmodified) costs one extra page flush, which is
        // safe. `first_dirty_lsn` keeps its drop-time semantics: the
        // modifying record's LSN only exists once the AM has appended it.
        // (content.write → meta is the sanctioned nesting order, same as
        // the FPI block in `ensure_fpi`.)
        self.frames[frame_id.0].meta.lock().dirty = true;

        Ok(PageGuardMut {
            frame_id,
            page_id,
            content_guard: Some(content_guard),
            pool: self,
        })
    }

    /// Allocate a new page and return it pinned for writing.
    ///
    /// The page content is zero-filled. A new page does not need an FPI
    /// because there is no previous on-disk version to restore.
    pub fn new_page(&self) -> Result<PageGuardMut<'_>> {
        let page_id = {
            let mut allocator = self.page_allocator.lock();
            allocator.alloc_page()?
        };

        let frame_id = self.alloc_frame(page_id, false)?;

        {
            let mut meta = self.frames[frame_id.0].meta.lock();
            // New pages have no on-disk previous version, so they do not need an
            // FPI before the first modification. They are already pinned and
            // referenced by alloc_frame.
            meta.needs_fpi = false;
            meta.dirty = true;
        }

        let content_guard = self.frames[frame_id.0].content.write();
        Ok(PageGuardMut {
            frame_id,
            page_id,
            content_guard: Some(content_guard),
            pool: self,
        })
    }

    /// Flush a dirty page to disk.
    ///
    /// Ensures WAL is fsynced to `frame.page_lsn` before writing the page.
    pub fn flush(&self, page_id: PageId) -> Result<()> {
        if page_id == PageId::INVALID {
            return Err(StorageError::InvalidConfig(
                "cannot flush PageId::INVALID".to_string(),
            ));
        }

        let frame_id = {
            let shard_idx = self.shard_index(page_id);
            let shard = self.page_table[shard_idx].lock();
            *shard
                .get(&page_id)
                .ok_or(StorageError::PageNotFound(page_id))?
        };

        self.flush_frame(frame_id)?;
        Ok(())
    }

    /// Force the pooled image of `page_id` back to its on-disk contents and
    /// return the reloaded page's `pd_lsn` (redo-repair only).
    ///
    /// Mid-replay, a page's POOLED image can legitimately lag its on-disk
    /// image: unconditional full-page-image replay restores an old image
    /// (e.g. one from the page's PREVIOUS identity before freelist
    /// recycling — M3 vacuum is the first freelist producer) and forward
    /// replay has only rebuilt the page up to the record currently being
    /// replayed. A redo handler whose record cannot be reconstructed from
    /// the pooled state (the B+Tree split `Copy` recomputes the moved
    /// entries from the LEFT page's pre-copy image, which is gone once the
    /// pooled left page is past the copy) uses this to adopt the durable
    /// on-disk state instead of declaring corruption — safe exactly when
    /// the online protocol guarantees the on-disk image is new enough
    /// (for `Copy`: the right page's post-copy image is flushed before the
    /// left page's latch is released, so `left durable post-copy` implies
    /// `right durable post-copy`).
    ///
    /// A page with no full on-disk image (beyond / partially at the data
    /// file's tail) yields `Ok(Lsn::INVALID)` and leaves the frame
    /// untouched: "no image" is a legitimate answer — the caller's guard
    /// then decides what that means (for `Copy`: genuine corruption).
    ///
    /// The caller must hold no latch on the page. Single-threaded redo
    /// only: no coordination with concurrent accessors beyond the frame's
    /// own locks is provided.
    pub fn force_reload_from_disk(&self, page_id: PageId) -> Result<Lsn> {
        debug_assert!(
            page_id != PageId::INVALID,
            "INVALID has no on-disk image (same contract as read_page_from_disk)"
        );
        let offset = (page_id.0 - 1) * self.config.page_size() as u64;
        if offset + self.config.page_size() as u64 > self.data_file.len()? {
            return Ok(Lsn::INVALID);
        }
        let frame_id = {
            let shard_idx = self.shard_index(page_id);
            let shard = self.page_table[shard_idx].lock();
            *shard
                .get(&page_id)
                .ok_or(StorageError::PageNotFound(page_id))?
        };
        self.read_page_from_disk(page_id, frame_id)?;
        let pd = {
            let content = self.frames[frame_id.0].content.read();
            page_pd_lsn(&content[..])
        };
        {
            let mut meta = self.frames[frame_id.0].meta.lock();
            // Memory now equals the durable disk image — adopt the same
            // per-field state `alloc_frame`'s load-from-disk path
            // establishes: not dirty (a stale regressed image must never
            // be flushed over the newer disk state), no dirty-era anchor,
            // the cached pd_lsn mirror refreshed, and `needs_fpi = true`
            // so the next modification in a later checkpoint cycle owes
            // an FPI.
            meta.dirty = false;
            meta.first_dirty_lsn = Lsn::INVALID;
            meta.cached_lsn = pd;
            meta.needs_fpi = true;
        }
        Ok(pd)
    }

    /// Return the number of frames in the pool.
    pub fn frame_count(&self) -> usize {
        self.frames.len()
    }

    /// Return the configured number of page-table shards.
    pub fn shard_count(&self) -> usize {
        self.page_table.len()
    }

    /// Test-only accessor for the cached `pd_lsn` of a resident frame.
    ///
    /// Returns `None` if the page is not currently in the pool. The cached
    /// value is a read-only mirror of `page[0..8]`; tests use it to check
    /// cache/page consistency and frame residency.
    #[cfg(test)]
    fn frame_cached_lsn(&self, page_id: PageId) -> Option<Lsn> {
        let shard_idx = self.shard_index(page_id);
        let frame_id = *self.page_table[shard_idx].lock().get(&page_id)?;
        let meta = self.frames[frame_id.0].meta.lock();
        Some(meta.cached_lsn)
    }

    /// Return the page IDs of all currently dirty frames.
    ///
    /// Intended for Stage I's checkpoint coordinator. The returned list is a
    /// snapshot at the time of the call; pages may become clean or dirty again
    /// before the caller observes them. The caller must pin each page before
    /// flushing to observe a consistent state.
    pub fn dirty_page_ids(&self) -> Vec<PageId> {
        self.frames
            .iter()
            .filter_map(|frame| {
                let meta = frame.meta.lock();
                if meta.dirty && meta.page_id != PageId::INVALID {
                    Some(meta.page_id)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Return `(page_id, rec_lsn)` for every currently dirty frame whose
    /// first-dirty LSN is known (M2b Stage N; tech-selection §11.1/§11.4).
    ///
    /// This is the buffer pool's contribution to the checkpoint's Dirty Page
    /// Table snapshot. Frames whose `first_dirty_lsn` is [`Lsn::INVALID`] —
    /// freshly allocated pages whose writer never stamped a WAL LSN — are
    /// filtered out: with no known rec_lsn there is no WAL position to anchor
    /// them at, and their `PageAlloc`/content records are picked up by the
    /// recovery WAL scan from the checkpoint LSN regardless.
    ///
    /// Like [`dirty_page_ids`](Self::dirty_page_ids), the result is a point-in
    /// time snapshot; frames may be flushed or re-dirtied immediately after.
    pub fn dirty_page_snapshot(&self) -> Vec<(PageId, Lsn)> {
        self.frames
            .iter()
            .filter_map(|frame| {
                let meta = frame.meta.lock();
                if meta.dirty && meta.page_id != PageId::INVALID && meta.first_dirty_lsn.is_valid()
                {
                    Some((meta.page_id, meta.first_dirty_lsn))
                } else {
                    None
                }
            })
            .collect()
    }

    fn shard_index(&self, page_id: PageId) -> usize {
        (page_id.0 as usize) % self.page_table.len()
    }

    /// Locate an existing resident page or load it from disk.
    fn locate_or_load(&self, page_id: PageId) -> Result<FrameId> {
        // Fast path: page is already resident.
        if let Some(frame_id) = self.try_pin_resident(page_id) {
            return Ok(frame_id);
        }

        // Slow path: allocate a frame and read from disk.
        self.alloc_frame(page_id, true)
    }

    /// Try to pin a page that is already resident.
    ///
    /// Returns `Some(frame_id)` if the page was found and pinned. Returns
    /// `None` if the page is not resident, is being evicted, or the frame lock
    /// is contended.
    fn try_pin_resident(&self, page_id: PageId) -> Option<FrameId> {
        let shard_idx = self.shard_index(page_id);
        let shard = self.page_table[shard_idx].lock();
        let frame_id = *shard.get(&page_id)?;

        // Do not block on the frame lock. If an evictor holds it, fall back to
        // the allocation path which serializes with eviction.
        let mut meta = self.frames[frame_id.0].meta.try_lock()?;
        if meta.page_id != page_id || meta.evicting {
            return None;
        }

        meta.pin_count += 1;
        meta.reference = true;
        Some(frame_id)
    }

    /// Allocate a frame for `page_id`.
    ///
    /// Requires the global allocation lock. If `load_from_disk` is true, the
    /// page content is read from the data file; otherwise the frame is left
    /// zero-filled.
    fn alloc_frame(&self, page_id: PageId, load_from_disk: bool) -> Result<FrameId> {
        let _alloc = self.allocation_lock.lock();

        // Double-check: another thread may have loaded the page while we were
        // waiting for the allocation lock.
        //
        // `try_pin_resident` fails *spuriously* when the frame's meta lock is
        // momentarily contended — the page may well be resident. Proceeding
        // to `shard.insert` in that case would overwrite the live mapping and
        // create a second frame for the same page_id (two copies of the page
        // evolving independently: duplicate slots, lost writes). So before
        // allocating a new frame we consult the page table itself:
        //
        // - Page present in the table: it is resident and will become
        //   pinnable as soon as the meta lock is released. Eviction cannot be
        //   in flight (`evict_frame` runs entirely under this same
        //   allocation lock), and the current meta holder never needs the
        //   allocation lock to release, so this retry loop terminates.
        // - Page absent from the table: nobody else can insert a mapping
        //   (all inserts happen under this lock), so it is safe to allocate.
        loop {
            if let Some(frame_id) = self.try_pin_resident(page_id) {
                return Ok(frame_id);
            }
            let shard_idx = self.shard_index(page_id);
            if !self.page_table[shard_idx].lock().contains_key(&page_id) {
                break;
            }
            std::thread::yield_now();
        }

        let frame_id = self.evict_frame()?;

        // Reset frame content for a new page.
        {
            let mut content = self.frames[frame_id.0].content.write();
            content.fill(0);
        }

        if load_from_disk {
            self.read_page_from_disk(page_id, frame_id)?;
        }

        // Read the page's pd_lsn before touching frame metadata: the two
        // locks are taken sequentially, never nested (pin_mut is the one
        // place that legitimately nests them, in content → meta order). The
        // frame is not yet visible in the page table, so no other thread can
        // contend for it.
        let cached_lsn = {
            let content = self.frames[frame_id.0].content.read();
            page_pd_lsn(&content[..])
        };

        {
            let mut meta = self.frames[frame_id.0].meta.lock();
            meta.page_id = page_id;
            // The caller is responsible for filling content; the frame is pinned
            // and referenced before we return.
            meta.pin_count = 1;
            meta.reference = true;
            meta.dirty = false;
            // The page's pd_lsn is authoritative; cache a copy in the frame.
            // A fresh (zeroed) page yields Lsn::INVALID, matching M1 semantics.
            meta.cached_lsn = cached_lsn;
            // A freshly loaded frame is clean, so it has no rec_lsn yet.
            meta.first_dirty_lsn = Lsn::INVALID;
            meta.needs_fpi = true;
        }

        {
            let shard_idx = self.shard_index(page_id);
            let mut shard = self.page_table[shard_idx].lock();
            shard.insert(page_id, frame_id);
        }

        Ok(frame_id)
    }

    /// Select a victim frame using CLOCK and evict it.
    fn evict_frame(&self) -> Result<FrameId> {
        let frame_count = self.frames.len();
        if frame_count == 0 {
            return Err(StorageError::BufferPoolFull);
        }

        let max_scans = frame_count * 2;
        for _ in 0..max_scans {
            let hand = self.clock_hand.fetch_add(1, Ordering::Relaxed) % frame_count;

            let mut meta = match self.frames[hand].meta.try_lock() {
                Some(m) => m,
                None => continue,
            };

            if meta.pin_count > 0 || meta.evicting {
                continue;
            }

            if meta.reference {
                meta.reference = false;
                continue;
            }

            // Mark the frame as evicting so new pins reject it even though
            // the page table entry is still visible.
            let old_page_id = meta.page_id;
            let dirty = meta.dirty;
            meta.evicting = true;
            drop(meta);

            // Flush BEFORE removing the page-table mapping (Stage Q final
            // review): the mapping must survive until the dirty content is
            // durable. A concurrent `flush(page)` with a durability
            // contract (B+Tree split_copy's right page) then either finds
            // the mapping and waits out THIS flush via the H1 flush_done
            // handshake, or observes PageNotFound only after this flush has
            // completed — never in the window between mapping removal and
            // fsync. Keeping the mapping meanwhile is harmless: `evicting`
            // rejects new pins.
            if dirty && old_page_id != PageId::INVALID {
                if let Err(e) = self.flush_frame(FrameId(hand)) {
                    // On I/O failure keep the frame usable: clear `evicting`
                    // and leave the mapping in place, so the page is NOT
                    // leaked with its dirty content (previously the mapping
                    // was dropped before the flush and a failed flush
                    // leaked the frame for good — Stage N leftover).
                    self.frames[hand].meta.lock().evicting = false;
                    return Err(e);
                }
            }

            // Remove the old mapping from the page table (only now is the
            // old tenant durable or clean).
            if old_page_id != PageId::INVALID {
                let shard_idx = self.shard_index(old_page_id);
                let mut shard = self.page_table[shard_idx].lock();
                shard.remove(&old_page_id);
            }

            // Reset metadata. The content will be initialized by the caller.
            {
                let mut meta = self.frames[hand].meta.lock();
                *meta = FrameMeta::default();
            }

            return Ok(FrameId(hand));
        }

        Err(StorageError::BufferPoolFull)
    }

    /// Read a page from disk into `frame_id`.
    ///
    /// `page_id` is 1-indexed: page 1 lives at offset 0, page 2 at offset
    /// `PAGE_SIZE`, etc. The data file does not reserve any space for
    /// `PageId(0)`, which is reserved as the invalid sentinel.
    fn read_page_from_disk(&self, page_id: PageId, frame_id: FrameId) -> Result<()> {
        let offset = (page_id.0 - 1) * self.config.page_size() as u64;
        let mut content = self.frames[frame_id.0].content.write();
        self.data_file.read_exact_at(&mut *content, offset)?;
        Ok(())
    }

    /// Flush a single frame to disk if it is dirty.
    ///
    /// **Dirty flag protocol (PG-style clear-before-write)**: `meta.dirty` is
    /// cleared *before* the write begins. If a concurrent `pin_mut` modifies
    /// the page while this flush is in progress, it will re-set `dirty = true`,
    /// ensuring the next checkpoint picks up the newer version. On I/O error
    /// the flag is restored so the page is retried later.
    ///
    /// **WAL-before-data invariant**: the content read lock is held from the
    /// moment we sample `pd_lsn` through `flush_to` and `write_all_at`. This
    /// prevents a concurrent `pin_mut` from advancing `pd_lsn` between the WAL
    /// flush and the data write.
    ///
    /// `sync_all` is issued **after** releasing `content.read()`. This is safe
    /// because:
    /// - During eviction the frame is marked `evicting = true`, which prevents
    ///   new `pin_mut` calls from targeting it.
    /// - `fsync` flushes all prior writes to the inode regardless of whether
    ///   the content lock is still held.
    ///
    /// **Group-fsync coalescing**: multiple concurrent flushes share a single
    /// `fsync` via the `flush_gen` / `synced_gen` atomic pair. A flusher that
    /// observes `synced_gen >= its_gen` knows a concurrent fsync — one that
    /// started after this thread's `write_all_at` returned — already made the
    /// write durable, so it can skip its own syscall.
    /// **In-flight flush tracking (Stage Q review H1)**: claiming the dirty
    /// epoch sets `meta.flushing`; it clears only AFTER the durability
    /// decision (fsync completed, or group-fsync coalescing confirmed a
    /// covering sync). A concurrent caller that finds the page clean must
    /// first wait out any in-flight flush — otherwise `split_copy`'s
    /// right-page flush could return while another flusher's write/fsync is
    /// still in flight, releasing the left-page latch before the right page
    /// is truly durable (a power loss then exposes the unrecoverable
    /// left-past-copy / right-missing state to redo).
    #[cfg(not(loom))]
    fn flush_frame(&self, frame_id: FrameId) -> Result<()> {
        let (page_id, saved_first_dirty_lsn) = {
            let mut meta = self.frames[frame_id.0].meta.lock();
            // Wait out an in-flight flush BEFORE judging cleanliness: the
            // dirty flag was cleared at claim time, so "clean" can still
            // mean "another flusher's write/fsync is in progress".
            while meta.flushing {
                self.flush_done.wait(&mut meta);
            }
            if !meta.dirty || meta.page_id == PageId::INVALID {
                return Ok(());
            }
            meta.flushing = true;
            meta.dirty = false;
            // Clear the rec_lsn atomically with the dirty flag (§11.1): the
            // dirty epoch this flush makes durable ends here. A guard that
            // re-dirties the page during the flush installs a fresh anchor;
            // on error we restore the saved one only if no newer anchor
            // appeared, mirroring the clear-before-write dirty protocol.
            let saved = meta.first_dirty_lsn;
            meta.first_dirty_lsn = Lsn::INVALID;
            (meta.page_id, saved)
        };

        // Hold content.read across WAL flush + data write (WAL-before-data).
        let content = self.frames[frame_id.0].content.read();
        let page_lsn = page_pd_lsn(&content[..]);
        // Skip the WAL flush when the page's LSN exceeds the clock: the LSN
        // came from recovery replay (not a live append), so the WAL record
        // is already durable on disk. `flush_to` would reject it anyway
        // (LsnNotAvailable — the clock was never advanced to this LSN).
        if page_lsn.is_valid()
            && page_lsn <= self.wal_writer.current_lsn()
            && self.wal_writer.synced_lsn() < page_lsn
        {
            if let Err(e) = self.wal_writer.flush_to(page_lsn) {
                let mut meta = self.frames[frame_id.0].meta.lock();
                meta.dirty = true;
                restore_first_dirty_lsn(&mut meta.first_dirty_lsn, saved_first_dirty_lsn);
                meta.flushing = false;
                self.flush_done.notify_all();
                return Err(e);
            }
        }

        let offset = (page_id.0 - 1) * self.config.page_size() as u64;
        if let Err(e) = self.data_file.write_all_at(&*content, offset) {
            let mut meta = self.frames[frame_id.0].meta.lock();
            meta.dirty = true;
            restore_first_dirty_lsn(&mut meta.first_dirty_lsn, saved_first_dirty_lsn);
            meta.flushing = false;
            self.flush_done.notify_all();
            return Err(e);
        }
        let my_gen = self.flush_gen.fetch_add(1, Ordering::AcqRel) + 1;

        // Mark needs_fpi BEFORE releasing the content lock. The page now has
        // an on-disk image (the write is issued; durability follows from the
        // fsync below or a later one), so a subsequent modification in a
        // later checkpoint cycle must be preceded by an FPI (torn-write
        // protection). This is what closes the cross-checkpoint window for
        // pages that stay RESIDENT across a checkpoint (never evicted):
        // eviction+reload sets needs_fpi via locate_or_load, but an in-place
        // checkpoint flush keeps the page resident, so flush must mark it
        // here. Doing it before `drop(content)` also closes the race where a
        // pin_mut slipping in after the drop would observe needs_fpi ==
        // false and modify the page WITHOUT an FPI. (content.read → meta is
        // the sanctioned nesting order, same as pin_mut.)
        self.frames[frame_id.0].meta.lock().needs_fpi = true;
        drop(content);

        // Group-fsync coalescing: skip if a concurrent sync already covers us.
        if self.synced_gen.load(Ordering::Acquire) < my_gen {
            let covered_gen = self.flush_gen.load(Ordering::Acquire);
            if let Err(e) = self.data_file.sync_all() {
                let mut meta = self.frames[frame_id.0].meta.lock();
                meta.dirty = true;
                restore_first_dirty_lsn(&mut meta.first_dirty_lsn, saved_first_dirty_lsn);
                meta.flushing = false;
                self.flush_done.notify_all();
                return Err(e);
            }
            self.synced_gen.fetch_max(covered_gen, Ordering::AcqRel);
        }

        // The durability decision is complete (fsync done or coverage
        // confirmed): only NOW may waiters observe the page as flushed.
        {
            let mut meta = self.frames[frame_id.0].meta.lock();
            meta.flushing = false;
            self.flush_done.notify_all();
        }
        Ok(())
    }

    /// `cfg(loom)` variant of [`Self::flush_frame`]: performs only the
    /// dirty / rec_lsn / needs_fpi / flushing state transitions and skips
    /// the WAL flush, the data-file write, and the fsync. Loom models must
    /// size the pool so no eviction happens — an evicted page reloaded from
    /// disk would read zeros, since nothing is ever written in a model
    /// build. See the `crate::sync` module docs.
    ///
    /// The `flushing` claim/clear is mirrored for state parity, but the
    /// production condvar WAIT is not: model builds have a single flusher
    /// (no eviction, no checkpoint, and B+Tree splits serialize on the root
    /// latch), so a second flush can never observe `flushing` set — assert
    /// that instead of adding a condvar wait loom's wrapper omits.
    #[cfg(loom)]
    fn flush_frame(&self, frame_id: FrameId) -> Result<()> {
        {
            let mut meta = self.frames[frame_id.0].meta.lock();
            debug_assert!(
                !meta.flushing,
                "concurrent flush in a loom model (models have a single flusher)"
            );
            if !meta.dirty || meta.page_id == PageId::INVALID {
                return Ok(());
            }
            meta.flushing = true;
            meta.dirty = false;
            meta.first_dirty_lsn = Lsn::INVALID;
        }
        // Scheduling-point parity with the real path (Stage Q review): the
        // real `flush_frame` holds `content.read` across the WAL flush and
        // data write, and re-locks `meta` (for `needs_fpi`) WHILE content
        // is still held. Mirror that exact lock sequence here — without
        // touching any data — so loom explores interleavings through the
        // same content → meta nesting instead of silently dropping that
        // dimension of the schedule space.
        let content = self.frames[frame_id.0].content.read();
        {
            let mut meta = self.frames[frame_id.0].meta.lock();
            meta.needs_fpi = true;
            meta.flushing = false;
        }
        drop(content);
        Ok(())
    }

    fn unpin(&self, frame_id: FrameId) {
        let mut meta = self.frames[frame_id.0].meta.lock();
        debug_assert!(meta.pin_count > 0, "unpin called on unpinned frame");
        meta.pin_count -= 1;
    }
}

/// Restore a saved first-dirty anchor after a failed flush, keeping the
/// OLDER of the saved anchor and any anchor a concurrent re-dirty installed
/// meanwhile (min-merge; Stage N review, P2-2).
///
/// The write failed, so the on-disk image is still the pre-flush one and
/// the correct rec_lsn of the current dirty epoch is the OLDEST anchor
/// that describes it. The previous restore-only-when-INVALID rule kept a
/// newer anchor (N > S) installed by a re-dirty during the flush — an
/// over-estimate that a future min-formula redo start would read as
/// "already on disk", silently skipping redo. A `saved` of
/// [`Lsn::INVALID`] (dirty page whose writer never stamped a WAL LSN)
/// restores nothing.
#[cfg_attr(loom, allow(dead_code))] // only called by the real `flush_frame`
fn restore_first_dirty_lsn(current: &mut Lsn, saved: Lsn) {
    if saved.is_valid() && (*current == Lsn::INVALID || saved < *current) {
        *current = saved;
    }
}

/// Read guard for a pinned page.
#[derive(Debug)]
pub struct PageGuard<'a> {
    frame_id: FrameId,
    page_id: PageId,
    content_guard: Option<RwLockReadGuard<'a, [u8; PAGE_SIZE]>>,
    pool: &'a BufferPool,
}

impl PageGuard<'_> {
    /// Return the frame ID held by this guard.
    pub fn frame_id(&self) -> FrameId {
        self.frame_id
    }

    /// Return the page ID held by this guard.
    pub fn page_id(&self) -> PageId {
        self.page_id
    }

    /// Return a reference to the page content.
    pub fn page(&self) -> &[u8] {
        &**self.content_guard.as_ref().expect("guard is active")
    }
}

impl AsRef<[u8]> for PageGuard<'_> {
    fn as_ref(&self) -> &[u8] {
        self.page()
    }
}

impl Drop for PageGuard<'_> {
    fn drop(&mut self) {
        // Drop the content lock before decrementing pin_count so the frame
        // cannot be evicted while we still hold the content.
        drop(self.content_guard.take());
        self.pool.unpin(self.frame_id);
    }
}

/// Write guard for a pinned page.
#[derive(Debug)]
pub struct PageGuardMut<'a> {
    frame_id: FrameId,
    page_id: PageId,
    content_guard: Option<RwLockWriteGuard<'a, [u8; PAGE_SIZE]>>,
    pool: &'a BufferPool,
}

impl PageGuardMut<'_> {
    /// Return the frame ID held by this guard.
    pub fn frame_id(&self) -> FrameId {
        self.frame_id
    }

    /// Return the page ID held by this guard.
    pub fn page_id(&self) -> PageId {
        self.page_id
    }

    /// Return a reference to the page content.
    pub fn page(&self) -> &[u8] {
        &**self.content_guard.as_ref().expect("guard is active")
    }

    /// Return a mutable reference to the page content.
    pub fn page_mut(&mut self) -> &mut [u8] {
        &mut **self.content_guard.as_mut().expect("guard is active")
    }
}

impl AsRef<[u8]> for PageGuardMut<'_> {
    fn as_ref(&self) -> &[u8] {
        self.page()
    }
}

impl AsMut<[u8]> for PageGuardMut<'_> {
    fn as_mut(&mut self) -> &mut [u8] {
        self.page_mut()
    }
}

impl Drop for PageGuardMut<'_> {
    fn drop(&mut self) {
        // Sample the authoritative page LSN (`page[0..8]`) while we still hold
        // the write latch; it anchors the frame's rec_lsn below.
        let lsn_at_drop = page_pd_lsn(&self.content_guard.as_ref().expect("guard is active")[..]);
        drop(self.content_guard.take());

        // A write guard may have modified the page. We cannot know whether it
        // actually did, so mark the frame dirty on drop. False positives are
        // safe (an unnecessary flush later) and cheaper than tracking every
        // write.
        {
            let mut meta = self.pool.frames[self.frame_id.0].meta.lock();
            if meta.page_id != PageId::INVALID {
                // DPT anchor (ARIES rec_lsn, §11.1): only fill in the anchor
                // when the frame has none — i.e. on the clean → dirty
                // transition, since `flush_frame` clears the anchor together
                // with the dirty flag. The value used is the page's `pd_lsn`
                // at drop time, which approximates "the LSN that first
                // dirtied the page since the last flush":
                //
                // - AMs stamp `pd_lsn = max(record.lsn, pd_lsn)` on every
                //   WAL-logged modification (heap/btree `stamp_pd_lsn`), so
                //   for the first dirtier of an epoch `lsn_at_drop` is exactly
                //   that modification's record LSN.
                // - For raw writes that never stamp `pd_lsn` the value is
                //   stale, i.e. an *under*-estimate of the true first-dirty
                //   LSN. An under-estimated rec_lsn is always safe: recovery
                //   replays a few extra, pd_lsn-guarded (idempotent) records.
                // - It cannot be an unsafe *over*-estimate: any WAL record
                //   that dirtied the page in this epoch carries an LSN ≤ the
                //   page's current `pd_lsn`, and a concurrent flush that
                //   already made this guard's content durable merely leaves a
                //   conservative extra anchor (redo skips via the pd_lsn
                //   guard).
                //
                // A fresh zeroed page has `pd_lsn == INVALID`; it then stays
                // INVALID and is filtered out of the DPT snapshot (its
                // PageAlloc record is replayed from the WAL scan anyway).
                if meta.first_dirty_lsn == Lsn::INVALID {
                    meta.first_dirty_lsn = lsn_at_drop;
                }
                meta.dirty = true;
            }
        }

        self.pool.unpin(self.frame_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::StorageConfig;
    use crate::page::{page_pd_lsn, PAGE_HEADER_SIZE};
    use proptest::prelude::*;
    use tempfile::TempDir;

    fn test_config(tmp: &TempDir) -> StorageConfig {
        let mut cfg = StorageConfig::new(tmp.path());
        cfg.buffer_pool_size = 1024 * 1024; // 1 MB = 128 frames at 8 KB
        cfg.buffer_pool_shards = 8;
        cfg.wal_group_commit_timeout_ms = 1;
        cfg.wal_group_commit_batch_size = 1;
        cfg
    }

    fn setup(tmp: &TempDir) -> (Arc<Mutex<PageAllocator>>, Arc<WalWriter>, BufferPool) {
        let cfg = test_config(tmp);
        let wal = Arc::new(WalWriter::open(tmp.path(), &cfg).unwrap());
        let allocator = Arc::new(Mutex::new(
            PageAllocator::open(tmp.path(), &cfg, Arc::clone(&wal)).unwrap(),
        ));
        let pool =
            BufferPool::open(tmp.path(), &cfg, Arc::clone(&allocator), Arc::clone(&wal)).unwrap();
        (allocator, wal, pool)
    }

    #[test]
    fn open_creates_expected_frame_count() {
        let tmp = TempDir::new().unwrap();
        let cfg = test_config(&tmp);
        let (_, _, pool) = setup(&tmp);
        assert_eq!(pool.frame_count(), cfg.buffer_pool_size / cfg.page_size());
        assert_eq!(pool.shard_count(), cfg.buffer_pool_shards);
    }

    #[test]
    fn new_page_returns_zeroed_writable_page() {
        let tmp = TempDir::new().unwrap();
        let (_, _, pool) = setup(&tmp);

        let guard = pool.new_page().unwrap();
        assert_eq!(guard.page().len(), PAGE_SIZE);
        assert!(guard.page().iter().all(|&b| b == 0));
    }

    #[test]
    fn pin_read_returns_existing_page() {
        let tmp = TempDir::new().unwrap();
        let (_, _, pool) = setup(&tmp);

        let mut guard = pool.new_page().unwrap();
        let page_id = guard.page_id();
        guard.page_mut()[PAGE_HEADER_SIZE] = 0xAB;
        drop(guard);

        let read_guard = pool.pin(page_id).unwrap();
        assert_eq!(read_guard.page()[PAGE_HEADER_SIZE], 0xAB);
    }

    #[test]
    fn pin_mut_writes_full_page_image() {
        let tmp = TempDir::new().unwrap();
        let (_, _wal, pool) = setup(&tmp);

        // Create and populate a page.
        let page_id = {
            let mut guard = pool.new_page().unwrap();
            let id = guard.page_id();
            guard.page_mut()[PAGE_HEADER_SIZE] = 0xCD;
            id
        };

        // Simulate a checkpoint so that the page is considered "before the
        // checkpoint" when it is reloaded. This triggers the FPI path.
        pool.set_checkpoint_lsn(Lsn(1_000));

        // First pin_mut after the page has been evicted should write an FPI.
        // Force eviction by allocating many new pages.
        for _ in 0..pool.frame_count() + 10 {
            let _ = pool.new_page().unwrap();
        }

        let mut guard = pool.pin_mut(page_id).unwrap();
        guard.page_mut()[PAGE_HEADER_SIZE + 1] = 0xEF;
        drop(guard);

        // WAL should contain at least one FullPageImage record.
        let mut reader = crate::wal::reader::WalReader::open(
            tmp.path().join("wal"),
            test_config(&tmp).wal_segment_size,
        )
        .unwrap();
        let mut found_fpi = false;
        while let Some(record) = reader.next_record().unwrap() {
            if record.record_type == crate::wal::record::WalRecordType::FullPageImage {
                found_fpi = true;
            }
        }
        assert!(
            found_fpi,
            "pin_mut should have written a FullPageImage record"
        );
    }

    #[test]
    fn fpi_fires_for_resident_page_across_checkpoint() {
        // A freshly allocated page that stays RESIDENT (never evicted) across a
        // checkpoint must still get an FPI on its next modification: after the
        // checkpoint flush it has an on-disk image a torn write could corrupt.
        // Regression for the Stage I Step 7 gap where `needs_fpi` was only set
        // on eviction+reload, so resident pages skipped the FPI.
        let tmp = TempDir::new().unwrap();
        let (_, _wal, pool) = setup(&tmp);

        let page_id = {
            let mut guard = pool.new_page().unwrap();
            let id = guard.page_id();
            guard.page_mut()[PAGE_HEADER_SIZE] = 0xCD;
            id
        };
        // Flush in place (as a checkpoint would), keeping the page resident.
        pool.flush(page_id).unwrap();
        pool.set_checkpoint_lsn(Lsn(10_000_000));

        // Next modification of the still-resident page must write an FPI.
        {
            let mut guard = pool.pin_mut(page_id).unwrap();
            guard.page_mut()[PAGE_HEADER_SIZE + 1] = 0x01;
        }

        let mut reader = crate::wal::reader::WalReader::open(
            tmp.path().join("wal"),
            test_config(&tmp).wal_segment_size,
        )
        .unwrap();
        let mut fpi_count = 0;
        while let Some(record) = reader.next_record().unwrap() {
            if record.record_type == crate::wal::record::WalRecordType::FullPageImage {
                fpi_count += 1;
            }
        }
        assert_eq!(
            fpi_count, 1,
            "resident page needs an FPI after a checkpoint (got {fpi_count})"
        );
    }

    #[test]
    fn fpi_refires_across_checkpoint_boundary() {
        // Within a single residency, an FPI must be re-written after each new
        // checkpoint begins. The `needs_fpi` flag is never cleared, so the
        // `pd_lsn < checkpoint_lsn` gate alone drives the decision (Step 7).
        let tmp = TempDir::new().unwrap();
        let (_, _wal, pool) = setup(&tmp);

        let page_id = {
            let mut guard = pool.new_page().unwrap();
            let id = guard.page_id();
            guard.page_mut()[PAGE_HEADER_SIZE] = 0xCD;
            id
        };

        // First checkpoint, then evict so the reloaded page has needs_fpi=true.
        pool.set_checkpoint_lsn(Lsn(1_000));
        for _ in 0..pool.frame_count() + 10 {
            let _ = pool.new_page().unwrap();
        }

        // Modification #1 in checkpoint cycle 1 -> FPI #1. The page stays
        // resident afterwards (no eviction), so needs_fpi is not re-set.
        {
            let mut guard = pool.pin_mut(page_id).unwrap();
            guard.page_mut()[PAGE_HEADER_SIZE + 1] = 0x01;
        }

        // A second checkpoint advances checkpoint_lsn past the FPI #1 pd_lsn.
        pool.set_checkpoint_lsn(Lsn(10_000_000));

        // Modification #2 in checkpoint cycle 2 -> FPI #2 (page_lsn < new
        // checkpoint_lsn), proving the cross-checkpoint window is closed.
        {
            let mut guard = pool.pin_mut(page_id).unwrap();
            guard.page_mut()[PAGE_HEADER_SIZE + 2] = 0x02;
        }

        let mut reader = crate::wal::reader::WalReader::open(
            tmp.path().join("wal"),
            test_config(&tmp).wal_segment_size,
        )
        .unwrap();
        let mut fpi_count = 0;
        while let Some(record) = reader.next_record().unwrap() {
            if record.record_type == crate::wal::record::WalRecordType::FullPageImage {
                fpi_count += 1;
            }
        }
        assert_eq!(
            fpi_count, 2,
            "FPI must re-fire after the second checkpoint (got {fpi_count})"
        );
    }

    #[test]
    fn flush_persists_page() {
        let tmp = TempDir::new().unwrap();
        let (_, _, pool) = setup(&tmp);

        let page_id = {
            let mut guard = pool.new_page().unwrap();
            let id = guard.page_id();
            guard.page_mut()[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + 4].copy_from_slice(&[1, 2, 3, 4]);
            id
        };

        pool.flush(page_id).unwrap();

        // Drop the pool and reopen it; the page should still be readable.
        let (_, _, pool2) = setup(&tmp);
        let guard = pool2.pin(page_id).unwrap();
        assert_eq!(
            &guard.page()[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + 4],
            &[1, 2, 3, 4]
        );
    }

    #[test]
    fn wal_before_data_on_evict() {
        // Single-frame pool: every page load evicts the resident page, so
        // eviction is fully deterministic. The group-commit worker is
        // configured to flush ONLY on its 1s timeout (the batch size is never
        // reached), so within this test's millisecond-scale critical section
        // the worker cannot fsync the FPI spontaneously: the only way
        // synced_lsn can advance past the FPI LSN is flush_frame's
        // flush_to(page_lsn). That makes the assertion in step 5 a genuine
        // WAL-before-data guard — delete the flush_to in flush_frame and it
        // fails. Each explicit flush_to stalls at most ~1s waiting for the
        // worker's timeout, which bounds the runtime.
        let tmp = TempDir::new().unwrap();
        let mut cfg = StorageConfig::new(tmp.path());
        cfg.buffer_pool_size = cfg.page_size(); // 1 frame
        cfg.buffer_pool_shards = 1;
        cfg.wal_group_commit_timeout_ms = 1_000;
        cfg.wal_group_commit_batch_size = 1_000_000;
        let wal = Arc::new(WalWriter::open(tmp.path(), &cfg).unwrap());
        let allocator = Arc::new(Mutex::new(
            PageAllocator::open(tmp.path(), &cfg, Arc::clone(&wal)).unwrap(),
        ));
        let pool =
            BufferPool::open(tmp.path(), &cfg, Arc::clone(&allocator), Arc::clone(&wal)).unwrap();

        // 1. Create the victim with on-disk content, then a second page whose
        //    allocation evicts the victim from the single frame (the dirty
        //    eviction writes the victim to the data file). User content lives
        //    past the 32-byte page header so it never collides with pd_lsn.
        let victim_id = {
            let mut guard = pool.new_page().unwrap();
            let id = guard.page_id();
            guard.page_mut()[PAGE_HEADER_SIZE] = 0xAA;
            id
        };
        let other_id = {
            let mut guard = pool.new_page().unwrap();
            let id = guard.page_id();
            guard.page_mut()[PAGE_HEADER_SIZE] = 0x11;
            id
        };
        assert!(
            pool.frame_cached_lsn(victim_id).is_none(),
            "victim should have been evicted by the second allocation"
        );

        // 2. Set checkpoint_lsn above everything written so far so that the
        //    next pin_mut on the victim appends an FPI. `alloc_page` is now
        //    append-only (fsync deferred), so `synced_lsn` may still be 0 here;
        //    we use `current_lsn` (end-of-WAL, sync-independent) as the
        //    boundary. The FPI appended in step 3 lands past this boundary, so
        //    the `synced_lsn() < fpi_lsn` guard in step 3 stays meaningful.
        pool.set_checkpoint_lsn(wal.current_lsn());

        // 3. pin_mut reloads the victim (evicting `other`, whose flush brings
        //    synced_lsn up to date) and appends its FPI without flushing. The
        //    authoritative FPI LSN is read from the page's pd_lsn field. The
        //    worker's next spontaneous flush is ~1s away, so the FPI cannot
        //    become durable on its own within this critical section.
        let fpi_lsn = {
            let mut guard = pool.pin_mut(victim_id).unwrap();
            guard.page_mut()[PAGE_HEADER_SIZE] = 0xBB;
            let fpi_lsn = page_pd_lsn(guard.page());
            drop(guard);
            assert!(
                pool.frame_cached_lsn(victim_id) == Some(fpi_lsn),
                "frame cache must mirror the page's pd_lsn"
            );
            fpi_lsn
        };
        assert!(
            wal.synced_lsn() < fpi_lsn,
            "setup: the FPI must not be durable yet: synced={}, fpi_lsn={}",
            wal.synced_lsn(),
            fpi_lsn
        );

        // 4. Reload `other`, evicting the dirty victim. flush_frame must call
        //    flush_to(fpi_lsn) before writing the page.
        {
            let _guard = pool.pin(other_id).unwrap();
        }
        assert!(
            pool.frame_cached_lsn(victim_id).is_none(),
            "victim should have been evicted"
        );

        // 5. WAL-before-data: synced_lsn now covers the real FPI LSN.
        let post_evict_synced = wal.synced_lsn();
        assert!(
            post_evict_synced >= fpi_lsn,
            "eviction must have called flush_to(fpi_lsn) for WAL-before-data: \
             fpi_lsn={fpi_lsn}, post_evict_synced={post_evict_synced}"
        );

        // 6. The victim page is durable on disk with the updated content.
        let guard = pool.pin(victim_id).unwrap();
        assert_eq!(guard.page()[PAGE_HEADER_SIZE], 0xBB);
    }

    #[test]
    fn pd_lsn_authoritative() {
        // After any WAL-covered mutation, the frame's cached LSN must equal
        // the page's own pd_lsn (page[0..8]); after an eviction/reload cycle
        // the cache is rebuilt from page[0..8] itself.
        let tmp = TempDir::new().unwrap();
        let (_, wal, pool) = setup(&tmp);

        let page_id = {
            let mut guard = pool.new_page().unwrap();
            guard.page_mut()[PAGE_HEADER_SIZE] = 0x42;
            guard.page_id()
        };
        pool.flush(page_id).unwrap();

        // Evict the page so the next pin_mut starts a new residency (FPI
        // eligible), then simulate a checkpoint.
        let frame_count = pool.frame_count();
        for _ in 0..frame_count + 2 {
            let _ = pool.new_page().unwrap();
        }
        pool.set_checkpoint_lsn(wal.synced_lsn());

        // First mutation in the new residency: the FPI publishes its LSN into
        // the page header, and the frame cache mirrors it.
        {
            let mut guard = pool.pin_mut(page_id).unwrap();
            guard.page_mut()[PAGE_HEADER_SIZE] = 0x43;
            let pd_lsn = page_pd_lsn(guard.page());
            assert!(
                pd_lsn.is_valid(),
                "pin_mut must publish the FPI LSN into page[0..8]"
            );
            assert_eq!(pool.frame_cached_lsn(page_id), Some(pd_lsn));
        }

        // Evict and reload: the cache must be rebuilt from page[0..8] (the
        // flush on eviction wrote pd_lsn to disk).
        for _ in 0..frame_count + 2 {
            let _ = pool.new_page().unwrap();
        }
        let pd_after_reload = {
            let guard = pool.pin(page_id).unwrap();
            page_pd_lsn(guard.page())
        };
        assert!(pd_after_reload.is_valid());
        assert_eq!(pool.frame_cached_lsn(page_id), Some(pd_after_reload));
    }

    #[test]
    fn clock_evicts_unreferenced_pages() {
        let tmp = TempDir::new().unwrap();
        let (_, _, pool) = setup(&tmp);

        let frame_count = pool.frame_count();
        // Allocate exactly frame_count pages and immediately drop the guards.
        // None are referenced, so CLOCK should be able to evict them.
        let mut ids = Vec::new();
        for _ in 0..frame_count {
            let guard = pool.new_page().unwrap();
            ids.push(guard.page_id());
        }
        drop(ids);

        // Allocate one more page; this must succeed by evicting an old frame.
        let guard = pool.new_page().unwrap();
        assert_eq!(guard.page().len(), PAGE_SIZE);
    }

    #[test]
    fn clock_gives_second_chance_to_referenced_pages() {
        let tmp = TempDir::new().unwrap();
        let (_, _, pool) = setup(&tmp);

        let frame_count = pool.frame_count();
        // Allocate and uniquely mark every frame.
        let mut ids = Vec::new();
        for i in 0..frame_count {
            let mut guard = pool.new_page().unwrap();
            let id = guard.page_id();
            guard.page_mut()[PAGE_HEADER_SIZE] = i as u8;
            ids.push(id);
        }
        drop(ids);

        // Reference a strict subset of the pages.
        let referenced: Vec<_> = (0..frame_count / 2)
            .map(|i| {
                let guard = pool.pin(PageId(i as u64 + 1)).unwrap();
                assert_eq!(guard.page()[PAGE_HEADER_SIZE], i as u8);
                guard.page_id()
            })
            .collect();
        drop(referenced);

        // Evict exactly the number of unreferenced pages.
        let unreferenced_count = frame_count - frame_count / 2;
        for _ in 0..unreferenced_count {
            drop(pool.new_page().unwrap());
        }

        // The referenced pages must still be resident (cache hits) with their
        // original content.
        for i in 0..frame_count / 2 {
            let guard = pool.pin(PageId(i as u64 + 1)).unwrap();
            assert_eq!(guard.page()[PAGE_HEADER_SIZE], i as u8);
        }
    }

    #[test]
    fn full_scan_does_not_pin_all_pages() {
        let tmp = TempDir::new().unwrap();
        let (_, _, pool) = setup(&tmp);

        let frame_count = pool.frame_count();
        // Fill the pool with unique data.
        let mut ids = Vec::new();
        for i in 0..frame_count {
            let mut guard = pool.new_page().unwrap();
            let id = guard.page_id();
            guard.page_mut()[PAGE_HEADER_SIZE] = i as u8;
            ids.push(id);
        }
        drop(ids);

        // Simulate a full table scan: pin every page once, then release.
        for i in 0..frame_count {
            let guard = pool.pin(PageId(i as u64 + 1)).unwrap();
            assert_eq!(guard.page()[PAGE_HEADER_SIZE], i as u8);
        }

        // After one full scan, all pages have reference=true. CLOCK should give
        // each page exactly one second chance, so allocating frame_count more
        // pages should evict all original pages without BufferPoolFull.
        for _ in 0..frame_count {
            drop(pool.new_page().unwrap());
        }
    }

    #[test]
    fn pin_nonexistent_page_returns_error() {
        let tmp = TempDir::new().unwrap();
        let (_, _, pool) = setup(&tmp);

        // PageId(1000) was never allocated; the data file has no space for it.
        let result = pool.pin(PageId(1000));
        assert!(result.is_err(), "pinning a non-existent page must fail");
    }

    #[test]
    fn pinned_pages_are_not_evicted() {
        let tmp = TempDir::new().unwrap();
        let (_, _, pool) = setup(&tmp);

        let frame_count = pool.frame_count();
        // Pin the first page and keep it alive.
        let pinned = pool.new_page().unwrap();

        // Fill the pool with additional pages and immediately release them so
        // they become evictable.
        for _ in 0..frame_count - 1 {
            drop(pool.new_page().unwrap());
        }

        // One more allocation should succeed (evicts one of the previously
        // allocated pages while keeping the pinned page resident).
        let _extra = pool.new_page().unwrap();

        // The originally pinned page must still be accessible.
        assert_eq!(pinned.page().len(), PAGE_SIZE);
    }

    #[test]
    fn concurrent_pin_and_new_page_are_safe() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::thread;

        let tmp = TempDir::new().unwrap();
        let (_, _, pool) = setup(&tmp);
        let pool = Arc::new(pool);

        let successes = Arc::new(AtomicUsize::new(0));
        let all_ids: Arc<Mutex<Vec<PageId>>> = Arc::new(Mutex::new(Vec::new()));
        let mut handles = Vec::new();

        for _ in 0..16 {
            let p = Arc::clone(&pool);
            let s = Arc::clone(&successes);
            let ids = Arc::clone(&all_ids);
            handles.push(thread::spawn(move || {
                for _ in 0..50 {
                    if let Ok(g) = p.new_page() {
                        ids.lock().push(g.page_id());
                        s.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(successes.load(Ordering::Relaxed), 16 * 50);

        let ids = all_ids.lock();
        assert_eq!(ids.len(), 16 * 50);
        let mut sorted = ids.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(
            sorted.len(),
            ids.len(),
            "concurrent new_page returned duplicate page IDs"
        );
    }

    #[test]
    fn dirty_page_ids_reflects_modified_pages() {
        let tmp = TempDir::new().unwrap();
        let (_, _, pool) = setup(&tmp);

        let mut dirty_ids = Vec::new();
        for _ in 0..3 {
            let guard = pool.new_page().unwrap();
            dirty_ids.push(guard.page_id());
        }

        // New pages are dirty.
        let mut reported = pool.dirty_page_ids();
        reported.sort();
        assert_eq!(reported, dirty_ids);

        // After flush, no pages are dirty.
        for id in &dirty_ids {
            pool.flush(*id).unwrap();
        }
        assert!(pool.dirty_page_ids().is_empty());
    }

    /// Stage N (§11.1/§11.4): the DPT snapshot reports `(page_id, rec_lsn)`
    /// for dirty frames, anchors rec_lsn at the FPI LSN (or the page's
    /// `pd_lsn` at guard drop when no FPI fires), keeps the epoch's first
    /// anchor across repeated modifications, and drops the entry once the
    /// page is flushed.
    #[test]
    fn dirty_page_snapshot_tracks_rec_lsn_epoch() {
        let tmp = TempDir::new().unwrap();
        let (allocator, wal, pool) = setup(&tmp);
        let _ = &allocator;

        // 1. A raw new-page write never stamps a WAL LSN, so its
        //    first_dirty_lsn stays INVALID and the frame is filtered out of
        //    the snapshot (its PageAlloc/content records are covered by the
        //    recovery WAL scan regardless).
        let page_id = {
            let mut guard = pool.new_page().unwrap();
            guard.page_mut()[PAGE_HEADER_SIZE] = 1;
            guard.page_id()
        };
        assert!(pool.dirty_page_ids().contains(&page_id));
        assert!(
            pool.dirty_page_snapshot().is_empty(),
            "unknown first-dirty LSN must be filtered out of the DPT snapshot"
        );

        // 2. Flush, publish a checkpoint LSN, and re-dirty via pin_mut: the
        //    FPI path fires and the FPI LSN becomes the rec_lsn anchor.
        pool.flush(page_id).unwrap();
        pool.set_checkpoint_lsn(wal.current_lsn());
        {
            let mut guard = pool.pin_mut(page_id).unwrap();
            guard.page_mut()[PAGE_HEADER_SIZE] = 2;
        }
        let fpi_lsn = pool.frame_cached_lsn(page_id).unwrap();
        assert!(fpi_lsn.is_valid());
        assert_eq!(pool.dirty_page_snapshot(), vec![(page_id, fpi_lsn)]);

        // 3. A second modification in the same dirty epoch keeps the epoch's
        //    first anchor (ARIES rec_lsn = FIRST dirtying LSN since flush).
        {
            let mut guard = pool.pin_mut(page_id).unwrap();
            guard.page_mut()[PAGE_HEADER_SIZE] = 3;
        }
        assert_eq!(pool.dirty_page_snapshot(), vec![(page_id, fpi_lsn)]);

        // 4. Flush ends the epoch: the page leaves the DPT snapshot.
        pool.flush(page_id).unwrap();
        assert!(pool.dirty_page_snapshot().is_empty());

        // 5. Re-dirty without an FPI (the page's pd_lsn is already past the
        //    checkpoint LSN): the guard-drop path anchors rec_lsn at the
        //    page's pd_lsn. No new WAL record stamped the page here, so the
        //    anchor is the stale FPI LSN — a safe under-estimate (see the
        //    approximation argument in PageGuardMut::drop).
        {
            let mut guard = pool.pin_mut(page_id).unwrap();
            guard.page_mut()[PAGE_HEADER_SIZE] = 4;
        }
        assert_eq!(pool.dirty_page_snapshot(), vec![(page_id, fpi_lsn)]);
    }

    /// Stage N review P2-1: `pin_mut` marks the frame dirty immediately, so
    /// a fuzzy checkpoint collecting `dirty_page_ids()` while a write guard
    /// is still held sees the page. Before the fix the dirty flag appeared
    /// only at guard drop, and a guard straddling the collection lost its
    /// update on crash (WAL record before begin_lsn, page never flushed).
    #[test]
    fn pin_mut_marks_frame_dirty_while_guard_is_held() {
        let tmp = TempDir::new().unwrap();
        let (_, _, pool) = setup(&tmp);

        let page_id = {
            let guard = pool.new_page().unwrap();
            guard.page_id()
        };
        pool.flush(page_id).unwrap();
        assert!(
            !pool.dirty_page_ids().contains(&page_id),
            "precondition: freshly flushed page is clean"
        );

        // Guard held, page not yet modified: write intent alone marks it.
        let guard = pool.pin_mut(page_id).unwrap();
        assert!(
            pool.dirty_page_ids().contains(&page_id),
            "pin_mut must mark the frame dirty while the guard is held"
        );
        drop(guard);
        assert!(pool.dirty_page_ids().contains(&page_id));

        // A read-only pin must NOT dirty the frame (flush resets first).
        pool.flush(page_id).unwrap();
        let read_guard = pool.pin(page_id).unwrap();
        assert!(
            !pool.dirty_page_ids().contains(&page_id),
            "read-only pin must not dirty the frame"
        );
        drop(read_guard);
    }

    /// Stage N review P2-2: the failed-flush anchor restore is a min-merge,
    /// not a restore-only-when-INVALID.
    #[test]
    fn restore_first_dirty_lsn_keeps_the_oldest_anchor() {
        // Current INVALID: the saved anchor is restored.
        let mut current = Lsn::INVALID;
        restore_first_dirty_lsn(&mut current, Lsn(100));
        assert_eq!(current, Lsn(100));

        // Saved older than current: min wins (the failed write left the
        // pre-flush image on disk, so the older anchor is the true rec_lsn).
        let mut current = Lsn(200);
        restore_first_dirty_lsn(&mut current, Lsn(100));
        assert_eq!(current, Lsn(100));

        // Saved newer than current: the concurrent re-dirty's older anchor
        // must not be moved backwards.
        let mut current = Lsn(100);
        restore_first_dirty_lsn(&mut current, Lsn(200));
        assert_eq!(current, Lsn(100));

        // Saved INVALID (writer never stamped a WAL LSN): no-op.
        let mut current = Lsn(100);
        restore_first_dirty_lsn(&mut current, Lsn::INVALID);
        assert_eq!(current, Lsn(100));
        let mut current = Lsn::INVALID;
        restore_first_dirty_lsn(&mut current, Lsn::INVALID);
        assert_eq!(current, Lsn::INVALID);
    }

    /// Regression test for the clear-before-write dirty protocol in
    /// `flush_frame`.
    ///
    /// A `pin_mut` that overlaps an in-flight flush must leave the frame
    /// dirty afterwards (so a later flush rewrites the page) — unless the
    /// flush already wrote the latest content to disk. The old
    /// clear-after-write protocol could wipe the dirty flag set by a
    /// concurrent writer *after* having written a stale image, leaving the
    /// page clean in memory but stale on disk; the next checkpoint would
    /// advance the redo point past the modification's WAL records and the
    /// change would be lost on crash recovery.
    ///
    /// Invariant checked after every iteration:
    ///   `meta.dirty == true`  OR  on-disk content == latest written value.
    ///
    /// With clear-before-write the invariant holds deterministically: the
    /// flusher clears `dirty` before touching content, so a dirty flag set
    /// by an overlapping writer always survives; and a flusher that sampled
    /// `dirty` after the writer's guard drop necessarily read the newer
    /// content (the guard releases `content.write` before setting `dirty`
    /// under the meta lock). With the old protocol this test fails whenever
    /// the writer lands inside the write/fsync window — which the small
    /// sleep below biases toward.
    #[test]
    fn concurrent_pin_mut_during_flush_never_loses_dirty_state() {
        use std::thread;
        use std::time::Duration;

        let tmp = TempDir::new().unwrap();
        let (_, _, pool) = setup(&tmp);
        let pool = Arc::new(pool);

        let page_id = pool.new_page().unwrap().page_id();
        pool.flush(page_id).unwrap();

        const OFFSET: usize = PAGE_HEADER_SIZE;
        const ITERATIONS: u8 = 100;

        for iter in 0..ITERATIONS {
            let v1 = iter.wrapping_mul(2); // first writer's value
            let v2 = v1 + 1; // overlapping writer's value (always != v1)

            // Dirty the page with v1.
            {
                let mut guard = pool.pin_mut(page_id).unwrap();
                guard.page_mut()[OFFSET] = v1;
            }

            // Flush from a second thread; overlap a v2 write with it.
            let flusher = {
                let pool = Arc::clone(&pool);
                thread::spawn(move || pool.flush(page_id).unwrap())
            };
            // Bias the v2 write into the flusher's write/fsync window.
            thread::sleep(Duration::from_micros(200));
            {
                let mut guard = pool.pin_mut(page_id).unwrap();
                guard.page_mut()[OFFSET] = v2;
            }
            flusher.join().unwrap();

            let still_dirty = pool.dirty_page_ids().contains(&page_id);
            if !still_dirty {
                // The frame is clean, so the flush must have persisted the
                // latest value. A stale v1 on disk with a clean frame is the
                // lost-dirty bug.
                let mut buf = [0u8; PAGE_SIZE];
                let offset = (page_id.0 - 1) * PAGE_SIZE as u64;
                pool.data_file.read_exact_at(&mut buf, offset).unwrap();
                assert_eq!(
                    buf[OFFSET], v2,
                    "iteration {iter}: frame clean but disk has stale value \
                     (expected {v2}, got {})",
                    buf[OFFSET]
                );
            }
        }
    }

    /// Stage Q review H1: a flush caller that observes a clean page must
    /// wait out an IN-FLIGHT concurrent flush instead of returning early —
    /// the loser's return only proves "already durable", never "someone
    /// else is still writing". Asserts no error, the in-flight flag clears,
    /// and the latest bytes are durable after both flushers return.
    #[test]
    fn concurrent_flush_waits_for_in_flight_flush() {
        use std::sync::Barrier;
        use std::thread;

        let tmp = TempDir::new().unwrap();
        let (_, _, pool) = setup(&tmp);
        let pool = Arc::new(pool);

        let page_id = pool.new_page().unwrap().page_id();
        const OFFSET: usize = PAGE_HEADER_SIZE;

        for round in 0..20u8 {
            // Dirty the page with this round's value.
            {
                let mut guard = pool.pin_mut(page_id).unwrap();
                guard.page_mut()[OFFSET] = round;
            }

            // Two racing flushers: one claims the epoch, the other finds
            // the page clean-but-flushing and must WAIT (the pre-H1 fast
            // path returned immediately on `!dirty`).
            let barrier = Arc::new(Barrier::new(2));
            let spawn_flusher = || {
                let pool = Arc::clone(&pool);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    pool.flush(page_id).unwrap();
                })
            };
            let f1 = spawn_flusher();
            let f2 = spawn_flusher();
            f1.join().unwrap();
            f2.join().unwrap();

            // Both returned: the frame must be clean, not mid-flush, and
            // the durable bytes must be this round's.
            let frame_id = {
                let shard = pool.page_table[pool.shard_index(page_id)].lock();
                *shard.get(&page_id).expect("page must stay resident")
            };
            let meta = pool.frames[frame_id.0].meta.lock();
            assert!(!meta.flushing, "round {round}: flush left mid-flight");
            assert!(!meta.dirty, "round {round}: frame still dirty");
            drop(meta);
            let mut buf = [0u8; PAGE_SIZE];
            let offset = (page_id.0 - 1) * PAGE_SIZE as u64;
            pool.data_file.read_exact_at(&mut buf, offset).unwrap();
            assert_eq!(buf[OFFSET], round, "round {round}: stale bytes on disk");
        }
    }

    #[test]
    fn repeated_pin_unpin_does_not_leak_pin_count() {
        let tmp = TempDir::new().unwrap();
        let (_, _, pool) = setup(&tmp);

        let page_id = {
            let guard = pool.new_page().unwrap();
            guard.page_id()
        };

        // Pin and drop the same page many times. If pin_count were leaked,
        // the frame would eventually become unevictable.
        for _ in 0..16 {
            let guard = pool.pin(page_id).unwrap();
            assert_eq!(guard.page().len(), PAGE_SIZE);
            drop(guard);
        }

        // Force eviction by filling the pool and then some. This must not
        // panic with BufferPoolFull.
        let frame_count = pool.frame_count();
        for _ in 0..frame_count + 16 {
            drop(pool.new_page().unwrap());
        }
    }

    #[test]
    fn concurrent_pin_unpin_stress() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::thread;

        let tmp = TempDir::new().unwrap();
        let (_, _, pool) = setup(&tmp);
        let pool = Arc::new(pool);

        // Pre-allocate a set of shared pages for concurrent pin/unpin/new_page
        // traffic, plus one exclusive page per thread so we can verify that
        // written data is readable after the stress burst.
        let mut shared_ids = Vec::new();
        for _ in 0..8 {
            let guard = pool.new_page().unwrap();
            shared_ids.push(guard.page_id());
        }
        let mut owned_ids = Vec::new();
        for _ in 0..100 {
            let mut guard = pool.new_page().unwrap();
            guard.page_mut()[PAGE_HEADER_SIZE] = 0; // initial baseline
            owned_ids.push(guard.page_id());
        }

        let ops = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();

        for thread_id in 0..100usize {
            let p = Arc::clone(&pool);
            let shared = shared_ids.clone();
            let owned = owned_ids.clone();
            let o = Arc::clone(&ops);
            handles.push(thread::spawn(move || {
                for i in 0..20usize {
                    let action = (thread_id + i) % 5;
                    match action {
                        0 => {
                            if let Ok(g) = p.pin(shared[i % shared.len()]) {
                                assert_eq!(g.page().len(), PAGE_SIZE);
                                o.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        1 => {
                            if let Ok(mut g) = p.pin_mut(shared[i % shared.len()]) {
                                g.page_mut()[PAGE_HEADER_SIZE] = thread_id as u8;
                                o.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        2 => {
                            if let Ok(g) = p.new_page() {
                                o.fetch_add(1, Ordering::Relaxed);
                                drop(g);
                            }
                        }
                        3 => {
                            if let Ok(g) = p.pin(shared[i % shared.len()]) {
                                o.fetch_add(1, Ordering::Relaxed);
                                drop(g);
                            }
                        }
                        _ => {
                            // Write to the thread's exclusive page. The final
                            // value should be the last successful write.
                            if let Ok(mut g) = p.pin_mut(owned[thread_id]) {
                                g.page_mut()[PAGE_HEADER_SIZE] = (thread_id + 1) as u8;
                                g.page_mut()[PAGE_HEADER_SIZE + 1..PAGE_HEADER_SIZE + 9]
                                    .copy_from_slice(&i.to_be_bytes());
                                o.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // The exact count is not important; the test passes if there are no
        // deadlocks, panics, or data races detected by Miri/TSan/loom in later
        // stages.
        assert!(ops.load(Ordering::Relaxed) > 0);

        // Verify that every thread's exclusive page can be read back and
        // contains one of the values the owning thread wrote.
        for (thread_id, &owned_id) in owned_ids.iter().enumerate() {
            let guard = pool.pin(owned_id).unwrap();
            assert_eq!(guard.page()[PAGE_HEADER_SIZE], (thread_id + 1) as u8);
            let last_iteration = u64::from_be_bytes(
                guard.page()[PAGE_HEADER_SIZE + 1..PAGE_HEADER_SIZE + 9]
                    .try_into()
                    .unwrap(),
            );
            assert!(last_iteration < 20, "owned page {thread_id} corrupted");
        }

        // Buffer pool must still be usable after the stress burst.
        let guard = pool.new_page().unwrap();
        assert_eq!(guard.page().len(), PAGE_SIZE);

        // And frames must still be evictable (no pin_count leak).
        let frame_count = pool.frame_count();
        for _ in 0..frame_count + 16 {
            drop(pool.new_page().unwrap());
        }
    }

    proptest! {
        // Coding plan target is 10,000 cases. 64 keeps normal CI fast while
        // exercising allocate-write-read invariants; set PROPTEST_CASES to
        // override.
        #![proptest_config(ProptestConfig::with_cases(
            std::env::var("PROPTEST_CASES")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(64)
        ))]

        #[test]
        fn allocated_pages_are_unique_and_readable(count in 1usize..50) {
            let tmp = TempDir::new().unwrap();
            let (_, _, pool) = setup(&tmp);

            let mut ids = Vec::with_capacity(count);
            for i in 0..count {
                let mut guard = pool.new_page().unwrap();
                guard.page_mut()[PAGE_HEADER_SIZE] = (i % 256) as u8;
                guard.page_mut()[PAGE_HEADER_SIZE + 1..PAGE_HEADER_SIZE + 9].copy_from_slice(&(i as u64).to_be_bytes());
                ids.push(guard.page_id());
            }

            prop_assert_eq!(ids.len(), count);
            let mut sorted = ids.clone();
            sorted.sort_unstable();
            sorted.dedup();
            prop_assert_eq!(sorted.len(), ids.len(), "duplicate page IDs");

            for (i, id) in ids.iter().enumerate() {
                let guard = pool.pin(*id).unwrap();
                prop_assert_eq!(guard.page()[PAGE_HEADER_SIZE], (i % 256) as u8);
                prop_assert_eq!(&guard.page()[PAGE_HEADER_SIZE + 1..PAGE_HEADER_SIZE + 9], &(i as u64).to_be_bytes());
            }
        }
    }
}
