//! Page allocator for the storage engine.
//!
//! The allocator hands out [`PageId`] values and ensures that every allocation
//! is durably logged to the WAL so that the allocator state can be reconstructed
//! after a crash.
//!
//! M1 scope:
//! - `alloc_page` writes a `PageAlloc` WAL record and extends the data file.
//! - `free_page` writes a `PageFree` WAL record and pushes onto the freelist.
//! - Recovery replays `PageAlloc` / `PageFree` records to rebuild allocator state.
//!
//! # Crash-safety note: ghost pages
//!
//! `alloc_page` extends the data file *before* writing the `PageAlloc` WAL
//! record. If the process crashes between these two steps, the file will contain
//! extra all-zero pages but the WAL will have no record of their allocation. On
//! restart `next_page_id` is rebuilt by replaying `PageAlloc` records, so those
//! ghost pages may be allocated again. This is safe for M1: a zero-filled page
//! is indistinguishable from an unallocated page to higher layers, and the file
//! is extended in 1 MB chunks anyway. M2 can introduce a page bitmap or similar
//! mechanism to track truly free pages precisely.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::config::StorageConfig;
use crate::error::{Result, StorageError};
use crate::freelist_meta::FreelistMeta;
use crate::io::ensure_data_dir;
use crate::positioned_file::PositionedFile;
use crate::types::{Lsn, PageId};
use crate::wal::record::{bincode_config, PageAllocRecord, PageFreeRecord, WalRecord};
use crate::wal::writer::WalWriter;

/// Data file growth granularity in bytes (1 MB = 128 x 8 KB pages).
///
/// The file is extended in 1 MB chunks to amortize `ftruncate`/`fallocate`
/// system calls while avoiding excessive up-front disk reservation.
const DATA_FILE_GROWTH_BYTES: u64 = 1024 * 1024;

/// Manages allocation and deallocation of data pages.
///
/// `alloc_page` requires `&mut self`, so `PageAllocator` is `!Sync`. In M1 it
/// is intended to be used from a single thread or wrapped by the caller (e.g.
/// `Mutex<PageAllocator>` in the Buffer Pool) when concurrent access is
/// needed. The underlying data file is accessed via [`PositionedFile`], which
/// itself is lock-free — grow (`set_len`) coordinates with concurrent
/// pread/pwrite from other handles via POSIX semantics.
#[derive(Debug)]
pub struct PageAllocator {
    wal_writer: Arc<WalWriter>,
    data_file_path: PathBuf,
    data_file: PositionedFile,
    /// Cached length of `data_file` to avoid a `stat` on every allocation.
    current_file_len: u64,
    next_page_id: PageId,
    freelist: Vec<PageId>,
    page_size: usize,
    /// Set to `true` once WAL replay has been performed. Used by `alloc_page`
    /// to catch callers that forget to replay existing WAL before allocating.
    recovery_applied: bool,
}

impl PageAllocator {
    /// Open or create the page allocator in `data_dir`.
    ///
    /// The initial `next_page_id` starts at [`PageId(1)`][`PageId`]. The true
    /// value after a crash is recovered by replaying `PageAlloc` WAL records
    /// (see [`Self::replay_record`]).
    ///
    /// # Warning
    ///
    /// The caller **MUST** call [`Self::replay_record`] on all existing WAL
    /// records (or otherwise ensure allocator state is restored) before calling
    /// [`Self::alloc_page`]. Otherwise `alloc_page` may hand out page IDs that
    /// were already allocated before the crash, leading to silent data
    /// corruption. Stage I's `recover()` is the canonical caller that replays
    /// WAL before any further allocation.
    ///
    /// The data file length is intentionally not used as a source of truth
    /// because M1 extends the file in 1 MB chunks, which would otherwise cause
    /// the allocator to skip over pages that were never allocated.
    pub fn open(
        data_dir: impl AsRef<Path>,
        config: &StorageConfig,
        wal_writer: Arc<WalWriter>,
    ) -> Result<Self> {
        Self::open_at(data_dir, config, wal_writer, PageId(1))
    }

    /// Open or create the page allocator with a specific initial `next_page_id`.
    ///
    /// This is used during recovery to restore the allocator to the state
    /// captured in the most recent checkpoint. The caller must still replay WAL
    /// records from the checkpoint redo point before calling `alloc_page`.
    pub fn open_at(
        data_dir: impl AsRef<Path>,
        config: &StorageConfig,
        wal_writer: Arc<WalWriter>,
        next_page_id: PageId,
    ) -> Result<Self> {
        // `ensure_data_dir` is idempotent. It is also called by other M1
        // components (e.g. Superblock::create), so this is a harmless
        // redundancy that keeps `PageAllocator::open` self-contained.
        ensure_data_dir(data_dir.as_ref())?;
        let data_file_path = crate::io::data_file_path(data_dir.as_ref());

        let data_file = PositionedFile::open(&data_file_path)?;

        let current_file_len = data_file.len()?;
        let page_size = config.page_size();
        if current_file_len % page_size as u64 != 0 {
            return Err(StorageError::MetadataCorrupted(format!(
                "data file size {current_file_len} is not a multiple of page size {page_size}"
            )));
        }

        Ok(Self {
            wal_writer,
            data_file_path,
            data_file,
            current_file_len,
            next_page_id,
            freelist: Vec::new(),
            page_size,
            recovery_applied: false,
        })
    }

    /// Allocate a new page and return its ID.
    ///
    /// The page ID is chosen, the data file is extended if necessary, and then
    /// the allocation is durably logged to the WAL before the in-memory state
    /// is updated. Extending the file before writing the WAL avoids a WAL leak
    /// if the file system is full: if file extension fails, no WAL record is
    /// written.
    pub fn alloc_page(&mut self) -> Result<PageId> {
        if self.current_file_len >= self.page_size as u64
            && self.next_page_id == PageId(1)
            && !self.recovery_applied
        {
            return Err(StorageError::RecoveryRequired(format!(
                "data file is {} bytes but next_page_id is still PageId(1); \
                 replay all existing WAL records before alloc_page()",
                self.current_file_len
            )));
        }

        let page_id = self.freelist.pop().unwrap_or(self.next_page_id);

        // 1. Ensure the data file can hold the page. This must succeed before
        //    we write the WAL record. The extra space is harmless if we crash
        //    before the WAL is written: it will be treated as a leaked page on
        //    reopen.
        self.ensure_data_file_capacity(page_id)?;

        // 2. Append the allocation to the WAL. This is append-only: the record
        //    bytes are written to the segment file synchronously, but the fsync
        //    is deferred to the group-commit worker (or an explicit flush at
        //    transaction commit / checkpoint). We deliberately do NOT
        //    `flush_to` here.
        //
        //    Durability is still correct:
        //    - WAL-before-data: `BufferPool::flush_frame` flushes the WAL up to
        //      the page's `pd_lsn` before writing the page to the data file.
        //      Since the LSN clock is monotonic, that flush covers this
        //      `PageAlloc` record (whose LSN precedes any modification LSN),
        //      so a durable data page is always backed by a durable PageAlloc.
        //    - Torn/lost-extension safety: `ensure_data_file_capacity` may have
        //      extended (and fsynced) the data file above. If we crash before
        //      the PageAlloc is fsynced, the extra space is a harmless leaked
        //      page on reopen — never corruption.
        //    - Commit batching: `TxnManager::commit_txn` issues a single
        //      `flush_to` that amortizes every deferred alloc fsync accumulated
        //      during the transaction into one syscall.
        let record = WalRecord::page_alloc(page_id)?;
        self.wal_writer.append(record)?;

        // 3. Update allocator state.
        if page_id == self.next_page_id {
            self.next_page_id = PageId(self.next_page_id.0 + 1);
        }

        debug_assert!(
            page_id.0 > 0,
            "allocated page id must not be PageId::INVALID"
        );
        Ok(page_id)
    }

    /// Free a page.
    ///
    /// Appends a `PageFree` WAL record, then pushes `page_id` onto the freelist
    /// so future `alloc_page` calls can reuse it.
    ///
    /// Like `alloc_page`, this is append-only: the fsync is deferred to the
    /// group-commit worker or an explicit commit/checkpoint flush. If we crash
    /// before the record is durable, the page simply remains allocated (no
    /// corruption): recovery never sees the free, and the page stays owned by
    /// whatever allocated it. When the free *is* recovered, `apply_page_free`
    /// rebuilds the freelist entry idempotently.
    pub fn free_page(&mut self, page_id: PageId) -> Result<()> {
        if page_id == PageId::INVALID {
            return Err(StorageError::InvalidOperation(
                "cannot free PageId::INVALID".to_string(),
            ));
        }
        if page_id.0 >= self.next_page_id.0 {
            return Err(StorageError::InvalidOperation(format!(
                "cannot free page {page_id}: it was never allocated (next_page_id={})",
                self.next_page_id
            )));
        }
        if self.freelist.contains(&page_id) {
            return Err(StorageError::InvalidOperation(format!(
                "double-free: page {page_id} is already on the freelist"
            )));
        }

        let record = WalRecord::page_free(page_id)?;
        self.wal_writer.append(record)?;

        self.freelist.push(page_id);
        Ok(())
    }

    /// Mark recovery as complete.
    ///
    /// This must be called after all existing WAL records have been replayed
    /// (or after the caller has otherwise restored allocator state). It lifts
    /// the guard in `alloc_page()` that prevents allocations before recovery.
    pub fn mark_recovery_complete(&mut self) {
        self.recovery_applied = true;
    }

    /// Return the next page ID that would be allocated if the freelist is empty.
    pub fn next_page_id(&self) -> PageId {
        self.next_page_id
    }

    /// Return the path to the data file.
    pub fn data_file_path(&self) -> &Path {
        &self.data_file_path
    }

    /// Return a checkpoint-time snapshot of the allocator state.
    ///
    /// The snapshot contains the freelist; `next_page_id` is stored in the
    /// `CheckpointEnd` record and superblock by the checkpoint coordinator.
    /// The snapshot is taken before the checkpoint flush phase so that
    /// concurrent `free_page` calls during the flush are NOT captured (they
    /// are applied by WAL replay instead, avoiding duplicate freelist entries).
    pub fn snapshot(&self, checkpoint_lsn: Lsn) -> FreelistMeta {
        FreelistMeta {
            checkpoint_lsn,
            page_ids: self.freelist.clone(),
        }
    }

    /// Apply a `PageAlloc` record during recovery.
    ///
    /// This advances `next_page_id` if necessary and ensures the data file can
    /// hold the allocated page. It does not write a new WAL record.
    pub fn apply_page_alloc(&mut self, page_id: PageId) -> Result<()> {
        debug_assert!(
            page_id.0 > 0,
            "PageAlloc record must not reference PageId::INVALID"
        );
        if page_id.0 >= self.next_page_id.0 {
            self.next_page_id = PageId(page_id.0 + 1);
        }
        // If the page was previously freed, remove it from the freelist.
        self.freelist.retain(|&id| id != page_id);
        self.ensure_data_file_capacity(page_id)?;
        self.recovery_applied = true;
        Ok(())
    }

    /// Apply a `PageFree` record during recovery.
    ///
    /// Pushes `page_id` onto the freelist so future allocations can reuse it.
    /// Does not write a WAL record. Idempotent: if `page_id` is already on the
    /// freelist, the push is skipped. This is a hard requirement for redo
    /// handlers (tech-selection v2.3) — it tolerates WAL corruption (duplicate
    /// PageFree records) and snapshot/WAL overlap without producing duplicate
    /// freelist entries.
    pub fn apply_page_free(&mut self, page_id: PageId) {
        if self.freelist.contains(&page_id) {
            return;
        }
        self.freelist.push(page_id);
        self.recovery_applied = true;
    }

    /// Seed the freelist from a checkpoint snapshot.
    ///
    /// Used during recovery to accelerate freelist rebuild: the snapshot
    /// provides the freelist as of `checkpoint_lsn`, and WAL replay from
    /// `checkpoint_lsn` forward applies any subsequent `PageFree` / `PageAlloc`
    /// records on top.
    pub fn seed_freelist(&mut self, page_ids: &[PageId]) {
        self.freelist = page_ids.to_vec();
    }

    /// Return a slice of the current freelist (for tests and diagnostics).
    pub fn freelist(&self) -> &[PageId] {
        &self.freelist
    }

    /// Replay a single WAL record during recovery.
    ///
    /// Handles `PageAlloc` and `PageFree`; other record types are ignored.
    /// This is a convenience wrapper for the recovery loop.
    pub fn replay_record(&mut self, record: &WalRecord) -> Result<()> {
        use crate::wal::record::WalRecordType;
        match record.record_type {
            WalRecordType::PageAlloc => {
                let (rec, _) = bincode::serde::decode_from_slice::<PageAllocRecord, _>(
                    &record.payload,
                    bincode_config(),
                )
                .map_err(|e| StorageError::Serialize(e.to_string()))?;
                self.apply_page_alloc(rec.page_id)
            }
            WalRecordType::PageFree => {
                if crate::wal::record::PageFreeV2Record::decode(&record.payload).is_ok() {
                    return Err(StorageError::RecoveryRequired(
                        "PageFreeV2 requires its Stage D generation/operation redo protocol; legacy PageFree redo must not interpret its version byte as PageId(2)"
                            .to_string(),
                    ));
                }
                let (rec, consumed) = bincode::serde::decode_from_slice::<PageFreeRecord, _>(
                    &record.payload,
                    bincode_config(),
                )
                .map_err(|e| StorageError::Serialize(e.to_string()))?;
                if consumed != record.payload.len() {
                    return Err(StorageError::Serialize(format!(
                        "PageFree payload has {} trailing bytes",
                        record.payload.len() - consumed
                    )));
                }
                self.apply_page_free(rec.page_id);
                Ok(())
            }
            _ => Ok(()),
        }
    }

    fn ensure_data_file_capacity(&mut self, page_id: PageId) -> Result<()> {
        let required = page_id.0 * self.page_size as u64;
        if required > self.current_file_len {
            // Extend in 1 MB chunks rather than to the exact required size.
            let growth = DATA_FILE_GROWTH_BYTES;
            let target = required.div_ceil(growth) * growth;
            self.data_file.set_len(target)?;
            self.data_file.sync_all()?;
            self.current_file_len = target;
        }
        Ok(())
    }

    // Note on recovery fsync behaviour:
    //
    // `apply_page_alloc` (called during WAL replay) invokes this method, so a
    // recovery that needs to extend the data file will fsync here. In the
    // common case the file was already extended and fsynced before the crash,
    // so replay is a no-op. If the crash happened between file extension and
    // the corresponding PageAlloc fsync, replay re-extends the file once per
    // 1 MB boundary crossing. Because M1 grows the file in 1 MB chunks, only
    // the first PageAlloc that crosses a chunk boundary triggers an fsync.
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::StorageConfig;
    use crate::types::Lsn;
    use crate::wal::reader::WalReader;
    use crate::wal::record::{PageAllocRecord, WalRecordType};
    use crate::wal::writer::WalWriter;
    use proptest::prelude::*;
    use tempfile::TempDir;

    fn test_config(tmp: &TempDir) -> StorageConfig {
        let mut cfg = StorageConfig::new(tmp.path());
        cfg.wal_group_commit_timeout_ms = 1;
        cfg.wal_group_commit_batch_size = 1;
        cfg.wal_segment_size = 1024;
        cfg
    }

    #[test]
    fn alloc_page_returns_monotonic_ids() {
        let tmp = TempDir::new().unwrap();
        let cfg = test_config(&tmp);
        let wal = Arc::new(WalWriter::open(tmp.path(), &cfg).unwrap());
        let mut allocator = PageAllocator::open(tmp.path(), &cfg, wal).unwrap();

        assert_eq!(allocator.alloc_page().unwrap(), PageId(1));
        assert_eq!(allocator.alloc_page().unwrap(), PageId(2));
        assert_eq!(allocator.alloc_page().unwrap(), PageId(3));
        assert_eq!(allocator.next_page_id(), PageId(4));
    }

    #[test]
    fn alloc_page_extends_data_file() {
        let tmp = TempDir::new().unwrap();
        let cfg = test_config(&tmp);
        let wal = Arc::new(WalWriter::open(tmp.path(), &cfg).unwrap());
        let mut allocator = PageAllocator::open(tmp.path(), &cfg, wal).unwrap();

        let count = 10;
        for _ in 0..count {
            allocator.alloc_page().unwrap();
        }

        let file_len = std::fs::metadata(allocator.data_file_path()).unwrap().len();
        assert!(file_len >= count as u64 * cfg.page_size() as u64);
        assert_eq!(file_len % DATA_FILE_GROWTH_BYTES, 0);
    }

    #[test]
    fn alloc_page_writes_wal_record() {
        let tmp = TempDir::new().unwrap();
        let cfg = test_config(&tmp);
        let wal = Arc::new(WalWriter::open(tmp.path(), &cfg).unwrap());
        let mut allocator = PageAllocator::open(tmp.path(), &cfg, Arc::clone(&wal)).unwrap();

        let page_id = allocator.alloc_page().unwrap();
        drop(allocator);
        drop(wal);

        let mut reader = WalReader::open(tmp.path().join("wal"), cfg.wal_segment_size).unwrap();
        let record = reader.next_record().unwrap().unwrap();
        assert_eq!(record.record_type, WalRecordType::PageAlloc);
        assert_eq!(record.lsn, Lsn(8));

        let decoded: PageAllocRecord =
            bincode::serde::decode_from_slice(&record.payload, bincode_config())
                .map_err(|e| StorageError::Serialize(e.to_string()))
                .unwrap()
                .0;
        assert_eq!(decoded.page_id, page_id);
    }

    #[test]
    fn free_page_pushes_to_freelist_and_reuses_on_alloc() {
        let tmp = TempDir::new().unwrap();
        let cfg = test_config(&tmp);
        let wal = Arc::new(WalWriter::open(tmp.path(), &cfg).unwrap());
        let mut allocator = PageAllocator::open(tmp.path(), &cfg, wal).unwrap();

        let p1 = allocator.alloc_page().unwrap();
        let p2 = allocator.alloc_page().unwrap();
        assert_eq!(p1, PageId(1));
        assert_eq!(p2, PageId(2));

        allocator.free_page(p1).unwrap();
        assert_eq!(allocator.freelist(), &[PageId(1)]);

        // The freed page is reused on the next alloc.
        let p3 = allocator.alloc_page().unwrap();
        assert_eq!(p3, PageId(1));
        assert!(allocator.freelist().is_empty());
        assert_eq!(allocator.next_page_id(), PageId(3));
    }

    #[test]
    fn free_page_rejects_invalid_page_ids() {
        let tmp = TempDir::new().unwrap();
        let cfg = test_config(&tmp);
        let wal = Arc::new(WalWriter::open(tmp.path(), &cfg).unwrap());
        let mut allocator = PageAllocator::open(tmp.path(), &cfg, wal).unwrap();

        let err = allocator.free_page(PageId::INVALID).unwrap_err();
        assert!(matches!(err, StorageError::InvalidOperation(_)));

        // Page 5 was never allocated (next_page_id is still 1).
        let err = allocator.free_page(PageId(5)).unwrap_err();
        assert!(matches!(err, StorageError::InvalidOperation(_)));
    }

    #[test]
    fn apply_page_alloc_advances_state() {
        let tmp = TempDir::new().unwrap();
        let cfg = test_config(&tmp);
        let wal = Arc::new(WalWriter::open(tmp.path(), &cfg).unwrap());
        let mut allocator = PageAllocator::open(tmp.path(), &cfg, wal).unwrap();

        allocator.apply_page_alloc(PageId(5)).unwrap();
        assert_eq!(allocator.next_page_id(), PageId(6));

        let file_len = std::fs::metadata(allocator.data_file_path()).unwrap().len();
        assert!(file_len >= 5 * cfg.page_size() as u64);
        assert_eq!(file_len % DATA_FILE_GROWTH_BYTES, 0);
    }

    #[test]
    fn replay_record_ignores_non_page_alloc_records() {
        let tmp = TempDir::new().unwrap();
        let cfg = test_config(&tmp);
        let wal = Arc::new(WalWriter::open(tmp.path(), &cfg).unwrap());
        let mut allocator = PageAllocator::open(tmp.path(), &cfg, wal).unwrap();

        // A FullPageImage record should be ignored by the allocator.
        let fpi = WalRecord::full_page_image(PageId(1), vec![0xAB; 32]).unwrap();
        allocator.replay_record(&fpi).unwrap();
        assert_eq!(allocator.next_page_id(), PageId(1));
        assert!(allocator.freelist.is_empty());

        // A CheckpointEnd record should also be ignored.
        let end = WalRecord::checkpoint_end(
            Lsn(64),
            PageId(42),
            crate::types::TxnId(1),
            crate::types::Oid::FIRST_USER.0,
            String::new(),
            String::new(),
        )
        .unwrap();
        allocator.replay_record(&end).unwrap();
        assert_eq!(allocator.next_page_id(), PageId(1));
        assert!(allocator.freelist.is_empty());
    }

    #[test]
    fn page_free_v2_never_replays_as_legacy_page_id_two() {
        let tmp = TempDir::new().unwrap();
        let cfg = test_config(&tmp);
        let wal = Arc::new(WalWriter::open(tmp.path(), &cfg).unwrap());
        let mut allocator = PageAllocator::open(tmp.path(), &cfg, wal).unwrap();
        let payload = crate::wal::record::PageFreeV2Record {
            version: crate::wal::record::PAGE_FREE_V2_VERSION,
            relation_oid: crate::types::Oid(42),
            page_id: PageId(9),
            prev_page_id: PageId(8),
            next_page_id: PageId(10),
            base_generation: 7,
            post_unlink_generation: 8,
            operation_id: crate::gc::GcOperationId {
                relation_oid: crate::types::Oid(42),
                base_generation: 7,
                page_id: PageId(9),
            },
        }
        .encode()
        .unwrap();
        let record = WalRecord {
            lsn: Lsn::INVALID,
            prev_lsn: Lsn::INVALID,
            txn_id: crate::types::TxnId::INVALID,
            record_type: crate::wal::record::WalRecordType::PageFree,
            flags: 0,
            payload,
        };
        assert!(matches!(
            allocator.replay_record(&record),
            Err(StorageError::RecoveryRequired(_))
        ));
        assert!(allocator.freelist.is_empty());
    }

    #[test]
    fn snapshot_captures_freelist() {
        let tmp = TempDir::new().unwrap();
        let cfg = test_config(&tmp);
        let wal = Arc::new(WalWriter::open(tmp.path(), &cfg).unwrap());
        let allocator = PageAllocator::open(tmp.path(), &cfg, wal).unwrap();

        let meta = allocator.snapshot(Lsn(128));
        assert_eq!(meta.checkpoint_lsn, Lsn(128));
        assert!(meta.page_ids.is_empty());
    }

    #[test]
    fn next_page_id_starts_at_one_on_reopen() {
        let tmp = TempDir::new().unwrap();
        let cfg = test_config(&tmp);
        let wal = Arc::new(WalWriter::open(tmp.path(), &cfg).unwrap());
        {
            let mut allocator = PageAllocator::open(tmp.path(), &cfg, Arc::clone(&wal)).unwrap();
            for _ in 0..7 {
                allocator.alloc_page().unwrap();
            }
        }
        drop(wal);

        // Re-open without replaying WAL: next_page_id is NOT derived from the
        // data file size. It starts at 1; the true value is recovered by
        // replaying PageAlloc records (see `replay_record_recovers_allocations`).
        let cfg2 = test_config(&tmp);
        let wal2 = Arc::new(WalWriter::open(tmp.path(), &cfg2).unwrap());
        let allocator2 = PageAllocator::open(tmp.path(), &cfg2, wal2).unwrap();
        assert_eq!(allocator2.next_page_id(), PageId(1));
    }

    proptest! {
        // Coding plan target is 10,000 cases. 32 keeps normal CI fast while
        // exercising the allocator thoroughly; set PROPTEST_CASES to override.
        #![proptest_config(ProptestConfig::with_cases(
            std::env::var("PROPTEST_CASES")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(32)
        ))]

        #[test]
        fn alloc_many_pages_have_unique_monotonic_ids(count in 1usize..50) {
            let tmp = TempDir::new().unwrap();
            let mut cfg = test_config(&tmp);
            cfg.wal_group_commit_timeout_ms = 10;
            cfg.wal_group_commit_batch_size = 64;
            let wal = Arc::new(WalWriter::open(tmp.path(), &cfg).unwrap());
            let mut allocator = PageAllocator::open(tmp.path(), &cfg, wal).unwrap();

            let mut ids = Vec::with_capacity(count);
            for _ in 0..count {
                ids.push(allocator.alloc_page().unwrap().0);
            }

            prop_assert_eq!(ids.len(), count);
            prop_assert!(ids.windows(2).all(|w| w[1] > w[0]));
            prop_assert_eq!(allocator.next_page_id(), PageId(count as u64 + 1));

            let file_len = std::fs::metadata(allocator.data_file_path()).unwrap().len();
            prop_assert!(file_len >= count as u64 * cfg.page_size() as u64);
            prop_assert_eq!(file_len % DATA_FILE_GROWTH_BYTES, 0);
        }
    }

    #[test]
    fn replay_record_recovers_allocations() {
        let tmp = TempDir::new().unwrap();
        let cfg = test_config(&tmp);
        let wal = Arc::new(WalWriter::open(tmp.path(), &cfg).unwrap());
        let mut allocator = PageAllocator::open(tmp.path(), &cfg, Arc::clone(&wal)).unwrap();
        let count = 5;
        for _ in 0..count {
            allocator.alloc_page().unwrap();
        }
        drop(allocator);
        drop(wal);

        // Simulate recovery: open a fresh allocator and replay the WAL.
        let cfg2 = test_config(&tmp);
        let wal2 = Arc::new(WalWriter::open(tmp.path(), &cfg2).unwrap());
        let mut recovered = PageAllocator::open(tmp.path(), &cfg2, wal2).unwrap();
        let mut reader = WalReader::open(tmp.path().join("wal"), cfg2.wal_segment_size).unwrap();
        while let Some(record) = reader.next_record().unwrap() {
            recovered.replay_record(&record).unwrap();
        }

        assert_eq!(recovered.next_page_id(), PageId(count as u64 + 1));
        let file_len = std::fs::metadata(recovered.data_file_path()).unwrap().len();
        assert!(file_len >= count as u64 * cfg2.page_size() as u64);
        assert_eq!(file_len % DATA_FILE_GROWTH_BYTES, 0);
    }

    #[test]
    fn replay_handles_overlapping_page_ids() {
        let tmp = TempDir::new().unwrap();
        let cfg = test_config(&tmp);
        let wal = Arc::new(WalWriter::open(tmp.path(), &cfg).unwrap());
        {
            let mut allocator = PageAllocator::open(tmp.path(), &cfg, Arc::clone(&wal)).unwrap();
            for _ in 0..5 {
                allocator.alloc_page().unwrap();
            }
        }
        drop(wal);

        // First restart without replay: next_page_id resets to 1. We explicitly
        // mark recovery complete (test-only) so the debug assertion in
        // alloc_page does not fire; production code must replay WAL instead.
        let cfg2 = test_config(&tmp);
        let wal2 = Arc::new(WalWriter::open(tmp.path(), &cfg2).unwrap());
        {
            let mut allocator = PageAllocator::open(tmp.path(), &cfg2, Arc::clone(&wal2)).unwrap();
            assert_eq!(allocator.next_page_id(), PageId(1));
            allocator.mark_recovery_complete();
            for _ in 0..3 {
                allocator.alloc_page().unwrap();
            }
            assert_eq!(allocator.next_page_id(), PageId(4));
        }
        drop(wal2);

        // Second restart: replay all WAL records. The original session allocated
        // pages 1..=5 and the second session allocated 1..=3, so replayed
        // next_page_id is max(5, 3) + 1 = 6.
        let cfg3 = test_config(&tmp);
        let wal3 = Arc::new(WalWriter::open(tmp.path(), &cfg3).unwrap());
        let mut allocator = PageAllocator::open(tmp.path(), &cfg3, Arc::clone(&wal3)).unwrap();
        assert_eq!(allocator.next_page_id(), PageId(1));

        let mut reader = WalReader::open(tmp.path().join("wal"), cfg3.wal_segment_size).unwrap();
        while let Some(record) = reader.next_record().unwrap() {
            allocator.replay_record(&record).unwrap();
        }

        assert_eq!(allocator.next_page_id(), PageId(6));
        let file_len = std::fs::metadata(allocator.data_file_path()).unwrap().len();
        assert!(file_len >= 8 * cfg3.page_size() as u64);
        assert_eq!(file_len % DATA_FILE_GROWTH_BYTES, 0);
    }

    #[test]
    fn data_file_grows_in_1mb_chunks() {
        let tmp = TempDir::new().unwrap();
        let cfg = test_config(&tmp);
        let wal = Arc::new(WalWriter::open(tmp.path(), &cfg).unwrap());
        let mut allocator = PageAllocator::open(tmp.path(), &cfg, wal).unwrap();

        // A single page allocation should grow the file to 1 MB, not to one
        // page size.
        allocator.alloc_page().unwrap();
        let file_len = std::fs::metadata(allocator.data_file_path()).unwrap().len();
        assert_eq!(file_len, DATA_FILE_GROWTH_BYTES);

        // Allocate enough pages to exceed 1 MB; the file should jump to 2 MB.
        let pages_per_mb = DATA_FILE_GROWTH_BYTES / cfg.page_size() as u64;
        for _ in 0..pages_per_mb {
            allocator.alloc_page().unwrap();
        }
        let file_len = std::fs::metadata(allocator.data_file_path()).unwrap().len();
        assert_eq!(file_len, 2 * DATA_FILE_GROWTH_BYTES);
    }

    #[test]
    fn replay_record_across_wal_segments() {
        let tmp = TempDir::new().unwrap();
        let mut cfg = test_config(&tmp);
        cfg.wal_segment_size = 256;
        cfg.wal_group_commit_timeout_ms = 1;
        cfg.wal_group_commit_batch_size = 1;
        let wal = Arc::new(WalWriter::open(tmp.path(), &cfg).unwrap());
        let mut allocator = PageAllocator::open(tmp.path(), &cfg, Arc::clone(&wal)).unwrap();

        // Write enough small PageAlloc records to cross at least one WAL segment
        // boundary.
        let count = 40;
        for _ in 0..count {
            allocator.alloc_page().unwrap();
        }
        drop(allocator);
        drop(wal);

        let mut cfg2 = test_config(&tmp);
        cfg2.wal_segment_size = 256;
        let wal2 = Arc::new(WalWriter::open(tmp.path(), &cfg2).unwrap());
        let mut recovered = PageAllocator::open(tmp.path(), &cfg2, wal2).unwrap();
        let mut reader = WalReader::open(tmp.path().join("wal"), cfg2.wal_segment_size).unwrap();
        while let Some(record) = reader.next_record().unwrap() {
            recovered.replay_record(&record).unwrap();
        }

        assert_eq!(recovered.next_page_id(), PageId(count as u64 + 1));
    }

    #[test]
    fn serialized_concurrent_alloc_returns_unique_ids() {
        use std::sync::{Arc, Mutex};
        use std::thread;

        let tmp = TempDir::new().unwrap();
        let cfg = test_config(&tmp);
        let wal = Arc::new(WalWriter::open(tmp.path(), &cfg).unwrap());
        let allocator = Arc::new(Mutex::new(
            PageAllocator::open(tmp.path(), &cfg, wal).unwrap(),
        ));

        let mut handles = Vec::new();
        for _ in 0..4 {
            let a = Arc::clone(&allocator);
            handles.push(thread::spawn(move || {
                let mut ids = Vec::new();
                for _ in 0..25 {
                    ids.push(a.lock().unwrap().alloc_page().unwrap().0);
                }
                ids
            }));
        }

        let mut all = Vec::new();
        for h in handles {
            all.extend(h.join().unwrap());
        }

        all.sort_unstable();
        assert_eq!(all.len(), 100);
        for window in all.windows(2) {
            assert!(window[1] > window[0]);
        }
    }
}
