//! Redo dispatch for crash recovery: [`RedoHandler`], [`RedoContext`], and
//! [`RedoRegistry`].
//!
//! Recovery replays WAL records through a registry that maps each
//! [`WalRecordType`] to exactly one handler. Two invariants come from the M2
//! tech-selection (§11.6, v2.3-24):
//!
//! - registering a second handler for the same record type **panics** — a
//!   duplicate registration is a programming error, not a runtime condition;
//! - applying a record whose type has no registered handler is a **hard
//!   failure** ([`StorageError::UnknownRecord`]) — recovery must never
//!   silently skip redo.
//!
//! M2 stages (heap, B+Tree, transactions) will register their own handlers
//! from their crates; `Engine::open` assembles them in one place (Stage F).

use std::collections::HashMap;
use std::sync::Arc;

use crate::sync::Mutex;

use crate::buffer_pool::BufferPool;
use crate::clog::ClogAccessor;
use crate::error::{Result, StorageError};
use crate::page::{page_pd_lsn, set_page_pd_lsn};
use crate::page_allocator::PageAllocator;
use crate::positioned_file::PositionedFile;
use crate::types::{Lsn, PageId, TxnId, PAGE_SIZE};
use crate::wal::record::{bincode_config, FullPageImageRecord, WalRecord, WalRecordType};
use crate::wal::WalWriter;

/// Active transaction table (ARIES analysis).
///
/// Stage D provides the type so that [`RedoContext`] is complete. The
/// analysis phase itself lives in [`crate::analysis`] (Stage N): it rebuilds
/// the crash-time ATT and exposes it via
/// [`crate::engine::StorageEngine::recovered_active_xids`]. Redo handlers
/// currently do not consult this table.
#[derive(Debug, Default)]
pub struct ActiveXactTable {
    last_lsn: HashMap<TxnId, Lsn>,
}

impl ActiveXactTable {
    /// Create an empty table.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record the latest WAL LSN of an active transaction.
    pub fn add(&mut self, txn_id: TxnId, lsn: Lsn) {
        self.last_lsn.insert(txn_id, lsn);
    }

    /// Remove a transaction (on commit/abort).
    pub fn remove(&mut self, txn_id: TxnId) {
        self.last_lsn.remove(&txn_id);
    }

    /// Number of tracked transactions.
    pub fn len(&self) -> usize {
        self.last_lsn.len()
    }

    /// True if no transactions are tracked.
    pub fn is_empty(&self) -> bool {
        self.last_lsn.is_empty()
    }
}

/// Dirty page table (ARIES analysis).
///
/// Maps a dirty page to its recovery LSN (the oldest record that may need
/// redoing for it). The analysis phase's rebuilt DPT lives in
/// [`crate::analysis::AnalysisResult`]; this type completes [`RedoContext`]
/// and is not consulted by redo handlers yet.
#[derive(Debug, Default)]
pub struct DirtyPageTable {
    rec_lsn: HashMap<PageId, Lsn>,
}

/// Source of the active transaction IDs captured into a checkpoint's ATT
/// snapshot (M2b Stage N; tech-selection §11.4).
///
/// The checkpoint coordinator lives in `pg-storage`, but the set of
/// in-flight transactions lives in `pg-txn` — which itself depends on
/// `pg-storage`. This trait keeps the dependency direction intact: the
/// coordinator holds a `dyn AttProvider`, and `pg-txn::TxnManager`
/// implements it. The engine wires the provider at open time via
/// [`crate::checkpoint::CheckpointCoordinator::set_att_provider`]; until a
/// provider is installed (M1/M2a configurations) checkpoints snapshot an
/// empty ATT, which analysis reads as "no snapshot — rebuild by a full WAL
/// scan from the checkpoint LSN".
pub trait AttProvider: std::fmt::Debug + Send + Sync {
    /// Return the XIDs of all transactions that have begun but not yet
    /// committed or aborted.
    fn active_xids(&self) -> Vec<TxnId>;
}

impl DirtyPageTable {
    /// Create an empty table.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that `page_id` is dirty as of `rec_lsn`.
    pub fn add(&mut self, page_id: PageId, rec_lsn: Lsn) {
        self.rec_lsn.insert(page_id, rec_lsn);
    }

    /// The recovery LSN of a page, if it is tracked as dirty.
    pub fn get(&self, page_id: PageId) -> Option<Lsn> {
        self.rec_lsn.get(&page_id).copied()
    }

    /// Number of tracked dirty pages.
    pub fn len(&self) -> usize {
        self.rec_lsn.len()
    }

    /// True if no dirty pages are tracked.
    pub fn is_empty(&self) -> bool {
        self.rec_lsn.is_empty()
    }
}

/// Tracks B+Tree splits that were started (Prepare) but never finished
/// (Commit) before a crash. Populated during redo and consumed during undo.
#[derive(Debug, Default)]
pub struct IncompleteSplitTracker {
    splits: HashMap<PageId, IncompleteSplit>,
}

/// A single incomplete split (Stage S, §11.3).
#[derive(Debug, Clone)]
pub struct IncompleteSplit {
    /// Page ID of the left half of the split.
    pub left_page: PageId,
    /// Page ID of the right half of the split.
    pub right_page: PageId,
    /// B+Tree level of the split page.
    pub level: u8,
    /// Original next-sibling pointer of the left page before the split.
    pub left_old_next: PageId,
    /// Slot where the Copy phase began (None if only Prepare was reached).
    pub copy_start_slot: Option<u16>,
    /// LSN of the SplitPrepare record that opened the split (post-Stage-S
    /// review B8): carried into the CLR's `redo_ref_lsn` for diagnostics.
    /// `Lsn::INVALID` when the split was detected by the undo-time page
    /// scan (its Prepare predates the replay window, so no LSN is known).
    pub prepare_lsn: Lsn,
}

impl IncompleteSplitTracker {
    /// Create an empty tracker.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a split that reached the Prepare phase, keeping the Prepare
    /// record's LSN for the CLR's diagnostic `redo_ref_lsn` (B8).
    pub fn mark_prepare(
        &mut self,
        left_page: PageId,
        right_page: PageId,
        level: u8,
        left_old_next: PageId,
        prepare_lsn: Lsn,
    ) {
        self.splits.insert(
            left_page,
            IncompleteSplit {
                left_page,
                right_page,
                level,
                left_old_next,
                copy_start_slot: None,
                prepare_lsn,
            },
        );
    }

    /// Record that the Copy phase was reached (updates `copy_start_slot`).
    pub fn mark_copy(&mut self, left_page: PageId, copy_start_slot: u16) {
        if let Some(split) = self.splits.get_mut(&left_page) {
            split.copy_start_slot = Some(copy_start_slot);
        }
    }

    /// Remove a split (Commit reached — split is complete, no undo needed).
    pub fn clear(&mut self, left_page: PageId) {
        self.splits.remove(&left_page);
    }

    /// All incomplete splits, keyed by left page.
    pub fn incomplete_splits(&self) -> &HashMap<PageId, IncompleteSplit> {
        &self.splits
    }

    /// Returns true if no incomplete splits are tracked.
    pub fn is_empty(&self) -> bool {
        self.splits.is_empty()
    }
}

/// Context passed to every redo handler during replay.
///
/// All M2 stages compile against this struct from day one; the set of
/// collaborators a handler may touch is fixed here.
pub struct RedoContext<'a> {
    /// The buffer pool, if it exists at replay time.
    ///
    /// M1 recovery replays *before* opening the buffer pool (FPI images are
    /// written straight to the data file), so this is `None` there. AM redo
    /// handlers introduced from Stage I must tolerate `None` or run at a
    /// phase where the pool exists.
    pub buffer_pool: Option<&'a BufferPool>,
    /// The page allocator, for `PageAlloc` / `PageFree` redo.
    pub page_allocator: &'a Mutex<PageAllocator>,
    /// Commit-status accessor (M1: [`crate::clog::NoOpClogAccessor`]).
    pub clog: &'a dyn ClogAccessor,
    /// Active transaction table (Stage N).
    pub att: &'a mut ActiveXactTable,
    /// Dirty page table (Stage N).
    pub dpt: &'a mut DirtyPageTable,
    /// Incomplete B+Tree split tracker (Stage S, §11.3): populated during
    /// redo by split Prepare/Copy/Commit handlers; consumed by undo handlers
    /// to finish or roll back incomplete splits.
    pub incomplete_splits: &'a mut IncompleteSplitTracker,
}

/// Redo handler for one [`WalRecordType`].
pub trait RedoHandler: Send + Sync {
    /// The record type this handler applies.
    fn kind(&self) -> WalRecordType;
    /// Apply `record` to the on-disk/in-memory state.
    fn apply(&self, record: &WalRecord, ctx: &mut RedoContext<'_>) -> Result<()>;
}

/// Undo handler (Stage S, §11.3): runs after redo + ATT filter to reverse
/// or complete the effects of crashed transactions. Injected from
/// `pg-engine` (which depends on all AM crates) since `pg-storage` cannot
/// depend on `pg-am-btree` or `pg-am-heap`.
pub trait UndoHandler: Send + Sync {
    /// Reverse or complete the effects of crashed transactions.
    fn undo(&self, ctx: &mut UndoContext<'_>) -> Result<()>;
}

/// Context passed to undo handlers.
pub struct UndoContext<'a> {
    /// Buffer pool for reading/writing pages.
    pub buffer_pool: &'a BufferPool,
    /// WAL writer for emitting CLR records.
    pub wal_writer: &'a WalWriter,
    /// Commit-status accessor (for stamping aborted XIDs).
    ///
    /// Undo handlers stamp ATT members `Aborted` via `set_state` with no WAL
    /// record and no explicit flush: the marks are in-memory until the first
    /// post-recovery checkpoint's CLOG flush (`ClogFlush` hook). A crash
    /// before that checkpoint loses them harmlessly — the next recovery
    /// re-derives the same ATT from the WAL and re-stamps (the marks are
    /// idempotent), and a missing CLOG entry already reads `InProgress`,
    /// i.e. MVCC-invisible (the Stage N "no explicit heap undo" decision).
    pub clog: &'a dyn ClogAccessor,
    /// XIDs of transactions that were active at crash time (the ATT).
    pub att: &'a [TxnId],
    /// Incomplete B+Tree splits detected during redo.
    pub incomplete_splits: &'a IncompleteSplitTracker,
    /// Page allocator (for scanning allocated pages to find meta pages).
    pub page_allocator: &'a Mutex<PageAllocator>,
}

/// Maps each [`WalRecordType`] to exactly one [`RedoHandler`].
#[derive(Default)]
pub struct RedoRegistry {
    handlers: HashMap<WalRecordType, Box<dyn RedoHandler>>,
}

impl RedoRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `handler`.
    ///
    /// # Panics
    ///
    /// Panics if a handler for the same record type is already registered:
    /// duplicate registration is a programming error (tech-selection
    /// v2.3-24), not a recoverable runtime condition.
    pub fn register(&mut self, handler: Box<dyn RedoHandler>) {
        let kind = handler.kind();
        if self.handlers.insert(kind, handler).is_some() {
            panic!("duplicate redo handler registered for {kind:?}");
        }
    }

    /// Dispatch `record` to its handler.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::UnknownRecord`] if no handler is registered
    /// for the record's type — recovery must never silently skip redo.
    pub fn apply(&self, record: &WalRecord, ctx: &mut RedoContext<'_>) -> Result<()> {
        let handler = self.handlers.get(&record.record_type).ok_or({
            StorageError::UnknownRecord {
                record_type: record.record_type.to_u8(),
                lsn: record.lsn,
            }
        })?;
        handler.apply(record, ctx)
    }
}

/// Redo handler for `PageAlloc` records: advances the page allocator past
/// the allocated page.
pub struct PageAllocRedoHandler;

impl RedoHandler for PageAllocRedoHandler {
    fn kind(&self) -> WalRecordType {
        WalRecordType::PageAlloc
    }

    fn apply(&self, record: &WalRecord, ctx: &mut RedoContext<'_>) -> Result<()> {
        ctx.page_allocator.lock().replay_record(record)
    }
}

/// Redo handler for `PageFree` records: pushes the freed page back onto the
/// allocator's freelist so it can be reused after recovery.
pub struct PageFreeRedoHandler;

impl RedoHandler for PageFreeRedoHandler {
    fn kind(&self) -> WalRecordType {
        WalRecordType::PageFree
    }

    fn apply(&self, record: &WalRecord, ctx: &mut RedoContext<'_>) -> Result<()> {
        ctx.page_allocator.lock().replay_record(record)
    }
}

/// Redo handler for `FullPageImage` records.
///
/// From Stage I onwards recovery opens the buffer pool *before* replay, so
/// FPI images are applied through the pool (pin_mut → overwrite → patch
/// pd_lsn → drop marks dirty), sharing the same dispatch and idempotency
/// contract as fine-grained heap redo. The direct-write path against
/// `data_file` is retained as a fallback for callers that replay with no
/// buffer pool (`RedoContext.buffer_pool == None`).
///
/// After applying the image, the page's `pd_lsn` is patched to the record's
/// own LSN (taking the max against the current page LSN so re-applying an
/// older FPI never rolls pd_lsn backwards) — matching PG, where the page LSN
/// becomes the LSN of the most recent redo record that touched it.
pub struct FullPageImageRedoHandler {
    data_file: Arc<PositionedFile>,
}

impl FullPageImageRedoHandler {
    /// Create a handler that writes page images to `data_file`.
    pub fn new(data_file: Arc<PositionedFile>) -> Self {
        Self { data_file }
    }
}

impl RedoHandler for FullPageImageRedoHandler {
    fn kind(&self) -> WalRecordType {
        WalRecordType::FullPageImage
    }

    fn apply(&self, record: &WalRecord, ctx: &mut RedoContext<'_>) -> Result<()> {
        let decoded: FullPageImageRecord =
            bincode::serde::decode_from_slice(&record.payload, bincode_config())
                .map_err(|e| StorageError::Serialize(e.to_string()))?
                .0;

        if let Some(pool) = ctx.buffer_pool {
            let mut guard = pool.pin_mut(decoded.page_id)?;
            let page = guard.page_mut();
            // An FPI is restored unconditionally: the on-disk page may be torn
            // (garbage pd_lsn), so we cannot trust a pd_lsn >= record.lsn guard
            // here — repairing torn writes is the entire purpose of the image.
            // Idempotency across replays is preserved because replay always
            // starts at the checkpoint redo point and reapplies the FPI plus any
            // later fine-grained records in LSN order.
            page.copy_from_slice(&decoded.image);
            let new_lsn = record.lsn.max(page_pd_lsn(page));
            set_page_pd_lsn(page, new_lsn);
            // Drop marks the frame dirty; engine flushes after the replay loop.
            return Ok(());
        }

        // Fallback: no buffer pool at replay time — write straight to the data
        // file. The FPI image keeps its captured pd_lsn; patch it to this
        // record's LSN so the on-disk page reflects the replay point.
        let mut image = decoded.image;
        set_page_pd_lsn(&mut image, record.lsn);

        let offset = (decoded.page_id.0 - 1) * PAGE_SIZE as u64;
        self.data_file.write_all_at(&image, offset)?;
        // Durability: individual apply calls skip fsync for throughput;
        // engine::replay_wal issues a single data_file.sync_all() after the
        // replay loop completes.
        Ok(())
    }
}

/// Redo handler for record types that carry no redo payload (currently the
/// checkpoint markers): replaying them is a no-op, but they must be
/// registered so that recovery does not fail them as unknown.
pub struct NoOpRedoHandler {
    kind: WalRecordType,
}

impl NoOpRedoHandler {
    /// Create a no-op handler for `kind`.
    pub fn new(kind: WalRecordType) -> Self {
        Self { kind }
    }
}

impl RedoHandler for NoOpRedoHandler {
    fn kind(&self) -> WalRecordType {
        self.kind
    }

    fn apply(&self, _record: &WalRecord, _ctx: &mut RedoContext<'_>) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clog::NoOpClogAccessor;
    use crate::config::StorageConfig;
    use crate::wal::writer::WalWriter;
    use tempfile::TempDir;

    struct TestComponents {
        _tmp: TempDir,
        allocator: Arc<Mutex<PageAllocator>>,
        _wal: Arc<WalWriter>,
    }

    fn setup() -> TestComponents {
        let tmp = TempDir::new().unwrap();
        let cfg = StorageConfig::new(tmp.path());
        let wal = Arc::new(WalWriter::open(tmp.path(), &cfg).unwrap());
        let allocator = Arc::new(Mutex::new(
            PageAllocator::open(tmp.path(), &cfg, Arc::clone(&wal)).unwrap(),
        ));
        TestComponents {
            _tmp: tmp,
            allocator,
            _wal: wal,
        }
    }

    struct CountingHandler {
        kind: WalRecordType,
        count: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl RedoHandler for CountingHandler {
        fn kind(&self) -> WalRecordType {
            self.kind
        }

        fn apply(&self, _record: &WalRecord, _ctx: &mut RedoContext<'_>) -> Result<()> {
            self.count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(())
        }
    }

    fn noop_ctx<'a>(
        allocator: &'a Mutex<PageAllocator>,
        clog: &'a dyn ClogAccessor,
        att: &'a mut ActiveXactTable,
        dpt: &'a mut DirtyPageTable,
        incomplete_splits: &'a mut IncompleteSplitTracker,
    ) -> RedoContext<'a> {
        RedoContext {
            buffer_pool: None,
            page_allocator: allocator,
            clog,
            att,
            dpt,
            incomplete_splits,
        }
    }

    #[test]
    fn registry_dispatches_to_registered_handler() {
        let c = setup();
        let clog = NoOpClogAccessor;
        let mut att = ActiveXactTable::new();
        let mut dpt = DirtyPageTable::new();

        let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut registry = RedoRegistry::new();
        registry.register(Box::new(CountingHandler {
            kind: WalRecordType::HeapInsert,
            count: Arc::clone(&count),
        }));

        let record = WalRecord {
            lsn: Lsn(8),
            prev_lsn: Lsn::INVALID,
            txn_id: TxnId::INVALID,
            record_type: WalRecordType::HeapInsert,
            flags: 0,
            payload: Vec::new(),
        };
        let mut incomplete_splits = IncompleteSplitTracker::new();
        let mut ctx = noop_ctx(
            &c.allocator,
            &clog,
            &mut att,
            &mut dpt,
            &mut incomplete_splits,
        );
        registry.apply(&record, &mut ctx).unwrap();
        assert_eq!(count.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    #[test]
    #[should_panic(expected = "duplicate redo handler")]
    fn duplicate_registration_panics() {
        let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut registry = RedoRegistry::new();
        registry.register(Box::new(CountingHandler {
            kind: WalRecordType::PageAlloc,
            count: Arc::clone(&count),
        }));
        registry.register(Box::new(CountingHandler {
            kind: WalRecordType::PageAlloc,
            count,
        }));
    }

    #[test]
    fn unregistered_record_is_hard_failure() {
        let c = setup();
        let clog = NoOpClogAccessor;
        let mut att = ActiveXactTable::new();
        let mut dpt = DirtyPageTable::new();

        let registry = RedoRegistry::new();
        let record = WalRecord {
            lsn: Lsn(8),
            prev_lsn: Lsn::INVALID,
            txn_id: TxnId::INVALID,
            record_type: WalRecordType::HeapInsert,
            flags: 0,
            payload: Vec::new(),
        };
        let mut incomplete_splits = IncompleteSplitTracker::new();
        let mut ctx = noop_ctx(
            &c.allocator,
            &clog,
            &mut att,
            &mut dpt,
            &mut incomplete_splits,
        );
        let err = registry.apply(&record, &mut ctx).unwrap_err();
        assert!(matches!(
            err,
            StorageError::UnknownRecord {
                record_type: 1,
                lsn: Lsn(8)
            }
        ));
    }
}
