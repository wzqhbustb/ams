//! Inverted access-method lifecycle and freshness hook.

use pg_am_heap::access_method::{
    AccessMethod, BuildContext, DeleteContext, InsertContext, ScanContext,
};
use pg_am_heap::tuple::Datum;
use pg_storage::recovery::RedoHandler;
use pg_storage::types::{Lsn, Oid, Tid};

use crate::daat::{Query, ScoredTid};
use crate::error::{InvertedError, Result};
use crate::segment::SegmentStore;

/// Inverted access method. Direct DML stays out of this AM: writes arrive
/// through heap WAL and Tier 2 instead of direct API calls.
#[derive(Debug)]
pub struct InvertedIndex {
    store: SegmentStore,
    merge_capacity_blocked: bool,
    seal_capacity_blocked: bool,
    fail_merge_with_capacity: bool,
    fail_seal_with_capacity: bool,
    merge_attempts: usize,
    seal_attempts: usize,
}

impl InvertedIndex {
    /// Create an empty inverted index with an initial manifest.
    pub fn new(index_oid: Oid) -> Self {
        Self {
            store: SegmentStore::new(index_oid),
            merge_capacity_blocked: false,
            seal_capacity_blocked: false,
            fail_merge_with_capacity: false,
            fail_seal_with_capacity: false,
            merge_attempts: 0,
            seal_attempts: 0,
        }
    }

    /// Wrap a recovered segment store in its access-method interface.
    pub fn from_store(store: SegmentStore) -> Self {
        Self {
            store,
            merge_capacity_blocked: false,
            seal_capacity_blocked: false,
            fail_merge_with_capacity: false,
            fail_seal_with_capacity: false,
            merge_attempts: 0,
            seal_attempts: 0,
        }
    }

    /// Execute exact DAAT and BM25 over the recovered segment.
    pub fn search(&self, query: &Query, limit: Option<usize>) -> Result<Vec<ScoredTid>> {
        self.store.search(query, limit)
    }

    /// Analyze query text with the index analyzer and execute exact DAAT.
    pub fn search_text(&self, text: &str, limit: Option<usize>) -> Result<Vec<ScoredTid>> {
        self.store.search_text(text, limit)
    }

    /// Evaluate a parsed query with exact WAND for bounded top-k.
    pub fn search_wand(&self, query: &Query, limit: Option<usize>) -> Result<Vec<ScoredTid>> {
        self.store.search_wand(query, limit)
    }

    /// Analyze query text and evaluate exact WAND for bounded top-k.
    pub fn search_wand_text(&self, text: &str, limit: Option<usize>) -> Result<Vec<ScoredTid>> {
        self.store.search_wand_text(text, limit)
    }

    /// Insert one committed heap document into the in-process write view.
    pub fn insert_document(&mut self, tid: Tid, text: &str) -> Result<()> {
        self.store.insert_document(tid, text)
    }

    /// Delete one committed heap document from the in-process write view.
    pub fn delete_document(&mut self, tid: Tid) -> Result<bool> {
        self.store.delete_document(tid)
    }

    /// Advance the query-visible consumed WAL frontier monotonically.
    pub fn advance_consumed(&mut self, lsn: Lsn) {
        self.store.advance_consumed(lsn);
    }

    /// Return the analyzer identity shared by postings and query parsing.
    pub fn analyzer_config(&self) -> &crate::AnalyzerConfig {
        self.store.analyzer_config()
    }

    /// Return the current consumed and durable watermarks.
    pub fn watermark(&self) -> crate::Watermark {
        self.store.watermark()
    }

    /// Whether the in-memory write view has changes that are not sealed yet.
    pub fn has_unsealed_changes(&self) -> bool {
        self.store.has_unsealed_changes()
    }

    /// Whether a previous seal hit the frame capacity and should not be
    /// retried by the automatic query-path worker in this process.
    pub fn seal_capacity_blocked(&self) -> bool {
        self.seal_capacity_blocked
    }

    /// Stop automatic seal attempts after a capacity failure.
    pub fn mark_seal_capacity_blocked(&mut self) {
        self.seal_capacity_blocked = true;
    }

    /// Inject a deterministic seal capacity failure for engine tests.
    pub fn inject_seal_capacity_error(&mut self) {
        self.fail_seal_with_capacity = true;
    }

    /// Number of seal calls observed by this handle.
    pub fn seal_attempts_for_test(&self) -> usize {
        self.seal_attempts
    }

    /// Persist the current write view and return the last seal marker's end LSN.
    pub fn seal(
        &mut self,
        data_dir: &std::path::Path,
        wal: &pg_storage::wal::WalWriter,
        xid: pg_storage::types::TxnId,
    ) -> Result<Option<Lsn>> {
        self.seal_attempts += 1;
        if self.fail_seal_with_capacity {
            return Err(InvertedError::Capacity(
                "injected seal capacity failure".to_string(),
            ));
        }
        self.store.seal(data_dir, wal, xid)
    }

    /// Number of active segments in the current manifest.
    pub fn active_segment_count(&self) -> usize {
        self.store.manifest().active_segment_ids.len()
    }

    /// Whether a previous merge hit the frame capacity and should not be
    /// retried by the automatic query-path worker in this process.
    pub fn merge_capacity_blocked(&self) -> bool {
        self.merge_capacity_blocked
    }

    /// Stop automatic merge attempts after a capacity failure.
    pub fn mark_merge_capacity_blocked(&mut self) {
        self.merge_capacity_blocked = true;
    }

    /// Inject a deterministic capacity failure for engine circuit-breaker tests.
    pub fn inject_merge_capacity_error(&mut self) {
        self.fail_merge_with_capacity = true;
    }

    /// Number of merge calls observed by this handle.
    pub fn merge_attempts_for_test(&self) -> usize {
        self.merge_attempts
    }

    /// Merge all active segments into a new generation when compaction is due.
    pub fn merge(
        &mut self,
        data_dir: &std::path::Path,
        wal: &pg_storage::wal::WalWriter,
        xid: pg_storage::types::TxnId,
    ) -> Result<Option<Lsn>> {
        {
            self.merge_attempts += 1;
            if self.fail_merge_with_capacity {
                return Err(InvertedError::Capacity(
                    "injected merge capacity failure".to_string(),
                ));
            }
        }
        self.store.merge(data_dir, wal, xid)
    }
}

impl AccessMethod for InvertedIndex {
    fn name(&self) -> &'static str {
        "inverted"
    }

    fn build(&self, _ctx: BuildContext<'_>) -> pg_am_heap::Result<()> {
        self.store.build().map_err(InvertedError::into_heap)
    }

    fn insert(&self, _ctx: InsertContext<'_>) -> pg_am_heap::Result<()> {
        Err(InvertedError::Unsupported("inverted insert is Tier 2 WAL-driven").into_heap())
    }

    fn scan(&self, _ctx: ScanContext<'_>) -> pg_am_heap::Result<Vec<(Tid, Vec<Option<Datum>>)>> {
        Err(InvertedError::Unsupported("inverted scan is not a heap scan").into_heap())
    }

    fn delete(&self, _ctx: DeleteContext<'_>) -> pg_am_heap::Result<()> {
        Err(InvertedError::Unsupported("inverted delete is Tier 2 WAL-driven").into_heap())
    }

    fn redo_handlers(&self) -> Vec<Box<dyn RedoHandler>> {
        Vec::new()
    }

    fn freshness(&self) -> Option<Lsn> {
        Some(self.store.watermark().consumed_lsn)
    }
}
