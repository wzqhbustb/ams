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
}

impl InvertedIndex {
    /// Create an empty inverted index with an initial manifest.
    pub fn new(index_oid: Oid) -> Self {
        Self {
            store: SegmentStore::new(index_oid),
        }
    }

    /// Wrap a recovered segment store in its access-method interface.
    pub fn from_store(store: SegmentStore) -> Self {
        Self { store }
    }

    /// Execute exact DAAT and BM25 over the recovered segment.
    pub fn search(&self, query: &Query, limit: Option<usize>) -> Result<Vec<ScoredTid>> {
        self.store.search(query, limit)
    }

    /// Analyze query text with the index analyzer and execute exact DAAT.
    pub fn search_text(&self, text: &str, limit: Option<usize>) -> Result<Vec<ScoredTid>> {
        self.store.search_text(text, limit)
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

    /// Persist the current write view and return the last seal marker's end LSN.
    pub fn seal(
        &mut self,
        data_dir: &std::path::Path,
        wal: &pg_storage::wal::WalWriter,
        xid: pg_storage::types::TxnId,
    ) -> Result<Option<Lsn>> {
        self.store.seal(data_dir, wal, xid)
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
