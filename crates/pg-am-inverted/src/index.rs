//! Inverted access-method lifecycle and freshness hook.

use pg_am_heap::access_method::{
    AccessMethod, BuildContext, DeleteContext, InsertContext, ScanContext,
};
use pg_am_heap::tuple::Datum;
use pg_storage::recovery::RedoHandler;
use pg_storage::types::{Lsn, Oid, Tid};

use crate::error::{InvertedError, Result};
use crate::segment::SegmentStore;

/// Inverted access method. Stage 0 deliberately keeps DML out of this AM:
/// writes arrive through heap WAL and Tier 2 instead of direct API calls.
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

    /// Stage 0 has no query implementation; this fails loudly.
    pub fn search(&self) -> Result<()> {
        self.store.search()
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
