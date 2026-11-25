//! GraphStore lifecycle and explicit access-method contracts.

use std::path::{Path, PathBuf};

use pg_am_heap::access_method::{
    AccessMethod, BuildContext, DeleteContext, InsertContext, ScanContext,
};
use pg_am_heap::tuple::Datum;
use pg_storage::recovery::RedoHandler;
use pg_storage::types::{Lsn, Oid, Tid};

use crate::error::{GraphError, Result};
use crate::model::{GraphHit, GraphMetadata, GRAPH_ITEM_COLUMNS};
use crate::traversal::GraphTraversalRequest;

/// Registered graph access-method OID.
pub const GRAPH_AM_OID: Oid = Oid(9880);

/// A graph handle backed by the common GraphItem heap relation and its side
/// file. pg-engine orchestrates DML through its graph adapter; real adjacency
/// execution remains a later-stage responsibility.
#[derive(Debug, Clone)]
pub struct GraphStore {
    data_dir: PathBuf,
    metadata: GraphMetadata,
}

impl GraphStore {
    /// Construct a handle after validating metadata, without writing files.
    pub fn new(data_dir: impl Into<PathBuf>, metadata: GraphMetadata) -> Result<Self> {
        metadata.validate()?;
        Ok(Self {
            data_dir: data_dir.into(),
            metadata,
        })
    }

    /// Initialize a graph's side file and return its handle.
    pub fn create(data_dir: impl Into<PathBuf>, metadata: GraphMetadata) -> Result<Self> {
        let store = Self::new(data_dir, metadata)?;
        store.ensure_metadata_compatible()?;
        store.persist_metadata()?;
        Ok(store)
    }

    /// Open a graph by OID. A missing side file means no such graph; malformed
    /// metadata is a loud error and is never guessed from `pg_class`.
    pub fn open(data_dir: impl Into<PathBuf>, oid: Oid) -> Result<Option<Self>> {
        let data_dir = data_dir.into();
        let path = Self::metadata_path(&data_dir, oid);
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(GraphError::Storage(format!(
                    "cannot read {}: {error}",
                    path.display()
                )));
            }
        };
        let metadata = GraphMetadata::decode(&bytes)?;
        if metadata.oid != oid {
            return Err(GraphError::Corrupted(format!(
                "graph side file OID {} does not match requested {oid}",
                metadata.oid
            )));
        }
        Self::new(data_dir, metadata).map(Some)
    }

    /// Location of `graph/<oid>/metadata.bin`.
    pub fn metadata_path(data_dir: &Path, oid: Oid) -> PathBuf {
        data_dir
            .join("graph")
            .join(oid.0.to_string())
            .join("metadata.bin")
    }

    /// Persisted graph identity.
    pub fn metadata(&self) -> &GraphMetadata {
        &self.metadata
    }

    /// Require the side file to be absent or byte-for-byte equivalent in
    /// identity. Existing but different metadata is never overwritten.
    fn ensure_metadata_compatible(&self) -> Result<()> {
        match Self::open(self.data_dir.clone(), self.metadata.oid)? {
            Some(existing) if existing.metadata == self.metadata => Ok(()),
            Some(_) => Err(GraphError::InvalidArgument(
                "graph metadata already exists with a different identity".to_string(),
            )),
            None => Ok(()),
        }
    }

    /// Persist the current metadata with fsync and atomic rename.
    pub fn persist_metadata(&self) -> Result<()> {
        write_atomic(
            &Self::metadata_path(&self.data_dir, self.metadata.oid),
            &self.metadata.encode()?,
        )
    }

    /// Reserved BFS/DFS entry point. It is deliberately loud until Stage B;
    /// an unsupported traversal never becomes an empty successful result.
    pub fn search(&self, _request: &GraphTraversalRequest) -> Result<Vec<GraphHit>> {
        Err(GraphError::Unsupported(
            "graph traversal execution is implemented in Stage B",
        ))
    }
}

impl AccessMethod for GraphStore {
    fn name(&self) -> &'static str {
        "graph"
    }

    fn build(&self, ctx: BuildContext<'_>) -> pg_am_heap::Result<()> {
        if ctx.rel.rel_oid != self.metadata.oid
            || ctx.rel.first_page != self.metadata.first_page
            || ctx.rel.columns != GRAPH_ITEM_COLUMNS
        {
            return Err(GraphError::InvalidArgument(
                "graph build relation does not match GraphStore metadata/schema".to_string(),
            )
            .into_heap());
        }
        self.ensure_metadata_compatible()
            .map_err(GraphError::into_heap)?;
        self.persist_metadata().map_err(GraphError::into_heap)
    }

    fn insert(&self, _ctx: InsertContext<'_>) -> pg_am_heap::Result<()> {
        Err(GraphError::Unsupported("graph DML uses GraphStore APIs, not AM insert").into_heap())
    }

    fn scan(&self, _ctx: ScanContext<'_>) -> pg_am_heap::Result<Vec<(Tid, Vec<Option<Datum>>)>> {
        Err(GraphError::Unsupported("graph traversal does not expose a heap scan").into_heap())
    }

    fn delete(&self, _ctx: DeleteContext<'_>) -> pg_am_heap::Result<()> {
        Err(GraphError::Unsupported("graph DML uses GraphStore APIs, not AM delete").into_heap())
    }

    fn redo_handlers(&self) -> Vec<Box<dyn RedoHandler>> {
        Vec::new()
    }

    fn freshness(&self) -> Option<Lsn> {
        None
    }
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    pg_storage::io::write_atomic(path, bytes)
        .map_err(|error| GraphError::Storage(error.to_string()))
}
