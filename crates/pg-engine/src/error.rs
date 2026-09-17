//! Engine error type (coding-plan Stage K).

use pg_catalog::CatalogError;
use pg_storage::error::StorageError;

/// Result type used by the engine API.
pub type Result<T> = std::result::Result<T, EngineError>;

/// Errors that can occur in engine operations.
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    /// A storage engine operation failed (WAL, buffer pool, checkpoint,
    /// commit/abort fsync).
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),

    /// A heap access-method operation failed.
    #[error("heap error: {0}")]
    Heap(#[from] pg_am_heap::HeapError),

    /// A catalog operation failed.
    #[error("catalog error: {0}")]
    Catalog(#[from] CatalogError),

    /// `create_table` named a table that already exists.
    #[error("table {0:?} already exists")]
    TableExists(String),

    /// The named table does not exist.
    #[error("table {0:?} does not exist")]
    TableNotFound(String),

    /// A system catalog's first page has no room for another row.
    ///
    /// M2a limitation: `Catalog::open` reads back only each system table's
    /// *first* page, so DDL must never let a system table overflow onto a
    /// second chain page — the overflow rows would be invisible after a
    /// reopen. DDL pre-checks free space and fails with this error instead
    /// of silently corrupting the catalog (see `engine::ensure_catalog_room`).
    #[error("system catalog page full: {0}")]
    CatalogFull(String),

    /// A caller argument is invalid (wrong value count, empty schema, ...).
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// A scan predicate references a column the table does not have.
    #[error("invalid predicate: {0}")]
    InvalidPredicate(String),

    /// On-disk catalog content is inconsistent with what the engine wrote
    /// (e.g. a `pg_class` row with no matching live version).
    #[error("corrupted catalog: {0}")]
    Corrupted(String),

    /// A B+Tree access-method operation failed (bulk load, index lookup).
    #[error("btree error: {0}")]
    BTree(#[from] pg_am_btree::BTreeError),

    /// An HNSW access-method failure surfaced through the M5 dependency
    /// edge (Phase 2 M5 Stage B slice 3b — the pg-am-hnsw create/open
    /// protocol surfaces here; same `#[from]` wiring style as `BTree`).
    #[error("hnsw error: {0}")]
    Hnsw(#[from] pg_am_hnsw::HnswError),

    /// The table lock manager rejected an acquisition: the deadlock
    /// detector (M2c Stage R, §9.3) chose this transaction as the victim of
    /// a wait-for cycle. The current statement fails with this error; the
    /// caller must abort the transaction (auto-commit does so for any
    /// statement failure).
    #[error("lock error: {0}")]
    Lock(#[from] pg_txn::LockError),

    /// The statement parses but its semantics are not implemented at this
    /// stage (e.g. `SELECT ... FOR SHARE`, which needs multixact row locks).
    #[error("unsupported: {0}")]
    Unsupported(String),

    /// `create_index` named a (table, column) pair that already has an index.
    #[error("index on {0} already exists")]
    IndexExists(String),

    /// `index_lookup` named a (table, column) pair with no index.
    #[error("no index on {0}")]
    IndexNotFound(String),
}
