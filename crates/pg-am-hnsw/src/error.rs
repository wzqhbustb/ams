//! HNSW access method error types.

use thiserror::Error;

/// Errors returned by the HNSW access method.
#[derive(Debug, Error)]
pub enum HnswError {
    /// A construction parameter violates the invariants of tech-selection
    /// §4.2/§4.4 (`M >= 2`, `ef_construction >= M`, `ef_search_default >= M`).
    /// Also raised when a snapshot header fails the re-run of these checks
    /// at load time (§3).
    #[error("invalid parameter: {0}")]
    InvalidParams(String),

    /// A caller supplied invalid arguments (dimension mismatch, `dim = 0`,
    /// a NaN vector component — §5 entry validation).
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// Cosine distance is undefined for a zero vector (§5: report loudly
    /// instead of silently returning 1.0).
    #[error("cosine distance undefined for a zero vector")]
    ZeroVector,

    /// Snapshot bytes are malformed or fail a load-validation checklist item
    /// (§3, §7). Corrupted bytes must never cause a panic (Stage G hardening
    /// style, inherited from Phase 1).
    #[error("corrupted data: {0}")]
    Corrupted(String),

    /// The CRC32 prefix does not match the body (§7: detect bit-rot rather
    /// than silently producing a "valid but wrong" graph — same convention
    /// as pg-storage's checkpoint.rs and FreelistMeta).
    #[error("checksum mismatch: stored 0x{stored:08x}, computed 0x{computed:08x}")]
    ChecksumMismatch {
        /// CRC32 stored in the 4-byte prefix.
        stored: u32,
        /// CRC32 recomputed over the body.
        computed: u32,
    },

    /// The operation is not allowed in the current graph state. Stage C uses
    /// this to reject `insert` into a snapshot-loaded graph (coding plan
    /// Stage C conservative default — continuation-insert semantics are an
    /// open question punted to tech-selection v1.6).
    #[error("invalid operation: {0}")]
    InvalidOperation(String),
}

/// A convenient type alias for HNSW AM results.
pub type Result<T> = std::result::Result<T, HnswError>;
