//! Wire-level error type for `pg-wire` (M3 Stage F).

use pg_engine::EngineError;

/// Result type used by the wire server.
pub type Result<T> = std::result::Result<T, WireError>;

/// Errors that can occur while serving a connection.
#[derive(Debug, thiserror::Error)]
pub enum WireError {
    /// Socket-level I/O failed (writes to a half-closed peer, ...).
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// The frame length itself is invalid. Framing is no longer trustworthy,
    /// so the session must close rather than attempt Sync recovery.
    #[error("framing violation: {0}")]
    Framing(String),

    /// A fully framed extended-protocol message body is malformed. The
    /// session reports 08P01 and can recover at the next Sync boundary.
    #[error("protocol violation: {0}")]
    Protocol(String),

    /// A fully framed Simple Query body is malformed. Its own command cycle
    /// ends immediately with ErrorResponse + ReadyForQuery, never with Sync.
    #[error("simple-query protocol violation: {0}")]
    SimpleQuery(String),

    /// A fully framed Sync body is malformed. Sync still performs normal
    /// error recovery and emits exactly one ReadyForQuery.
    #[error("sync protocol violation: {0}")]
    Sync(String),

    /// A value cannot be encoded, or a parameter type is unsupported.
    #[error("encoding error: {0}")]
    Encode(String),

    /// A parameter value is invalid in its declared text or binary format.
    #[error("{message}")]
    Value {
        /// PostgreSQL SQLSTATE describing the value failure.
        sqlstate: &'static str,
        /// Human-readable description of the invalid value.
        message: String,
    },
}

impl WireError {
    /// SQLSTATE for an engine error surfaced to the client. Only the
    /// distinctions a client can act on are mapped; everything else is the
    /// generic internal-error class `XX000`.
    pub(crate) fn sqlstate_of(err: &EngineError) -> &'static str {
        match err {
            EngineError::TableNotFound(_) => "42P01", // undefined_table
            EngineError::TableExists(_) => "42P07",   // duplicate_table
            EngineError::IndexNotFound(_) => "42P01", // undefined_table-ish
            EngineError::IndexExists(_) | EngineError::GraphExists(_) => "42P07", // duplicate_table-ish
            EngineError::Unsupported(_) => "0A000", // feature_not_supported
            EngineError::InvalidArgument(_) | EngineError::InvalidPredicate(_) => "42601", // syntax_error
            _ => "XX000", // internal_error
        }
    }
}
