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

    /// The peer sent bytes that violate the v3 framing rules (bad length
    /// word, missing NUL terminator, unknown startup code, ...). The
    /// session reports it and closes — framing is no longer trustworthy.
    #[error("protocol violation: {0}")]
    Protocol(String),

    /// A value cannot be text-encoded for the wire (currently only the
    /// unresolved-TOAST `Datum::External` case; M3 does not resolve
    /// out-of-line values on the read path).
    #[error("encoding error: {0}")]
    Encode(String),
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
            EngineError::IndexExists(_) => "42P07",   // duplicate_table-ish
            EngineError::Unsupported(_) => "0A000",   // feature_not_supported
            EngineError::InvalidArgument(_) | EngineError::InvalidPredicate(_) => "42601", // syntax_error
            _ => "XX000", // internal_error
        }
    }
}
