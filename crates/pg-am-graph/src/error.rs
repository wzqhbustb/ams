//! Errors produced by graph storage, codecs, and traversal contracts.

/// Errors produced by graph access-method operations.
#[derive(Debug, thiserror::Error)]
pub enum GraphError {
    /// The requested operation is outside the current graph AM contract.
    #[error("unsupported operation: {0}")]
    Unsupported(&'static str),
    /// On-disk bytes, graph metadata, or GraphItem data are malformed.
    #[error("corrupted data: {0}")]
    Corrupted(String),
    /// Caller supplied invalid identifiers, properties, or configuration.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    /// A bounded traversal or metadata structure exceeded its budget.
    #[error("capacity limit exceeded: {0}")]
    Capacity(String),
    /// A lower-level storage operation failed.
    #[error("storage error: {0}")]
    Storage(String),
}

/// Convenient result alias for graph operations.
pub type Result<T> = std::result::Result<T, GraphError>;

impl GraphError {
    /// Convert a contract violation into the heap AM's public error without
    /// hiding it behind an empty result.
    pub fn into_heap(self) -> pg_am_heap::HeapError {
        match self {
            Self::Unsupported(message) => {
                pg_am_heap::HeapError::InvalidArgument(message.to_string())
            }
            Self::Corrupted(message) => pg_am_heap::HeapError::Corrupted(message),
            Self::InvalidArgument(message) | Self::Capacity(message) => {
                pg_am_heap::HeapError::InvalidArgument(message)
            }
            Self::Storage(message) => pg_am_heap::HeapError::Storage(
                pg_storage::error::StorageError::Io(std::io::Error::other(message)),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_failures_are_not_reported_as_caller_arguments() {
        let error = GraphError::Storage("disk offline".to_string()).into_heap();
        assert!(matches!(
            error,
            pg_am_heap::HeapError::Storage(pg_storage::error::StorageError::Io(_))
        ));
    }
}
