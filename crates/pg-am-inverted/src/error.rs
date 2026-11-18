//! Error types for the inverted access method.

/// Errors produced by inverted indexing, decoding, and lifecycle operations.
#[derive(Debug, thiserror::Error)]
pub enum InvertedError {
    /// The requested operation is outside the Stage 0 contract.
    #[error("unsupported operation: {0}")]
    Unsupported(&'static str),
    /// On-disk bytes, manifest state, or TOAST data are malformed.
    #[error("corrupted data: {0}")]
    Corrupted(String),
    /// Caller supplied invalid arguments or configuration.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    /// A lower-level storage operation failed.
    #[error("storage error: {0}")]
    Storage(String),
}

/// Convenient result alias for inverted-index operations.
pub type Result<T> = std::result::Result<T, InvertedError>;

impl InvertedError {
    /// Convert an unsupported contract violation into the heap AM's public
    /// invalid-argument error without hiding it behind an empty result.
    pub fn into_heap(self) -> pg_am_heap::HeapError {
        match self {
            Self::Unsupported(message) => {
                pg_am_heap::HeapError::InvalidArgument(message.to_string())
            }
            Self::Corrupted(message) => pg_am_heap::HeapError::Corrupted(message),
            Self::InvalidArgument(message) => pg_am_heap::HeapError::InvalidArgument(message),
            Self::Storage(message) => pg_am_heap::HeapError::Storage(
                pg_storage::error::StorageError::MetadataCorrupted(message),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_failures_are_not_reported_as_caller_arguments() {
        let error = InvertedError::Storage("disk offline".to_string()).into_heap();
        assert!(matches!(
            error,
            pg_am_heap::HeapError::Storage(pg_storage::error::StorageError::MetadataCorrupted(_))
        ));
    }
}
