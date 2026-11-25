//! Bounded version+CRC framing for graph side files.

use pg_storage::metadata::MetadataFrame;
use serde::{Deserialize, Serialize};

use crate::error::{GraphError, Result};

pub(crate) const MAGIC: &[u8; 4] = b"PGGR";
pub(crate) const FORMAT_VERSION: u16 = 1;
/// Maximum decoded side-file payload, including nested allocation budget.
pub const MAX_METADATA_BYTES: usize = 16 * 1024 * 1024;
const FRAME: MetadataFrame<MAX_METADATA_BYTES> = MetadataFrame::new(*MAGIC, FORMAT_VERSION);

/// Serialize a value into a bounded metadata frame with a CRC32 trailer.
pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    FRAME.encode(value).map_err(|error| {
        if error.contains("exceeds") {
            GraphError::Capacity(error)
        } else {
            GraphError::InvalidArgument(error)
        }
    })
}

/// Decode a frame after checking magic, outer version, exact length, CRC32,
/// nested decoder limits, and trailing payload bytes.
pub fn decode<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T> {
    FRAME.decode(bytes).map_err(GraphError::Corrupted)
}
