//! WAND execution placeholder for a later stage.

use crate::error::{InvertedError, Result};

/// Stage 0 has no WAND implementation; this entry point fails loudly.
pub fn search() -> Result<()> {
    Err(InvertedError::Unsupported(
        "WAND execution is not implemented in Stage 0",
    ))
}
