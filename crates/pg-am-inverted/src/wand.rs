//! WAND execution placeholder for a later stage.

use crate::error::{InvertedError, Result};

/// WAND is deferred to the optimization stage; this entry point fails loudly.
pub fn search() -> Result<()> {
    Err(InvertedError::Unsupported(
        "WAND execution is not implemented",
    ))
}
