//! Text analysis contracts shared by indexing and query execution.

use serde::{Deserialize, Serialize};

use crate::error::{InvertedError, Result};

/// Configuration identity of the built-in Phase 3 mixed analyzer. The hash is
/// SHA256(`pg_rust.phase3.analyzer.mixed.v1`); analyzer or dictionary changes
/// must use a new identity and an explicit index rebuild.
pub const BUILTIN_ANALYZER_CONFIG_HASH: [u8; 32] = [
    0xce, 0x16, 0x9a, 0x2a, 0x3b, 0x62, 0x11, 0x69, 0x5a, 0x40, 0x49, 0x42, 0xe8, 0x39, 0x25, 0xd5,
    0xaf, 0x65, 0xb7, 0x13, 0xc5, 0x3f, 0xef, 0xa1, 0xc7, 0xeb, 0xe3, 0x42, 0xa6, 0x09, 0x92, 0x68,
];

/// Analyzer identity persisted with an inverted index. Opening an index with
/// a different implementation, dictionary, or stop-word configuration is an
/// error rather than an invitation to mix posting formats.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AnalyzerConfig {
    /// Stable analyzer name (`english`, `chinese`, or `mixed`).
    pub name: String,
    /// SHA256 of the complete analyzer/dictionary/stop-word configuration.
    pub config_hash: [u8; 32],
}

impl AnalyzerConfig {
    /// The Phase 3 built-in mixed analyzer configuration.
    pub fn builtin() -> Self {
        Self {
            name: "mixed".to_string(),
            config_hash: BUILTIN_ANALYZER_CONFIG_HASH,
        }
    }

    /// Reject an unassigned or otherwise incomplete persisted identity.
    pub fn validate(&self) -> Result<()> {
        if !matches!(self.name.as_str(), "english" | "chinese" | "mixed") {
            return Err(InvertedError::InvalidArgument(format!(
                "unknown analyzer name {:?}",
                self.name
            )));
        }
        if self.config_hash == [0; 32] {
            return Err(InvertedError::InvalidArgument(
                "analyzer configuration hash must not be zero".to_string(),
            ));
        }
        Ok(())
    }
}

impl Default for AnalyzerConfig {
    fn default() -> Self {
        Self::builtin()
    }
}

/// One analyzed token and its zero-based position in the original text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Token {
    /// Token text after the analyzer's normalization rules.
    pub text: String,
    /// Zero-based token position used by future phrase queries.
    pub position: u32,
}

/// Pluggable text analyzer. Indexing and query parsing must use the same
/// instance, dictionary, and configuration hash.
pub trait Analyzer: Send + Sync {
    /// Analyze `text` into ordered tokens. Empty tokens are filtered by the
    /// implementation, while positions remain contiguous.
    fn analyze(&self, text: &str) -> Vec<Token>;
}
