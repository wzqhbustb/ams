//! Posting-list and per-segment statistics formats.

use std::collections::BTreeMap;

use pg_storage::segment::{SegmentId, SegmentState};
use pg_storage::types::{Lsn, Oid, Tid};
use serde::{Deserialize, Serialize};

use crate::analyzer::AnalyzerConfig;

/// One document's entry for a single term.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Posting {
    /// Heap tuple identifier of the document.
    pub tid: Tid,
    /// Number of occurrences of the term in this document.
    pub term_freq: u32,
    /// Ordered token positions for future phrase queries.
    pub positions: Vec<u32>,
}

impl Posting {
    /// Encode this posting in the versioned metadata frame.
    pub fn encode(&self) -> crate::Result<Vec<u8>> {
        crate::recovery::encode(self)
    }

    /// Validate internal frequency and strictly increasing positions. Stage B
    /// must call this before persisting a posting.
    pub fn validate(&self) -> crate::Result<()> {
        if self.term_freq as usize != self.positions.len() {
            return Err(crate::InvertedError::Corrupted(format!(
                "term_freq {} does not match position count {}",
                self.term_freq,
                self.positions.len()
            )));
        }
        if self.positions.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(crate::InvertedError::Corrupted(
                "posting positions are not strictly increasing".to_string(),
            ));
        }
        Ok(())
    }

    /// Decode a posting and validate its frame, internal frequency, and
    /// strictly increasing position sequence.
    pub fn decode(bytes: &[u8]) -> crate::Result<Self> {
        let posting: Self = crate::recovery::decode(bytes)?;
        posting.validate()?;
        Ok(posting)
    }
}

/// BM25 length of one document in a segment.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DocLength {
    /// Heap tuple identifier of the document.
    pub tid: Tid,
    /// Number of analyzed tokens in the document.
    pub token_count: u64,
}

impl DocLength {
    /// Encode this document length in the versioned metadata frame.
    pub fn encode(&self) -> crate::Result<Vec<u8>> {
        crate::recovery::encode(self)
    }

    /// Decode a document length and validate magic, version, length, and CRC32.
    pub fn decode(bytes: &[u8]) -> crate::Result<Self> {
        crate::recovery::decode(bytes)
    }
}

/// Current per-segment metadata layout version.
pub const SEGMENT_FORMAT_VERSION: u16 = 1;

/// Per-segment statistics and identity metadata.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SegmentMeta {
    /// Binary container format version.
    pub format_version: u16,
    /// Owning inverted index relation OID.
    pub index_oid: Oid,
    /// Stable segment identifier.
    pub segment_id: SegmentId,
    /// Analyzer identity used by every posting in this segment.
    pub analyzer: AnalyzerConfig,
    /// Lifecycle state of this segment.
    pub state: SegmentState,
    /// Number of committed documents in the segment.
    pub doc_count: u64,
    /// Total token count across all documents.
    pub total_tokens: u64,
    /// Number of documents containing each term.
    pub term_df: BTreeMap<String, u64>,
    /// WAL position of the first included change.
    pub created_from_lsn: Lsn,
    /// WAL prefix fully covered by this segment.
    pub applied_lsn: Lsn,
    /// Number of source heap rows represented by this segment.
    pub row_count: u64,
    /// Number of posting entries in the segment.
    pub posting_count: u64,
    /// Number of bytes in the encoded segment container.
    pub byte_count: u64,
}

/// Wire form that preserves duplicate `term_df` keys for validation. Its
/// bincode layout is identical to `SegmentMeta`; only the decoded container
/// differs (`Vec` of pairs instead of a silently deduplicating `BTreeMap`).
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct SegmentMetaWire {
    format_version: u16,
    index_oid: Oid,
    segment_id: SegmentId,
    analyzer: AnalyzerConfig,
    state: SegmentState,
    doc_count: u64,
    total_tokens: u64,
    pub(crate) term_df: Vec<(String, u64)>,
    created_from_lsn: Lsn,
    applied_lsn: Lsn,
    row_count: u64,
    posting_count: u64,
    byte_count: u64,
}

impl From<SegmentMeta> for SegmentMetaWire {
    fn from(meta: SegmentMeta) -> Self {
        Self {
            format_version: meta.format_version,
            index_oid: meta.index_oid,
            segment_id: meta.segment_id,
            analyzer: meta.analyzer,
            state: meta.state,
            doc_count: meta.doc_count,
            total_tokens: meta.total_tokens,
            term_df: meta.term_df.into_iter().collect(),
            created_from_lsn: meta.created_from_lsn,
            applied_lsn: meta.applied_lsn,
            row_count: meta.row_count,
            posting_count: meta.posting_count,
            byte_count: meta.byte_count,
        }
    }
}

impl SegmentMetaWire {
    fn into_meta(self) -> crate::Result<SegmentMeta> {
        let mut term_df = BTreeMap::new();
        for (term, df) in self.term_df {
            if term_df.insert(term.clone(), df).is_some() {
                return Err(crate::InvertedError::Corrupted(format!(
                    "duplicate term_df key {term:?}"
                )));
            }
        }
        Ok(SegmentMeta {
            format_version: self.format_version,
            index_oid: self.index_oid,
            segment_id: self.segment_id,
            analyzer: self.analyzer,
            state: self.state,
            doc_count: self.doc_count,
            total_tokens: self.total_tokens,
            term_df,
            created_from_lsn: self.created_from_lsn,
            applied_lsn: self.applied_lsn,
            row_count: self.row_count,
            posting_count: self.posting_count,
            byte_count: self.byte_count,
        })
    }
}

impl SegmentMeta {
    /// Encode this segment's metadata in the versioned metadata frame.
    pub fn encode(&self) -> crate::Result<Vec<u8>> {
        crate::recovery::encode(self)
    }

    /// Decode segment metadata and validate its container and semantic
    /// version, analyzer identity, exact length, and CRC32.
    pub fn decode(bytes: &[u8]) -> crate::Result<Self> {
        let meta = crate::recovery::decode::<SegmentMetaWire>(bytes)?.into_meta()?;
        if meta.format_version != SEGMENT_FORMAT_VERSION {
            return Err(crate::InvertedError::Corrupted(format!(
                "unsupported segment metadata version {}",
                meta.format_version
            )));
        }
        meta.analyzer.validate().map_err(|error| {
            crate::InvertedError::Corrupted(format!("invalid analyzer metadata: {error}"))
        })?;
        Ok(meta)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duplicate_term_df_keys_are_rejected() {
        let meta = SegmentMeta {
            format_version: SEGMENT_FORMAT_VERSION,
            index_oid: Oid(9879),
            segment_id: SegmentId(1),
            analyzer: AnalyzerConfig::builtin(),
            state: SegmentState::Sealed,
            doc_count: 1,
            total_tokens: 1,
            term_df: BTreeMap::from([("contract".to_string(), 1)]),
            created_from_lsn: Lsn::INVALID,
            applied_lsn: Lsn::FIRST,
            row_count: 1,
            posting_count: 1,
            byte_count: 0,
        };
        let mut wire = SegmentMetaWire::from(meta);
        wire.term_df.push(("contract".to_string(), 1));
        let bytes = crate::recovery::encode(&wire).unwrap();
        assert!(matches!(
            SegmentMeta::decode(&bytes),
            Err(crate::InvertedError::Corrupted(_))
        ));
    }
}
