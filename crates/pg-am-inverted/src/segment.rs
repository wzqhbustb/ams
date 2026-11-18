//! Segment manifest and store contracts.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use pg_storage::segment::SegmentId;
use pg_storage::types::{Lsn, Oid};
use serde::{Deserialize, Serialize};

use crate::analyzer::AnalyzerConfig;
use crate::error::{InvertedError, Result};
use crate::posting::SegmentMeta;
use crate::tier2::Watermark;

/// The only legal read set for a generation of an inverted index.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SegmentManifest {
    /// Monotonically increasing manifest generation.
    pub generation: u64,
    /// Segment IDs readable by queries in this generation.
    pub active_segment_ids: Vec<SegmentId>,
    /// Durable WAL prefix fully covered by this generation.
    pub applied_lsn: Lsn,
}

impl SegmentManifest {
    /// Encode this manifest with magic/version, bounded length, and CRC32.
    pub fn encode(&self) -> Result<Vec<u8>> {
        crate::recovery::encode(self)
    }

    /// Decode a manifest and validate CRC32 and exact input length.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        crate::recovery::decode(bytes)
    }
}

/// Segment storage contract. Stage 0 freezes the format and recovery path;
/// query execution remains unimplemented.
#[derive(Debug)]
pub struct SegmentStore {
    manifest: SegmentManifest,
    analyzer: AnalyzerConfig,
    watermark: Watermark,
}

impl SegmentStore {
    /// Create an empty in-memory store and its initial manifest.
    pub fn new(_index_oid: Oid) -> Self {
        Self {
            manifest: SegmentManifest {
                generation: 1,
                active_segment_ids: Vec::new(),
                applied_lsn: Lsn::INVALID,
            },
            analyzer: AnalyzerConfig::builtin(),
            watermark: Watermark {
                consumed_lsn: Lsn::INVALID,
                durable_applied_lsn: Lsn::INVALID,
            },
        }
    }

    /// Location of the generation manifest for one inverted index.
    pub fn manifest_path(data_dir: &Path, index_oid: Oid) -> PathBuf {
        data_dir
            .join("inverted")
            .join(index_oid.0.to_string())
            .join("manifest.bin")
    }

    /// Location of one segment's metadata within its owning index.
    pub fn segment_metadata_path(
        data_dir: &Path,
        index_oid: Oid,
        segment_id: SegmentId,
    ) -> PathBuf {
        data_dir
            .join("inverted")
            .join(index_oid.0.to_string())
            .join("segments")
            .join(format!("{}.meta", segment_id.0))
    }

    /// Recover the manifest and every active segment's metadata. An empty
    /// manifest is valid only at `Lsn::INVALID`. Otherwise the manifest
    /// watermark must equal the maximum active segment coverage. The analyzer
    /// must match the caller's configured instance exactly; watermark replay
    /// starts at the durable manifest position, not a fabricated zero value.
    pub fn open(
        data_dir: &Path,
        index_oid: Oid,
        expected_analyzer: &AnalyzerConfig,
    ) -> Result<Self> {
        expected_analyzer.validate()?;
        let manifest_path = Self::manifest_path(data_dir, index_oid);
        let manifest = SegmentManifest::decode(&read_bytes(&manifest_path)?)?;
        if manifest.generation == 0 {
            return Err(InvertedError::Corrupted(
                "manifest generation must be greater than zero".to_string(),
            ));
        }
        if manifest.active_segment_ids.is_empty() {
            if manifest.applied_lsn != Lsn::INVALID {
                return Err(InvertedError::Corrupted(
                    "empty manifest carries a nonzero watermark".to_string(),
                ));
            }
            return Ok(Self {
                manifest,
                analyzer: expected_analyzer.clone(),
                watermark: Watermark {
                    consumed_lsn: Lsn::INVALID,
                    durable_applied_lsn: Lsn::INVALID,
                },
            });
        }
        let unique_ids = manifest.active_segment_ids.iter().collect::<BTreeSet<_>>();
        if unique_ids.len() != manifest.active_segment_ids.len() {
            return Err(InvertedError::Corrupted(
                "manifest contains duplicate active segment IDs".to_string(),
            ));
        }

        let mut analyzer: Option<AnalyzerConfig> = None;
        let mut max_applied_lsn = Lsn::INVALID;
        for segment_id in &manifest.active_segment_ids {
            let path = Self::segment_metadata_path(data_dir, index_oid, *segment_id);
            let meta = SegmentMeta::decode(&read_bytes(&path)?)?;
            if meta.index_oid != index_oid {
                return Err(InvertedError::Corrupted(format!(
                    "segment {} belongs to index {}, expected {index_oid}",
                    segment_id.0, meta.index_oid.0
                )));
            }
            if meta.segment_id != *segment_id {
                return Err(InvertedError::Corrupted(format!(
                    "segment metadata ID {} does not match manifest ID {}",
                    meta.segment_id.0, segment_id.0
                )));
            }
            if meta.created_from_lsn.0 > meta.applied_lsn.0 {
                return Err(InvertedError::Corrupted(format!(
                    "segment {} has created_from_lsn after applied_lsn",
                    segment_id.0
                )));
            }
            if meta.applied_lsn.0 > manifest.applied_lsn.0 {
                return Err(InvertedError::Corrupted(format!(
                    "segment {} is ahead of the manifest watermark",
                    segment_id.0
                )));
            }
            if let Some(expected) = &analyzer {
                if *expected != meta.analyzer {
                    return Err(InvertedError::Corrupted(format!(
                        "segment {} uses a different analyzer configuration",
                        segment_id.0
                    )));
                }
            } else {
                analyzer = Some(meta.analyzer.clone());
            }
            max_applied_lsn = max_applied_lsn.max(meta.applied_lsn);
        }
        if max_applied_lsn != manifest.applied_lsn {
            return Err(InvertedError::Corrupted(format!(
                "manifest watermark {} exceeds active segment coverage {max_applied_lsn}",
                manifest.applied_lsn.0
            )));
        }
        let analyzer = analyzer.ok_or_else(|| {
            InvertedError::Corrupted("manifest has no analyzer metadata".to_string())
        })?;
        if analyzer != *expected_analyzer {
            return Err(InvertedError::Corrupted(format!(
                "analyzer configuration mismatch: expected {} / {:02x?}, got {} / {:02x?}",
                expected_analyzer.name,
                expected_analyzer.config_hash,
                analyzer.name,
                analyzer.config_hash
            )));
        }
        let watermark = Watermark {
            consumed_lsn: manifest.applied_lsn,
            durable_applied_lsn: manifest.applied_lsn,
        };
        Ok(Self {
            manifest,
            analyzer,
            watermark,
        })
    }

    /// Return the current read-set manifest.
    pub fn manifest(&self) -> &SegmentManifest {
        &self.manifest
    }

    /// Analyzer identity recovered from the active segments.
    pub fn analyzer_config(&self) -> &AnalyzerConfig {
        &self.analyzer
    }

    /// Recovered consumed/durable watermark. A clean reopen starts the WAL
    /// consumer from the same durable position.
    pub fn watermark(&self) -> Watermark {
        self.watermark
    }

    /// Install a consumed/durable watermark pair for freshness unit tests.
    #[cfg(test)]
    pub(crate) fn set_watermark_for_test(&mut self, watermark: Watermark) {
        self.watermark = watermark;
    }

    /// Build an empty segment store without inventing a fake index build.
    pub fn build(&self) -> Result<()> {
        Ok(())
    }

    /// Stage 0 does not execute a query yet.
    pub fn search(&self) -> Result<()> {
        Err(InvertedError::Unsupported(
            "inverted search is not implemented in Stage 0",
        ))
    }

    /// Sealing is defined by the next stage, not simulated by Stage 0.
    pub fn seal(&self) -> Result<()> {
        Err(InvertedError::Unsupported(
            "segment sealing is not implemented in Stage 0",
        ))
    }
}

fn read_bytes(path: &Path) -> Result<Vec<u8>> {
    std::fs::read(path)
        .map_err(|error| InvertedError::Storage(format!("cannot read {}: {error}", path.display())))
}
