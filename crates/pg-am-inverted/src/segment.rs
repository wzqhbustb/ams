//! Segment manifest and store contracts.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use pg_storage::segment::{SegmentId, SegmentState};
use pg_storage::types::{Lsn, Oid, Tid};
use serde::{Deserialize, Serialize};

use crate::analyzer::{AnalyzerConfig, DefaultAnalyzer};
use crate::daat::{InMemoryIndex, InMemoryStats, Query, ScoredTid};
use crate::error::{InvertedError, Result};
use crate::posting::{DocLength, Posting, SegmentMeta};
use crate::tier2::Watermark;

/// The only legal read set for a generation of an inverted index.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SegmentManifest {
    /// Monotonically increasing manifest generation.
    pub generation: u64,
    /// Segment IDs readable by queries in this generation, oldest to newest.
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

/// Segment storage with durable metadata, postings, and an in-memory DAAT view.
pub struct SegmentStore {
    manifest: SegmentManifest,
    index_oid: Oid,
    analyzer: AnalyzerConfig,
    watermark: Watermark,
    index: InMemoryIndex<Arc<DefaultAnalyzer>>,
    /// Whether no segment publication or initial marker work is outstanding.
    /// Marker emission is advisory after a generation has been published.
    wal_sealed: bool,
    /// Whether the in-memory document set differs from the durable generation.
    dirty: bool,
    /// Upserts and tombstones not yet included in a durable segment.
    pending_documents: BTreeMap<Tid, Option<String>>,
    /// Number of pending documents required before a new delta is sealed.
    flush_threshold: usize,
}

impl std::fmt::Debug for SegmentStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SegmentStore")
            .field("manifest", &self.manifest)
            .field("index_oid", &self.index_oid)
            .field("analyzer", &self.analyzer)
            .field("watermark", &self.watermark)
            .finish()
    }
}

impl SegmentStore {
    /// Create an empty in-memory store and its initial manifest.
    pub fn new(index_oid: Oid) -> Self {
        Self::empty_with_analyzer(index_oid, AnalyzerConfig::builtin())
    }

    fn empty_with_analyzer(index_oid: Oid, analyzer: AnalyzerConfig) -> Self {
        Self {
            manifest: SegmentManifest {
                generation: 1,
                active_segment_ids: Vec::new(),
                applied_lsn: Lsn::INVALID,
            },
            index_oid,
            analyzer: analyzer.clone(),
            watermark: Watermark {
                consumed_lsn: Lsn::INVALID,
                durable_applied_lsn: Lsn::INVALID,
            },
            index: InMemoryIndex::new(DefaultAnalyzer::from_config(analyzer)),
            wal_sealed: true,
            dirty: false,
            pending_documents: BTreeMap::new(),
            flush_threshold: 1,
        }
    }

    /// Build and durably publish an initial segment. Empty input publishes an
    /// empty manifest without inventing an empty on-disk segment.
    pub fn create(
        data_dir: &Path,
        index_oid: Oid,
        analyzer: AnalyzerConfig,
        documents: Vec<(Tid, String)>,
        base_lsn: Lsn,
    ) -> Result<Self> {
        analyzer.validate()?;
        let mut index = InMemoryIndex::new(DefaultAnalyzer::from_config(analyzer.clone()));
        for (tid, text) in documents {
            index.insert_document(tid, &text)?;
        }
        let stats = index.stats();
        let active_segment_ids = if stats.doc_count == 0 {
            Vec::new()
        } else {
            vec![SegmentId(1)]
        };
        let store = Self {
            manifest: SegmentManifest {
                generation: 1,
                active_segment_ids,
                applied_lsn: base_lsn,
            },
            index_oid,
            analyzer: analyzer.clone(),
            watermark: Watermark {
                consumed_lsn: base_lsn,
                durable_applied_lsn: base_lsn,
            },
            index,
            wal_sealed: false,
            dirty: false,
            pending_documents: BTreeMap::new(),
            flush_threshold: 1,
        };
        store.persist(data_dir, base_lsn)?;
        Ok(store)
    }

    fn persist(&self, data_dir: &Path, base_lsn: Lsn) -> Result<()> {
        let stats = self.index.stats();
        if stats.doc_count > 0 {
            let segment_id = SegmentId(1);
            let postings_bytes = crate::recovery::encode(self.index.postings())?;
            let doc_lengths = self
                .index
                .doc_lengths()
                .iter()
                .map(|(tid, token_count)| DocLength {
                    tid: *tid,
                    token_count: *token_count,
                })
                .collect::<Vec<_>>();
            let doc_length_bytes = crate::recovery::encode(&doc_lengths)?;
            write_atomic(
                &Self::postings_path(data_dir, self.index_oid, segment_id),
                &postings_bytes,
            )?;
            write_atomic(
                &Self::doc_lengths_path(data_dir, self.index_oid, segment_id),
                &doc_length_bytes,
            )?;
            let metadata = SegmentMeta {
                format_version: crate::posting::SEGMENT_FORMAT_VERSION,
                index_oid: self.index_oid,
                segment_id,
                analyzer: self.analyzer.clone(),
                state: SegmentState::Sealed,
                doc_count: stats.doc_count,
                total_tokens: stats.total_tokens,
                term_df: self
                    .index
                    .postings()
                    .iter()
                    .map(|(term, docs)| (term.clone(), docs.len() as u64))
                    .collect(),
                created_from_lsn: base_lsn,
                applied_lsn: base_lsn,
                row_count: stats.row_count,
                posting_count: stats.posting_count,
                byte_count: (postings_bytes.len() + doc_length_bytes.len()) as u64,
            };
            write_atomic(
                &Self::segment_metadata_path(data_dir, self.index_oid, segment_id),
                &metadata.encode()?,
            )?;
        }
        write_atomic(
            &Self::analyzer_path(data_dir, self.index_oid),
            &crate::recovery::encode(&self.analyzer)?,
        )?;
        write_atomic(
            &Self::manifest_path(data_dir, self.index_oid),
            &self.manifest.encode()?,
        )
    }

    /// Location of the analyzer identity for one inverted index.
    pub fn analyzer_path(data_dir: &Path, index_oid: Oid) -> PathBuf {
        data_dir
            .join("inverted")
            .join(index_oid.0.to_string())
            .join("analyzer.bin")
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

    /// Location of the `(term, document)` posting map for one segment.
    pub fn postings_path(data_dir: &Path, index_oid: Oid, segment_id: SegmentId) -> PathBuf {
        data_dir
            .join("inverted")
            .join(index_oid.0.to_string())
            .join("segments")
            .join(format!("{}.postings", segment_id.0))
    }

    /// Location of deleted document identities for one segment.
    pub fn tombstones_path(data_dir: &Path, index_oid: Oid, segment_id: SegmentId) -> PathBuf {
        data_dir
            .join("inverted")
            .join(index_oid.0.to_string())
            .join("segments")
            .join(format!("{}.tombstones", segment_id.0))
    }

    /// Location of the per-document token-length table for one segment.
    pub fn doc_lengths_path(data_dir: &Path, index_oid: Oid, segment_id: SegmentId) -> PathBuf {
        data_dir
            .join("inverted")
            .join(index_oid.0.to_string())
            .join("segments")
            .join(format!("{}.doc_lengths", segment_id.0))
    }

    /// Recover the manifest and every active segment's metadata and payload.
    /// An empty manifest is valid only at `Lsn::INVALID`. Otherwise the
    /// manifest watermark must equal the maximum active segment coverage.
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
        let analyzer_path = Self::analyzer_path(data_dir, index_oid);
        let persisted_analyzer = if analyzer_path.exists() {
            Some(crate::recovery::decode::<AnalyzerConfig>(&read_bytes(
                &analyzer_path,
            )?)?)
        } else {
            None
        };
        if let Some(analyzer) = &persisted_analyzer {
            if analyzer != expected_analyzer {
                return Err(InvertedError::Corrupted(format!(
                    "analyzer configuration mismatch: expected {} / {:02x?}, got {} / {:02x?}",
                    expected_analyzer.name,
                    expected_analyzer.config_hash,
                    analyzer.name,
                    analyzer.config_hash
                )));
            }
        }
        if manifest.active_segment_ids.is_empty() {
            if persisted_analyzer.is_none() {
                return Err(InvertedError::Corrupted(
                    "empty manifest has no analyzer metadata".to_string(),
                ));
            }
            // An empty backfill still has a real WAL base frontier. Preserve
            // it so the first post-create DML is consumed rather than skipped.
            let mut store = Self::empty_with_analyzer(index_oid, expected_analyzer.clone());
            store.watermark = Watermark {
                consumed_lsn: manifest.applied_lsn,
                durable_applied_lsn: manifest.applied_lsn,
            };
            store.manifest = manifest;
            return Ok(store);
        }
        if manifest
            .active_segment_ids
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        {
            return Err(InvertedError::Corrupted(
                "manifest active segment IDs must be strictly increasing".to_string(),
            ));
        }

        let mut analyzer: Option<AnalyzerConfig> = None;
        let mut max_applied_lsn = Lsn::INVALID;
        let mut postings: BTreeMap<String, BTreeMap<Tid, Posting>> = BTreeMap::new();
        let mut doc_lengths: BTreeMap<Tid, u64> = BTreeMap::new();
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
            if matches!(meta.state, SegmentState::Merging | SegmentState::Retired) {
                return Err(InvertedError::Corrupted(format!(
                    "segment {} is not readable in the active manifest",
                    segment_id.0
                )));
            }
            validate_segment_stats(&meta)?;
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

            let segment_postings: BTreeMap<_, _> = crate::recovery::decode(&read_bytes(
                &Self::postings_path(data_dir, index_oid, *segment_id),
            )?)?;
            let segment_doc_lengths: Vec<DocLength> = crate::recovery::decode(&read_bytes(
                &Self::doc_lengths_path(data_dir, index_oid, *segment_id),
            )?)?;
            let segment_doc_lengths =
                segment_doc_lengths
                    .into_iter()
                    .try_fold(BTreeMap::new(), |mut lengths, doc| {
                        if lengths.insert(doc.tid, doc.token_count).is_some() {
                            return Err(InvertedError::Corrupted(format!(
                                "duplicate document length for {} in segment {}",
                                doc.tid, segment_id.0
                            )));
                        }
                        Ok(lengths)
                    })?;
            validate_segment_payload(&meta, &segment_postings, &segment_doc_lengths)?;
            // Doc-length rows, not postings, define upsert identity. A new
            // version with only stop words has no postings but must still
            // supersede every posting from its older version.
            let upsert_tids = segment_doc_lengths.keys().copied().collect::<BTreeSet<_>>();
            for tid in upsert_tids {
                for documents in postings.values_mut() {
                    documents.remove(&tid);
                }
                doc_lengths.remove(&tid);
            }
            for (term, documents) in segment_postings {
                let target = postings.entry(term).or_default();
                for (tid, posting) in documents {
                    // Segment order is oldest to newest: a newer version
                    // replaces the same document's older posting.
                    target.insert(tid, posting);
                }
            }
            let tombstone_path = Self::tombstones_path(data_dir, index_oid, *segment_id);
            let segment_tombstones: Vec<Tid> = if tombstone_path.exists() {
                crate::recovery::decode(&read_bytes(&tombstone_path)?)?
            } else {
                Vec::new()
            };
            let tombstone_count = segment_tombstones.len();
            let segment_tombstones = segment_tombstones.into_iter().collect::<BTreeSet<_>>();
            if segment_tombstones.len() != tombstone_count
                || segment_tombstones
                    .iter()
                    .any(|tid| segment_doc_lengths.contains_key(tid))
            {
                return Err(InvertedError::Corrupted(format!(
                    "segment {segment_id:?} has duplicate or ambiguous tombstones"
                )));
            }
            for (tid, token_count) in segment_doc_lengths {
                doc_lengths.insert(tid, token_count);
            }
            for tid in segment_tombstones {
                for documents in postings.values_mut() {
                    documents.remove(&tid);
                }
                doc_lengths.remove(&tid);
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
        let total_tokens = doc_lengths.values().try_fold(0u64, |total, count| {
            total.checked_add(*count).ok_or_else(|| {
                InvertedError::Corrupted("recovered total_tokens overflows u64".to_string())
            })
        })?;
        let posting_count = postings.values().try_fold(0u64, |total, documents| {
            total.checked_add(documents.len() as u64).ok_or_else(|| {
                InvertedError::Corrupted("recovered posting_count overflows u64".to_string())
            })
        })?;
        let stats = InMemoryStats {
            doc_count: doc_lengths.len() as u64,
            total_tokens,
            row_count: doc_lengths.len() as u64,
            posting_count,
        };
        let index = InMemoryIndex::from_parts(
            DefaultAnalyzer::from_config(analyzer.clone()),
            postings,
            doc_lengths,
            stats,
        );
        let watermark = Watermark {
            consumed_lsn: manifest.applied_lsn,
            durable_applied_lsn: manifest.applied_lsn,
        };
        remove_orphan_segments(data_dir, index_oid, &manifest);
        Ok(Self {
            manifest,
            index_oid,
            analyzer,
            watermark,
            index,
            wal_sealed: true,
            dirty: false,
            pending_documents: BTreeMap::new(),
            flush_threshold: 1,
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

    /// Set the number of pending documents required before sealing a delta.
    /// A value of zero is invalid because it would disable the batching contract.
    pub fn set_flush_threshold(&mut self, threshold: usize) -> Result<()> {
        if threshold == 0 {
            return Err(InvertedError::InvalidArgument(
                "flush threshold must be greater than zero".to_string(),
            ));
        }
        self.flush_threshold = threshold;
        Ok(())
    }

    /// Whether unsealed changes are present in the in-memory write view.
    pub fn has_unsealed_changes(&self) -> bool {
        !self.pending_documents.is_empty()
    }

    /// Build an empty segment store without inventing a fake index build.
    pub fn build(&self) -> Result<()> {
        Ok(())
    }

    /// Execute the recovered in-memory DAAT index with exact BM25.
    pub fn search(&self, query: &Query, limit: Option<usize>) -> Result<Vec<ScoredTid>> {
        self.index.search(query, limit)
    }

    /// Analyze query text with the segment's own analyzer and execute DAAT.
    pub fn search_text(&self, text: &str, limit: Option<usize>) -> Result<Vec<ScoredTid>> {
        let query = crate::daat::parse_query(self.index.analyzer(), text);
        self.search(&query, limit)
    }

    /// Evaluate a parsed query with exact WAND for bounded top-k.
    pub fn search_wand(&self, query: &Query, limit: Option<usize>) -> Result<Vec<ScoredTid>> {
        self.index.search_wand(query, limit)
    }

    /// Analyze query text and evaluate exact WAND for bounded top-k.
    pub fn search_wand_text(&self, text: &str, limit: Option<usize>) -> Result<Vec<ScoredTid>> {
        let query = crate::daat::parse_query(self.index.analyzer(), text);
        self.search_wand(&query, limit)
    }

    /// Insert one committed document into the in-process write view.
    pub fn insert_document(&mut self, tid: Tid, text: &str) -> Result<()> {
        // Upsert: a newer document version replaces the older posting rather
        // than being rejected as a duplicate.
        self.index.delete_document(tid)?;
        self.index.insert_document(tid, text)?;
        self.pending_documents.insert(tid, Some(text.to_string()));
        self.dirty = true;
        self.wal_sealed = false;
        Ok(())
    }

    /// Delete one committed document from the in-process write view.
    pub fn delete_document(&mut self, tid: Tid) -> Result<bool> {
        let deleted = self.index.delete_document(tid)?;
        if deleted {
            self.pending_documents.insert(tid, None);
            self.dirty = true;
            self.wal_sealed = false;
        }
        Ok(deleted)
    }

    /// Advance the in-process consumed frontier monotonically. Metadata-only
    /// WAL records do not by themselves require a new segment generation.
    pub fn advance_consumed(&mut self, lsn: Lsn) {
        self.watermark.consumed_lsn = self.watermark.consumed_lsn.max(lsn);
    }

    /// Persist only the pending upserts and tombstones as a new segment. Old
    /// files remain intact until the manifest atomically selects the new ID,
    /// so a crash before that rename replays onto the old generation instead
    /// of replaying onto an already-updated file and double-counting documents.
    fn persist_generation(&mut self, data_dir: &Path) -> Result<()> {
        self.persist_generation_inner(data_dir, |_| Ok(()))
    }

    fn persist_generation_inner(
        &mut self,
        data_dir: &Path,
        mut crash_hook: impl FnMut(SealCrashPoint) -> Result<()>,
    ) -> Result<()> {
        if self.pending_documents.is_empty() {
            return Ok(());
        }
        let previous_lsn = self.watermark.durable_applied_lsn;
        let applied_lsn = self.watermark.consumed_lsn;
        let segment_id = self.next_segment_id(data_dir)?;
        let mut delta = InMemoryIndex::new(DefaultAnalyzer::from_config(self.analyzer.clone()));
        let mut tombstones = Vec::new();
        for (tid, change) in &self.pending_documents {
            match change {
                Some(text) => delta.insert_document(*tid, text)?,
                None => tombstones.push(*tid),
            }
        }
        let stats = delta.stats();
        let postings_bytes = crate::recovery::encode(delta.postings())?;
        let doc_lengths = delta
            .doc_lengths()
            .iter()
            .map(|(tid, token_count)| DocLength {
                tid: *tid,
                token_count: *token_count,
            })
            .collect::<Vec<_>>();
        let doc_length_bytes = crate::recovery::encode(&doc_lengths)?;
        let tombstone_bytes = crate::recovery::encode(&tombstones)?;
        write_atomic(
            &Self::postings_path(data_dir, self.index_oid, segment_id),
            &postings_bytes,
        )?;
        write_atomic(
            &Self::doc_lengths_path(data_dir, self.index_oid, segment_id),
            &doc_length_bytes,
        )?;
        write_atomic(
            &Self::tombstones_path(data_dir, self.index_oid, segment_id),
            &tombstone_bytes,
        )?;
        let metadata = SegmentMeta {
            format_version: crate::posting::SEGMENT_FORMAT_VERSION,
            index_oid: self.index_oid,
            segment_id,
            analyzer: self.analyzer.clone(),
            state: SegmentState::Sealed,
            doc_count: stats.doc_count,
            total_tokens: stats.total_tokens,
            term_df: delta
                .postings()
                .iter()
                .map(|(term, docs)| (term.clone(), docs.len() as u64))
                .collect(),
            created_from_lsn: previous_lsn,
            applied_lsn,
            row_count: stats.row_count,
            posting_count: stats.posting_count,
            byte_count: (postings_bytes.len() + doc_length_bytes.len() + tombstone_bytes.len())
                as u64,
        };
        write_atomic(
            &Self::segment_metadata_path(data_dir, self.index_oid, segment_id),
            &metadata.encode()?,
        )?;
        crash_hook(SealCrashPoint::TargetDurable)?;
        let mut active_segment_ids = self.manifest.active_segment_ids.clone();
        active_segment_ids.push(segment_id);
        let new_manifest = SegmentManifest {
            generation: self.manifest.generation.checked_add(1).ok_or_else(|| {
                InvertedError::Corrupted("manifest generation overflow".to_string())
            })?,
            active_segment_ids,
            applied_lsn,
        };
        write_atomic(
            &Self::manifest_path(data_dir, self.index_oid),
            &new_manifest.encode()?,
        )?;
        crash_hook(SealCrashPoint::ManifestPublished)?;
        self.manifest = new_manifest;
        self.watermark.durable_applied_lsn = applied_lsn;
        self.pending_documents.clear();
        Ok(())
    }

    /// Merge all active segments into one target using a bounded-memory
    /// k-way merge. The first pass selects the newest version of every TID;
    /// the second emits only postings owned by that version. Only the current
    /// term's lists are retained, and the old manifest remains authoritative
    /// until the atomic rename.
    pub fn merge(
        &mut self,
        data_dir: &Path,
        wal: &pg_storage::wal::WalWriter,
        xid: pg_storage::types::TxnId,
    ) -> Result<Option<Lsn>> {
        self.merge_inner(data_dir, wal, xid, |_| Ok(()))
    }

    fn merge_inner(
        &mut self,
        data_dir: &Path,
        wal: &pg_storage::wal::WalWriter,
        xid: pg_storage::types::TxnId,
        mut crash_hook: impl FnMut(MergeCrashPoint) -> Result<()>,
    ) -> Result<Option<Lsn>> {
        if self.manifest.active_segment_ids.len() <= 1 {
            return Ok(None);
        }
        if self.dirty || !self.pending_documents.is_empty() || !self.wal_sealed {
            return Err(InvertedError::InvalidArgument(
                "inverted changes must be sealed before merge".to_string(),
            ));
        }
        let mut inputs = self
            .manifest
            .active_segment_ids
            .iter()
            .map(|segment_id| self.read_segment(data_dir, *segment_id))
            .collect::<Result<Vec<_>>>()?;
        let merged_applied_lsn = inputs
            .iter()
            .map(|input| input.meta.applied_lsn)
            .max()
            .unwrap_or(self.manifest.applied_lsn);
        if merged_applied_lsn != self.manifest.applied_lsn {
            return Err(InvertedError::Corrupted(format!(
                "active segment coverage {merged_applied_lsn} does not match manifest watermark {}",
                self.manifest.applied_lsn.0
            )));
        }
        let target_id = self.next_segment_id(data_dir)?;
        let input_segment_ids = self.manifest.active_segment_ids.clone();

        // Resolve versions newest to oldest. A tombstone resolves its TID just
        // like an upsert, but contributes no document to the merged output.
        let mut versions = BTreeMap::new();
        for (input_index, input) in inputs.iter().enumerate().rev() {
            for (tid, token_count) in &input.doc_lengths {
                versions
                    .entry(*tid)
                    .or_insert(Some((input_index, *token_count)));
            }
            for tid in &input.tombstones {
                versions.entry(*tid).or_insert(None);
            }
        }
        let doc_lengths = versions
            .iter()
            .filter_map(|(tid, version)| {
                version.map(|(_, token_count)| DocLength {
                    tid: *tid,
                    token_count,
                })
            })
            .collect::<Vec<_>>();

        let mut heads = inputs
            .iter_mut()
            .map(next_input_term)
            .collect::<Result<Vec<_>>>()?;
        let postings_path = Self::postings_path(data_dir, self.index_oid, target_id);
        let mut writer = crate::segment_io::PostingsWriter::create(&postings_path)?;
        let mut term_df = BTreeMap::new();
        let mut posting_count = 0u64;
        while let Some(term) = heads
            .iter()
            .filter_map(|head| head.as_ref().map(|(term, _)| term.as_str()))
            .min()
            .map(str::to_owned)
        {
            let mut merged_documents = BTreeMap::new();
            for (input_index, head) in heads.iter_mut().enumerate() {
                if head
                    .as_ref()
                    .is_none_or(|(head_term, _)| *head_term != term)
                {
                    continue;
                }
                let (_, documents) = head.take().expect("matched head is present");
                for (tid, posting) in documents {
                    let selected = versions.get(&tid).is_some_and(|version| {
                        version.is_some_and(|(owner, _)| owner == input_index)
                    });
                    if selected && merged_documents.insert(tid, posting).is_some() {
                        return Err(InvertedError::Corrupted(format!(
                            "multiple live postings for {tid} at term {term:?}"
                        )));
                    }
                }
                *head = next_input_term(&mut inputs[input_index])?;
            }
            if !merged_documents.is_empty() {
                writer.write_term(&term, &merged_documents)?;
                posting_count = posting_count
                    .checked_add(merged_documents.len() as u64)
                    .ok_or_else(|| {
                        InvertedError::Corrupted("merged posting_count overflows u64".to_string())
                    })?;
                term_df.insert(term, merged_documents.len() as u64);
            }
        }
        writer.finish()?;
        let postings_len = std::fs::metadata(&postings_path)
            .map_err(|error| {
                InvertedError::Storage(format!("cannot stat {}: {error}", postings_path.display()))
            })?
            .len();

        let doc_length_bytes = crate::recovery::encode(&doc_lengths)?;
        write_atomic(
            &Self::doc_lengths_path(data_dir, self.index_oid, target_id),
            &doc_length_bytes,
        )?;
        // A full merge has no older active segments left to mask, so retaining
        // tombstones would only grow metadata without affecting visibility.
        let tombstones = Vec::<Tid>::new();
        let tombstone_bytes = crate::recovery::encode(&tombstones)?;
        write_atomic(
            &Self::tombstones_path(data_dir, self.index_oid, target_id),
            &tombstone_bytes,
        )?;
        let total_tokens = doc_lengths.iter().try_fold(0u64, |total, doc| {
            total.checked_add(doc.token_count).ok_or_else(|| {
                InvertedError::Corrupted("merged total_tokens overflows u64".to_string())
            })
        })?;
        let metadata = SegmentMeta {
            format_version: crate::posting::SEGMENT_FORMAT_VERSION,
            index_oid: self.index_oid,
            segment_id: target_id,
            analyzer: self.analyzer.clone(),
            state: SegmentState::Sealed,
            doc_count: doc_lengths.len() as u64,
            total_tokens,
            term_df,
            created_from_lsn: inputs
                .iter()
                .map(|input| input.meta.created_from_lsn)
                .min()
                .unwrap_or(Lsn::INVALID),
            applied_lsn: merged_applied_lsn,
            row_count: doc_lengths.len() as u64,
            posting_count,
            byte_count: postings_len
                .checked_add(doc_length_bytes.len() as u64)
                .and_then(|bytes| bytes.checked_add(tombstone_bytes.len() as u64))
                .ok_or_else(|| {
                    InvertedError::Corrupted("merged byte_count overflows u64".to_string())
                })?,
        };
        validate_streaming_segment(
            &metadata,
            &postings_path,
            &doc_lengths
                .iter()
                .map(|doc| (doc.tid, doc.token_count))
                .collect::<BTreeMap<_, _>>(),
        )?;
        write_atomic(
            &Self::segment_metadata_path(data_dir, self.index_oid, target_id),
            &metadata.encode()?,
        )?;
        drop(inputs);
        crash_hook(MergeCrashPoint::TargetDurable)?;
        let new_manifest = SegmentManifest {
            generation: self.manifest.generation.checked_add(1).ok_or_else(|| {
                InvertedError::Corrupted("manifest generation overflow".to_string())
            })?,
            active_segment_ids: vec![target_id],
            applied_lsn: metadata.applied_lsn,
        };
        write_atomic(
            &Self::manifest_path(data_dir, self.index_oid),
            &new_manifest.encode()?,
        )?;
        crash_hook(MergeCrashPoint::ManifestPublished)?;
        self.manifest = new_manifest;
        self.watermark.durable_applied_lsn = metadata.applied_lsn;
        let mut record_ids = input_segment_ids.clone();
        record_ids.push(target_id);
        let record = pg_storage::wal::record::WalRecord {
            lsn: Lsn::INVALID,
            prev_lsn: Lsn::INVALID,
            txn_id: xid,
            record_type: pg_storage::wal::record::WalRecordType::SegmentMerge,
            flags: 0,
            payload: bincode::serde::encode_to_vec(
                record_ids,
                pg_storage::wal::record::bincode_config(),
            )
            .map_err(|error| InvertedError::Storage(error.to_string()))?,
        };
        let record_len = record.record_size() as u64;
        let start = wal
            .append(record)
            .map_err(|error| InvertedError::Storage(error.to_string()))?;
        remove_retired_segments(data_dir, self.index_oid, &input_segment_ids);
        self.wal_sealed = true;
        Ok(Some(start.advance(record_len)))
    }

    fn read_segment(&self, data_dir: &Path, segment_id: SegmentId) -> Result<SegmentInput> {
        let meta = SegmentMeta::decode(&read_bytes(&Self::segment_metadata_path(
            data_dir,
            self.index_oid,
            segment_id,
        ))?)?;
        if meta.index_oid != self.index_oid || meta.segment_id != segment_id {
            return Err(InvertedError::Corrupted(format!(
                "segment {segment_id:?} has inconsistent identity metadata"
            )));
        }
        if meta.state != SegmentState::Sealed {
            return Err(InvertedError::Corrupted(format!(
                "segment {} is not sealed for merge",
                segment_id.0
            )));
        }
        if meta.analyzer != self.analyzer {
            return Err(InvertedError::Corrupted(format!(
                "segment {segment_id:?} uses a different analyzer configuration"
            )));
        }
        validate_segment_stats(&meta)?;
        if meta.created_from_lsn > meta.applied_lsn || meta.applied_lsn > self.manifest.applied_lsn
        {
            return Err(InvertedError::Corrupted(format!(
                "segment {segment_id:?} has inconsistent WAL coverage"
            )));
        }
        let doc_lengths = crate::recovery::decode::<Vec<DocLength>>(&read_bytes(
            &Self::doc_lengths_path(data_dir, self.index_oid, segment_id),
        )?)?
        .into_iter()
        .try_fold(BTreeMap::new(), |mut lengths, doc| {
            if lengths.insert(doc.tid, doc.token_count).is_some() {
                return Err(InvertedError::Corrupted(format!(
                    "duplicate document length for {} in segment {}",
                    doc.tid, segment_id.0
                )));
            }
            Ok(lengths)
        })?;
        validate_segment_doc_lengths(&meta, &doc_lengths)?;
        let tombstone_path = Self::tombstones_path(data_dir, self.index_oid, segment_id);
        let tombstone_rows: Vec<Tid> = if tombstone_path.exists() {
            crate::recovery::decode(&read_bytes(&tombstone_path)?)?
        } else {
            Vec::new()
        };
        let tombstone_count = tombstone_rows.len();
        let tombstones = tombstone_rows.into_iter().collect::<BTreeSet<_>>();
        if tombstones.len() != tombstone_count
            || tombstones.iter().any(|tid| doc_lengths.contains_key(tid))
        {
            return Err(InvertedError::Corrupted(format!(
                "segment {segment_id:?} has duplicate or ambiguous tombstones"
            )));
        }
        let postings = crate::segment_io::PostingsReader::open(&Self::postings_path(
            data_dir,
            self.index_oid,
            segment_id,
        ))?;
        Ok(SegmentInput {
            meta,
            postings,
            doc_lengths,
            tombstones,
            token_sums: BTreeMap::new(),
            seen_terms: 0,
            seen_postings: 0,
        })
    }

    fn next_segment_id(&self, data_dir: &Path) -> Result<SegmentId> {
        let directory = data_dir
            .join("inverted")
            .join(self.index_oid.0.to_string())
            .join("segments");
        let mut highest = 0;
        let entries = match std::fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(SegmentId(1)),
            Err(error) => {
                return Err(InvertedError::Storage(format!(
                    "cannot read {}: {error}",
                    directory.display()
                )))
            }
        };
        for entry in entries {
            let entry = entry.map_err(|error| InvertedError::Storage(error.to_string()))?;
            let path = entry.path();
            // Every published or orphaned artifact reserves its ID. Reusing an
            // ID after a pre-manifest crash could overwrite that orphan.
            if let Some(id) = path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .and_then(|stem| stem.parse::<u64>().ok())
            {
                highest = highest.max(id);
            }
        }
        Ok(SegmentId(highest.checked_add(1).ok_or_else(|| {
            InvertedError::Corrupted("segment ID space exhausted".to_string())
        })?))
    }

    /// Durably publish any new consumed prefix, then emit `SegmentSeal=110`
    /// for the active generation. Repeating an unchanged seal is a no-op.
    /// Returns the end LSN of the last emitted marker, when one was appended.
    pub fn seal(
        &mut self,
        data_dir: &Path,
        wal: &pg_storage::wal::WalWriter,
        xid: pg_storage::types::TxnId,
    ) -> Result<Option<Lsn>> {
        self.seal_inner(data_dir, wal, xid, || Ok(()))
    }

    fn seal_inner(
        &mut self,
        data_dir: &Path,
        wal: &pg_storage::wal::WalWriter,
        xid: pg_storage::types::TxnId,
        mut crash_hook: impl FnMut() -> Result<()>,
    ) -> Result<Option<Lsn>> {
        if self.dirty && self.pending_documents.len() < self.flush_threshold {
            // Small write batches remain in memory and are recovered from WAL
            // after a crash. Deferring their fsync avoids a segment per query.
            return Ok(None);
        }
        if self.wal_sealed {
            return Ok(None);
        }
        let emit_markers = self.dirty || self.manifest.generation == 1;
        if self.dirty {
            self.persist_generation(data_dir)?;
            // Segment publication is the required state transition. The
            // idempotent validation marker does not gate later seal or merge.
            self.dirty = false;
            self.wal_sealed = true;
        }
        let mut last_seal_end = None;
        if emit_markers {
            for segment_id in self.manifest.active_segment_ids.iter().copied() {
                let payload = bincode::serde::encode_to_vec(
                    segment_id,
                    pg_storage::wal::record::bincode_config(),
                )
                .map_err(|error| InvertedError::Storage(error.to_string()))?;
                let record = pg_storage::wal::record::WalRecord {
                    lsn: Lsn::INVALID,
                    prev_lsn: Lsn::INVALID,
                    txn_id: xid,
                    record_type: pg_storage::wal::record::WalRecordType::SegmentSeal,
                    flags: 0,
                    payload,
                };
                let record_len = record.record_size() as u64;
                crash_hook()?;
                let start = wal
                    .append(record)
                    .map_err(|error| InvertedError::Storage(error.to_string()))?;
                last_seal_end = Some(start.advance(record_len));
            }
        }
        self.dirty = false;
        self.wal_sealed = true;
        Ok(last_seal_end)
    }
}

struct SegmentInput {
    meta: SegmentMeta,
    postings: crate::segment_io::PostingsReader,
    doc_lengths: BTreeMap<Tid, u64>,
    tombstones: BTreeSet<Tid>,
    token_sums: BTreeMap<Tid, u64>,
    seen_terms: u64,
    seen_postings: u64,
}

#[derive(Debug, Clone, Copy)]
enum SealCrashPoint {
    TargetDurable,
    ManifestPublished,
}

#[derive(Debug, Clone, Copy)]
enum MergeCrashPoint {
    TargetDurable,
    ManifestPublished,
}

fn next_input_term(input: &mut SegmentInput) -> Result<Option<crate::segment_io::TermEntries>> {
    let Some((term, documents)) = input.postings.next_term()? else {
        if input.seen_terms != input.meta.term_df.len() as u64
            || input.seen_postings != input.meta.posting_count
        {
            return Err(InvertedError::Corrupted(format!(
                "segment {:?} posting counts do not match its metadata",
                input.meta.segment_id
            )));
        }
        validate_document_token_sums(&input.doc_lengths, &input.token_sums)?;
        return Ok(None);
    };
    let document_count = documents.len() as u64;
    if input.meta.term_df.get(&term) != Some(&document_count) {
        return Err(InvertedError::Corrupted(format!(
            "segment {:?} term_df does not match postings",
            input.meta.segment_id
        )));
    }
    for (tid, posting) in &documents {
        if !input.doc_lengths.contains_key(tid) {
            return Err(InvertedError::Corrupted(format!(
                "posting for {tid} has no document length"
            )));
        }
        accumulate_document_tokens(&mut input.token_sums, *tid, posting.term_freq)?;
    }
    input.seen_terms = input
        .seen_terms
        .checked_add(1)
        .ok_or_else(|| InvertedError::Corrupted("segment term count overflows u64".to_string()))?;
    input.seen_postings = input
        .seen_postings
        .checked_add(document_count)
        .ok_or_else(|| {
            InvertedError::Corrupted("segment posting_count overflows u64".to_string())
        })?;
    Ok(Some((term, documents)))
}

/// Redo handler for the initial-segment seal marker. File publication happens
/// before this record is appended, so applying it again is intentionally a
/// no-op after strict payload validation.
#[derive(Debug)]
pub struct SegmentSealRedoHandler;

impl pg_storage::recovery::RedoHandler for SegmentSealRedoHandler {
    fn kind(&self) -> pg_storage::wal::record::WalRecordType {
        pg_storage::wal::record::WalRecordType::SegmentSeal
    }

    fn apply(
        &self,
        record: &pg_storage::wal::record::WalRecord,
        _ctx: &mut pg_storage::recovery::RedoContext<'_>,
    ) -> pg_storage::error::Result<()> {
        let (segment_id, consumed) = bincode::serde::decode_from_slice::<SegmentId, _>(
            &record.payload,
            pg_storage::wal::record::bincode_config(),
        )
        .map_err(|error| pg_storage::error::StorageError::Serialize(error.to_string()))?;
        if consumed != record.payload.len() || segment_id.0 == 0 {
            return Err(pg_storage::error::StorageError::MetadataCorrupted(
                "malformed SegmentSeal payload".to_string(),
            ));
        }
        Ok(())
    }
}

/// Redo handler for merge markers. Target files and the manifest are already
/// durable, so replay validates the ordered input/target IDs and is otherwise
/// intentionally idempotent.
#[derive(Debug)]
pub struct SegmentMergeRedoHandler;

impl pg_storage::recovery::RedoHandler for SegmentMergeRedoHandler {
    fn kind(&self) -> pg_storage::wal::record::WalRecordType {
        pg_storage::wal::record::WalRecordType::SegmentMerge
    }

    fn apply(
        &self,
        record: &pg_storage::wal::record::WalRecord,
        _ctx: &mut pg_storage::recovery::RedoContext<'_>,
    ) -> pg_storage::error::Result<()> {
        decode_segment_merge_ids(&record.payload)?;
        Ok(())
    }
}

fn decode_segment_merge_ids(payload: &[u8]) -> pg_storage::error::Result<Vec<SegmentId>> {
    let (ids, consumed) = bincode::serde::decode_from_slice::<Vec<SegmentId>, _>(
        payload,
        pg_storage::wal::record::bincode_config(),
    )
    .map_err(|error| pg_storage::error::StorageError::Serialize(error.to_string()))?;
    if consumed != payload.len()
        || ids.len() < 2
        || ids.iter().any(|id| id.0 == 0)
        || ids.windows(2).any(|pair| pair[0] >= pair[1])
    {
        return Err(pg_storage::error::StorageError::MetadataCorrupted(
            "malformed SegmentMerge payload".to_string(),
        ));
    }
    Ok(ids)
}

fn remove_retired_segments(data_dir: &Path, index_oid: Oid, segment_ids: &[SegmentId]) {
    for segment_id in segment_ids {
        let postings = SegmentStore::postings_path(data_dir, index_oid, *segment_id);
        for path in [
            SegmentStore::segment_metadata_path(data_dir, index_oid, *segment_id),
            postings.clone(),
            SegmentStore::doc_lengths_path(data_dir, index_oid, *segment_id),
            SegmentStore::tombstones_path(data_dir, index_oid, *segment_id),
            postings.with_extension("tmp"),
            postings.with_extension("spool"),
        ] {
            // The new manifest is already authoritative. Cleanup is best-effort:
            // an I/O failure may leave an invisible orphan, but must not turn a
            // committed merge into an apparent failure. The target's higher ID
            // still prevents ID reuse, so the orphan cannot affect correctness.
            let _ = std::fs::remove_file(path);
        }
    }
}

/// Best-effort cleanup after a complete active generation has been validated.
/// Only IDs below the active high-water mark are eligible. Newer orphans stay
/// in place so the directory scan cannot reuse their IDs. The engine serializes
/// recovery and writers for one index, so no merge reads these files here.
fn remove_orphan_segments(data_dir: &Path, index_oid: Oid, manifest: &SegmentManifest) {
    let Some(high_water) = manifest.active_segment_ids.last().copied() else {
        return;
    };
    let active = manifest
        .active_segment_ids
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let directory = data_dir
        .join("inverted")
        .join(index_oid.0.to_string())
        .join("segments");
    let Ok(entries) = std::fs::read_dir(directory) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Some(id) = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .and_then(|stem| stem.parse::<u64>().ok())
        else {
            continue;
        };
        if id < high_water.0 && !active.contains(&SegmentId(id)) {
            let _ = std::fs::remove_file(path);
        }
    }
}

fn read_bytes(path: &Path) -> Result<Vec<u8>> {
    std::fs::read(path)
        .map_err(|error| InvertedError::Storage(format!("cannot read {}: {error}", path.display())))
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path.parent().ok_or_else(|| {
        InvertedError::InvalidArgument(format!("{} has no parent directory", path.display()))
    })?;
    std::fs::create_dir_all(parent).map_err(|error| {
        InvertedError::Storage(format!("cannot create {}: {error}", parent.display()))
    })?;
    let temporary = path.with_extension("tmp");
    let mut file = std::fs::File::create(&temporary).map_err(|error| {
        InvertedError::Storage(format!("cannot create {}: {error}", temporary.display()))
    })?;
    std::io::Write::write_all(&mut file, bytes).map_err(|error| {
        InvertedError::Storage(format!("cannot write {}: {error}", temporary.display()))
    })?;
    file.sync_all().map_err(|error| {
        InvertedError::Storage(format!("cannot fsync {}: {error}", temporary.display()))
    })?;
    std::fs::rename(&temporary, path).map_err(|error| {
        InvertedError::Storage(format!(
            "cannot rename {} to {}: {error}",
            temporary.display(),
            path.display()
        ))
    })?;
    let directory = std::fs::File::open(parent).map_err(|error| {
        InvertedError::Storage(format!("cannot open {}: {error}", parent.display()))
    })?;
    directory.sync_all().map_err(|error| {
        InvertedError::Storage(format!("cannot fsync {}: {error}", parent.display()))
    })
}

fn validate_segment_stats(meta: &SegmentMeta) -> Result<()> {
    if meta.row_count < meta.doc_count {
        return Err(InvertedError::Corrupted(
            "segment row_count is smaller than doc_count".to_string(),
        ));
    }
    if meta.posting_count < meta.term_df.len() as u64 || meta.posting_count > meta.total_tokens {
        return Err(InvertedError::Corrupted(format!(
            "segment posting_count {} is outside term_df.len()..=total_tokens ({}..={})",
            meta.posting_count,
            meta.term_df.len(),
            meta.total_tokens
        )));
    }
    if meta.doc_count == 0
        && (meta.total_tokens != 0 || meta.posting_count != 0 || !meta.term_df.is_empty())
    {
        return Err(InvertedError::Corrupted(
            "empty segment has postings or document frequencies".to_string(),
        ));
    }
    if meta.total_tokens == 0 && !meta.term_df.is_empty() {
        return Err(InvertedError::Corrupted(
            "segment has term frequencies but no tokens".to_string(),
        ));
    }
    if meta.term_df.values().any(|df| *df > meta.doc_count) {
        return Err(InvertedError::Corrupted(
            "segment term_df exceeds doc_count".to_string(),
        ));
    }
    Ok(())
}

fn validate_segment_doc_lengths(
    meta: &SegmentMeta,
    doc_lengths: &BTreeMap<Tid, u64>,
) -> Result<()> {
    if doc_lengths.len() as u64 != meta.doc_count {
        return Err(InvertedError::Corrupted(
            "segment doc_count does not match doc-length rows".to_string(),
        ));
    }
    let total_tokens = doc_lengths.values().try_fold(0u64, |total, count| {
        total.checked_add(*count).ok_or_else(|| {
            InvertedError::Corrupted("segment total_tokens overflows u64".to_string())
        })
    })?;
    if total_tokens != meta.total_tokens {
        return Err(InvertedError::Corrupted(
            "segment total_tokens does not match doc lengths".to_string(),
        ));
    }
    Ok(())
}

fn accumulate_document_tokens(
    token_sums: &mut BTreeMap<Tid, u64>,
    tid: Tid,
    term_freq: u32,
) -> Result<()> {
    let total = token_sums
        .get(&tid)
        .copied()
        .unwrap_or(0)
        .checked_add(u64::from(term_freq))
        .ok_or_else(|| InvertedError::Corrupted(format!("token count for {tid} overflows u64")))?;
    token_sums.insert(tid, total);
    Ok(())
}

fn validate_document_token_sums(
    doc_lengths: &BTreeMap<Tid, u64>,
    token_sums: &BTreeMap<Tid, u64>,
) -> Result<()> {
    if token_sums.keys().any(|tid| !doc_lengths.contains_key(tid)) {
        return Err(InvertedError::Corrupted(
            "posting tokens reference a missing document length".to_string(),
        ));
    }
    for (tid, expected) in doc_lengths {
        let actual = token_sums.get(tid).copied().unwrap_or(0);
        if actual != *expected {
            return Err(InvertedError::Corrupted(format!(
                "document {tid} has token_count {expected}, but postings total {actual}"
            )));
        }
    }
    Ok(())
}

fn validate_streaming_segment(
    meta: &SegmentMeta,
    postings_path: &Path,
    doc_lengths: &BTreeMap<Tid, u64>,
) -> Result<()> {
    validate_segment_doc_lengths(meta, doc_lengths)?;
    let mut reader = crate::segment_io::PostingsReader::open(postings_path)?;
    let mut term_df = BTreeMap::new();
    let mut posting_count = 0u64;
    let mut token_sums = BTreeMap::new();
    while let Some((term, documents)) = reader.next_term()? {
        for (tid, posting) in &documents {
            if !doc_lengths.contains_key(tid) {
                return Err(InvertedError::Corrupted(format!(
                    "posting for {tid} has no document length"
                )));
            }
            accumulate_document_tokens(&mut token_sums, *tid, posting.term_freq)?;
        }
        posting_count = posting_count
            .checked_add(documents.len() as u64)
            .ok_or_else(|| {
                InvertedError::Corrupted("segment posting_count overflows u64".to_string())
            })?;
        term_df.insert(term, documents.len() as u64);
    }
    if posting_count != meta.posting_count || term_df != meta.term_df {
        return Err(InvertedError::Corrupted(
            "segment statistics do not match streamed postings".to_string(),
        ));
    }
    validate_document_token_sums(doc_lengths, &token_sums)
}

fn validate_segment_payload(
    meta: &SegmentMeta,
    postings: &BTreeMap<String, BTreeMap<Tid, Posting>>,
    doc_lengths: &BTreeMap<Tid, u64>,
) -> Result<()> {
    validate_segment_doc_lengths(meta, doc_lengths)?;
    let posting_count = postings.values().try_fold(0u64, |total, documents| {
        total.checked_add(documents.len() as u64).ok_or_else(|| {
            InvertedError::Corrupted("segment posting_count overflows u64".to_string())
        })
    })?;
    if posting_count != meta.posting_count {
        return Err(InvertedError::Corrupted(
            "segment posting_count does not match posting rows".to_string(),
        ));
    }
    let term_df = postings
        .iter()
        .map(|(term, documents)| (term.clone(), documents.len() as u64))
        .collect::<BTreeMap<_, _>>();
    if term_df != meta.term_df {
        return Err(InvertedError::Corrupted(
            "segment term_df does not match postings".to_string(),
        ));
    }
    let mut token_sums = BTreeMap::new();
    for documents in postings.values() {
        for (tid, posting) in documents {
            if *tid != posting.tid {
                return Err(InvertedError::Corrupted(format!(
                    "posting map key {tid} disagrees with posting TID {:?}",
                    posting.tid
                )));
            }
            if !doc_lengths.contains_key(&posting.tid) {
                return Err(InvertedError::Corrupted(format!(
                    "posting for {:?} has no document length",
                    posting.tid
                )));
            }
            posting.validate()?;
            accumulate_document_tokens(&mut token_sums, *tid, posting.term_freq)?;
        }
    }
    validate_document_token_sums(doc_lengths, &token_sums)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tid(slot: u16) -> Tid {
        Tid {
            page_id: pg_storage::types::PageId(1),
            slot_id: slot,
        }
    }

    fn merge_fixture() -> (tempfile::TempDir, pg_storage::wal::WalWriter, SegmentStore) {
        let tmp = tempfile::TempDir::new().unwrap();
        let index_oid = Oid(9879);
        let config = pg_storage::config::StorageConfig::new(tmp.path());
        let wal = pg_storage::wal::WalWriter::open(tmp.path(), &config).unwrap();
        let mut store = SegmentStore::create(
            tmp.path(),
            index_oid,
            AnalyzerConfig::builtin(),
            vec![(tid(1), "rust".to_string())],
            Lsn::FIRST,
        )
        .unwrap();
        store
            .seal(tmp.path(), &wal, pg_storage::types::TxnId(1))
            .unwrap();
        for (slot, text, lsn) in [(2, "contract", 128), (3, "other", 256)] {
            store.insert_document(tid(slot), text).unwrap();
            store.advance_consumed(Lsn(lsn));
            store
                .seal(tmp.path(), &wal, pg_storage::types::TxnId(1))
                .unwrap();
        }
        (tmp, wal, store)
    }

    #[test]
    fn merge_rejects_unsealed_changes_without_losing_them() {
        let (tmp, wal, mut store) = merge_fixture();
        store.insert_document(tid(4), "pending").unwrap();
        store.advance_consumed(Lsn(384));
        let error = store
            .merge(tmp.path(), &wal, pg_storage::types::TxnId(1))
            .unwrap_err();
        assert!(matches!(error, InvertedError::InvalidArgument(_)));
        assert_eq!(store.manifest().active_segment_ids.len(), 3);
        store
            .seal(tmp.path(), &wal, pg_storage::types::TxnId(1))
            .unwrap();
        store
            .merge(tmp.path(), &wal, pg_storage::types::TxnId(1))
            .unwrap();
        let reopened =
            SegmentStore::open(tmp.path(), Oid(9879), &AnalyzerConfig::builtin()).unwrap();
        assert_eq!(reopened.search_text("pending", None).unwrap().len(), 1);
        assert_eq!(reopened.manifest().active_segment_ids, vec![SegmentId(5)]);
    }

    #[test]
    fn seal_marker_failure_leaves_published_generation_clean() {
        let (tmp, wal, mut store) = merge_fixture();
        store.insert_document(tid(4), "fresh").unwrap();
        store.advance_consumed(Lsn(384));
        let error = store
            .seal_inner(tmp.path(), &wal, pg_storage::types::TxnId(1), || {
                Err(InvertedError::Storage(
                    "injected seal marker failure".to_string(),
                ))
            })
            .unwrap_err();
        assert!(matches!(error, InvertedError::Storage(_)));
        assert!(!store.dirty);
        assert!(store.wal_sealed);
        assert!(store.pending_documents.is_empty());

        let manifest = store.manifest().clone();
        assert_eq!(
            store
                .seal(tmp.path(), &wal, pg_storage::types::TxnId(1))
                .unwrap(),
            None
        );
        assert_eq!(store.manifest(), &manifest);
        store
            .merge(tmp.path(), &wal, pg_storage::types::TxnId(1))
            .unwrap();
        let reopened =
            SegmentStore::open(tmp.path(), Oid(9879), &AnalyzerConfig::builtin()).unwrap();
        assert_eq!(reopened.search_text("fresh", None).unwrap().len(), 1);
    }

    #[test]
    fn open_and_merge_reject_duplicate_or_ambiguous_tombstones() {
        let (tmp, wal, mut store) = merge_fixture();
        let path = SegmentStore::tombstones_path(tmp.path(), Oid(9879), SegmentId(1));
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(
            &path,
            crate::recovery::encode(&vec![tid(1), tid(1)]).unwrap(),
        )
        .unwrap();
        assert!(matches!(
            SegmentStore::open(tmp.path(), Oid(9879), &AnalyzerConfig::builtin()),
            Err(InvertedError::Corrupted(_))
        ));
        assert!(matches!(
            store.merge(tmp.path(), &wal, pg_storage::types::TxnId(1)),
            Err(InvertedError::Corrupted(_))
        ));
    }

    #[test]
    fn merge_rejects_manifest_coverage_gap_without_publishing() {
        let (tmp, wal, mut store) = merge_fixture();
        let path = SegmentStore::segment_metadata_path(tmp.path(), Oid(9879), SegmentId(3));
        let mut metadata = SegmentMeta::decode(&std::fs::read(&path).unwrap()).unwrap();
        metadata.applied_lsn = Lsn(128);
        std::fs::write(&path, metadata.encode().unwrap()).unwrap();
        let manifest_path = SegmentStore::manifest_path(tmp.path(), Oid(9879));
        let old_manifest = std::fs::read(&manifest_path).unwrap();
        assert!(matches!(
            store.merge(tmp.path(), &wal, pg_storage::types::TxnId(1)),
            Err(InvertedError::Corrupted(_))
        ));
        assert_eq!(std::fs::read(&manifest_path).unwrap(), old_manifest);
        assert_eq!(store.manifest().applied_lsn, Lsn(256));
        assert!(!SegmentStore::segment_metadata_path(tmp.path(), Oid(9879), SegmentId(4)).exists());
    }

    #[test]
    fn document_token_counts_are_cross_checked_on_open_and_merge() {
        for inflate_posting in [true, false] {
            let (tmp, wal, mut store) = merge_fixture();
            let first = SegmentId(1);
            if inflate_posting {
                let path = SegmentStore::postings_path(tmp.path(), Oid(9879), first);
                let mut postings: BTreeMap<String, BTreeMap<Tid, Posting>> =
                    crate::recovery::decode(&std::fs::read(&path).unwrap()).unwrap();
                let posting = postings.get_mut("rust").unwrap().get_mut(&tid(1)).unwrap();
                posting.term_freq = 2;
                posting.positions = vec![0, 1];
                std::fs::write(&path, crate::recovery::encode(&postings).unwrap()).unwrap();
            } else {
                let metadata_path =
                    SegmentStore::segment_metadata_path(tmp.path(), Oid(9879), first);
                let mut metadata =
                    SegmentMeta::decode(&std::fs::read(&metadata_path).unwrap()).unwrap();
                metadata.total_tokens = 2;
                std::fs::write(&metadata_path, metadata.encode().unwrap()).unwrap();
                let lengths_path = SegmentStore::doc_lengths_path(tmp.path(), Oid(9879), first);
                let mut lengths: Vec<DocLength> =
                    crate::recovery::decode(&std::fs::read(&lengths_path).unwrap()).unwrap();
                lengths[0].token_count = 2;
                std::fs::write(&lengths_path, crate::recovery::encode(&lengths).unwrap()).unwrap();
            }
            assert!(matches!(
                SegmentStore::open(tmp.path(), Oid(9879), &AnalyzerConfig::builtin()),
                Err(InvertedError::Corrupted(_))
            ));
            let manifest_path = SegmentStore::manifest_path(tmp.path(), Oid(9879));
            let old_manifest = std::fs::read(&manifest_path).unwrap();
            assert!(matches!(
                store.merge(tmp.path(), &wal, pg_storage::types::TxnId(1)),
                Err(InvertedError::Corrupted(_))
            ));
            assert_eq!(std::fs::read(&manifest_path).unwrap(), old_manifest);
        }
    }

    #[test]
    fn merge_rejects_non_sealed_inputs_without_publishing() {
        for state in [SegmentState::Active, SegmentState::Frozen] {
            let (tmp, wal, mut store) = merge_fixture();
            let path = SegmentStore::segment_metadata_path(tmp.path(), Oid(9879), SegmentId(1));
            let mut metadata = SegmentMeta::decode(&std::fs::read(&path).unwrap()).unwrap();
            metadata.state = state;
            std::fs::write(&path, metadata.encode().unwrap()).unwrap();
            let manifest_path = SegmentStore::manifest_path(tmp.path(), Oid(9879));
            let old_manifest = std::fs::read(&manifest_path).unwrap();
            assert!(matches!(
                store.merge(tmp.path(), &wal, pg_storage::types::TxnId(1)),
                Err(InvertedError::Corrupted(_))
            ));
            assert_eq!(std::fs::read(&manifest_path).unwrap(), old_manifest);
        }
    }

    #[test]
    fn segment_merge_payload_validation_is_strict_and_repeatable() {
        let encode = |ids: &[SegmentId]| {
            bincode::serde::encode_to_vec(ids, pg_storage::wal::record::bincode_config()).unwrap()
        };
        let ids = vec![SegmentId(1), SegmentId(2), SegmentId(5)];
        let payload = encode(&ids);
        assert_eq!(decode_segment_merge_ids(&payload).unwrap(), ids);
        assert_eq!(decode_segment_merge_ids(&payload).unwrap(), ids);
        for mut invalid in [
            encode(&[]),
            encode(&[SegmentId(1)]),
            encode(&[SegmentId(0), SegmentId(1)]),
            encode(&[SegmentId(1), SegmentId(1)]),
            encode(&[SegmentId(2), SegmentId(1)]),
            payload.clone(),
        ] {
            if invalid == payload {
                invalid.push(0);
            }
            assert!(decode_segment_merge_ids(&invalid).is_err());
        }
    }

    #[test]
    fn seal_crash_windows_expose_old_or_complete_new_generation() {
        for point in [
            SealCrashPoint::TargetDurable,
            SealCrashPoint::ManifestPublished,
        ] {
            let (tmp, wal, mut store) = merge_fixture();
            store.insert_document(tid(4), "fresh").unwrap();
            store.advance_consumed(Lsn(384));
            let manifest_path = SegmentStore::manifest_path(tmp.path(), Oid(9879));
            let old_manifest = std::fs::read(&manifest_path).unwrap();
            let error = store
                .persist_generation_inner(tmp.path(), |current| {
                    if std::mem::discriminant(&current) == std::mem::discriminant(&point) {
                        Err(InvertedError::Storage("injected seal crash".to_string()))
                    } else {
                        Ok(())
                    }
                })
                .unwrap_err();
            assert!(matches!(error, InvertedError::Storage(_)));
            let after = matches!(point, SealCrashPoint::ManifestPublished);
            if after {
                let reopened =
                    SegmentStore::open(tmp.path(), Oid(9879), &AnalyzerConfig::builtin()).unwrap();
                assert_eq!(
                    reopened.manifest().active_segment_ids,
                    vec![SegmentId(1), SegmentId(2), SegmentId(3), SegmentId(4)]
                );
                assert_eq!(reopened.search_text("fresh", None).unwrap().len(), 1);
            } else {
                assert_eq!(std::fs::read(&manifest_path).unwrap(), old_manifest);
                let mut reopened =
                    SegmentStore::open(tmp.path(), Oid(9879), &AnalyzerConfig::builtin()).unwrap();
                assert!(reopened.search_text("fresh", None).unwrap().is_empty());
                reopened.insert_document(tid(4), "fresh").unwrap();
                reopened.advance_consumed(Lsn(384));
                reopened
                    .seal(tmp.path(), &wal, pg_storage::types::TxnId(1))
                    .unwrap();
                assert!(reopened
                    .manifest()
                    .active_segment_ids
                    .contains(&SegmentId(5)));
                assert_eq!(reopened.search_text("fresh", None).unwrap().len(), 1);
            }
        }
    }

    #[test]
    fn merge_crash_before_manifest_keeps_old_generation_and_reserves_target_id() {
        let (tmp, wal, mut store) = merge_fixture();
        let manifest_path = SegmentStore::manifest_path(tmp.path(), Oid(9879));
        let old_manifest = std::fs::read(&manifest_path).unwrap();
        let error = store
            .merge_inner(
                tmp.path(),
                &wal,
                pg_storage::types::TxnId(1),
                |point| match point {
                    MergeCrashPoint::TargetDurable => Err(InvertedError::Storage(
                        "injected pre-manifest crash".to_string(),
                    )),
                    MergeCrashPoint::ManifestPublished => Ok(()),
                },
            )
            .unwrap_err();
        assert!(matches!(error, InvertedError::Storage(_)));
        assert_eq!(std::fs::read(&manifest_path).unwrap(), old_manifest);
        assert!(SegmentStore::postings_path(tmp.path(), Oid(9879), SegmentId(4)).exists());

        let mut reopened =
            SegmentStore::open(tmp.path(), Oid(9879), &AnalyzerConfig::builtin()).unwrap();
        assert_eq!(
            reopened.manifest().active_segment_ids,
            vec![SegmentId(1), SegmentId(2), SegmentId(3)]
        );
        reopened.insert_document(tid(4), "fresh").unwrap();
        reopened.advance_consumed(Lsn(384));
        reopened
            .seal(tmp.path(), &wal, pg_storage::types::TxnId(1))
            .unwrap();
        assert!(reopened
            .manifest()
            .active_segment_ids
            .contains(&SegmentId(5)));
        assert!(!reopened
            .manifest()
            .active_segment_ids
            .contains(&SegmentId(4)));
        reopened
            .merge(tmp.path(), &wal, pg_storage::types::TxnId(1))
            .unwrap();
        assert_eq!(reopened.manifest().active_segment_ids, vec![SegmentId(6)]);
        let recovered =
            SegmentStore::open(tmp.path(), Oid(9879), &AnalyzerConfig::builtin()).unwrap();
        for term in ["rust", "contract", "other", "fresh"] {
            assert_eq!(recovered.search_text(term, None).unwrap().len(), 1);
        }
    }

    #[test]
    fn merge_crash_after_manifest_exposes_only_complete_new_generation() {
        let (tmp, wal, mut store) = merge_fixture();
        let error = store
            .merge_inner(
                tmp.path(),
                &wal,
                pg_storage::types::TxnId(1),
                |point| match point {
                    MergeCrashPoint::TargetDurable => Ok(()),
                    MergeCrashPoint::ManifestPublished => Err(InvertedError::Storage(
                        "injected post-manifest crash".to_string(),
                    )),
                },
            )
            .unwrap_err();
        assert!(matches!(error, InvertedError::Storage(_)));
        let reopened =
            SegmentStore::open(tmp.path(), Oid(9879), &AnalyzerConfig::builtin()).unwrap();
        assert_eq!(reopened.manifest().active_segment_ids, vec![SegmentId(4)]);
        for term in ["rust", "contract", "other"] {
            assert_eq!(reopened.search_text(term, None).unwrap().len(), 1);
        }
        let segments = tmp.path().join("inverted/9879/segments");
        for entry in std::fs::read_dir(segments).unwrap() {
            let path = entry.unwrap().path();
            assert!(!matches!(
                path.extension().and_then(|extension| extension.to_str()),
                Some("tmp" | "spool")
            ));
        }
    }

    #[test]
    fn reopen_cleans_only_orphans_below_active_high_water() {
        let (tmp, wal, mut store) = merge_fixture();
        store
            .merge(tmp.path(), &wal, pg_storage::types::TxnId(1))
            .unwrap();
        let directory = tmp.path().join("inverted/9879/segments");
        let old_orphan = directory.join("2.spool");
        let high_orphan = directory.join("5.spool");
        std::fs::write(&old_orphan, b"old").unwrap();
        std::fs::write(&high_orphan, b"high").unwrap();

        let mut reopened =
            SegmentStore::open(tmp.path(), Oid(9879), &AnalyzerConfig::builtin()).unwrap();
        assert!(!old_orphan.exists());
        assert!(high_orphan.exists());
        reopened.insert_document(tid(4), "fresh").unwrap();
        reopened.advance_consumed(Lsn(512));
        reopened
            .seal(tmp.path(), &wal, pg_storage::types::TxnId(1))
            .unwrap();
        assert_eq!(
            reopened.manifest().active_segment_ids,
            vec![SegmentId(4), SegmentId(6)]
        );
    }

    #[test]
    fn manifest_rejects_non_monotonic_segment_read_order() {
        let (tmp, _wal, _store) = merge_fixture();
        let path = SegmentStore::manifest_path(tmp.path(), Oid(9879));
        let mut manifest = SegmentManifest::decode(&std::fs::read(&path).unwrap()).unwrap();
        manifest.active_segment_ids.reverse();
        std::fs::write(&path, manifest.encode().unwrap()).unwrap();
        assert!(matches!(
            SegmentStore::open(tmp.path(), Oid(9879), &AnalyzerConfig::builtin()),
            Err(InvertedError::Corrupted(_))
        ));
    }

    #[test]
    fn reader_merge_and_dml_are_serialized_without_mixed_generations() {
        let (tmp, wal, store) = merge_fixture();
        let store = std::sync::Arc::new(std::sync::RwLock::new(store));
        let wal = std::sync::Arc::new(wal);
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(5));
        let mut readers = Vec::new();
        for _ in 0..4 {
            let store = std::sync::Arc::clone(&store);
            let barrier = std::sync::Arc::clone(&barrier);
            readers.push(std::thread::spawn(move || {
                barrier.wait();
                for _ in 0..20 {
                    let store = store.read().unwrap();
                    for term in ["rust", "contract", "other"] {
                        assert_eq!(store.search_text(term, None).unwrap().len(), 1);
                    }
                }
            }));
        }
        {
            let store = std::sync::Arc::clone(&store);
            let barrier = std::sync::Arc::clone(&barrier);
            let wal = std::sync::Arc::clone(&wal);
            barrier.wait();
            for offset in 0..8u16 {
                let mut store = store.write().unwrap();
                store.insert_document(tid(10 + offset), "shared").unwrap();
                store.advance_consumed(Lsn(512 + u64::from(offset) * 8));
                store
                    .seal(tmp.path(), &wal, pg_storage::types::TxnId(1))
                    .unwrap();
                if store.manifest().active_segment_ids.len() >= 4 {
                    store
                        .merge(tmp.path(), &wal, pg_storage::types::TxnId(1))
                        .unwrap();
                }
            }
        }
        for reader in readers {
            reader.join().unwrap();
        }
        let store = store.read().unwrap();
        assert_eq!(store.search_text("shared", None).unwrap().len(), 8);
        for term in ["rust", "contract", "other"] {
            assert_eq!(store.search_text(term, None).unwrap().len(), 1);
        }
        drop(store);
        wal.flush().unwrap();
        let reopened =
            SegmentStore::open(tmp.path(), Oid(9879), &AnalyzerConfig::builtin()).unwrap();
        assert_eq!(reopened.search_text("shared", None).unwrap().len(), 8);
    }

    #[test]
    fn empty_or_single_segment_merge_is_a_no_op() {
        let tmp = tempfile::TempDir::new().unwrap();
        let config = pg_storage::config::StorageConfig::new(tmp.path());
        let wal = pg_storage::wal::WalWriter::open(tmp.path(), &config).unwrap();
        let mut empty = SegmentStore::new(Oid(9879));
        assert!(empty
            .merge(tmp.path(), &wal, pg_storage::types::TxnId(1))
            .unwrap()
            .is_none());
        let mut single = SegmentStore::create(
            tmp.path(),
            Oid(9879),
            AnalyzerConfig::builtin(),
            vec![(tid(1), "rust".to_string())],
            Lsn::FIRST,
        )
        .unwrap();
        single
            .seal(tmp.path(), &wal, pg_storage::types::TxnId(1))
            .unwrap();
        assert!(single
            .merge(tmp.path(), &wal, pg_storage::types::TxnId(1))
            .unwrap()
            .is_none());
    }
}
