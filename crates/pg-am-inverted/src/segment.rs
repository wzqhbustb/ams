//! Segment manifest and store contracts.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

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

/// Segment storage with durable metadata, postings, and an in-memory DAAT view.
pub struct SegmentStore {
    manifest: SegmentManifest,
    index_oid: Oid,
    analyzer: AnalyzerConfig,
    watermark: Watermark,
    index: InMemoryIndex<DefaultAnalyzer>,
    /// Whether every active segment already has its idempotent seal record.
    wal_sealed: bool,
    /// Whether the in-memory document set differs from the durable generation.
    dirty: bool,
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
        let unique_ids = manifest.active_segment_ids.iter().collect::<BTreeSet<_>>();
        if unique_ids.len() != manifest.active_segment_ids.len() {
            return Err(InvertedError::Corrupted(
                "manifest contains duplicate active segment IDs".to_string(),
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
            for (term, documents) in segment_postings {
                let target = postings.entry(term).or_default();
                for (tid, posting) in documents {
                    if target.insert(tid, posting).is_some() {
                        return Err(InvertedError::Corrupted(format!(
                            "duplicate posting for {tid} across active segments"
                        )));
                    }
                }
            }
            for (tid, token_count) in segment_doc_lengths {
                if doc_lengths.insert(tid, token_count).is_some() {
                    return Err(InvertedError::Corrupted(format!(
                        "duplicate document length for {tid} across active segments"
                    )));
                }
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
        let stats = InMemoryStats {
            doc_count: doc_lengths.len() as u64,
            total_tokens: doc_lengths.values().sum(),
            row_count: doc_lengths.len() as u64,
            posting_count: postings
                .values()
                .map(|documents| documents.len() as u64)
                .sum(),
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
        Ok(Self {
            manifest,
            index_oid,
            analyzer,
            watermark,
            index,
            wal_sealed: true,
            dirty: false,
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

    /// Execute the recovered in-memory DAAT index with exact BM25.
    pub fn search(&self, query: &Query, limit: Option<usize>) -> Result<Vec<ScoredTid>> {
        self.index.search(query, limit)
    }

    /// Analyze query text with the segment's own analyzer and execute DAAT.
    pub fn search_text(&self, text: &str, limit: Option<usize>) -> Result<Vec<ScoredTid>> {
        let query = crate::daat::parse_query(self.index.analyzer(), text);
        self.search(&query, limit)
    }

    /// Insert one committed document into the in-process write view.
    pub fn insert_document(&mut self, tid: Tid, text: &str) -> Result<()> {
        self.index.insert_document(tid, text)?;
        self.dirty = true;
        self.wal_sealed = false;
        Ok(())
    }

    /// Delete one committed document from the in-process write view.
    pub fn delete_document(&mut self, tid: Tid) -> Result<bool> {
        let deleted = self.index.delete_document(tid)?;
        if deleted {
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

    /// Persist the full current write view as a new segment generation. Old
    /// files remain intact until the manifest atomically selects the new ID,
    /// so a crash before that rename replays onto the old generation instead
    /// of replaying onto an already-updated file and double-counting documents.
    fn persist_generation(&mut self, data_dir: &Path) -> Result<()> {
        let previous_lsn = self.watermark.durable_applied_lsn;
        let applied_lsn = self.watermark.consumed_lsn;
        let segment_id = self.next_segment_id(data_dir)?;
        let stats = self.index.stats();
        let mut active_segment_ids = Vec::new();
        if stats.doc_count > 0 {
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
                created_from_lsn: previous_lsn,
                applied_lsn,
                row_count: stats.row_count,
                posting_count: stats.posting_count,
                byte_count: (postings_bytes.len() + doc_length_bytes.len()) as u64,
            };
            write_atomic(
                &Self::segment_metadata_path(data_dir, self.index_oid, segment_id),
                &metadata.encode()?,
            )?;
            active_segment_ids.push(segment_id);
        }
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
        self.manifest = new_manifest;
        self.watermark.durable_applied_lsn = applied_lsn;
        Ok(())
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
            if path.extension().and_then(|extension| extension.to_str()) != Some("meta") {
                continue;
            }
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
        if self.wal_sealed {
            return Ok(None);
        }
        if self.dirty {
            self.persist_generation(data_dir)?;
        }
        let mut last_seal_end = None;
        if self.dirty || self.manifest.generation == 1 {
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

fn validate_segment_payload(
    meta: &SegmentMeta,
    postings: &BTreeMap<String, BTreeMap<Tid, Posting>>,
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
        }
    }
    Ok(())
}
