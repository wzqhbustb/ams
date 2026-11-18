//! Phase 3 inverted index access method: BM25, segments, and Tier 2.
//!
//! This crate owns only the format and access-method contracts. It does not
//! depend on pg-engine or DataFusion.

#![warn(missing_docs)]
#![warn(rust_2018_idioms)]

pub mod analyzer;
pub mod bm25;
pub mod daat;
pub mod error;
pub mod index;
pub mod posting;
pub mod recovery;
pub mod segment;
pub mod tier2;
pub mod wand;

pub use analyzer::{Analyzer, AnalyzerConfig, DefaultAnalyzer, Token};
pub use daat::{parse_query, InMemoryIndex, InMemoryStats, Query, ScoredTid};
pub use error::{InvertedError, Result};
pub use index::InvertedIndex;
pub use posting::{DocLength, Posting, SegmentMeta};
pub use segment::{SegmentManifest, SegmentStore};
pub use tier2::Watermark;

#[cfg(test)]
mod tests {
    use super::*;
    use pg_storage::types::Lsn;

    fn write_active_store(data_dir: &std::path::Path, manifest_lsn: Lsn, segment_lsn: Lsn) {
        write_active_store_with_state(
            data_dir,
            manifest_lsn,
            segment_lsn,
            pg_storage::segment::SegmentState::Sealed,
        );
    }

    fn segment_meta_fixture() -> SegmentMeta {
        SegmentMeta {
            format_version: posting::SEGMENT_FORMAT_VERSION,
            index_oid: pg_storage::types::Oid(9879),
            segment_id: pg_storage::segment::SegmentId(7),
            analyzer: AnalyzerConfig::builtin(),
            state: pg_storage::segment::SegmentState::Sealed,
            doc_count: 0,
            total_tokens: 0,
            term_df: Default::default(),
            created_from_lsn: Lsn::INVALID,
            applied_lsn: Lsn(128),
            row_count: 0,
            posting_count: 0,
            byte_count: 0,
        }
    }

    fn segment_meta_golden_shape() -> SegmentMeta {
        SegmentMeta {
            doc_count: 2,
            total_tokens: 18,
            term_df: std::collections::BTreeMap::from([
                ("contract".to_string(), 2),
                ("rust".to_string(), 1),
            ]),
            row_count: 2,
            posting_count: 3,
            ..segment_meta_fixture()
        }
    }

    #[test]
    fn golden_shaped_segment_opens() {
        let tmp = tempfile::TempDir::new().unwrap();
        write_active_store_with_meta(tmp.path(), Lsn(128), segment_meta_golden_shape());
        let store = SegmentStore::open(
            tmp.path(),
            pg_storage::types::Oid(9879),
            &AnalyzerConfig::builtin(),
        )
        .unwrap();
        assert_eq!(store.manifest().applied_lsn, Lsn(128));
    }

    fn write_active_store_with_state(
        data_dir: &std::path::Path,
        manifest_lsn: Lsn,
        segment_lsn: Lsn,
        state: pg_storage::segment::SegmentState,
    ) {
        let metadata = SegmentMeta {
            state,
            applied_lsn: segment_lsn,
            ..segment_meta_fixture()
        };
        write_active_store_with_meta(data_dir, manifest_lsn, metadata);
    }

    fn write_active_store_with_meta(
        data_dir: &std::path::Path,
        manifest_lsn: Lsn,
        metadata: SegmentMeta,
    ) {
        let manifest = SegmentManifest {
            generation: 1,
            active_segment_ids: vec![metadata.segment_id],
            applied_lsn: manifest_lsn,
        };
        let manifest_path = SegmentStore::manifest_path(data_dir, metadata.index_oid);
        std::fs::create_dir_all(manifest_path.parent().unwrap()).unwrap();
        std::fs::write(manifest_path, manifest.encode().unwrap()).unwrap();
        let metadata_path =
            SegmentStore::segment_metadata_path(data_dir, metadata.index_oid, metadata.segment_id);
        std::fs::create_dir_all(metadata_path.parent().unwrap()).unwrap();
        std::fs::write(metadata_path, metadata.encode().unwrap()).unwrap();
    }

    #[test]
    fn token_and_manifest_shapes_are_stable() {
        let token = analyzer::Token {
            text: "合同".to_string(),
            position: 7,
        };
        assert_eq!(token.text, "合同");
        assert_eq!(token.position, 7);

        let manifest = SegmentManifest {
            generation: 9,
            active_segment_ids: Vec::new(),
            applied_lsn: pg_storage::types::Lsn(128),
        };
        let bytes = manifest.encode().unwrap();
        let decoded = SegmentManifest::decode(&bytes).unwrap();
        assert_eq!(decoded, manifest);
    }

    #[test]
    fn manifest_rejects_crc_and_trailing_bytes() {
        let manifest = SegmentManifest {
            generation: 1,
            active_segment_ids: Vec::new(),
            applied_lsn: pg_storage::types::Lsn::INVALID,
        };
        let mut bytes = manifest.encode().unwrap();
        bytes[0] ^= 1;
        assert!(matches!(
            SegmentManifest::decode(&bytes),
            Err(InvertedError::Corrupted(_))
        ));
        let mut bytes = manifest.encode().unwrap();
        bytes.push(0);
        assert!(matches!(
            SegmentManifest::decode(&bytes),
            Err(InvertedError::Corrupted(_))
        ));
    }

    #[test]
    fn bm25_matches_tantivy_formula() {
        assert!((bm25::idf(1, 2).unwrap() - std::f64::consts::LN_2).abs() < 1e-12);
        assert_eq!(bm25::idf(0, 2).unwrap(), 0.0);
        assert_eq!(bm25::score(0, 10, 20.0, 1, 2).unwrap(), 0.0);
        let score = bm25::score(2, 10, 20.0, 1, 2).unwrap();
        assert!(score.is_finite() && score > 0.0);
    }

    #[test]
    fn posting_doc_length_segment_and_manifest_byte_golden() {
        fn hex(bytes: &[u8]) -> String {
            bytes.iter().map(|byte| format!("{byte:02x}")).collect()
        }

        let tid = pg_storage::types::Tid {
            page_id: pg_storage::types::PageId(42),
            slot_id: 7,
        };
        let posting = posting::Posting {
            tid,
            term_freq: 2,
            positions: vec![1, 4],
        };
        let posting_bytes = posting.encode().unwrap();
        assert_eq!(
            hex(&posting_bytes),
            "504749560100060000002a07020201045eb22e12"
        );
        assert_eq!(posting::Posting::decode(&posting_bytes).unwrap(), posting);

        let doc_length = posting::DocLength {
            tid,
            token_count: 9,
        };
        let doc_length_bytes = doc_length.encode().unwrap();
        assert_eq!(hex(&doc_length_bytes), "504749560100030000002a0709473406fc");
        assert_eq!(
            posting::DocLength::decode(&doc_length_bytes).unwrap(),
            doc_length
        );

        let meta = posting::SegmentMeta {
            format_version: 1,
            index_oid: pg_storage::types::Oid(9879),
            segment_id: pg_storage::segment::SegmentId(7),
            analyzer: AnalyzerConfig::builtin(),
            state: pg_storage::segment::SegmentState::Sealed,
            doc_count: 2,
            total_tokens: 18,
            term_df: std::collections::BTreeMap::from([
                ("contract".to_string(), 2),
                ("rust".to_string(), 1),
            ]),
            created_from_lsn: pg_storage::types::Lsn(120),
            applied_lsn: pg_storage::types::Lsn(128),
            row_count: 2,
            posting_count: 3,
            byte_count: 256,
        };
        let meta_bytes = meta.encode().unwrap();
        assert_eq!(
            hex(&meta_bytes),
            "5047495601004600000001fb972607056d6978656484d0b3945ec7c3cb3aea7b017027f704cf1af4be25243aa52586cebf9f5557e30202120208636f6e74726163740204727573740178800203fb000171953cd0"
        );
        assert_eq!(posting::SegmentMeta::decode(&meta_bytes).unwrap(), meta);
        let mut bad_version = meta.clone();
        bad_version.format_version = 2;
        assert!(matches!(
            posting::SegmentMeta::decode(&bad_version.encode().unwrap()),
            Err(InvertedError::Corrupted(_))
        ));
        let mut bad_analyzer = meta.clone();
        bad_analyzer.analyzer.name = "unknown".to_string();
        assert!(matches!(
            posting::SegmentMeta::decode(&bad_analyzer.encode().unwrap()),
            Err(InvertedError::Corrupted(_))
        ));

        let manifest = SegmentManifest {
            generation: 9,
            active_segment_ids: vec![
                pg_storage::segment::SegmentId(3),
                pg_storage::segment::SegmentId(5),
            ],
            applied_lsn: pg_storage::types::Lsn(128),
        };
        let manifest_bytes = manifest.encode().unwrap();
        assert_eq!(
            hex(&manifest_bytes),
            "504749560100050000000902030580db94b2f3"
        );
        assert_eq!(SegmentManifest::decode(&manifest_bytes).unwrap(), manifest);
    }

    #[test]
    fn initial_empty_manifest_reopens_but_nonzero_watermark_is_rejected() {
        let tmp = tempfile::TempDir::new().unwrap();
        let index_oid = pg_storage::types::Oid(9879);
        let manifest = SegmentStore::new(index_oid).manifest().clone();
        let path = SegmentStore::manifest_path(tmp.path(), index_oid);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, manifest.encode().unwrap()).unwrap();
        let store = SegmentStore::open(tmp.path(), index_oid, &AnalyzerConfig::builtin()).unwrap();
        assert!(store.manifest().active_segment_ids.is_empty());
        assert_eq!(store.watermark().consumed_lsn, Lsn::INVALID);
        assert_eq!(store.watermark().durable_applied_lsn, Lsn::INVALID);

        let manifest = SegmentManifest {
            generation: 1,
            active_segment_ids: Vec::new(),
            applied_lsn: Lsn(128),
        };
        std::fs::write(&path, manifest.encode().unwrap()).unwrap();
        assert!(SegmentStore::open(tmp.path(), index_oid, &AnalyzerConfig::builtin()).is_err());
    }

    #[test]
    fn manifest_watermark_must_not_exceed_active_coverage() {
        let tmp = tempfile::TempDir::new().unwrap();
        write_active_store(tmp.path(), Lsn(128), Lsn::INVALID);
        let error = SegmentStore::open(
            tmp.path(),
            pg_storage::types::Oid(9879),
            &AnalyzerConfig::builtin(),
        )
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("exceeds active segment coverage"));
    }

    #[test]
    fn freshness_returns_consumed_not_durable_watermark() {
        let mut store = SegmentStore::new(pg_storage::types::Oid(9879));
        store.set_watermark_for_test(Watermark {
            consumed_lsn: Lsn(128),
            durable_applied_lsn: Lsn(64),
        });
        let index = InvertedIndex::from_store(store);
        assert_eq!(
            pg_am_heap::access_method::AccessMethod::freshness(&index),
            Some(Lsn(128))
        );
    }

    #[test]
    fn posting_decode_rejects_bad_frequency_and_position_order() {
        let posting = Posting {
            tid: pg_storage::types::Tid {
                page_id: pg_storage::types::PageId(42),
                slot_id: 7,
            },
            term_freq: 2,
            positions: vec![1, 4],
        };
        let mut missing_positions = posting.clone();
        missing_positions.term_freq = 1;
        missing_positions.positions.clear();
        assert!(matches!(
            Posting::decode(&missing_positions.encode().unwrap()),
            Err(InvertedError::Corrupted(_))
        ));
        let mut unordered = posting;
        unordered.positions = vec![2, 1];
        assert!(matches!(
            Posting::decode(&unordered.encode().unwrap()),
            Err(InvertedError::Corrupted(_))
        ));
    }

    #[test]
    fn active_manifest_rejects_merging_and_retired_segments() {
        for state in [
            pg_storage::segment::SegmentState::Active,
            pg_storage::segment::SegmentState::Frozen,
            pg_storage::segment::SegmentState::Sealed,
        ] {
            let tmp = tempfile::TempDir::new().unwrap();
            write_active_store_with_state(tmp.path(), Lsn(128), Lsn(128), state);
            assert!(SegmentStore::open(
                tmp.path(),
                pg_storage::types::Oid(9879),
                &AnalyzerConfig::builtin()
            )
            .is_ok());
        }
        for state in [
            pg_storage::segment::SegmentState::Merging,
            pg_storage::segment::SegmentState::Retired,
        ] {
            let tmp = tempfile::TempDir::new().unwrap();
            write_active_store_with_state(tmp.path(), Lsn(128), Lsn(128), state);
            let error = SegmentStore::open(
                tmp.path(),
                pg_storage::types::Oid(9879),
                &AnalyzerConfig::builtin(),
            )
            .unwrap_err();
            assert!(error.to_string().contains("not readable"));
        }
    }

    #[test]
    fn segment_stats_are_validated_at_open() {
        let reject = |metadata: SegmentMeta| {
            let tmp = tempfile::TempDir::new().unwrap();
            write_active_store_with_meta(tmp.path(), Lsn(128), metadata);
            assert!(SegmentStore::open(
                tmp.path(),
                pg_storage::types::Oid(9879),
                &AnalyzerConfig::builtin(),
            )
            .is_err());
        };
        let mut invalid = segment_meta_fixture();
        invalid.doc_count = 1;
        invalid.row_count = 0;
        reject(invalid);

        let mut invalid = segment_meta_fixture();
        invalid.total_tokens = 1;
        invalid.posting_count = 1;
        invalid.term_df.insert("contract".to_string(), 1);
        reject(invalid);

        let mut invalid = segment_meta_fixture();
        invalid.doc_count = 1;
        invalid.term_df.insert("contract".to_string(), 1);
        reject(invalid);

        let mut invalid = segment_meta_fixture();
        invalid.doc_count = 1;
        invalid.posting_count = 1;
        reject(invalid);

        let mut invalid = segment_meta_fixture();
        invalid.doc_count = 1;
        invalid.total_tokens = 1;
        invalid.posting_count = 1;
        invalid.term_df.insert("contract".to_string(), 2);
        reject(invalid);

        let mut invalid = segment_meta_fixture();
        invalid.doc_count = 2;
        invalid.total_tokens = 18;
        invalid.posting_count = 1;
        invalid.term_df.insert("contract".to_string(), 2);
        invalid.term_df.insert("rust".to_string(), 1);
        reject(invalid);

        let mut valid = segment_meta_fixture();
        valid.doc_count = 2;
        valid.row_count = 2;
        let tmp = tempfile::TempDir::new().unwrap();
        write_active_store_with_meta(tmp.path(), Lsn(128), valid);
        assert!(SegmentStore::open(
            tmp.path(),
            pg_storage::types::Oid(9879),
            &AnalyzerConfig::builtin()
        )
        .is_ok());
    }

    #[test]
    fn analyzer_identity_is_validated() {
        AnalyzerConfig::builtin().validate().unwrap();
        let mut invalid = AnalyzerConfig::builtin();
        invalid.name.clear();
        assert!(invalid.validate().is_err());
        let mut invalid = AnalyzerConfig::builtin();
        invalid.name = "unknown".to_string();
        assert!(invalid.validate().is_err());
        let mut invalid = AnalyzerConfig::builtin();
        invalid.config_hash = [0; 32];
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn inverted_methods_are_explicitly_unsupported() {
        use pg_am_heap::access_method::{
            AccessMethod, DeleteContext, InsertContext, RelationDesc, ScanContext,
        };
        use pg_storage::clog::NoOpClogAccessor;
        use pg_txn::Snapshot;

        let index = InvertedIndex::new(pg_storage::types::Oid(9879));
        assert!(matches!(index.search(), Err(InvertedError::Unsupported(_))));
        let rel = RelationDesc {
            rel_oid: pg_storage::types::Oid(1),
            first_page: pg_storage::types::PageId(1),
            columns: &[],
        };
        let snapshot = Snapshot::everything();
        let insert_error = AccessMethod::insert(
            &index,
            InsertContext {
                rel,
                snapshot: &snapshot,
                tuple: &[],
                out_tid: None,
            },
        )
        .unwrap_err();
        assert!(matches!(
            insert_error,
            pg_am_heap::HeapError::InvalidArgument(message) if message.contains("Tier 2")
        ));
        let clog = NoOpClogAccessor;
        let scan_error = AccessMethod::scan(
            &index,
            ScanContext {
                rel,
                snapshot: &snapshot,
                clog: &clog,
            },
        )
        .unwrap_err();
        assert!(matches!(
            scan_error,
            pg_am_heap::HeapError::InvalidArgument(message) if message.contains("not a heap scan")
        ));
        let delete_error = AccessMethod::delete(
            &index,
            DeleteContext {
                rel,
                snapshot: &snapshot,
                tid: pg_storage::types::Tid {
                    page_id: pg_storage::types::PageId(1),
                    slot_id: 0,
                },
                clog: &clog,
            },
        )
        .unwrap_err();
        assert!(matches!(
            delete_error,
            pg_am_heap::HeapError::InvalidArgument(message) if message.contains("Tier 2")
        ));
    }
}
