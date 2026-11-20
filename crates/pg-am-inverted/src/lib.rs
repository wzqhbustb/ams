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
pub(crate) mod segment_io;
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
    use std::collections::BTreeMap;

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
        let (postings, doc_lengths) = if metadata.total_tokens == 0 {
            let doc_lengths = (0..metadata.doc_count)
                .map(|index| DocLength {
                    tid: pg_storage::types::Tid {
                        page_id: pg_storage::types::PageId(42),
                        slot_id: index as u16 + 1,
                    },
                    token_count: 0,
                })
                .collect();
            (BTreeMap::new(), doc_lengths)
        } else {
            golden_payload()
        };
        let postings_path =
            SegmentStore::postings_path(data_dir, metadata.index_oid, metadata.segment_id);
        std::fs::create_dir_all(postings_path.parent().unwrap()).unwrap();
        std::fs::write(postings_path, crate::recovery::encode(&postings).unwrap()).unwrap();
        let doc_lengths_path =
            SegmentStore::doc_lengths_path(data_dir, metadata.index_oid, metadata.segment_id);
        std::fs::create_dir_all(doc_lengths_path.parent().unwrap()).unwrap();
        std::fs::write(
            doc_lengths_path,
            crate::recovery::encode(&doc_lengths).unwrap(),
        )
        .unwrap();
    }

    fn golden_payload() -> (
        std::collections::BTreeMap<
            String,
            std::collections::BTreeMap<pg_storage::types::Tid, Posting>,
        >,
        Vec<DocLength>,
    ) {
        let first = pg_storage::types::Tid {
            page_id: pg_storage::types::PageId(42),
            slot_id: 1,
        };
        let second = pg_storage::types::Tid {
            page_id: pg_storage::types::PageId(42),
            slot_id: 2,
        };
        let postings = std::collections::BTreeMap::from([
            (
                "contract".to_string(),
                std::collections::BTreeMap::from([
                    (
                        first,
                        Posting {
                            tid: first,
                            term_freq: 9,
                            positions: (0..9).collect(),
                        },
                    ),
                    (
                        second,
                        Posting {
                            tid: second,
                            term_freq: 8,
                            positions: (0..8).collect(),
                        },
                    ),
                ]),
            ),
            (
                "rust".to_string(),
                std::collections::BTreeMap::from([(
                    first,
                    Posting {
                        tid: first,
                        term_freq: 1,
                        positions: vec![0],
                    },
                )]),
            ),
        ]);
        let doc_lengths = vec![
            DocLength {
                tid: first,
                token_count: 10,
            },
            DocLength {
                tid: second,
                token_count: 8,
            },
        ];
        (postings, doc_lengths)
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
    fn empty_manifest_reopens_at_its_backfill_watermark() {
        let tmp = tempfile::TempDir::new().unwrap();
        let index_oid = pg_storage::types::Oid(9879);
        let path = SegmentStore::manifest_path(tmp.path(), index_oid);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(
            SegmentStore::analyzer_path(tmp.path(), index_oid),
            crate::recovery::encode(&AnalyzerConfig::builtin()).unwrap(),
        )
        .unwrap();
        for applied_lsn in [Lsn::INVALID, Lsn(128)] {
            let manifest = SegmentManifest {
                generation: 1,
                active_segment_ids: Vec::new(),
                applied_lsn,
            };
            std::fs::write(&path, manifest.encode().unwrap()).unwrap();
            let store =
                SegmentStore::open(tmp.path(), index_oid, &AnalyzerConfig::builtin()).unwrap();
            assert!(store.manifest().active_segment_ids.is_empty());
            assert_eq!(store.watermark().consumed_lsn, applied_lsn);
            assert_eq!(store.watermark().durable_applied_lsn, applied_lsn);
        }
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
    fn segment_create_seal_reopen_and_delete_are_consistent() {
        let tmp = tempfile::TempDir::new().unwrap();
        let index_oid = pg_storage::types::Oid(9879);
        let tid = pg_storage::types::Tid {
            page_id: pg_storage::types::PageId(1),
            slot_id: 0,
        };
        let mut store = SegmentStore::create(
            tmp.path(),
            index_oid,
            AnalyzerConfig::builtin(),
            vec![(tid, "rust contract".to_string())],
            Lsn::FIRST,
        )
        .unwrap();
        let config = pg_storage::config::StorageConfig::new(tmp.path());
        let wal = pg_storage::wal::WalWriter::open(tmp.path(), &config).unwrap();
        store
            .seal(tmp.path(), &wal, pg_storage::types::TxnId(1))
            .unwrap();
        store
            .seal(tmp.path(), &wal, pg_storage::types::TxnId(1))
            .unwrap();
        let empty_oid = pg_storage::types::Oid(9880);
        let mut empty = SegmentStore::create(
            tmp.path(),
            empty_oid,
            AnalyzerConfig::builtin(),
            Vec::new(),
            Lsn(128),
        )
        .unwrap();
        empty
            .seal(tmp.path(), &wal, pg_storage::types::TxnId(2))
            .unwrap();
        wal.flush().unwrap();
        drop(wal);

        let mut reader = pg_storage::wal::reader::WalReader::open(
            tmp.path().join("wal"),
            config.wal_segment_size,
        )
        .unwrap();
        let mut seal_count = 0;
        while let Some(record) = reader.next_record().unwrap() {
            if record.record_type == pg_storage::wal::record::WalRecordType::SegmentSeal {
                seal_count += 1;
            }
        }
        assert_eq!(seal_count, 1);
        drop(reader);
        let storage = pg_storage::engine::StorageEngine::open_with_redo_handlers(
            tmp.path(),
            &config,
            vec![Box::new(segment::SegmentSealRedoHandler)],
            Vec::new(),
        )
        .unwrap();
        storage.shutdown();

        let mut recovered =
            SegmentStore::open(tmp.path(), index_oid, &AnalyzerConfig::builtin()).unwrap();
        assert_eq!(recovered.search_text("contract", None).unwrap().len(), 1);
        assert!(recovered.delete_document(tid).unwrap());
        assert!(!recovered.delete_document(tid).unwrap());
        assert!(recovered.search_text("contract", None).unwrap().is_empty());
        recovered.insert_document(tid, "replacement").unwrap();
        assert_eq!(recovered.search_text("replacement", None).unwrap().len(), 1);
        let empty = SegmentStore::open(tmp.path(), empty_oid, &AnalyzerConfig::builtin()).unwrap();
        assert!(empty.manifest().active_segment_ids.is_empty());
        assert_eq!(empty.watermark().consumed_lsn, Lsn(128));
    }

    #[test]
    fn seal_generations_are_crash_safe_and_never_reuse_ids() {
        let tmp = tempfile::TempDir::new().unwrap();
        let index_oid = pg_storage::types::Oid(9879);
        let tid = |slot| pg_storage::types::Tid {
            page_id: pg_storage::types::PageId(1),
            slot_id: slot,
        };
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
        let old_manifest =
            std::fs::read(SegmentStore::manifest_path(tmp.path(), index_oid)).unwrap();

        store.insert_document(tid(2), "contract").unwrap();
        store.advance_consumed(Lsn(128));
        store
            .seal(tmp.path(), &wal, pg_storage::types::TxnId(1))
            .unwrap();
        assert_eq!(store.manifest().generation, 2);
        assert_eq!(
            store.manifest().active_segment_ids,
            vec![
                pg_storage::segment::SegmentId(1),
                pg_storage::segment::SegmentId(2)
            ]
        );
        assert_eq!(store.watermark().durable_applied_lsn, Lsn(128));
        store
            .seal(tmp.path(), &wal, pg_storage::types::TxnId(1))
            .unwrap();
        assert_eq!(store.manifest().generation, 2);
        store.advance_consumed(Lsn(256));
        store
            .seal(tmp.path(), &wal, pg_storage::types::TxnId(1))
            .unwrap();
        assert_eq!(store.manifest().generation, 2);
        assert_eq!(store.watermark().durable_applied_lsn, Lsn(128));
        let recovered =
            SegmentStore::open(tmp.path(), index_oid, &AnalyzerConfig::builtin()).unwrap();
        assert_eq!(recovered.search_text("rust", None).unwrap().len(), 1);
        assert_eq!(recovered.search_text("contract", None).unwrap().len(), 1);

        // Simulate a crash before the generation-2 manifest rename. The old
        // segment is untouched, so replaying the same commit cannot double it.
        std::fs::write(
            SegmentStore::manifest_path(tmp.path(), index_oid),
            old_manifest,
        )
        .unwrap();
        let mut crashed =
            SegmentStore::open(tmp.path(), index_oid, &AnalyzerConfig::builtin()).unwrap();
        assert!(crashed.search_text("contract", None).unwrap().is_empty());
        crashed.insert_document(tid(2), "contract").unwrap();
        crashed.advance_consumed(Lsn(128));
        crashed
            .seal(tmp.path(), &wal, pg_storage::types::TxnId(1))
            .unwrap();
        assert_eq!(
            crashed.manifest().active_segment_ids,
            vec![
                pg_storage::segment::SegmentId(1),
                pg_storage::segment::SegmentId(3)
            ]
        );
        assert_eq!(crashed.search_text("rust", None).unwrap().len(), 1);
        assert_eq!(crashed.search_text("contract", None).unwrap().len(), 1);

        assert!(crashed.delete_document(tid(1)).unwrap());
        assert!(crashed.delete_document(tid(2)).unwrap());
        crashed.advance_consumed(Lsn(256));
        crashed
            .seal(tmp.path(), &wal, pg_storage::types::TxnId(1))
            .unwrap();
        assert_eq!(
            crashed.manifest().active_segment_ids,
            vec![
                pg_storage::segment::SegmentId(1),
                pg_storage::segment::SegmentId(3),
                pg_storage::segment::SegmentId(4),
            ]
        );
        crashed.insert_document(tid(3), "fresh").unwrap();
        crashed.advance_consumed(Lsn(512));
        crashed
            .seal(tmp.path(), &wal, pg_storage::types::TxnId(1))
            .unwrap();
        assert_eq!(
            crashed.manifest().active_segment_ids,
            vec![
                pg_storage::segment::SegmentId(1),
                pg_storage::segment::SegmentId(3),
                pg_storage::segment::SegmentId(4),
                pg_storage::segment::SegmentId(5),
            ]
        );
        let reopened =
            SegmentStore::open(tmp.path(), index_oid, &AnalyzerConfig::builtin()).unwrap();
        assert_eq!(reopened.search_text("fresh", None).unwrap().len(), 1);
        assert_eq!(reopened.search_text("rust", None).unwrap().len(), 0);
        wal.flush().unwrap();
    }

    #[test]
    fn merge_compacts_segments_and_preserves_update_delete_semantics() {
        let tmp = tempfile::TempDir::new().unwrap();
        let index_oid = pg_storage::types::Oid(9879);
        let tid = |slot| pg_storage::types::Tid {
            page_id: pg_storage::types::PageId(1),
            slot_id: slot,
        };
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
        assert_eq!(store.manifest().active_segment_ids.len(), 3);
        let before_merge = store
            .search_text("rust OR contract OR other", None)
            .unwrap();
        assert_eq!(
            before_merge.iter().map(|hit| hit.tid).collect::<Vec<_>>(),
            vec![tid(1), tid(2), tid(3)]
        );

        store
            .merge(tmp.path(), &wal, pg_storage::types::TxnId(1))
            .unwrap();
        assert_eq!(
            store.manifest().active_segment_ids,
            vec![pg_storage::segment::SegmentId(4)]
        );
        for retired_id in 1..=3 {
            assert!(!SegmentStore::segment_metadata_path(
                tmp.path(),
                index_oid,
                pg_storage::segment::SegmentId(retired_id),
            )
            .exists());
        }
        let mut expected = InMemoryIndex::new(DefaultAnalyzer::mixed());
        for (slot, text) in [(1, "rust"), (2, "contract"), (3, "other")] {
            expected.insert_document(tid(slot), text).unwrap();
        }
        assert_segment_matches(&tmp, index_oid, 4, &expected);
        let reopened =
            SegmentStore::open(tmp.path(), index_oid, &AnalyzerConfig::builtin()).unwrap();
        assert_eq!(
            reopened
                .search_text("rust OR contract OR other", None)
                .unwrap(),
            before_merge
        );
        assert_eq!(reopened.search_text("other", None).unwrap().len(), 1);

        store.insert_document(tid(1), "updated").unwrap();
        store.advance_consumed(Lsn(384));
        store
            .seal(tmp.path(), &wal, pg_storage::types::TxnId(1))
            .unwrap();
        store
            .merge(tmp.path(), &wal, pg_storage::types::TxnId(1))
            .unwrap();
        assert!(store.search_text("rust", None).unwrap().is_empty());
        let mut updated_expected = InMemoryIndex::new(DefaultAnalyzer::mixed());
        for (slot, text) in [(1, "updated"), (2, "contract"), (3, "other")] {
            updated_expected.insert_document(tid(slot), text).unwrap();
        }
        assert_segment_matches(&tmp, index_oid, 6, &updated_expected);

        assert!(store.delete_document(tid(2)).unwrap());
        store.advance_consumed(Lsn(512));
        store
            .seal(tmp.path(), &wal, pg_storage::types::TxnId(1))
            .unwrap();
        store
            .merge(tmp.path(), &wal, pg_storage::types::TxnId(1))
            .unwrap();
        let mut final_expected = InMemoryIndex::new(DefaultAnalyzer::mixed());
        for (slot, text) in [(1, "updated"), (3, "other")] {
            final_expected.insert_document(tid(slot), text).unwrap();
        }
        assert_segment_matches(&tmp, index_oid, 8, &final_expected);
        let reopened =
            SegmentStore::open(tmp.path(), index_oid, &AnalyzerConfig::builtin()).unwrap();
        assert!(reopened.search_text("contract", None).unwrap().is_empty());
        assert_eq!(reopened.search_text("updated", None).unwrap().len(), 1);
        assert_eq!(reopened.search_text("other", None).unwrap().len(), 1);
        assert!(store
            .merge(tmp.path(), &wal, pg_storage::types::TxnId(1))
            .unwrap()
            .is_none());
    }

    fn assert_segment_matches(
        tmp: &tempfile::TempDir,
        index_oid: pg_storage::types::Oid,
        segment_id: u64,
        expected: &InMemoryIndex<DefaultAnalyzer>,
    ) {
        let segment_id = pg_storage::segment::SegmentId(segment_id);
        let postings: BTreeMap<_, _> = crate::recovery::decode(
            &std::fs::read(SegmentStore::postings_path(
                tmp.path(),
                index_oid,
                segment_id,
            ))
            .unwrap(),
        )
        .unwrap();
        assert_eq!(postings, *expected.postings());
        let expected_lengths = expected
            .doc_lengths()
            .iter()
            .map(|(tid, token_count)| DocLength {
                tid: *tid,
                token_count: *token_count,
            })
            .collect::<Vec<_>>();
        let lengths: Vec<DocLength> = crate::recovery::decode(
            &std::fs::read(SegmentStore::doc_lengths_path(
                tmp.path(),
                index_oid,
                segment_id,
            ))
            .unwrap(),
        )
        .unwrap();
        assert_eq!(lengths, expected_lengths);
        let tombstones: Vec<pg_storage::types::Tid> = crate::recovery::decode(
            &std::fs::read(SegmentStore::tombstones_path(
                tmp.path(),
                index_oid,
                segment_id,
            ))
            .unwrap(),
        )
        .unwrap();
        assert!(tombstones.is_empty());
        let metadata = SegmentMeta::decode(
            &std::fs::read(SegmentStore::segment_metadata_path(
                tmp.path(),
                index_oid,
                segment_id,
            ))
            .unwrap(),
        )
        .unwrap();
        let stats = expected.stats();
        assert_eq!(metadata.doc_count, stats.doc_count);
        assert_eq!(metadata.row_count, stats.row_count);
        assert_eq!(metadata.total_tokens, stats.total_tokens);
        assert_eq!(metadata.posting_count, stats.posting_count);
    }

    #[test]
    fn zero_token_upserts_supersede_old_postings_after_reopen_and_merge() {
        let tmp = tempfile::TempDir::new().unwrap();
        let index_oid = pg_storage::types::Oid(9879);
        let tid = pg_storage::types::Tid {
            page_id: pg_storage::types::PageId(1),
            slot_id: 1,
        };
        let other = pg_storage::types::Tid {
            page_id: pg_storage::types::PageId(1),
            slot_id: 2,
        };
        let config = pg_storage::config::StorageConfig::new(tmp.path());
        let wal = pg_storage::wal::WalWriter::open(tmp.path(), &config).unwrap();
        let mut store = SegmentStore::create(
            tmp.path(),
            index_oid,
            AnalyzerConfig::builtin(),
            vec![(tid, "rust".to_string())],
            Lsn::FIRST,
        )
        .unwrap();
        store
            .seal(tmp.path(), &wal, pg_storage::types::TxnId(1))
            .unwrap();
        store.insert_document(tid, "the").unwrap();
        store.advance_consumed(Lsn(128));
        store
            .seal(tmp.path(), &wal, pg_storage::types::TxnId(1))
            .unwrap();
        assert!(store.search_text("rust", None).unwrap().is_empty());

        let mut store =
            SegmentStore::open(tmp.path(), index_oid, &AnalyzerConfig::builtin()).unwrap();
        assert!(store.search_text("rust", None).unwrap().is_empty());
        store.insert_document(tid, "").unwrap();
        store.advance_consumed(Lsn(256));
        store
            .seal(tmp.path(), &wal, pg_storage::types::TxnId(1))
            .unwrap();
        let mut store =
            SegmentStore::open(tmp.path(), index_oid, &AnalyzerConfig::builtin()).unwrap();
        assert!(store.search_text("rust", None).unwrap().is_empty());

        store.insert_document(other, "contract").unwrap();
        store.advance_consumed(Lsn(384));
        store
            .seal(tmp.path(), &wal, pg_storage::types::TxnId(1))
            .unwrap();
        store
            .merge(tmp.path(), &wal, pg_storage::types::TxnId(1))
            .unwrap();
        let merged = SegmentStore::open(tmp.path(), index_oid, &AnalyzerConfig::builtin()).unwrap();
        assert!(merged.search_text("rust", None).unwrap().is_empty());
        assert_eq!(merged.search_text("contract", None).unwrap().len(), 1);
    }

    #[test]
    fn recovered_multi_segment_token_totals_are_checked_for_overflow() {
        let tmp = tempfile::TempDir::new().unwrap();
        let index_oid = pg_storage::types::Oid(9879);
        for (segment_id, slot_id) in [(1, 1), (2, 2)] {
            let segment_id = pg_storage::segment::SegmentId(segment_id);
            let tid = pg_storage::types::Tid {
                page_id: pg_storage::types::PageId(1),
                slot_id,
            };
            let metadata = SegmentMeta {
                doc_count: 1,
                total_tokens: u64::MAX,
                row_count: 1,
                segment_id,
                ..segment_meta_fixture()
            };
            let postings = BTreeMap::<String, BTreeMap<pg_storage::types::Tid, Posting>>::new();
            let lengths = vec![DocLength {
                tid,
                token_count: u64::MAX,
            }];
            for (path, bytes) in [
                (
                    SegmentStore::segment_metadata_path(tmp.path(), index_oid, segment_id),
                    metadata.encode().unwrap(),
                ),
                (
                    SegmentStore::postings_path(tmp.path(), index_oid, segment_id),
                    crate::recovery::encode(&postings).unwrap(),
                ),
                (
                    SegmentStore::doc_lengths_path(tmp.path(), index_oid, segment_id),
                    crate::recovery::encode(&lengths).unwrap(),
                ),
            ] {
                std::fs::create_dir_all(path.parent().unwrap()).unwrap();
                std::fs::write(path, bytes).unwrap();
            }
        }
        let manifest = SegmentManifest {
            generation: 2,
            active_segment_ids: vec![
                pg_storage::segment::SegmentId(1),
                pg_storage::segment::SegmentId(2),
            ],
            applied_lsn: Lsn(128),
        };
        let path = SegmentStore::manifest_path(tmp.path(), index_oid);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, manifest.encode().unwrap()).unwrap();
        let result = std::panic::catch_unwind(|| {
            SegmentStore::open(tmp.path(), index_oid, &AnalyzerConfig::builtin())
        });
        assert!(result.is_ok(), "overflow must not panic");
        assert!(matches!(result.unwrap(), Err(InvertedError::Corrupted(_))));
    }

    #[test]
    fn active_segments_merge_terms_and_allow_newer_document_versions() {
        fn write_segment(
            data_dir: &std::path::Path,
            index_oid: pg_storage::types::Oid,
            segment_id: pg_storage::segment::SegmentId,
            tid: pg_storage::types::Tid,
        ) {
            let postings = BTreeMap::from([(
                "contract".to_string(),
                BTreeMap::from([(
                    tid,
                    Posting {
                        tid,
                        term_freq: 1,
                        positions: vec![0],
                    },
                )]),
            )]);
            let doc_lengths = vec![DocLength {
                tid,
                token_count: 1,
            }];
            let metadata = SegmentMeta {
                format_version: posting::SEGMENT_FORMAT_VERSION,
                index_oid,
                segment_id,
                analyzer: AnalyzerConfig::builtin(),
                state: pg_storage::segment::SegmentState::Sealed,
                doc_count: 1,
                total_tokens: 1,
                term_df: BTreeMap::from([("contract".to_string(), 1)]),
                created_from_lsn: Lsn::INVALID,
                applied_lsn: Lsn(128),
                row_count: 1,
                posting_count: 1,
                byte_count: 0,
            };
            for (path, bytes) in [
                (
                    SegmentStore::segment_metadata_path(data_dir, index_oid, segment_id),
                    metadata.encode().unwrap(),
                ),
                (
                    SegmentStore::postings_path(data_dir, index_oid, segment_id),
                    crate::recovery::encode(&postings).unwrap(),
                ),
                (
                    SegmentStore::doc_lengths_path(data_dir, index_oid, segment_id),
                    crate::recovery::encode(&doc_lengths).unwrap(),
                ),
            ] {
                std::fs::create_dir_all(path.parent().unwrap()).unwrap();
                std::fs::write(path, bytes).unwrap();
            }
        }

        let tmp = tempfile::TempDir::new().unwrap();
        let index_oid = pg_storage::types::Oid(9879);
        let tid = |slot| pg_storage::types::Tid {
            page_id: pg_storage::types::PageId(1),
            slot_id: slot,
        };
        write_segment(
            tmp.path(),
            index_oid,
            pg_storage::segment::SegmentId(1),
            tid(1),
        );
        write_segment(
            tmp.path(),
            index_oid,
            pg_storage::segment::SegmentId(2),
            tid(2),
        );
        let manifest = SegmentManifest {
            generation: 2,
            active_segment_ids: vec![
                pg_storage::segment::SegmentId(1),
                pg_storage::segment::SegmentId(2),
            ],
            applied_lsn: Lsn(128),
        };
        let manifest_path = SegmentStore::manifest_path(tmp.path(), index_oid);
        std::fs::create_dir_all(manifest_path.parent().unwrap()).unwrap();
        std::fs::write(&manifest_path, manifest.encode().unwrap()).unwrap();
        let store = SegmentStore::open(tmp.path(), index_oid, &AnalyzerConfig::builtin()).unwrap();
        assert_eq!(store.search_text("contract", None).unwrap().len(), 2);

        write_segment(
            tmp.path(),
            index_oid,
            pg_storage::segment::SegmentId(2),
            tid(1),
        );
        let store = SegmentStore::open(tmp.path(), index_oid, &AnalyzerConfig::builtin()).unwrap();
        assert_eq!(store.search_text("contract", None).unwrap().len(), 1);

        let tmp = tempfile::TempDir::new().unwrap();
        write_segment(
            tmp.path(),
            index_oid,
            pg_storage::segment::SegmentId(1),
            tid(1),
        );
        let manifest = SegmentManifest {
            generation: 1,
            active_segment_ids: vec![pg_storage::segment::SegmentId(1)],
            applied_lsn: Lsn(128),
        };
        let manifest_path = SegmentStore::manifest_path(tmp.path(), index_oid);
        std::fs::create_dir_all(manifest_path.parent().unwrap()).unwrap();
        std::fs::write(&manifest_path, manifest.encode().unwrap()).unwrap();
        let path = SegmentStore::doc_lengths_path(
            tmp.path(),
            index_oid,
            pg_storage::segment::SegmentId(1),
        );
        let mut lengths: Vec<DocLength> =
            crate::recovery::decode(&std::fs::read(&path).unwrap()).unwrap();
        lengths.push(lengths[0].clone());
        std::fs::write(&path, crate::recovery::encode(&lengths).unwrap()).unwrap();
        let error =
            SegmentStore::open(tmp.path(), index_oid, &AnalyzerConfig::builtin()).unwrap_err();
        assert!(
            error.to_string().contains("duplicate document length"),
            "{error}"
        );

        let tmp = tempfile::TempDir::new().unwrap();
        write_segment(
            tmp.path(),
            index_oid,
            pg_storage::segment::SegmentId(1),
            tid(1),
        );
        let manifest = SegmentManifest {
            generation: 1,
            active_segment_ids: vec![pg_storage::segment::SegmentId(1)],
            applied_lsn: Lsn(128),
        };
        let manifest_path = SegmentStore::manifest_path(tmp.path(), index_oid);
        std::fs::create_dir_all(manifest_path.parent().unwrap()).unwrap();
        std::fs::write(&manifest_path, manifest.encode().unwrap()).unwrap();
        let path =
            SegmentStore::postings_path(tmp.path(), index_oid, pg_storage::segment::SegmentId(1));
        let mut postings: BTreeMap<String, BTreeMap<pg_storage::types::Tid, Posting>> =
            crate::recovery::decode(&std::fs::read(&path).unwrap()).unwrap();
        let mut posting = postings
            .get_mut("contract")
            .unwrap()
            .remove(&tid(1))
            .unwrap();
        posting.tid = tid(2);
        postings
            .get_mut("contract")
            .unwrap()
            .insert(tid(1), posting);
        std::fs::write(&path, crate::recovery::encode(&postings).unwrap()).unwrap();
        let error =
            SegmentStore::open(tmp.path(), index_oid, &AnalyzerConfig::builtin()).unwrap_err();
        assert!(error.to_string().contains("disagrees"), "{error}");
    }

    #[test]
    fn inverted_methods_are_explicitly_unsupported() {
        use pg_am_heap::access_method::{
            AccessMethod, DeleteContext, InsertContext, RelationDesc, ScanContext,
        };
        use pg_storage::clog::NoOpClogAccessor;
        use pg_txn::Snapshot;

        let index = InvertedIndex::new(pg_storage::types::Oid(9879));
        assert!(index
            .search(&Query::And(Vec::new()), None)
            .unwrap()
            .is_empty());
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
