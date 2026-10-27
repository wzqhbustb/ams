//! Guards the on-disk WAL record type discriminants (Stage C).
//!
//! Discriminants are part of the on-disk format: renaming a variant is fine,
//! renumbering is not. M1-implemented values must stay exactly as shipped,
//! and Stage C's newly reserved values must match the M2 tech-selection
//! (v2.3-6) exactly.

use pg_storage::wal::record::WalRecordType;

/// Every declared variant and its on-disk value. Keep in sync with
/// `WalRecordType` in `crates/pg-storage/src/wal/record.rs`.
const ALL_TYPES: [(WalRecordType, u8); 32] = [
    (WalRecordType::HeapInsert, 1),
    (WalRecordType::HeapUpdate, 2),
    (WalRecordType::HeapDelete, 3),
    (WalRecordType::BTreeInsert, 4),
    (WalRecordType::BTreeSplitPrepare, 5),
    (WalRecordType::BTreeDelete, 6),
    (WalRecordType::HeapHotUpdate, 7),
    (WalRecordType::HeapCleanup, 8),
    (WalRecordType::FullPageImage, 10),
    (WalRecordType::TxnBegin, 20),
    (WalRecordType::TxnCommit, 21),
    (WalRecordType::TxnAbort, 22),
    (WalRecordType::CheckpointBegin, 30),
    (WalRecordType::CheckpointEnd, 31),
    (WalRecordType::PageAlloc, 40),
    (WalRecordType::PageFree, 41),
    (WalRecordType::BTreeSplitCLR, 50),
    (WalRecordType::BTreeSplitCopy, 51),
    (WalRecordType::BTreeSplitCommit, 52),
    (WalRecordType::LogicalHnsw, 100),
    (WalRecordType::LogicalInverted, 101),
    (WalRecordType::LogicalGraph, 102),
    (WalRecordType::LogicalTimeSeries, 103),
    (WalRecordType::SegmentSeal, 110),
    (WalRecordType::SegmentMerge, 111),
    // Phase 2 M5 Stage 0 (tech-selection §4.1/§4.2): physiological HNSW WAL.
    (WalRecordType::HnswNodeInit, 121),
    (WalRecordType::HnswSetNeighbors, 122),
    (WalRecordType::HnswMetaUpdate, 123),
    (WalRecordType::HnswNodeTombstone, 124),
    (WalRecordType::HnswDirAppend, 125),
    (WalRecordType::HnswDirLink, 126),
    (WalRecordType::HnswPublishLive, 127),
];

#[test]
fn test_wal_record_type_discriminant_matches_m1() {
    // M1-implemented values: never renumber.
    assert_eq!(WalRecordType::FullPageImage.to_u8(), 10);
    assert_eq!(WalRecordType::CheckpointBegin.to_u8(), 30);
    assert_eq!(WalRecordType::CheckpointEnd.to_u8(), 31);
    assert_eq!(WalRecordType::PageAlloc.to_u8(), 40);

    // Stage C rename (tech-selection v2.3-8): BTreeSplit -> BTreeSplitPrepare
    // keeps discriminant 5; it is a rename, not a new value.
    assert_eq!(WalRecordType::BTreeSplitPrepare.to_u8(), 5);

    // Phase 2/3 reserved ranges: never renumber.
    assert_eq!(WalRecordType::LogicalHnsw.to_u8(), 100);
    assert_eq!(WalRecordType::LogicalInverted.to_u8(), 101);
    assert_eq!(WalRecordType::LogicalGraph.to_u8(), 102);
    assert_eq!(WalRecordType::LogicalTimeSeries.to_u8(), 103);
    assert_eq!(WalRecordType::SegmentSeal.to_u8(), 110);
    assert_eq!(WalRecordType::SegmentMerge.to_u8(), 111);
}

#[test]
fn all_record_types_round_trip() {
    for (ty, value) in ALL_TYPES {
        assert_eq!(ty.to_u8(), value, "{ty:?} discriminant drifted");
        assert_eq!(
            WalRecordType::from_u8(value).unwrap(),
            ty,
            "{ty:?} does not round-trip through from_u8"
        );
    }
}

#[test]
fn unassigned_discriminants_are_rejected() {
    for v in [0u8, 9, 11, 42, 53, 104, 255] {
        assert!(
            WalRecordType::from_u8(v).is_err(),
            "discriminant {v} must stay unassigned"
        );
    }
}
