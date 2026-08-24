//! M3 Stage B (tech-selection §4.5): `HeapCleanup = 8` WAL payload
//! encode/decode roundtrip + analysis-phase DPT classification.

use pg_storage::analysis::{find_latest_checkpoint_end, run_analysis};
use pg_storage::config::StorageConfig;
use pg_storage::types::{PageId, TxnId};
use pg_storage::wal::record::{
    HeapCleanupRecord, WalRecord, WalRecordType, MAX_HEAP_CLEANUP_SLOTS,
};
use pg_storage::wal::writer::WalWriter;

use tempfile::TempDir;

fn test_config(tmp: &TempDir) -> StorageConfig {
    let mut cfg = StorageConfig::new(tmp.path());
    cfg.wal_group_commit_timeout_ms = 1;
    cfg.wal_group_commit_batch_size = 1;
    cfg
}

#[test]
fn heap_cleanup_payload_roundtrip() {
    // Compaction + chain unlink shape.
    let mut record =
        WalRecord::heap_cleanup(PageId(7), vec![1, 3, 5], PageId(4), PageId(6)).unwrap();
    // The discriminant is the Stage-0-reserved value, not a new assignment.
    assert_eq!(record.record_type.to_u8(), 8);
    // Vacuum is not transactional: the record stays untransactional.
    assert_eq!(record.txn_id, TxnId::INVALID);
    record.lsn = pg_storage::types::Lsn(96);
    let buf = record.encode().unwrap();
    let (decoded, consumed) = WalRecord::decode(&buf).unwrap();
    assert_eq!(consumed, buf.len());
    assert_eq!(decoded.record_type, WalRecordType::HeapCleanup);
    let payload = HeapCleanupRecord::decode(&decoded.payload).unwrap();
    assert_eq!(payload.page_id, PageId(7));
    assert_eq!(payload.dead_slots, vec![1, 3, 5]);
    assert_eq!(payload.unlink_prev_page, PageId(4));
    assert_eq!(payload.unlink_next_page, PageId(6));

    // Compaction-only shape (no unlink): INVALID sentinels roundtrip.
    let record =
        WalRecord::heap_cleanup(PageId(9), vec![0, 2], PageId::INVALID, PageId::INVALID).unwrap();
    let payload = HeapCleanupRecord::decode(&record.payload).unwrap();
    assert_eq!(payload.page_id, PageId(9));
    assert_eq!(payload.dead_slots, vec![0, 2]);
    assert_eq!(payload.unlink_prev_page, PageId::INVALID);
    assert_eq!(payload.unlink_next_page, PageId::INVALID);

    // Empty kill list roundtrips too (pure relink records are legal).
    let record =
        WalRecord::heap_cleanup(PageId(9), Vec::new(), PageId::INVALID, PageId::INVALID).unwrap();
    let payload = HeapCleanupRecord::decode(&record.payload).unwrap();
    assert!(payload.dead_slots.is_empty());
}

/// Two-layer rejection of a bad kill list (F3):
/// 1. The CONSTRUCTOR refuses non-ascending or over-bound lists with a hard
///    `Err` — under the WAL-first protocol a poison record that reached the
///    log would hard-fail every subsequent recovery.
/// 2. `decode` still enforces the length bound on its own (defense in depth
///    for bytes already on disk, same policy as the CLR separator bound): a
///    corrupt length prefix on a CRC-valid record must not be trusted.
#[test]
fn heap_cleanup_rejects_bad_dead_slots() {
    // Layer 1: constructor validation.
    assert!(
        WalRecord::heap_cleanup(PageId(1), vec![2, 1], PageId::INVALID, PageId::INVALID).is_err(),
        "non-ascending kill list must be rejected at construction"
    );
    assert!(
        WalRecord::heap_cleanup(PageId(1), vec![1, 1], PageId::INVALID, PageId::INVALID).is_err(),
        "duplicate slots are not strictly ascending"
    );
    let ascending = |n: usize| (0..n).map(|i| i as u16).collect::<Vec<u16>>();
    assert!(
        WalRecord::heap_cleanup(
            PageId(1),
            ascending(MAX_HEAP_CLEANUP_SLOTS + 1),
            PageId::INVALID,
            PageId::INVALID,
        )
        .is_err(),
        "over-bound kill list must be rejected at construction"
    );

    // A kill list exactly at the bound still roundtrips through both layers.
    let at_bound = WalRecord::heap_cleanup(
        PageId(1),
        ascending(MAX_HEAP_CLEANUP_SLOTS),
        PageId::INVALID,
        PageId::INVALID,
    )
    .unwrap();
    assert!(HeapCleanupRecord::decode(&at_bound.payload).is_ok());

    // Layer 2: decode enforces the bound on hand-encoded bytes (bypassing
    // the constructor, as on-disk bytes from an older/buggier binary would).
    let over_bound = HeapCleanupRecord {
        page_id: PageId(1),
        unlink_prev_page: PageId::INVALID,
        unlink_next_page: PageId::INVALID,
        dead_slots: ascending(MAX_HEAP_CLEANUP_SLOTS + 1),
    };
    let payload = bincode::serde::encode_to_vec(&over_bound, bincode::config::standard()).unwrap();
    assert!(
        HeapCleanupRecord::decode(&payload).is_err(),
        "a kill list past the bound must be rejected at decode"
    );
}

/// Analysis (DPT) classification: a `HeapCleanup` record dirties the
/// compacted page, plus the predecessor page when it unlinks — never the
/// relink target, which is not itself modified.
#[test]
fn heap_cleanup_analysis_dpt_classification() {
    let tmp = TempDir::new().unwrap();
    let cfg = test_config(&tmp);
    let wal = WalWriter::open(tmp.path(), &cfg).unwrap();

    let begin = wal.append(WalRecord::checkpoint_begin()).unwrap();
    wal.append(
        WalRecord::checkpoint_end(
            begin,
            PageId(7),
            TxnId(3),
            16_384,
            String::new(),
            String::new(),
        )
        .unwrap(),
    )
    .unwrap();
    // Compaction-only on page 20; compaction+unlink on page 30 (predecessor
    // 29 relinked to 31).
    wal.append(
        WalRecord::heap_cleanup(PageId(20), vec![1, 3], PageId::INVALID, PageId::INVALID).unwrap(),
    )
    .unwrap();
    wal.append(WalRecord::heap_cleanup(PageId(30), vec![0], PageId(29), PageId(31)).unwrap())
        .unwrap();
    wal.flush().unwrap();
    drop(wal);

    let (end, _) = find_latest_checkpoint_end(&tmp.path().join("wal"), cfg.wal_segment_size, begin)
        .unwrap()
        .unwrap();
    let result = run_analysis(tmp.path(), cfg.wal_segment_size, &end).unwrap();

    let pages: Vec<PageId> = result.dpt.iter().map(|(p, _)| *p).collect();
    assert_eq!(
        pages,
        vec![PageId(20), PageId(29), PageId(30)],
        "DPT must cover the compacted page and the unlink predecessor, not the relink target"
    );
    // Untransactional: no ATT entry is created for vacuum records.
    assert!(result.att.is_empty());
}
