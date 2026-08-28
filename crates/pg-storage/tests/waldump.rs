//! M3 Stage E (tech-selection §6.1): `pg-waldump` acceptance.
//!
//! Covered:
//!
//! - a WAL containing every record family (heap / btree / txn / checkpoint
//!   / HeapCleanup / reserved) dumps cleanly, one parseable line per record,
//!   with key payload fields decoded;
//! - reserved types (`SegmentSeal` = 110, `SegmentMerge` = 111) print their
//!   raw payload bytes instead of erroring — the diagnostic policy is the
//!   opposite of recovery's hard-fail (§6.1);
//! - `--start-lsn` / `--end-lsn` filtering is inclusive and exact at both
//!   boundaries;
//! - both directory forms work (data dir with a `wal/` subdirectory, and
//!   the segment directory itself), and records spanning a segment
//!   boundary are dumped like any other.
//!
//! Acceptance: `cargo test -p pg-storage --test waldump`

use std::path::Path;
use std::process::Command;

use pg_storage::config::StorageConfig;
use pg_storage::types::{Lsn, PageId, Tid, TxnId};
use pg_storage::wal::record::{BTreeSplitCLRRecord, WalRecord, WalRecordType};
use pg_storage::wal::writer::WalWriter;
use tempfile::TempDir;

/// The compiled binary under test (same package, so cargo provides it).
const WALDUMP: &str = env!("CARGO_BIN_EXE_pg-waldump");

fn test_config(tmp: &TempDir) -> StorageConfig {
    let mut cfg = StorageConfig::new(tmp.path());
    cfg.wal_group_commit_timeout_ms = 1;
    cfg.wal_group_commit_batch_size = 1;
    // Small segments keep the fixture fast; large enough that one record
    // (an 8 KiB FPI) fits comfortably.
    cfg.wal_segment_size = 1024 * 1024;
    cfg
}

/// A `SegmentSeal`/`SegmentMerge`-style record: no producer exists (the
/// discriminants are Stage-0-reserved), so the fixture hand-builds the
/// record — every `WalRecord` field is public exactly so tooling/tests can.
fn reserved_record(record_type: WalRecordType, payload: Vec<u8>) -> WalRecord {
    WalRecord {
        lsn: Lsn::INVALID,
        prev_lsn: Lsn::INVALID,
        txn_id: TxnId::INVALID,
        record_type,
        flags: 0,
        payload,
    }
}

/// Write one record of every family and return their LSNs in write order.
fn write_full_family_wal(tmp: &TempDir) -> Vec<Lsn> {
    let cfg = test_config(tmp);
    let writer = WalWriter::open(tmp.path(), &cfg).unwrap();
    let tid = |page, slot| Tid {
        page_id: PageId(page),
        slot_id: slot,
    };
    let clr = BTreeSplitCLRRecord {
        left_page: PageId(10),
        right_page: PageId(11),
        level: 0,
        copy_start_slot: 2,
        redo_ref_lsn: Lsn(4_200),
        parent_page: PageId(12),
        parent_insert_slot: 1,
        new_root_page: PageId::INVALID,
        meta_page: PageId::INVALID,
        separator_key: vec![0xAA, 0xBB],
    };
    let records = vec![
        WalRecord::checkpoint_begin(),
        WalRecord::checkpoint_end(
            Lsn(8),
            PageId(50),
            TxnId(7),
            20_000,
            "meta/att-1.snapshot".to_string(),
            "meta/dpt-1.snapshot".to_string(),
        )
        .unwrap(),
        WalRecord::page_alloc(PageId(3)).unwrap(),
        WalRecord::page_free(PageId(4)).unwrap(),
        WalRecord::full_page_image(PageId(5), vec![0xAB; 8192]).unwrap(),
        WalRecord::heap_insert(PageId(7), 3, vec![1, 2, 3], TxnId(42)).unwrap(),
        WalRecord::heap_update(tid(7, 3), tid(7, 4), TxnId(42), vec![4, 5], TxnId(42)).unwrap(),
        WalRecord::heap_delete(tid(7, 4), TxnId(43), TxnId(43)).unwrap(),
        WalRecord::heap_hot_update(PageId(7), 5, 6, vec![7], TxnId(44), TxnId(44)).unwrap(),
        WalRecord::heap_cleanup(PageId(7), vec![1, 3], PageId::INVALID, PageId::INVALID).unwrap(),
        WalRecord::btree_insert(PageId(20), 1, 0, 1, vec![9; 14]).unwrap(),
        WalRecord::btree_delete(PageId(20), 1).unwrap(),
        WalRecord::btree_split_prepare(PageId(20), PageId(21), 0, PageId::INVALID, vec![0x01])
            .unwrap(),
        WalRecord::btree_split_copy(PageId(20), PageId(21), 3, Lsn(1_000)).unwrap(),
        WalRecord::btree_split_commit(PageId(20), PageId(21), PageId(22), vec![0x02], 0).unwrap(),
        WalRecord::btree_split_clr(&clr).unwrap(),
        WalRecord::txn_commit(TxnId(42)).unwrap(),
        WalRecord::txn_abort(TxnId(43)).unwrap(),
        reserved_record(WalRecordType::SegmentSeal, vec![0xDE, 0xAD, 0xBE, 0xEF]),
        reserved_record(WalRecordType::SegmentMerge, vec![0x01, 0x02]),
    ];
    let mut lsns = Vec::new();
    for record in records {
        lsns.push(writer.append(record).unwrap());
    }
    writer.flush().unwrap();
    lsns
}

/// Run the binary; returns stdout. Asserts a clean exit.
fn dump(dir: &Path, extra_args: &[String]) -> String {
    let out = Command::new(WALDUMP)
        .args(extra_args)
        .arg(dir)
        .output()
        .expect("failed to spawn pg-waldump");
    assert!(
        out.status.success(),
        "pg-waldump failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap()
}

/// The record lines of a dump (everything starting with `lsn=`).
fn record_lines(stdout: &str) -> Vec<&str> {
    stdout.lines().filter(|l| l.starts_with("lsn=")).collect()
}

/// The `lsn=` value of a dump line.
fn line_lsn(line: &str) -> u64 {
    line.strip_prefix("lsn=")
        .and_then(|rest| rest.split_whitespace().next())
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| panic!("unparseable dump line: {line}"))
}

#[test]
fn dump_covers_every_record_family() {
    let tmp = TempDir::new().unwrap();
    let cfg = test_config(&tmp);
    let lsns = write_full_family_wal(&tmp);

    // Data-dir form: the tool must find the `wal/` subdirectory itself.
    let stdout = dump(
        tmp.path(),
        &[
            "--segment-size".to_string(),
            cfg.wal_segment_size.to_string(),
        ],
    );
    let lines = record_lines(&stdout);
    assert_eq!(
        lines.len(),
        lsns.len(),
        "every record dumped once:\n{stdout}"
    );
    let dumped_lsns: Vec<u64> = lines.iter().map(|l| line_lsn(l)).collect();
    assert_eq!(dumped_lsns, lsns.iter().map(|l| l.0).collect::<Vec<_>>());

    // One line per family, with key fields decoded (spot-checked).
    let expect = [
        "type=CheckpointBegin(30)",
        "type=CheckpointEnd(31)",
        "type=PageAlloc(40) len=1 page=3",
        "type=PageFree(41) len=1 page=4",
        "type=FullPageImage(10)",
        "type=HeapInsert(1)",
        "type=HeapUpdate(2)",
        "type=HeapDelete(3)",
        "type=HeapHotUpdate(7)",
        "type=HeapCleanup(8)",
        "type=BTreeInsert(4)",
        "type=BTreeDelete(6)",
        "type=BTreeSplitPrepare(5)",
        "type=BTreeSplitCopy(51)",
        "type=BTreeSplitCommit(52)",
        "type=BTreeSplitCLR(50)",
        "type=TxnCommit(21)",
        "type=TxnAbort(22)",
        "type=SegmentSeal(110)",
        "type=SegmentMerge(111)",
    ];
    for needle in expect {
        assert!(
            lines.iter().any(|l| l.contains(needle)),
            "missing {needle} in:\n{stdout}"
        );
    }
    // Key payload fields are decoded, not just the type byte.
    let cleanup = lines.iter().find(|l| l.contains("HeapCleanup")).unwrap();
    assert!(cleanup.contains("page=7"), "{cleanup}");
    assert!(cleanup.contains("dead_slots=[1, 3]"), "{cleanup}");
    let commit = lines.iter().find(|l| l.contains("TxnCommit")).unwrap();
    assert!(commit.contains("commit_xid=42"), "{commit}");
    let fpi = lines.iter().find(|l| l.contains("FullPageImage")).unwrap();
    assert!(fpi.contains("page=5 image=8192B"), "{fpi}");

    // Reserved types: raw payload bytes, no error (§6.1).
    let seal = lines.iter().find(|l| l.contains("SegmentSeal")).unwrap();
    assert!(seal.contains("reserved payload=deadbeef"), "{seal}");
    let merge = lines.iter().find(|l| l.contains("SegmentMerge")).unwrap();
    assert!(merge.contains("reserved payload=0102"), "{merge}");

    assert!(stdout.contains(&format!("# dumped={} filtered=0", lsns.len())));

    // Segment-dir form: dump the `wal/` directory directly, same records.
    let stdout2 = dump(
        &tmp.path().join("wal"),
        &[
            "--segment-size".to_string(),
            cfg.wal_segment_size.to_string(),
        ],
    );
    assert_eq!(record_lines(&stdout2), lines);
}

#[test]
fn lsn_filter_boundaries_are_inclusive() {
    let tmp = TempDir::new().unwrap();
    let cfg = test_config(&tmp);
    let lsns = write_full_family_wal(&tmp);
    let seg = cfg.wal_segment_size.to_string();

    // [lsns[5], lsns[9]] inclusive: exactly records 5..=9, boundaries exact.
    let stdout = dump(
        tmp.path(),
        &[
            "--segment-size".into(),
            seg.clone(),
            "--start-lsn".into(),
            lsns[5].0.to_string(),
            "--end-lsn".into(),
            lsns[9].0.to_string(),
        ],
    );
    let lines = record_lines(&stdout);
    assert_eq!(lines.len(), 5, "{stdout}");
    assert_eq!(line_lsn(lines[0]), lsns[5].0);
    assert_eq!(line_lsn(lines[4]), lsns[9].0);
    assert!(stdout.contains("dumped=5 filtered=15"));

    // Open-ended filters: start only, end only.
    let stdout = dump(
        tmp.path(),
        &[
            "--segment-size".into(),
            seg.clone(),
            "--start-lsn".into(),
            lsns[19].0.to_string(),
        ],
    );
    let lines = record_lines(&stdout);
    assert_eq!(lines.len(), 1, "{stdout}");
    assert_eq!(line_lsn(lines[0]), lsns[19].0);

    let stdout = dump(
        tmp.path(),
        &[
            "--segment-size".into(),
            seg,
            "--end-lsn".into(),
            lsns[0].0.to_string(),
        ],
    );
    let lines = record_lines(&stdout);
    assert_eq!(lines.len(), 1, "{stdout}");
    assert_eq!(line_lsn(lines[0]), lsns[0].0);

    // Hex LSN arguments are accepted too.
    let stdout = dump(
        tmp.path(),
        &[
            "--segment-size".into(),
            cfg.wal_segment_size.to_string(),
            "--start-lsn".into(),
            format!("{:#x}", lsns[19].0),
        ],
    );
    assert_eq!(record_lines(&stdout).len(), 1, "{stdout}");

    // start > end is a usage error, not a silent empty dump.
    let out = Command::new(WALDUMP)
        .args([
            "--segment-size".to_string(),
            cfg.wal_segment_size.to_string(),
            "--start-lsn".to_string(),
            "1000".to_string(),
            "--end-lsn".to_string(),
            "8".to_string(),
        ])
        .arg(tmp.path())
        .output()
        .unwrap();
    assert!(!out.status.success());
}

#[test]
fn dump_reads_across_segment_boundary() {
    let tmp = TempDir::new().unwrap();
    let mut cfg = test_config(&tmp);
    // 256-byte segments: the fixture's ~40-byte records force several
    // segment hops (records themselves never straddle a boundary — the
    // writer aligns them — but the reader must chain files).
    cfg.wal_segment_size = 256;
    let writer = WalWriter::open(tmp.path(), &cfg).unwrap();
    let n = 20;
    for i in 0..n {
        writer
            .append(WalRecord::page_alloc(PageId(i + 1)).unwrap())
            .unwrap();
    }
    writer.flush().unwrap();

    let stdout = dump(
        tmp.path(),
        &[
            "--segment-size".to_string(),
            cfg.wal_segment_size.to_string(),
        ],
    );
    let lines = record_lines(&stdout);
    assert_eq!(lines.len(), n as usize, "{stdout}");
    assert!(lines.iter().all(|l| l.contains("type=PageAlloc(40)")));
}
