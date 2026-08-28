//! `pg-waldump` — human-readable WAL dump (M3 Stage E, tech-selection §6.1).
//!
//! Prints every WAL record as one line: LSN, record type, and the payload's
//! key fields. Lives in `pg-storage` because the record format,
//! `WalRecordType` decoding, and the segment-file layout are all defined
//! here (§6.1 选型 (a): bin 与格式同 crate 演进、零新依赖边).
//!
//! ```text
//! pg-waldump [--start-lsn <LSN>] [--end-lsn <LSN>] [--segment-size <N>] <DIR>
//! ```
//!
//! `<DIR>` is either a data directory (its `wal/` subdirectory is dumped)
//! or a WAL segment directory directly. `--start-lsn` / `--end-lsn` filter
//! inclusively on the record's own LSN; values are decimal or `0x` hex.
//! `--segment-size` must match the writer's `StorageConfig::
//! wal_segment_size` (default 16 MiB).
//!
//! # Error policy — the opposite of recovery
//!
//! Recovery hard-fails on records it cannot replay; this tool shows as much
//! as possible (§6.1):
//!
//! - Reserved types with no producer/replay handler (`SegmentSeal` = 110,
//!   `SegmentMerge` = 111, the Phase-2+ logical index types 100–103) print
//!   their raw payload bytes instead of erroring.
//! - A record whose payload fails to decode (corrupt length prefix, newer
//!   payload layout) still prints its header fields plus the raw payload.
//! - A genuinely unknown type discriminant (written by a newer binary) or a
//!   mid-file CRC failure cannot be walked past — the record boundary is
//!   unreadable — so the dump stops there and reports the LSN; everything
//!   before it has already been printed. Torn tail records (crash
//!   mid-write) are clean end-of-WAL, same as recovery.

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use pg_storage::types::WAL_SEGMENT_SIZE;
use pg_storage::wal::reader::WalReader;
use pg_storage::wal::record::{
    BTreeDeleteRecord, BTreeInsertRecord, BTreeSplitCLRRecord, BTreeSplitCommitRecord,
    BTreeSplitCopyRecord, BTreeSplitPrepareRecord, CheckpointEndRecord, FullPageImageRecord,
    HeapCleanupRecord, HeapDeleteRecord, HeapHotUpdateRecord, HeapInsertRecord, HeapUpdateRecord,
    PageAllocRecord, PageFreeRecord, TxnAbortRecord, TxnCommitRecord, WalRecord, WalRecordType,
};

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("pg-waldump: error: {e}");
            ExitCode::FAILURE
        }
    }
}

/// Parsed command line.
struct Args {
    /// Data directory or WAL segment directory.
    dir: PathBuf,
    /// Inclusive LSN filter bounds.
    start_lsn: Option<u64>,
    /// Inclusive LSN filter bounds.
    end_lsn: Option<u64>,
    /// The writer's segment size (needed to map LSN → segment file).
    segment_size: u64,
}

fn parse_args(argv: &[String]) -> Result<Args, String> {
    let mut dir = None;
    let mut start_lsn = None;
    let mut end_lsn = None;
    let mut segment_size = WAL_SEGMENT_SIZE;
    let mut i = 0;
    while i < argv.len() {
        let arg = argv[i].as_str();
        let mut take_value = |name: &str| -> Result<String, String> {
            i += 1;
            argv.get(i)
                .cloned()
                .ok_or_else(|| format!("{name} requires a value"))
        };
        match arg {
            "--start-lsn" => start_lsn = Some(parse_u64(&take_value(arg)?)?),
            "--end-lsn" => end_lsn = Some(parse_u64(&take_value(arg)?)?),
            "--segment-size" => segment_size = parse_u64(&take_value(arg)?)?,
            "-h" | "--help" => return Err(usage()),
            _ if arg.starts_with('-') => return Err(format!("unknown option {arg}\n{}", usage())),
            _ => {
                if dir.is_some() {
                    return Err(format!("multiple directory arguments\n{}", usage()));
                }
                dir = Some(PathBuf::from(arg));
            }
        }
        i += 1;
    }
    let dir = dir.ok_or_else(usage)?;
    if let (Some(s), Some(e)) = (start_lsn, end_lsn) {
        if s > e {
            return Err(format!("--start-lsn {s} is greater than --end-lsn {e}"));
        }
    }
    Ok(Args {
        dir,
        start_lsn,
        end_lsn,
        segment_size,
    })
}

fn usage() -> String {
    "usage: pg-waldump [--start-lsn <LSN>] [--end-lsn <LSN>] [--segment-size <N>] \
     <DATA_DIR_OR_WAL_DIR>"
        .to_string()
}

/// Parse a `u64` given as decimal or `0x`-prefixed hex.
fn parse_u64(s: &str) -> Result<u64, String> {
    match s.strip_prefix("0x") {
        Some(hex) => u64::from_str_radix(hex, 16),
        None => s.parse::<u64>(),
    }
    .map_err(|e| format!("invalid number {s:?}: {e}"))
}

/// Resolve the segment directory: a data directory's `wal/` subdirectory
/// when it exists, otherwise the argument itself.
fn resolve_wal_dir(dir: &Path) -> Result<PathBuf, String> {
    let wal_subdir = dir.join("wal");
    if wal_subdir.is_dir() {
        return Ok(wal_subdir);
    }
    if dir.is_dir() {
        return Ok(dir.to_path_buf());
    }
    Err(format!("{} is not a directory", dir.display()))
}

fn run() -> Result<(), String> {
    let args = parse_args(&std::env::args().skip(1).collect::<Vec<_>>())?;
    let wal_dir = resolve_wal_dir(&args.dir)?;
    println!(
        "# pg-waldump dir={} segment_size={} start_lsn={} end_lsn={}",
        wal_dir.display(),
        args.segment_size,
        args.start_lsn.map_or("-".to_string(), |v| v.to_string()),
        args.end_lsn.map_or("-".to_string(), |v| v.to_string()),
    );

    let mut reader = WalReader::open(&wal_dir, args.segment_size)
        .map_err(|e| format!("cannot open {}: {e}", wal_dir.display()))?;
    let mut dumped = 0u64;
    let mut filtered = 0u64;
    loop {
        // The position of the record about to be read, for error reporting.
        let record_start = reader.current_lsn();
        let record = match reader.next_record() {
            Ok(Some(record)) => record,
            Ok(None) => break,
            // See the module docs: an unwalkable record ends the dump, but
            // everything before it has been printed.
            Err(e) => return Err(format!("stopped at lsn={}: {e}", record_start.0)),
        };
        let in_range = args.start_lsn.is_none_or(|s| record.lsn.0 >= s)
            && args.end_lsn.is_none_or(|e| record.lsn.0 <= e);
        if in_range {
            println!("{}", format_record(&record));
            dumped += 1;
        } else {
            filtered += 1;
        }
    }
    println!("# dumped={dumped} filtered={filtered}");
    Ok(())
}

/// One dump line: `lsn=.. prev=.. xid=.. type=Name(N) len=.. <fields>`.
fn format_record(record: &WalRecord) -> String {
    let head = format!(
        "lsn={} prev={} xid={} type={:?}({}) len={}",
        record.lsn.0,
        record.prev_lsn.0,
        record.txn_id.0,
        record.record_type,
        record.record_type.to_u8(),
        record.payload.len(),
    );
    let fields = payload_fields(record);
    if fields.is_empty() {
        head
    } else {
        format!("{head} {fields}")
    }
}

/// The type-specific key fields; reserved types get their raw payload hex.
fn payload_fields(record: &WalRecord) -> String {
    let p = &record.payload;
    match record.record_type {
        WalRecordType::PageAlloc => {
            decoded::<PageAllocRecord>(p, |r| format!("page={}", r.page_id.0))
        }
        WalRecordType::PageFree => {
            decoded::<PageFreeRecord>(p, |r| format!("page={}", r.page_id.0))
        }
        WalRecordType::FullPageImage => decoded::<FullPageImageRecord>(p, |r| {
            format!("page={} image={}B", r.page_id.0, r.image.len())
        }),
        WalRecordType::HeapInsert => match HeapInsertRecord::decode(p) {
            Ok(r) => format!(
                "page={} slot={} tuple={}B",
                r.page_id.0,
                r.slot_id,
                r.tuple_bytes.len()
            ),
            Err(e) => undecodable(p, &e),
        },
        WalRecordType::HeapUpdate => match HeapUpdateRecord::decode(p) {
            Ok(r) => format!(
                "old=({}, {}) new=({}, {}) xmax_old={} tuple={}B",
                r.old_tid.page_id.0,
                r.old_tid.slot_id,
                r.new_tid.page_id.0,
                r.new_tid.slot_id,
                r.xmax_old.0,
                r.new_tuple_bytes.len()
            ),
            Err(e) => undecodable(p, &e),
        },
        WalRecordType::HeapDelete => match HeapDeleteRecord::decode(p) {
            Ok(r) => format!(
                "tid=({}, {}) xmax={}",
                r.tid.page_id.0, r.tid.slot_id, r.xmax.0
            ),
            Err(e) => undecodable(p, &e),
        },
        WalRecordType::HeapHotUpdate => match HeapHotUpdateRecord::decode(p) {
            Ok(r) => format!(
                "page={} old_slot={} new_slot={} xmax={} tuple={}B",
                r.page_id.0,
                r.old_slot,
                r.new_slot,
                r.xmax.0,
                r.new_tuple_bytes.len()
            ),
            Err(e) => undecodable(p, &e),
        },
        WalRecordType::HeapCleanup => match HeapCleanupRecord::decode(p) {
            Ok(r) => format!(
                "page={} dead_slots={:?} unlink_prev={} unlink_next={}",
                r.page_id.0, r.dead_slots, r.unlink_prev_page.0, r.unlink_next_page.0
            ),
            Err(e) => undecodable(p, &e),
        },
        WalRecordType::BTreeInsert => match BTreeInsertRecord::decode(p) {
            Ok(r) => format!(
                "page={} slot={} level={} flags={} entry={}B",
                r.page_id.0,
                r.slot_id,
                r.level,
                r.flags,
                r.tuple_bytes.len()
            ),
            Err(e) => undecodable(p, &e),
        },
        WalRecordType::BTreeDelete => match BTreeDeleteRecord::decode(p) {
            Ok(r) => format!("page={} slot={}", r.page_id.0, r.slot_id),
            Err(e) => undecodable(p, &e),
        },
        WalRecordType::BTreeSplitPrepare => match BTreeSplitPrepareRecord::decode(p) {
            Ok(r) => format!(
                "left={} new_right={} level={} left_old_next={} high_key={}B",
                r.left_page.0,
                r.new_right_page.0,
                r.level,
                r.left_old_next.0,
                r.high_key_bytes.len()
            ),
            Err(e) => undecodable(p, &e),
        },
        WalRecordType::BTreeSplitCopy => match BTreeSplitCopyRecord::decode(p) {
            Ok(r) => format!(
                "left={} right={} copy_start_slot={} left_pre_lsn={}",
                r.left_page.0, r.right_page.0, r.copy_start_slot, r.left_page_pre_lsn.0
            ),
            Err(e) => undecodable(p, &e),
        },
        WalRecordType::BTreeSplitCommit => match BTreeSplitCommitRecord::decode(p) {
            Ok(r) => format!(
                "left={} right={} parent={} parent_slot={} separator={}B",
                r.left_page.0,
                r.right_page.0,
                r.parent_page.0,
                r.parent_insert_slot,
                r.separator_key.len()
            ),
            Err(e) => undecodable(p, &e),
        },
        WalRecordType::BTreeSplitCLR => match BTreeSplitCLRRecord::decode(p) {
            Ok(r) => format!(
                "left={} right={} level={} parent={} new_root={} meta={} separator={}B",
                r.left_page.0,
                r.right_page.0,
                r.level,
                r.parent_page.0,
                r.new_root_page.0,
                r.meta_page.0,
                r.separator_key.len()
            ),
            Err(e) => undecodable(p, &e),
        },
        WalRecordType::TxnCommit => match TxnCommitRecord::decode(p) {
            Ok(r) => format!("commit_xid={}", r.xid.0),
            Err(e) => undecodable(p, &e),
        },
        WalRecordType::TxnAbort => match TxnAbortRecord::decode(p) {
            Ok(r) => format!("abort_xid={}", r.xid.0),
            Err(e) => undecodable(p, &e),
        },
        WalRecordType::CheckpointBegin => String::new(),
        WalRecordType::CheckpointEnd => match CheckpointEndRecord::decode(p, record.flags) {
            Ok(r) => format!(
                "checkpoint_lsn={} next_page={} next_xid={} next_oid={} att={:?} dpt={:?}",
                r.checkpoint_lsn.0,
                r.next_page_id.0,
                r.next_txn_id.0,
                r.next_oid,
                r.att_file,
                r.dpt_file
            ),
            Err(e) => undecodable(p, &e),
        },
        // Reserved (no producer / replay handler yet): show the raw payload
        // bytes instead of erroring (§6.1 — the opposite of recovery).
        WalRecordType::TxnBegin
        | WalRecordType::LogicalHnsw
        | WalRecordType::LogicalInverted
        | WalRecordType::LogicalGraph
        | WalRecordType::LogicalTimeSeries
        | WalRecordType::SegmentSeal
        | WalRecordType::SegmentMerge => format!("reserved payload={}", hex(p)),
    }
}

/// Decode a payload that has no crate-provided `decode` fn, using the same
/// bincode configuration as `wal::record` — shared via the pub
/// `wal::bincode_config()` so the bin and the lib can never drift apart.
fn decoded<T: serde::de::DeserializeOwned>(
    payload: &[u8],
    show: impl FnOnce(&T) -> String,
) -> String {
    match bincode::serde::decode_from_slice::<T, _>(payload, pg_storage::wal::bincode_config()) {
        Ok((value, _)) => show(&value),
        Err(e) => format!("payload={} (undecodable: {e})", hex(payload)),
    }
}

/// Fallback for a payload that fails its bounded decode: header fields were
/// already printed; show the raw bytes too.
fn undecodable(payload: &[u8], e: &impl std::fmt::Display) -> String {
    format!("payload={} (undecodable: {e})", hex(payload))
}

/// Lowercase hex dump of raw payload bytes.
fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}
