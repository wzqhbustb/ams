//! ARIES analysis phase (M2b Stage N; tech-selection §11.1, §11.4).
//!
//! Crash recovery runs in three phases — Analysis, Redo, Undo — of which
//! Stage N implements the first two:
//!
//! - **Analysis** (this module): locate the latest *completed*
//!   `CheckpointEnd` record, seed the ATT/DPT from the snapshot files it
//!   references (v2 payload), and scan the WAL from the checkpoint's redo
//!   point to the tail, rebuilding the active transaction table (ATT) and
//!   dirty page table (DPT) as of the crash. The scan never invokes redo
//!   handlers and never DECODES bulk payload fields (tuple images,
//!   full-page images) — only the `PageId` prefix fields. Note the reader
//!   still reads every record wholesale and verifies its CRC, so the I/O
//!   volume is the same as redo's; what analysis saves is payload
//!   decoding, allocation, and handler dispatch.
//! - **Redo** ([`crate::engine::StorageEngine`]): replay from
//!   [`AnalysisResult::redo_start`] through the `RedoRegistry`, exactly as
//!   before — analysis only changes *where* replay starts and what is known
//!   about in-flight transactions.
//! - **Undo**: deliberately absent in this stage; see below.
//!
//! # Simplified Undo — semantic decision (§11.3 alignment)
//!
//! §11.3 step 1 asks recovery to stamp every ATT member `ABORTED` in the
//! CLOG. Stage N ships no such write, on purpose: an XID that is still in
//! the ATT after the analysis scan has no terminal record (`TxnCommit` /
//! `TxnAbort`) anywhere in the durable WAL — one would have removed it. The
//! rebuilt CLOG therefore holds no entry for that XID and reads it as
//! `InProgress`, and `InProgress` tuples are invisible under MVCC — exactly
//! the visibility state an explicit `ABORTED` stamp would produce. The
//! stamp becomes load-bearing only if a later stage ever interprets a
//! missing CLOG entry as anything other than `InProgress` (v2.3-2 already
//! forbids reading it as committed); emitting `TxnAbort` records for
//! crashed XIDs is deferred to the txn layer, which owns the CLOG. The ATT
//! is exposed to callers via
//! [`crate::engine::StorageEngine::recovered_active_xids`].
//!
//! What Stage N DOES do is the filter half of undo (Stage N review, P2-3):
//! after redo has rebuilt the CLOG, the engine drops recovered-ATT members
//! the CLOG already knows as `Committed`/`Aborted`. This closes the §11.4
//! ATT-snapshot race — a commit whose WAL record predates the checkpoint
//! begin is invisible to the analysis scan, yet the racy snapshot may still
//! list the XID (its CLOG bit was persisted by the checkpoint's CLOG flush,
//! so filtering is sound). The survivors are genuinely uncommitted. The
//! filter runs in
//! [`crate::engine::StorageEngine::recover_with_redo_handlers`], not here,
//! because the CLOG is only complete after redo; note that under the
//! `NoOp` CLOG (M1/heap-only configurations) every XID reads `Committed`,
//! so the recovered ATT comes back empty — correct for configurations with
//! no visibility decisions to inform.
//!
//! # Crash mid-checkpoint
//!
//! A checkpoint that crashed between `CheckpointBegin` and `CheckpointEnd`
//! leaves a dangling `CheckpointBegin` in the WAL but never updates the
//! superblock (checkpoint.rs writes the superblock only after
//! `flush_to(end_lsn)`). Analysis therefore starts its checkpoint scan at
//! the superblock's redo point and finds the *previous* completed
//! `CheckpointEnd`; the dangling begin is inert (not page-modifying, no
//! `txn_id`). If no completed checkpoint exists at all, recovery falls back
//! to [`Lsn::FIRST`].

use std::collections::{HashMap, HashSet};
use std::path::Path;

use tracing::{debug, info, warn};

use crate::error::{Result, StorageError};
use crate::types::{Lsn, PageId, Tid, TxnId};
use crate::wal::reader::WalReader;
use crate::wal::record::{bincode_config, CheckpointEndRecord, WalRecord, WalRecordType};

/// Outcome of the analysis phase: where redo must start, plus the ARIES
/// tables rebuilt as of the crash (tech-selection §11.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnalysisResult {
    /// LSN at which the redo phase starts replaying. Currently always the
    /// checkpoint redo point; see [`run_analysis`] for why the §11.1
    /// min-formula collapses to it.
    pub redo_start: Lsn,
    /// XIDs active (neither committed nor aborted) at the crash, sorted by
    /// XID. See the module docs for why no explicit undo runs for them.
    pub att: Vec<TxnId>,
    /// Dirty page table — `page_id → rec_lsn` (the oldest record that may
    /// need redoing for the page), sorted by page id. Seeded from the
    /// checkpoint's DPT snapshot and extended by the WAL-tail scan.
    pub dpt: Vec<(PageId, Lsn)>,
}

/// Find the most recent `CheckpointEnd` record in the WAL, scanning forward
/// from `scan_start` and keeping the last one seen. Returns the decoded
/// record (v1/v2 dispatched on the record's `flags`; see
/// [`CheckpointEndRecord::decode`]) and the LSN at which the record itself
/// was written.
///
/// `scan_start` must be a guaranteed record boundary — in practice the
/// superblock's `checkpoint_lsn`, or [`Lsn::FIRST`] when no checkpoint ever
/// completed. Scanning from the oldest retained segment's *boundary*
/// instead would be wrong: segment recycling keeps the segment containing
/// the redo point, whose first bytes may cut through a record (records span
/// segments).
///
/// A truncated or corrupt tail is treated as end-of-WAL, matching redo's
/// policy: every complete record before it has been seen.
///
/// # Errors
///
/// Returns an error only when the reader cannot be opened or a
/// `CheckpointEnd` payload fails to decode (a CRC-valid record with a
/// malformed payload is corruption, not end-of-WAL).
pub fn find_latest_checkpoint_end(
    wal_dir: &Path,
    segment_size: u64,
    scan_start: Lsn,
) -> Result<Option<(CheckpointEndRecord, Lsn)>> {
    let mut reader = WalReader::open_at(wal_dir, segment_size, scan_start)?;
    let mut latest: Option<(CheckpointEndRecord, Lsn)> = None;
    loop {
        match reader.next_record() {
            Ok(Some(record)) => {
                if record.record_type == WalRecordType::CheckpointEnd {
                    let decoded = CheckpointEndRecord::decode(&record.payload, record.flags)?;
                    debug!(lsn = %record.lsn, checkpoint_lsn = %decoded.checkpoint_lsn, "found CheckpointEnd");
                    latest = Some((decoded, record.lsn));
                }
            }
            Ok(None) => break,
            Err(e) => {
                // Propagate hard errors (hole detection, CRC mismatch) —
                // they are not end-of-WAL and must not be silently skipped.
                if is_hard_error(&e) {
                    return Err(e);
                }
                warn!(error = %e, "analysis checkpoint scan stopped at truncated/final record");
                break;
            }
        }
    }
    Ok(latest)
}

/// Run the analysis phase against a located `checkpoint_end` anchor
/// (tech-selection §11.1):
///
/// 1. Seed the ATT/DPT from the snapshot files the record references. An
///    empty `att_file`/`dpt_file` (v1 record, or a provider-less M2
///    checkpoint) means "no snapshot — rebuild by a full WAL scan from
///    `checkpoint_lsn`". An unreadable snapshot file degrades to the same
///    path with a warning: it is always *correct*, because the completing
///    checkpoint flushed every page its DPT snapshot contained (§11.4's
///    descending-LSN retry across the retained snapshots is future work).
/// 2. Scan the WAL from `checkpoint_end.checkpoint_lsn` to the tail: a
///    record with a valid `txn_id` inserts the XID into the ATT, a
///    `TxnCommit`/`TxnAbort` removes it; a page-modifying record inserts
///    `page_id → record.lsn` into the DPT on first touch only
///    (`entry(page_id).or_insert(lsn)` — the rec_lsn is the *first* record
///    that dirtied the page).
/// 3. Compute `redo_start`.
///
/// # `redo_start` and the empty-DPT semantics
///
/// §11.1 sets the redo LSN to `min(DPT rec_lsns)`, or `checkpoint_lsn` when
/// the DPT is empty. Two clamps apply to that formula here:
///
/// - **Never later than `checkpoint_lsn`**: non-page records between the
///   redo point and the first dirtying record — above all
///   `TxnCommit`/`TxnAbort`, which redo piggybacks into the CLOG — must be
///   replayed too.
/// - **Never earlier than `checkpoint_lsn`**: baseline DPT entries loaded
///   from the snapshot were captured at `CheckpointBegin`, and the
///   completing checkpoint flushed every one of those pages before emitting
///   `CheckpointEnd`, so their pre-begin records are already durable — and
///   the WAL segments holding them may already be recycled, making an
///   earlier start point unreadable. Dirt written after the begin is
///   re-discovered by the scan above with `rec_lsn >= checkpoint_lsn`.
///
/// Both clamps make the formula collapse to the checkpoint redo point
/// itself; the full DPT is still returned for observability and for the
/// (future) undo phase.
///
/// # Errors
///
/// Returns an error when the WAL reader cannot be opened or a record's
/// payload prefix fails to decode. A truncated/corrupt WAL tail is treated
/// as end-of-WAL (see [`find_latest_checkpoint_end`]).
pub fn run_analysis(
    data_dir: &Path,
    segment_size: u64,
    checkpoint_end: &CheckpointEndRecord,
) -> Result<AnalysisResult> {
    let checkpoint_lsn = checkpoint_end.checkpoint_lsn;
    let mut att = load_att_snapshot(data_dir, &checkpoint_end.att_file);
    let mut dpt = load_dpt_snapshot(data_dir, &checkpoint_end.dpt_file);

    let mut reader = WalReader::open_at(data_dir.join("wal"), segment_size, checkpoint_lsn)?;
    let mut records_scanned = 0usize;
    loop {
        match reader.next_record() {
            Ok(Some(record)) => {
                records_scanned += 1;
                match record.record_type {
                    WalRecordType::TxnCommit | WalRecordType::TxnAbort => {
                        att.remove(&record.txn_id);
                    }
                    _ => {
                        if record.txn_id != TxnId::INVALID {
                            att.insert(record.txn_id);
                        }
                    }
                }
                for_each_touched_page(&record, &mut |page_id| {
                    dpt.entry(page_id).or_insert(record.lsn);
                })?;
            }
            Ok(None) => break,
            Err(e) => {
                if is_hard_error(&e) {
                    return Err(e);
                }
                warn!(error = %e, "analysis WAL scan stopped at truncated/final record");
                break;
            }
        }
    }

    // See the doc comment above: the §11.1 min-formula, clamped at the
    // checkpoint redo point on both sides, collapses to the redo point.
    let redo_start = checkpoint_lsn;

    let mut att: Vec<TxnId> = att.into_iter().collect();
    att.sort_unstable_by_key(|xid| xid.0);
    let mut dpt: Vec<(PageId, Lsn)> = dpt.into_iter().collect();
    dpt.sort_unstable_by_key(|(page_id, _)| page_id.0);

    info!(
        %checkpoint_lsn,
        %redo_start,
        records_scanned,
        att = att.len(),
        dpt = dpt.len(),
        "analysis phase complete"
    );
    Ok(AnalysisResult {
        redo_start,
        att,
        dpt,
    })
}

/// Invoke `f` for every page a record physically modifies.
///
/// Only the `PageId`-bearing prefix fields of the payload are decoded —
/// the bulk fields (tuple images, full-page images) are never deserialized
/// or copied out. (The `WalReader` has already read the whole record and
/// verified its CRC; the saving here is payload decoding/allocation, not
/// I/O.) Decoding an 8 KiB FPI per record on top of that would make
/// analysis a payload-copying scan for no benefit. This is what the
/// Stage N performance sanity check ("analysis far faster than redo")
/// relies on.
///
/// Record types without a page-modifying producer (`Txn*`, `Checkpoint*`,
/// and the Phase-2+ logical/segment records) touch no pages here.
/// `HeapCleanup` — since M3 Stage B — carries a fixed page-id prefix
/// (`page_id`, `unlink_prev_page`, `unlink_next_page`) and IS tracked; its
/// variable-length `dead_slots` tail is never decoded (same prefix-decode
/// discipline as `BTreeSplitCLR`).
fn for_each_touched_page(record: &WalRecord, f: &mut impl FnMut(PageId)) -> Result<()> {
    use WalRecordType::*;
    let payload = record.payload.as_slice();
    match record.record_type {
        // All of these carry `page_id` as the first payload field.
        PageAlloc | PageFree | FullPageImage | HeapInsert | BTreeInsert | BTreeDelete => {
            let mut off = 0;
            f(decode_prefix::<PageId>(payload, &mut off)?);
        }
        HeapDelete => {
            let mut off = 0;
            let tid = decode_prefix::<Tid>(payload, &mut off)?;
            f(tid.page_id);
        }
        HeapUpdate => {
            // delete-old + insert-new: touches both the old and the new page.
            let mut off = 0;
            let old_tid = decode_prefix::<Tid>(payload, &mut off)?;
            let new_tid = decode_prefix::<Tid>(payload, &mut off)?;
            f(old_tid.page_id);
            f(new_tid.page_id);
        }
        HeapHotUpdate => {
            // Page-local HOT update: touches only the one page.
            let mut off = 0;
            f(decode_prefix::<PageId>(payload, &mut off)?);
        }
        HeapCleanup => {
            // Vacuum compaction (M3 Stage B): the compacted page, plus the
            // predecessor page when the record also unlinks the compacted
            // page from the chain (its `next_page` is rewritten). Only the
            // fixed-size page-id prefix is decoded — `dead_slots` is the
            // payload's LAST field by layout contract (see
            // `HeapCleanupRecord`) and is never read here. The unlink target
            // itself is not modified, so it is not tracked.
            let mut off = 0;
            let page = decode_prefix::<PageId>(payload, &mut off)?;
            let prev = decode_prefix::<PageId>(payload, &mut off)?;
            f(page);
            if prev != PageId::INVALID {
                f(prev);
            }
        }
        BTreeSplitPrepare => {
            // left_page, new_right_page. `left_old_next` is only *read* by
            // redo, never modified by the split.
            let mut off = 0;
            let left = decode_prefix::<PageId>(payload, &mut off)?;
            let new_right = decode_prefix::<PageId>(payload, &mut off)?;
            f(left);
            f(new_right);
        }
        BTreeSplitCopy => {
            let mut off = 0;
            let left = decode_prefix::<PageId>(payload, &mut off)?;
            let right = decode_prefix::<PageId>(payload, &mut off)?;
            f(left);
            f(right);
        }
        BTreeSplitCommit => {
            let mut off = 0;
            let left = decode_prefix::<PageId>(payload, &mut off)?;
            let right = decode_prefix::<PageId>(payload, &mut off)?;
            let parent = decode_prefix::<PageId>(payload, &mut off)?;
            f(left);
            f(right);
            f(parent);
        }
        BTreeSplitCLR => {
            // Prefix-decode only the fixed-size leading fields (post-Stage-S
            // fix B5): `separator_key` — the record's LAST field by layout
            // contract, see `BTreeSplitCLRRecord` — is a length-prefixed
            // `Vec<u8>`, and bincode's standard config has no size limit, so
            // full-decoding it here would trust a corrupt length prefix on a
            // CRC-valid record with an unbounded allocation. Analysis needs
            // only the page ids; the separator is never read.
            let mut off = 0;
            let left = decode_prefix::<PageId>(payload, &mut off)?;
            let right = decode_prefix::<PageId>(payload, &mut off)?;
            let _level = decode_prefix::<u8>(payload, &mut off)?;
            let _copy_start_slot = decode_prefix::<u16>(payload, &mut off)?;
            let _redo_ref_lsn = decode_prefix::<Lsn>(payload, &mut off)?;
            let parent = decode_prefix::<PageId>(payload, &mut off)?;
            let _parent_insert_slot = decode_prefix::<u16>(payload, &mut off)?;
            let new_root = decode_prefix::<PageId>(payload, &mut off)?;
            let meta = decode_prefix::<PageId>(payload, &mut off)?;
            f(left);
            f(right);
            if parent != PageId::INVALID {
                f(parent);
            }
            if new_root != PageId::INVALID {
                f(new_root);
            }
            if meta != PageId::INVALID {
                f(meta);
            }
        }
        _ => {}
    }
    Ok(())
}

/// Hard errors (hole detection, corrupt metadata, I/O failures) must propagate
/// to the caller, not be silently treated as end-of-WAL. Genuine tail truncation
/// (`WalCorrupted` — CRC mismatch / short read) is the only error class safe to
/// swallow. Everything else (including `WalReadFailed` for unknown discriminants
/// after valid CRC) propagates.
pub(crate) fn is_hard_error(e: &StorageError) -> bool {
    !matches!(e, StorageError::WalCorrupted(_))
}

/// Decode a single value from the payload prefix at `*offset`, advancing
/// the offset past it. `bincode::serde::decode_from_slice` does not require
/// the buffer to be fully consumed, so prefix decoding skips the bulk
/// fields (tuple bytes, page images) entirely.
fn decode_prefix<T: serde::de::DeserializeOwned>(payload: &[u8], offset: &mut usize) -> Result<T> {
    let (value, read) =
        bincode::serde::decode_from_slice::<T, _>(&payload[*offset..], bincode_config())
            .map_err(|e| StorageError::Serialize(e.to_string()))?;
    *offset += read;
    Ok(value)
}

/// Load the ATT snapshot referenced by `att_file` (§11.4). Empty path or an
/// unreadable file both degrade to an empty baseline, i.e. "rebuild by a
/// full WAL scan from the checkpoint LSN" — the v1 semantics.
fn load_att_snapshot(data_dir: &Path, att_file: &str) -> HashSet<TxnId> {
    if att_file.is_empty() {
        return HashSet::new();
    }
    match read_snapshot::<Vec<TxnId>>(&data_dir.join(att_file)) {
        Ok(xids) => xids.into_iter().collect(),
        Err(e) => {
            warn!(file = att_file, error = %e, "ATT snapshot unreadable; rebuilding from checkpoint LSN");
            HashSet::new()
        }
    }
}

/// Load the DPT snapshot referenced by `dpt_file` (§11.4), with the same
/// degradation semantics as [`load_att_snapshot`].
fn load_dpt_snapshot(data_dir: &Path, dpt_file: &str) -> HashMap<PageId, Lsn> {
    if dpt_file.is_empty() {
        return HashMap::new();
    }
    match read_snapshot::<Vec<(PageId, Lsn)>>(&data_dir.join(dpt_file)) {
        Ok(entries) => entries.into_iter().collect(),
        Err(e) => {
            warn!(file = dpt_file, error = %e, "DPT snapshot unreadable; rebuilding from checkpoint LSN");
            HashMap::new()
        }
    }
}

/// Read and bincode-decode a CRC32-guarded snapshot file written by the
/// checkpoint coordinator (`meta/att-*.snapshot` / `meta/dpt-*.snapshot`).
///
/// Format: `crc32(4) + body` where `body` is the bincode-encoded payload.
/// A CRC mismatch or truncated file returns an error so the caller degrades
/// to rebuilding from the checkpoint LSN.
fn read_snapshot<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T> {
    let bytes = std::fs::read(path).map_err(StorageError::Io)?;
    if bytes.len() < 4 {
        return Err(StorageError::MetadataCorrupted(format!(
            "snapshot file {} is too short for CRC prefix",
            path.display()
        )));
    }
    let stored_crc = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
    let body = &bytes[4..];
    let computed_crc = crc32fast::hash(body);
    if stored_crc != computed_crc {
        return Err(StorageError::MetadataCorrupted(format!(
            "snapshot file {} CRC mismatch: stored {stored_crc:#010x}, computed {computed_crc:#010x}",
            path.display()
        )));
    }
    Ok(bincode::serde::decode_from_slice(body, bincode_config())
        .map_err(|e| StorageError::Serialize(e.to_string()))?
        .0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::StorageConfig;
    use crate::wal::writer::WalWriter;
    use tempfile::TempDir;

    fn test_config(tmp: &TempDir) -> StorageConfig {
        let mut cfg = StorageConfig::new(tmp.path());
        cfg.wal_group_commit_timeout_ms = 1;
        cfg.wal_group_commit_batch_size = 1;
        cfg
    }

    /// A v1 (M1) CheckpointEnd payload: three fields, encoded like the M1
    /// struct — bincode encodes a tuple as its element sequence, identical
    /// to the struct's field sequence.
    fn v1_checkpoint_end_record(checkpoint_lsn: Lsn) -> WalRecord {
        let payload =
            bincode::serde::encode_to_vec((checkpoint_lsn, PageId(7), TxnId(3)), bincode_config())
                .unwrap();
        WalRecord {
            lsn: Lsn::INVALID,
            prev_lsn: Lsn::INVALID,
            txn_id: TxnId::INVALID,
            record_type: WalRecordType::CheckpointEnd,
            flags: 0,
            payload,
        }
    }

    fn v2_checkpoint_end_record(checkpoint_lsn: Lsn) -> WalRecord {
        WalRecord::checkpoint_end(
            checkpoint_lsn,
            PageId(7),
            TxnId(3),
            16_384,
            String::new(),
            String::new(),
        )
        .unwrap()
    }

    #[test]
    fn find_latest_returns_last_completed_checkpoint_end() {
        let tmp = TempDir::new().unwrap();
        let cfg = test_config(&tmp);
        let wal = WalWriter::open(tmp.path(), &cfg).unwrap();

        // First completed checkpoint.
        let begin1 = wal.append(WalRecord::checkpoint_begin()).unwrap();
        wal.append(v2_checkpoint_end_record(begin1)).unwrap();
        wal.append(WalRecord::page_alloc(PageId(1)).unwrap())
            .unwrap();
        // Second completed checkpoint.
        let begin2 = wal.append(WalRecord::checkpoint_begin()).unwrap();
        let end2 = wal.append(v2_checkpoint_end_record(begin2)).unwrap();
        // Dangling begin: crash mid-third-checkpoint.
        wal.append(WalRecord::checkpoint_begin()).unwrap();
        wal.flush().unwrap();
        drop(wal);

        let (end, end_lsn) =
            find_latest_checkpoint_end(&tmp.path().join("wal"), cfg.wal_segment_size, Lsn::FIRST)
                .unwrap()
                .expect("two CheckpointEnd records exist");
        assert_eq!(end.checkpoint_lsn, begin2);
        assert_eq!(end_lsn, end2);
    }

    #[test]
    fn find_latest_dispatches_v1_and_v2_by_flags() {
        let tmp = TempDir::new().unwrap();
        let cfg = test_config(&tmp);
        let wal = WalWriter::open(tmp.path(), &cfg).unwrap();

        let begin1 = wal.append(WalRecord::checkpoint_begin()).unwrap();
        wal.append(v1_checkpoint_end_record(begin1)).unwrap();
        let begin2 = wal.append(WalRecord::checkpoint_begin()).unwrap();
        wal.append(v2_checkpoint_end_record(begin2)).unwrap();
        wal.flush().unwrap();
        drop(wal);

        // Scan from the first begin: the v2 record is the latest.
        let (end, _) =
            find_latest_checkpoint_end(&tmp.path().join("wal"), cfg.wal_segment_size, begin1)
                .unwrap()
                .unwrap();
        assert_eq!(end.checkpoint_lsn, begin2);

        // Scan from the second begin: only the v2 record is visible.
        let (end, _) =
            find_latest_checkpoint_end(&tmp.path().join("wal"), cfg.wal_segment_size, begin2)
                .unwrap()
                .unwrap();
        assert_eq!(end.checkpoint_lsn, begin2);

        // Decode the v1 record directly to confirm the defaults path.
        let mut reader =
            WalReader::open_at(tmp.path().join("wal"), cfg.wal_segment_size, begin1).unwrap();
        let rec = reader.next_record().unwrap().unwrap();
        assert_eq!(rec.record_type, WalRecordType::CheckpointBegin);
        let rec = reader.next_record().unwrap().unwrap();
        assert_eq!(rec.flags, 0);
        let v1 = CheckpointEndRecord::decode(&rec.payload, rec.flags).unwrap();
        assert_eq!(v1.checkpoint_lsn, begin1);
        assert_eq!(v1.next_oid, crate::types::Oid::FIRST_USER.0);
        assert!(v1.att_file.is_empty());
        assert!(v1.dpt_file.is_empty());
    }

    #[test]
    fn analysis_rebuilds_att_and_dpt_with_empty_baseline() {
        let tmp = TempDir::new().unwrap();
        let cfg = test_config(&tmp);
        let wal = WalWriter::open(tmp.path(), &cfg).unwrap();

        let begin = wal.append(WalRecord::checkpoint_begin()).unwrap();
        wal.append(v2_checkpoint_end_record(begin)).unwrap();
        // XID 10: two page touches, never commits -> stays in the ATT.
        wal.append(WalRecord::heap_insert(PageId(5), 0, vec![1], TxnId(10)).unwrap())
            .unwrap();
        wal.append(WalRecord::heap_insert(PageId(5), 1, vec![2], TxnId(10)).unwrap())
            .unwrap();
        // XID 11: touches a page, then commits -> removed from the ATT.
        wal.append(WalRecord::heap_insert(PageId(6), 0, vec![3], TxnId(11)).unwrap())
            .unwrap();
        wal.append(WalRecord::txn_commit(TxnId(11)).unwrap())
            .unwrap();
        // Untransactional page record.
        wal.append(WalRecord::page_alloc(PageId(9)).unwrap())
            .unwrap();
        wal.flush().unwrap();
        drop(wal);

        let end = v2_decoded_at(&tmp, &cfg, begin);
        let result = run_analysis(tmp.path(), cfg.wal_segment_size, &end).unwrap();

        assert_eq!(result.redo_start, begin);
        assert_eq!(result.att, vec![TxnId(10)]);
        // Page 5 keeps the LSN of its FIRST dirtying record.
        assert_eq!(result.dpt.len(), 3);
        let page5: Vec<_> = result.dpt.iter().filter(|(p, _)| *p == PageId(5)).collect();
        assert_eq!(page5.len(), 1);
        // All rec_lsns come from the tail scan, so none predate the redo point.
        assert!(result.dpt.iter().all(|(_, lsn)| *lsn >= begin));
    }

    /// Read back the v2 CheckpointEnd record whose `checkpoint_lsn` equals
    /// `begin` and decode it.
    fn v2_decoded_at(tmp: &TempDir, cfg: &StorageConfig, begin: Lsn) -> CheckpointEndRecord {
        let (end, _) =
            find_latest_checkpoint_end(&tmp.path().join("wal"), cfg.wal_segment_size, begin)
                .unwrap()
                .unwrap();
        assert_eq!(end.checkpoint_lsn, begin);
        end
    }

    #[test]
    fn analysis_consumes_snapshot_baseline() {
        let tmp = TempDir::new().unwrap();
        let cfg = test_config(&tmp);
        let wal = WalWriter::open(tmp.path(), &cfg).unwrap();

        let begin = wal.append(WalRecord::checkpoint_begin()).unwrap();

        // Snapshot files as the checkpoint coordinator would have written
        // them: XIDs 41 and 42 active, page 3 dirty since before the begin.
        // Format: crc32(4) + body (matching write_snapshot_file).
        let att_body =
            bincode::serde::encode_to_vec(vec![TxnId(41), TxnId(42)], bincode_config()).unwrap();
        let pre_begin_rec_lsn = Lsn(8); // before the redo point
        let dpt_body =
            bincode::serde::encode_to_vec(vec![(PageId(3), pre_begin_rec_lsn)], bincode_config())
                .unwrap();
        let mut att_bytes = Vec::with_capacity(4 + att_body.len());
        att_bytes.extend_from_slice(&crc32fast::hash(&att_body).to_le_bytes());
        att_bytes.extend_from_slice(&att_body);
        let mut dpt_bytes = Vec::with_capacity(4 + dpt_body.len());
        dpt_bytes.extend_from_slice(&crc32fast::hash(&dpt_body).to_le_bytes());
        dpt_bytes.extend_from_slice(&dpt_body);
        let att_rel = format!("meta/att-{:016}.snapshot", begin.0);
        let dpt_rel = format!("meta/dpt-{:016}.snapshot", begin.0);
        crate::io::write_atomic(&tmp.path().join(&att_rel), &att_bytes).unwrap();
        crate::io::write_atomic(&tmp.path().join(&dpt_rel), &dpt_bytes).unwrap();

        let end_payload =
            WalRecord::checkpoint_end(begin, PageId(7), TxnId(3), 16_384, att_rel, dpt_rel)
                .unwrap();
        wal.append(end_payload).unwrap();
        // XID 41 commits after the checkpoint -> dropped from the baseline.
        wal.append(WalRecord::txn_commit(TxnId(41)).unwrap())
            .unwrap();
        wal.flush().unwrap();
        drop(wal);

        let end = v2_decoded_at(&tmp, &cfg, begin);
        let result = run_analysis(tmp.path(), cfg.wal_segment_size, &end).unwrap();

        assert_eq!(result.att, vec![TxnId(42)]);
        // The baseline DPT entry survives the scan...
        assert_eq!(result.dpt, vec![(PageId(3), pre_begin_rec_lsn)]);
        // ...but it does NOT pull redo_start before the redo point: the
        // completing checkpoint flushed page 3, and its segment may be
        // recycled (see run_analysis docs).
        assert_eq!(result.redo_start, begin);
    }

    #[test]
    fn analysis_treats_missing_snapshot_files_as_empty_baseline() {
        let tmp = TempDir::new().unwrap();
        let cfg = test_config(&tmp);
        let wal = WalWriter::open(tmp.path(), &cfg).unwrap();

        let begin = wal.append(WalRecord::checkpoint_begin()).unwrap();
        // Reference snapshot files that do not exist (lost/corrupt meta dir).
        let end_record = WalRecord::checkpoint_end(
            begin,
            PageId(7),
            TxnId(3),
            16_384,
            "meta/att-missing.snapshot".to_string(),
            "meta/dpt-missing.snapshot".to_string(),
        )
        .unwrap();
        wal.append(end_record).unwrap();
        wal.append(WalRecord::heap_insert(PageId(2), 0, vec![9], TxnId(77)).unwrap())
            .unwrap();
        wal.flush().unwrap();
        drop(wal);

        let end = v2_decoded_at(&tmp, &cfg, begin);
        let result = run_analysis(tmp.path(), cfg.wal_segment_size, &end).unwrap();

        // Full-rebuild fallback: the tail scan still finds everything.
        assert_eq!(result.att, vec![TxnId(77)]);
        assert_eq!(result.dpt.len(), 1);
        assert_eq!(result.redo_start, begin);
    }

    #[test]
    fn touched_pages_cover_every_page_modifying_type() {
        let cases: Vec<(WalRecord, Vec<PageId>)> = vec![
            (WalRecord::page_alloc(PageId(1)).unwrap(), vec![PageId(1)]),
            (WalRecord::page_free(PageId(2)).unwrap(), vec![PageId(2)]),
            (
                WalRecord::full_page_image(PageId(3), vec![0xAB; 64]).unwrap(),
                vec![PageId(3)],
            ),
            (
                WalRecord::heap_insert(PageId(4), 0, vec![1, 2, 3], TxnId(1)).unwrap(),
                vec![PageId(4)],
            ),
            (
                WalRecord::heap_update(
                    Tid {
                        page_id: PageId(5),
                        slot_id: 0,
                    },
                    Tid {
                        page_id: PageId(6),
                        slot_id: 1,
                    },
                    TxnId(1),
                    vec![4, 5],
                    TxnId(1),
                )
                .unwrap(),
                vec![PageId(5), PageId(6)],
            ),
            (
                WalRecord::heap_delete(
                    Tid {
                        page_id: PageId(7),
                        slot_id: 2,
                    },
                    TxnId(1),
                    TxnId(1),
                )
                .unwrap(),
                vec![PageId(7)],
            ),
            (
                WalRecord::btree_insert(PageId(8), 0, 0, 0, vec![1]).unwrap(),
                vec![PageId(8)],
            ),
            (
                WalRecord::btree_delete(PageId(9), 0).unwrap(),
                vec![PageId(9)],
            ),
            (
                WalRecord::btree_split_prepare(PageId(10), PageId(11), 0, PageId::INVALID, vec![9])
                    .unwrap(),
                vec![PageId(10), PageId(11)],
            ),
            (
                WalRecord::btree_split_copy(PageId(12), PageId(13), 0, Lsn(8)).unwrap(),
                vec![PageId(12), PageId(13)],
            ),
            (
                WalRecord::btree_split_commit(PageId(14), PageId(15), PageId(16), vec![7], 0)
                    .unwrap(),
                vec![PageId(14), PageId(15), PageId(16)],
            ),
            (
                WalRecord::heap_hot_update(PageId(17), 0, 1, vec![1, 2], TxnId(1), TxnId(1))
                    .unwrap(),
                vec![PageId(17)],
            ),
            (
                // Compaction-only: just the page itself.
                WalRecord::heap_cleanup(PageId(18), vec![1, 3], PageId::INVALID, PageId::INVALID)
                    .unwrap(),
                vec![PageId(18)],
            ),
            (
                // Compaction + unlink: the compacted page AND the predecessor
                // whose next_page is relinked. The relink target (19) is not
                // itself modified, so it is not tracked.
                WalRecord::heap_cleanup(PageId(20), vec![0], PageId(19), PageId(21)).unwrap(),
                vec![PageId(20), PageId(19)],
            ),
            // Non-page records touch nothing.
            (WalRecord::checkpoint_begin(), vec![]),
            (WalRecord::txn_commit(TxnId(1)).unwrap(), vec![]),
            (WalRecord::txn_abort(TxnId(1)).unwrap(), vec![]),
        ];

        for (mut record, expected) in cases {
            record.lsn = Lsn(64);
            let mut pages = Vec::new();
            for_each_touched_page(&record, &mut |p| pages.push(p)).unwrap();
            assert_eq!(pages, expected, "record type {:?}", record.record_type);
        }
    }

    /// Exhaustiveness guard (Stage N review, P3-1): every
    /// [`WalRecordType`] — present and FUTURE — must be explicitly
    /// classified as page-modifying (tracked in the DPT) or non-page
    /// (ignored by the DPT scan). The two sets are disjoint and their
    /// union is the full discriminant space, so adding a record type
    /// without updating this test fails it — the `_` arm of
    /// [`for_each_touched_page`] can never silently swallow a new
    /// page-modifying type.
    #[test]
    fn every_record_type_is_classified_for_the_dpt() {
        use WalRecordType::*;

        const PAGE_MODIFYING: &[WalRecordType] = &[
            PageAlloc,
            PageFree,
            FullPageImage,
            HeapInsert,
            HeapUpdate,
            HeapDelete,
            HeapCleanup,
            BTreeInsert,
            BTreeDelete,
            BTreeSplitPrepare,
            BTreeSplitCopy,
            BTreeSplitCommit,
            HeapHotUpdate,
            BTreeSplitCLR,
        ];
        // Transaction and checkpoint markers, and the Phase-2+ logical/
        // segment records: no pages are tracked for them. If any of these
        // gains a producer that modifies pages, move it to PAGE_MODIFYING and
        // extend `for_each_touched_page` — this test fails until you do.
        const NON_PAGE: &[WalRecordType] = &[
            TxnBegin,
            TxnCommit,
            TxnAbort,
            CheckpointBegin,
            CheckpointEnd,
            LogicalHnsw,
            LogicalInverted,
            LogicalGraph,
            LogicalTimeSeries,
            SegmentSeal,
            SegmentMerge,
        ];

        let mut all = std::collections::HashSet::new();
        for v in 0..=u8::MAX {
            let Ok(kind) = WalRecordType::from_u8(v) else {
                continue;
            };
            let modifying = PAGE_MODIFYING.contains(&kind);
            let non_page = NON_PAGE.contains(&kind);
            assert!(
                modifying ^ non_page,
                "{kind:?} must be classified in exactly one of PAGE_MODIFYING / NON_PAGE"
            );
            all.insert(kind);
        }
        assert_eq!(
            all.len(),
            PAGE_MODIFYING.len() + NON_PAGE.len(),
            "the two classification sets must partition every known record type"
        );
    }

    /// Post-Stage-S fix B5: analysis decodes only the CLR's fixed-size
    /// prefix. A payload truncated right after the last fixed field (the
    /// `separator_key` bytes missing entirely) must still yield every
    /// touched page, while a full decode of the same bytes fails — proving
    /// the analysis path never reads the variable-length tail.
    #[test]
    fn clr_analysis_decodes_only_the_fixed_prefix() {
        use crate::wal::record::BTreeSplitCLRRecord;

        let rec = BTreeSplitCLRRecord {
            left_page: PageId(10),
            right_page: PageId(11),
            level: 0,
            copy_start_slot: 113,
            redo_ref_lsn: Lsn(4_200),
            parent_page: PageId(12),
            parent_insert_slot: 57,
            new_root_page: PageId(13),
            meta_page: PageId(14),
            separator_key: vec![1, 2, 3, 4],
        };
        let mut record = WalRecord::btree_split_clr(&rec).unwrap();
        record.lsn = Lsn(64);

        // `separator_key` is the LAST field (layout contract on the struct),
        // so the fixed-size prefix is the payload minus the separator's own
        // encoding (length varint + bytes).
        let sep_len = bincode::serde::encode_to_vec(&rec.separator_key, bincode_config())
            .unwrap()
            .len();
        let prefix = record.payload[..record.payload.len() - sep_len].to_vec();
        assert!(BTreeSplitCLRRecord::decode(&prefix).is_err());
        record.payload = prefix;

        let mut pages = Vec::new();
        for_each_touched_page(&record, &mut |p| pages.push(p)).unwrap();
        assert_eq!(
            pages,
            vec![PageId(10), PageId(11), PageId(12), PageId(13), PageId(14)]
        );

        // INVALID sentinels are filtered, same as before the fix.
        let unlink = BTreeSplitCLRRecord {
            parent_page: PageId::INVALID,
            new_root_page: PageId::INVALID,
            meta_page: PageId::INVALID,
            separator_key: Vec::new(),
            ..rec
        };
        let mut record = WalRecord::btree_split_clr(&unlink).unwrap();
        record.lsn = Lsn(64);
        let mut pages = Vec::new();
        for_each_touched_page(&record, &mut |p| pages.push(p)).unwrap();
        assert_eq!(pages, vec![PageId(10), PageId(11)]);
    }

    #[test]
    fn analysis_scan_ignores_dangling_checkpoint_begin() {
        let tmp = TempDir::new().unwrap();
        let cfg = test_config(&tmp);
        let wal = WalWriter::open(tmp.path(), &cfg).unwrap();

        // Completed checkpoint, then a crash mid-next-checkpoint (begin only).
        let begin1 = wal.append(WalRecord::checkpoint_begin()).unwrap();
        wal.append(v2_checkpoint_end_record(begin1)).unwrap();
        wal.append(WalRecord::heap_insert(PageId(4), 0, vec![1], TxnId(50)).unwrap())
            .unwrap();
        wal.append(WalRecord::checkpoint_begin()).unwrap(); // never ended
        wal.flush().unwrap();
        drop(wal);

        let end = v2_decoded_at(&tmp, &cfg, begin1);
        let result = run_analysis(tmp.path(), cfg.wal_segment_size, &end).unwrap();
        assert_eq!(result.redo_start, begin1);
        assert_eq!(result.att, vec![TxnId(50)]);
        assert_eq!(result.dpt.len(), 1);
    }
}
