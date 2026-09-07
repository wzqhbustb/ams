//! fvecs/ivecs dataset parsing and recall scoring — Phase 2 M4 Stage D
//! local toolchain (coding plan 2026-09 Stage D "数据集工具链" + recall
//! harness rows).
//!
//! Why this lives inside the crate as a `pub` module (2026-09-03, Stage D
//! round 1): both the measurement probe (`examples/m4_recall_probe.rs`) and
//! the CI hard gate (`tests/recall_siftsmall.rs`) need the same parser and
//! the same scoring function, and this project's single-implementation
//! discipline (the Stage B "both sides share one function" lesson) forbids
//! two copies in `examples/` and `tests/`. Nothing here is on the
//! insert/search hot path — this is benchmark tooling that happens to share
//! the crate's error type.
//!
//! File format (ANN-benchmarks convention, ftp.irisa.fr texmex corpus): no
//! header, no record count — each record is a 4-byte little-endian i32
//! dimension followed by `dim` 4-byte LE values (f32 for `.fvecs`, i32 for
//! `.ivecs`). The record count is implicit: `file_len / record_size`.
//!
//! Error discipline (inherited from the Stage C "corrupted bytes must never
//! panic" contract): malformed files fail loudly through
//! [`HnswError::Corrupted`], filesystem failures through [`HnswError::Io`].
//! No new error variants were added for this module (2026-09-03, Stage D
//! round 1 — the contract pins the existing variant set).

use std::fs::File;
use std::io::{BufReader, ErrorKind, Read};
use std::path::Path;

use crate::error::{HnswError, Result};
use crate::params::NodeId;

/// Read a `.fvecs` file into one `Vec<f32>` per record (see module docs for
/// the format). Streaming via `BufReader` — the file is consumed record by
/// record, never slurped into one big buffer first — but the return value is
/// the materialized vector set (1M-scale streaming consumers are a separate
/// main-orchestrator evaluation, not this stage's scope).
///
/// Threat model (2026-09-07, Stage D review round 3 — same wording as
/// `snapshot::load`'s rustdoc): this is the UNLIMITED convenience wrapper
/// for trusted local benchmark files; it materializes the whole file with
/// no size budget. Untrusted sources must use [`read_fvecs_with_budget`].
///
/// # Errors
///
/// - [`HnswError::InvalidArgument`] — the path is not a regular file
///   (directory/FIFO/device/socket; 2026-09-07 round 4 P2, same gate as
///   `snapshot::load`).
/// - [`HnswError::Io`] — open/read filesystem failure.
/// - [`HnswError::Corrupted`] — file length not record-aligned, record dim
///   field `<= 0` or `> u16::MAX`, dim inconsistent across records,
///   truncation, or the file changing under read (TOCTOU shrink/growth).
pub fn read_fvecs(path: impl AsRef<Path>) -> Result<Vec<Vec<f32>>> {
    read_fvecs_with_budget(path, u64::MAX)
}

/// [`read_fvecs`] with a caller-supplied byte budget: if the file's
/// metadata length exceeds `max_file_bytes`, the read is rejected loudly
/// BEFORE any byte is read or buffer allocated (the budget is the first
/// gate, ahead of every format check — aligned with the
/// `snapshot::load`/`load_with_budget` dual-track precedent, 2026-09-07
/// Stage D review round 3). The u16 dim cap, alignment pre-checks, and
/// truncation detection below the budget gate are unchanged.
///
/// **Byte budget ≠ memory budget** (2026-09-07 Stage D review round 5,
/// same lesson as snapshot's `LoadBudget.max_memory_bytes`): the budget
/// bounds bytes READ, not the heap. The materialized `Vec<Vec<f32>>`
/// amplifies tiny-dim files through per-record `Vec` headers (24 B header
/// vs 4 B payload at dim = 1 — measured: a 64 MB dim=1 file peaks at
/// ~329 MB RSS, ≈ 5×). Worst case ≈ `file_len × (24 + 4·dim) / (8 +
/// 4·dim)`; at dim ≥ 128 (every benchmark dataset here) the ratio is
/// ~1.01×. A dedicated memory budget is a registered residual, not
/// implemented — size `max_file_bytes` with the amplification in mind.
///
/// # Errors
///
/// [`HnswError::InvalidArgument`] when the file exceeds the budget; all
/// [`read_fvecs`] errors otherwise.
pub fn read_fvecs_with_budget(
    path: impl AsRef<Path>,
    max_file_bytes: u64,
) -> Result<Vec<Vec<f32>>> {
    read_vecs(path.as_ref(), "fvecs", f32::from_le_bytes, max_file_bytes)
}

/// Read a `.ivecs` file into one `Vec<i32>` per record. Same format, error
/// discipline, and trusted-local-only threat model as [`read_fvecs`];
/// untrusted sources must use [`read_ivecs_with_budget`].
pub fn read_ivecs(path: impl AsRef<Path>) -> Result<Vec<Vec<i32>>> {
    read_ivecs_with_budget(path, u64::MAX)
}

/// [`read_ivecs`] with a caller-supplied byte budget — see
/// [`read_fvecs_with_budget`].
pub fn read_ivecs_with_budget(
    path: impl AsRef<Path>,
    max_file_bytes: u64,
) -> Result<Vec<Vec<i32>>> {
    read_vecs(path.as_ref(), "ivecs", i32::from_le_bytes, max_file_bytes)
}

/// Shared streaming reader behind the four public entry points — one
/// implementation, two value decoders, one budget parameter
/// (single-implementation discipline).
fn read_vecs<T>(
    path: &Path,
    kind: &str,
    parse: fn([u8; 4]) -> T,
    max_file_bytes: u64,
) -> Result<Vec<Vec<T>>> {
    let file = File::open(path)?;
    let md = file.metadata()?;
    // Regular-file gate (2026-09-07, Stage D review round 4 P2 — aligned
    // with snapshot.rs's round-5 gate, same "regular file" wording): FIFOs,
    // devices and sockets have no trustworthy length. Pre-fix, /dev/null
    // and /dev/zero (metadata length 0) fell through to the file_len == 0
    // branch and were silently accepted as an EMPTY dataset, and opening a
    // FIFO blocked in File::open until a writer appeared; directories
    // reach this gate on platforms where open(O_RDONLY) on a directory
    // succeeds. The gate lives HERE, not in read_vecs_stream: the
    // Cursor-injected test path has no file at all, so the stream core
    // cannot check it.
    if !md.is_file() {
        return Err(HnswError::InvalidArgument(format!(
            "{kind} path {} is not a regular file (FIFOs/devices/sockets have no trustworthy length)",
            path.display()
        )));
    }
    let file_len = md.len();
    read_vecs_stream(
        BufReader::new(file),
        file_len,
        max_file_bytes,
        kind,
        &path.display().to_string(),
        parse,
    )
}

/// Stream core of [`read_vecs`], split out so tests can drive it with an
/// in-memory reader whose DECLARED length disagrees with the actual bytes —
/// the deterministic stand-in for "file shrank between metadata() and read"
/// (2026-09-03, Stage D review round 2 P2) and for oversized files without
/// building multi-GB fixtures (round 3 budget gate). `file_len` is the
/// metadata length.
fn read_vecs_stream<T, R: Read>(
    reader: R,
    file_len: u64,
    max_file_bytes: u64,
    kind: &str,
    display: &str,
    parse: fn([u8; 4]) -> T,
) -> Result<Vec<Vec<T>>> {
    // Budget gate — the FIRST check, before any read or allocation
    // (2026-09-07, Stage D review round 3 P2): the round-2 u16 dim cap only
    // bounds a single record; a legally-shaped but huge file would still
    // materialize an unbounded Vec<Vec<T>>. InvalidArgument (not Corrupted):
    // the file may be perfectly valid — it is the caller's budget that
    // forbids it, same classification as snapshot::load_with_budget.
    if file_len > max_file_bytes {
        return Err(HnswError::InvalidArgument(format!(
            "{kind} file {display} is {file_len} bytes, exceeding the caller budget of {max_file_bytes} bytes"
        )));
    }

    // Cheap pre-check before any allocation: every record starts with a
    // 4-byte dim field, so a length not divisible by 4 cannot be aligned.
    // (2026-09-03, Stage D round 1: a byte-shifted file would otherwise
    // parse garbage dims deep into the stream before failing.)
    if file_len % 4 != 0 {
        return Err(HnswError::Corrupted(format!(
            "{kind} file {display} has {file_len} bytes, not a multiple of the 4-byte record alignment"
        )));
    }
    if file_len == 0 {
        // Zero records is a legal (if useless) dataset; the modulo check
        // above already holds vacuously.
        return Ok(Vec::new());
    }

    let mut out: Vec<Vec<T>> = Vec::new();
    let mut expected_dim: Option<usize> = None;
    // Exact record count implied by the metadata length once the first
    // record pins the geometry — the anti-truncation gate (see the
    // post-loop check below).
    let mut expected_records: u64 = 0;

    // Hard read cap (2026-09-07, Stage D review round 4 P1 — aligned with
    // snapshot.rs's `take()` precedent): every read goes through this
    // capped reader, so it is PHYSICALLY impossible to read past
    // min(max_file_bytes, file_len) + 1 no matter how the file mutates
    // under us. (The budget gate already rejected budget < file_len, so in
    // practice cap == file_len + 1; the min keeps the budget semantics
    // complete. saturating_add: file_len == u64::MAX must not wrap.) RSS
    // is thereby bounded by min(budget, file_len) + one record + buffers —
    // the round-3 budget gate alone only bounded the METADATA length, and
    // a file grown mid-read (40MB budget observed at 48MB, RSS 292MB in
    // the review probe) blew past it. The "+1" byte is the growth probe's
    // allowance (see post-loop).
    let cap = max_file_bytes.min(file_len).saturating_add(1);
    let mut capped = reader.take(cap);

    loop {
        let mut dim_buf = [0u8; 4];
        // Counted read instead of read_exact: we must DISTINGUISH clean EOF
        // (0 bytes) from a partial dim field (1..=3 bytes). A partial dim
        // read means the stream ended mid-field — under the take cap that
        // is exactly what a file GROWN past its metadata length looks like
        // (the cap lets precisely one extra byte through), or a mid-record
        // shrink; either way the file changed under read and must fail
        // loudly (round 4 P1).
        let filled = read_counted(&mut capped, &mut dim_buf).map_err(HnswError::Io)?;
        match filled {
            0 => {
                // Clean EOF on a record boundary ends the stream. A
                // premature CLEAN boundary EOF (file truncated by a whole
                // number of records after metadata) is caught by the
                // expected-records check after the loop — 2026-09-03 Stage
                // D review round 2 P2: the pre-fix code returned the
                // surviving prefix as a successful read.
                break;
            }
            4 => {}
            _ => {
                return Err(HnswError::Corrupted(format!(
                    "{kind} file {display} record {} has a partial dim field ({filled} of 4 bytes) — file changed under read",
                    out.len()
                )));
            }
        }
        let dim = i32::from_le_bytes(dim_buf);
        if dim <= 0 {
            return Err(HnswError::Corrupted(format!(
                "{kind} file {display} record {} has non-positive dim {dim}",
                out.len()
            )));
        }
        // Budget gate BEFORE any dim-sized allocation (2026-09-03, Stage D
        // review round 2 P1-3): `Hnsw`'s dim is a u16, so no dataset this
        // parser serves can be wider; a forged `dim = i32::MAX` header
        // paired with an 8GB sparse file would otherwise pass the
        // record-size modulo check below and allocate ~8GB for one record.
        // Deliberately placed BEFORE the modulo check so the rejection —
        // and its negative test — needs no multi-GB file.
        if dim > i32::from(u16::MAX) {
            return Err(HnswError::Corrupted(format!(
                "{kind} file {display} record {} has dim {dim} > u16::MAX (Hnsw dims are u16; forged header rejected before allocation)",
                out.len()
            )));
        }
        let dim = dim as usize;

        match expected_dim {
            None => {
                // First record pins the geometry: with a known record size
                // we can reject misaligned files BEFORE allocating per-record
                // buffers — this is also the allocation guard, since
                // file_len % record_size == 0 with file_len > 0 implies
                // record_size <= file_len (2026-09-03, Stage D round 1 —
                // Stage C forged-header lesson applied; the residual gap it
                // left, dim-sized single-record allocation, is closed by the
                // u16 budget gate above).
                let record_size = 4u64 + 4 * dim as u64;
                if file_len % record_size != 0 {
                    return Err(HnswError::Corrupted(format!(
                        "{kind} file {display} has {file_len} bytes, not a multiple of record size {record_size} (dim {dim})"
                    )));
                }
                expected_records = file_len / record_size;
                expected_dim = Some(dim);
            }
            Some(expected) if expected != dim => {
                // The dim field is re-read and re-validated per record (the
                // contract's "逐条重读 dim 字段校验"): a mid-file corruption
                // must fail loudly, not silently shift the shape.
                return Err(HnswError::Corrupted(format!(
                    "{kind} file {display} record {} has dim {dim}, expected {expected} (dims must be uniform)",
                    out.len()
                )));
            }
            Some(_) => {}
        }

        let mut buf = vec![0u8; 4 * dim];
        let filled = read_counted(&mut capped, &mut buf).map_err(HnswError::Io)?;
        if filled != buf.len() {
            return Err(HnswError::Corrupted(format!(
                "{kind} file {display} record {} truncated after its dim field ({filled} of {} value bytes) — file changed under read",
                out.len(),
                buf.len()
            )));
        }
        out.push(
            buf.chunks_exact(4)
                .map(|c| parse([c[0], c[1], c[2], c[3]]))
                .collect(),
        );
    }

    // Growth probe (2026-09-07, Stage D review round 4 P1): the loop ended
    // at a clean EOF — try one more byte through the capped reader (the
    // "+1" allowance). If a byte comes back, the file grew after
    // metadata() by an exact record boundary, which the expected-records
    // check below CANNOT see (the counts match). Dataset files carry no
    // CRC — this probe plus the record-count check is the integrity floor.
    // With an honest contiguous reader, growth is usually caught earlier
    // by the partial-dim-field path (the cap passes exactly one extra byte
    // into the next dim read); this probe pins the behavior for Read impls
    // that report a spurious early Ok(0) (legal — Ok(0) is a hint, not a
    // guarantee).
    let mut probe = [0u8; 1];
    match capped.read(&mut probe) {
        Ok(0) => {}
        Ok(_) => {
            return Err(HnswError::Corrupted(format!(
                "{kind} file {display} grew after metadata (more than {file_len} bytes readable)"
            )));
        }
        Err(e) => return Err(HnswError::Io(e)),
    }

    // Anti-truncation gate (2026-09-03, Stage D review round 2 P2): the
    // pre-checks pinned file_len to an exact record count, so the stream
    // MUST yield exactly that many records. Fewer means the file was
    // truncated by whole records after metadata() (concurrent writer /
    // TOCTOU); growth is caught above (partial dim field / growth probe).
    // Returning a silent prefix would feed the recall harness a short base
    // set with a full-size ground truth — precisely the "valid but wrong"
    // outcome Stage C's CRC exists to prevent on the snapshot side.
    if out.len() as u64 != expected_records {
        return Err(HnswError::Corrupted(format!(
            "{kind} file {display} yielded {} records, expected {expected_records} from metadata length {file_len} (file shrank after metadata)",
            out.len()
        )));
    }
    Ok(out)
}

/// `read_exact` with byte counting (2026-09-07, Stage D review round 4 P1):
/// returns the number of bytes read, short only at stream end, so callers
/// can distinguish clean EOF (0) from a torn field (1..buf.len()-1) — the
/// distinction the `take()`-capped TOCTOU detection depends on. Retries on
/// `Interrupted`, like `read_exact`.
fn read_counted<R: Read>(reader: &mut R, buf: &mut [u8]) -> std::io::Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        match reader.read(&mut buf[filled..]) {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(ref e) if e.kind() == ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(filled)
}

/// recall@k scored by set intersection (coding plan Stage D ground-truth
/// contract: **ivecs original order, first k, NOT re-sorted** — a hard
/// constraint, not a preference).
///
/// For each query, the ground-truth set is the literal first `k` entries of
/// its ivecs row and the retrieved set is the first `k` returned [`NodeId`]s;
/// the per-query score is `|intersection| / k` and the result is the mean
/// over all queries.
///
/// Tie handling (2026-09-03, Stage D round 1 — the contract asks for this to
/// be written down): if the k-th and (k+1)-th ground-truth entries are
/// equidistant to the query, the gt set is NOT expanded to admit both — the
/// ivecs row's own order is the referee, per the hard constraint above. On
/// our side, [`crate::Hnsw::search`] breaks distance ties by ascending
/// `NodeId`, so the retrieved set is deterministic and the comparison is
/// reproducible bit-for-bit.
///
/// Intersection is counted with sort + two pointers, not a `HashSet`: the
/// crate guardrail grep (`HashMap`/`rand`/`parking_lot` zero-hit discipline,
/// Stage C) extends in spirit to hashing containers, and at k ~ 10 the
/// sort is cheaper than hashing anyway.
///
/// # Errors
///
/// [`HnswError::InvalidArgument`] (all messages carry the query index) when
/// `k == 0`, the query counts disagree, there are no queries, or — per row —
/// any integrity check fails (2026-09-03, Stage D review round 2 P2: a
/// corrupt input must fail loudly, never silently lower the score):
///
/// - a ground-truth row has fewer than `k` entries, holds a negative id, an
///   id `>= base_count` (an out-of-range gt id would silently score as a
///   miss), or a duplicate id within its first `k` (the gt set is a SET;
///   duplicates would double-count under intersection scoring);
/// - a retrieved row has fewer than `k` entries (silently scoring a short
///   row would under-count), an id `>= base_count`, or a duplicate id.
pub fn recall_at_k(
    ground_truth: &[Vec<i32>],
    retrieved: &[Vec<NodeId>],
    k: usize,
    base_count: u32,
) -> Result<f64> {
    if k == 0 {
        return Err(HnswError::InvalidArgument(
            "recall@k needs k >= 1".to_string(),
        ));
    }
    if ground_truth.len() != retrieved.len() {
        return Err(HnswError::InvalidArgument(format!(
            "ground-truth rows ({}) != retrieved result sets ({})",
            ground_truth.len(),
            retrieved.len()
        )));
    }
    if ground_truth.is_empty() {
        return Err(HnswError::InvalidArgument(
            "recall@k needs at least one query".to_string(),
        ));
    }
    let mut hits = 0usize;
    for (q, (gt_row, ret_row)) in ground_truth.iter().zip(retrieved).enumerate() {
        if gt_row.len() < k {
            return Err(HnswError::InvalidArgument(format!(
                "query {q}: ground-truth row has {} entries < k = {k}",
                gt_row.len()
            )));
        }
        let mut gt_sorted: Vec<u32> = Vec::with_capacity(k);
        for &g in &gt_row[..k] {
            // 2026-09-03 Stage D review: compare in u32, not i32 — casting
            // the retrieved NodeId `as i32` would silently wrap at N >= 2^31
            // (today's datasets are 1M-scale, far below; the boundary must
            // still fail loud rather than corrupt the count). The mirror
            // conversion is i32 -> u32, which rejects a negative ground-truth
            // id loudly instead (a corrupt gt must never lower the score
            // silently).
            let id = u32::try_from(g).map_err(|_| {
                HnswError::InvalidArgument(format!(
                    "query {q}: ground-truth row holds a negative id {g} — corrupt ground truth"
                ))
            })?;
            if id >= base_count {
                return Err(HnswError::InvalidArgument(format!(
                    "query {q}: ground-truth id {id} >= base_count {base_count} — corrupt ground truth"
                )));
            }
            gt_sorted.push(id);
        }
        gt_sorted.sort_unstable();
        if gt_sorted.windows(2).any(|w| w[0] == w[1]) {
            return Err(HnswError::InvalidArgument(format!(
                "query {q}: ground-truth row has a duplicate id within its first {k} — the gt set must be a set"
            )));
        }
        if ret_row.len() < k {
            return Err(HnswError::InvalidArgument(format!(
                "query {q}: retrieved row has {} entries < k = {k}",
                ret_row.len()
            )));
        }
        let mut ret_sorted: Vec<u32> = Vec::with_capacity(k);
        for id in ret_row.iter().take(k) {
            if id.0 >= base_count {
                return Err(HnswError::InvalidArgument(format!(
                    "query {q}: retrieved id {} >= base_count {base_count}",
                    id.0
                )));
            }
            ret_sorted.push(id.0);
        }
        ret_sorted.sort_unstable();
        if ret_sorted.windows(2).any(|w| w[0] == w[1]) {
            return Err(HnswError::InvalidArgument(format!(
                "query {q}: retrieved row has a duplicate id within its first {k}"
            )));
        }
        // Two-pointer intersection over sorted slices. Both sides are
        // duplicate-free (enforced above), so no dedup pass is needed.
        let (mut i, mut j) = (0, 0);
        while i < gt_sorted.len() && j < ret_sorted.len() {
            let g = gt_sorted[i];
            let r = ret_sorted[j];
            match g.cmp(&r) {
                std::cmp::Ordering::Less => i += 1,
                std::cmp::Ordering::Greater => j += 1,
                std::cmp::Ordering::Equal => {
                    hits += 1;
                    i += 1;
                    j += 1;
                }
            }
        }
    }
    Ok(hits as f64 / (ground_truth.len() * k) as f64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// 2026-09-03 Stage D review round 2 nano-1: pid + length alone collides
    /// when parallel tests write same-length files; the process-local counter
    /// follows snapshot.rs's `.tmp-<pid>-<counter>` precedent.
    static TMP_COUNTER: AtomicUsize = AtomicUsize::new(0);

    fn write_tmp(bytes: &[u8]) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "pg_am_hnsw_dataset_test-{}-{}-{}.bin",
            std::process::id(),
            TMP_COUNTER.fetch_add(1, Ordering::Relaxed),
            bytes.len()
        ));
        File::create(&path).unwrap().write_all(bytes).unwrap();
        path
    }

    fn fvecs_bytes(records: &[&[f32]]) -> Vec<u8> {
        let mut v = Vec::new();
        for r in records {
            v.extend_from_slice(&(r.len() as i32).to_le_bytes());
            for x in *r {
                v.extend_from_slice(&x.to_le_bytes());
            }
        }
        v
    }

    #[test]
    fn roundtrip_fvecs_and_ivecs() {
        let path = write_tmp(&fvecs_bytes(&[&[1.0, 2.0], &[3.0, 4.0]]));
        let got = read_fvecs(&path).unwrap();
        assert_eq!(got, vec![vec![1.0, 2.0], vec![3.0, 4.0]]);
        std::fs::remove_file(&path).unwrap();

        let mut iv = Vec::new();
        iv.extend_from_slice(&2i32.to_le_bytes());
        iv.extend_from_slice(&7i32.to_le_bytes());
        iv.extend_from_slice(&(-3i32).to_le_bytes());
        let path = write_tmp(&iv);
        assert_eq!(read_ivecs(&path).unwrap(), vec![vec![7, -3]]);
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn bad_files_fail_loudly() {
        // Not 4-byte aligned.
        let path = write_tmp(&[0u8; 6]);
        assert!(matches!(read_fvecs(&path), Err(HnswError::Corrupted(_))));
        std::fs::remove_file(&path).unwrap();

        // dim <= 0.
        let path = write_tmp(&0i32.to_le_bytes());
        assert!(matches!(read_fvecs(&path), Err(HnswError::Corrupted(_))));
        std::fs::remove_file(&path).unwrap();

        // Length not a multiple of the first record's size.
        let mut b = fvecs_bytes(&[&[1.0, 2.0]]);
        b.extend_from_slice(&[0u8; 4]);
        let path = write_tmp(&b);
        assert!(matches!(read_fvecs(&path), Err(HnswError::Corrupted(_))));
        std::fs::remove_file(&path).unwrap();

        // Mid-file dim change, with total length kept a multiple of the
        // FIRST record's size (12 + 4 + 8 = 24, 24 % 12 == 0) so the
        // record-size pre-check passes and the per-record uniform-dim check
        // is the one that fires.
        let mut b = fvecs_bytes(&[&[1.0, 2.0]]);
        b.extend_from_slice(&3i32.to_le_bytes());
        b.extend_from_slice(&[0u8; 8]);
        let path = write_tmp(&b);
        assert!(matches!(read_fvecs(&path), Err(HnswError::Corrupted(_))));
        std::fs::remove_file(&path).unwrap();

        // Missing file is Io, not Corrupted.
        let missing = std::env::temp_dir().join("pg_am_hnsw_dataset_test-nonexistent.bin");
        assert!(matches!(read_fvecs(&missing), Err(HnswError::Io(_))));
    }

    /// 2026-09-03 Stage D review round 2 P1-3: a forged dim = i32::MAX
    /// header must be rejected by the u16 budget gate BEFORE the record-size
    /// modulo check — so this test needs only 8 bytes, not an aligned 8GB
    /// sparse file (the gate's placement ahead of the modulo check is what
    /// makes the cheap negative test possible; keep that order).
    #[test]
    fn forged_huge_dim_rejected_before_allocation() {
        let mut b = i32::MAX.to_le_bytes().to_vec();
        b.extend_from_slice(&[0u8; 4]); // keep the file 4-byte aligned
        let path = write_tmp(&b);
        let err = read_fvecs(&path).unwrap_err();
        assert!(
            matches!(&err, HnswError::Corrupted(m) if m.contains("u16::MAX")),
            "unexpected error: {err}"
        );
        std::fs::remove_file(&path).unwrap();

        // Boundary pins: u16::MAX itself is legal (fails later, on the
        // modulo check, for this short file), u16::MAX + 1 is not.
        let dim = i32::from(u16::MAX) + 1;
        let mut b = dim.to_le_bytes().to_vec();
        b.extend_from_slice(&[0u8; 4]);
        let path = write_tmp(&b);
        assert!(matches!(read_fvecs(&path), Err(HnswError::Corrupted(_))));
        std::fs::remove_file(&path).unwrap();
    }

    /// 2026-09-03 Stage D review round 2 P2: metadata says 2 records, the
    /// stream delivers 1 (file truncated by a whole record after metadata —
    /// the length stays record-aligned, so every pre-check passes; only the
    /// expected-record-count gate catches it). Driven deterministically
    /// through read_vecs_stream with a declared length larger than the
    /// cursor's content.
    #[test]
    fn whole_record_truncation_after_metadata_is_rejected() {
        let two = fvecs_bytes(&[&[1.0, 2.0], &[3.0, 4.0]]);
        let one = &two[..12]; // one full record, still record-aligned
        let err = read_vecs_stream(
            std::io::Cursor::new(one),
            two.len() as u64, // metadata saw BOTH records
            u64::MAX,         // unlimited budget
            "fvecs",
            "cursor",
            f32::from_le_bytes,
        )
        .unwrap_err();
        assert!(
            matches!(&err, HnswError::Corrupted(m) if m.contains("shrank")),
            "unexpected error: {err}"
        );

        // Mirror direction: the file holds MORE bytes than the metadata
        // count implies. 2026-09-07 round 4 P1: with the take() cap at
        // file_len + 1, exactly one extra byte leaks into the next dim
        // read, so growth is now caught as a partial dim field (the
        // round-2 "more records than expected" path is unreachable under
        // the cap — the surplus records are never read).
        let err = read_vecs_stream(
            std::io::Cursor::new(&two[..]),
            12,       // metadata saw ONE record
            u64::MAX, // unlimited budget
            "fvecs",
            "cursor",
            f32::from_le_bytes,
        )
        .unwrap_err();
        assert!(
            matches!(&err, HnswError::Corrupted(m) if m.contains("partial dim field")),
            "unexpected error: {err}"
        );

        // And the honest case still passes through the same gate.
        let got = read_vecs_stream(
            std::io::Cursor::new(&two[..]),
            two.len() as u64,
            u64::MAX,
            "fvecs",
            "cursor",
            f32::from_le_bytes,
        )
        .unwrap();
        assert_eq!(got, vec![vec![1.0, 2.0], vec![3.0, 4.0]]);
    }

    /// 2026-09-07 Stage D review round 4 P1: the post-loop growth probe is
    /// pinned with a reader that reports a spurious clean EOF once and then
    /// "grows" by a byte — legal per Read's contract (Ok(0) is a hint, not
    /// a guarantee). With an honest contiguous reader the same growth is
    /// caught one step earlier by the partial-dim-field path (see
    /// whole_record_truncation_after_metadata_is_rejected).
    #[test]
    fn growth_after_clean_eof_is_rejected_by_the_probe() {
        struct GrowAfterFirstEof {
            prefix: std::io::Cursor<Vec<u8>>,
            tail: std::io::Cursor<Vec<u8>>,
            eof_reported: bool,
        }
        impl Read for GrowAfterFirstEof {
            fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                let n = self.prefix.read(buf)?;
                if n == 0 && !self.eof_reported {
                    self.eof_reported = true;
                    return Ok(0);
                }
                if n == 0 {
                    return self.tail.read(buf);
                }
                Ok(n)
            }
        }

        let one_record = fvecs_bytes(&[&[1.0, 2.0]]); // 12 bytes
        let reader = GrowAfterFirstEof {
            prefix: std::io::Cursor::new(one_record.clone()),
            tail: std::io::Cursor::new(vec![0u8]),
            eof_reported: false,
        };
        let err = read_vecs_stream(
            reader,
            one_record.len() as u64,
            u64::MAX,
            "fvecs",
            "grow-after-eof",
            f32::from_le_bytes,
        )
        .unwrap_err();
        assert!(
            matches!(&err, HnswError::Corrupted(m) if m.contains("grew after metadata")),
            "unexpected error: {err}"
        );
    }

    /// 2026-09-07 Stage D review round 4 P1: the take() cap makes it
    /// PHYSICALLY impossible to read past file_len + 1 — pinned against an
    /// infinite record stream with a byte-accurate counter. Pre-fix this
    /// reader would have looped (and materialized records) forever.
    #[test]
    fn infinite_source_is_capped_at_metadata_length_plus_one() {
        use std::sync::Arc;
        struct InfiniteRecords {
            record: [u8; 12], // dim = 2, two zero values
            pos: usize,
            bytes_read: Arc<AtomicUsize>,
        }
        impl Read for InfiniteRecords {
            fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                for (i, b) in buf.iter_mut().enumerate() {
                    *b = self.record[(self.pos + i) % 12];
                }
                self.pos = (self.pos + buf.len()) % 12;
                self.bytes_read.fetch_add(buf.len(), Ordering::Relaxed);
                Ok(buf.len())
            }
        }

        let mut record = [0u8; 12];
        record[..4].copy_from_slice(&2i32.to_le_bytes());
        let counter = Arc::new(AtomicUsize::new(0));
        let reader = InfiniteRecords {
            record,
            pos: 0,
            bytes_read: Arc::clone(&counter),
        };
        let declared_len = 12u64 * 100_000; // 100k records "at metadata time"
        let err = read_vecs_stream(
            reader,
            declared_len,
            u64::MAX,
            "fvecs",
            "infinite",
            f32::from_le_bytes,
        )
        .unwrap_err();
        // The 100_001st dim read gets exactly one byte (cap = len + 1) ->
        // partial dim field, loud Corrupted.
        assert!(
            matches!(&err, HnswError::Corrupted(m) if m.contains("partial dim field")),
            "unexpected error: {err}"
        );
        assert_eq!(
            counter.load(Ordering::Relaxed) as u64,
            declared_len + 1,
            "the take() cap must stop the read at exactly file_len + 1"
        );
    }

    /// 2026-09-07 Stage D review round 4 P2: non-regular files are rejected
    /// by the is_file gate (aligned with snapshot_roundtrip.rs's fifo test:
    /// a writer thread completes the open handshake so File::open does not
    /// block). Pre-fix, /dev/null and /dev/zero (metadata length 0) were
    /// silently accepted as an EMPTY dataset via the file_len == 0 branch.
    #[cfg(unix)]
    #[test]
    fn non_regular_files_are_rejected() {
        // FIFO.
        let fifo = std::env::temp_dir().join(format!(
            "pg_am_hnsw_dataset_test-{}-{}.fifo",
            std::process::id(),
            TMP_COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        let status = std::process::Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .unwrap();
        assert!(status.success(), "mkfifo failed");
        let fifo_w = fifo.clone();
        let writer = std::thread::spawn(move || {
            // Opening a FIFO for writing blocks until a reader opens it —
            // read_fvecs's File::open completes the pair; after the gate
            // rejects and closes its end, the write fails with EPIPE (the
            // Rust runtime ignores SIGPIPE).
            let _ = File::create(&fifo_w).map(|mut f| f.write_all(&[0u8; 16]));
        });
        let err = read_fvecs(&fifo).unwrap_err();
        assert!(
            matches!(&err, HnswError::InvalidArgument(m) if m.contains("regular file")),
            "unexpected error: {err}"
        );
        writer.join().unwrap();
        std::fs::remove_file(&fifo).unwrap();

        // Character devices: /dev/null and /dev/zero must NOT come back as
        // Ok(empty) — that was the round-4 P2 hole.
        for dev in ["/dev/null", "/dev/zero"] {
            let err = read_fvecs(dev).unwrap_err();
            assert!(
                matches!(&err, HnswError::InvalidArgument(m) if m.contains("regular file")),
                "{dev}: unexpected error: {err}"
            );
        }

        // Directory (open succeeds on macOS/Linux; the gate fires before
        // any read, so no EISDIR-at-read classification question arises).
        let err = read_fvecs(std::env::temp_dir()).unwrap_err();
        assert!(
            matches!(&err, HnswError::InvalidArgument(m) if m.contains("regular file")),
            "directory: unexpected error: {err}"
        );
    }

    /// 2026-09-07 Stage D review round 3 P2: the byte-budget gate is the
    /// FIRST check — a legally-shaped but oversized file must be rejected
    /// with InvalidArgument before any read or allocation. Boundaries are
    /// pinned exactly (== budget passes, budget - 1 byte fails); the
    /// oversized case is driven through read_vecs_stream with a huge
    /// DECLARED length so no multi-GB fixture is needed.
    #[test]
    fn budget_gate_rejects_oversized_files_before_reading() {
        let bytes = fvecs_bytes(&[&[1.0, 2.0], &[3.0, 4.0]]);
        let len = bytes.len() as u64;
        let path = write_tmp(&bytes);

        // Exactly at budget -> ok; one byte under -> InvalidArgument.
        let got = read_fvecs_with_budget(&path, len).unwrap();
        assert_eq!(got, vec![vec![1.0, 2.0], vec![3.0, 4.0]]);
        let err = read_fvecs_with_budget(&path, len - 1).unwrap_err();
        assert!(
            matches!(&err, HnswError::InvalidArgument(m) if m.contains("budget")),
            "unexpected error: {err}"
        );
        // Same dual track for ivecs.
        let mut iv = Vec::new();
        iv.extend_from_slice(&1i32.to_le_bytes());
        iv.extend_from_slice(&7i32.to_le_bytes());
        let ipath = write_tmp(&iv);
        let got = read_ivecs_with_budget(&ipath, iv.len() as u64).unwrap();
        assert_eq!(got, vec![vec![7]]);
        assert!(matches!(
            read_ivecs_with_budget(&ipath, iv.len() as u64 - 1),
            Err(HnswError::InvalidArgument(_))
        ));
        std::fs::remove_file(&path).unwrap();
        std::fs::remove_file(&ipath).unwrap();

        // Forged oversized file via declared length: 1 TiB "metadata", 1 GB
        // budget — deterministic, no fixture. (The declared length is
        // 4-aligned so only the budget gate can fire.)
        let err = read_vecs_stream(
            std::io::Cursor::new(&bytes[..]),
            1u64 << 40,
            1u64 << 30,
            "fvecs",
            "cursor",
            f32::from_le_bytes,
        )
        .unwrap_err();
        assert!(
            matches!(&err, HnswError::InvalidArgument(m) if m.contains("1099511627776") && m.contains("1073741824")),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn recall_scoring_known_answers() {
        let gt = vec![vec![10, 20, 30, 40]];
        let ret = vec![vec![NodeId(20), NodeId(10), NodeId(99)]];
        // gt prefix {10, 20, 30}; retrieved {20, 10, 99} -> 2/3 hits.
        let r = recall_at_k(&gt, &ret, 3, 1000).unwrap();
        assert!((r - 2.0 / 3.0).abs() < 1e-12);

        // Perfect and zero scores.
        let ret = vec![vec![NodeId(10), NodeId(20), NodeId(30)]];
        assert_eq!(recall_at_k(&gt, &ret, 3, 1000).unwrap(), 1.0);
        let ret = vec![vec![NodeId(1), NodeId(2), NodeId(3)]];
        assert_eq!(recall_at_k(&gt, &ret, 3, 1000).unwrap(), 0.0);

        // Error paths (all InvalidArgument; all loud).
        assert!(matches!(
            recall_at_k(&gt, &ret, 0, 1000),
            Err(HnswError::InvalidArgument(_))
        ));
        assert!(matches!(
            recall_at_k(&gt, &[], 3, 1000),
            Err(HnswError::InvalidArgument(_))
        ));
        assert!(matches!(
            recall_at_k(&[vec![10, 20]], &ret, 3, 1000),
            Err(HnswError::InvalidArgument(_))
        ));
        assert!(matches!(
            recall_at_k(&[], &[], 3, 1000),
            Err(HnswError::InvalidArgument(_))
        ));

        // 2026-09-03 Stage D review: a negative ground-truth id must fail
        // loudly (the u32 comparison rejects it) rather than silently score
        // lower — and the mirror case, a retrieved id >= 2^31, must NOT wrap
        // through `as i32`. 2026-09-03 review round 2: the round-2
        // base_count gate would now reject NodeId(u32::MAX - 1) as
        // out-of-range, so this case passes base_count = u32::MAX to keep
        // pinning exactly the "no wrap" semantics it was written for.
        let gt_neg = vec![vec![-1, 20, 30]];
        assert!(matches!(
            recall_at_k(&gt_neg, &ret, 3, 1000),
            Err(HnswError::InvalidArgument(_))
        ));
        let ret_huge = vec![vec![NodeId(u32::MAX - 1), NodeId(10), NodeId(20)]];
        assert_eq!(recall_at_k(&gt, &ret_huge, 3, u32::MAX).unwrap(), 2.0 / 3.0);
    }

    /// 2026-09-03 Stage D review round 2 P2: the integrity gates on both
    /// sides of the comparison — every violation must fail loudly with the
    /// query index in the message.
    #[test]
    fn recall_scoring_rejects_corrupt_inputs() {
        let ok_gt = vec![vec![10, 20, 30, 40]];
        let ok_ret = vec![vec![NodeId(10), NodeId(20), NodeId(30)]];

        // gt id out of range.
        let gt = vec![vec![10, 20, 1000]];
        let err = recall_at_k(&gt, &ok_ret, 3, 1000).unwrap_err();
        assert!(
            matches!(&err, HnswError::InvalidArgument(m) if m.contains("query 0") && m.contains("base_count")),
            "unexpected error: {err}"
        );

        // gt duplicate within the prefix.
        let gt = vec![vec![10, 10, 30]];
        let err = recall_at_k(&gt, &ok_ret, 3, 1000).unwrap_err();
        assert!(
            matches!(&err, HnswError::InvalidArgument(m) if m.contains("duplicate")),
            "unexpected error: {err}"
        );

        // Retrieved row shorter than k.
        let ret = vec![vec![NodeId(10), NodeId(20)]];
        let err = recall_at_k(&ok_gt, &ret, 3, 1000).unwrap_err();
        assert!(
            matches!(&err, HnswError::InvalidArgument(m) if m.contains("query 0")),
            "unexpected error: {err}"
        );

        // Retrieved duplicate.
        let ret = vec![vec![NodeId(10), NodeId(10), NodeId(30)]];
        let err = recall_at_k(&ok_gt, &ret, 3, 1000).unwrap_err();
        assert!(
            matches!(&err, HnswError::InvalidArgument(m) if m.contains("duplicate")),
            "unexpected error: {err}"
        );

        // Retrieved id out of range.
        let ret = vec![vec![NodeId(10), NodeId(20), NodeId(1000)]];
        let err = recall_at_k(&ok_gt, &ret, 3, 1000).unwrap_err();
        assert!(
            matches!(&err, HnswError::InvalidArgument(m) if m.contains("base_count")),
            "unexpected error: {err}"
        );
    }
}
