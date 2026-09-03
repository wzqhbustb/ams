//! Snapshot `save`/`load` file API (tech-selection §7) — **Stage C
//! deliverable**.
//!
//! The byte-stream layer lives in [`crate::encoding`] (frozen format §3,
//! CRC32 prefix §7, full load-validation checklist); this module adds the
//! file-level API on top of it and the SoA graph rebuild via
//! `Hnsw::from_parts` (dependency direction: snapshot → {encoding, graph};
//! graph never imports encoding). Round-trip equivalence (`save → load →
//! search` identical to the in-memory original) is M4's crash-injection
//! surrogate (§9).
//!
//! **Format freeze (coding plan Stage C):** this module is the first
//! producer to actually write the §3 layout to disk, so the format is
//! de-facto frozen from this stage on — any later change is a format
//! revision requiring a `FORMAT_VERSION` bump and a revision-log entry (§3:
//! "frozen once written to disk").
//!
//! **Memory peaks** (2026-09-02 Stage C review P2-1 / P2-2): `save` streams
//! through [`crate::encoding::BodyEncoder`] with no `Vec<NodeRecord>`
//! materialization — peak ≈ graph + 1 byte/node (the levels summary) + one
//! record + the `BufWriter` buffer. `load` still materializes the decoded
//! `Vec<NodeRecord>` before splitting it into the SoA arenas (the file
//! image itself is dropped right after decode, saving one file-size
//! factor). The load peak therefore depends on the snapshot's shape:
//!
//! - **realistic shape** (normal dims, levels ≈ log_M N): ≈ 2× the file
//!   size (~8 GB at the §11 R3 1M-gist scale with ~4 GB snapshots);
//! - **pathological legal shape** (dim = 1, many empty levels): the SoA
//!   rebuild pays the nested `Vec<Vec<Vec<NodeId>>>` per-level Vec headers
//!   (24 B/level vs 2 B/level on disk), amplifying to ~12× the file size —
//!   bounded in absolute terms by checklist item 12
//!   ([`crate::encoding::MAX_LEVEL_COUNT`], ~1.5 KB/node worst case). The
//!   structural fix (flat CSR adjacency) is a registered residual for the
//!   Stage D evaluation, as is streaming decode: the decode checklist is
//!   pinned by four review rounds, so the rewrite risk is not taken here.
//!
//! Naming note: types here deliberately avoid the bare name `Snapshot` — the
//! CI snapshot-construction guardrail greps every crate except pg-txn for
//! literal-construction and impl-block patterns on that name (coding plan
//! Stage A), so composite names ([`crate::encoding::SnapshotHeader`],
//! [`crate::encoding::SnapshotFileData`]) are used throughout.
//!
//! Conservative default (coding plan Stage C): a snapshot-loaded graph is
//! **read-only** — `insert` rejects it with
//! [`crate::HnswError::InvalidOperation`]. Continuation-insert semantics are
//! an open question punted to tech-selection v1.6; the PRNG state never
//! enters the snapshot (§4.1 prerequisite ③), so the loaded graph's rng is
//! inert by construction.

use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::encoding::{
    self, SnapshotFileData, SnapshotHeader, CRC32_SIZE, EMPTY_GRAPH_ENTRY_POINT,
    SNAPSHOT_HEADER_SIZE,
};
use crate::error::{HnswError, Result};
use crate::graph::{Hnsw, Metric};
use crate::params::NodeId;

/// Process-local counter making temporary file names unique across
/// concurrent `save` calls (2026-09-02 review P3-3).
static TEMP_FILE_COUNTER: AtomicUsize = AtomicUsize::new(0);

/// Temporary-name collisions tolerated before `save` gives up (2026-09-02
/// review P1-1). The fast path is a single attempt; the retry loop runs
/// only under collision (a pre-planted symlink or a stale `.tmp-*` pileup).
const MAX_TEMP_NAME_ATTEMPTS: u32 = 1024;

/// Byte budgets for [`load_with_budget`] (2026-09-02 review rounds 5/6 P2).
///
/// Two DIFFERENT quantities: a **file-byte** budget bounds what is read
/// from disk, while a **memory** budget bounds the estimated in-memory
/// footprint of the materialized graph. They diverge because the §6 SoA
/// materialization amplifies the on-disk bytes on pathological (but
/// format-legal) shapes — the per-level `Vec` headers cost 24 B against
/// 2 B on disk, so a small file of many empty levels can decode to a much
/// larger heap image; checklist item 12
/// ([`crate::encoding::MAX_LEVEL_COUNT`]) caps the amplification factor,
/// and [`crate::encoding::max_memory_estimate`] derives the resulting
/// upper bound from the validated header alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LoadBudget {
    /// Maximum file size in bytes. Must be at least the fixed-width prefix
    /// (`CRC32_SIZE + SNAPSHOT_HEADER_SIZE` = 29) — a smaller budget is
    /// rejected before anything is read (2026-09-02 review round 6 P1).
    pub max_file_bytes: u64,
    /// Maximum estimated in-memory footprint in bytes, compared against
    /// [`crate::encoding::max_memory_estimate`] before any
    /// decode/materialization.
    pub max_memory_bytes: u64,
}

impl LoadBudget {
    /// No limits — the [`load`] default; intended for trusted local files
    /// (the §12 benchmark scale, ~4 GB).
    pub fn unlimited() -> Self {
        Self {
            max_file_bytes: u64::MAX,
            max_memory_bytes: u64::MAX,
        }
    }
}

/// Serialize `graph` to `path` in the frozen §3 format (header + node-record
/// stream, CRC32 prefix §7).
///
/// The write is **atomic at the file level**: the bytes go to a temporary
/// file in the same directory first and are then renamed over `path` —
/// `rename(2)` is atomic within one filesystem, so a concurrent reader sees
/// either the old file or the new one, never a torn write. Platform note
/// (2026-09-02 review P2-4): the atomic **replace** (rename over an
/// existing target) is POSIX semantics; on Windows, renaming over an
/// existing target fails and `save` returns an `Io` error instead —
/// fail-safe (no corruption, no replacement), and outside M4's supported
/// platform set (the CI matrix: Linux/macOS).
///
/// The temporary name is `path` + `.tmp-<pid>-<counter>`, opened with
/// `O_CREAT | O_EXCL` (`OpenOptions::create_new`): it **never follows a
/// pre-planted symlink and never truncates an existing file** (2026-09-02
/// review P1-1 — a predictable name plus plain `File::create` would let an
/// attacker in a shared writable directory redirect the write onto a victim
/// file). A taken name (`AlreadyExists`) is retried with the next counter
/// value, up to `MAX_TEMP_NAME_ATTEMPTS` (1024) times; exhaustion is reported as
/// [`HnswError::InvalidOperation`]. Concurrent saves to the *same* target
/// within one process are safe (each gets a distinct temporary name; the
/// last rename wins); cross-process concurrency is not guaranteed (review
/// P3-3). A failure after creation removes only our own temporary file
/// (best-effort); `path` is never touched. If the *process* crashes between
/// create and rename, a `.tmp-*` file is left behind and is NOT cleaned up
/// automatically on the next run (harmless; callers may remove them by
/// pattern).
///
/// Error classification note (2026-09-02 review round 7): a **directory**
/// as the save target fails at the final rename with [`HnswError::Io`]
/// (EISDIR) — the atomic-replace protocol deliberately never inspects the
/// target beforehand (any pre-check would be a TOCTOU lie: the target can
/// change between check and rename). This is asymmetric with `load`, where
/// a directory is an [`HnswError::InvalidArgument`] from the regular-file
/// gate — load MUST inspect the target (untrustworthy metadata length
/// would otherwise poison the arithmetic). Both are loud; neither panics,
/// and the failed save leaves no `.tmp-*` residue (best-effort cleanup).
///
/// Trade-off, recorded: no `fsync` — durability across an OS crash is not
/// the M4 snapshot's job (M5's WAL owns durability; the §7 snapshot is a
/// benchmark/reload format), and an fsync would dominate `save` latency on
/// small graphs.
///
/// The body is **streamed** through [`crate::encoding::BodyEncoder`]
/// (2026-09-02 review P2-1): records are read from the graph's accessors
/// one at a time, and the CRC32 placeholder at the head of the temporary
/// file is backpatched after the stream finishes (the on-disk bytes are
/// identical to `wrap_crc32(body)` — the §7 prefix layout is frozen). No
/// `Vec<NodeRecord>` is ever materialized: memory peak ≈ graph + 1
/// byte/node + one record + the `BufWriter` buffer. Two linear passes
/// (levels summary + streaming encode), O(n) total (2026-09-02 review
/// round 5: an earlier revision of this paragraph said "single pass",
/// which undersold the levels-summary collection).
///
/// `graph` is only read (via the read-only accessors); saving does not
/// mutate it — in particular it does **not** set the read-only flag, and
/// saving a read-only (snapshot-loaded) graph is legal.
pub fn save(graph: &Hnsw, path: impl AsRef<Path>) -> Result<()> {
    let path = path.as_ref();
    let params = graph.params();
    let node_count = graph.node_count();
    let header = SnapshotHeader {
        dim: graph.dim(),
        m: params.m(),
        m_max0: params.m_max0(),
        ef_construction: params.ef_construction(),
        // Dense NodeIds and the insert-time u32 guard keep this cast exact
        // (the id space is exhausted AT u32::MAX — allocated ids never reach
        // it, so the INVALID sentinel stays unallocated, §3).
        node_count: node_count as u32,
        entry_point: graph
            .entry_point()
            .map(|id| id.0)
            .unwrap_or(EMPTY_GRAPH_ENTRY_POINT),
        max_level: graph.max_level(),
    };
    // Whole-graph top-level summary (1 byte/node) — the streaming encoder's
    // validation needs it for the entry/max_level relations and the
    // "target has the edge's level" check; collecting it from level() is
    // O(n) with a tiny constant.
    let levels: Vec<u8> = (0..node_count)
        .map(|i| graph.level(NodeId(i as u32)))
        .collect();

    let pid = std::process::id();
    for _ in 0..MAX_TEMP_NAME_ATTEMPTS {
        let mut tmp_name = path.as_os_str().to_owned();
        tmp_name.push(format!(
            ".tmp-{pid}-{}",
            TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        let tmp = PathBuf::from(tmp_name);
        let file = match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp)
        {
            Ok(f) => f,
            // Name collision (planted symlink / stale leftover / racing
            // save): the file was NOT created by us — leave it untouched
            // and try the next counter value.
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e.into()),
        };
        let result = stream_and_rename(graph, &header, &levels, &tmp, path, file);
        if result.is_err() {
            let _ = std::fs::remove_file(&tmp); // best-effort cleanup of OUR file
        }
        return result;
    }
    Err(HnswError::InvalidOperation(format!(
        "no available temporary snapshot name after {MAX_TEMP_NAME_ATTEMPTS} attempts — possible tampering or stale .tmp-* pileup"
    )))
}

/// Stream the graph through the encoder into `tmp` (an already-created,
/// exclusively-ours file), then atomically rename over `target`. On any
/// error the caller removes `tmp`; `target` is never touched (the rename is
/// the only operation performed on it).
fn stream_and_rename(
    graph: &Hnsw,
    header: &SnapshotHeader,
    levels: &[u8],
    tmp: &Path,
    target: &Path,
    file: std::fs::File,
) -> Result<()> {
    let mut w = std::io::BufWriter::new(file);
    // §7 layout is `crc32 prefix + body`, but the streaming CRC is only
    // known once the body has been written: reserve 4 placeholder bytes,
    // stream the body (hashing incrementally), then seek back and
    // backpatch. The on-disk bytes are exactly `crc LE ++ body`, identical
    // to wrap_crc32's output — the frozen layout does not move.
    w.write_all(&[0u8; CRC32_SIZE])?;
    let crc = {
        let mut enc = encoding::BodyEncoder::new(*header, levels.to_vec(), &mut w)?;
        for i in 0..graph.node_count() {
            let id = NodeId(i as u32);
            enc.push_record(graph.vector(id), graph.node_adjacency(id))?;
        }
        enc.finish()?.0
    };
    // `BufWriter::seek` flushes the buffer before repositioning, so the
    // backpatch lands at offset 0 of the already-written body.
    w.seek(SeekFrom::Start(0))?;
    w.write_all(&crc.to_le_bytes())?;
    w.flush()?;
    drop(w);
    std::fs::rename(tmp, target)?;
    Ok(())
}

/// Load a snapshot from `path` and rebuild the in-memory SoA graph (§6/§7)
/// — a thin wrapper over [`load_with_budget`] with **no budgets**
/// ([`LoadBudget::unlimited()`], 2026-09-02 review rounds 5/6 P2).
///
/// `load` is intended for trusted local files (the §12 benchmark scale,
/// ~4 GB). **Untrusted sources MUST use [`load_with_budget`] with an
/// explicit budget** (§7 threat model): the format-derived ceiling
/// ([`encoding::max_records_size`]) only closes the "declares small, is huge"
/// case; a file that declares AND is huge is bounded only by a
/// caller-supplied budget — and file bytes are not heap bytes (see
/// [`LoadBudget`]).
///
/// The full contract — validation order, metric contract, read-only
/// semantics, complexity — lives on [`load_with_budget`].
pub fn load(path: impl AsRef<Path>, metric: Metric, ef_search_default: u32) -> Result<Hnsw> {
    load_with_budget(path, metric, ef_search_default, LoadBudget::unlimited())
}

/// Load a snapshot from `path` with explicit budgets and rebuild the
/// in-memory SoA graph (§6/§7).
///
/// **Budgets (2026-09-02 review rounds 5/6 P2):** `budget.max_file_bytes`
/// bounds what is read from disk — a larger file fails with
/// [`HnswError::InvalidArgument`] right after the header pre-read, without
/// reading the body, and the body read itself is capped
/// (`min(max_file_bytes, format ceiling) + 1` bytes), so a file grown
/// between `metadata()` and the read cannot inflate the read (the over-cap
/// bytes are cut and the CRC / decode trailing-byte check rejects the
/// result). `budget.max_memory_bytes` bounds the **estimated in-memory
/// footprint** ([`encoding::max_memory_estimate`], compared before any
/// decode/materialization) — a distinct quantity, because the SoA
/// materialization amplifies pathological shapes (see [`LoadBudget`]).
///
/// **Validation order** (cheap checks first, the body is read last):
///
/// 1. the path must be a **regular file** (2026-09-02 review round 5 P1):
///    a snapshot is a regular-file format — FIFOs, devices and sockets
///    have no trustworthy length, and a 0-length special file would
///    underflow the length arithmetic in check 5 (a debug-build
///    subtraction panic / release-build wrap pre-fix), so a non-regular
///    file fails with `InvalidArgument` before any length math. Then the
///    fixed-width prefix (`CRC32_SIZE + SNAPSHOT_HEADER_SIZE` bytes) is
///    pre-read; a shorter file fails with a `Corrupted` truncation error
///    in the decode checklist's wording style, and any non-EOF I/O error
///    belongs to the `Io` variant (2026-09-02 review round 4 P3);
/// 2. [`SnapshotHeader::decode`] on it — magic, format version, and the
///    construction-parameter re-run;
/// 3. the budgets (rounds 5/6 P2): `file_len > budget.max_file_bytes`
///    fails `InvalidArgument` before the body is read, and
///    `max_memory_estimate(&header) > budget.max_memory_bytes` fails
///    `InvalidArgument` before any decode/materialization;
/// 4. `ef_search_default` re-validation against the header's `m` (§4.4, via
///    [`encoding::params_from_header`]) — **before the body is read**, so a
///    bad caller argument fails fast even on a truncated file;
/// 5. file-length interval cross-check against `node_count`, both sides
///    compared by division so they cannot overflow, failing loudly without
///    reading the body (the subtraction itself is `saturating_sub` —
///    belt-and-suspenders behind the check-1 regular-file gate):
///    - min: the file must hold at least `node_count` minimum-size records
///      (`2 + 4*dim + 1 + 2` bytes each — the same bound as decode's
///      checklist item 6);
///    - max (2026-09-02 review round 4 P2-1): the file must not exceed
///      [`encoding::max_records_size`] for its header either (the
///      records-only counterpart of [`encoding::max_body_size`] — round 7:
///      `body_bytes` excludes the 29-byte prefix, so the ceiling must too).
///      Encode produces
///      exact sizes and decode rejects trailing bytes (checklist item 7),
///      so the [min, max] interval does not shrink the legal acceptance
///      set — it only turns "reject after reading" into "reject without
///      reading" for a file that claims a small header but is physically
///      huge (trailing garbage or a hostile sparse file, which is then
///      never fully read). The memory cost of a legitimately large file
///      (§11 R3: 1M gist, ~4 GB) is inherent and unaffected — what this
///      side closes is the "declares small, is huge" amplification path.
///
/// Check 6 then reads the rest of the file (same fd, no reopen race,
/// read-capped per the budget paragraph) and verifies it:
/// [`encoding::unwrap_crc32`], then [`encoding::decode_snapshot_body`] —
/// the full load-validation checklist (a corrupt file fails loudly and
/// never panics). The CRC also backstops the TOCTOU window between the
/// pre-read and the full read.
///
/// Check 7 is metric fitness (2026-09-02 review P1-2): for
/// [`Metric::Cosine`], every node vector is re-checked through the same
/// `validate_entry_vector` funnel (`Metric::distance` self-distance) — a
/// snapshot legally built under L2 may contain zero vectors, which would
/// otherwise surface as `ZeroVector` inside the search path's `expect` and
/// panic. O(n·dim), the same order as decode's finiteness scan.
///
/// Threat model, recorded: **the CRC guards against bit-rot, not malice —
/// load assumes a non-adversarial source**; the size interval (check 5)
/// plus the caller budget (check 3) bound even the adversarial
/// size-amplification cases, and streaming validation remains a follow-up.
///
/// **Metric contract (§3):** the metric is *not* part of the snapshot — a
/// snapshot stores geometry only, and `metric` is supplied by the caller.
/// The caller MUST pass the same metric the graph was built with. A
/// mismatch silently changes distance semantics, **but it can no longer
/// panic**: the one panicking combination (Cosine + zero vector) is
/// rejected loudly at load (check 7 above).
///
/// `ef_search_default` is query-time state and stays out of the snapshot (§3
/// v1.3), so it is an explicit caller input here; it must satisfy the §4.4
/// construction check `ef_search_default >= m` against the snapshot's `m`
/// (violations fail with [`crate::HnswError::InvalidParams`]).
///
/// The loaded graph is **read-only**: `insert` rejects it with
/// [`crate::HnswError::InvalidOperation`] (coding plan Stage C conservative
/// default — continuation-insert semantics are an open question punted to
/// tech-selection v1.6). Search behaves exactly as on the original graph;
/// round-trip equivalence is asserted bit-for-bit by the Stage C test suite.
///
/// The load path is a constant number of serial linear passes (pre-read,
/// decode, the cosine check, the SoA split — O(n) total; the §12 budget is
/// "1M nodes, load including validation < 5 min" — no quadratic path may
/// hide here). See the module docs for the load-side memory-peak residual
/// (≈ 2× the file size for realistic shapes; streaming decode deferred).
pub fn load_with_budget(
    path: impl AsRef<Path>,
    metric: Metric,
    ef_search_default: u32,
    budget: LoadBudget,
) -> Result<Hnsw> {
    let path = path.as_ref();
    // Budget floor (2026-09-02 review round 6 P1): a budget below the
    // fixed-width prefix is rejected before anything is read, so every
    // length computation below can rely on max_file_bytes >= 29.
    if budget.max_file_bytes < (CRC32_SIZE + SNAPSHOT_HEADER_SIZE) as u64 {
        return Err(HnswError::InvalidArgument(format!(
            "budget {} below the {}-byte fixed-width prefix (§7)",
            budget.max_file_bytes,
            CRC32_SIZE + SNAPSHOT_HEADER_SIZE
        )));
    }
    // Check 1: regular-file gate (2026-09-02 review round 5 P1) — a
    // snapshot is a regular-file format; FIFOs/devices/sockets have no
    // trustworthy length, and a 0-length special file would underflow the
    // length arithmetic at check 5 (debug panic / release wrap pre-fix).
    let mut file = std::fs::File::open(path)?;
    let md = file.metadata()?;
    if !md.is_file() {
        return Err(HnswError::InvalidArgument(
            "snapshot must be a regular file (FIFOs/devices/sockets have no trustworthy length, §7)"
                .to_string(),
        ));
    }
    let file_len = md.len();
    // Check 1, continued: pre-read the fixed-width prefix only. Error
    // classification (2026-09-02 review round 4 P3): UnexpectedEof means
    // the file is shorter than the fixed prefix — truncation, reported in
    // the decode checklist's wording style; every other kind (EISDIR,
    // permissions, ...) is a filesystem error and belongs to the Io
    // variant.
    let mut prefix = [0u8; CRC32_SIZE + SNAPSHOT_HEADER_SIZE];
    if let Err(e) = file.read_exact(&mut prefix) {
        return Err(match e.kind() {
            std::io::ErrorKind::UnexpectedEof => HnswError::Corrupted(format!(
                "snapshot too short: {file_len} bytes, need at least {} (CRC32 prefix + fixed-width header, §7/§3)",
                CRC32_SIZE + SNAPSHOT_HEADER_SIZE
            )),
            _ => HnswError::Io(e),
        });
    }
    // Check 2: magic / version / construction-parameter re-run (the CRC
    // prefix is verified later, over the full body).
    let header = SnapshotHeader::decode(&prefix[CRC32_SIZE..])?;
    // Check 3 (2026-09-02 review rounds 5/6 P2): the caller's budgets,
    // before any body read or decode. The format ceiling (check 5, max
    // side) only closes "declares small, is huge"; a file that declares
    // AND is huge is bounded only here. The memory budget is a distinct
    // quantity: the SoA materialization amplifies pathological shapes
    // (24 B/level Vec headers vs 2 B on disk — see LoadBudget).
    if file_len > budget.max_file_bytes {
        return Err(HnswError::InvalidArgument(format!(
            "snapshot file is {file_len} bytes, budget is {} (load_with_budget; §7 threat model: untrusted sources must set a budget)",
            budget.max_file_bytes
        )));
    }
    let memory_estimate = encoding::max_memory_estimate(&header);
    if memory_estimate > budget.max_memory_bytes {
        return Err(HnswError::InvalidArgument(format!(
            "estimated in-memory footprint of this snapshot is {memory_estimate} bytes, budget is {} (load_with_budget; §7 threat model: byte budget != memory budget — the SoA materialization amplifies pathological shapes)",
            budget.max_memory_bytes
        )));
    }
    // Check 4: ef_search_default vs the snapshot's m — before the body read.
    let params = encoding::params_from_header(&header, ef_search_default)?;
    // Check 5, min side: node_count vs the actual file length (division
    // form — a u32::MAX node_count must not overflow a multiplication).
    // All length arithmetic in this function is saturating (2026-09-02
    // review round 6 P1): the is_file gate is the primary defense against
    // untrustworthy lengths, but metadata can also go STALE — a file grown
    // from < 29 bytes after metadata() lets read_exact succeed with a
    // sub-prefix file_len; body_len then saturates to 0 and the min check
    // rejects any node_count > 0 (an empty-graph header proceeds to check
    // 6, which reads the real — now grown — bytes and validates normally).
    let min_record_size = 2 + 4 * u64::from(header.dim) + 1 + 2;
    let body_bytes = body_len(file_len);
    if u64::from(header.node_count) > body_bytes / min_record_size {
        return Err(HnswError::Corrupted(format!(
            "node_count {} exceeds the {} records that fit in the {} body bytes of the file (min record size {min_record_size}) — pre-read length cross-check",
            header.node_count,
            body_bytes / min_record_size,
            body_bytes
        )));
    }
    // Check 5, max side (2026-09-02 review round 4 P2-1): the mirror
    // bound — the file must not exceed the maximum legal size for its
    // header either (see `max_body_size` for the legality argument). A
    // "claims small, is huge" file (trailing garbage, hostile sparse file)
    // is rejected here WITHOUT being read. Round 7: compare like with
    // like — `body_bytes` is records-only (file length minus the 29-byte
    // prefix), so the ceiling is `max_records_size`, not the
    // header-inclusive `max_body_size` (which left this gate 25 bytes
    // loose — safe direction, but not the exact ceiling).
    let max_records = encoding::max_records_size(&header);
    if body_bytes > max_records {
        return Err(HnswError::Corrupted(format!(
            "file is larger than the maximum legal size for its header — trailing garbage ({body_bytes} body bytes > max {max_records}; §3 checklist item 7 rejects trailing bytes, checked pre-read so a hostile/sparse file is never fully read)"
        )));
    }

    // Check 6: full body read, CRC, then the complete decode checklist. The
    // read is CAPPED (rounds 5/6 P2/P1) at min(file budget, format
    // ceiling) + 1 bytes total — the TOCTOU window between metadata() and
    // this read cannot inflate it; an over-cap file loses its tail here and
    // is then rejected by the CRC / decode trailing-byte check. All
    // arithmetic goes through the saturating helpers (`read_cap`), so no
    // budget/stale-metadata combination can underflow. Round 7: `read_cap`
    // takes the records-only ceiling and adds the 29-byte prefix itself.
    let read_cap_total = read_cap(budget.max_file_bytes, max_records);
    let remaining_cap = read_cap_total.saturating_sub((CRC32_SIZE + SNAPSHOT_HEADER_SIZE) as u64);
    let mut bytes = prefix.to_vec();
    file.take(remaining_cap).read_to_end(&mut bytes)?;
    let decoded = encoding::decode_snapshot_body(encoding::unwrap_crc32(&bytes)?)?;
    debug_assert_eq!(decoded.header, header, "same fd, same bytes");
    // 2026-09-02 review P2-1: free the file image before materializing the
    // SoA arenas — peak drops by one file-size factor. (The decoded records
    // themselves stay materialized until the split below; making decode
    // itself streaming is the deferred residual, see module docs.)
    drop(bytes);
    let SnapshotFileData { header, nodes } = decoded;

    // Check 7 (review P1-2): §5 entry validation holds at load — Cosine is
    // undefined for zero vectors, and a snapshot built under another metric
    // may legally contain them. Same funnel as `validate_entry_vector`;
    // decode already proved finiteness and the dim match, so ZeroVector is
    // the only error this can produce.
    if metric == Metric::Cosine {
        for (i, rec) in nodes.iter().enumerate() {
            if metric.distance(&rec.vector, &rec.vector).is_err() {
                return Err(HnswError::InvalidArgument(format!(
                    "node {i}: zero vector cannot serve Metric::Cosine — the snapshot was built for a different metric (§3: metric is caller-supplied; §5 entry validation holds at load)"
                )));
            }
        }
    }

    // Split the validated records into the §6 SoA arenas — one pass.
    let dim = usize::from(header.dim);
    let mut vectors = Vec::with_capacity(nodes.len() * dim);
    let mut levels = Vec::with_capacity(nodes.len());
    let mut adjacency = Vec::with_capacity(nodes.len());
    for rec in nodes {
        debug_assert_eq!(rec.vector.len(), dim, "decode validated the record length");
        vectors.extend_from_slice(&rec.vector);
        // level_count = top level + 1 (§3); decode validated level_count >= 1.
        levels.push((rec.neighbors.len() - 1) as u8);
        adjacency.push(rec.neighbors);
    }
    let entry_point = (header.node_count > 0).then_some(NodeId(header.entry_point));

    // Seed 0: the rng is inert on a read-only graph — no level is ever
    // drawn (see the module docs and `Hnsw::from_parts`).
    Hnsw::from_parts(
        header.dim,
        metric,
        params,
        entry_point,
        header.max_level,
        vectors,
        levels,
        adjacency,
        0,
    )
}

/// Body length derived from a metadata file length, saturating (2026-09-02
/// review round 6 P1: metadata can be stale — a file grown from < 29 bytes
/// after `metadata()` lets the prefix read succeed with a sub-prefix
/// `file_len`; saturating keeps every downstream check total).
fn body_len(file_len: u64) -> u64 {
    file_len.saturating_sub((CRC32_SIZE + SNAPSHOT_HEADER_SIZE) as u64)
}

/// Total read cap in bytes, prefix included (2026-09-02 review rounds 5/6
/// P2/P1): the smaller of the caller's file budget and the format ceiling,
/// plus one so an over-cap (e.g. TOCTOU-grown) file reads past the legal
/// prefix and is rejected downstream by the CRC / decode trailing-byte
/// check instead of being silently truncated to a passing prefix. All
/// saturating — no budget/stale-metadata combination can underflow.
/// `max_records` is the RECORDS-ONLY ceiling
/// ([`encoding::max_records_size`], round 7) — this helper adds the
/// 29-byte prefix itself; passing the header-inclusive
/// [`encoding::max_body_size`] would double-count the header.
fn read_cap(max_file_bytes: u64, max_records: u64) -> u64 {
    max_file_bytes
        .min(((CRC32_SIZE + SNAPSHOT_HEADER_SIZE) as u64).saturating_add(max_records))
        .saturating_add(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Round-6 P1: the length arithmetic helpers must be total — no
    /// underflow/overflow at any boundary (the pre-fix code had a bare
    /// `read_cap_total - 29`).
    #[test]
    fn length_helpers_never_underflow() {
        const PREFIX: u64 = (CRC32_SIZE + SNAPSHOT_HEADER_SIZE) as u64; // 29
                                                                        // body_len: below/at/above the prefix, and huge.
        assert_eq!(body_len(0), 0);
        assert_eq!(body_len(PREFIX - 1), 0);
        assert_eq!(body_len(PREFIX), 0);
        assert_eq!(body_len(PREFIX + 1), 1);
        assert_eq!(body_len(u64::MAX), u64::MAX - PREFIX);
        // read_cap: tiny budget (below the prefix — load_with_budget's
        // floor check rejects this before the read, but the helper itself
        // must still be total), budget above/below the ceiling, and
        // u64::MAX on both axes (saturating_add, not wrapping).
        assert_eq!(read_cap(0, 0), 1);
        assert_eq!(read_cap(10, 1_000), 11);
        assert_eq!(read_cap(1_000, 10), PREFIX + 10 + 1);
        assert_eq!(read_cap(u64::MAX, 100), PREFIX + 100 + 1);
        assert_eq!(read_cap(u64::MAX, u64::MAX), u64::MAX);
        // The remaining-after-prefix subtraction is saturating too.
        assert_eq!(read_cap(0, 0).saturating_sub(PREFIX), 0);
        assert_eq!(read_cap(u64::MAX, 100).saturating_sub(PREFIX), 101);
    }

    /// The budget floor: a budget below the 29-byte fixed-width prefix is
    /// rejected before anything is read (round 6 P1) — no file access, no
    /// panic, InvalidArgument naming the budget.
    #[test]
    fn budget_below_prefix_fails_before_any_read() {
        let budget = LoadBudget {
            max_file_bytes: 10,
            ..LoadBudget::unlimited()
        };
        // A nonexistent path — the floor check must fire BEFORE File::open.
        let err = load_with_budget(
            "/nonexistent/pg_am_hnsw_budget_floor_test.bin",
            Metric::L2,
            64,
            budget,
        )
        .unwrap_err();
        assert!(
            matches!(err, HnswError::InvalidArgument(_)),
            "expected InvalidArgument, got: {err}"
        );
        assert!(err.to_string().contains("budget"), "message: {err}");
    }
}
