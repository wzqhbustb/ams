//! Frozen snapshot byte-stream encoding primitives (tech-selection §3, §7).
//!
//! Layout (all fixed-width fields little-endian, hand-written per §3 — no
//! self-describing overhead):
//!
//! ```text
//! snapshot        := crc32:u32 (over body) | body                 // §7 prefix
//! body            := snapshot_header | node_record[node_count]
//! snapshot_header := magic:u32 | format_version:u16 | dim:u16 |
//!                    m:u16 | m_max0:u16 | ef_construction:u32 |
//!                    node_count:u32 | entry_point:u32 | max_level:u8
//! node_record     := flags:u8 | reserved:u8 | vector:f32[dim] |
//!                    level_count:u8 |
//!                    per-level { neighbor_count:u16 | neighbors:NodeId[.] }
//! ```
//!
//! Frozen semantics (§3):
//!
//! - **Position is identity** (v1.2): the i-th node record *is* `NodeId(i)`
//!   — no explicit id field, valid only while NodeIds stay dense (M6 deletes
//!   are tombstone-in-place, §11 O3).
//! - `dim` lives only in the header; node records do not embed it (the graph
//!   never mixes dimensions).
//! - `level_count` = number of levels the node appears on = top level + 1;
//!   level 0 always exists, so `level_count >= 1`. Load additionally
//!   accepts only `level_count <= 64` ([`MAX_LEVEL_COUNT`], checklist item
//!   12 — an acceptance tightening, not a format change).
//! - Empty graph: `node_count == 0`, `entry_point == u32::MAX`
//!   ([`NodeId::INVALID`]), `max_level == 0` (no nodes, no levels).
//! - `flags` / `reserved` are always 0 in M4 (`flags` is pre-allocated for
//!   the M6 tombstone bit); load treats non-zero as unknown-version content
//!   and fails loudly.
//! - Only the construction parameters (`m`, `m_max0`, `ef_construction`)
//!   enter the snapshot; `ef_search_default` is a query-time default and
//!   stays out (§3 v1.3).
//!
//! This module is byte-stream only: the `save(path)`/`load(path)` file API
//! lives in [`crate::snapshot`] (Stage C). The full load-validation
//! checklist lives in [`decode_snapshot_body`]; the write path is the
//! streaming [`BodyEncoder`] (2026-09-02 Stage C review P2-1), with
//! [`encode_snapshot_body`] as a thin wrapper over it — one encoder
//! implementation, no drift.

use std::io::Write;

use crate::error::{HnswError, Result};
use crate::params::{HnswParams, NodeId};

/// Snapshot magic: the ASCII bytes `HNSW` read as a little-endian u32.
pub const SNAPSHOT_MAGIC: u32 = 0x5753_4E48;

/// Snapshot format version. Any change to the frozen layout is a format
/// revision and must bump this (§3: "frozen once written to disk").
pub const FORMAT_VERSION: u16 = 1;

/// Byte size of the fixed-width snapshot header (§3 v1.3: no loose fields).
pub const SNAPSHOT_HEADER_SIZE: usize = 4 + 2 + 2 + 2 + 2 + 4 + 4 + 4 + 1;

/// Byte size of the CRC32 prefix (§7).
pub const CRC32_SIZE: usize = 4;

/// `entry_point` encoding for an empty graph (§3 v1.2) — the raw value of
/// [`NodeId::INVALID`].
pub const EMPTY_GRAPH_ENTRY_POINT: u32 = u32::MAX;

/// Load-acceptance cap on a record's `level_count` (checklist item 12,
/// 2026-09-02 review P2-2). **Not a format change** — the encoding is
/// untouched; this only tightens what load accepts (and therefore what the
/// write path produces, via the shared validation helpers).
///
/// Justification: the level draw is geometric with P(level ≥ L) = M⁻ˡ, and
/// `rng::Xoshiro256StarStar::next_level` hard-bounds the draw at
/// floor(53·ln2 / ln M) ≤ 53 (at M = 2), so no buildable graph ever
/// exceeds level_count = 54 — 64 clears every legal graph with margin.
/// Without the cap, a legal-CRC record could claim 255 mostly-empty levels
/// and the SoA rebuild would pay the per-level `Vec` headers (24 B/level
/// vs 2 B/level on disk) — a memory amplification on pathological (but
/// format-legal) input.
pub const MAX_LEVEL_COUNT: usize = 64;

/// Fixed-width snapshot header (§3). Only the construction parameters are
/// carried; `ef_search_default` is query-time state and absent by design.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotHeader {
    /// Vector dimension, fixed for the whole graph (§3: same graph never
    /// mixes dimensions).
    pub dim: u16,
    /// Construction parameter `M` (§4.2).
    pub m: u16,
    /// Construction parameter `M_max0` (§4.2).
    pub m_max0: u16,
    /// Construction parameter `ef_construction` (§4.2).
    pub ef_construction: u32,
    /// Number of node records following the header.
    pub node_count: u32,
    /// Raw entry-point id; [`EMPTY_GRAPH_ENTRY_POINT`] when `node_count == 0`.
    pub entry_point: u32,
    /// Top level of the entry-point node; 0 for an empty graph.
    pub max_level: u8,
}

impl SnapshotHeader {
    /// Encode as the fixed-width little-endian header (§3).
    pub fn encode(&self) -> [u8; SNAPSHOT_HEADER_SIZE] {
        let mut out = [0u8; SNAPSHOT_HEADER_SIZE];
        out[0..4].copy_from_slice(&SNAPSHOT_MAGIC.to_le_bytes());
        out[4..6].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
        out[6..8].copy_from_slice(&self.dim.to_le_bytes());
        out[8..10].copy_from_slice(&self.m.to_le_bytes());
        out[10..12].copy_from_slice(&self.m_max0.to_le_bytes());
        out[12..16].copy_from_slice(&self.ef_construction.to_le_bytes());
        out[16..20].copy_from_slice(&self.node_count.to_le_bytes());
        out[20..24].copy_from_slice(&self.entry_point.to_le_bytes());
        out[24] = self.max_level;
        out
    }

    /// Decode a header and validate it: magic, format version, and a re-run
    /// of the construction-parameter checks it carries (§3: load adopts the
    /// snapshot's construction parameters, so `m < 2` / `ef_construction < m`
    /// / `dim == 0` are loud errors — §5's `dim = 0` rejection applies at
    /// every entry point).
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < SNAPSHOT_HEADER_SIZE {
            return Err(HnswError::Corrupted(format!(
                "snapshot header truncated: {} bytes, need {SNAPSHOT_HEADER_SIZE}",
                bytes.len()
            )));
        }
        let magic = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        if magic != SNAPSHOT_MAGIC {
            return Err(HnswError::Corrupted(format!(
                "bad magic 0x{magic:08x} (expected 0x{SNAPSHOT_MAGIC:08x} — not an HNSW snapshot)"
            )));
        }
        let version = u16::from_le_bytes(bytes[4..6].try_into().unwrap());
        if version != FORMAT_VERSION {
            return Err(HnswError::Corrupted(format!(
                "unknown format_version {version} (this build reads {FORMAT_VERSION})"
            )));
        }
        let header = Self {
            dim: u16::from_le_bytes(bytes[6..8].try_into().unwrap()),
            m: u16::from_le_bytes(bytes[8..10].try_into().unwrap()),
            m_max0: u16::from_le_bytes(bytes[10..12].try_into().unwrap()),
            ef_construction: u32::from_le_bytes(bytes[12..16].try_into().unwrap()),
            node_count: u32::from_le_bytes(bytes[16..20].try_into().unwrap()),
            entry_point: u32::from_le_bytes(bytes[20..24].try_into().unwrap()),
            max_level: bytes[24],
        };
        header.validate_construction_params()?;
        Ok(header)
    }

    /// Re-run the construction checks the snapshot carries (§3): `dim != 0`,
    /// `m >= 2`, `m_max0 >= m`, `ef_construction >= m`. The
    /// `ef_search_default` check has no snapshot-carried operand and stays
    /// out by design. Shared by [`SnapshotHeader::decode`] and
    /// [`encode_snapshot_body`] — the write path must refuse anything the
    /// read path would reject (2026-08-31 review: encode accepted headers
    /// its own decoder could not load, e.g. `node_count: 0` with a
    /// non-sentinel `entry_point`).
    pub fn validate_construction_params(&self) -> Result<()> {
        if self.dim == 0 {
            return Err(HnswError::Corrupted(
                "dim = 0 in snapshot header (§5: rejected at every entry point)".to_string(),
            ));
        }
        if self.m < 2 {
            return Err(HnswError::Corrupted(format!(
                "M = {} < 2 in snapshot header (construction validation re-run, §3)",
                self.m
            )));
        }
        if self.ef_construction < u32::from(self.m) {
            return Err(HnswError::Corrupted(format!(
                "ef_construction = {} < M = {} in snapshot header (construction validation re-run, §3)",
                self.ef_construction, self.m
            )));
        }
        if self.m_max0 < self.m {
            return Err(HnswError::Corrupted(format!(
                "M_max0 = {} < M = {} in snapshot header (construction validation re-run, §3 v1.7)",
                self.m_max0, self.m
            )));
        }
        Ok(())
    }
}

/// A decoded node record (§3). `neighbors[l]` is the level-`l` adjacency
/// list; level 0 is always present.
#[derive(Debug, Clone, PartialEq)]
pub struct NodeRecord {
    /// The node's vector, `dim` components (never contains NaN or ±inf —
    /// §5/§7).
    pub vector: Vec<f32>,
    /// Per-level neighbor lists, index = level. `neighbors.len()` is the
    /// encoded `level_count` = top level + 1.
    pub neighbors: Vec<Vec<NodeId>>,
}

/// Append one node record to `out` (§3 layout; `flags`/`reserved` written
/// as 0 — the M6 tombstone bit is a future format revision).
pub fn encode_node_record(
    out: &mut Vec<u8>,
    dim: u16,
    vector: &[f32],
    neighbors: &[Vec<NodeId>],
) -> Result<()> {
    if vector.len() != usize::from(dim) {
        return Err(HnswError::InvalidArgument(format!(
            "vector has {} components, graph dim is {dim} (§3: same graph never mixes dimensions)",
            vector.len()
        )));
    }
    if neighbors.is_empty() {
        return Err(HnswError::InvalidArgument(
            "level_count = 0 is unencodable: level 0 always exists (§3)".to_string(),
        ));
    }
    if neighbors.len() > MAX_LEVEL_COUNT {
        return Err(HnswError::InvalidArgument(format!(
            "level_count {} > {MAX_LEVEL_COUNT} (checklist item 12; 2026-09-02 review round 5 P3: the pub codec must refuse what the decoder refuses — the decoder enforces the same cap at its parse point. Two guardrails, one rule, one constant)",
            neighbors.len()
        )));
    }
    // 2026-09-02 review round 6 P3: the pub codec must also refuse
    // non-finite vector components — decode_node_record rejects them at its
    // parse point (§5/§7), so a standalone encode must not emit them. The
    // BodyEncoder path is already guarded upstream by
    // `validate_record_basics`; this line is the symmetry defense for
    // independent codec users. Two guardrails, one rule.
    for (i, &x) in vector.iter().enumerate() {
        if !x.is_finite() {
            return Err(HnswError::InvalidArgument(format!(
                "non-finite vector component (NaN or ±inf) at index {i} (§5/§7 — the decoder rejects this at its parse point; write/read symmetry)"
            )));
        }
    }
    out.push(0); // flags — always 0 in M4 (§3: reserved for the M6 tombstone bit)
    out.push(0); // reserved — always 0
    for &x in vector {
        out.extend_from_slice(&x.to_le_bytes());
    }
    out.push(neighbors.len() as u8); // level_count = top level + 1 (§3 v1.2)
    for (level, list) in neighbors.iter().enumerate() {
        if list.len() > usize::from(u16::MAX) {
            return Err(HnswError::InvalidArgument(format!(
                "level {level} has {} neighbors, does not fit u16",
                list.len()
            )));
        }
        out.extend_from_slice(&(list.len() as u16).to_le_bytes());
        for n in list {
            out.extend_from_slice(&n.0.to_le_bytes());
        }
    }
    Ok(())
}

/// Read `n` bytes from the front of `*cursor`, or a truncation error naming
/// what was being read.
fn take<'a>(cursor: &mut &'a [u8], n: usize, what: &str) -> Result<&'a [u8]> {
    if cursor.len() < n {
        return Err(HnswError::Corrupted(format!(
            "truncated node record: need {n} bytes for {what}, have {}",
            cursor.len()
        )));
    }
    let (head, tail) = cursor.split_at(n);
    *cursor = tail;
    Ok(head)
}

/// Decode one node record from the front of `bytes`; returns the record and
/// the number of bytes consumed (records are variable-length, so callers
/// walk the stream with the cursor).
///
/// Per-record load validation (§3, §5/§7): `flags == 0`, `reserved == 0`,
/// `level_count >= 1`, `level_count <= MAX_LEVEL_COUNT` (checklist item 12,
/// enforced here at the parse point before any per-level allocation —
/// 2026-09-02 review round 4 P2-2), and **non-finite component rejection**
/// (NaN or ±inf)
/// — our own encoder never produces one and the CRC catches bit-flips, but
/// entry validation holds at every entry point (§7 v1.4 belt-and-suspenders;
/// ±inf added 2026-08-31: cosine's inf/inf would silently produce NaN).
pub fn decode_node_record(bytes: &[u8], dim: u16) -> Result<(NodeRecord, usize)> {
    let mut cursor = bytes;
    let flags = take(&mut cursor, 1, "flags")?[0];
    if flags != 0 {
        return Err(HnswError::Corrupted(format!(
            "unknown node flags 0x{flags:02x} (M4 writes 0; the tombstone bit is an M6 format revision, §3)"
        )));
    }
    let reserved = take(&mut cursor, 1, "reserved")?[0];
    if reserved != 0 {
        return Err(HnswError::Corrupted(format!(
            "non-zero reserved byte 0x{reserved:02x} (§3: always 0 in this format version)"
        )));
    }
    let vec_bytes = take(&mut cursor, 4 * usize::from(dim), "vector")?;
    let mut vector = Vec::with_capacity(usize::from(dim));
    for (i, chunk) in vec_bytes.chunks_exact(4).enumerate() {
        let x = f32::from_le_bytes(chunk.try_into().unwrap());
        if !x.is_finite() {
            return Err(HnswError::Corrupted(format!(
                "non-finite vector component at index {i} (§5/§7: load-side NaN/±inf rejection)"
            )));
        }
        vector.push(x);
    }
    let level_count = take(&mut cursor, 1, "level_count")?[0];
    if level_count == 0 {
        return Err(HnswError::Corrupted(
            "level_count = 0: level 0 always exists (level_count = top level + 1, §3)".to_string(),
        ));
    }
    // Checklist item 12, enforced AT THE PARSE POINT (2026-09-02 review
    // round 4 P2-2): reject before allocating any per-level Vec — without
    // this, a forged record could claim 255 levels and the decode would
    // allocate the nested Vec skeleton before `validate_record_basics`
    // ever ran. Same rule, same constant, two lines of defense:
    // `validate_record_basics` keeps the check for the write / one-shot
    // paths, which never pass through this function.
    if usize::from(level_count) > MAX_LEVEL_COUNT {
        return Err(HnswError::Corrupted(format!(
            "level_count {level_count} > {MAX_LEVEL_COUNT} (checklist item 12: the geometric draw is hard-bounded at level 53 even at M = 2, so no legal graph can reach this, §4.1)"
        )));
    }
    let mut neighbors = Vec::with_capacity(usize::from(level_count));
    for _ in 0..level_count {
        let count = u16::from_le_bytes(take(&mut cursor, 2, "neighbor_count")?.try_into().unwrap());
        let id_bytes = take(&mut cursor, 4 * usize::from(count), "neighbors")?;
        let mut list = Vec::with_capacity(usize::from(count));
        for chunk in id_bytes.chunks_exact(4) {
            list.push(NodeId(u32::from_le_bytes(chunk.try_into().unwrap())));
        }
        neighbors.push(list);
    }
    let consumed = bytes.len() - cursor.len();
    Ok((NodeRecord { vector, neighbors }, consumed))
}

/// A fully decoded and validated snapshot body. Stage C's `load` rebuilds
/// the SoA graph from this (position is identity: `nodes[i]` is `NodeId(i)`).
#[derive(Debug, Clone, PartialEq)]
pub struct SnapshotFileData {
    /// The validated header.
    pub header: SnapshotHeader,
    /// The node records in stream order — `nodes[i]` *is* `NodeId(i)` (§3).
    pub nodes: Vec<NodeRecord>,
}

/// Encode a snapshot body (header + node records, no CRC prefix — wrap with
/// [`wrap_crc32`] for the on-disk form, §7).
///
/// **Thin wrapper over [`BodyEncoder`]** (2026-09-02 Stage C review P2-1):
/// the streaming encoder is the single write-path implementation; this
/// function just streams into a `Vec`. **Write/read symmetry** (2026-08-31
/// review P2): the write path runs the same graph-data validation the read
/// path would, so a producer-side bug fails here, at write time, instead of
/// materializing as a corrupt file that only decode can diagnose.
pub fn encode_snapshot_body(header: &SnapshotHeader, nodes: &[NodeRecord]) -> Result<Vec<u8>> {
    // `saturating_sub`: a level_count = 0 record saturates to top level 0.
    // The saturation never masks a defect: the first line of defense is
    // `push_record`'s summary/record consistency check (2026-09-02 review),
    // which rejects both level_count = 0 ("disagrees with the levels
    // summary") and level_count > 256 (the truncating `as u8` here then
    // disagrees with the real count); anything above MAX_LEVEL_COUNT is
    // rejected by `validate_record_basics` and by `encode_node_record`
    // itself (checklist item 12, round 5 P3). All unreachable from decode,
    // where level_count is a u8 by construction.
    let levels: Vec<u8> = nodes
        .iter()
        .map(|rec| rec.neighbors.len().saturating_sub(1) as u8)
        .collect();
    let mut enc = BodyEncoder::new(*header, levels, Vec::new())?;
    for rec in nodes {
        enc.push_record(&rec.vector, &rec.neighbors)?;
    }
    let (crc, body) = enc.finish()?;
    debug_assert_eq!(
        crc,
        crc32fast::hash(&body),
        "incremental and one-shot CRC32 must agree"
    );
    Ok(body)
}

/// Streaming snapshot-body encoder (§3/§7) — **the single write-path
/// implementation**. [`encode_snapshot_body`] streams into a `Vec` through
/// it; [`crate::snapshot::save`] streams into a file through it (2026-09-02
/// Stage C review P2-1: no `Vec<NodeRecord>` materialization, so save's
/// memory peak is graph + 1 byte/node + one record + the `BufWriter`
/// buffer instead of ~3× the file size).
///
/// The encoder writes **body bytes only** (header + node records, §3) and
/// computes the CRC32 incrementally alongside; [`BodyEncoder::finish`]
/// returns the hash. The on-disk §7 layout is `crc32 prefix + body`, so a
/// file-writing caller reserves 4 placeholder bytes up front and
/// backpatches the CRC after finishing — the resulting bytes are identical
/// to `crc LE ++ body` from [`wrap_crc32`] (the format stays frozen).
///
/// Validation is inlined into the stream and is exactly the graph-data
/// validation the read path runs (write/read symmetry, shared helpers with
/// `validate_graph_data`): the construction-parameter re-run and the
/// entry-point/`max_level` relations at [`new`](BodyEncoder::new) (against
/// the whole-graph `levels` summary), per-record checks at
/// [`push_record`](BodyEncoder::push_record) (vector length/finiteness,
/// `level_count >= 1`, top level ≤ `max_level`, adjacency ascending without
/// duplicates or self-loops, endpoints < `node_count`, targets having the
/// edge's level, degree caps). Stream-level checks (magic, format version,
/// CRC, truncation) stay decode-only. A validation failure means the caller
/// has already written a prefix of the body — harmless behind
/// `save`'s write-temp-then-rename protocol, which discards the partial
/// temporary file and never touches the target.
pub struct BodyEncoder<W: Write> {
    inner: W,
    hasher: crc32fast::Hasher,
    header: SnapshotHeader,
    /// Whole-graph top-level summary, 1 byte/node (`levels[i]` = node `i`'s
    /// top level == level_count − 1): supports the entry/`max_level`
    /// relations and the "target has the edge's level" check without
    /// materializing node records.
    levels: Vec<u8>,
    written: u32,
    scratch: Vec<u8>,
}

impl<W: Write> BodyEncoder<W> {
    /// Validate the header (construction-parameter re-run, record count, and
    /// the entry-point/`max_level` relations against `levels`) and write
    /// the fixed-width header. `levels.len()` must equal
    /// `header.node_count`; `levels[i]` is node `i`'s top level.
    pub fn new(header: SnapshotHeader, levels: Vec<u8>, inner: W) -> Result<Self> {
        header.validate_construction_params()?;
        if header.node_count as usize != levels.len() {
            return Err(HnswError::Corrupted(format!(
                "header node_count {} != {} node records",
                header.node_count,
                levels.len()
            )));
        }
        validate_entry_and_max_level(&header, &levels)?;
        let mut enc = Self {
            inner,
            hasher: crc32fast::Hasher::new(),
            header,
            levels,
            written: 0,
            scratch: Vec::new(),
        };
        let header_bytes = header.encode();
        enc.write_hashed(&header_bytes)?;
        Ok(enc)
    }

    /// Validate and append one node record. Position is identity (§3): the
    /// next record pushed *is* `NodeId(self.written)`, which is also the
    /// index the validation messages report.
    pub fn push_record(&mut self, vector: &[f32], neighbors: &[Vec<NodeId>]) -> Result<()> {
        if self.written == self.header.node_count {
            return Err(HnswError::Corrupted(format!(
                "more node records pushed than header node_count {}",
                self.header.node_count
            )));
        }
        let i = self.written as usize;
        // The caller-supplied `levels` summary must agree with the record
        // actually being pushed (2026-09-02 review): decode derives the
        // per-node top levels from the records themselves, so a
        // summary/record mismatch would emit a body the read path rejects
        // (or accepts under different entry/max_level semantics) — the
        // write/read symmetry contract requires failing here instead. With
        // this check, "top level <= max_level" (validated on the summary at
        // construction) also holds per record.
        if neighbors.len() != usize::from(self.levels[i]) + 1 {
            return Err(HnswError::Corrupted(format!(
                "node {i}: record level_count {} disagrees with the levels summary ({}); the summary and the records must agree (§3)",
                neighbors.len(),
                usize::from(self.levels[i]) + 1
            )));
        }
        validate_record_basics(&self.header, i, vector, neighbors.len())?;
        validate_adjacency(&self.header, &self.levels, i, neighbors)?;
        self.scratch.clear();
        encode_node_record(&mut self.scratch, self.header.dim, vector, neighbors)?;
        self.hasher.update(&self.scratch);
        self.inner.write_all(&self.scratch)?;
        self.written += 1;
        Ok(())
    }

    /// Verify the record count and finish: returns the incremental CRC32 of
    /// everything written (the §7 prefix value) and the underlying writer.
    pub fn finish(self) -> Result<(u32, W)> {
        if self.written != self.header.node_count {
            return Err(HnswError::Corrupted(format!(
                "header node_count {} != {} node records",
                self.header.node_count, self.written
            )));
        }
        Ok((self.hasher.finalize(), self.inner))
    }

    fn write_hashed(&mut self, bytes: &[u8]) -> Result<()> {
        self.hasher.update(bytes);
        self.inner.write_all(bytes)?;
        Ok(())
    }
}

/// Graph-data validation shared by the write and read paths (2026-08-31
/// review P2 ×2). Stream-level checks (magic / version / CRC / truncation /
/// trailing bytes) stay decode-only; everything about the *graph's*
/// well-formedness lives in the three helpers below, which this function
/// composes for the materialized-record (decode) path and [`BodyEncoder`]
/// composes for the streaming (write) path (2026-09-02 Stage C review
/// P2-1 — one validation implementation, no drift):
///
/// - record count matches `node_count`;
/// - every record: `vector.len() == dim`, all components finite,
///   `level_count >= 1` ([`validate_record_basics`]);
/// - empty graph: entry-point sentinel + `max_level == 0`; non-empty:
///   `entry_point < node_count`, `max_level` == the entry node's top level,
///   and no node's top level exceeds `max_level`
///   ([`validate_entry_and_max_level`]);
/// - every adjacency list: endpoints exist (`< node_count`); the target has
///   the edge's level (`level_count > level`, checklist item 10); the list
///   is **strictly ascending** (the canonical order `push_edge`'s binary
///   search assumes — no duplicates, no descent); **no self-loops**; and
///   **degree ≤ m_max(level)** (`m_max0` on level 0, `m` elsewhere — a
///   quiescent graph never exceeds the cap; the insert-time `m_max + 1`
///   transient is unobservable behind `&mut self`)
///   ([`validate_adjacency`]).
fn validate_graph_data(header: &SnapshotHeader, nodes: &[NodeRecord]) -> Result<()> {
    if header.node_count as usize != nodes.len() {
        return Err(HnswError::Corrupted(format!(
            "header node_count {} != {} node records",
            header.node_count,
            nodes.len()
        )));
    }
    for (i, rec) in nodes.iter().enumerate() {
        validate_record_basics(header, i, &rec.vector, rec.neighbors.len())?;
    }
    // Safe `as u8`: the decode path is the only caller, and there
    // `level_count` is a u8 by construction (≤ 255 lists per record).
    let levels: Vec<u8> = nodes
        .iter()
        .map(|rec| (rec.neighbors.len() - 1) as u8)
        .collect();
    validate_entry_and_max_level(header, &levels)?;
    for (i, rec) in nodes.iter().enumerate() {
        validate_adjacency(header, &levels, i, &rec.neighbors)?;
    }
    Ok(())
}

/// Per-record basics shared by the read and write paths: exact `dim` match,
/// all components finite (§5/§7 NaN/±inf rejection), and
/// `1 <= level_count <= MAX_LEVEL_COUNT` (level 0 always exists, §3; the
/// upper bound is checklist item 12, 2026-09-02 review P2-2). The decode
/// path has an earlier line of defense for the cap — `decode_node_record`
/// rejects it at the parse point, before allocating per-level Vecs (review
/// round 4 P2-2); this instance covers the write / one-shot paths, which
/// never pass through the parser. Two lines of defense, one rule, one
/// constant.
fn validate_record_basics(
    header: &SnapshotHeader,
    i: usize,
    vector: &[f32],
    level_count: usize,
) -> Result<()> {
    if vector.len() != usize::from(header.dim) {
        return Err(HnswError::Corrupted(format!(
            "node {i}: vector has {} components, header dim is {} (§3)",
            vector.len(),
            header.dim
        )));
    }
    for (c, &x) in vector.iter().enumerate() {
        if !x.is_finite() {
            return Err(HnswError::Corrupted(format!(
                "node {i}: non-finite component (NaN or ±inf) at index {c} (§5)"
            )));
        }
    }
    if level_count == 0 {
        return Err(HnswError::Corrupted(format!(
            "node {i}: level_count = 0 (level 0 always exists, §3)"
        )));
    }
    if level_count > MAX_LEVEL_COUNT {
        return Err(HnswError::Corrupted(format!(
            "node {i}: level_count {level_count} > {MAX_LEVEL_COUNT} (checklist item 12: the geometric draw is hard-bounded at level 53 even at M = 2, so no legal graph can reach this, §4.1)"
        )));
    }
    Ok(())
}

/// Entry-point/`max_level` relations against the whole-graph top-level
/// summary (`levels[i]` = node `i`'s top level; `levels.len()` is already
/// known to equal `node_count`): empty graphs carry the sentinel and
/// `max_level == 0`; non-empty graphs have `entry_point < node_count`,
/// `max_level` == the entry node's top level, and no node above
/// `max_level` (the entry point is the graph's highest node).
fn validate_entry_and_max_level(header: &SnapshotHeader, levels: &[u8]) -> Result<()> {
    if header.node_count == 0 {
        if header.entry_point != EMPTY_GRAPH_ENTRY_POINT {
            return Err(HnswError::Corrupted(format!(
                "empty graph must encode entry_point = u32::MAX sentinel, got 0x{:08x} (§3)",
                header.entry_point
            )));
        }
        if header.max_level != 0 {
            return Err(HnswError::Corrupted(format!(
                "empty graph must encode max_level = 0, got {} (no nodes, no levels)",
                header.max_level
            )));
        }
    } else {
        if header.entry_point >= header.node_count {
            return Err(HnswError::Corrupted(format!(
                "entry_point {} >= node_count {} (§3)",
                header.entry_point, header.node_count
            )));
        }
        let entry_top = levels[header.entry_point as usize];
        if header.max_level != entry_top {
            return Err(HnswError::Corrupted(format!(
                "max_level {} != entry node's top level {entry_top} (§3)",
                header.max_level
            )));
        }
    }
    for (i, &top) in levels.iter().enumerate() {
        if top > header.max_level {
            return Err(HnswError::Corrupted(format!(
                "node {i} top level {top} exceeds max_level {} (the entry point is the highest node)",
                header.max_level
            )));
        }
    }
    Ok(())
}

/// Per-node adjacency well-formedness shared by the read and write paths
/// (checklist items 10–11): degree ≤ `m_max(level)`, no self-loops,
/// strictly ascending (the canonical order — no duplicates, no descent),
/// endpoints exist, and the target *has* the edge's level (HNSW edges only
/// ever connect nodes that both have the level).
fn validate_adjacency(
    header: &SnapshotHeader,
    levels: &[u8],
    i: usize,
    neighbors: &[Vec<NodeId>],
) -> Result<()> {
    for (level, list) in neighbors.iter().enumerate() {
        let cap = if level == 0 {
            usize::from(header.m_max0)
        } else {
            usize::from(header.m)
        };
        if list.len() > cap {
            return Err(HnswError::Corrupted(format!(
                "node {i} level {level}: degree {} exceeds m_max({level}) = {cap} (a quiescent graph never exceeds the cap, §4.2)",
                list.len()
            )));
        }
        for (j, n) in list.iter().enumerate() {
            if n.0 as usize == i {
                return Err(HnswError::Corrupted(format!(
                    "node {i} level {level}: self-loop (an adjacency list never contains its owner, §3)"
                )));
            }
            if j > 0 && list[j - 1] >= *n {
                return Err(HnswError::Corrupted(format!(
                    "node {i} level {level}: adjacency list not strictly ascending ({:?} then {:?} — the canonical order has no duplicates and no descent, §3)",
                    list[j - 1], *n
                )));
            }
            if n.0 >= header.node_count {
                return Err(HnswError::Corrupted(format!(
                    "node {i} level {level}: neighbor {} >= node_count {} (adjacency endpoint does not exist)",
                    n.0, header.node_count
                )));
            }
            // Checklist item 10: an edge on `level` requires the target to
            // *have* that level (2026-08-31 review P2). `levels[target]`
            // is the target's top level, so `level > levels[target]` is
            // exactly "level_count <= level".
            if level > usize::from(levels[n.0 as usize]) {
                return Err(HnswError::Corrupted(format!(
                    "node {i} level {level}: neighbor {} has no level {level} (its level_count is {}; an edge requires both endpoints to have the level, §3)",
                    n.0,
                    usize::from(levels[n.0 as usize]) + 1
                )));
            }
        }
    }
    Ok(())
}

/// Decode a snapshot body and run the **full load-validation checklist**
/// (§3, coding plan Stage A):
///
/// 1. magic / format_version (via [`SnapshotHeader::decode`]);
/// 2. construction-parameter re-run (`m >= 2`, `ef_construction >= m`,
///    `dim != 0`);
/// 3. `flags` / `reserved` == 0 on every record;
/// 4. `level_count >= 1` (the `level_count == top level + 1` identity — the
///    stream encodes level lists contiguously from level 0, so a record's
///    top level is exactly `level_count - 1`);
/// 5. non-finite (NaN / ±inf) component rejection on every vector;
/// 6. `node_count` fits the remaining body at the minimum record size
///    (`2 + 4*dim + 1 + 2` bytes) — the header is untrusted input, so the
///    pre-allocation must be bounded by the actual body length, not by the
///    claimed count (2026-08-31 review: a legal-CRC header claiming
///    billions of nodes over a short body otherwise triggers a giant
///    allocation and aborts the process);
/// 7. exactly `node_count` records, no trailing bytes — NodeId density is
///    thereby guaranteed by position-is-identity (record i = `NodeId(i)`);
/// 8. empty graph: `entry_point == u32::MAX` sentinel and `max_level == 0`;
///    non-empty: `entry_point < node_count`;
/// 9. `max_level` == the entry node's top level, and no node's top level
///    exceeds `max_level` (the entry point is the graph's highest node);
/// 10. every adjacency endpoint exists (`< node_count`) **and has the
///     edge's level**: a level-L edge requires the target's
///     `level_count > L` (HNSW edges only ever connect nodes that both have
///     the level). Without the level half a legal-CRC snapshot passes decode
///     and the rebuilt graph panics on traversal (2026-08-31 review P2);
/// 11. every adjacency list is **well-formed** (2026-08-31 review P2):
///     strictly ascending (the canonical order `push_edge`'s binary search
///     assumes — no duplicates, no descent), no self-loops, and
///     degree ≤ `m_max(level)` (`m_max0` on level 0, `m` elsewhere).
/// 12. every record's `level_count <= 64` ([`MAX_LEVEL_COUNT`], 2026-09-02
///     review P2-2): the geometric level draw is hard-bounded at level 53
///     even at M = 2 (§4.1), so no legal graph can exceed this — the cap
///     only rejects pathological legal-CRC input whose per-level `Vec`
///     headers would otherwise amplify memory ~12× over the on-disk bytes
///     at the SoA rebuild.
///
/// Items 2 and 8–11 live in `validate_graph_data`, which
/// [`encode_snapshot_body`] also runs (write/read symmetry: the write path
/// refuses anything the read path would reject, so a producer-side bug
/// fails at write time instead of materializing as a corrupt file).
pub fn decode_snapshot_body(body: &[u8]) -> Result<SnapshotFileData> {
    let header = SnapshotHeader::decode(body)?;
    let mut cursor = &body[SNAPSHOT_HEADER_SIZE..];
    // Checklist item 6: bound the untrusted node_count against the remaining
    // body BEFORE pre-allocating (see the checklist above).
    let min_record_size = 2 + 4 * usize::from(header.dim) + 1 + 2;
    if header.node_count as usize > cursor.len() / min_record_size {
        return Err(HnswError::Corrupted(format!(
            "node_count {} exceeds the {} records that fit in the {} remaining body bytes (min record size {min_record_size})",
            header.node_count,
            cursor.len() / min_record_size,
            cursor.len()
        )));
    }
    let mut nodes = Vec::with_capacity(header.node_count as usize);
    for i in 0..header.node_count {
        let (rec, used) = decode_node_record(cursor, header.dim).map_err(|e| match e {
            HnswError::Corrupted(msg) => HnswError::Corrupted(format!("node record {i}: {msg}")),
            other => other,
        })?;
        cursor = &cursor[used..];
        nodes.push(rec);
    }
    if !cursor.is_empty() {
        return Err(HnswError::Corrupted(format!(
            "{} trailing bytes after {} node records (position is identity — the stream must end exactly)",
            cursor.len(),
            header.node_count
        )));
    }

    // Checklist items 8–11: graph-data validation, shared with the write
    // path (see `validate_graph_data`).
    validate_graph_data(&header, &nodes)?;

    Ok(SnapshotFileData { header, nodes })
}

/// Wrap a body in the CRC32 prefix (`crc32(4B LE) + body`, §7 — the same
/// convention as pg-storage's checkpoint.rs and FreelistMeta).
pub fn wrap_crc32(body: &[u8]) -> Vec<u8> {
    let crc = crc32fast::hash(body);
    let mut out = Vec::with_capacity(CRC32_SIZE + body.len());
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(body);
    out
}

/// Strip and verify the CRC32 prefix; returns the body on success
/// ([`HnswError::ChecksumMismatch`] on bit-rot — never a "valid but wrong"
/// graph, §7).
pub fn unwrap_crc32(bytes: &[u8]) -> Result<&[u8]> {
    if bytes.len() < CRC32_SIZE {
        return Err(HnswError::Corrupted(format!(
            "too short for the CRC32 prefix: {} bytes",
            bytes.len()
        )));
    }
    let (prefix, body) = bytes.split_at(CRC32_SIZE);
    let stored = u32::from_le_bytes(prefix.try_into().unwrap());
    let computed = crc32fast::hash(body);
    if stored != computed {
        return Err(HnswError::ChecksumMismatch { stored, computed });
    }
    Ok(body)
}

/// Maximum legal **body** size (header + records, no CRC prefix) for a
/// snapshot with this (already header-validated) header — 2026-09-02 review
/// round 4 P2-1.
///
/// Per-record maximum, from the frozen checklist: flags + reserved (2 B),
/// the vector (4·dim B), the level_count byte (1 B), level 0's neighbor
/// list (2 B count + 4·m_max0 B of ids — the degree cap of checklist item
/// 11), and at most 63 further levels (checklist item 12 caps
/// `level_count` at 64) at 2 + 4·m bytes each. All arithmetic is u64:
/// node_count (u32) × the per-record maximum cannot overflow.
///
/// Legality: encode produces exact sizes and decode rejects trailing bytes
/// (checklist item 7), so the [min, max] interval does not shrink the
/// legal acceptance set — [`crate::snapshot::load`] uses it to turn
/// "reject after reading the whole file" into "reject without reading"
/// for files that claim a small header but are physically huge (trailing
/// garbage or a hostile sparse file).
pub fn max_body_size(header: &SnapshotHeader) -> u64 {
    let per_record_max = 2
        + 4 * u64::from(header.dim)
        + 1
        + (2 + 4 * u64::from(header.m_max0))
        + 63 * (2 + 4 * u64::from(header.m));
    SNAPSHOT_HEADER_SIZE as u64 + u64::from(header.node_count) * per_record_max
}

/// Records-only counterpart of [`max_body_size`] (2026-09-02 review round
/// 7): `max_body_size` counts header + records — the CRC-protected body in
/// [`unwrap_crc32`]'s sense — while [`crate::snapshot::load`]'s pre-read
/// length cross-checks hold **records-only** lengths (file length minus
/// the 29-byte CRC+header prefix). Comparing across the two conventions
/// leaves the pre-read gate 25 bytes loose (the safe direction — the CRC
/// still rejects the over-read tail — but not the exact ceiling the
/// format promises). Subtract the header and compare like with like;
/// saturating for symmetry with the rest of the length arithmetic.
pub fn max_records_size(header: &SnapshotHeader) -> u64 {
    max_body_size(header).saturating_sub(SNAPSHOT_HEADER_SIZE as u64)
}

/// Conservative upper bound on the **in-memory footprint** of loading a
/// snapshot with this (already header-validated) header — 2026-09-02
/// review round 6 P2. A file-size budget cannot express this: the §6 SoA
/// materialization amplifies the on-disk bytes (a per-level `Vec` header
/// is 24 B vs 2 B on disk), so the bound is derived from the header and
/// the frozen caps alone. Per node:
///
/// - vector bytes `4·dim`, charged twice (the SoA arena + the decode-time
///   `NodeRecord` heap — both coexist transiently during load);
/// - the `levels` byte;
/// - `Vec` headers: the adjacency outer `Vec` (24 B) + the decode-time
///   `NodeRecord` struct (two `Vec` headers, 48 B) + its neighbors outer
///   `Vec` (24 B);
/// - per-level inner `Vec` headers: 24 B × [`MAX_LEVEL_COUNT`], charged
///   twice (SoA + decode) — the conservative part: a real graph has
///   ≈ log_M N levels, far below 64;
/// - neighbor-id payload: `4·m_max0` (level 0) + `63 × 4·m` (checklist
///   item 11 degree caps), also charged twice.
///
/// All arithmetic is u64 (`node_count` is u32; the product cannot
/// overflow). This is deliberately an upper bound, not an estimate of the
/// realistic shape — [`crate::snapshot::load_with_budget`] compares it
/// against the caller's memory budget before any decode/materialization.
pub fn max_memory_estimate(header: &SnapshotHeader) -> u64 {
    let dim = u64::from(header.dim);
    let (m, m_max0) = (u64::from(header.m), u64::from(header.m_max0));
    let per_node = 2 * (4 * dim) // arena vector + decode-time vector heap
        + 1 // levels byte
        + 24 + 48 + 24 // adjacency outer + NodeRecord struct + decode neighbors outer
        + 2 * 24 * MAX_LEVEL_COUNT as u64 // per-level Vec headers, SoA + decode
        + 2 * (4 * m_max0 + 63 * 4 * m); // neighbor-id payload, SoA + decode
    u64::from(header.node_count) * per_node
}

/// The [`HnswParams`] a snapshot restores (§3: load adopts the snapshot's
/// construction parameters). `ef_search_default` is query-time state and
/// stays out of the snapshot, so it is an explicit caller input — a
/// hard-coded default would fail §4.4's `ef_search_default >= m` check on
/// legal graphs whose `m` exceeds that default. Stage C uses this when
/// rebuilding the graph.
pub fn params_from_header(header: &SnapshotHeader, ef_search_default: u32) -> Result<HnswParams> {
    HnswParams::new(
        header.m,
        header.m_max0,
        header.ef_construction,
        ef_search_default,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_header() -> SnapshotHeader {
        SnapshotHeader {
            dim: 2,
            m: 16,
            m_max0: 32,
            ef_construction: 200,
            node_count: 3,
            entry_point: 2,
            max_level: 1,
        }
    }

    /// node 2 is the two-level entry point; every other node lives on
    /// level 0 only — so node 2's level-1 list is necessarily EMPTY: an
    /// edge requires both endpoints to have the level (checklist item 10,
    /// 2026-08-31 review P2 — this fixture previously carried a level-1
    /// edge to node 1, which only has level 0; the graph was semantically
    /// invalid all along and the new check caught it).
    fn sample_nodes() -> Vec<NodeRecord> {
        vec![
            NodeRecord {
                vector: vec![0.0, 0.0],
                neighbors: vec![vec![NodeId(1), NodeId(2)]],
            },
            NodeRecord {
                vector: vec![1.0, 0.0],
                neighbors: vec![vec![NodeId(0), NodeId(2)]],
            },
            NodeRecord {
                vector: vec![0.5, 1.0],
                neighbors: vec![vec![NodeId(0), NodeId(1)], vec![]],
            },
        ]
    }

    fn sample_body() -> Vec<u8> {
        encode_snapshot_body(&sample_header(), &sample_nodes()).unwrap()
    }

    /// Round 7: `max_records_size` is `max_body_size` minus exactly the
    /// fixed-width header — the records-only ceiling `snapshot::load`'s
    /// pre-read gate compares against. An empty graph has zero legal
    /// records bytes, so ANY trailing byte crosses the boundary.
    #[test]
    fn max_records_size_is_header_exclusive() {
        let h = sample_header();
        assert_eq!(
            max_records_size(&h),
            max_body_size(&h) - SNAPSHOT_HEADER_SIZE as u64
        );
        let mut empty = h;
        empty.node_count = 0;
        empty.entry_point = u32::MAX; // the empty-graph sentinel
        empty.max_level = 0;
        assert_eq!(max_records_size(&empty), 0);
    }

    /// Encode WITHOUT the graph-data validation (test-only): produces the
    /// byte streams `encode_snapshot_body` now refuses, so decode-side
    /// negative tests still have invalid-but-well-formed streams to reject
    /// (2026-08-31 review P2: encode and decode share `validate_graph_data`).
    fn raw_body(header: &SnapshotHeader, nodes: &[NodeRecord]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&header.encode());
        for rec in nodes {
            encode_node_record(&mut out, header.dim, &rec.vector, &rec.neighbors).unwrap();
        }
        out
    }

    /// Variant-level negative assertion (2026-08-31 review): the error must
    /// be the `Corrupted` variant AND its message must name the specific
    /// check that fired — most load checks share the variant, so the
    /// substring still discriminates *which* check tripped.
    fn assert_corrupted(err: &HnswError, needle: &str) {
        assert!(
            matches!(err, HnswError::Corrupted(_)),
            "unexpected variant: {err}"
        );
        let msg = err.to_string();
        assert!(msg.contains(needle), "unexpected message: {msg}");
    }

    // ---- round-trips ----

    #[test]
    fn header_round_trip() {
        let h = sample_header();
        assert_eq!(SnapshotHeader::decode(&h.encode()).unwrap(), h);
    }

    #[test]
    fn node_record_round_trip() {
        let rec = &sample_nodes()[2];
        let mut buf = Vec::new();
        encode_node_record(&mut buf, 2, &rec.vector, &rec.neighbors).unwrap();
        let (decoded, consumed) = decode_node_record(&buf, 2).unwrap();
        assert_eq!(&decoded, rec);
        assert_eq!(consumed, buf.len());
    }

    #[test]
    fn body_round_trip_with_crc() {
        let (header, nodes) = (sample_header(), sample_nodes());
        let body = encode_snapshot_body(&header, &nodes).unwrap();
        let wrapped = wrap_crc32(&body);
        let data = decode_snapshot_body(unwrap_crc32(&wrapped).unwrap()).unwrap();
        assert_eq!(data.header, header);
        assert_eq!(data.nodes, nodes);
    }

    #[test]
    fn body_encoder_matches_one_shot_and_checks_counts() {
        // 2026-09-02 Stage C review P2-1: the streaming encoder is the
        // single write-path implementation — its bytes and CRC must equal
        // the one-shot wrapper's, and its count checks must fire loudly.
        let (header, nodes) = (sample_header(), sample_nodes());
        let levels: Vec<u8> = nodes
            .iter()
            .map(|r| (r.neighbors.len() - 1) as u8)
            .collect();
        let mut enc = BodyEncoder::new(header, levels.clone(), Vec::new()).unwrap();
        for rec in &nodes {
            enc.push_record(&rec.vector, &rec.neighbors).unwrap();
        }
        let (crc, body) = enc.finish().unwrap();
        assert_eq!(body, encode_snapshot_body(&header, &nodes).unwrap());
        assert_eq!(crc, crc32fast::hash(&body));
        // ... and the CRC is exactly the §7 prefix wrap_crc32 would write.
        assert_eq!(wrap_crc32(&body)[..CRC32_SIZE], crc.to_le_bytes());

        // finish before all records are pushed: loud.
        let enc = BodyEncoder::new(header, levels.clone(), Vec::new()).unwrap();
        assert!(matches!(enc.finish(), Err(HnswError::Corrupted(_))));
        // pushing past node_count: loud.
        let mut enc = BodyEncoder::new(header, levels, Vec::new()).unwrap();
        for rec in &nodes {
            enc.push_record(&rec.vector, &rec.neighbors).unwrap();
        }
        assert!(matches!(
            enc.push_record(&nodes[0].vector, &nodes[0].neighbors),
            Err(HnswError::Corrupted(_))
        ));
        // levels/node_count mismatch at construction: loud.
        assert!(matches!(
            BodyEncoder::new(header, vec![0], Vec::new()),
            Err(HnswError::Corrupted(_))
        ));
    }

    #[test]
    fn body_encoder_rejects_summary_record_disagreement() {
        // 2026-09-02 review: the caller-supplied levels summary must agree
        // with each record pushed — otherwise the encoder would emit a body
        // its own decode rejects (write/read asymmetry). Both production
        // call sites are consistent by construction, but BodyEncoder is a
        // pub API, so the check is a real error, not a debug_assert.
        let (header, nodes) = (sample_header(), sample_nodes());
        let mut levels: Vec<u8> = nodes
            .iter()
            .map(|r| (r.neighbors.len() - 1) as u8)
            .collect();
        // Lie about node 0's top level (the summary stays self-consistent
        // with the header: node 0 is not the entry point and 1 <= max_level).
        levels[0] = 1;
        let mut enc = BodyEncoder::new(header, levels, Vec::new()).unwrap();
        let err = enc
            .push_record(&nodes[0].vector, &nodes[0].neighbors)
            .unwrap_err();
        assert!(
            matches!(err, HnswError::Corrupted(_))
                && err
                    .to_string()
                    .contains("disagrees with the levels summary"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn empty_graph_round_trip() {
        let header = SnapshotHeader {
            node_count: 0,
            entry_point: EMPTY_GRAPH_ENTRY_POINT,
            max_level: 0,
            ..sample_header()
        };
        let body = encode_snapshot_body(&header, &[]).unwrap();
        let data = decode_snapshot_body(&body).unwrap();
        assert_eq!(data.header, header);
        assert!(data.nodes.is_empty());
    }

    #[test]
    fn params_from_header_restores_construction_params() {
        // ef_search_default is not in the snapshot (§3) — caller-supplied.
        let p = params_from_header(&sample_header(), 64).unwrap();
        assert_eq!((p.m(), p.m_max0(), p.ef_construction()), (16, 32, 200));
        assert_eq!(p.ef_search_default(), 64);
    }

    // ---- decode robustness (2026-08-31 adversarial review) ----

    #[test]
    fn rejects_huge_node_count_with_short_body() {
        // Legal CRC + node_count = u32::MAX + a header-only body: pre-fix
        // this tried to pre-allocate node_count × size_of::<NodeRecord>()
        // (~192 GiB) and aborted the process. The node-count-vs-body-length
        // bound must reject it as Corrupted instead.
        let header = SnapshotHeader {
            node_count: u32::MAX,
            entry_point: 0,
            max_level: 0,
            ..sample_header()
        };
        let body = header.encode().to_vec(); // header only, no node records
        let wrapped = wrap_crc32(&body);
        assert_eq!(wrapped.len(), CRC32_SIZE + SNAPSHOT_HEADER_SIZE);
        let err = decode_snapshot_body(unwrap_crc32(&wrapped).unwrap()).unwrap_err();
        assert!(matches!(err, HnswError::Corrupted(_)), "unexpected: {err}");
        // Pre-fix the error is an incidental "truncated node record" from
        // record 0 (after the giant pre-allocation); post-fix the bound
        // check names node_count explicitly.
        let msg = err.to_string();
        assert!(msg.contains("node_count"), "unexpected: {msg}");
    }

    #[test]
    fn params_from_header_supports_m_above_64() {
        // m = 100 > 64 (the frozen ef_search_default) is a legal graph: the
        // snapshot passes every decode check, so rebuilding its params must
        // not fail on a hard-coded 64 < m.
        let header = SnapshotHeader {
            m: 100,
            m_max0: 200,
            ef_construction: 200,
            node_count: 0,
            entry_point: EMPTY_GRAPH_ENTRY_POINT,
            max_level: 0,
            ..sample_header()
        };
        let body = encode_snapshot_body(&header, &[]).unwrap();
        let data = decode_snapshot_body(&body).unwrap();
        let p = params_from_header(&data.header, 100).unwrap();
        assert_eq!(
            (
                p.m(),
                p.m_max0(),
                p.ef_construction(),
                p.ef_search_default()
            ),
            (100, 200, 200, 100)
        );
        // The §4.4 check still applies — to the caller's value, not a
        // hard-coded 64 (which was < m here and made every m > 64 snapshot
        // unloadable pre-fix).
        let err = params_from_header(&data.header, 64).unwrap_err();
        assert!(
            matches!(err, HnswError::InvalidParams(_)),
            "unexpected: {err}"
        );
    }

    // ---- header validation negatives ----

    #[test]
    fn rejects_bad_magic() {
        let mut body = sample_body();
        body[0] ^= 0xff;
        assert_corrupted(&decode_snapshot_body(&body).unwrap_err(), "bad magic");
    }

    #[test]
    fn rejects_unknown_format_version() {
        let mut body = sample_body();
        body[4..6].copy_from_slice(&99u16.to_le_bytes());
        assert_corrupted(&decode_snapshot_body(&body).unwrap_err(), "format_version");
    }

    #[test]
    fn rejects_truncated_header() {
        let body = sample_body();
        assert_corrupted(
            &decode_snapshot_body(&body[..SNAPSHOT_HEADER_SIZE - 1]).unwrap_err(),
            "truncated",
        );
    }

    #[test]
    fn rejects_dim_zero_in_header() {
        let mut body = sample_body();
        body[6..8].copy_from_slice(&0u16.to_le_bytes());
        assert_corrupted(&decode_snapshot_body(&body).unwrap_err(), "dim = 0");
    }

    #[test]
    fn rejects_m_below_two_in_header() {
        let mut body = sample_body();
        body[8..10].copy_from_slice(&1u16.to_le_bytes());
        assert_corrupted(&decode_snapshot_body(&body).unwrap_err(), "M = 1 < 2");
    }

    #[test]
    fn rejects_ef_construction_below_m_in_header() {
        let mut body = sample_body();
        body[12..16].copy_from_slice(&4u32.to_le_bytes());
        assert_corrupted(&decode_snapshot_body(&body).unwrap_err(), "ef_construction");
    }

    #[test]
    fn rejects_m_max0_below_m_in_header() {
        // §3 v1.7: m_max0 is re-validated on load like every other
        // construction parameter (m = 16 in `sample_body`).
        let mut body = sample_body();
        body[10..12].copy_from_slice(&8u16.to_le_bytes());
        assert_corrupted(
            &decode_snapshot_body(&body).unwrap_err(),
            "M_max0 = 8 < M = 16",
        );
        let mut body = sample_body();
        body[10..12].copy_from_slice(&0u16.to_le_bytes());
        assert_corrupted(&decode_snapshot_body(&body).unwrap_err(), "M_max0 = 0");
    }

    // ---- entry-point / max_level negatives ----

    #[test]
    fn rejects_entry_point_out_of_range() {
        let mut body = sample_body();
        body[20..24].copy_from_slice(&3u32.to_le_bytes()); // == node_count
        assert_corrupted(&decode_snapshot_body(&body).unwrap_err(), "entry_point");
    }

    #[test]
    fn rejects_empty_graph_without_sentinel() {
        let mut body = encode_snapshot_body(
            &SnapshotHeader {
                node_count: 0,
                entry_point: EMPTY_GRAPH_ENTRY_POINT,
                max_level: 0,
                ..sample_header()
            },
            &[],
        )
        .unwrap();
        body[20..24].copy_from_slice(&0u32.to_le_bytes());
        assert_corrupted(&decode_snapshot_body(&body).unwrap_err(), "sentinel");
    }

    #[test]
    fn rejects_empty_graph_with_nonzero_max_level() {
        let mut body = encode_snapshot_body(
            &SnapshotHeader {
                node_count: 0,
                entry_point: EMPTY_GRAPH_ENTRY_POINT,
                max_level: 0,
                ..sample_header()
            },
            &[],
        )
        .unwrap();
        body[24] = 3;
        assert_corrupted(&decode_snapshot_body(&body).unwrap_err(), "max_level");
    }

    #[test]
    fn rejects_max_level_mismatch_with_entry_node() {
        let mut body = sample_body();
        body[24] = 0; // entry node's top level is 1
        assert_corrupted(&decode_snapshot_body(&body).unwrap_err(), "max_level");
    }

    #[test]
    fn rejects_node_above_max_level() {
        // entry (node 2) has top level 1 == max_level, but node 0 climbs to
        // level 2 — the entry point must be the graph's highest node.
        // `raw_body` bypasses the (shared) encode-side validation, which
        // rejects this too — both halves are asserted below.
        let mut nodes = sample_nodes();
        nodes[0].neighbors.push(vec![NodeId(2)]);
        nodes[0].neighbors.push(vec![NodeId(2)]);
        let body = raw_body(&sample_header(), &nodes);
        assert_corrupted(
            &decode_snapshot_body(&body).unwrap_err(),
            "exceeds max_level",
        );
        assert_corrupted(
            &encode_snapshot_body(&sample_header(), &nodes).unwrap_err(),
            "exceeds max_level",
        );
    }

    // ---- node-record negatives (offsets: header = 25 bytes; node 0 starts
    // at 25: flags@25, reserved@26, vector@27..35, level_count@35,
    // neighbor_count@36..38, neighbors@38..) ----

    #[test]
    fn rejects_nonzero_flags() {
        let mut body = sample_body();
        body[25] = 1;
        assert_corrupted(&decode_snapshot_body(&body).unwrap_err(), "flags");
    }

    #[test]
    fn rejects_nonzero_reserved() {
        let mut body = sample_body();
        body[26] = 1;
        assert_corrupted(&decode_snapshot_body(&body).unwrap_err(), "reserved");
    }

    #[test]
    fn rejects_nan_vector_component() {
        let mut body = sample_body();
        body[27..31].copy_from_slice(&f32::NAN.to_le_bytes());
        assert_corrupted(&decode_snapshot_body(&body).unwrap_err(), "non-finite");
    }

    #[test]
    fn rejects_inf_vector_component() {
        // ±inf is rejected with the same non-finite semantics as NaN —
        // cosine's inf/inf would otherwise silently produce NaN (§5).
        let mut body = sample_body();
        body[27..31].copy_from_slice(&f32::INFINITY.to_le_bytes());
        assert_corrupted(&decode_snapshot_body(&body).unwrap_err(), "non-finite");
        let mut body = sample_body();
        body[27..31].copy_from_slice(&f32::NEG_INFINITY.to_le_bytes());
        assert_corrupted(&decode_snapshot_body(&body).unwrap_err(), "non-finite");
    }

    #[test]
    fn rejects_level_count_zero() {
        let mut body = sample_body();
        body[35] = 0;
        assert_corrupted(&decode_snapshot_body(&body).unwrap_err(), "level_count = 0");
    }

    #[test]
    fn rejects_neighbor_id_out_of_range() {
        let mut body = sample_body();
        body[38..42].copy_from_slice(&99u32.to_le_bytes());
        assert_corrupted(&decode_snapshot_body(&body).unwrap_err(), "does not exist");
    }

    #[test]
    fn rejects_edge_to_node_without_that_level() {
        // 2026-08-31 review P2: a level-1 edge whose target has level_count
        // 1 (level 0 only). The stream is well-formed with a legal CRC —
        // pre-fix it passed decode and the Stage C rebuild would panic
        // indexing `adjacency[target][1]` on traversal. `raw_body` bypasses
        // the encode-side half of the shared validation; both halves are
        // asserted.
        let header = SnapshotHeader {
            node_count: 2,
            entry_point: 1,
            max_level: 1,
            ..sample_header()
        };
        let nodes = vec![
            NodeRecord {
                vector: vec![0.0, 0.0],
                neighbors: vec![vec![]], // node 0: level 0 only
            },
            NodeRecord {
                vector: vec![1.0, 1.0],
                neighbors: vec![vec![], vec![NodeId(0)]], // node 1: level-1 edge to node 0
            },
        ];
        let body = raw_body(&header, &nodes);
        let err = decode_snapshot_body(&body).unwrap_err();
        assert!(matches!(err, HnswError::Corrupted(_)), "unexpected: {err}");
        assert_corrupted(&err, "has no level");
        assert_corrupted(
            &encode_snapshot_body(&header, &nodes).unwrap_err(),
            "has no level",
        );
    }

    #[test]
    fn rejects_malformed_adjacency() {
        // 2026-08-31 review P2: endpoint existence and level membership are
        // not enough — the lists themselves must be well-formed, because
        // `push_edge`'s binary search assumes the canonical order and Stage
        // C would otherwise insert edges at wrong positions *silently*.
        // Each case is checked at BOTH the read path (via `raw_body`) and
        // the write path (shared `validate_graph_data`).
        let header = sample_header();
        let case = |neighbors0: Vec<NodeId>| {
            let mut nodes = sample_nodes();
            nodes[0].neighbors[0] = neighbors0;
            nodes
        };
        // duplicate edge
        let nodes = case(vec![NodeId(1), NodeId(1)]);
        assert_corrupted(
            &decode_snapshot_body(&raw_body(&header, &nodes)).unwrap_err(),
            "not strictly ascending",
        );
        assert_corrupted(
            &encode_snapshot_body(&header, &nodes).unwrap_err(),
            "not strictly ascending",
        );
        // descending order
        let nodes = case(vec![NodeId(2), NodeId(1)]);
        assert_corrupted(
            &decode_snapshot_body(&raw_body(&header, &nodes)).unwrap_err(),
            "not strictly ascending",
        );
        // self-loop
        let nodes = case(vec![NodeId(0), NodeId(1)]);
        assert_corrupted(
            &decode_snapshot_body(&raw_body(&header, &nodes)).unwrap_err(),
            "self-loop",
        );
        // degree beyond the m_max cap (m_max0 = 32 in `sample_header`):
        // 33 ascending distinct neighbors, none a self-loop, all with the
        // level — every earlier check passes, only the cap fires.
        {
            let mut ns = sample_nodes();
            ns[0].neighbors[0] = (1..=33u32).map(NodeId).collect();
            // node_count must cover the endpoints; give every extra node a
            // minimal valid record (level 0, empty list).
            let header = SnapshotHeader {
                node_count: 34,
                ..sample_header()
            };
            while ns.len() < 34 {
                ns.push(NodeRecord {
                    vector: vec![0.0, 0.0],
                    neighbors: vec![vec![]],
                });
            }
            // entry point (node 2) relations are unchanged and still valid.
            assert_corrupted(
                &decode_snapshot_body(&raw_body(&header, &ns)).unwrap_err(),
                "exceeds m_max",
            );
            assert_corrupted(
                &encode_snapshot_body(&header, &ns).unwrap_err(),
                "exceeds m_max",
            );
        }
    }

    #[test]
    fn encode_rejects_unloadable_header() {
        // 2026-08-31 review P2: `SnapshotHeader` fields are pub, so an
        // incoherent header can be hand-built — pre-fix the write path
        // encoded it happily and only decode failed. The write path now
        // refuses anything its own decoder would reject.
        let bad_entry = SnapshotHeader {
            node_count: 0,
            entry_point: 7,
            max_level: 3,
            ..sample_header()
        };
        assert!(encode_snapshot_body(&bad_entry, &[]).is_err());
        let bad_params = SnapshotHeader {
            m: 1,
            ..sample_header()
        };
        assert!(matches!(
            encode_snapshot_body(&bad_params, &sample_nodes()).unwrap_err(),
            HnswError::Corrupted(_)
        ));
    }

    #[test]
    fn rejects_truncated_node_record() {
        let body = sample_body();
        assert_corrupted(
            &decode_snapshot_body(&body[..body.len() - 1]).unwrap_err(),
            "truncated",
        );
    }

    #[test]
    fn rejects_trailing_bytes() {
        let mut body = sample_body();
        body.push(0);
        assert_corrupted(&decode_snapshot_body(&body).unwrap_err(), "trailing");
    }

    // ---- CRC prefix ----

    #[test]
    fn rejects_crc_mismatch() {
        let mut wrapped = wrap_crc32(&sample_body());
        let last = wrapped.len() - 1;
        wrapped[last] ^= 0x01; // bit-rot in the body
        assert!(matches!(
            unwrap_crc32(&wrapped),
            Err(HnswError::ChecksumMismatch { .. })
        ));
    }

    #[test]
    fn rejects_crc_mismatch_in_prefix_itself() {
        let mut wrapped = wrap_crc32(&sample_body());
        wrapped[0] ^= 0x01; // bit-rot in the stored CRC
        assert!(matches!(
            unwrap_crc32(&wrapped),
            Err(HnswError::ChecksumMismatch { .. })
        ));
    }

    #[test]
    fn rejects_too_short_for_crc() {
        assert_corrupted(&unwrap_crc32(&[1, 2, 3]).unwrap_err(), "CRC32");
    }

    // ---- encode-side validation ----

    #[test]
    fn encode_rejects_node_count_mismatch() {
        // Variant note (2026-08-31): this check moved into
        // `validate_graph_data`, whose variant is `Corrupted` at both the
        // write and the read path.
        assert!(matches!(
            encode_snapshot_body(&sample_header(), &sample_nodes()[..2]),
            Err(HnswError::Corrupted(_))
        ));
    }

    #[test]
    fn encode_rejects_dim_mismatch() {
        let mut buf = Vec::new();
        assert!(matches!(
            encode_node_record(&mut buf, 3, &[1.0, 2.0], &[vec![]]),
            Err(HnswError::InvalidArgument(_))
        ));
    }

    #[test]
    fn encode_node_record_enforces_the_level_count_cap() {
        // 2026-09-02 review round 5 P3: the pub codec must refuse what the
        // decoder refuses — 65 levels is rejected (same MAX_LEVEL_COUNT
        // rule as the decoder's parse point), 64 passes (the boundary is
        // aligned with decode, which the integration suite's
        // level_count_at_cap_loads proves end to end).
        let mut buf = Vec::new();
        let err = encode_node_record(&mut buf, 2, &[1.0, 0.0], &vec![Vec::new(); 65]).unwrap_err();
        assert!(
            matches!(err, HnswError::InvalidArgument(_)),
            "unexpected variant: {err}"
        );
        assert!(
            err.to_string().contains("level_count"),
            "unexpected message: {err}"
        );
        let mut buf = Vec::new();
        encode_node_record(&mut buf, 2, &[1.0, 0.0], &vec![Vec::new(); 64]).unwrap();
    }

    #[test]
    fn encode_node_record_rejects_non_finite_components() {
        // 2026-09-02 review round 6 P3: NaN and ±inf are refused at encode
        // (the decoder rejects them at its parse point, §5/§7 — write/read
        // symmetry at the pub codec level).
        for bad in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
            let mut buf = Vec::new();
            let err = encode_node_record(&mut buf, 2, &[bad, 0.0], &[vec![]]).unwrap_err();
            assert!(
                matches!(err, HnswError::InvalidArgument(_)),
                "unexpected variant: {err}"
            );
            assert!(
                err.to_string().contains("non-finite"),
                "unexpected message: {err}"
            );
        }
        // Finite vectors (including -0.0) are unaffected.
        let mut buf = Vec::new();
        encode_node_record(&mut buf, 2, &[-0.0, 1.0], &[vec![]]).unwrap();
    }
}
