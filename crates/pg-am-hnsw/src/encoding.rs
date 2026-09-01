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
//!   level 0 always exists, so `level_count >= 1`.
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
//! is Stage C ([`crate::snapshot`]). The full load-validation checklist
//! lives in [`decode_snapshot_body`].

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
        // Re-run the construction checks the snapshot carries (§3). The
        // ef_search_default check has no snapshot-carried operand and stays
        // out by design.
        if header.dim == 0 {
            return Err(HnswError::Corrupted(
                "dim = 0 in snapshot header (§5: rejected at every entry point)".to_string(),
            ));
        }
        if header.m < 2 {
            return Err(HnswError::Corrupted(format!(
                "M = {} < 2 in snapshot header (construction validation re-run, §3)",
                header.m
            )));
        }
        if header.ef_construction < u32::from(header.m) {
            return Err(HnswError::Corrupted(format!(
                "ef_construction = {} < M = {} in snapshot header (construction validation re-run, §3)",
                header.ef_construction, header.m
            )));
        }
        if header.m_max0 < header.m {
            return Err(HnswError::Corrupted(format!(
                "M_max0 = {} < M = {} in snapshot header (construction validation re-run, §3 v1.7)",
                header.m_max0, header.m
            )));
        }
        Ok(header)
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
    if neighbors.len() > usize::from(u8::MAX) {
        return Err(HnswError::InvalidArgument(format!(
            "level_count {} does not fit u8",
            neighbors.len()
        )));
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
/// `level_count >= 1`, and **non-finite component rejection** (NaN or ±inf)
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
pub fn encode_snapshot_body(header: &SnapshotHeader, nodes: &[NodeRecord]) -> Result<Vec<u8>> {
    if header.node_count as usize != nodes.len() {
        return Err(HnswError::InvalidArgument(format!(
            "header node_count {} != {} node records",
            header.node_count,
            nodes.len()
        )));
    }
    let mut out = Vec::new();
    out.extend_from_slice(&header.encode());
    for rec in nodes {
        encode_node_record(&mut out, header.dim, &rec.vector, &rec.neighbors)?;
    }
    Ok(out)
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
/// 10. every adjacency endpoint exists (`< node_count`).
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
        let entry_top = nodes[header.entry_point as usize].neighbors.len() - 1;
        if usize::from(header.max_level) != entry_top {
            return Err(HnswError::Corrupted(format!(
                "max_level {} != entry node's top level {entry_top} (§3)",
                header.max_level
            )));
        }
    }

    for (i, rec) in nodes.iter().enumerate() {
        let top = rec.neighbors.len() - 1;
        if top > usize::from(header.max_level) {
            return Err(HnswError::Corrupted(format!(
                "node {i} top level {top} exceeds max_level {} (the entry point is the highest node)",
                header.max_level
            )));
        }
        for (level, list) in rec.neighbors.iter().enumerate() {
            for n in list {
                if n.0 >= header.node_count {
                    return Err(HnswError::Corrupted(format!(
                        "node {i} level {level}: neighbor {} >= node_count {} (adjacency endpoint does not exist)",
                        n.0, header.node_count
                    )));
                }
            }
        }
    }

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
    /// level 0 only.
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
                neighbors: vec![vec![NodeId(0), NodeId(1)], vec![NodeId(1)]],
            },
        ]
    }

    fn sample_body() -> Vec<u8> {
        encode_snapshot_body(&sample_header(), &sample_nodes()).unwrap()
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
        assert_eq!((p.m, p.m_max0, p.ef_construction), (16, 32, 200));
        assert_eq!(p.ef_search_default, 64);
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
            (p.m, p.m_max0, p.ef_construction, p.ef_search_default),
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
        let mut nodes = sample_nodes();
        nodes[0].neighbors.push(vec![NodeId(2)]);
        nodes[0].neighbors.push(vec![NodeId(2)]);
        let body = encode_snapshot_body(&sample_header(), &nodes).unwrap();
        assert_corrupted(
            &decode_snapshot_body(&body).unwrap_err(),
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
        assert!(matches!(
            encode_snapshot_body(&sample_header(), &sample_nodes()[..2]),
            Err(HnswError::InvalidArgument(_))
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
}
