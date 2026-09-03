//! Stage C snapshot round-trip suite (§7/§9): `save → load → search` must be
//! **bit-identical** to the in-memory original — the crash-injection
//! surrogate of coding plan Stage C.
//!
//! - Round-trip matrix: three metrics × three seeds × dim ∈ {2, 16, 128},
//!   N ≈ 1k (MixtureGen synthetic data, §8.3). Structure (params, entry
//!   point, max_level, per-node vector/adjacency) compares **bitwise**, and
//!   search results compare as exact `(NodeId, f64)` sequences — no epsilon:
//!   the loaded graph runs the same frozen §5 distance functions over
//!   bit-equal arenas, so divergence means a real bug, not float noise.
//! - Boundary shapes: empty graph (entry-point sentinel, max_level 0),
//!   single-node graph, multi-level graph (level ≥ 2 present).
//! - Read-only contract (coding plan Stage C conservative default): a loaded
//!   graph rejects `insert` with `InvalidOperation`; saving does not affect
//!   the original graph.
//! - Corruption negatives: bit flip / truncation / sub-CRC-length files, and
//!   forged legal-CRC bodies whose bytes bypass the encode-side validation
//!   (hand-assembled via `SnapshotHeader::encode` + `encode_node_record` +
//!   `wrap_crc32` — exactly the streams `encode_snapshot_body` refuses to
//!   write, which is why the raw assembly path exists).
//! - Hardening (2026-09-02 review round 3): planted-symlink temp-name
//!   attacks on `save` (P1-1, unix), cosine zero-vector metric fitness at
//!   load (P1-2), load validation order and the length cross-check
//!   (P2-1/P2-3), the level_count ≤ 64 cap (P2-2), rename-over-target
//!   semantics and temp-residue cleanup (P2-4), a non-default-params cell
//!   (P3-1), the v1 golden-bytes format pin (P3-2), and concurrent
//!   save/load (P3-4).
//! - Hardening (2026-09-02 review round 5): non-regular-file rejection at
//!   load (P1: FIFO + /dev/null, unix), the caller byte budget
//!   (`load_with_budget`, P2), and the codec-side level_count cap (P3 —
//!   codec half asserted in encoding.rs's own unit test).
//! - Hardening (2026-09-02 review round 6): total length arithmetic
//!   (P1: saturating helpers + budget floor, unit-tested in snapshot.rs),
//!   the memory budget distinct from the byte budget (P2:
//!   `LoadBudget::max_memory_bytes` vs `max_memory_estimate`), the
//!   codec-side non-finite rejection (P3 — codec half in encoding.rs), and
//!   the overlap-proven concurrent save/load rewrite below.
//!
//! All seeds are explicit literals (§4.1). Temporary files live in
//! `std::env::temp_dir()` under pid + counter-unique names and are removed
//! on drop (no `tempfile` crate — the dependency freeze forbids it).

mod common;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use common::*;
use pg_am_hnsw::encoding::{
    encode_node_record, max_memory_estimate, wrap_crc32, BodyEncoder, NodeRecord, SnapshotHeader,
    EMPTY_GRAPH_ENTRY_POINT, SNAPSHOT_HEADER_SIZE,
};
use pg_am_hnsw::snapshot::LoadBudget;
use pg_am_hnsw::{snapshot, Hnsw, HnswError, HnswParams, Metric, NodeId};

// ---------------------------------------------------------------------
// temp files (pid + counter unique; parallel tests cannot collide)
// ---------------------------------------------------------------------

static TEMP_COUNTER: AtomicUsize = AtomicUsize::new(0);

/// A uniquely named temp file removed on drop (success or panic).
struct TempSnapshotPath(PathBuf);

impl TempSnapshotPath {
    fn new(tag: &str) -> Self {
        let seq = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "pg_am_hnsw_snapshot_roundtrip-{}-{tag}-{seq}.bin",
            std::process::id()
        ));
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }

    /// Write raw bytes to a fresh temp file (corruption-test fixture).
    fn with_bytes(tag: &str, bytes: &[u8]) -> Self {
        let tmp = Self::new(tag);
        std::fs::write(tmp.path(), bytes).unwrap();
        tmp
    }
}

impl Drop for TempSnapshotPath {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// A uniquely named temp DIRECTORY removed on drop (recursive).
struct TempDir(PathBuf);

impl TempDir {
    fn new(tag: &str) -> Self {
        let seq = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "pg_am_hnsw_snapshot_roundtrip-{}-{tag}-{seq}",
            std::process::id()
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Rebuild the exact temporary name `save` would try for `target` at
/// save-side counter value `seq` (same pid, same naming scheme).
fn save_temp_name(target: &Path, seq: u32) -> PathBuf {
    let mut name = target.as_os_str().to_owned();
    name.push(format!(".tmp-{}-{seq}", std::process::id()));
    PathBuf::from(name)
}

// ---------------------------------------------------------------------
// shared assertions
// ---------------------------------------------------------------------

/// Bitwise structural identity between an original graph and its
/// save→load twin: params (the snapshot-carried construction three),
/// node count, entry point, max_level, and every node's level, vector and
/// full adjacency. `ef_search_default` is caller-supplied at load, so it is
/// checked against the value the caller passed.
fn assert_identical(orig: &Hnsw, loaded: &Hnsw, ef_search_default: u32) {
    assert_eq!(loaded.dim(), orig.dim());
    assert_eq!(loaded.metric(), orig.metric());
    let (op, lp) = (orig.params(), loaded.params());
    assert_eq!(
        (lp.m(), lp.m_max0(), lp.ef_construction()),
        (op.m(), op.m_max0(), op.ef_construction()),
        "the §3 header carries exactly the construction parameters"
    );
    assert_eq!(lp.ef_search_default(), ef_search_default);
    assert_eq!(loaded.node_count(), orig.node_count());
    assert_eq!(loaded.entry_point(), orig.entry_point());
    assert_eq!(loaded.max_level(), orig.max_level());
    assert!(
        loaded.is_read_only(),
        "a snapshot-loaded graph is read-only"
    );
    assert!(!orig.is_read_only(), "save must not mark the original");
    for i in 0..orig.node_count() {
        let id = NodeId(i as u32);
        assert_eq!(loaded.level(id), orig.level(id), "node {i}: level");
        // Bit-level comparison (2026-09-02 review P3): to_bits distinguishes
        // +0.0/-0.0 and any NaN payload — f32 `==` would conflate them.
        assert!(
            loaded
                .vector(id)
                .iter()
                .map(|&x| x.to_bits())
                .eq(orig.vector(id).iter().map(|&x| x.to_bits())),
            "node {i}: vector bit pattern differs (bit-level comparison, +0.0 != -0.0)"
        );
        assert_eq!(
            loaded.node_adjacency(id),
            orig.node_adjacency(id),
            "node {i}: per-level adjacency"
        );
    }
}

// ---------------------------------------------------------------------
// round-trip matrix
// ---------------------------------------------------------------------

/// Matrix cell: `(graph seed, data seed, dim, N, metric, load ef)`. Three
/// seeds per (metric, dim) pair; N ≈ 1k so the release run stays fast
/// (coding plan Stage C acceptance), and dim 128 trims N slightly to keep
/// the debug run in the same minute range as the Stage B2 suites. `load ef`
/// is the `ef_search_default` supplied at load time: one cell per metric
/// uses 128 ≠ 64 so the `ef = None` search leg genuinely exercises the
/// load-time default against a beam width different from the explicit leg
/// (2026-09-02 review P3-1: previously both legs ran ef = 64).
const RT_MATRIX: &[(u64, u64, u16, usize, Metric, u32)] = &[
    (201, 9201, 2, 1000, Metric::L2, 64),
    (202, 9202, 2, 1000, Metric::L2, 64),
    (203, 9203, 2, 1000, Metric::L2, 64),
    (204, 9204, 16, 1000, Metric::L2, 64),
    (205, 9205, 16, 1000, Metric::L2, 64),
    (206, 9206, 16, 1000, Metric::L2, 64),
    (207, 9207, 128, 800, Metric::L2, 64),
    (208, 9208, 128, 800, Metric::L2, 64),
    (209, 9209, 128, 800, Metric::L2, 128),
    (211, 9211, 2, 1000, Metric::Cosine, 64),
    (212, 9212, 2, 1000, Metric::Cosine, 64),
    (213, 9213, 2, 1000, Metric::Cosine, 64),
    (214, 9214, 16, 1000, Metric::Cosine, 64),
    (215, 9215, 16, 1000, Metric::Cosine, 64),
    (216, 9216, 16, 1000, Metric::Cosine, 64),
    (217, 9217, 128, 800, Metric::Cosine, 64),
    (218, 9218, 128, 800, Metric::Cosine, 64),
    (219, 9219, 128, 800, Metric::Cosine, 128),
    (221, 9221, 2, 1000, Metric::InnerProduct, 64),
    (222, 9222, 2, 1000, Metric::InnerProduct, 64),
    (223, 9223, 2, 1000, Metric::InnerProduct, 64),
    (224, 9224, 16, 1000, Metric::InnerProduct, 64),
    (225, 9225, 16, 1000, Metric::InnerProduct, 64),
    (226, 9226, 16, 1000, Metric::InnerProduct, 64),
    (227, 9227, 128, 800, Metric::InnerProduct, 64),
    (228, 9228, 128, 800, Metric::InnerProduct, 64),
    (229, 9229, 128, 800, Metric::InnerProduct, 128),
];

#[test]
fn round_trip_matrix_structure_and_search_bitwise() {
    for &(gseed, dseed, dim, n, metric, load_ef) in RT_MATRIX {
        let mut gen = MixtureGen::new(usize::from(dim), 8, dseed);
        let g = build_graph(dim, metric, HnswParams::default(), gseed, n, &mut gen);
        // Queries continue the SAME generator past the data points — the
        // in-distribution convention of hnsw_bruteforce.rs (2026-08-31
        // review P3).
        let queries: Vec<Vec<f32>> = (0..4).map(|_| gen.next_vec()).collect();

        let tmp = TempSnapshotPath::new("rt");
        snapshot::save(&g, tmp.path()).unwrap();
        let loaded = snapshot::load(tmp.path(), metric, load_ef).unwrap();

        assert_identical(&g, &loaded, load_ef);
        for (qi, q) in queries.iter().enumerate() {
            // Leg 1: explicit beam ef = 64 (the §12 acceptance width).
            let expect = g.search(q, 10, Some(64)).unwrap();
            let got = loaded.search(q, 10, Some(64)).unwrap();
            assert_eq!(
                got, expect,
                "§9 round-trip: search diverged (seed={gseed} dim={dim} \
                 metric={metric:?} query {qi} ef=Some(64))"
            );
            // Leg 2: ef = None resolves to the LOAD-TIME default
            // (load_ef), so the original is queried with that same beam
            // width. Cells with load_ef = 128 make this leg genuinely
            // distinct from leg 1 (review P3-1).
            let expect = g.search(q, 10, Some(load_ef as usize)).unwrap();
            let got = loaded.search(q, 10, None).unwrap();
            assert_eq!(
                got, expect,
                "§9 round-trip: default-ef search diverged (seed={gseed} dim={dim} \
                 metric={metric:?} query {qi} ef=None -> {load_ef})"
            );
        }
        eprintln!(
            "rt cell seed={gseed} dim={dim} N={n} metric={metric:?} load_ef={load_ef}: bitwise identical"
        );
    }
}

#[test]
fn save_load_save_is_byte_identical() {
    // Format-freeze regression pin (2026-09-02 review P3-2): for any
    // non-empty graph the bytes of save(g) equal the bytes of
    // save(load(save(g))) — position-is-identity (§3) makes the round-trip
    // canonical, so any format or encoder drift breaks byte equality
    // loudly. Also pins that re-saving a read-only (loaded) graph is legal
    // and does not change its read-only flag.
    let mut gen = MixtureGen::new(16, 8, 9310);
    let g = build_graph(16, Metric::L2, HnswParams::default(), 310, 500, &mut gen);

    let tmp1 = TempSnapshotPath::new("canon1");
    snapshot::save(&g, tmp1.path()).unwrap();
    let bytes1 = std::fs::read(tmp1.path()).unwrap();

    let loaded = snapshot::load(tmp1.path(), Metric::L2, 64).unwrap();
    assert!(loaded.is_read_only());
    let tmp2 = TempSnapshotPath::new("canon2");
    snapshot::save(&loaded, tmp2.path()).unwrap();
    assert!(
        loaded.is_read_only(),
        "save must not mutate the saved graph"
    );
    let bytes2 = std::fs::read(tmp2.path()).unwrap();

    assert_eq!(
        bytes1, bytes2,
        "§3 freeze: save -> load -> save must reproduce the file byte-for-byte"
    );
}

// ---------------------------------------------------------------------
// boundary shapes
// ---------------------------------------------------------------------

#[test]
fn round_trip_empty_graph() {
    // §3 empty-graph encoding: node_count = 0, entry_point = u32::MAX
    // sentinel, max_level = 0. Search on the loaded graph must return an
    // empty result, not panic.
    let g = Hnsw::new(4, Metric::L2, HnswParams::default(), 301).unwrap();
    let tmp = TempSnapshotPath::new("empty");
    snapshot::save(&g, tmp.path()).unwrap();
    let loaded = snapshot::load(tmp.path(), Metric::L2, 64).unwrap();
    assert_identical(&g, &loaded, 64);
    assert_eq!(loaded.node_count(), 0);
    assert_eq!(loaded.entry_point(), None);
    assert_eq!(loaded.max_level(), 0);
    assert_eq!(loaded.search(&[0.0; 4], 10, Some(64)).unwrap(), vec![]);
}

#[test]
fn round_trip_single_node_graph() {
    let mut g = Hnsw::new(2, Metric::L2, HnswParams::default(), 302).unwrap();
    g.insert(&[3.0, 4.0]).unwrap();
    let tmp = TempSnapshotPath::new("single");
    snapshot::save(&g, tmp.path()).unwrap();
    let loaded = snapshot::load(tmp.path(), Metric::L2, 64).unwrap();
    assert_identical(&g, &loaded, 64);
    // The only node answers every query, with its exact distance.
    let expect = g.search(&[3.0, 4.0], 10, Some(64)).unwrap();
    assert_eq!(expect, vec![(NodeId(0), 0.0)]);
    assert_eq!(loaded.search(&[3.0, 4.0], 10, Some(64)).unwrap(), expect);
}

#[test]
fn round_trip_multi_level_graph() {
    // A graph containing level ≥ 2 nodes: at M = 16 a node reaches level 2
    // with probability 16⁻² = 1/256 per insert, so the loop below expects
    // one within ~hundreds of inserts; the 50k cap is a deterministically
    // unreachable backstop (fixed seed — either the draw sequence produces
    // a level-2 node or the test fails loudly on a changed rng).
    let mut gen = MixtureGen::new(2, 4, 9303);
    let mut g = Hnsw::new(2, Metric::L2, HnswParams::default(), 303).unwrap();
    for _ in 0..50_000 {
        g.insert(&gen.next_vec()).unwrap();
        if g.max_level() >= 2 {
            break;
        }
    }
    assert!(
        g.max_level() >= 2,
        "test premise: a level ≥ 2 node must appear (max_level = {})",
        g.max_level()
    );
    let entry = g.entry_point().unwrap();
    assert_eq!(
        g.level(entry),
        g.max_level(),
        "the entry point is the highest node"
    );

    let tmp = TempSnapshotPath::new("multi");
    snapshot::save(&g, tmp.path()).unwrap();
    let loaded = snapshot::load(tmp.path(), Metric::L2, 64).unwrap();
    assert_identical(&g, &loaded, 64);
    // Queries cross the upper layers, so equality here also covers the
    // greedy-descent path on the rebuilt adjacency.
    for _ in 0..4 {
        let q = gen.next_vec();
        assert_eq!(
            loaded.search(&q, 10, Some(64)).unwrap(),
            g.search(&q, 10, Some(64)).unwrap()
        );
    }
}

// ---------------------------------------------------------------------
// read-only contract (coding plan Stage C conservative default)
// ---------------------------------------------------------------------

#[test]
fn loaded_graph_rejects_insert_save_keeps_original_writable() {
    let mut gen = MixtureGen::new(2, 4, 9304);
    let mut g = build_graph(2, Metric::L2, HnswParams::default(), 304, 64, &mut gen);
    let tmp = TempSnapshotPath::new("ro");
    snapshot::save(&g, tmp.path()).unwrap();

    // The original is untouched by save: still writable, not read-only.
    assert!(!g.is_read_only());
    let before = g.node_count();
    let id = g.insert(&gen.next_vec()).unwrap();
    assert_eq!(id, NodeId(before as u32));

    let mut loaded = snapshot::load(tmp.path(), Metric::L2, 64).unwrap();
    assert!(loaded.is_read_only());
    let err = loaded.insert(&gen.next_vec()).unwrap_err();
    assert!(
        matches!(err, HnswError::InvalidOperation(_)),
        "expected InvalidOperation, got: {err}"
    );
    assert!(
        err.to_string().contains("read-only"),
        "message should name the read-only contract: {err}"
    );
    // The rejected insert must not have mutated anything.
    assert_eq!(loaded.node_count(), 64);
}

#[test]
fn load_rejects_ef_search_default_below_m() {
    // ef_search_default is caller-supplied at load (§3 v1.3) but still bound
    // by the §4.4 construction check against the snapshot's M (= 16 here).
    let mut gen = MixtureGen::new(2, 4, 9305);
    let g = build_graph(2, Metric::L2, HnswParams::default(), 305, 8, &mut gen);
    let tmp = TempSnapshotPath::new("ef");
    snapshot::save(&g, tmp.path()).unwrap();
    let err = snapshot::load(tmp.path(), Metric::L2, 8).unwrap_err();
    assert!(
        matches!(err, HnswError::InvalidParams(_)),
        "expected InvalidParams, got: {err}"
    );
    // Boundary: ef_search_default == M is legal.
    snapshot::load(tmp.path(), Metric::L2, 16).unwrap();
}

#[test]
fn load_missing_file_is_io_error() {
    let tmp = TempSnapshotPath::new("missing"); // never written; dropped after
    let err = snapshot::load(tmp.path(), Metric::L2, 64).unwrap_err();
    assert!(matches!(err, HnswError::Io(_)), "expected Io, got: {err}");
}

// ---------------------------------------------------------------------
// corruption: real files, damaged after save
// ---------------------------------------------------------------------

/// Save a small real graph and return its on-disk bytes (a valid §7 file).
fn saved_bytes(tag: &str) -> (TempSnapshotPath, Vec<u8>) {
    let mut gen = MixtureGen::new(2, 4, 9306);
    let g = build_graph(2, Metric::L2, HnswParams::default(), 306, 32, &mut gen);
    let tmp = TempSnapshotPath::new(tag);
    snapshot::save(&g, tmp.path()).unwrap();
    let bytes = std::fs::read(tmp.path()).unwrap();
    (tmp, bytes)
}

#[test]
fn corrupt_bit_flip_detected_by_crc() {
    let (_base, bytes) = saved_bytes("flip");
    // Flip one bit inside the body (past the 4-byte CRC prefix), and in a
    // second case inside the stored CRC itself — both are bit-rot (§7).
    for offset in [10, 0] {
        let mut damaged = bytes.clone();
        damaged[offset] ^= 0x01;
        let tmp = TempSnapshotPath::with_bytes("flip", &damaged);
        let err = snapshot::load(tmp.path(), Metric::L2, 64).unwrap_err();
        assert!(
            matches!(err, HnswError::ChecksumMismatch { .. }),
            "bit flip at offset {offset}: expected ChecksumMismatch, got: {err}"
        );
    }
}

#[test]
fn corrupt_truncation_detected() {
    let (_base, bytes) = saved_bytes("trunc");
    // Dropping tail bytes changes the body, so the stored CRC no longer
    // matches — the truncation surfaces as ChecksumMismatch before any
    // structural decode runs (§7 layer order).
    let damaged = &bytes[..bytes.len() - 3];
    let tmp = TempSnapshotPath::with_bytes("trunc", damaged);
    let err = snapshot::load(tmp.path(), Metric::L2, 64).unwrap_err();
    assert!(
        matches!(err, HnswError::ChecksumMismatch { .. }),
        "expected ChecksumMismatch, got: {err}"
    );
}

#[test]
fn corrupt_shorter_than_crc_prefix() {
    let tmp = TempSnapshotPath::with_bytes("short", &[1, 2, 3]);
    let err = snapshot::load(tmp.path(), Metric::L2, 64).unwrap_err();
    assert!(
        matches!(err, HnswError::Corrupted(_)),
        "expected Corrupted, got: {err}"
    );
    assert!(err.to_string().contains("CRC32"), "message: {err}");
}

// ---------------------------------------------------------------------
// forged files: legal CRC + invalid content (the load-validation checklist)
// ---------------------------------------------------------------------

/// A valid 3-node fixture (node 2 is the two-level entry point; every other
/// node lives on level 0 only, so its level-1 list is necessarily empty —
/// an edge requires both endpoints to have the level, checklist item 10).
fn forged_header() -> SnapshotHeader {
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

fn forged_nodes() -> Vec<NodeRecord> {
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

/// Assemble a body WITHOUT the graph-data validation — the raw counterpart
/// of `encode_snapshot_body`, which would refuse these streams (write/read
/// symmetry, 2026-08-31 review P2). This is how "legal CRC + invalid
/// content" files are forged.
fn forged_body(header: &SnapshotHeader, nodes: &[NodeRecord]) -> Vec<u8> {
    let mut body = header.encode().to_vec();
    for rec in nodes {
        encode_node_record(&mut body, header.dim, &rec.vector, &rec.neighbors).unwrap();
    }
    body
}

/// Hand-rolled body for a single all-empty-levels record, byte by byte.
/// Needed for level_count > 64 fixtures: since 2026-09-02 round 5 P3 the
/// pub codec (`encode_node_record`) itself enforces the cap, so over-cap
/// streams can only be forged by hand (the codec's own refusal is asserted
/// in encoding.rs's `encode_node_record_enforces_the_level_count_cap`).
fn forged_body_raw_record(header: &SnapshotHeader, vector: &[f32], level_count: u8) -> Vec<u8> {
    let mut body = header.encode().to_vec();
    body.push(0); // flags
    body.push(0); // reserved
    for &x in vector {
        body.extend_from_slice(&x.to_le_bytes());
    }
    body.push(level_count);
    for _ in 0..level_count {
        body.extend_from_slice(&0u16.to_le_bytes()); // empty neighbor list
    }
    body
}

/// Forge a complete on-disk file (CRC32 prefix included) from a body.
fn forged_file(tag: &str, body: &[u8]) -> TempSnapshotPath {
    TempSnapshotPath::with_bytes(tag, &wrap_crc32(body))
}

/// Load must fail with the expected variant — and never panic.
fn assert_load_rejected(tag: &str, body: &[u8], needle: &str) {
    let tmp = forged_file(tag, body);
    let err = snapshot::load(tmp.path(), Metric::L2, 64).unwrap_err();
    assert!(
        matches!(err, HnswError::Corrupted(_)),
        "{tag}: expected Corrupted, got: {err}"
    );
    let msg = err.to_string();
    assert!(msg.contains(needle), "{tag}: unexpected message: {msg}");
}

#[test]
fn forged_valid_file_loads() {
    // Positive control: the raw assembly path produces a genuinely valid
    // file — the negatives below fail because of their injected defect, not
    // because the fixture was broken all along.
    let tmp = forged_file("valid", &forged_body(&forged_header(), &forged_nodes()));
    let g = snapshot::load(tmp.path(), Metric::L2, 64).unwrap();
    assert_eq!(g.node_count(), 3);
    assert_eq!(g.entry_point(), Some(NodeId(2)));
    assert_eq!(g.max_level(), 1);
    assert!(g.is_read_only());
}

#[test]
fn forged_bad_magic_and_version() {
    let mut body = forged_body(&forged_header(), &forged_nodes());
    body[0] ^= 0xff; // magic is body[0..4] (the CRC prefix is added later)
    assert_load_rejected("magic", &body, "bad magic");

    let mut body = forged_body(&forged_header(), &forged_nodes());
    body[4..6].copy_from_slice(&99u16.to_le_bytes()); // format_version
    assert_load_rejected("version", &body, "format_version");
}

#[test]
fn forged_param_rerun_rejections() {
    // Construction-parameter re-run (§3): m < 2 and m_max0 < m fail even
    // though the rest of the stream is well-formed.
    let mut header = forged_header();
    header.m = 1;
    assert_load_rejected("m1", &forged_body(&header, &forged_nodes()), "M = 1 < 2");

    let mut header = forged_header();
    header.m_max0 = 8; // < m = 16
    assert_load_rejected(
        "mmax0",
        &forged_body(&header, &forged_nodes()),
        "M_max0 = 8 < M = 16",
    );
}

#[test]
fn forged_entry_point_out_of_range() {
    let mut header = forged_header();
    header.entry_point = 3; // == node_count
    assert_load_rejected("ep", &forged_body(&header, &forged_nodes()), "entry_point");
}

#[test]
fn forged_max_level_mismatch_with_entry_node() {
    let mut header = forged_header();
    header.max_level = 0; // the entry node (2) has top level 1
    assert_load_rejected(
        "ml",
        &forged_body(&header, &forged_nodes()),
        "!= entry node's top level",
    );
}

#[test]
fn forged_empty_graph_without_sentinel() {
    let header = SnapshotHeader {
        node_count: 0,
        entry_point: 0, // must be EMPTY_GRAPH_ENTRY_POINT on an empty graph
        max_level: 0,
        ..forged_header()
    };
    assert_eq!(EMPTY_GRAPH_ENTRY_POINT, u32::MAX, "sentinel sanity");
    assert_load_rejected("sentinel", &forged_body(&header, &[]), "sentinel");
}

#[test]
fn forged_nonzero_flags_and_reserved() {
    // Node 0's record starts right after the fixed-width header: flags at
    // offset 0 of the record, reserved at offset 1.
    let mut body = forged_body(&forged_header(), &forged_nodes());
    body[SNAPSHOT_HEADER_SIZE] = 1;
    assert_load_rejected("flags", &body, "flags");

    let mut body = forged_body(&forged_header(), &forged_nodes());
    body[SNAPSHOT_HEADER_SIZE + 1] = 1;
    assert_load_rejected("reserved", &body, "reserved");
}

#[test]
fn forged_non_finite_vector_components() {
    // NaN and ±inf rejection holds at every entry point (§5/§7): the
    // decoder rejects a non-finite component at its parse point.
    //
    // The bytes are hand-rolled: since 2026-09-02 round 6 P3 the pub codec
    // (encode_node_record) itself rejects non-finite components, so this
    // fixture can no longer be built through it (that refusal is asserted
    // in encoding.rs's encode_node_record_rejects_non_finite_components).
    let header = SnapshotHeader {
        node_count: 1,
        entry_point: 0,
        max_level: 0,
        ..forged_header()
    };
    for bad in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
        assert_load_rejected(
            "nonfinite",
            &forged_body_raw_record(&header, &[bad, 0.0], 1),
            "non-finite",
        );
    }
}

#[test]
fn forged_malformed_adjacency() {
    // Each case violates one adjacency well-formedness rule (checklist item
    // 11 / item 10); all carry a legal CRC and pass the header checks.
    let header = forged_header();

    // descending order
    let mut nodes = forged_nodes();
    nodes[0].neighbors[0] = vec![NodeId(2), NodeId(1)];
    assert_load_rejected(
        "desc",
        &forged_body(&header, &nodes),
        "not strictly ascending",
    );

    // duplicate edge
    let mut nodes = forged_nodes();
    nodes[0].neighbors[0] = vec![NodeId(1), NodeId(1)];
    assert_load_rejected(
        "dup",
        &forged_body(&header, &nodes),
        "not strictly ascending",
    );

    // self-loop
    let mut nodes = forged_nodes();
    nodes[0].neighbors[0] = vec![NodeId(0), NodeId(1)];
    assert_load_rejected("selfloop", &forged_body(&header, &nodes), "self-loop");

    // endpoint out of range
    let mut nodes = forged_nodes();
    nodes[0].neighbors[0] = vec![NodeId(1), NodeId(99)];
    assert_load_rejected("oob", &forged_body(&header, &nodes), "does not exist");

    // level-1 edge whose target has level 0 only (checklist item 10)
    let header2 = SnapshotHeader {
        node_count: 2,
        entry_point: 1,
        max_level: 1,
        ..forged_header()
    };
    let nodes2 = vec![
        NodeRecord {
            vector: vec![0.0, 0.0],
            neighbors: vec![vec![]], // level 0 only
        },
        NodeRecord {
            vector: vec![1.0, 1.0],
            neighbors: vec![vec![], vec![NodeId(0)]], // level-1 edge to node 0
        },
    ];
    assert_load_rejected("nolevel", &forged_body(&header2, &nodes2), "has no level");
}

// ---------------------------------------------------------------------
// P1-2 (2026-09-02 review): metric fitness at load
// ---------------------------------------------------------------------

#[test]
fn cosine_load_rejects_zero_vector_snapshot() {
    // An L2-built graph may legally contain zero vectors; loading it as
    // Cosine must fail loudly at load (§5 entry validation holds at every
    // entry point, load included) instead of panicking inside search's
    // distance `expect` on ZeroVector.
    let mut g = Hnsw::new(2, Metric::L2, HnswParams::default(), 402).unwrap();
    g.insert(&[0.0, 0.0]).unwrap(); // legal under L2, undefined under Cosine
    g.insert(&[1.0, 0.0]).unwrap();
    g.insert(&[0.0, 1.0]).unwrap();
    let tmp = TempSnapshotPath::new("coszero");
    snapshot::save(&g, tmp.path()).unwrap();

    let err = snapshot::load(tmp.path(), Metric::Cosine, 64).unwrap_err();
    assert!(
        matches!(err, HnswError::InvalidArgument(_)),
        "expected InvalidArgument, got: {err}"
    );
    assert!(
        err.to_string().contains("zero vector"),
        "message should name the zero vector: {err}"
    );
    // Returning Err means no graph exists — there is no partial state to
    // inspect; the same file loads fine under its build metric, proving the
    // rejection is metric fitness, not corruption.
    let loaded = snapshot::load(tmp.path(), Metric::L2, 64).unwrap();
    assert_identical(&g, &loaded, 64);
}

// ---------------------------------------------------------------------
// P2-1 / P2-3 (2026-09-02 review): load validation order
// ---------------------------------------------------------------------

#[test]
fn load_validates_ef_search_default_before_reading_body() {
    // Proof by ordering: a header-only file (node_count = 0, valid header,
    // garbage CRC prefix) with ef < m fails InvalidParams (check 4, on the
    // pre-read header); with a legal ef it proceeds to the body stage and
    // fails ChecksumMismatch on the garbage prefix (check 6). The two
    // failures are distinguishable, so the order is pinned. (Check numbers
    // per load_with_budget's rustdoc; the byte budget is check 3.)
    let header = SnapshotHeader {
        dim: 2,
        m: 16,
        m_max0: 32,
        ef_construction: 200,
        node_count: 0,
        entry_point: EMPTY_GRAPH_ENTRY_POINT,
        max_level: 0,
    };
    let mut bytes = vec![0xDE, 0xAD, 0xBE, 0xEF]; // garbage CRC prefix
    bytes.extend_from_slice(&header.encode());
    let tmp = TempSnapshotPath::with_bytes("eforder", &bytes);

    let err = snapshot::load(tmp.path(), Metric::L2, 8).unwrap_err();
    assert!(
        matches!(err, HnswError::InvalidParams(_)),
        "ef check must precede the body read, got: {err}"
    );
    let err = snapshot::load(tmp.path(), Metric::L2, 64).unwrap_err();
    assert!(
        matches!(err, HnswError::ChecksumMismatch { .. }),
        "legal ef reaches the body stage (CRC), got: {err}"
    );
}

#[test]
fn load_rejects_huge_node_count_by_length_crosscheck() {
    // File-level counterpart of decode's checklist item 6: node_count =
    // u32::MAX over a 29-byte file fails at the pre-read length cross-check
    // — without reading or allocating for the claimed body (division form,
    // no u64 overflow).
    let header = SnapshotHeader {
        node_count: u32::MAX,
        entry_point: 0,
        max_level: 0,
        ..forged_header()
    };
    let mut bytes = vec![0u8; 4]; // CRC prefix (never reached)
    bytes.extend_from_slice(&header.encode());
    let tmp = TempSnapshotPath::with_bytes("huge", &bytes);
    let err = snapshot::load(tmp.path(), Metric::L2, 64).unwrap_err();
    assert!(
        matches!(err, HnswError::Corrupted(_)),
        "expected Corrupted, got: {err}"
    );
    assert!(err.to_string().contains("node_count"), "message: {err}");
}

// ---------------------------------------------------------------------
// P2-2 (2026-09-02 review): level_count cap (checklist item 12)
// ---------------------------------------------------------------------

#[test]
fn forged_level_count_above_cap_rejected() {
    // level_count <= 64 (checklist item 12): the geometric draw is
    // hard-bounded at level 53 even at M = 2 (§4.1), so a 65-level record
    // can only come from a pathological forged stream — legal CRC, valid
    // header, well-formed empty adjacency lists, only the cap fires.
    //
    // The bytes are hand-rolled: since 2026-09-02 round 5 P3 the pub codec
    // (encode_node_record) itself enforces the cap, so this fixture can no
    // longer be built through it.
    let header = SnapshotHeader {
        node_count: 1,
        entry_point: 0,
        max_level: 64,
        ..forged_header()
    };
    assert_load_rejected(
        "lvl65",
        &forged_body_raw_record(&header, &[1.0, 0.0], 65),
        "level_count 65 > 64",
    );
}

#[test]
fn level_count_at_cap_loads() {
    // The cap boundary itself is accepted: a single-node graph whose entry
    // point sits at top level 63 (level_count = 64) is self-consistent and
    // must load; the rebuilt search descends through all 64 empty layers
    // without panicking.
    let header = SnapshotHeader {
        node_count: 1,
        entry_point: 0,
        max_level: 63,
        ..forged_header()
    };
    let nodes = vec![NodeRecord {
        vector: vec![1.0, 0.0],
        neighbors: vec![Vec::new(); 64],
    }];
    let tmp = forged_file("lvl64", &forged_body(&header, &nodes));
    let g = snapshot::load(tmp.path(), Metric::L2, 64).unwrap();
    assert_eq!(g.node_count(), 1);
    assert_eq!(g.entry_point(), Some(NodeId(0)));
    assert_eq!(g.max_level(), 63);
    assert_eq!(g.level(NodeId(0)), 63);
    assert_eq!(
        g.search(&[1.0, 0.0], 1, Some(64)).unwrap(),
        vec![(NodeId(0), 0.0)]
    );
}

// ---------------------------------------------------------------------
// P2-4 (2026-09-02 review): rename platform semantics + temp cleanup
// ---------------------------------------------------------------------

#[cfg(unix)] // POSIX replace semantics; M4 supported platforms = CI matrix (Linux/macOS)
#[test]
fn save_overwrites_existing_target() {
    // rename(2) over an existing target replaces it atomically; the second
    // save's content must be what load observes.
    let mut gen = MixtureGen::new(2, 4, 9414);
    let g1 = build_graph(2, Metric::L2, HnswParams::default(), 414, 32, &mut gen);
    let g2 = build_graph(2, Metric::L2, HnswParams::default(), 415, 48, &mut gen);
    let tmp = TempSnapshotPath::new("overwrite");
    snapshot::save(&g1, tmp.path()).unwrap();
    snapshot::save(&g2, tmp.path()).unwrap();
    let loaded = snapshot::load(tmp.path(), Metric::L2, 64).unwrap();
    assert_eq!(
        loaded.node_count(),
        48,
        "the second save replaced the first"
    );
    assert_identical(&g2, &loaded, 64);
}

#[test]
fn save_failure_leaves_no_temp_residue() {
    // Post-create failure path: the target is an existing DIRECTORY, so the
    // final rename fails after the temp file was fully written — save must
    // remove its own temp file (best-effort cleanup). Classification is Io
    // (EISDIR from rename), deliberately asymmetric with load(directory) →
    // InvalidArgument (regular-file gate): save never pre-inspects the
    // target — a pre-check would be a TOCTOU lie (round 7, documented in
    // save's rustdoc).
    let dir = TempDir::new("cleanup");
    let target = dir.path().join("target_is_a_dir");
    std::fs::create_dir(&target).unwrap();
    let mut gen = MixtureGen::new(2, 4, 9416);
    let g = build_graph(2, Metric::L2, HnswParams::default(), 416, 16, &mut gen);

    let err = snapshot::save(&g, &target).unwrap_err();
    assert!(matches!(err, HnswError::Io(_)), "expected Io, got: {err}");
    assert!(
        target.is_dir(),
        "a failed save must leave the directory target untouched"
    );
    let residue: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .filter(|n| n.to_string_lossy().contains(".tmp-"))
        .collect();
    assert!(residue.is_empty(), "temp residue left behind: {residue:?}");

    // Pre-create failure path: the parent directory does not exist, so
    // create_new fails with NotFound — nothing was ever created, nothing to
    // clean (asserted via the same residue scan).
    let missing = dir.path().join("no_such_dir").join("snap.bin");
    let err = snapshot::save(&g, &missing).unwrap_err();
    assert!(
        matches!(err, HnswError::Io(_)),
        "expected Io for a missing parent, got: {err}"
    );
    let residue: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .filter(|n| n.to_string_lossy().contains(".tmp-"))
        .collect();
    assert!(residue.is_empty(), "temp residue left behind: {residue:?}");
}

// ---------------------------------------------------------------------
// P1-1 (2026-09-02 review): planted-symlink temp-name attack (unix)
// ---------------------------------------------------------------------

/// Plant dangling-ish symlinks `save_temp_name(target, seq) -> victim` for
/// `seq` in `0..=count`; returns how many were planted.
#[cfg(unix)]
fn plant_symlinks(target: &Path, victim: &Path, count: u32) -> u32 {
    let mut planted = 0;
    for seq in 0..=count {
        let link = save_temp_name(target, seq);
        if link.symlink_metadata().is_err() {
            std::os::unix::fs::symlink(victim, &link).unwrap();
            planted += 1;
        }
    }
    planted
}

#[cfg(unix)]
#[test]
fn save_never_follows_planted_symlink() {
    // The pre-fix pattern (predictable temp name + File::create) would
    // follow a pre-planted symlink and truncate its target; create_new
    // (O_CREAT|O_EXCL) refuses the taken name and the retry loop skips it.
    //
    // Coverage note: the save-side counter is process-global and advanced
    // by the other tests running in parallel threads, so its exact value is
    // unknowable here. The whole binary performs well under 512 saves, so
    // in practice the counter starts inside the planted 0..=512 range and
    // the retry loop IS exercised (hundreds of collisions before reaching a
    // free name; 512 < 1024 keeps the retry budget intact). If scheduling
    // ever pushed the counter past the range, the safety assertions below
    // still hold (no collision occurred).
    let dir = TempDir::new("symlink");
    let victim = dir.path().join("victim.txt");
    std::fs::write(&victim, b"victim content \x00\x01\xff").unwrap();
    let target = dir.path().join("index.hnsw");
    let planted = plant_symlinks(&target, &victim, 512);
    assert_eq!(planted, 513, "fresh dir: every planted name was free");

    let mut gen = MixtureGen::new(2, 4, 9417);
    let g = build_graph(2, Metric::L2, HnswParams::default(), 417, 16, &mut gen);
    snapshot::save(&g, &target).unwrap();

    // The victim is byte-identical, the final target is a real file (not a
    // symlink), it loads back as the graph — and save deleted nothing it
    // did not create (every planted symlink survives).
    assert_eq!(
        std::fs::read(&victim).unwrap(),
        b"victim content \x00\x01\xff"
    );
    let md = std::fs::symlink_metadata(&target).unwrap();
    assert!(
        md.is_file() && !md.file_type().is_symlink(),
        "target must be a regular file, not a symlink"
    );
    let loaded = snapshot::load(&target, Metric::L2, 64).unwrap();
    assert_identical(&g, &loaded, 64);
    for seq in 0..=512u32 {
        let link = save_temp_name(&target, seq);
        assert!(
            link.symlink_metadata().unwrap().file_type().is_symlink(),
            "save must not remove files it did not create (seq {seq})"
        );
    }
}

#[cfg(unix)]
#[test]
fn save_exhausts_planted_temp_names_loudly() {
    // Retry-cap path: the planted window (0..=3072) is wider than the 1024
    // attempt budget and starts below any counter value this binary can
    // realistically reach (<< 1024 saves total), so every attempt collides
    // and save fails loudly instead of ever following a symlink.
    let dir = TempDir::new("symlink_exhaust");
    let victim = dir.path().join("victim.txt");
    std::fs::write(&victim, b"victim").unwrap();
    let target = dir.path().join("index.hnsw");
    plant_symlinks(&target, &victim, 3072);

    let mut gen = MixtureGen::new(2, 4, 9418);
    let g = build_graph(2, Metric::L2, HnswParams::default(), 418, 16, &mut gen);
    let err = snapshot::save(&g, &target).unwrap_err();
    assert!(
        matches!(err, HnswError::InvalidOperation(_)),
        "expected InvalidOperation after 1024 collisions, got: {err}"
    );
    assert!(
        err.to_string().contains("1024"),
        "message should name the attempt budget: {err}"
    );
    assert_eq!(std::fs::read(&victim).unwrap(), b"victim");
    assert!(
        std::fs::symlink_metadata(&target).is_err(),
        "target must not be created on exhaustion"
    );
}

// ---------------------------------------------------------------------
// P3 (2026-09-02 review): custom params, golden bytes, concurrency
// ---------------------------------------------------------------------

#[test]
fn round_trip_custom_params() {
    // The main matrix uses only the frozen §4.2 defaults — a serialization
    // that hard-coded them would pass it. These cells build with
    // non-default params and assert all four params fields after load (the
    // construction three from the header; ef_search_default supplied at
    // load, here equal to the original's).
    let params = HnswParams::new(4, 8, 40, 16).unwrap();
    for &(gseed, dseed, dim, metric) in &[
        (411u64, 9411u64, 16u16, Metric::L2),
        (412, 9412, 2, Metric::InnerProduct),
    ] {
        let mut gen = MixtureGen::new(usize::from(dim), 4, dseed);
        let g = build_graph(dim, metric, params, gseed, 500, &mut gen);
        let tmp = TempSnapshotPath::new("custp");
        snapshot::save(&g, tmp.path()).unwrap();
        let loaded = snapshot::load(tmp.path(), metric, 16).unwrap();
        assert_identical(&g, &loaded, 16);
        let q = gen.next_vec();
        assert_eq!(
            loaded.search(&q, 10, Some(64)).unwrap(),
            g.search(&q, 10, Some(64)).unwrap(),
            "custom-params round-trip search diverged (dim={dim} metric={metric:?})"
        );
    }
}

#[test]
fn golden_bytes_v1_format_pin() {
    // §3 format-freeze pin: save() of this exact fixture must reproduce
    // GOLDEN_V1 byte-for-byte. Determinism (§4.1: fixed seed + fixed insert
    // order + frozen f64-accumulator distances) makes the bytes
    // platform-independent. The constant was generated 2026-09-02 by saving
    // this exact fixture and hex-dumping the file (save is the only
    // producer). If this test goes red the format drifted — the fix is a
    // FORMAT_VERSION bump plus a revision-log entry, never an edit of the
    // constant to match new behavior.
    let mut g = Hnsw::new(2, Metric::L2, HnswParams::default(), 42).unwrap();
    for v in [[0.0, 0.0], [1.0, 0.0], [0.0, 1.0], [1.0, 1.0], [0.5, 0.5]] {
        g.insert(&v).unwrap();
    }
    let tmp = TempSnapshotPath::new("golden");
    snapshot::save(&g, tmp.path()).unwrap();
    let bytes = std::fs::read(tmp.path()).unwrap();
    assert_eq!(
        bytes,
        hex_decode(GOLDEN_V1),
        "§3 format drift — see the test docs (FORMAT_VERSION bump, do not edit the constant)"
    );
    // The golden file is of course a valid load input.
    let loaded = snapshot::load(tmp.path(), Metric::L2, 64).unwrap();
    assert_identical(&g, &loaded, 64);
}

/// The frozen v1 bytes of the golden fixture above, hex-encoded (see the
/// test's docs for the generation procedure and the drift policy).
const GOLDEN_V1: &str = "f119c699484e53570100020010002000c8000000050000000000000000000000000000000000000104000100000002000000030000000400000000000000803f00000000010400000000000200000003000000040000000000000000000000803f0104000000000001000000030000000400000000000000803f0000803f0104000000000001000000020000000400000000000000003f0000003f01040000000000010000000200000003000000";

/// Lowercase-hex decode (no hex crate — the dependency freeze forbids it).
fn hex_decode(s: &str) -> Vec<u8> {
    assert!(s.len() % 2 == 0, "hex string must have even length");
    (0..s.len() / 2)
        .map(|i| u8::from_str_radix(&s[2 * i..2 * i + 2], 16).unwrap())
        .collect()
}

#[test]
fn concurrent_save_same_target_with_concurrent_load() {
    // 8 threads save the SAME graph to the SAME target (distinct temp names
    // via the pid+counter scheme; last rename wins), while a reader thread
    // loads in a loop. Every load must observe either "no file yet"
    // (ENOENT) or a complete file passing the full checklist — never a torn
    // write (rename atomicity, §7). Which of old/new/ENOENT any single load
    // sees is deliberately NOT asserted (timing-dependent); no panic and no
    // tear is.
    //
    // Liveness + REAL overlap (2026-09-02 review rounds 4/5/6): an
    // all-ENOENT reader would pass vacuously, and a finished-before-reading
    // writer set would leave the "concurrent" claim unproven (rounds 4/5
    // proved only "writes happened", not temporal overlap). So: writers
    // keep a `saves_in_flight` gauge (incremented before each save,
    // decremented after) and keep saving until the reader has observed at
    // least one Ok return WHILE the gauge was non-zero
    // (`reader_saw_overlap`) — the Ok return point is strictly after that
    // load began, and a non-zero gauge at that point means some save's
    // execution interval covered the return point: true temporal overlap.
    // Round 7: the gauge is sampled at the return point itself, BEFORE
    // `assert_identical` runs — a save starting during the assert never
    // overlapped the load and must not count. Both loops carry a
    // 10_000-attempt cap — exhaustion means the test logic itself is
    // broken (panic). ENOENT is legitimate only before the first save
    // completes.
    use std::sync::atomic::AtomicBool;
    let mut gen = MixtureGen::new(16, 8, 9419);
    let g = build_graph(16, Metric::L2, HnswParams::default(), 419, 2_000, &mut gen);
    let tmp = TempSnapshotPath::new("conc");
    let stop = AtomicBool::new(false);
    let reader_saw_overlap = AtomicBool::new(false);
    let saves_in_flight = AtomicUsize::new(0);
    std::thread::scope(|s| {
        let reader = s.spawn(|| {
            let (mut ok, mut overlapped, mut enoent, mut attempts) =
                (0usize, 0usize, 0usize, 0usize);
            while !stop.load(Ordering::Relaxed) || overlapped == 0 {
                attempts += 1;
                assert!(
                    attempts <= 10_000,
                    "reader never overlapped a save in 10_000 attempts — test logic failure"
                );
                match snapshot::load(tmp.path(), Metric::L2, 64) {
                    Ok(loaded) => {
                        // Round 7: sample the gauge AT the Ok return point,
                        // BEFORE assert_identical — a save that starts
                        // during the assert did not overlap this load's
                        // execution and must not count towards overlap.
                        let overlapped_now = saves_in_flight.load(Ordering::Relaxed) > 0;
                        // A completed load passed CRC + the full checklist;
                        // since every writer saves the same input, it must
                        // also BE that graph.
                        assert_identical(&g, &loaded, 64);
                        ok += 1;
                        if overlapped_now {
                            overlapped += 1;
                            reader_saw_overlap.store(true, Ordering::Relaxed);
                        }
                    }
                    Err(HnswError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
                        enoent += 1;
                    }
                    Err(e) => panic!("concurrent load saw a torn/invalid file: {e}"),
                }
            }
            (ok, overlapped, enoent)
        });
        let mut writers = Vec::new();
        for _ in 0..8 {
            writers.push(s.spawn(|| {
                // Every writer saves at least once, then keeps saving until
                // the reader has observed an overlapped load — so writes
                // are provably in flight while the reader is active.
                let mut saves = 0usize;
                loop {
                    saves_in_flight.fetch_add(1, Ordering::Relaxed);
                    let result = snapshot::save(&g, tmp.path());
                    saves_in_flight.fetch_sub(1, Ordering::Relaxed);
                    result.unwrap();
                    saves += 1;
                    if reader_saw_overlap.load(Ordering::Relaxed) {
                        break;
                    }
                    assert!(
                        saves <= 10_000,
                        "writer saved 10_000 times without an overlapped load — test logic failure"
                    );
                }
                saves
            }));
        }
        let mut total_saves = 0usize;
        for w in writers {
            total_saves += w.join().unwrap();
        }
        stop.store(true, Ordering::Relaxed);
        let (ok, overlapped, enoent) = reader.join().unwrap();
        assert!(ok >= 1, "reader must complete at least one load");
        assert!(
            overlapped >= 1,
            "no load returned while a save was in flight — overlap unproven ({total_saves} saves, {enoent} ENOENTs)"
        );
        eprintln!(
            "concurrent save/load smoke: {total_saves} saves, {ok} loads ok ({overlapped} overlapped), {enoent} ENOENT"
        );
    });
    // The final state is one complete file holding exactly the input graph.
    let loaded = snapshot::load(tmp.path(), Metric::L2, 64).unwrap();
    assert_identical(&g, &loaded, 64);
}

// ---------------------------------------------------------------------
// 2026-09-02 review round 4: size ceiling, parse-point cap, Io classification
// ---------------------------------------------------------------------

#[test]
fn load_rejects_trailing_garbage_beyond_max_size() {
    // P2-1 (round 4): the header pins the maximum legal body size
    // (`max_body_size`); a file beyond it is trailing garbage by definition
    // and is rejected WITHOUT being read. A 64 MiB tail on a 16-node file
    // exercises the path; the coarse timing assert (< 5 s — actual: µs)
    // exists only to prove the body was never read (reading + CRC-ing 64
    // MiB is fast on modern hardware, so the variant + message are the
    // real assertions, timing is the tripwire).
    let mut gen = MixtureGen::new(2, 4, 9420);
    let g = build_graph(2, Metric::L2, HnswParams::default(), 420, 16, &mut gen);
    let good = TempSnapshotPath::new("garbage_base");
    snapshot::save(&g, good.path()).unwrap();
    let mut bytes = std::fs::read(good.path()).unwrap();
    bytes.resize(bytes.len() + 64 * 1024 * 1024, 0u8);
    let tmp = TempSnapshotPath::with_bytes("garbage", &bytes);

    let start = std::time::Instant::now();
    let err = snapshot::load(tmp.path(), Metric::L2, 64).unwrap_err();
    let elapsed = start.elapsed();
    assert!(
        matches!(err, HnswError::Corrupted(_)),
        "expected Corrupted, got: {err}"
    );
    assert!(
        err.to_string().contains("trailing garbage"),
        "message: {err}"
    );
    assert!(
        elapsed.as_secs() < 5,
        "load took {elapsed:?} — the 64 MiB tail must be rejected pre-read"
    );

    // Within-budget trailing bytes are NOT caught by the max side of check
    // 5 (the file is still under the maximum legal size) — they are caught
    // by the CRC at check 6, exactly as before.
    let mut bytes = std::fs::read(good.path()).unwrap();
    bytes.push(0u8); // one byte of trailing garbage, under the ceiling
    let tmp = TempSnapshotPath::with_bytes("garbage1", &bytes);
    let err = snapshot::load(tmp.path(), Metric::L2, 64).unwrap_err();
    assert!(
        matches!(err, HnswError::ChecksumMismatch { .. }),
        "one trailing byte stays a CRC matter, got: {err}"
    );

    // And the untouched file still loads (the interval check must not
    // over-reject — the whole suite is the wider proof).
    let loaded = snapshot::load(good.path(), Metric::L2, 64).unwrap();
    assert_identical(&g, &loaded, 64);
}

#[test]
fn empty_graph_trailing_byte_rejected_pre_read() {
    // Round 7: the max side of check 5 compares records-only lengths
    // against `max_records_size` (header-exclusive). An EMPTY graph has a
    // zero records ceiling, so a single trailing byte crosses the exact
    // boundary and is rejected pre-read as trailing garbage — pre-fix the
    // header-inclusive ceiling left the gate 25 bytes loose and this same
    // file surfaced as ChecksumMismatch after a full read. (Non-empty
    // small graphs stay far under their ceiling — dominated by the
    // 63-level term — so their single trailing byte remains a CRC matter,
    // pinned by the test above.)
    let g = Hnsw::new(4, Metric::L2, HnswParams::default(), 9421).unwrap();
    let tmp = TempSnapshotPath::new("empty_trailing");
    snapshot::save(&g, tmp.path()).unwrap();
    let mut bytes = std::fs::read(tmp.path()).unwrap();
    assert_eq!(
        bytes.len(),
        4 + 25, // CRC + header, zero records
        "empty-graph snapshot must be exactly the 29-byte prefix"
    );
    bytes.push(0u8);
    let tmp = TempSnapshotPath::with_bytes("empty_trailing1", &bytes);
    let err = snapshot::load(tmp.path(), Metric::L2, 64).unwrap_err();
    assert!(
        matches!(err, HnswError::Corrupted(_)),
        "expected pre-read Corrupted, got: {err}"
    );
    assert!(
        err.to_string().contains("trailing garbage"),
        "message: {err}"
    );
}

#[test]
fn forged_level_count_200_rejected_at_parse_point() {
    // P2-2 (round 4): the level_count cap fires inside decode_node_record,
    // before any per-level Vec allocation — a 200-level claim never gets to
    // allocate its 200-entry Vec skeleton. Legal CRC, self-consistent
    // header (max_level = 199), so only the cap can fire.
    //
    // The bytes are hand-rolled: since 2026-09-02 round 5 P3 the pub codec
    // (encode_node_record) itself enforces the cap, so this fixture can no
    // longer be built through it.
    let header = SnapshotHeader {
        node_count: 1,
        entry_point: 0,
        max_level: 199,
        ..forged_header()
    };
    let body = forged_body_raw_record(&header, &[1.0, 0.0], 200);
    let tmp = forged_file("lvl200", &body);
    let err = snapshot::load(tmp.path(), Metric::L2, 64).unwrap_err();
    assert!(
        matches!(err, HnswError::Corrupted(_)),
        "expected Corrupted, got: {err}"
    );
    assert!(err.to_string().contains("level_count"), "message: {err}");
}

#[test]
fn load_directory_path_is_rejected_by_the_regular_file_gate() {
    // Added in round 4 (P3) as an EISDIR-at-read_exact → Io case; the
    // round-5 P1 is_file gate fires first — a directory is not a regular
    // file, so the classification is now InvalidArgument. (Filesystem
    // failures at File::open itself, e.g. ENOENT, stay Io — see
    // load_missing_file_is_io_error.)
    let dir = TempDir::new("isdir");
    let err = snapshot::load(dir.path(), Metric::L2, 64).unwrap_err();
    assert!(
        matches!(err, HnswError::InvalidArgument(_)),
        "expected InvalidArgument, got: {err}"
    );
    assert!(err.to_string().contains("regular file"), "message: {err}");
}

// ---------------------------------------------------------------------
// 2026-09-02 review round 5: non-regular files, caller byte budget
// ---------------------------------------------------------------------

#[cfg(unix)]
#[test]
fn load_fifo_is_rejected_without_panic() {
    // Round-5 P1: a FIFO's metadata length is 0 while a reader could still
    // read from it — pre-fix the length arithmetic underflowed (debug
    // panic / release wrap). Post-fix the is_file gate rejects non-regular
    // files before any length math, and load must RETURN Err, never panic.
    let dir = TempDir::new("fifo");
    let fifo = dir.path().join("snap.fifo");
    let status = std::process::Command::new("mkfifo")
        .arg(&fifo)
        .status()
        .unwrap();
    assert!(status.success(), "mkfifo failed");
    // Writer thread: opening a FIFO for writing blocks until a reader
    // opens it — which is exactly load()'s File::open (the handshake
    // completes the pair). Once load has rejected and closed its end, the
    // write fails with EPIPE (the Rust runtime sets SIGPIPE to ignore) or,
    // on a racier schedule, lands in the pipe buffer; either way the
    // thread exits and joins — no timeout needed.
    let fifo_w = fifo.clone();
    let writer = std::thread::spawn(move || {
        let _ = std::fs::write(&fifo_w, [0u8; 64]);
    });
    let err = snapshot::load(&fifo, Metric::L2, 64).unwrap_err();
    assert!(
        matches!(err, HnswError::InvalidArgument(_)),
        "expected InvalidArgument for a non-regular file, got: {err}"
    );
    assert!(err.to_string().contains("regular file"), "message: {err}");
    writer.join().unwrap();

    // /dev/null reclassification (round 5 P1): a character device — before
    // the is_file gate it fell through to the truncation wording
    // (Corrupted, "too short"); now it is InvalidArgument like every
    // non-regular file.
    let err = snapshot::load("/dev/null", Metric::L2, 64).unwrap_err();
    assert!(
        matches!(err, HnswError::InvalidArgument(_)),
        "expected InvalidArgument for /dev/null, got: {err}"
    );
}

#[test]
fn load_with_budget_enforces_the_budget() {
    // Round-5 P2: a budget below the actual file size fails with
    // InvalidArgument naming the budget, before the body is read; a budget
    // at the exact size loads identically to the unbudgeted path.
    let mut gen = MixtureGen::new(2, 4, 9421);
    let g = build_graph(2, Metric::L2, HnswParams::default(), 421, 16, &mut gen);
    let tmp = TempSnapshotPath::new("budget");
    snapshot::save(&g, tmp.path()).unwrap();
    let len = std::fs::metadata(tmp.path()).unwrap().len();

    let budget = LoadBudget {
        max_file_bytes: len - 1,
        ..LoadBudget::unlimited()
    };
    let err = snapshot::load_with_budget(tmp.path(), Metric::L2, 64, budget).unwrap_err();
    assert!(
        matches!(err, HnswError::InvalidArgument(_)),
        "expected InvalidArgument, got: {err}"
    );
    assert!(err.to_string().contains("budget"), "message: {err}");

    // Round-6 P1 floor: a budget below the 29-byte fixed-width prefix is
    // rejected before anything is read (also unit-tested in snapshot.rs
    // against a nonexistent path — proving it precedes File::open).
    let budget = LoadBudget {
        max_file_bytes: 10,
        ..LoadBudget::unlimited()
    };
    let err = snapshot::load_with_budget(tmp.path(), Metric::L2, 64, budget).unwrap_err();
    assert!(
        matches!(err, HnswError::InvalidArgument(_)),
        "expected InvalidArgument, got: {err}"
    );
    assert!(err.to_string().contains("budget"), "message: {err}");

    let budget = LoadBudget {
        max_file_bytes: len,
        ..LoadBudget::unlimited()
    };
    let loaded = snapshot::load_with_budget(tmp.path(), Metric::L2, 64, budget).unwrap();
    assert_identical(&g, &loaded, 64);
}

#[test]
fn load_with_budget_rejects_declared_huge_sparse_file() {
    // The round-5 P2 probe: a forged header claiming node_count = 100_000
    // (format ceiling ≈ 430 MB) over a physically 128 MiB sparse file sits
    // INSIDE the format-derived [min, max] interval — the ceiling alone
    // cannot reject it; only the caller budget does, pre-read.
    let header = SnapshotHeader {
        dim: 2,
        m: 16,
        m_max0: 32,
        ef_construction: 200,
        node_count: 100_000,
        entry_point: 0,
        max_level: 0,
    };
    let mut bytes = vec![0u8; 4]; // CRC prefix (never reached)
    bytes.extend_from_slice(&header.encode());
    let tmp = TempSnapshotPath::with_bytes("sparse", &bytes);
    // Sparse growth: metadata claims 128 MiB without writing them.
    std::fs::OpenOptions::new()
        .write(true)
        .open(tmp.path())
        .unwrap()
        .set_len(128 * 1024 * 1024)
        .unwrap();
    let budget = LoadBudget {
        max_file_bytes: 1024 * 1024,
        ..LoadBudget::unlimited()
    };
    let err = snapshot::load_with_budget(tmp.path(), Metric::L2, 64, budget).unwrap_err();
    assert!(
        matches!(err, HnswError::InvalidArgument(_)),
        "expected InvalidArgument, got: {err}"
    );
    assert!(err.to_string().contains("budget"), "message: {err}");
}

// ---------------------------------------------------------------------
// 2026-09-02 review round 6: memory budget (bytes != heap)
// ---------------------------------------------------------------------

#[test]
fn load_with_budget_memory_estimate_gate() {
    // Round-6 P2: the byte budget cannot express the heap amplification of
    // a pathological (but format-legal) snapshot — dim = 1 with 64 empty
    // levels per node costs ~24 B/level of Vec headers against 2 B/level
    // on disk. `max_memory_estimate` derives the conservative bound from
    // the validated header, and the memory budget is compared BEFORE any
    // decode/materialization.
    //
    // Fixture: a legal snapshot built through the streaming BodyEncoder —
    // 1000 nodes, node 0 at top level 63 (the entry point), every other
    // node level-0 only, all adjacency lists empty.
    let header = SnapshotHeader {
        dim: 1,
        m: 16,
        m_max0: 32,
        ef_construction: 200,
        node_count: 1000,
        entry_point: 0,
        max_level: 63,
    };
    let levels: Vec<u8> = std::iter::once(63)
        .chain(std::iter::repeat_n(0, 999))
        .collect();
    let mut enc = BodyEncoder::new(header, levels, Vec::new()).unwrap();
    for i in 0..1000usize {
        let level_count = if i == 0 { 64 } else { 1 };
        enc.push_record(&[1.0], &vec![Vec::new(); level_count])
            .unwrap();
    }
    let (_crc, body) = enc.finish().unwrap();
    let tmp = TempSnapshotPath::with_bytes("memest", &wrap_crc32(&body));

    let estimate = max_memory_estimate(&header);
    let file_len = std::fs::metadata(tmp.path()).unwrap().len();
    eprintln!("memory probe: file {file_len} B, estimate {estimate} B");
    assert!(estimate > u64::from(header.node_count) * 1_000);

    // One byte below the estimate: rejected before the body is read.
    let budget = LoadBudget {
        max_file_bytes: u64::MAX,
        max_memory_bytes: estimate - 1,
    };
    let err = snapshot::load_with_budget(tmp.path(), Metric::L2, 64, budget).unwrap_err();
    assert!(
        matches!(err, HnswError::InvalidArgument(_)),
        "expected InvalidArgument, got: {err}"
    );
    assert!(err.to_string().contains("memory budget"), "message: {err}");

    // At the estimate: loads fine, structure intact.
    let budget = LoadBudget {
        max_file_bytes: u64::MAX,
        max_memory_bytes: estimate,
    };
    let g = snapshot::load_with_budget(tmp.path(), Metric::L2, 64, budget).unwrap();
    assert_eq!(g.node_count(), 1000);
    assert_eq!(g.max_level(), 63);
    assert_eq!(g.level(NodeId(0)), 63);
}
