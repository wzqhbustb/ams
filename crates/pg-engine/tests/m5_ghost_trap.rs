//! M5 Stage E regression pin (2026-09-23) → **M6 Stage A semantic
//! inversion** (2026-10-09): the ghost trap is no longer constructible.
//!
//! The M5 order (§8.1 steps 5→6: other nodes' lists first, own lists
//! second) made a kill after the first backward edge leave a ghost with
//! INBOUND edges but EMPTY own lists; a query whose greedy descent moved
//! into it was trapped (this exact stream, seed 521: query 0 returned
//! 1 hit for k = 5 with 71 live nodes). M6 §8.2 swapped the order: the
//! own lists land FIRST (step 5), the backward edges SECOND (step 6), so
//! the connect-window residue is now an ORPHAN (complete own lists, zero
//! inbound edges) — unreachable, hence harmless; and any node reached
//! mid-connect carries complete own lists (M6 §4.2: reachable ⟹ own
//! lists complete).
//!
//! This file pins the inversion ON THE RECORDED GEOMETRY (the crash-rounds
//! seed-521 stream that exposed the trap): the same window that trapped a
//! query under the old order now answers in full.

use pg_am_hnsw::index::ProbeMark;
use pg_am_hnsw::{ExpectedParams, HnswParams, Metric, NeighborSelection, NodeId};
use pg_engine::{Engine, EngineConfig};

const DIM: u16 = 128;
/// The crash-rounds round whose mid-mode kill exposed the M5 trap.
const SEED: u64 = 521;

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
}

/// The vector stream — byte-identical to m5_hnsw_crash_rounds.rs.
fn vector_at(seed: u64, seq: u64) -> Vec<f32> {
    let mut r = Rng(seed ^ 0x9E37_79B9_7F4A_7C15u64.wrapping_mul(seq + 1));
    (0..DIM as usize)
        .map(|_| ((r.next() % 2000) as f32 - 1000.0) / 250.0)
        .collect()
}

fn expected_params() -> ExpectedParams {
    let p = HnswParams::default();
    ExpectedParams {
        dim: DIM,
        m: p.m(),
        m_max0: p.m_max0(),
        ef_construction: p.ef_construction(),
        ef_search_default: p.ef_search_default(),
        metric: Metric::L2,
        selection: NeighborSelection::Simple, // odd round
    }
}

/// The committed prefix of round 521's mid-mode kill (71 inserts).
const N_COMMITTED: u64 = 71; // mid-mode target: 30 + (521 % 60)

/// Build the prefix and return the live (engine, index) pair plus the
/// catalog OID, with insert #72 still pending.
fn build_prefix() -> (
    tempfile::TempDir,
    Engine,
    pg_am_hnsw::HnswIndex,
    pg_engine::Oid,
) {
    let tmp = tempfile::TempDir::new().unwrap();
    let dir = tmp.path().to_path_buf();
    let engine = Engine::open(&dir, EngineConfig::new(&dir)).unwrap();
    let (oid, meta) = engine
        .create_hnsw_index(
            HnswParams::default(),
            DIM,
            Metric::L2,
            NeighborSelection::Simple,
            SEED ^ 0x5EED_5EED_5EED_5EED,
        )
        .unwrap();
    let outcome = engine.open_hnsw_index(meta, &expected_params()).unwrap();
    let mut index = outcome.index;
    for i in 0..N_COMMITTED {
        if i > 0 && i % 47 == 0 {
            engine.checkpoint().unwrap();
        }
        let id = engine.hnsw_insert(&mut index, &vector_at(SEED, i)).unwrap();
        assert_eq!(id, NodeId(i as u32));
    }
    (tmp, engine, index, oid)
}

/// Discover insert #72's append-mark sequence under the M6 order, pinning
/// its shape: NodeInit, DirAppend, the own lists FIRST (step 5), the
/// backward edges SECOND (step 6), PublishLive. (No MetaUpdate: node 72's
/// drawn level does not exceed the running max on this stream.)
fn discover_victim_marks() -> Vec<ProbeMark> {
    let (_tmp, engine, mut index, _oid) = build_prefix();
    index.probe_clear_marks();
    index.probe_set_logging(true);
    engine
        .hnsw_insert(&mut index, &vector_at(SEED, N_COMMITTED))
        .unwrap();
    let marks = index.probe_marks();
    engine.shutdown();

    assert_eq!(marks[0], ProbeMark::NodeInit);
    assert_eq!(marks[1], ProbeMark::DirAppend);
    assert_eq!(marks.last().copied(), Some(ProbeMark::PublishLive));
    let first_own = marks
        .iter()
        .position(|x| matches!(x, ProbeMark::OwnList { .. }))
        .expect("a non-empty graph inserts own lists");
    assert_eq!(first_own, 2, "step 5 immediately follows DirAppend");
    let first_edge = marks
        .iter()
        .position(|x| matches!(x, ProbeMark::NeighborEdge { .. }))
        .expect("a non-empty graph inserts backward edges");
    let last_own = marks
        .iter()
        .rposition(|x| matches!(x, ProbeMark::OwnList { .. }))
        .unwrap();
    assert_eq!(
        first_edge,
        last_own + 1,
        "ALL own lists precede the first backward edge (the M6 order)"
    );
    assert!(
        !marks.contains(&ProbeMark::MetaUpdate),
        "no MetaUpdate on this stream (level 0 insert)"
    );
    marks
}

/// Build round 521's prefix (71 committed inserts), stop insert #72 after
/// `crash_after` append-boundary marks, crash, recover, and return the hit
/// counts of the harness's two sanity queries plus the audit residue.
fn replay_window(crash_after: usize) -> (Vec<usize>, pg_am_hnsw::AuditReport, u64) {
    let (tmp, engine, mut index, oid) = build_prefix();
    let dir = tmp.path().to_path_buf();

    index.probe_clear_marks();
    index.probe_set_crash_after(Some(crash_after));
    let r = engine.hnsw_insert(&mut index, &vector_at(SEED, N_COMMITTED));
    assert!(
        r.is_err(),
        "window {crash_after}: the probe must fail the insert"
    );

    std::mem::forget(index);
    std::mem::forget(engine);
    let _ = std::fs::remove_file(dir.join("lock"));

    let engine = Engine::open(&dir, EngineConfig::new(&dir)).unwrap();
    let meta = engine
        .hnsw_index_first_page(oid)
        .unwrap()
        .expect("catalog lost the index OID");
    let outcome = engine.open_hnsw_index(meta, &expected_params()).unwrap();
    let index = outcome.index;
    let report = index.audit(engine.storage().buffer_pool()).unwrap();

    let query_seed = SEED ^ 0x0A11_CE55_0A11_CE55;
    let hit_counts: Vec<usize> = (0..2u64)
        .map(|j| {
            engine
                .hnsw_search(&index, &vector_at(query_seed, j), 5, None)
                .unwrap()
                .len()
        })
        .collect();
    let hwm = index.hwm();
    engine.shutdown();
    (hit_counts, report, hwm)
}

/// Pre-connect window (unchanged by the swap): crash right after DirAppend
/// — mapping published, NO edges of either direction yet. Unreachable,
/// search unaffected.
#[test]
fn ghost_without_inbound_edges_is_unreachable_and_harmless() {
    let (hits, report, hwm) = replay_window(2); // NodeInit + DirAppend
    assert_eq!(report.initializing_count, 1, "one ghost");
    assert_eq!(hwm, 72, "the ghost's mapping was published");
    assert_eq!(
        hits,
        vec![5, 5],
        "an unreachable ghost never enters the search path"
    );
}

/// THE INVERSION, window 1: crash after the LAST own-list write, before
/// the first backward edge — the M5 order's symmetric window was the
/// trapped ghost (inbound edges, empty own lists). Under the M6 order
/// this residue is a pure ORPHAN (complete own lists, zero inbound
/// edges): unreachable, and both queries answer in full.
#[test]
fn orphan_after_own_lists_never_traps_a_query() {
    let marks = discover_victim_marks();
    let first_edge = marks
        .iter()
        .position(|x| matches!(x, ProbeMark::NeighborEdge { .. }))
        .unwrap();
    let (hits, report, hwm) = replay_window(first_edge);
    assert_eq!(report.initializing_count, 1, "one orphan");
    assert_eq!(hwm, 72, "the orphan's mapping was published");
    assert_eq!(
        hits,
        vec![5, 5],
        "orphan (own lists complete, no inbound edges): unreachable, harmless"
    );
}

/// THE INVERSION, window 2 — the M5 trap's exact counterpart: crash after
/// the FIRST backward edge. M5 residue: the ghost had an inbound edge and
/// EMPTY own lists, and query 0 was trapped inside it (1 hit for k = 5).
/// M6 residue: the victim's own lists are ALL in place before any inbound
/// edge exists (reachable ⟹ own lists complete), so a descent that moves
/// into the victim expands through it normally — both queries answer in
/// full on the same geometry.
#[test]
fn first_backward_edge_no_longer_traps_a_query() {
    let marks = discover_victim_marks();
    let first_edge = marks
        .iter()
        .position(|x| matches!(x, ProbeMark::NeighborEdge { .. }))
        .unwrap();
    let (hits, report, hwm) = replay_window(first_edge + 1);
    assert_eq!(report.initializing_count, 1);
    assert_eq!(hwm, 72);
    // Geometry pin (seed 521, this exact stream): the M5 order trapped
    // query 0 here (1 hit); the M6 order answers both queries in full.
    // If a future change shifts the geometry, re-derive these numbers —
    // the CONTRACT is "the trap shape is unconstructible", the counts are
    // evidence.
    assert_eq!(
        hits,
        vec![5, 5],
        "the M5 trap window now answers in full (got {hits:?})"
    );
}
