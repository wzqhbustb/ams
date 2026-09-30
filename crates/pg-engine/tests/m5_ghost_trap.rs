//! M5 Stage E regression pin (2026-09-23, 1000-round acceptance round 521):
//! the **ghost trap** is a legal crash residue.
//!
//! A kill inside §8.1 step 5 (the new node's NEIGHBORS' lists are updated
//! first) before step 6 (the new node's OWN lists) leaves a ghost
//! (INITIALIZING, mapping published) that has INBOUND edges but EMPTY own
//! lists. A query whose greedy descent moves into that ghost is trapped:
//! the beam cannot expand past it, so the search may return fewer than k
//! hits — on this exact stream (the crash-rounds seed-521 geometry) the
//! first query below returns 1 hit for k = 5 with 71 live nodes.
//!
//! This is NOT corruption: the ghost is a real vector and a legal answer
//! (INITIALIZING semantics, §8.1③), every committed insert is intact, and
//! the §11.3 audit passes. The degradation is registered for M6's
//! ghost-recycle protocol (and the §8.1 step 5↔6 reorder candidate —
//! own-lists-first would make this ghost unreachable instead of
//! half-reachable). The slice-2 window matrix already pins the loose
//! legality floor on these windows (`assert_hits_legal`); this test pins
//! that the TRAP ITSELF exists on the recorded geometry, so nobody can
//! re-tighten the crash-rounds sanity assert without reading this file.

use pg_am_hnsw::{ExpectedParams, HnswParams, Metric, NeighborSelection, NodeId};
use pg_engine::{Engine, EngineConfig};

const DIM: u16 = 128;
/// The crash-rounds round whose mid-mode kill exposed the trap.
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

/// Build round 521's prefix (71 committed inserts), stop insert #72 after
/// `crash_after` append-boundary marks, crash, recover, and return the hit
/// counts of the harness's two sanity queries plus the audit residue.
fn replay_window(crash_after: usize) -> (Vec<usize>, pg_am_hnsw::AuditReport, u64) {
    let n_committed = 71u64; // mid-mode target: 30 + (521 % 60)
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

    for i in 0..n_committed {
        if i > 0 && i % 47 == 0 {
            engine.checkpoint().unwrap();
        }
        let id = engine.hnsw_insert(&mut index, &vector_at(SEED, i)).unwrap();
        assert_eq!(id, NodeId(i as u32));
    }

    index.probe_clear_marks();
    index.probe_set_crash_after(Some(crash_after));
    let r = engine.hnsw_insert(&mut index, &vector_at(SEED, n_committed));
    assert!(
        r.is_err(),
        "window {crash_after}: the probe must fail the insert"
    );

    std::mem::forget(index);
    std::mem::forget(engine);
    let _ = std::fs::remove_file(dir.join("lock"));

    let engine = Engine::open(&dir, EngineConfig::new(&dir)).unwrap();
    let meta = engine.hnsw_index_first_page(oid).unwrap().unwrap();
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

/// Window boundary pin: crash right after DirAppend (ghost published, NO
/// inbound edges yet) → unreachable ghost, search unaffected.
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

/// The trap itself: crash after the FIRST NeighborEdge (some existing
/// node's list now points at the ghost, whose own lists are still empty)
/// → a query descending into the ghost returns fewer than k.
#[test]
fn ghost_with_inbound_only_edges_can_trap_a_query() {
    let (hits, report, hwm) = replay_window(3); // + first NeighborEdge
    assert_eq!(report.initializing_count, 1, "one ghost");
    assert_eq!(hwm, 72);
    // Geometry pin (seed 521, this exact stream): query 0 traps in the
    // ghost (1 hit < k = 5), query 1 routes around it. If a future change
    // shifts the geometry, re-derive these two numbers — the CONTRACT is
    // "traps are possible on this residue shape", the counts are evidence.
    assert_eq!(hits[1], 5, "query 1 routes around the ghost");
    assert!(
        hits[0] < 5,
        "query 0 must demonstrate the trap (got {} hits)",
        hits[0]
    );
    assert!(
        hits[0] >= 1,
        "even a trapped query answers (the ghost itself)"
    );
}
