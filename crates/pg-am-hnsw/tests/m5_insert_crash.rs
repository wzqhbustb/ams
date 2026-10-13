//! M5 Stage D slice 2 (re-numbered for the M6 §4.2 order, Stage A): the
//! §8.1 × §8.2 crash-window matrix — every append boundary of the real
//! `insert` path is crashed (test-only probe barrier, `HnswIndex::probe_*`),
//! recovered (slice-1 redo + the open protocol), and audited (Stage D
//! slice-1 `§11.3` audit + the Stage-0 reachability scan). Determinism
//! premise: same seed + same params + same vector sequence ⇒ identical
//! append-mark sequences in the discovery run (dir A) and the crashing
//! run (dir B) — pinned by the sanity assertions and `probe_log_shape`.
//!
//! M6 Stage A note: the §8.2 swap moved the node's OWN lists (step 5)
//! BEFORE the backward edges into other nodes' lists (step 6). Window
//! consequences, pinned by the rows below: the M5 "inbound edges, empty
//! own lists" GHOST shape is no longer constructible; the connect-window
//! residue is now an ORPHAN (complete own lists, no inbound edges yet)
//! that is unreachable and bitwise-invisible to search, and any node that
//! IS reachable mid-connect carries complete own lists (M6 §4.2:
//! reachable ⟹ own lists complete).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use pg_am_hnsw::graph::{Metric, NeighborSelection};
use pg_am_hnsw::index::{self, ProbeMark};
use pg_am_hnsw::{AuditReport, ExpectedParams, HnswError, HnswIndex, HnswParams, NodeId};
use pg_storage::config::StorageConfig;
use pg_storage::engine::StorageEngine;
use pg_storage::types::PageId;

static COUNTER: AtomicUsize = AtomicUsize::new(0);

/// Manual temp dir (no tempfile dev-dependency — M4's dependency freeze;
/// m5_create_open.rs uses the same pattern).
fn fresh_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pg_am_hnsw_m5_crash-{}-{}-{tag}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// One test configuration (everything determinism flows from).
struct Cfg {
    dim: u16,
    params: HnswParams,
    seed: u64,
}

/// Default shape: dim 4, m 4 / m_max0 8 (fast, mid-size pages).
fn cfg_default() -> Cfg {
    Cfg {
        dim: 4,
        params: HnswParams::new(4, 8, 16, 4).unwrap(),
        seed: 0xC0FFEE,
    }
}

/// Shrink-heavy shape: m 2 / m_max0 4 (level-0 lists overflow constantly).
fn cfg_shrink() -> Cfg {
    Cfg {
        dim: 4,
        params: HnswParams::new(2, 4, 4, 2).unwrap(),
        seed: 0xBEEF,
    }
}

fn expected(cfg: &Cfg) -> ExpectedParams {
    ExpectedParams {
        dim: cfg.dim,
        m: cfg.params.m(),
        m_max0: cfg.params.m_max0(),
        ef_construction: cfg.params.ef_construction(),
        ef_search_default: cfg.params.ef_search_default(),
        metric: Metric::L2,
        selection: NeighborSelection::Heuristic,
    }
}

/// Deterministic arithmetic vectors (no rng dependency in the test file).
fn det_vectors(n: usize, dim: usize) -> Vec<Vec<f32>> {
    (0..n)
        .map(|i| {
            (0..dim)
                .map(|j| ((i * 37 + j * 101 + 13) % 89) as f32 / 8.0)
                .collect()
        })
        .collect()
}

/// Fixed probe queries (never inserted).
fn det_queries(n: usize, dim: usize) -> Vec<Vec<f32>> {
    (0..n)
        .map(|i| {
            (0..dim)
                .map(|j| ((i * 53 + j * 11 + 5) % 97) as f32 / 8.0)
                .collect()
        })
        .collect()
}

/// A live (engine, index) pair on a fresh or recovered directory.
struct Lab {
    dir: PathBuf,
    engine: StorageEngine,
    index: HnswIndex,
}

fn start_lab(cfg: &Cfg, tag: &str) -> Lab {
    let dir = fresh_dir(tag);
    let engine = StorageEngine::open(&dir, &StorageConfig::new(&dir)).unwrap();
    let index = index::create(
        engine.buffer_pool(),
        engine.wal_writer(),
        cfg.params,
        cfg.dim,
        Metric::L2,
        NeighborSelection::Heuristic,
        cfg.seed,
    )
    .unwrap();
    index.probe_set_logging(true);
    Lab { dir, engine, index }
}

/// Insert with probe logging: clears the mark log first so the returned
/// marks are local to THIS insert.
fn insert_logged(lab: &mut Lab, v: &[f32]) -> (Result<NodeId, HnswError>, Vec<ProbeMark>) {
    lab.index.probe_clear_marks();
    let r = lab
        .index
        .insert(lab.engine.buffer_pool(), lab.engine.wal_writer(), v);
    let marks = lab.index.probe_marks();
    (r, marks)
}

/// kill -9: forget the index handle and the engine (no checkpoint, no
/// clean shutdown), keep the directory.
fn crash(lab: Lab) -> (PathBuf, PageId) {
    let meta = lab.index.meta_page_id();
    std::mem::forget(lab.index);
    std::mem::forget(lab.engine);
    (lab.dir, meta)
}

/// Recover: redo-equipped engine reopen + the open protocol + the §11.3
/// audit (which must PASS — the window shapes are all legal residues).
fn recover(dir: &Path, cfg: &Cfg, meta: PageId) -> (StorageEngine, HnswIndex, AuditReport) {
    let engine = StorageEngine::open_with_redo_handlers(
        dir,
        &StorageConfig::new(dir),
        pg_am_hnsw::redo::hnsw_redo_handlers(),
        vec![],
    )
    .unwrap();
    let outcome = index::open(
        engine.buffer_pool(),
        engine.wal_writer(),
        meta,
        &expected(cfg),
    )
    .unwrap();
    assert!(
        outcome.warnings.is_empty(),
        "test configs match the creation params — no WARN expected: {:?}",
        outcome.warnings
    );
    let index = outcome.index;
    let report = index.audit(engine.buffer_pool()).unwrap();
    (engine, index, report)
}

/// Recover twice in a row (the second recovery replays the first
/// recovery's own records — replay idempotence): the two audit reports
/// AND the two reachability scans must be EQUAL.
fn recover_twice(
    dir: &Path,
    cfg: &Cfg,
    meta: PageId,
) -> (
    StorageEngine,
    HnswIndex,
    AuditReport,
    pg_am_hnsw::ReachabilityReport,
) {
    let (engine1, index1, report1) = recover(dir, cfg, meta);
    let reach1 = pg_am_hnsw::audit::audit_reachability(engine1.buffer_pool(), meta)
        .expect("reachability scan must parse the first recovery");
    std::mem::forget(index1);
    std::mem::forget(engine1);
    let (engine2, index2, report2) = recover(dir, cfg, meta);
    let reach2 = pg_am_hnsw::audit::audit_reachability(engine2.buffer_pool(), meta)
        .expect("reachability scan must parse the second recovery");
    assert_eq!(report1, report2, "replay must be idempotent across reopens");
    assert_eq!(
        reach1, reach2,
        "reachability must be idempotent across reopens"
    );
    (engine2, index2, report2, reach2)
}

/// `(NodeId, distance)` as exact bits for comparisons.
fn bits(hits: &[(NodeId, f64)]) -> Vec<(u32, u64)> {
    hits.iter().map(|(id, d)| (id.0, d.to_bits())).collect()
}

fn search_bits(lab: &Lab, q: &[f32]) -> Vec<(u32, u64)> {
    bits(
        &lab.index
            .search(lab.engine.buffer_pool(), q, 3, Some(10))
            .unwrap(),
    )
}

/// Search legality (the loose form for windows with a reachable residue):
/// every hit names a real node and its distance is EXACTLY the recomputed
/// distance to that node's stored vector (zero tolerance — the distance
/// functions are deterministic, and slice 4 pins the iterator form
/// bit-identical to the slice form).
fn assert_hits_legal(hits: &[(NodeId, f64)], query: &[f32], vectors: &[Vec<f32>], hwm: u64) {
    assert!(!hits.is_empty(), "a non-empty graph must answer");
    for (id, dist) in hits {
        assert!((id.0 as u64) < hwm, "hit {id:?} names no real node");
        let expected = pg_am_hnsw::distance::l2_squared(query, &vectors[id.0 as usize]).unwrap();
        assert_eq!(
            dist.to_bits(),
            expected.to_bits(),
            "hit {id:?}: distance must be the exact stored-vector distance"
        );
    }
}

/// Phase A (discovery): run every insert with probe logging; returns the
/// per-insert marks, per-insert search baselines, and the pre-insert
/// `(entry_point, max_level, hwm)` snapshots.
struct Discovery {
    /// Dir A's path — returned for cleanup (the crashed lab leaks its
    /// engine by design; the DIRECTORY must not leak too).
    dir: PathBuf,
    marks: Vec<Vec<ProbeMark>>,
    /// baselines[i] = the query grid's results BEFORE insert i.
    baselines: Vec<Vec<Vec<(u32, u64)>>>,
    /// pre_state[i] = (entry_point, max_level, hwm) BEFORE insert i.
    pre_state: Vec<(u32, u8, u64)>,
}

fn discover(cfg: &Cfg, vectors: &[Vec<f32>], queries: &[Vec<f32>], tag: &str) -> Discovery {
    let mut lab = start_lab(cfg, tag);
    let mut d = Discovery {
        dir: lab.dir.clone(),
        marks: Vec::new(),
        baselines: Vec::new(),
        pre_state: Vec::new(),
    };
    for v in vectors {
        d.baselines
            .push(queries.iter().map(|q| search_bits(&lab, q)).collect());
        d.pre_state.push((
            lab.index.entry_point(),
            lab.index.max_level(),
            lab.index.hwm(),
        ));
        let (r, marks) = insert_logged(&mut lab, v);
        r.unwrap();
        d.marks.push(marks);
    }
    let _ = crash(lab);
    d
}

/// The window-test shared driver: discovery in dir A, then dir B replays
/// the same inserts (sanity: identical marks), arms the probe at the
/// victim's boundary, crashes, and recovers twice.
struct Window {
    dir: PathBuf,
    engine: StorageEngine,
    index: HnswIndex,
    report: AuditReport,
    /// The Stage-0 reachability scan of the recovered graph — the M6 §4.2
    /// invariant hook (orphan rows assert the victim UNREACHABLE; the
    /// backward-edge rows assert it reachable).
    reach: pg_am_hnsw::ReachabilityReport,
    victim: usize,
    baseline: Vec<Vec<(u32, u64)>>,
    pre_state: (u32, u8, u64),
    /// The pre-victim graph's audit (phase B, pre-crash) — the exact
    /// edge-count baseline the edge-window rows pin their deltas against.
    pre_report: AuditReport,
    /// The victim's full-run mark sequence (dir A) and the armed boundary:
    /// the crash prefix is `victim_marks[..crash_after]`.
    victim_marks: Vec<ProbeMark>,
    /// See `victim_marks`.
    crash_after: usize,
}

fn run_window(
    tag: &str,
    cfg: &Cfg,
    vectors: &[Vec<f32>],
    victim_pick: impl Fn(&[Vec<ProbeMark>]) -> Option<(usize, usize)>,
) -> Window {
    let queries = det_queries(3, cfg.dim as usize);
    let d = discover(cfg, vectors, &queries, &format!("{tag}-a"));
    let (victim, crash_after) = victim_pick(&d.marks).expect("no victim matches the window");
    // Dir A has served its purpose (marks/baselines harvested) — its engine
    // leak is the crash idiom, but the directory must not leak with it.
    let _ = std::fs::remove_dir_all(&d.dir);
    // baselines[i] is the query grid BEFORE insert i (i nodes visible) —
    // the pre-victim graph is therefore baselines[victim].
    let baseline = d.baselines[victim].clone();
    let pre_state = d.pre_state[victim];

    // Phase B: replay up to the victim (identical marks = determinism
    // sanity), then arm the probe at the victim-local boundary.
    let mut b = start_lab(cfg, &format!("{tag}-b"));
    for (i, v) in vectors.iter().enumerate().take(victim) {
        let (r, marks) = insert_logged(&mut b, v);
        r.unwrap();
        assert_eq!(
            marks, d.marks[i],
            "same seed+vectors must give the identical append sequence (insert {i})"
        );
    }
    b.index.probe_clear_marks();
    // The pre-victim graph's audit (phase B, pre-crash) — the exact
    // edge-count baseline for the window rows' delta pins.
    let pre_report = b.index.audit(b.engine.buffer_pool()).unwrap();
    // The baseline must be CLEAN: every pre-victim insert completed (the
    // unwraps above ⟹ PublishLive landed), so all three residue counters
    // are zero — the rows' `== 1` / `== 0` pins read deltas against a zero
    // baseline, never an inherited residue.
    assert_eq!(
        pre_report.initializing_count, 0,
        "clean pre-victim baseline"
    );
    assert_eq!(
        pre_report.orphan_entry_count, 0,
        "clean pre-victim baseline"
    );
    assert_eq!(
        pre_report.hidden_high_level_count, 0,
        "clean pre-victim baseline"
    );
    b.index.probe_set_crash_after(Some(crash_after));
    let r = b.index.insert(
        b.engine.buffer_pool(),
        b.engine.wal_writer(),
        &vectors[victim],
    );
    let err = r.expect_err("the crash probe must fail the victim insert");
    assert!(err.to_string().contains("crash probe"), "{err}");
    assert_eq!(
        b.index.probe_marks().len(),
        crash_after,
        "the probe must crash exactly at the armed boundary"
    );
    let (dir, meta) = crash(b);

    let (engine, index, report, reach) = recover_twice(&dir, cfg, meta);
    Window {
        dir,
        engine,
        index,
        report,
        reach,
        victim,
        baseline,
        pre_state,
        pre_report,
        victim_marks: d.marks[victim].clone(),
        crash_after,
    }
}

/// The directed-edge delta of a crash prefix under the M6 order (§4.2):
/// the OWN lists land before any backward edge, so a prefix that reached
/// the backward-edge phase holds ALL own-list edges. That count equals
/// the full run's NeighborEdge count — every selected neighbor receives
/// exactly one backward edge, shrink or not — and comes on top of the
/// prefix's own backward edges (+1 each, net 0 for a shrank one: the
/// victim in, one evicted node out). Caller contract: the prefix must
/// contain every OwnList mark of the full run — one row asserts it
/// explicitly and the phase separation (step 5 fully precedes step 6)
/// makes it structural everywhere else; the own-list edge count is not
/// derivable from a partial prefix (list lengths are not recorded in
/// OwnList marks). Implicit assumption, stated: a shrank edge's net-0
/// delta presumes the shrink never evicts the VICTIM itself (it entered
/// the candidate set by being selected; were it evicted the true delta
/// would be −1 and the exact edge-count pins would turn RED — loud
/// direction, never silent).
fn edge_delta(full_marks: &[ProbeMark], prefix: &[ProbeMark]) -> u64 {
    let own = full_marks
        .iter()
        .filter(|x| matches!(x, ProbeMark::NeighborEdge { .. }))
        .count() as u64;
    let backward = prefix
        .iter()
        .filter(|x| matches!(x, ProbeMark::NeighborEdge { shrank: false, .. }))
        .count() as u64;
    own + backward
}

impl Window {
    fn done(self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Find the first insert at or after `lo` whose marks satisfy `pred`;
/// returns `(victim, crash_after)` = (insert index, local mark count to
/// crash after).
fn pick(
    marks: &[Vec<ProbeMark>],
    lo: usize,
    pred: impl Fn(&[ProbeMark]) -> Option<usize>,
) -> Option<(usize, usize)> {
    (lo..marks.len()).find_map(|i| pred(&marks[i]).map(|ca| (i, ca)))
}

/// Local crash-after for "right after the first mark matching `m`".
fn after_first(marks: &[ProbeMark], m: ProbeMark) -> Option<usize> {
    marks.iter().position(|&x| x == m).map(|i| i + 1)
}

// ---------------------------------------------------------------------
// The window rows (§8.2 table).
// ---------------------------------------------------------------------

/// Row "before 1": no insert in flight at all — a clean run crashes
/// between inserts. Everything must recover pristine.
#[test]
fn window_before_step1() {
    let cfg = cfg_default();
    let vectors = det_vectors(64, cfg.dim as usize);
    let queries = det_queries(3, cfg.dim as usize);
    let mut lab = start_lab(&cfg, "before1");
    for v in &vectors {
        let (r, _) = insert_logged(&mut lab, v);
        r.unwrap();
    }
    let baseline: Vec<_> = queries.iter().map(|q| search_bits(&lab, q)).collect();
    lab.engine.wal_writer().flush().unwrap();
    let (dir, meta) = crash(lab);

    let (engine, index, report, _reach) = recover_twice(&dir, &cfg, meta);
    assert_eq!(report.node_count, 64);
    assert_eq!(index.hwm(), 64);
    assert_eq!(report.live_count, 64);
    assert_eq!(report.initializing_count, 0);
    assert_eq!(report.orphan_entry_count, 0);
    assert_eq!(report.hidden_high_level_count, 0);
    for (qi, q) in queries.iter().enumerate() {
        let after = bits(&index.search(engine.buffer_pool(), q, 3, Some(10)).unwrap());
        assert_eq!(after, baseline[qi], "query {qi} must be bitwise stable");
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// Rows 1–2, node-page variant: crash right after the victim's fresh node
/// page is init-FPI'd. The orphan page is invisible to the audit (it is
/// referenced by nothing); the victim's NodeId is reissued on the next
/// insert (§8.1④: unpublished ids are reused).
#[test]
fn window_orphan_node_page() {
    let cfg = cfg_default();
    let vectors = det_vectors(200, cfg.dim as usize);
    let w = run_window("orphan-node-page", &cfg, &vectors, |marks| {
        // A page-full re-allocation (NOT the first insert's initial page).
        pick(marks, 1, |m| after_first(m, ProbeMark::NodePageInit))
    });
    assert_eq!(w.report.node_count, w.victim as u64);
    assert_eq!(w.report.live_count, w.victim as u64);
    assert_eq!(w.report.orphan_entry_count, 0);
    assert_eq!(w.report.initializing_count, 0);
    assert_eq!(w.index.hwm(), w.victim as u64);
    // The unpublished NodeId is reissued.
    let victim = w.victim;
    let Window {
        dir, engine, index, ..
    } = w;
    let id = index
        .insert(engine.buffer_pool(), engine.wal_writer(), &vectors[victim])
        .unwrap();
    assert_eq!(id, NodeId(victim as u32));
    // The continued graph must re-audit clean: the window residue
    // stays accounted and the re-issued victim completes (LIVE).
    let report2 = index.audit(engine.buffer_pool()).unwrap();
    assert_eq!(report2.node_count, victim as u64 + 1);
    assert_eq!(report2.live_count, victim as u64 + 1);
    assert_eq!(report2.initializing_count, 0);
    assert_eq!(report2.orphan_entry_count, 0);
    assert_eq!(report2.hidden_high_level_count, 0);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Rows 1–2, directory-page variant: the tail is exactly full (813
/// entries), the victim allocates the next chain page and crashes right
/// after its init FPI. (dim 4 + m 2 keeps 814 inserts cheap.)
#[test]
fn window_orphan_dir_page() {
    let cfg = cfg_shrink();
    let vectors = det_vectors(814, cfg.dim as usize);
    let w = run_window("orphan-dir-page", &cfg, &vectors, |marks| {
        pick(marks, 1, |m| after_first(m, ProbeMark::DirPageInit))
    });
    assert_eq!(w.victim, 813, "the 814th insert overflows the head page");
    assert_eq!(w.report.node_count, 813);
    assert_eq!(w.index.hwm(), 813);
    let victim = w.victim;
    let Window {
        dir, engine, index, ..
    } = w;
    let id = index
        .insert(engine.buffer_pool(), engine.wal_writer(), &vectors[victim])
        .unwrap();
    assert_eq!(id, NodeId(813));
    // The continued graph must re-audit clean: the window residue
    // stays accounted and the re-issued victim completes (LIVE).
    let report2 = index.audit(engine.buffer_pool()).unwrap();
    assert_eq!(report2.node_count, victim as u64 + 1);
    assert_eq!(report2.live_count, victim as u64 + 1);
    assert_eq!(report2.initializing_count, 0);
    assert_eq!(report2.orphan_entry_count, 0);
    assert_eq!(report2.hidden_high_level_count, 0);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Row 2–3: crash after DirLink — the chain has a fresh, empty, linked
/// tail. A legal shape (middle page exactly full, tail empty).
#[test]
fn window_empty_linked_tail() {
    let cfg = cfg_shrink();
    let vectors = det_vectors(814, cfg.dim as usize);
    let w = run_window("empty-linked-tail", &cfg, &vectors, |marks| {
        pick(marks, 1, |m| after_first(m, ProbeMark::DirLink))
    });
    assert_eq!(w.victim, 813);
    assert_eq!(w.report.node_count, 813);
    assert_eq!(w.index.hwm(), 813);
    let victim = w.victim;
    let Window {
        dir, engine, index, ..
    } = w;
    let id = index
        .insert(engine.buffer_pool(), engine.wal_writer(), &vectors[victim])
        .unwrap();
    assert_eq!(id, NodeId(813));
    // The continued graph must re-audit clean: the window residue
    // stays accounted and the re-issued victim completes (LIVE).
    let report2 = index.audit(engine.buffer_pool()).unwrap();
    assert_eq!(report2.node_count, victim as u64 + 1);
    assert_eq!(report2.live_count, victim as u64 + 1);
    assert_eq!(report2.initializing_count, 0);
    assert_eq!(report2.orphan_entry_count, 0);
    assert_eq!(report2.hidden_high_level_count, 0);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Row 3–4: crash after NodeInit — the orphan entry occupies a slot no
/// directory entry names. Counted, legal, invisible to search; the NodeId
/// is reissued.
#[test]
fn window_orphan_entry() {
    let cfg = cfg_default();
    let vectors = det_vectors(200, cfg.dim as usize);
    let w = run_window("orphan-entry", &cfg, &vectors, |marks| {
        pick(marks, 1, |m| after_first(m, ProbeMark::NodeInit))
    });
    assert_eq!(w.report.node_count, w.victim as u64);
    assert_eq!(w.report.orphan_entry_count, 1, "the §8.3⑤ orphan");
    assert_eq!(w.report.initializing_count, 0, "orphans are unmapped");
    assert_eq!(w.index.hwm(), w.victim as u64);
    // Search is bitwise-identical to the pre-victim graph (the orphan is
    // unreachable by construction — no mapping, no edges).
    let queries = det_queries(3, cfg.dim as usize);
    for (qi, q) in queries.iter().enumerate() {
        let after = bits(
            &w.index
                .search(w.engine.buffer_pool(), q, 3, Some(10))
                .unwrap(),
        );
        assert_eq!(after, w.baseline[qi], "query {qi} must be bitwise stable");
    }
    let victim = w.victim;
    let Window {
        dir, engine, index, ..
    } = w;
    let id = index
        .insert(engine.buffer_pool(), engine.wal_writer(), &vectors[victim])
        .unwrap();
    assert_eq!(id, NodeId(victim as u32));
    // The continued graph must re-audit clean: the window residue
    // stays accounted and the re-issued victim completes (LIVE).
    let report2 = index.audit(engine.buffer_pool()).unwrap();
    assert_eq!(report2.node_count, victim as u64 + 1);
    assert_eq!(report2.live_count, victim as u64 + 1);
    assert_eq!(report2.initializing_count, 0);
    assert_eq!(report2.orphan_entry_count, 1);
    assert_eq!(report2.hidden_high_level_count, 0);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Row 4–5: crash after DirAppend — the ghost mapping: published NodeId,
/// INITIALIZING entry, no edges. Counted by the audit, unreachable by
/// search (no in-edges), so search is bitwise-identical to baseline.
/// (Unchanged by the M6 swap: step 5's own lists have not landed either.)
#[test]
fn window_ghost_mapping() {
    let cfg = cfg_default();
    let vectors = det_vectors(200, cfg.dim as usize);
    let w = run_window("ghost-mapping", &cfg, &vectors, |marks| {
        // Non-first victim whose full run connects edges (has
        // NeighborEdge) — crash before any of them.
        pick(marks, 1, |m| {
            if m.contains(&ProbeMark::DirAppend)
                && m.iter()
                    .any(|x| matches!(x, ProbeMark::NeighborEdge { .. }))
            {
                after_first(m, ProbeMark::DirAppend)
            } else {
                None
            }
        })
    });
    assert_eq!(w.report.node_count, w.victim as u64 + 1);
    assert_eq!(w.index.hwm(), w.victim as u64 + 1);
    assert_eq!(w.report.initializing_count, 1, "the ghost is INITIALIZING");
    assert_eq!(w.report.live_count, w.victim as u64);
    assert_eq!(
        w.report.edge_count, w.pre_report.edge_count,
        "the ghost contributed no edges (crash before step 5)"
    );
    assert_eq!(
        w.reach.unreachable_initializing_count, 1,
        "the ghost mapping is unreachable (no in-edges)"
    );
    let queries = det_queries(3, cfg.dim as usize);
    for (qi, q) in queries.iter().enumerate() {
        let after = bits(
            &w.index
                .search(w.engine.buffer_pool(), q, 3, Some(10))
                .unwrap(),
        );
        assert_eq!(
            after, w.baseline[qi],
            "query {qi}: the unreachable ghost must not change answers"
        );
    }
    w.done();
}

/// Row 6 interleave (M6 order): crash after the FIRST backward edge — the
/// victim's own lists are ALL in place (step 5 now precedes step 6), so
/// the residue is a reachable node with one inbound edge and COMPLETE
/// out-edges; the M5 "reachable with EMPTY own lists" ghost shape is no
/// longer constructible. Audit passes; every hit is a real node with an
/// exact distance.
#[test]
fn window_partial_backward_edges() {
    let cfg = cfg_default();
    let vectors = det_vectors(200, cfg.dim as usize);
    let w = run_window("partial-edges", &cfg, &vectors, |marks| {
        pick(marks, 1, |m| {
            let edges = m
                .iter()
                .filter(|x| matches!(x, ProbeMark::NeighborEdge { .. }))
                .count();
            if edges >= 2 {
                after_first(
                    m,
                    m.iter()
                        .find(|x| matches!(x, ProbeMark::NeighborEdge { .. }))
                        .copied()
                        .unwrap(),
                )
            } else {
                None
            }
        })
    });
    assert_eq!(w.report.initializing_count, 1);
    assert_eq!(w.report.node_count, w.victim as u64 + 1);
    // New-order sanity: the whole step 5 precedes step 6 — the prefix
    // holds every OwnList mark of the full run (the edge_delta contract).
    let prefix = &w.victim_marks[..w.crash_after];
    let own_total = w
        .victim_marks
        .iter()
        .filter(|x| matches!(x, ProbeMark::OwnList { .. }))
        .count();
    assert_eq!(
        prefix
            .iter()
            .filter(|x| matches!(x, ProbeMark::OwnList { .. }))
            .count(),
        own_total,
        "all own lists must land before the first backward edge"
    );
    assert_eq!(
        prefix
            .iter()
            .filter(|x| matches!(x, ProbeMark::NeighborEdge { .. }))
            .count(),
        1,
        "first-edge crash: exactly one backward edge in the prefix"
    );
    // Exact directed-edge pin: ALL own-list edges plus the single
    // backward edge's delta (+1, or 0 if it shrank).
    assert_eq!(
        w.report.edge_count,
        w.pre_report.edge_count + edge_delta(&w.victim_marks, prefix),
        "exact directed-edge delta: all own lists + the single backward edge"
    );
    let queries = det_queries(3, cfg.dim as usize);
    for q in &queries {
        let hits = w
            .index
            .search(w.engine.buffer_pool(), q, 3, Some(10))
            .unwrap();
        assert_hits_legal(&hits, q, &vectors, w.index.hwm());
    }
    w.done();
}

/// Row 6 shrink interleave (M6 order): m=2/m_max0=4 makes the shrink
/// heuristic fire constantly; crash right after a SHRUNK edge that is not
/// the victim's last record. The own lists are all in place (step 5
/// precedes step 6); the residue is partial INBOUND edges over complete
/// out-edges.
#[test]
fn window_shrink_interleave() {
    let cfg = cfg_shrink();
    let vectors = det_vectors(200, cfg.dim as usize);
    let w = run_window("shrink-interleave", &cfg, &vectors, |marks| {
        pick(marks, 1, |m| {
            m.iter()
                .position(|x| matches!(x, ProbeMark::NeighborEdge { shrank: true, .. }))
                // The crash point must leave unwritten records behind.
                .filter(|&i| i + 1 < m.len())
                .map(|i| i + 1)
        })
    });
    assert_eq!(w.report.initializing_count, 1);
    assert_eq!(w.report.node_count, w.victim as u64 + 1);
    // Exact directed-edge pin across the shrink: ALL own-list edges
    // (step 5 preceded step 6), +1 per landed backward edge, net 0 per
    // shrank one (victim in, evicted node out); the pick guarantees at
    // least one shrink in the prefix.
    let prefix = &w.victim_marks[..w.crash_after];
    // Same edge_delta contract as the partial-edges row: the prefix must
    // hold every OwnList mark of the full run (structural under the M6
    // order — asserted, not assumed).
    let own_total = w
        .victim_marks
        .iter()
        .filter(|x| matches!(x, ProbeMark::OwnList { .. }))
        .count();
    assert_eq!(
        prefix
            .iter()
            .filter(|x| matches!(x, ProbeMark::OwnList { .. }))
            .count(),
        own_total,
        "all own lists must land before the first backward edge"
    );
    assert!(
        prefix
            .iter()
            .any(|x| matches!(x, ProbeMark::NeighborEdge { shrank: true, .. })),
        "the pick guarantees a shrank edge in the prefix"
    );
    assert_eq!(
        w.report.edge_count,
        w.pre_report.edge_count + edge_delta(&w.victim_marks, prefix),
        "exact directed-edge delta across the shrink"
    );
    let queries = det_queries(3, cfg.dim as usize);
    for q in &queries {
        let hits = w
            .index
            .search(w.engine.buffer_pool(), q, 3, Some(10))
            .unwrap();
        assert_hits_legal(&hits, q, &vectors, w.index.hwm());
    }
    w.done();
}

/// Row 5 interleave (new under the M6 order): crash with only SOME own
/// lists written — partial out-edges, ZERO in-edges: a pure orphan.
/// Unreachable by construction, so search is bitwise-identical to the
/// pre-victim graph. (The exact edge delta is not derivable from the
/// marks — OwnList carries no count — so the reachability scan is the
/// pin here.)
#[test]
fn window_partial_own_lists() {
    let cfg = cfg_default();
    let vectors = det_vectors(200, cfg.dim as usize);
    let w = run_window("partial-own-lists", &cfg, &vectors, |marks| {
        pick(marks, 1, |m| {
            let owns = m
                .iter()
                .filter(|x| matches!(x, ProbeMark::OwnList { .. }))
                .count();
            // Crash after the first own list, with at least one more own
            // list (and every backward edge) still unwritten.
            if owns >= 2 {
                m.iter()
                    .position(|x| matches!(x, ProbeMark::OwnList { .. }))
                    .map(|i| i + 1)
            } else {
                None
            }
        })
    });
    assert_eq!(w.report.initializing_count, 1, "the orphan is INITIALIZING");
    assert_eq!(w.report.node_count, w.victim as u64 + 1);
    let prefix = &w.victim_marks[..w.crash_after];
    assert!(
        !prefix
            .iter()
            .any(|x| matches!(x, ProbeMark::NeighborEdge { .. })),
        "zero inbound edges — pure orphan"
    );
    assert_eq!(
        w.reach.unreachable_initializing_count, 1,
        "partial own lists, no in-edges: unreachable orphan"
    );
    let queries = det_queries(3, cfg.dim as usize);
    for (qi, q) in queries.iter().enumerate() {
        let after = bits(
            &w.index
                .search(w.engine.buffer_pool(), q, 3, Some(10))
                .unwrap(),
        );
        assert_eq!(
            after, w.baseline[qi],
            "query {qi}: an unreachable orphan is bitwise-invisible"
        );
    }
    w.done();
}

/// Row 5–6 (the M6-order swap row): crash after the LAST own-list write,
/// before ANY backward edge — the victim has complete out-edges but zero
/// in-edges: a pure orphan. This is the residue the M5 order could never
/// produce here (its symmetric window was the dangerous ghost — inbound
/// edges with EMPTY own lists, now unconstructible). Unreachable, so
/// search is bitwise-identical to the pre-victim graph; the edge delta is
/// exactly the own lists' contribution.
#[test]
fn window_own_lists_complete_no_inbound() {
    let cfg = cfg_default();
    let vectors = det_vectors(200, cfg.dim as usize);
    let w = run_window("own-lists-complete", &cfg, &vectors, |marks| {
        pick(marks, 1, |m| {
            // Crash after the last OwnList with at least one backward
            // edge still unwritten (the window must be mid-connect).
            let has_edge = m
                .iter()
                .any(|x| matches!(x, ProbeMark::NeighborEdge { .. }));
            let last_own = m
                .iter()
                .rposition(|x| matches!(x, ProbeMark::OwnList { .. }));
            if has_edge {
                last_own.map(|i| i + 1)
            } else {
                None
            }
        })
    });
    assert_eq!(w.report.initializing_count, 1);
    assert_eq!(w.report.node_count, w.victim as u64 + 1);
    let prefix = &w.victim_marks[..w.crash_after];
    assert!(
        !prefix
            .iter()
            .any(|x| matches!(x, ProbeMark::NeighborEdge { .. })),
        "crash before any backward edge"
    );
    // Exact directed-edge pin: every own list landed, no backward edge
    // did — the delta is exactly the own lists' contribution.
    assert_eq!(
        w.report.edge_count,
        w.pre_report.edge_count + edge_delta(&w.victim_marks, prefix),
        "exact directed-edge delta of the own lists"
    );
    assert_eq!(
        w.reach.unreachable_initializing_count, 1,
        "complete own lists, no in-edges: unreachable orphan"
    );
    let queries = det_queries(3, cfg.dim as usize);
    for (qi, q) in queries.iter().enumerate() {
        let after = bits(
            &w.index
                .search(w.engine.buffer_pool(), q, 3, Some(10))
                .unwrap(),
        );
        assert_eq!(
            after, w.baseline[qi],
            "query {qi}: an unreachable orphan is bitwise-invisible"
        );
    }
    w.done();
}

/// Row 6–7 (non-first victim): crash right BEFORE MetaUpdate — the victim
/// towers above the published max_level (the weakened §8.3① invariant
/// tolerates it; the audit COUNTS it). The window POSITION is unchanged
/// by the M6 swap; the prefix content is now all own lists + all backward
/// edges, so the victim is fully connected and REACHABLE.
#[test]
fn window_hidden_high_level() {
    let cfg = cfg_default();
    let vectors = det_vectors(200, cfg.dim as usize);
    let w = run_window("hidden-high-level", &cfg, &vectors, |marks| {
        pick(marks, 1, |m| {
            // MetaUpdate present ⇔ the victim's level exceeds the running
            // max; crash after the mark right before it.
            m.iter()
                .position(|x| *x == ProbeMark::MetaUpdate)
                .filter(|&i| i >= 1)
        })
    });
    let (pre_ep, pre_ml, _) = w.pre_state;
    assert_eq!(w.report.hidden_high_level_count, 1, "the towering victim");
    assert_eq!(w.report.entry_point, pre_ep, "meta was never updated");
    assert_eq!(w.report.max_level, pre_ml);
    assert_eq!(w.report.initializing_count, 1);
    assert_eq!(
        w.reach.unreachable_initializing_count, 0,
        "fully connected: the towering victim is reachable"
    );
    let queries = det_queries(3, cfg.dim as usize);
    for q in &queries {
        let hits = w
            .index
            .search(w.engine.buffer_pool(), q, 3, Some(10))
            .unwrap();
        assert_hits_legal(&hits, q, &vectors, w.index.hwm());
    }
    w.done();
}

/// Row 6–7, FIRST-node sub-case: the very first insert crashes after
/// DirAppend — the chain is non-empty but the meta still says "empty
/// graph". The open-time repair (§10.3) must publish entry_point = 0;
/// the second reopen proves the repair idempotent.
#[test]
fn window_first_node_entry_repair() {
    let cfg = cfg_default();
    let vectors = det_vectors(1, cfg.dim as usize);
    let w = run_window("first-node-repair", &cfg, &vectors, |marks| {
        pick(marks, 0, |m| after_first(m, ProbeMark::DirAppend))
    });
    assert_eq!(w.victim, 0);
    // The repair published the entry point (OpenOutcome carries no
    // explicit repair signal — the visible state IS the signal).
    assert_eq!(w.report.node_count, 1);
    assert_eq!(w.report.entry_point, 0);
    assert_eq!(w.report.max_level, w.index.max_level());
    assert_eq!(w.report.initializing_count, 1, "PublishLive never landed");
    assert_eq!(
        w.reach.unreachable_initializing_count, 0,
        "the repaired entry point is reachable by construction"
    );
    // The repaired entry point is reachable (INITIALIZING is recallable,
    // §8.1③): the vector recalls itself at distance 0.
    let hits = w
        .index
        .search(w.engine.buffer_pool(), &vectors[0], 1, Some(1))
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].0, NodeId(0));
    assert_eq!(hits[0].1, 0.0);
    w.done();
}

/// Row 7–8: crash after MetaUpdate — the victim IS the published entry
/// point but still INITIALIZING. Reachable and recallable (§8.1③).
#[test]
fn window_meta_before_publish() {
    let cfg = cfg_default();
    let vectors = det_vectors(200, cfg.dim as usize);
    let w = run_window("meta-before-publish", &cfg, &vectors, |marks| {
        pick(marks, 1, |m| after_first(m, ProbeMark::MetaUpdate))
    });
    assert_eq!(w.report.initializing_count, 1);
    assert_eq!(w.report.entry_point, w.victim as u32);
    assert_eq!(w.report.max_level, w.index.max_level());
    assert_eq!(
        w.reach.unreachable_initializing_count, 0,
        "the entry point itself is trivially reachable"
    );
    let queries = det_queries(3, cfg.dim as usize);
    for q in &queries {
        let hits = w
            .index
            .search(w.engine.buffer_pool(), q, 3, Some(10))
            .unwrap();
        assert_hits_legal(&hits, q, &vectors, w.index.hwm());
    }
    w.done();
}

/// Sanity pin: the probe log's shape — the first insert's exact mark
/// sequence, and two same-config runs producing identical per-insert
/// marks (the whole matrix's determinism premise).
#[test]
fn probe_log_shape() {
    let cfg = cfg_default();
    let vectors = det_vectors(2, cfg.dim as usize);
    let run = |tag: &str| {
        let mut lab = start_lab(&cfg, tag);
        let mut out = Vec::new();
        for v in &vectors {
            let (r, marks) = insert_logged(&mut lab, v);
            r.unwrap();
            out.push(marks);
        }
        let dir = crash(lab).0;
        let _ = std::fs::remove_dir_all(&dir);
        out
    };
    let a = run("shape-a");
    let b = run("shape-b");
    assert_eq!(a, b, "identical inputs must give identical mark streams");

    // Insert 0 (empty graph): allocate page, init entry, publish mapping,
    // publish entry point, go live — never any edge work.
    assert_eq!(
        a[0],
        vec![
            ProbeMark::NodePageInit,
            ProbeMark::NodeInit,
            ProbeMark::DirAppend,
            ProbeMark::MetaUpdate,
            ProbeMark::PublishLive,
        ]
    );
    // Insert 1 (seed 0xC0FFEE's second draw is level 0 — pinned by the
    // exact sequence): no page work; the M6 order writes the own level-0
    // list FIRST (step 5), then the one backward edge to node 0 (step 6);
    // no MetaUpdate (level 0 does not exceed max_level 0).
    assert_eq!(
        a[1],
        vec![
            ProbeMark::NodeInit,
            ProbeMark::DirAppend,
            ProbeMark::OwnList { level: 0 },
            ProbeMark::NeighborEdge {
                level: 0,
                shrank: false,
            },
            ProbeMark::PublishLive,
        ]
    );
}
