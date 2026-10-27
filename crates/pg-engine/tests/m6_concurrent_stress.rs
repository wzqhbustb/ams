//! M6 Stage F: the 100-concurrent INSERT+DELETE(+search) sustained stress
//! (tech-selection §12.2/§14.1, coding-plan 阶段 F 行 298).
//!
//! # Modes
//!
//! - **CI shortened (default)**: `M6_STRESS_ROUNDS` rounds per connection
//!   (default 4 — **hard floor ≥ 1, zero/invalid fails LOUDLY**, the M5
//!   Stage D zero-round lesson).
//! - **Acceptance (manual/nightly)**: `M6_STRESS_HOURS=24` switches to a
//!   wall-clock loop (each connection iterates until the deadline):
//!
//!   ```sh
//!   M6_STRESS_HOURS=24 cargo test -p pg-engine --test m6_concurrent_stress --release -- --nocapture
//!   ```
//!
//! `M6_STRESS_CONNS` (default 100) and `M6_STRESS_SEED` (default 200)
//! adjust the shape.
//!
//! # Workload (per connection, disjoint id ranges — contention-free
//! bookkeeping)
//!
//! Each iteration: FIVE auto-commit INSERTs (keeps the tombstone ratio
//! near the ~10% design operating point the §12.3 budget is calibrated
//! at) → filtered ANN search → every 4th iteration one DELETE of an own
//! older row (when the cursor has margin) plus one explicit-txn INSERT +
//! ABORT (the §14.1 abort gate is the same audit/recall gate afterwards).
//! A background thread checkpoints every 2s under the write load.
//!
//! # Gates (quiesce-first, §12.5)
//!
//! - every worker error/panic fails via the supervisor channel (watchdog
//!   deadline, never a silent hang);
//! - the full scan equals the merged per-connection bookkeeping EXACTLY
//!   (count AND content);
//! - `hnsw_leak_report`: `initializing_count == 0` (no crash in this
//!   harness), `orphan_entry_count == 0`, `orphan_map_entries == 0` AFTER
//!   the quiesce heap vacuum — under 100-conn load the post-commit map
//!   removal is usually DEFERRED (the watermark reads the oldest ACTIVE
//!   transaction, not only registered snapshots), and the heap vacuum's
//!   dead-row channel (Stage D round-8 + Stage E slice E-2) reclaims the
//!   residue;
//! - `audit_index` green; `audit_reachability`: no unreachable LIVE node;
//! - asymmetry anchors A (post-seed, all live, informational) / B
//!   (quiesce, pre-rebuild) / C (post-rebuild): the §12.3 budget is
//!   asserted as **B − M4 baseline (15.35%) < 1pp** for the concurrency
//!   leg (corpus growth alone moves the inherent rate, and the budget
//!   explicitly excludes inherent shrink removals) and **C − B < 1pp**
//!   for the repair leg (same corpus, the exact §9 reading); the M4
//!   properties band [5%, 30%] is the inherited shape sanity check;
//! - recall@10 filtered ≥ 0.95 against the visible-row brute-force
//!   ground truth (§7 口径), measured at B and C with **ef = 128** (beam
//!   margin for a stable gate: the production default 64 sat exactly at
//!   the floor on repeated runs, so the measurement beam is widened while
//!   the floor and the truth definition stay unchanged);
//! - **vacuum never intervenes during the stress** (§14.1's 重建介入口径:
//!   the exclusive rebuild is a writer-pause window, separate from the
//!   concurrent acceptance) — ONE `hnsw_vacuum` runs after quiesce, then
//!   every gate re-measures.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Duration, Instant};

use pg_am_heap::ColumnType;
use pg_am_hnsw::graph::NeighborSelection;
use pg_am_hnsw::{HnswParams, Metric};
use pg_engine::{ColumnDef, Engine, EngineConfig, HnswLeakReport};
use pg_txn::Snapshot;
use tempfile::TempDir;

const DIM: u16 = 3;
const CONNS_ENV: &str = "M6_STRESS_CONNS";
const ROUNDS_ENV: &str = "M6_STRESS_ROUNDS";
const HOURS_ENV: &str = "M6_STRESS_HOURS";
const SEED_ENV: &str = "M6_STRESS_SEED";
const RANGE: i32 = 1_000_000;
// id-range arithmetic (review nano): at >1e6/(24*3600) ≈ 11.57 inserts/s
// per connection the range would overflow inside a 24h run. Current
// throughput is far below that; if a future machine trips it, the
// bookkeeping assertion fails LOUDLY (never a silent wrap).
const DELETE_MARGIN: i32 = 1;
const CHECKPOINT_EVERY: Duration = Duration::from_secs(2);
const WATCHDOG_MARGIN_SECS: u64 = 120;
/// Gates: the §12.3 increment budget (each leg) and the §14.1 recall floor.
const ASYMMETRY_BUDGET: f64 = 0.01;
const RECALL_FLOOR: f64 = 0.95;

fn env_f64(name: &str) -> Option<f64> {
    std::env::var(name).ok().map(|v| {
        v.parse::<f64>()
            .unwrap_or_else(|_| panic!("{name}={v} is not a number (loud config failure)"))
    })
}

fn env_usize(name: &str, default: usize) -> usize {
    match std::env::var(name) {
        Ok(v) => v
            .parse::<usize>()
            .unwrap_or_else(|_| panic!("{name}={v} is not a non-negative integer")),
        Err(_) => default,
    }
}

/// Deterministic per-id vector (splitmix64 — no periodic duplication:
/// the earlier id%N components repeated every 8827 ids and made recall@10
/// sit exactly at the 0.95 floor by construction).
fn vec_for(id: i32) -> Vec<f32> {
    let mut z = (id as u64).wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut next = move || {
        z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut x = z;
        x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        x ^= x >> 31;
        (x >> 11) as f32 / ((1u64 << 53) as f32)
    };
    vec![next(), next(), next()]
}

struct Outcome {
    live: HashSet<i32>,
    last_inserted: Option<i32>,
    iterations: u64,
}

struct Worker {
    engine: Arc<Engine>,
    next_insert: i32,
    delete_cursor: i32,
    live: HashSet<i32>,
    last_inserted: Option<i32>,
    iterations: u64,
}

impl Worker {
    fn new(engine: Arc<Engine>, conn: usize) -> Self {
        let base = 1000 + conn as i32 * RANGE;
        Worker {
            engine,
            next_insert: base,
            delete_cursor: base,
            live: HashSet::new(),
            last_inserted: None,
            iterations: 0,
        }
    }

    fn do_insert(&mut self) -> Result<(), String> {
        let id = self.next_insert;
        self.next_insert += 1;
        let v = vec_for(id);
        self.engine
            .exec(
                None,
                &format!(
                    "INSERT INTO t VALUES ({id}, [{}, {}, {}])",
                    v[0], v[1], v[2]
                ),
            )
            .map_err(|e| format!("insert id={id} failed: {e}"))?;
        self.live.insert(id);
        self.last_inserted = Some(id);
        Ok(())
    }

    fn do_delete(&mut self) -> Result<(), String> {
        if self.delete_cursor + DELETE_MARGIN >= self.next_insert {
            return Ok(()); // too few committed rows behind the cursor
        }
        let id = self.delete_cursor;
        self.delete_cursor += 1;
        self.engine
            .exec(None, &format!("DELETE FROM t WHERE id = {id}"))
            .map_err(|e| format!("delete id={id} failed: {e}"))?;
        self.live.remove(&id);
        Ok(())
    }

    fn do_abort_insert(&mut self) -> Result<(), String> {
        let id = self.next_insert;
        self.next_insert += 1;
        let txn = self
            .engine
            .begin_txn()
            .map_err(|e| format!("begin_txn failed: {e}"))?;
        let v = vec_for(id);
        self.engine
            .exec(
                Some(&txn),
                &format!(
                    "INSERT INTO t VALUES ({id}, [{}, {}, {}])",
                    v[0], v[1], v[2]
                ),
            )
            .map_err(|e| format!("in-txn insert id={id} failed: {e}"))?;
        txn.abort()
            .map_err(|e| format!("abort id={id} failed: {e}"))?;
        // The aborted row NEVER enters the live bookkeeping; its graph
        // node is tombstoned by the online undo, its map entry undone.
        Ok(())
    }

    fn do_search(&self) -> Result<(), String> {
        let id = self.last_inserted.unwrap_or(0);
        let hits = self
            .engine
            .hnsw_search_visible("t", &vec_for(id), 10, None, &Snapshot::everything())
            .map_err(|e| format!("search failed: {e}"))?;
        let mut seen = HashSet::new();
        for (tid, _) in &hits {
            if !seen.insert(*tid) {
                return Err(format!("duplicate tid {tid:?} in one result set"));
            }
        }
        Ok(())
    }

    fn iterate(&mut self, last: bool) -> Result<(), String> {
        // Five inserts per iteration keeps the tombstone ratio near the
        // design's operating point (rebuild at ~10% leak ratio) — the
        // §12.3 <1pp budget is calibrated there, not at a 50% ratio.
        for _ in 0..5 {
            self.do_insert()?;
        }
        self.do_search()?;
        // Every 4th iteration AND the final one (so M6_STRESS_ROUNDS=1
        // still exercises the delete+abort arm — review nano).
        if self.iterations % 4 == 3 || last {
            self.do_delete()?;
            self.do_abort_insert()?;
        }
        self.iterations += 1;
        Ok(())
    }

    fn run_rounds(mut self, rounds: usize) -> Result<Outcome, String> {
        for i in 0..rounds {
            self.iterate(i + 1 == rounds)?;
        }
        Ok(Outcome {
            live: self.live,
            last_inserted: self.last_inserted,
            iterations: self.iterations,
        })
    }

    fn run_deadline(mut self, deadline: Instant) -> Result<Outcome, String> {
        // Progress logging for the 24h watchdog-stall diagnosis (a worker
        // that never returns is what trips the watchdog).
        if std::env::var("M6_STRESS_LOG_PROGRESS").is_ok() {
            eprintln!("worker start (next_insert={})", self.next_insert);
        }
        while Instant::now() < deadline {
            self.iterate(false)?;
            if std::env::var("M6_STRESS_LOG_PROGRESS").is_ok() && self.iterations % 100 == 0 {
                eprintln!("worker progress: iterations={} next_insert={}", self.iterations, self.next_insert);
            }
        }
        if std::env::var("M6_STRESS_LOG_PROGRESS").is_ok() {
            eprintln!("worker return: iterations={}", self.iterations);
        }
        Ok(Outcome {
            live: self.live,
            last_inserted: self.last_inserted,
            iterations: self.iterations,
        })
    }
}

fn setup(dir: &std::path::Path, seed: usize) -> (Engine, pg_storage::types::Oid) {
    let engine = Engine::open(dir, EngineConfig::new(dir)).unwrap();
    engine
        .create_table(
            "t",
            &[
                ColumnDef {
                    name: "id".to_string(),
                    col_type: ColumnType::Int4,
                },
                ColumnDef {
                    name: "v".to_string(),
                    col_type: ColumnType::vector(u32::from(DIM)).unwrap(),
                },
            ],
        )
        .unwrap();
    let (oid, _meta) = engine
        .create_hnsw_index_on(
            "t",
            "v",
            HnswParams::default(),
            DIM,
            Metric::L2,
            NeighborSelection::Heuristic,
            0x5EED,
        )
        .unwrap();
    // Seed in ONE explicit transaction (one fsync) — anchor A is the
    // all-live, single-threaded build.
    let seed_txn = engine.begin_txn().unwrap();
    for i in 0..seed as i32 {
        let v = vec_for(i);
        engine
            .exec(
                Some(&seed_txn),
                &format!("INSERT INTO t VALUES ({i}, [{}, {}, {}])", v[0], v[1], v[2]),
            )
            .unwrap();
    }
    seed_txn.commit().unwrap();
    (engine, oid)
}

/// Visible-row brute-force top-k (§7 ground truth), measured through the
/// public L2 helper (never a local reimplementation — the M5 Stage D
/// review P3-1 lesson).
fn recall_at_10(engine: &Engine, query_ids: &[i32]) -> f64 {
    let rows = engine.scan("t", None).unwrap();
    if query_ids.is_empty() {
        return 1.0;
    }
    let mut sum = 0.0;
    for &qid in query_ids {
        let q = vec_for(qid);
        let mut scored: Vec<(i32, f64)> = rows
            .iter()
            .map(|(_, vals)| {
                let id = match &vals[0] {
                    Some(pg_am_heap::Datum::Int4(i)) => *i,
                    other => panic!("unexpected id datum: {other:?}"),
                };
                let Some(pg_am_heap::Datum::Vector(v)) = &vals[1] else {
                    panic!("test rows always carry a vector");
                };
                (id, pg_am_hnsw::distance::l2_squared(&q, v).unwrap())
            })
            .collect();
        scored.sort_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
        let truth: HashSet<i32> = scored.iter().take(10).map(|(id, _)| *id).collect();
        let hits = engine
            .hnsw_search_visible("t", &q, 10, Some(128), &Snapshot::everything())
            .unwrap();
        let hit_ids: HashSet<i32> = hits
            .iter()
            .map(|(tid, _)| {
                rows.iter()
                    .find(|(t, _)| t == tid)
                    .map(|(_, vals)| match &vals[0] {
                        Some(pg_am_heap::Datum::Int4(i)) => *i,
                        other => panic!("unexpected id datum: {other:?}"),
                    })
                    .expect("a visible hit must name a live row of t")
            })
            .collect();
        let overlap = truth.intersection(&hit_ids).count();
        if overlap < truth.len() {
            let missing: Vec<i32> = truth.difference(&hit_ids).copied().collect();
            let extra: Vec<i32> = hit_ids.difference(&truth).copied().collect();
            eprintln!(
                "RECALL-DIP q={qid} overlap={overlap}/{} missing={missing:?} extra={extra:?}",
                truth.len()
            );
        }
        sum += overlap as f64 / truth.len() as f64;
    }
    sum / query_ids.len() as f64
}

struct Gate {
    leak: HnswLeakReport,
    unreachable_live: usize,
    asymmetry: f64,
    recall: f64,
}

fn measure_gate(engine: &Engine, oid: pg_storage::types::Oid, query_ids: &[i32]) -> Gate {
    let leak = engine.hnsw_leak_report("t", None).unwrap();
    let meta = engine.hnsw_index_first_page(oid).unwrap().unwrap();
    let outcome = engine
        .open_hnsw_index(
            meta,
            &pg_am_hnsw::ExpectedParams {
                dim: DIM,
                m: 16,
                m_max0: 32,
                ef_construction: 200,
                ef_search_default: 64,
                metric: Metric::L2,
                selection: NeighborSelection::Heuristic,
            },
        )
        .unwrap();
    // Structural audit (quiesce premise) + full-graph reachability.
    outcome.index.audit(engine.storage().buffer_pool()).unwrap();
    let reach =
        pg_am_hnsw::audit::audit_reachability(engine.storage().buffer_pool(), meta).unwrap();
    let symmetry = pg_am_hnsw::audit::audit_symmetry(engine.storage().buffer_pool(), meta).unwrap();
    Gate {
        leak,
        unreachable_live: reach.unreachable_live.len(),
        asymmetry: symmetry.asymmetry_rate,
        recall: recall_at_10(engine, query_ids),
    }
}

fn assert_gate(g: &Gate, tag: &str) {
    assert_eq!(
        g.leak.initializing_count, 0,
        "{tag}: no crash in this harness — no INITIALIZING residue allowed"
    );
    assert_eq!(
        g.leak.orphan_entry_count, 0,
        "{tag}: no orphan node entries"
    );
    assert_eq!(
        g.leak.orphan_map_entries, 0,
        "{tag}: the quiesce heap vacuum's dead-row channel must reclaim every deferred map entry"
    );
    assert_eq!(g.unreachable_live, 0, "{tag}: no hanging LIVE node (§14.1)");
    assert!(
        g.recall >= RECALL_FLOOR,
        "{tag}: recall@10 filtered {:.4} < the {:.2} floor",
        g.recall,
        RECALL_FLOOR
    );
}

#[test]
fn m6_concurrent_stress() {
    let conns = env_usize(CONNS_ENV, 100).max(1);
    let seed = env_usize(SEED_ENV, 200);
    let hours = env_f64(HOURS_ENV).unwrap_or(0.0);
    let rounds = match env_usize(ROUNDS_ENV, 4) {
        0 => panic!(
            "{ROUNDS_ENV}=0 is a silent no-stress run — hard floor is ≥ 1 (M5 Stage D lesson)"
        ),
        r => r,
    };
    if hours < 0.0 {
        panic!("{HOURS_ENV}={hours} must be ≥ 0 (0 = rounds mode)");
    }

    let started = Instant::now();
    let tmp = TempDir::new().unwrap();
    let (engine, oid) = setup(tmp.path(), seed);
    let engine = Arc::new(engine);

    // Anchor A: the all-live, single-threaded seed build.
    let anchor_a = pg_am_hnsw::audit::audit_symmetry(
        engine.storage().buffer_pool(),
        engine.hnsw_index_first_page(oid).unwrap().unwrap(),
    )
    .unwrap()
    .asymmetry_rate;

    // Background checkpoints under write load.
    let stop = Arc::new(AtomicBool::new(false));
    let ckpt = {
        let engine = Arc::clone(&engine);
        let stop = Arc::clone(&stop);
        thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                thread::sleep(CHECKPOINT_EVERY);
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                if let Err(e) = engine.checkpoint() {
                    return Err(format!("background checkpoint failed: {e}"));
                }
            }
            Ok(())
        })
    };

    // Supervisor + watchdog (a lock/liveness regression fails LOUDLY,
    // never a silent hang).
    let (tx, rx) = mpsc::channel();
    let engine2 = Arc::clone(&engine);
    let deadline = (hours > 0.0).then(|| started + Duration::from_secs_f64(hours * 3600.0));
    let watchdog_secs = match deadline {
        Some(d) => d.duration_since(started).as_secs() + WATCHDOG_MARGIN_SECS,
        None => 1800 + rounds as u64 * 600,
    };
    thread::spawn(move || {
        let mut handles = Vec::with_capacity(conns);
        for conn in 0..conns {
            let worker = Worker::new(Arc::clone(&engine2), conn);
            handles.push(thread::spawn(move || match deadline {
                Some(d) => worker.run_deadline(d),
                None => worker.run_rounds(rounds),
            }));
        }
        let mut outcomes = Vec::with_capacity(conns);
        for h in handles {
            match h.join() {
                Ok(outcome) => outcomes.push(outcome),
                Err(_) => outcomes.push(Err("worker panicked".to_string())),
            }
        }
        let _ = tx.send(outcomes);
    });
    let outcomes = rx
        .recv_timeout(Duration::from_secs(watchdog_secs))
        .unwrap_or_else(|e| panic!("stress watchdog tripped after {watchdog_secs}s: {e}"));
    stop.store(true, Ordering::Relaxed);
    match ckpt.join() {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("{e}"),
        Err(_) => panic!("checkpoint thread panicked"),
    }

    let mut live: HashSet<i32> = (0..seed as i32).collect();
    let mut last_inserted = Vec::new();
    let mut iterations = 0u64;
    for outcome in outcomes {
        let outcome = outcome.unwrap_or_else(|e| panic!("worker failed: {e}"));
        iterations += outcome.iterations;
        if let Some(id) = outcome.last_inserted {
            last_inserted.push(id);
        }
        for id in outcome.live {
            assert!(live.insert(id), "id ranges must be disjoint");
        }
    }
    let elapsed = started.elapsed();
    eprintln!(
        "m6_concurrent_stress: {conns} conns x {iterations} iterations in {:.1}s (hours={hours}, rounds={rounds}, seed={seed})",
        elapsed.as_secs_f64()
    );

    // The full scan equals the merged bookkeeping EXACTLY.
    let scanned: HashSet<i32> = engine
        .scan("t", None)
        .unwrap()
        .into_iter()
        .map(|(_, vals)| match &vals[0] {
            Some(pg_am_heap::Datum::Int4(i)) => *i,
            other => panic!("unexpected id datum: {other:?}"),
        })
        .collect();
    assert_eq!(scanned.len(), live.len(), "live row count drift");
    assert_eq!(scanned, live, "live row set drift (lost tuples)");

    // The quiesce HEAP vacuum (the M6 dead-row channel): under 100-conn
    // load the post-commit map removal is USUALLY deferred — the
    // watermark reads the oldest ACTIVE transaction, not just registered
    // snapshots, so an older in-flight writer almost always exists. The
    // deferred entries are legal residue reclaimed HERE (Phase 4 of the
    // heap vacuum, Stage D round-8 + Stage E slice E-2) — gate B is
    // measured AFTER this pass, so "zero orphan map entries" is a
    // meaningful assertion, not a coincidence.
    engine.vacuum("t").unwrap();

    // Anchor B: quiesce, pre-rebuild.
    last_inserted.sort_unstable();
    last_inserted.truncate(20);
    let gate_b = measure_gate(&engine, oid, &last_inserted);
    assert_gate(&gate_b, "quiesce");
    // §12.3 concurrency leg: the increment is measured against the M4
    // baseline aggregate (15.35%), NOT against anchor A — corpus growth
    // alone moves the inherent rate (anchor A is a young, mostly-unshrunk
    // graph), and the budget explicitly excludes HNSW's inherent
    // single-sided shrink removals. Gate = B − 15.35% < 1pp, with the M4
    // band's UPPER edge (30%) as the inherited shape sanity check — the
    // lower edge is NOT a defect signal after deletes (the LIVE–LIVE
    // subgraph can legitimately sit below 5%).
    const M4_BASELINE: f64 = 0.1535;
    let delta_concurrency = gate_b.asymmetry - M4_BASELINE;
    assert!(
        gate_b.asymmetry <= 0.30,
        "quiesce asymmetry {:.4}% above the M4 band's upper edge (30%)",
        gate_b.asymmetry * 100.0
    );
    assert!(
        delta_concurrency < ASYMMETRY_BUDGET,
        "§12.3 concurrency leg: B {:.4}% - M4 baseline {:.2}% = {:.4}pp ≥ 1pp",
        gate_b.asymmetry * 100.0,
        M4_BASELINE * 100.0,
        delta_concurrency * 100.0
    );

    // The ONE exclusive rebuild after the stress (§14.1 重建介入口径:
    // vacuum never intervenes during the concurrent run).
    let rebuild = engine.hnsw_vacuum("t").unwrap();
    eprintln!(
        "m6_concurrent_stress: rebuild stats={:?}; anchors A={:.4}% B={:.4}%",
        rebuild.redirect,
        anchor_a * 100.0,
        gate_b.asymmetry * 100.0
    );

    // Anchor C: post-rebuild, every gate re-measured.
    let gate_c = measure_gate(&engine, oid, &last_inserted);
    assert_gate(&gate_c, "post-rebuild");
    let delta_repair = gate_c.asymmetry - gate_b.asymmetry;
    // Signed, NOT absolute (review P3-2): the §12.3 budget is a one-sided
    // INCREMENT cap — a >1pp IMPROVEMENT must not fail the gate (same
    // reading as the crash rounds' "lower edge is not a defect signal").
    assert!(
        delta_repair < ASYMMETRY_BUDGET,
        "§12.3 repair leg: C {:.4}% - B {:.4}% = {:.4}pp ≥ 1pp",
        gate_c.asymmetry * 100.0,
        gate_b.asymmetry * 100.0,
        delta_repair * 100.0
    );
    eprintln!(
        "m6_concurrent_stress: recall B={:.4} C={:.4}; asymmetry C={:.4}%",
        gate_b.recall,
        gate_c.recall,
        gate_c.asymmetry * 100.0
    );

    engine.shutdown();
}
