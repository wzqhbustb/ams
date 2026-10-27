//! M6 Stage F (coding-plan 行 299): concurrent stress × SIGKILL crash
//! rounds — the M5 crash-rounds shape (child process, atomic expectation
//! files, RAII reaper), with the M6 concurrent workload and the §14.2
//! transaction-semantics pins.
//!
//! # Round shapes
//!
//! - **full** (even rounds, except the special round below): the
//!   concurrent workload completes, then the kill. Exact checks: scan ==
//!   recorded live rows; every audit gate.
//! - **mid** (odd rounds): kill at a seed-derived progress point. Prefix
//!   checks: recorded-deleted ∉ scan; |recorded-live \ scan| ≤ conns AND
//!   |scan \ recorded-live| ≤ conns — each connection rewrites its
//!   expectation after EVERY committed op, so at most ONE op per
//!   connection is committed-but-unrecorded at the kill (an unrecorded
//!   delete can hide a recorded-live row, an unrecorded insert can add
//!   one); every audit gate.
//! - **v1.4 P1 regression** (every 8th round, `r % 8 == 3`): an
//!   in-flight DELETE × checkpoint × SIGKILL. After recovery the row must
//!   be VISIBLE again (the delete aborted), its node LIVE, and the map
//!   entry present — the M6 v1.4 zero-graph-undo ruling.
//!
//! # §14.2 pins
//!
//! - committed rows visible, in-flight/aborted rows not visible (the
//!   expectation files only ever record COMMITTED operations, so
//!   committed-but-unrecorded rows are tolerated in mid mode);
//! - **in-flight residue bound**: `initializing_count`, `orphan_entry_count`,
//!   `hidden_high_level_count` each ≤ the writer concurrency (the
//!   "崩溃窗口 × 并发度" bound — each in-flight insert leaves at most one
//!   residue of each kind);
//! - recovery < 30s (the M5 §13.2 checkpoint-increment window);
//! - **committed delete × cleanup skipped**: the dead-row channel
//!   converges it — heap vacuum after recovery reclaims every deferred
//!   map entry (`orphan_map_entries == 0`) and lands every tombstone
//!   (§14.2's last clause);
//! - quiesce audit + recall@10 ≥ 0.95 (visible-row brute force, ef=128)
//!   after recovery, and again after one `hnsw_vacuum` rebuild;
//! - asymmetry stays below the M4 properties band's UPPER edge (30%);
//!   the band's lower edge is not a defect signal after deletes (the
//!   LIVE–LIVE subgraph can legitimately sit below 5%);
//!
//! # Configuration
//!
//! `M6_CRASH_ROUNDS` (default 25; **hard floor ≥ 1, zero loud**),
//! `M6_CRASH_CONNS` (default 16 writers). Acceptance shape:
//!
//! ```sh
//! M6_CRASH_ROUNDS=1000 cargo test -p pg-engine --test m6_crash_rounds --release -- --nocapture
//! ```

use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use pg_am_heap::ColumnType;
use pg_am_hnsw::graph::NeighborSelection;
use pg_am_hnsw::{HnswParams, Metric};
use pg_engine::{ColumnDef, Engine, EngineConfig, Oid};
use pg_txn::Snapshot;

const CHILD_ENV_VAR: &str = "M6_CRASH_CHILD";
const DIR_ENV_VAR: &str = "M6_CRASH_DIR";
const SEED_ENV_VAR: &str = "M6_CRASH_SEED";
const ROUNDS_ENV_VAR: &str = "M6_CRASH_ROUNDS";
const CONNS_ENV_VAR: &str = "M6_CRASH_CONNS";
const CHILD_TEST_NAME: &str = "m6_crash_child_entry";
const READY_MARKER: &str = "ready-to-die";
const ENGINE_READY_MARKER: &str = "engine-ready";
const DIM: u16 = 3;
const RANGE: i32 = 1_000_000;
const RECOVERY_BUDGET: Duration = Duration::from_secs(30);

/// splitmix64 per-id vector (the F-1 corpus shape — no periodic
/// duplication, so the recall gate has real margin).
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

fn conns() -> usize {
    std::env::var(CONNS_ENV_VAR)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(16)
}

// ---------------------------------------------------------------------------
// Child process
// ---------------------------------------------------------------------------

/// Reopen after the kill: the schema already exists (never re-create) —
/// resolve the table's HNSW index OID from the live registry.
fn reopen(data_dir: &Path) -> (Engine, Oid) {
    let engine = Engine::open(data_dir, EngineConfig::new(data_dir)).unwrap();
    let idx = engine
        .indexes()
        .into_iter()
        .find(|e| matches!(e.key_type, ColumnType::Vector(_)))
        .expect("the crashed database must still carry its HNSW index");
    (engine, idx.index_oid)
}

#[test]
fn m6_crash_child_entry() {
    if std::env::var(CHILD_ENV_VAR).is_err() {
        return;
    }
    let data_dir = std::env::var(DIR_ENV_VAR).expect("data dir required");
    let seed: u64 = std::env::var(SEED_ENV_VAR)
        .expect("seed required")
        .parse()
        .expect("seed is u64");
    let special = seed % 8 == 3;
    if special {
        run_p1_child(Path::new(&data_dir));
    } else {
        run_child(Path::new(&data_dir), seed);
    }
}

fn setup(dir: &Path) -> (Engine, Oid) {
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
    (engine, oid)
}

/// The in-flight DELETE × checkpoint × SIGKILL scenario (v1.4 P1). The
/// engine stays in scope until the kill.
fn run_p1_child(data_dir: &Path) -> ! {
    let (engine, _oid) = setup(data_dir);
    let v = vec_for(1);
    engine
        .exec(
            None,
            &format!("INSERT INTO t VALUES (1, [{}, {}, {}])", v[0], v[1], v[2]),
        )
        .unwrap();
    let tid = engine.scan("t", None).unwrap()[0].0;
    let txn = engine.begin_txn().unwrap();
    engine
        .exec(Some(&txn), "DELETE FROM t WHERE id = 1")
        .unwrap();
    engine.checkpoint().unwrap();
    let _ = tid;
    std::mem::forget(txn); // the delete never commits, never aborts
    fs::write(data_dir.join(READY_MARKER), b"").unwrap();
    loop {
        thread::sleep(Duration::from_secs(1));
    }
}

/// Per-connection committed bookkeeping, atomically rewritten after every
/// committed operation (tmp + fsync + rename; the parent only ever reads
/// the last complete version).
fn write_expectation(data_dir: &Path, conn: usize, live: &HashSet<i32>, dead: &HashSet<i32>) {
    let mut live_v: Vec<i32> = live.iter().copied().collect();
    live_v.sort_unstable();
    let mut dead_v: Vec<i32> = dead.iter().copied().collect();
    dead_v.sort_unstable();
    let body = format!(
        "LIVE {}\nDEAD {}\n",
        live_v
            .iter()
            .map(|i| i.to_string())
            .collect::<Vec<_>>()
            .join(","),
        dead_v
            .iter()
            .map(|i| i.to_string())
            .collect::<Vec<_>>()
            .join(",")
    );
    let tmp = data_dir.join(format!("expect-{conn}.tmp"));
    fs::write(&tmp, body).unwrap();
    fs::File::open(&tmp).unwrap().sync_all().unwrap();
    fs::rename(&tmp, data_dir.join(format!("expect-{conn}.txt"))).unwrap();
}

/// The concurrent workload child: CONNS writers, disjoint id ranges,
/// insert/delete/abort iterations for ~4s. The engine stays in scope
/// until the kill (the `-> !` contract — a live-engine SIGKILL shape).
fn run_child(data_dir: &Path, seed: u64) -> ! {
    let (engine, oid) = setup(data_dir);
    let engine = std::sync::Arc::new(engine);
    fs::write(data_dir.join(ENGINE_READY_MARKER), b"").unwrap();
    let conns = conns();
    let deadline = Instant::now() + Duration::from_secs(4);
    let mut handles = Vec::with_capacity(conns);
    for conn in 0..conns {
        let engine = std::sync::Arc::clone(&engine);
        let dir = data_dir.to_path_buf();
        handles.push(thread::spawn(move || {
            let base = 1000 + conn as i32 * RANGE;
            let mut next_insert = base;
            let mut delete_cursor = base;
            let mut live = HashSet::new();
            let mut dead = HashSet::new();
            let mut iteration = 0u64;
            while Instant::now() < deadline {
                for _ in 0..2 {
                    let id = next_insert;
                    next_insert += 1;
                    let v = vec_for(id);
                    engine
                        .exec(
                            None,
                            &format!(
                                "INSERT INTO t VALUES ({id}, [{}, {}, {}])",
                                v[0], v[1], v[2]
                            ),
                        )
                        .unwrap();
                    live.insert(id);
                    write_expectation(&dir, conn, &live, &dead);
                }
                if iteration % 4 == 3 {
                    if delete_cursor + 1 < next_insert {
                        let id = delete_cursor;
                        delete_cursor += 1;
                        engine
                            .exec(None, &format!("DELETE FROM t WHERE id = {id}"))
                            .unwrap();
                        live.remove(&id);
                        dead.insert(id);
                        write_expectation(&dir, conn, &live, &dead);
                    }
                    // The abort-insert: never committed, never in the
                    // live set; the graph node is tombstoned by undo.
                    let id = next_insert;
                    next_insert += 1;
                    let txn = engine.begin_txn().unwrap();
                    let v = vec_for(id);
                    engine
                        .exec(
                            Some(&txn),
                            &format!(
                                "INSERT INTO t VALUES ({id}, [{}, {}, {}])",
                                v[0], v[1], v[2]
                            ),
                        )
                        .unwrap();
                    txn.abort().unwrap();
                    write_expectation(&dir, conn, &live, &dead);
                }
                iteration += 1;
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    let _ = oid;
    let _ = seed;
    fs::write(data_dir.join(READY_MARKER), b"").unwrap();
    loop {
        thread::sleep(Duration::from_secs(1));
    }
}

// ---------------------------------------------------------------------------
// Parent harness
// ---------------------------------------------------------------------------

fn spawn_child(data_dir: &Path, seed: u64) -> std::process::Child {
    let mut cmd = Command::new(std::env::current_exe().expect("test binary path"));
    cmd.arg("--test-threads=1")
        .arg(CHILD_TEST_NAME)
        .arg("--exact")
        .env(CHILD_ENV_VAR, "1")
        .env(DIR_ENV_VAR, data_dir.as_os_str())
        .env(SEED_ENV_VAR, seed.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    cmd.spawn().expect("failed to spawn crash child")
}

fn wait_for_file(path: &Path, timeout: Duration, what: &str, round: u64) {
    let start = Instant::now();
    while !path.exists() {
        assert!(
            start.elapsed() < timeout,
            "round {round}: child did not produce {what} in time"
        );
        thread::sleep(Duration::from_millis(2));
    }
}

/// RAII reaper for a crash-round child (the M5 review shape: kill/wait
/// failures panic, Drop still retries).
struct ChildGuard {
    child: Option<std::process::Child>,
}

impl ChildGuard {
    fn spawn(data_dir: &Path, seed: u64) -> Self {
        Self {
            child: Some(spawn_child(data_dir, seed)),
        }
    }

    fn kill_and_reap(&mut self, round: u64) {
        let child = self.child.as_mut().expect("child already reaped");
        child
            .kill()
            .unwrap_or_else(|e| panic!("round {round}: failed to kill crash child: {e}"));
        child
            .wait()
            .unwrap_or_else(|e| panic!("round {round}: failed to reap crash child: {e}"));
        self.child = None;
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

struct Expectation {
    live: HashSet<i32>,
    dead: HashSet<i32>,
}

fn read_expectation(data_dir: &Path, conn: usize) -> Expectation {
    let path = data_dir.join(format!("expect-{conn}.txt"));
    // A missing file means this connection had no completed expectation
    // write at the kill: either it committed nothing, or its FIRST op
    // committed but the rewrite had not landed yet — the latter is exactly
    // the one-unrecorded-op-per-conn case the mid-mode bound tolerates
    // (review nano: a slow first write must not panic the harness).
    let text = match fs::read_to_string(&path) {
        Ok(text) => text,
        Err(_) => {
            return Expectation {
                live: HashSet::new(),
                dead: HashSet::new(),
            };
        }
    };
    let mut live = HashSet::new();
    let mut dead = HashSet::new();
    for line in text.lines() {
        let (tag, ids) = line.split_once(' ').expect("malformed expectation line");
        let set: HashSet<i32> = if ids.is_empty() {
            HashSet::new()
        } else {
            ids.split(',').map(|s| s.parse().unwrap()).collect()
        };
        match tag {
            "LIVE" => live = set,
            "DEAD" => dead = set,
            other => panic!("unknown expectation tag {other}"),
        }
    }
    Expectation { live, dead }
}

#[test]
fn m6_crash_rounds() {
    if std::env::var(CHILD_ENV_VAR).is_ok() {
        return; // we are the child; the entry test does the work
    }
    let rounds: u64 = match std::env::var(ROUNDS_ENV_VAR) {
        Ok(v) => v
            .parse()
            .unwrap_or_else(|_| panic!("{ROUNDS_ENV_VAR} must be a positive integer, got {v:?}")),
        Err(_) => 25,
    };
    assert!(
        rounds >= 1,
        "{ROUNDS_ENV_VAR}=0 runs zero crash rounds — a vacuous green is worse than a loud failure"
    );

    for round in 0..rounds {
        let tmp = tempfile::TempDir::new().unwrap();
        let data_dir = tmp.path().to_path_buf();
        let mut child = ChildGuard::spawn(&data_dir, round);

        let special = round % 8 == 3;
        let full = round % 2 == 0;
        if special || full {
            wait_for_file(
                &data_dir.join(READY_MARKER),
                Duration::from_secs(120),
                "the ready-to-die marker",
                round,
            );
        } else {
            wait_for_file(
                &data_dir.join(ENGINE_READY_MARKER),
                Duration::from_secs(30),
                "the engine-ready marker",
                round,
            );
            // Kill at a seed-derived progress point (mid-workload).
            thread::sleep(Duration::from_millis(500 + (round % 5) * 400));
        }
        child.kill_and_reap(round);

        assert!(
            data_dir.join("lock").exists(),
            "round {round}: lock file missing after SIGKILL — the child's engine was already dropped (not a live-engine kill)"
        );
        let _ = std::fs::remove_file(data_dir.join("lock"));

        let reopen_started = Instant::now();
        let (engine, oid) = reopen(&data_dir);
        let recovery = reopen_started.elapsed();
        assert!(
            recovery < RECOVERY_BUDGET,
            "round {round}: recovery {recovery:?} exceeded the 30s budget"
        );

        if special {
            verify_p1_round(round, &engine, oid);
        } else {
            verify_round(round, &data_dir, &engine, oid, full);
        }
        engine.shutdown();
    }
}

/// The v1.4 P1 regression: the in-flight delete aborted, so the row is
/// visible again, the node is LIVE, and the map entry is present.
fn verify_p1_round(round: u64, engine: &Engine, oid: Oid) {
    let scanned = engine.scan("t", None).unwrap();
    assert_eq!(scanned.len(), 1, "round {round}: the row must be visible");
    let report = engine.hnsw_leak_report("t", None).unwrap();
    assert_eq!(report.node_count, 1);
    assert_eq!(
        report.tombstoned_count, 0,
        "round {round}: no graph tombstone"
    );
    assert!(engine.hnsw_map_get(oid, 0).unwrap().is_some());
    let meta = engine.hnsw_index_first_page(oid).unwrap().unwrap();
    let outcome = engine.open_hnsw_index(meta, &expected_params()).unwrap();
    outcome.index.audit(engine.storage().buffer_pool()).unwrap();
}

fn expected_params() -> pg_am_hnsw::ExpectedParams {
    pg_am_hnsw::ExpectedParams {
        dim: DIM,
        m: 16,
        m_max0: 32,
        ef_construction: 200,
        ef_search_default: 64,
        metric: Metric::L2,
        selection: NeighborSelection::Heuristic,
    }
}

fn scanned_ids(engine: &Engine) -> HashSet<i32> {
    engine
        .scan("t", None)
        .unwrap()
        .into_iter()
        .map(|(_, vals)| match &vals[0] {
            Some(pg_am_heap::Datum::Int4(i)) => *i,
            other => panic!("unexpected id datum: {other:?}"),
        })
        .collect()
}

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
        sum += truth.intersection(&hit_ids).count() as f64 / truth.len() as f64;
    }
    sum / query_ids.len() as f64
}

fn verify_round(round: u64, data_dir: &Path, engine: &Engine, oid: Oid, full: bool) {
    let scanned = scanned_ids(engine);
    let conns = conns();

    // Committed bookkeeping. Each connection rewrites its expectation
    // after EVERY committed op, so at most ONE op per connection can be
    // committed-but-unrecorded at the kill:
    // - recorded-deleted ∩ scan = ∅ (a committed delete is never visible);
    // - |recorded-live \ scan| ≤ conns (rows deleted by a committed but
    //   unrecorded delete — at most one per conn);
    // - |scan \ recorded-live| ≤ conns (rows inserted by a committed but
    //   unrecorded insert — at most one per conn).
    // Full mode completed the workload, so every op is recorded: exact.
    let mut expected_live = HashSet::new();
    for conn in 0..conns {
        let exp = read_expectation(data_dir, conn);
        for id in exp.live {
            expected_live.insert(id);
        }
        for id in exp.dead {
            assert!(
                !scanned.contains(&id),
                "round {round}: committed-deleted row {id} (conn {conn}) is visible after recovery"
            );
        }
    }
    if full {
        assert_eq!(scanned, expected_live, "round {round}: full-mode exact set");
    } else {
        let missing = expected_live.difference(&scanned).count();
        let unexpected = scanned.difference(&expected_live).count();
        assert!(
            missing <= conns,
            "round {round}: {missing} recorded-live rows missing after recovery — exceeds the one-unrecorded-op-per-conn bound ({conns})"
        );
        assert!(
            unexpected <= conns,
            "round {round}: {unexpected} rows appear after recovery without a committed record — exceeds the one-unrecorded-op-per-conn bound ({conns})"
        );
    }

    // §14.2 residue bound: each in-flight insert leaves at most one
    // residue of each kind — bounded by the writer concurrency.
    let report = engine.hnsw_leak_report("t", None).unwrap();
    for (name, count) in [
        ("initializing_count", report.initializing_count),
        ("orphan_entry_count", report.orphan_entry_count),
        ("hidden_high_level_count", {
            let meta = engine.hnsw_index_first_page(oid).unwrap().unwrap();
            let outcome = engine.open_hnsw_index(meta, &expected_params()).unwrap();
            let audit = outcome.index.audit(engine.storage().buffer_pool()).unwrap();
            audit.hidden_high_level_count
        }),
    ] {
        assert!(
            count <= conns as u64,
            "round {round}: {name} = {count} exceeds the crash-window × concurrency bound ({conns})"
        );
    }

    // Quiesce audit: structural green + no hanging LIVE node.
    let meta = engine.hnsw_index_first_page(oid).unwrap().unwrap();
    let reach =
        pg_am_hnsw::audit::audit_reachability(engine.storage().buffer_pool(), meta).unwrap();
    assert!(
        reach.unreachable_live.is_empty(),
        "round {round}: hanging LIVE nodes {:?}",
        reach.unreachable_live
    );
    let symmetry = pg_am_hnsw::audit::audit_symmetry(engine.storage().buffer_pool(), meta).unwrap();
    // The M4 band's UPPER edge is the quality guard; its lower edge is
    // not a defect signal (low asymmetry is good). After deletes, the
    // LIVE–LIVE subgraph can legitimately sit below the M4 band's 5%
    // lower edge (tombstone-involved edges leave the口径) — round 8
    // measured 4.9753% and is a legal shape, not a finding.
    assert!(
        symmetry.asymmetry_rate <= 0.30,
        "round {round}: asymmetry {:.4}% above the M4 properties band's upper edge",
        symmetry.asymmetry_rate * 100.0
    );

    // The dead-row channel converges every deferred map entry after
    // recovery (committed deletes whose cleanup was skipped/deferred).
    engine.vacuum("t").unwrap();
    let after = engine.hnsw_leak_report("t", None).unwrap();
    assert_eq!(
        after.orphan_map_entries, 0,
        "round {round}: heap vacuum must reclaim every deferred map entry"
    );

    // recall@10 after recovery (visible-row brute force, ef=128).
    let mut query_ids: Vec<i32> = scanned.iter().copied().collect();
    query_ids.sort_unstable();
    query_ids.truncate(20);
    let recall = recall_at_10(engine, &query_ids);
    assert!(
        recall >= 0.95,
        "round {round}: recall@10 after recovery {recall:.4} < 0.95"
    );

    // One exclusive rebuild after recovery, then re-audit + recall again.
    engine.hnsw_vacuum("t").unwrap();
    let reach2 =
        pg_am_hnsw::audit::audit_reachability(engine.storage().buffer_pool(), meta).unwrap();
    assert!(reach2.unreachable_live.is_empty());
    let recall2 = recall_at_10(engine, &query_ids);
    assert!(
        recall2 >= 0.95,
        "round {round}: recall@10 after rebuild {recall2:.4} < 0.95"
    );
    eprintln!(
        "round {round}: recovery ok (nodes={}, live={}, recall={recall:.3}/{recall2:.3}, asym={:.3}%)",
        report.node_count,
        scanned.len(),
        symmetry.asymmetry_rate * 100.0
    );
}
