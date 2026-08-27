//! M2a Stage K crash automation (coding-plan Stage K "崩溃自动化",
//! `test_m2a_crash_1000_rounds`): repeated random kill -9 + reopen cycles
//! against the assembled engine.
//!
//! Harness shape follows `pg-storage/tests/crash_recovery.rs`: the parent
//! spawns the test binary itself as a child process (`M2A_CRASH_CHILD=1`),
//! the child runs a deterministic pseudo-random DDL/DML workload (seeded by
//! the round number — no `rand` dependency) and is then SIGKILLed; the
//! parent reopens the data directory and validates consistency.
//!
//! # What is verified each round
//!
//! Every child op is auto-committed (each commit fsyncs), so the committed
//! state is always a prefix of the op stream. After every op the child
//! rewrites `{data_dir}/expectation.txt` atomically (tmp + rename) with the
//! full committed state — live tables with their visible rows, dropped
//! tables, and the terminal CLOG state of two back-door transactions (one
//! committed, one aborted, driven through `Engine::txn_manager` /
//! `Engine::heap`). The parent compares the reopened engine against the last
//! surviving expectation:
//!
//! - every expected table exists with exactly the expected visible rows
//!   (row count AND content);
//! - every dropped table is absent from the registry;
//! - `clog().get_state` reports the recorded terminal states (this exercises
//!   the disk CLOG's checkpoint-time flush whenever the workload included
//!   checkpoints — the M2a `clog-snapshot.bin` bridge is gone in M2b).
//!
//! # Kill timing
//!
//! Even rounds wait for the child's `ready-to-die` marker (full workload
//! committed, then killed). Odd rounds wait only for the child's
//! `engine-ready` marker (written the moment `Engine::open` returns) and are
//! then killed as soon as the expectation file shows a seed-derived number
//! of committed ops — a true mid-DDL / mid-workload crash at a PROGRESS
//! point rather than a wall-clock guess (wall-clock kills used to land
//! before the first expectation write, silently vacating the mid-workload
//! verifier). Kills are deliberately NOT delivered before `engine-ready`:
//! process startup + engine creation is pg-storage's create path, which
//! pg-storage's own crash harness also never kills inside (a kill inside
//! `Superblock::create`'s two-copy write can leave a data directory no
//! recovery can open — a known create-time window, out of M2a engine
//! scope). Both kill modes are verified against the same atomic expectation
//! file (always a committed prefix).
//!
//! # Rounds
//!
//! `M2A_CRASH_ROUNDS` controls the round count (default 25, CI-friendly).
//! The plan's literal 1000-round run:
//!
//! ```sh
//! M2A_CRASH_ROUNDS=1000 cargo test -p pg-engine --test m2a_crash_rounds -- --nocapture
//! ```

//! # Mid-workload verification semantics
//!
//! The expectation file and the engine's WAL are two durable stores that
//! cannot be updated atomically: a kill between an op's commit and the
//! expectation rename leaves the expectation one op STALE (lag <= 1 op; the
//! expectation is rewritten after every op). Exact equality would therefore
//! be wrong for mid-workload kills. The harness keeps verification exact for
//! even rounds (full workload committed before the kill) and, for odd
//! rounds, restricts the op stream to non-destructive ops (INSERT /
//! CHECKPOINT / CREATE — no UPDATE/DELETE/DROP) and verifies the
//! prefix-durability property instead: every expected row must be present
//! with exact content (no committed data lost), with at most one extra
//! row/table from the single in-flight op.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use pg_am_heap::access_method::{AccessMethod, InsertContext, RelationDesc};
use pg_am_heap::tuple::{encode_tuple, TupleHeader};
use pg_engine::{ColumnDef, ColumnType, Datum, Engine, EngineConfig, Tid};
use pg_storage::types::{PageId, TxnId};
use pg_txn::{ClogAccessor, Snapshot, TxnState};

const CHILD_ENV_VAR: &str = "M2A_CRASH_CHILD";
const DIR_ENV_VAR: &str = "M2A_CRASH_DIR";
const SEED_ENV_VAR: &str = "M2A_CRASH_SEED";
const ROUNDS_ENV_VAR: &str = "M2A_CRASH_ROUNDS";
const CHILD_TEST_NAME: &str = "m2a_crash_child_entry";

const READY_MARKER: &str = "ready-to-die";
/// Marker written by the child the moment `Engine::open` returns: kills are
/// only delivered after this point (see the module docs' kill-timing note).
const ENGINE_READY_MARKER: &str = "engine-ready";
const EXPECTATION_FILE: &str = "expectation.txt";
const EXPECTATION_TMP: &str = "expectation.tmp";

/// Base tables every child creates before the random op stream.
const BASE_TABLES: [&str; 3] = ["rt0", "rt1", "rt2"];

fn schema() -> Vec<ColumnDef> {
    vec![
        ColumnDef {
            name: "id".to_string(),
            col_type: ColumnType::Int4,
        },
        ColumnDef {
            name: "name".to_string(),
            col_type: ColumnType::Text,
        },
    ]
}

/// xorshift64* — a tiny deterministic PRNG so each round's op stream is
/// reproducible from its seed alone.
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

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

// ---------------------------------------------------------------------------
// Child process
// ---------------------------------------------------------------------------

/// Child entry point: runs the seeded workload, then sleeps until killed.
#[test]
fn m2a_crash_child_entry() {
    if std::env::var(CHILD_ENV_VAR).is_err() {
        return;
    }
    let data_dir = std::env::var(DIR_ENV_VAR).expect("data dir required");
    let seed: u64 = std::env::var(SEED_ENV_VAR)
        .expect("seed required")
        .parse()
        .expect("seed is u64");
    run_child(Path::new(&data_dir), seed);

    // Full workload committed: tell the parent it may kill us now.
    fs::write(Path::new(&data_dir).join(READY_MARKER), b"").unwrap();
    loop {
        thread::sleep(Duration::from_secs(1));
    }
}

/// The child's in-memory model of live tids, used only to pick update /
/// delete victims. The authoritative expected state is re-derived from
/// scans when the expectation file is written.
struct ChildModel {
    /// Live tids per table name.
    tids: BTreeMap<String, Vec<Tid>>,
    /// Tables dropped over the run (for the expectation file).
    dropped: Vec<String>,
    /// Extra (non-base) tables created so far, for unique naming.
    extra_seq: u64,
    /// Global row id sequence (unique across all tables).
    row_seq: i32,
}

fn run_child(data_dir: &Path, seed: u64) {
    let engine = Engine::open(data_dir, EngineConfig::new(data_dir)).unwrap();
    // From here on the data directory holds a complete engine; kills are
    // fair (module docs, kill timing).
    fs::write(data_dir.join(ENGINE_READY_MARKER), b"").unwrap();
    for table in BASE_TABLES {
        engine.create_table(table, &schema()).unwrap();
    }

    // Odd seeds model the parent's mid-workload kill: the op stream is
    // restricted to non-destructive ops so the parent's prefix-durability
    // check is well-defined (module docs).
    let destructive = seed % 2 == 0;
    let mut rng = Rng(seed | 0x9E37_79B9_7F4A_7C15);
    let mut model = ChildModel {
        tids: BASE_TABLES
            .iter()
            .map(|t| (t.to_string(), Vec::new()))
            .collect(),
        dropped: Vec::new(),
        extra_seq: 0,
        row_seq: 0,
    };

    let op_count = 40 + rng.below(40);
    for completed in 1..=op_count {
        do_random_op(&engine, &mut rng, &mut model, destructive);
        // After every op, persist the committed expectation atomically so a
        // mid-workload kill leaves a valid prefix expectation (no back-door
        // transactions have run yet at this point). The op counter lets the
        // parent kill at a seeded PROGRESS point rather than a wall-clock
        // guess (Stage K review P1-1: wall-clock kills used to land before
        // the first expectation write, making the mid-workload verifier
        // vacuous).
        write_expectation(
            &engine,
            data_dir,
            &model,
            &[],
            destructive,
            completed as usize,
        );
    }

    // Two back-door transactions to populate the CLOG with explicit
    // committed / aborted terminal states (recorded in the expectation).
    let back_door = drive_back_door_txns(&engine);
    write_expectation(
        &engine,
        data_dir,
        &model,
        &back_door,
        destructive,
        op_count as usize,
    );
}

fn pick_table<'a>(rng: &mut Rng, model: &'a ChildModel) -> &'a str {
    let idx = rng.below(model.tids.len() as u64) as usize;
    model.tids.keys().nth(idx).expect("non-empty map")
}

fn do_random_op(engine: &Engine, rng: &mut Rng, model: &mut ChildModel, destructive: bool) {
    match rng.below(12) {
        // INSERT (50% — plus the destructive-op slots when !destructive).
        0..=5 => {
            let table = pick_table(rng, model).to_string();
            let id = model.row_seq;
            model.row_seq += 1;
            let tid = engine
                .insert(
                    &table,
                    &[
                        Some(Datum::Int4(id)),
                        Some(Datum::Text(format!("row-{id}"))),
                    ],
                )
                .unwrap();
            model.tids.get_mut(&table).unwrap().push(tid);
        }
        // UPDATE (17%): replace a random live row's name. Non-destructive
        // streams substitute an insert.
        6..=7 if destructive => {
            let table = pick_table(rng, model).to_string();
            let tids = model.tids.get_mut(&table).unwrap();
            if tids.is_empty() {
                return;
            }
            let idx = rng.below(tids.len() as u64) as usize;
            let old_tid = tids[idx];
            let id = model.row_seq;
            model.row_seq += 1;
            let new_tid = engine
                .update(
                    &table,
                    old_tid,
                    &[
                        Some(Datum::Int4(id)),
                        Some(Datum::Text(format!("upd-{id}"))),
                    ],
                )
                .unwrap();
            tids[idx] = new_tid;
        }
        // DELETE (8%): remove a random live row.
        8 if destructive => {
            let table = pick_table(rng, model).to_string();
            let tids = model.tids.get_mut(&table).unwrap();
            if tids.is_empty() {
                return;
            }
            let idx = rng.below(tids.len() as u64) as usize;
            let tid = tids.swap_remove(idx);
            engine.delete(&table, tid).unwrap();
        }
        // CHECKPOINT (8%): advances the redo point, may recycle WAL — the
        // disk CLOG's checkpoint flush is what keeps pre-checkpoint terminal
        // states alive on the parent's reopen.
        9 => engine.checkpoint().unwrap(),
        // CREATE extra table (8%).
        10 => {
            let name = format!("extra{}", model.extra_seq);
            model.extra_seq += 1;
            engine.create_table(&name, &schema()).unwrap();
            model.tids.insert(name, Vec::new());
        }
        // DROP an extra table (8%).
        _ if destructive => {
            let extras: Vec<String> = model
                .tids
                .keys()
                .filter(|t| t.starts_with("extra"))
                .cloned()
                .collect();
            if extras.is_empty() {
                return;
            }
            let victim = extras[rng.below(extras.len() as u64) as usize].clone();
            engine.drop_table(&victim).unwrap();
            model.tids.remove(&victim);
            model.dropped.push(victim);
        }
        // Non-destructive substitute for UPDATE/DELETE/DROP slots: INSERT.
        _ => {
            let table = pick_table(rng, model).to_string();
            let id = model.row_seq;
            model.row_seq += 1;
            let tid = engine
                .insert(
                    &table,
                    &[
                        Some(Datum::Int4(id)),
                        Some(Datum::Text(format!("row-{id}"))),
                    ],
                )
                .unwrap();
            model.tids.get_mut(&table).unwrap().push(tid);
        }
    }
}

/// One committed and one aborted transaction through the engine's own
/// TxnManager + HeapAM (the back door lands in the same disk CLOG).
/// Returns `(xid, final state)` pairs for the expectation file.
fn drive_back_door_txns(engine: &Engine) -> Vec<(TxnId, TxnState)> {
    let entry = engine.describe_table(BASE_TABLES[0]).unwrap();
    let col_types = [ColumnType::Int4, ColumnType::Text];
    let rel = RelationDesc {
        rel_oid: entry.oid,
        first_page: entry.first_page,
        columns: &col_types,
    };

    let mut outcomes = Vec::new();
    for (id, commit) in [(9_000_001, true), (9_000_002, false)] {
        let xid = engine.txn_manager().begin_txn();
        let mut snap = Snapshot::everything();
        snap.set_current_xid(xid);
        let tuple = encode_tuple(
            TupleHeader::new(
                TxnId::INVALID,
                TxnId::INVALID,
                0,
                [0; 16],
                Tid {
                    page_id: PageId::INVALID,
                    slot_id: 0,
                },
                0,
            ),
            &col_types,
            &[
                Some(Datum::Int4(id)),
                Some(Datum::Text(format!("backdoor-{id}"))),
            ],
        )
        .unwrap();
        engine
            .heap()
            .insert(InsertContext {
                rel,
                snapshot: &snap,
                tuple: &tuple,
                out_tid: None,
            })
            .unwrap();
        let state = if commit {
            engine.txn_manager().commit_txn(xid).unwrap();
            TxnState::Committed
        } else {
            engine.txn_manager().abort_txn(xid).unwrap();
            TxnState::Aborted
        };
        outcomes.push((xid, state));
    }
    outcomes
}

/// Rewrite the expectation file atomically: live tables with their visible
/// rows (from authoritative scans), dropped tables, and the back-door XIDs
/// with their terminal states. `destructive` records the op-stream mode the
/// parent's verifier must apply (module docs).
fn write_expectation(
    engine: &Engine,
    data_dir: &Path,
    model: &ChildModel,
    back_door: &[(TxnId, TxnState)],
    destructive: bool,
    completed_ops: usize,
) {
    let mut out = String::new();
    out.push_str(if destructive {
        "MODE full\n"
    } else {
        "MODE mid\n"
    });
    out.push_str(&format!("OPS {completed_ops}\n"));
    for table in model.tids.keys() {
        out.push_str(&format!("TABLE {table}\n"));
        let mut rows: Vec<(i32, String)> = engine
            .scan(table, None)
            .unwrap()
            .into_iter()
            .map(|(_, vals)| match (&vals[0], &vals[1]) {
                (Some(Datum::Int4(id)), Some(Datum::Text(name))) => (*id, name.clone()),
                other => panic!("unexpected row shape: {other:?}"),
            })
            .collect();
        rows.sort_unstable();
        for (id, name) in rows {
            out.push_str(&format!("ROW {table} {id} {name}\n"));
        }
    }
    for name in &model.dropped {
        out.push_str(&format!("DROPPED {name}\n"));
    }
    for (xid, state) in back_door {
        let state = match state {
            TxnState::Committed => "committed",
            TxnState::Aborted => "aborted",
            other => panic!("back-door txn ended in non-terminal state {other:?}"),
        };
        out.push_str(&format!("XID {} {state}\n", xid.0));
    }

    let tmp = data_dir.join(EXPECTATION_TMP);
    fs::write(&tmp, &out).unwrap();
    fs::File::open(&tmp).unwrap().sync_all().unwrap();
    fs::rename(&tmp, data_dir.join(EXPECTATION_FILE)).unwrap();
}

// ---------------------------------------------------------------------------
// Parent harness
// ---------------------------------------------------------------------------

struct Expectation {
    /// table name -> sorted (id, name) rows.
    tables: BTreeMap<String, Vec<(i32, String)>>,
    dropped: Vec<String>,
    /// XID -> expected terminal state.
    xids: BTreeMap<u64, TxnState>,
    /// True when the child's op stream was non-destructive (mid-workload
    /// kill mode): the verifier applies prefix-durability semantics.
    mid: bool,
    /// Number of workload ops committed when this expectation was written.
    ops: usize,
}

fn parse_expectation(text: &str) -> Expectation {
    let mut tables: BTreeMap<String, Vec<(i32, String)>> = BTreeMap::new();
    let mut dropped = Vec::new();
    let mut xids = BTreeMap::new();
    let mut mid = None;
    let mut ops = 0;
    for line in text.lines() {
        let parts: Vec<&str> = line.splitn(4, ' ').collect();
        match parts.as_slice() {
            ["MODE", mode] => {
                mid = Some(match *mode {
                    "full" => false,
                    "mid" => true,
                    other => panic!("unknown expectation mode {other}"),
                });
            }
            ["OPS", n] => {
                ops = n.parse().expect("OPS line must carry a number");
            }
            ["TABLE", name] => {
                tables.entry(name.to_string()).or_default();
            }
            ["ROW", table, id, name] => {
                tables
                    .entry(table.to_string())
                    .or_default()
                    .push((id.parse().unwrap(), name.to_string()));
            }
            ["DROPPED", name] => dropped.push(name.to_string()),
            ["XID", id, state] => {
                let state = match *state {
                    "committed" => TxnState::Committed,
                    "aborted" => TxnState::Aborted,
                    other => panic!("unknown expectation state {other}"),
                };
                xids.insert(id.parse().unwrap(), state);
            }
            other => panic!("malformed expectation line: {other:?}"),
        }
    }
    Expectation {
        tables,
        dropped,
        xids,
        mid: mid.expect("expectation must start with a MODE line"),
        ops,
    }
}

fn spawn_child(data_dir: &Path, seed: u64) -> std::process::Child {
    let mut cmd = Command::new(std::env::current_exe().expect("test binary path"));
    cmd.arg("--test-threads=1")
        .arg(CHILD_TEST_NAME)
        .env(CHILD_ENV_VAR, "1")
        .env(DIR_ENV_VAR, data_dir.as_os_str())
        .env(SEED_ENV_VAR, seed.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    cmd.spawn().expect("failed to spawn crash child")
}

fn wait_for_marker(data_dir: &Path, timeout: Duration) -> bool {
    let marker = data_dir.join(READY_MARKER);
    let start = Instant::now();
    while start.elapsed() < timeout {
        if marker.exists() {
            return true;
        }
        thread::sleep(Duration::from_millis(2));
    }
    false
}

/// The M2a crash-automation acceptance: `M2A_CRASH_ROUNDS` random kill -9 +
/// reopen cycles (default 25; 1000 for the plan's literal acceptance).
#[test]
fn m2a_crash_rounds() {
    if std::env::var(CHILD_ENV_VAR).is_ok() {
        return; // we are the child; the entry test does the work
    }
    let rounds: u64 = std::env::var(ROUNDS_ENV_VAR)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(25);

    for round in 0..rounds {
        let tmp = tempfile::TempDir::new().unwrap();
        let data_dir = tmp.path().to_path_buf();
        let mut child = spawn_child(&data_dir, round);

        if round % 2 == 0 {
            // Even rounds: let the full workload commit, then kill.
            assert!(
                wait_for_marker(&data_dir, Duration::from_secs(120)),
                "round {round}: child did not finish its workload in time"
            );
            thread::sleep(Duration::from_millis(round % 20));
        } else {
            // Odd rounds: kill at a seed-derived PROGRESS point — as soon as
            // the expectation file shows `target_ops` committed ops — instead
            // of a wall-clock guess. (Stage K review P1-1: wall-clock kills
            // landed before the first expectation write, so the mid-workload
            // verifier silently compared nothing.) The engine-ready marker
            // still gates the kill out of the create-path window.
            let engine_ready = data_dir.join(ENGINE_READY_MARKER);
            let start = Instant::now();
            while !engine_ready.exists() {
                assert!(
                    start.elapsed() < Duration::from_secs(30),
                    "round {round}: child engine did not open in time"
                );
                thread::sleep(Duration::from_millis(2));
            }
            let target_ops = 1 + (round % 20) as usize;
            let start = Instant::now();
            loop {
                let reached = data_dir.join(EXPECTATION_FILE).exists()
                    && parse_expectation(
                        &fs::read_to_string(data_dir.join(EXPECTATION_FILE)).unwrap(),
                    )
                    .ops >= target_ops;
                if reached {
                    break;
                }
                assert!(
                    start.elapsed() < Duration::from_secs(120),
                    "round {round}: child did not reach op {target_ops} in time"
                );
                thread::sleep(Duration::from_millis(2));
            }
        }
        child.kill().expect("failed to kill crash child");
        child.wait().expect("failed to reap crash child");

        // M3 Stage E F1: the SIGKILLed child left its `{data_dir}/lock`
        // file behind; the harness plays the documented operator action
        // for crash residue (remove the stale lock) before reopening.
        let _ = std::fs::remove_file(data_dir.join("lock"));

        verify_round(round, &data_dir);
    }
}

fn verify_round(round: u64, data_dir: &Path) {
    let engine = Engine::open(data_dir, EngineConfig::new(data_dir))
        .unwrap_or_else(|e| panic!("round {round}: engine failed to reopen: {e}"));

    // A kill before the child's first expectation write leaves nothing to
    // compare; recovery succeeding is the assertion for that window.
    let expectation_path = data_dir.join(EXPECTATION_FILE);
    if !expectation_path.exists() {
        engine.shutdown();
        return;
    }
    let expectation = parse_expectation(&fs::read_to_string(&expectation_path).unwrap());

    if expectation.mid {
        // Prefix-durability semantics (module docs): the expectation may lag
        // the recovered state by at most one op. Every expected row must be
        // present with exact content (no committed data lost); at most one
        // extra row overall (the single in-flight op, if it was an insert).
        let mut extras = 0usize;
        for (table, expected_rows) in &expectation.tables {
            let mut rows: Vec<(i32, String)> = engine
                .scan(table, None)
                .unwrap_or_else(|e| panic!("round {round}: scan of {table} failed: {e}"))
                .into_iter()
                .map(|(_, vals)| match (&vals[0], &vals[1]) {
                    (Some(Datum::Int4(id)), Some(Datum::Text(name))) => (*id, name.clone()),
                    other => panic!("round {round}: unexpected row shape: {other:?}"),
                })
                .collect();
            for expected in expected_rows {
                let pos = rows.iter().position(|r| r == expected).unwrap_or_else(|| {
                    panic!("round {round}: committed row {expected:?} of {table} lost across crash recovery")
                });
                rows.swap_remove(pos);
            }
            extras += rows.len();
        }
        assert!(
            extras <= 1,
            "round {round}: recovered state is more than one op ahead of the expectation ({extras} extra rows)"
        );
        assert!(
            expectation.dropped.is_empty(),
            "round {round}: mid-workload streams never drop tables"
        );
    } else {
        // Full-workload semantics: the expectation is the exact committed
        // state (the kill happened after the workload marker).
        for (table, expected_rows) in &expectation.tables {
            let mut rows: Vec<(i32, String)> = engine
                .scan(table, None)
                .unwrap_or_else(|e| panic!("round {round}: scan of {table} failed: {e}"))
                .into_iter()
                .map(|(_, vals)| match (&vals[0], &vals[1]) {
                    (Some(Datum::Int4(id)), Some(Datum::Text(name))) => (*id, name.clone()),
                    other => panic!("round {round}: unexpected row shape: {other:?}"),
                })
                .collect();
            rows.sort_unstable();
            assert_eq!(
                &rows, expected_rows,
                "round {round}: table {table} content diverged across crash recovery"
            );
        }
        for name in &expectation.dropped {
            assert!(
                engine.describe_table(name).is_none(),
                "round {round}: dropped table {name} resurrected after recovery"
            );
        }
    }
    for (xid, state) in &expectation.xids {
        // The terminal state must survive the crash — including the case
        // where a workload checkpoint truncated the WAL prefix holding the
        // original commit/abort record (the disk CLOG's checkpoint flush
        // path). A lost entry reads InProgress, so this catches both
        // directions.
        assert_eq!(
            engine.clog().get_state(TxnId(*xid)),
            *state,
            "round {round}: CLOG state of back-door xid {xid} lost across crash recovery"
        );
    }
    engine.shutdown();
}
