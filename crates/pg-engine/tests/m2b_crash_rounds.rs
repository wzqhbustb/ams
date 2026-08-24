//! M2b Stage O crash automation: repeated random kill -9 + reopen cycles
//! driven by the SQL `exec` API.
//!
//! Harness shape follows `m2a_crash_rounds.rs`: the parent spawns the test
//! binary itself as a child (`M2B_CRASH_CHILD=1`), the child runs a
//! deterministic pseudo-random DDL/DML workload (seeded by the round number)
//! through `Engine::exec` and is then SIGKILLed; the parent reopens the data
//! directory and validates consistency against an atomic expectation file.
//!
//! # What is verified each round
//!
//! Every child op is auto-committed (each commit fsyncs), so the committed
//! state is always a prefix of the op stream. After every op the child
//! rewrites `{data_dir}/expectation.txt` atomically (tmp + rename) with the
//! full committed state read back through `SELECT`. The parent compares the
//! reopened engine against the last surviving expectation:
//!
//! - every expected table exists with exactly the expected visible rows
//!   (row count AND content);
//! - for mid-workload kills (odd rounds): prefix-durability — every expected
//!   row present with exact content, at most one extra row from the single
//!   in-flight op.
//! - the B+Tree on `ixt(name)` validates and every committed `ixt` row is
//!   reachable through an index lookup, so the index agrees with the heap.
//!
//! # Stage S coverage
//!
//! The workload deliberately keeps a B+Tree split in flight when the kill
//! lands: `ixt(name)` carries ~500-byte keys, so only ~15 entries fit in a
//! leaf and the ~60-80 indexed inserts a round performs drive several leaf
//! splits plus a root split. Recovery must therefore run the undo pass and
//! finish those splits through a CLR. The same stream also carries HOT updates
//! (only the unindexed `id` changes), non-HOT updates (the indexed `name`
//! changes, forcing index maintenance) and `FOR SHARE` shared-lock stamping.
//!
//! # Kill timing
//!
//! Even rounds wait for the child's `ready-to-die` marker (full workload
//! committed, then killed). Odd rounds wait only for the child's
//! `engine-ready` marker and are then killed as soon as the expectation file
//! shows a seed-derived number of committed ops — a true mid-workload crash at
//! a PROGRESS point. Kills are never delivered before `engine-ready`.
//!
//! # Rounds
//!
//! `M2B_CRASH_ROUNDS` controls the round count. The default of 25 is the CI
//! configuration; the Stage O acceptance configuration is 1000 rounds, run
//! manually (~30-60 min):
//!
//! ```sh
//! M2B_CRASH_ROUNDS=1000 cargo test -p pg-engine --test m2b_crash_rounds -- --nocapture
//! ```
//!
//! # Stage T upgrade: concurrent-write rounds
//!
//! `m2b_crash_rounds_concurrent` (below) is the Stage T variant: the child
//! runs `M2B_CRASH_CONC_THREADS` (default 4) writer threads CONCURRENTLY —
//! each owning a heap-only table `ct{t}` and an indexed table `cix{t}`
//! whose wide keys keep B+Tree splits in flight. The op mix per thread
//! covers heap inserts, indexed inserts (splits), HOT updates, non-HOT
//! updates (index maintenance) and deletes, all auto-committed; every
//! thread rewrites its own atomic expectation file `expectation-{t}.txt`
//! after each committed op. The parent kills SIGKILL either at a
//! seed-derived PROGRESS point (rounds 1,2,3 mod 4 — a kill
//! mid-concurrent-write) or after the full workload (round 0 mod 4).
//!
//! A checkpoint thread fires checkpoints every 150ms under the write load,
//! so a kill can land between CheckpointBegin/End with splits in flight.
//! It is ON by default (the Stage T checkpoint/FPI P0 — a split Commit's
//! cycle FPI landing after the Commit record — is fixed in pg-am-btree;
//! see `run_conc_child`). Setting `M2B_CRASH_CONC_CKPT=1` switches to an
//! aggressive 20ms interval for extra stress.
//!
//! Verification per thread file:
//!
//! - full mode: exact match, as in the single-threaded harness;
//! - mid mode: the recovered state may diverge from the expectation by AT
//!   MOST ONE in-flight op per thread — i.e. ≤1 missing row AND ≤1 extra
//!   row (a committed-but-unrecorded insert shows as 1 extra, a delete as
//!   1 missing, an update as 1+1);
//! - every `cix{t}` index validates, and every row present in BOTH the
//!   expectation and the recovered scan is reachable through its index.
//!
//! CI default is `M2B_CRASH_CONC_ROUNDS=4` (one full-completion round +
//! three mid-write kills). The Stage T acceptance configuration — 1000
//! rounds including mid-concurrent-write kills — is a manual run
//! (~30-60 min):
//!
//! ```sh
//! M2B_CRASH_CONC_ROUNDS=1000 cargo test -p pg-engine --test m2b_crash_rounds \
//!   m2b_crash_rounds_concurrent -- --nocapture
//! ```

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use pg_engine::{Datum, Engine, EngineConfig, QueryResult};

const CHILD_ENV_VAR: &str = "M2B_CRASH_CHILD";
const DIR_ENV_VAR: &str = "M2B_CRASH_DIR";
const SEED_ENV_VAR: &str = "M2B_CRASH_SEED";
const ROUNDS_ENV_VAR: &str = "M2B_CRASH_ROUNDS";
const CHILD_TEST_NAME: &str = "m2b_crash_child_entry";

const READY_MARKER: &str = "ready-to-die";
const ENGINE_READY_MARKER: &str = "engine-ready";
const EXPECTATION_FILE: &str = "expectation.txt";
const EXPECTATION_TMP: &str = "expectation.tmp";

const BASE_TABLES: [&str; 3] = ["rt0", "rt1", "rt2"];

/// Table carrying a B+Tree index on its `name` column. Its keys are padded
/// wide on purpose: at ~500 bytes only ~15 entries fit in a leaf page, so the
/// few dozen inserts a round performs drive several leaf splits and a root
/// split. That is what puts a split in flight when the kill lands.
const IX_TABLE: &str = "ixt";
const IX_KEY_PAD: usize = 500;

/// A unique, space-free, order-preserving index key of `IX_KEY_PAD` bytes.
fn ix_key(seq: i32) -> String {
    format!("k{seq:08}{}", "x".repeat(IX_KEY_PAD))
}

/// xorshift64* — deterministic PRNG so each round's op stream is reproducible.
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
fn m2b_crash_child_entry() {
    if std::env::var(CHILD_ENV_VAR).is_err() {
        return;
    }
    let data_dir = std::env::var(DIR_ENV_VAR).expect("data dir required");
    let seed: u64 = std::env::var(SEED_ENV_VAR)
        .expect("seed required")
        .parse()
        .expect("seed is u64");
    run_child(Path::new(&data_dir), seed);

    fs::write(Path::new(&data_dir).join(READY_MARKER), b"").unwrap();
    loop {
        thread::sleep(Duration::from_secs(1));
    }
}

/// In-memory model of live row ids per table — the authoritative state is
/// re-derived from SELECT scans when the expectation file is written.
struct ChildModel {
    tables: BTreeMap<String, Vec<i32>>,
    extra_seq: u64,
    row_seq: i32,
}

fn run_child(data_dir: &Path, seed: u64) {
    let engine = Engine::open(data_dir, EngineConfig::new(data_dir)).unwrap();
    fs::write(data_dir.join(ENGINE_READY_MARKER), b"").unwrap();

    for table in BASE_TABLES {
        engine
            .exec(None, &format!("CREATE TABLE {table} (id INT, name TEXT)"))
            .unwrap();
    }
    engine
        .exec(
            None,
            &format!("CREATE TABLE {IX_TABLE} (id INT, name TEXT)"),
        )
        .unwrap();
    engine
        .exec(None, &format!("CREATE INDEX ON {IX_TABLE} (name)"))
        .unwrap();

    let destructive = seed % 2 == 0;
    let mut rng = Rng(seed | 0x9E37_79B9_7F4A_7C15);
    let mut model = ChildModel {
        tables: BASE_TABLES
            .iter()
            .map(|t| (t.to_string(), Vec::new()))
            .chain(std::iter::once((IX_TABLE.to_string(), Vec::new())))
            .collect(),
        extra_seq: 0,
        row_seq: 0,
    };

    // Enough ops that the indexed table reaches several leaf splits and a
    // root split before the workload ends.
    let op_count = 160 + rng.below(80);
    for completed in 1..=op_count {
        do_random_op(&engine, &mut rng, &mut model, destructive);
        write_expectation(&engine, data_dir, &model, destructive, completed as usize);
    }
}

/// Pick a heap-only table. The indexed table is excluded so the split pressure
/// on its B+Tree comes only from the dedicated indexed ops below.
fn pick_table<'a>(rng: &mut Rng, model: &'a ChildModel) -> &'a str {
    let names: Vec<&str> = model
        .tables
        .keys()
        .map(String::as_str)
        .filter(|t| *t != IX_TABLE)
        .collect();
    let idx = rng.below(names.len() as u64) as usize;
    names[idx]
}

fn insert_base_row(engine: &Engine, rng: &mut Rng, model: &mut ChildModel) {
    let table = pick_table(rng, model).to_string();
    let id = model.row_seq;
    model.row_seq += 1;
    engine
        .exec(
            None,
            &format!("INSERT INTO {table} VALUES ({id}, 'row-{id}')"),
        )
        .unwrap();
    model.tables.get_mut(&table).unwrap().push(id);
}

/// Every arm adds at most one row, which is what lets the parent's mid-workload
/// verifier bound the in-flight op at a single extra row.
fn do_random_op(engine: &Engine, rng: &mut Rng, model: &mut ChildModel, destructive: bool) {
    match rng.below(20) {
        // Heap INSERT (25%).
        0..=4 => insert_base_row(engine, rng, model),
        // Indexed INSERT (35%): the wide key means ~15 entries per leaf, so
        // this is the op that drives leaf and root splits.
        5..=11 => {
            let id = model.row_seq;
            model.row_seq += 1;
            let key = ix_key(id);
            engine
                .exec(
                    None,
                    &format!("INSERT INTO {IX_TABLE} VALUES ({id}, '{key}')"),
                )
                .unwrap();
            model.tables.get_mut(IX_TABLE).unwrap().push(id);
        }
        // Heap UPDATE (10%): replace a random live row's id and name.
        12..=13 if destructive => {
            let table = pick_table(rng, model).to_string();
            let ids = model.tables.get_mut(&table).unwrap();
            if ids.is_empty() {
                return;
            }
            let idx = rng.below(ids.len() as u64) as usize;
            let old_id = ids[idx];
            let new_id = model.row_seq;
            model.row_seq += 1;
            engine
                .exec(
                    None,
                    &format!(
                        "UPDATE {table} SET id = {new_id}, name = 'upd-{new_id}' WHERE id = {old_id}"
                    ),
                )
                .unwrap();
            ids[idx] = new_id;
        }
        // Heap DELETE (5%): remove a random live row.
        14 if destructive => {
            let table = pick_table(rng, model).to_string();
            let ids = model.tables.get_mut(&table).unwrap();
            if ids.is_empty() {
                return;
            }
            let idx = rng.below(ids.len() as u64) as usize;
            let id = ids.swap_remove(idx);
            engine
                .exec(None, &format!("DELETE FROM {table} WHERE id = {id}"))
                .unwrap();
        }
        // HOT UPDATE (5%): only the unindexed `id` changes, so the new version
        // stays on the page and no index entry is added.
        15 if destructive => {
            let ids = model.tables.get_mut(IX_TABLE).unwrap();
            if ids.is_empty() {
                return;
            }
            let idx = rng.below(ids.len() as u64) as usize;
            let old_id = ids[idx];
            let new_id = model.row_seq;
            model.row_seq += 1;
            engine
                .exec(
                    None,
                    &format!("UPDATE {IX_TABLE} SET id = {new_id} WHERE id = {old_id}"),
                )
                .unwrap();
            ids[idx] = new_id;
        }
        // Non-HOT UPDATE (5%): the indexed `name` changes, forcing an index
        // insert (and possibly a split) alongside the heap update.
        16 if destructive => {
            let ids = model.tables.get(IX_TABLE).unwrap();
            if ids.is_empty() {
                return;
            }
            let idx = rng.below(ids.len() as u64) as usize;
            let id = ids[idx];
            let key_seq = model.row_seq;
            model.row_seq += 1;
            let key = ix_key(key_seq);
            engine
                .exec(
                    None,
                    &format!("UPDATE {IX_TABLE} SET name = '{key}' WHERE id = {id}"),
                )
                .unwrap();
        }
        // FOR SHARE (5%): stamps a shared lock in xmax, safe in both modes
        // because it changes no visible column.
        17 => {
            let ids = model.tables.get(IX_TABLE).unwrap();
            if ids.is_empty() {
                return;
            }
            let idx = rng.below(ids.len() as u64) as usize;
            let id = ids[idx];
            engine
                .exec(
                    None,
                    &format!("SELECT id FROM {IX_TABLE} WHERE id = {id} FOR SHARE"),
                )
                .unwrap();
        }
        // CHECKPOINT (5%).
        18 => engine.checkpoint().unwrap(),
        // CREATE extra table (5%).
        19 => {
            let name = format!("extra{}", model.extra_seq);
            model.extra_seq += 1;
            engine
                .exec(None, &format!("CREATE TABLE {name} (id INT, name TEXT)"))
                .unwrap();
            model.tables.insert(name, Vec::new());
        }
        // Non-destructive substitute for the UPDATE/DELETE slots: INSERT.
        _ => insert_base_row(engine, rng, model),
    }
}

/// Rewrite the expectation file atomically: live tables with their visible
/// rows (from authoritative SELECT scans), and the op-stream mode the
/// parent's verifier must apply.
fn write_expectation(
    engine: &Engine,
    data_dir: &Path,
    model: &ChildModel,
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
    for table in model.tables.keys() {
        out.push_str(&format!("TABLE {table}\n"));
        let res = engine
            .exec(None, &format!("SELECT * FROM {table}"))
            .unwrap();
        let mut rows: Vec<(i32, String)> = match res {
            QueryResult::Rows { rows, .. } => rows
                .iter()
                .map(|row| match (&row[0], &row[1]) {
                    (Some(Datum::Int4(id)), Some(Datum::Text(name))) => (*id, name.clone()),
                    other => panic!("unexpected row shape: {other:?}"),
                })
                .collect(),
            other => panic!("expected Rows, got {other:?}"),
        };
        rows.sort_unstable();
        for (id, name) in rows {
            out.push_str(&format!("ROW {table} {id} {name}\n"));
        }
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
    tables: BTreeMap<String, Vec<(i32, String)>>,
    mid: bool,
    ops: usize,
}

fn parse_expectation(text: &str) -> Expectation {
    let mut tables: BTreeMap<String, Vec<(i32, String)>> = BTreeMap::new();
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
            other => panic!("malformed expectation line: {other:?}"),
        }
    }
    Expectation {
        tables,
        mid: mid.expect("expectation must start with a MODE line"),
        ops,
    }
}

fn spawn_child_at(
    data_dir: &Path,
    seed: u64,
    env_var: &str,
    test_name: &str,
) -> std::process::Child {
    let mut cmd = Command::new(std::env::current_exe().expect("test binary path"));
    cmd.arg("--test-threads=1")
        .arg(test_name)
        .env(env_var, "1")
        .env(DIR_ENV_VAR, data_dir.as_os_str())
        .env(SEED_ENV_VAR, seed.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    cmd.spawn().expect("failed to spawn crash child")
}

fn spawn_child(data_dir: &Path, seed: u64) -> std::process::Child {
    spawn_child_at(data_dir, seed, CHILD_ENV_VAR, CHILD_TEST_NAME)
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

/// Read all visible rows of `table` via `SELECT * FROM {table}` as sorted
/// (id, name) pairs.
fn scan_table(engine: &Engine, table: &str) -> Vec<(i32, String)> {
    let res = engine
        .exec(None, &format!("SELECT * FROM {table}"))
        .unwrap_or_else(|e| panic!("scan of {table} failed: {e}"));
    let mut rows: Vec<(i32, String)> = match res {
        QueryResult::Rows { rows, .. } => rows
            .iter()
            .map(|row| match (&row[0], &row[1]) {
                (Some(Datum::Int4(id)), Some(Datum::Text(name))) => (*id, name.clone()),
                other => panic!("unexpected row shape: {other:?}"),
            })
            .collect(),
        other => panic!("expected Rows, got {other:?}"),
    };
    rows.sort_unstable();
    rows
}

/// The M2b crash-automation acceptance: `M2B_CRASH_ROUNDS` random kill -9 +
/// reopen cycles (default 25; 1000 for the plan's literal acceptance).
#[test]
fn m2b_crash_rounds() {
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
            // the expectation file shows `target_ops` committed ops.
            let engine_ready = data_dir.join(ENGINE_READY_MARKER);
            let start = Instant::now();
            while !engine_ready.exists() {
                assert!(
                    start.elapsed() < Duration::from_secs(30),
                    "round {round}: child engine did not open in time"
                );
                thread::sleep(Duration::from_millis(2));
            }
            // Deep enough that the indexed table has already split at least
            // once, so the kill can land inside a split protocol.
            let target_ops = 40 + (round % 60) as usize;
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

        verify_round(round, &data_dir);
    }
}

fn verify_round(round: u64, data_dir: &Path) {
    let engine = Engine::open(data_dir, EngineConfig::new(data_dir))
        .unwrap_or_else(|e| panic!("round {round}: engine failed to reopen: {e}"));

    let expectation_path = data_dir.join(EXPECTATION_FILE);
    assert!(
        expectation_path.exists(),
        "round {round}: expectation file missing — the child never committed its first op (silent pass is worse than a loud failure)"
    );
    let expectation = parse_expectation(&fs::read_to_string(&expectation_path).unwrap());

    if expectation.mid {
        // Prefix-durability: every expected row present with exact content;
        // at most one extra row overall (the single in-flight op).
        let mut extras = 0usize;
        for (table, expected_rows) in &expectation.tables {
            let mut rows = scan_table(&engine, table);
            for expected in expected_rows {
                let pos = rows.iter().position(|r| r == expected).unwrap_or_else(|| {
                    panic!(
                        "round {round}: committed row {expected:?} of {table} lost across crash recovery"
                    )
                });
                rows.swap_remove(pos);
            }
            extras += rows.len();
        }
        assert!(
            extras <= 1,
            "round {round}: recovered state is more than one op ahead of the expectation ({extras} extra rows)"
        );
    } else {
        // Full-workload: exact match.
        for (table, expected_rows) in &expectation.tables {
            let rows = scan_table(&engine, table);
            assert_eq!(
                &rows, expected_rows,
                "round {round}: table {table} content diverged across crash recovery"
            );
        }
    }
    verify_index(round, &engine, &expectation);
    engine.shutdown();
}

/// The index must agree with the heap after recovery. `validate` walks the
/// whole tree and cross-checks the leaf chain against the root-reachable
/// leaves, which is exactly how a split left unfinished by undo shows up: its
/// right sibling is in the chain but has no downlink.
fn verify_index(round: u64, engine: &Engine, expectation: &Expectation) {
    let index = engine
        .btree_index(IX_TABLE, "name")
        .unwrap_or_else(|e| panic!("round {round}: {IX_TABLE} index failed to open: {e}"));
    index.validate().unwrap_or_else(|e| {
        panic!("round {round}: {IX_TABLE} index is corrupt after crash recovery: {e}")
    });

    let ix_rows = expectation.tables.get(IX_TABLE).map_or(0, Vec::len);
    // ~15 wide keys per leaf: past 30 rows the tree cannot still be a single
    // root leaf. Asserting it keeps a round from passing vacuously with no
    // split records in the stream that was just recovered.
    if ix_rows > 30 {
        assert!(
            index.tree_level() >= 1,
            "round {round}: {ix_rows} rows in {IX_TABLE} but its index never split — \
             this round exercised no split recovery"
        );
    }

    for (id, name) in expectation.tables.get(IX_TABLE).into_iter().flatten() {
        let found = engine
            .index_lookup(IX_TABLE, "name", &Datum::Text(name.clone()))
            .unwrap_or_else(|e| panic!("round {round}: index lookup for row {id} failed: {e}"));
        assert!(
            found.is_some(),
            "round {round}: committed row {id} of {IX_TABLE} is not reachable through its index"
        );
    }
}

// ---------------------------------------------------------------------------
// Stage T: concurrent-write crash rounds
// ---------------------------------------------------------------------------

const CHILD_CONC_ENV_VAR: &str = "M2B_CRASH_CHILD_CONC";
const CONC_ROUNDS_ENV_VAR: &str = "M2B_CRASH_CONC_ROUNDS";
const CONC_THREADS_ENV_VAR: &str = "M2B_CRASH_CONC_THREADS";
const CONC_OPS_ENV_VAR: &str = "M2B_CRASH_CONC_OPS";
const CONC_CHILD_TEST_NAME: &str = "m2b_crash_child_concurrent_entry";

/// Per-thread expectation files: `expectation-{t}.txt` / `expectation-{t}.tmp`.
fn conc_expectation_file(t: usize) -> String {
    format!("expectation-{t}.txt")
}

fn conc_expectation_tmp(t: usize) -> String {
    format!("expectation-{t}.tmp")
}

fn conc_heap_table(t: usize) -> String {
    format!("ct{t}")
}

fn conc_index_table(t: usize) -> String {
    format!("cix{t}")
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Concurrent child entry point: multi-threaded writers + a checkpoint
/// thread, then sleep until killed.
#[test]
fn m2b_crash_child_concurrent_entry() {
    if std::env::var(CHILD_CONC_ENV_VAR).is_err() {
        return;
    }
    let data_dir = std::env::var(DIR_ENV_VAR).expect("data dir required");
    let seed: u64 = std::env::var(SEED_ENV_VAR)
        .expect("seed required")
        .parse()
        .expect("seed is u64");
    run_conc_child(Path::new(&data_dir), seed);

    fs::write(Path::new(&data_dir).join(READY_MARKER), b"").unwrap();
    loop {
        thread::sleep(Duration::from_secs(1));
    }
}

fn run_conc_child(data_dir: &Path, seed: u64) {
    let threads = env_usize(CONC_THREADS_ENV_VAR, 4);
    let ops = env_usize(CONC_OPS_ENV_VAR, 60);

    let engine = Arc::new(Engine::open(data_dir, EngineConfig::new(data_dir)).unwrap());
    fs::write(data_dir.join(ENGINE_READY_MARKER), b"").unwrap();

    // One heap-only and one indexed (wide-key, split-heavy) table per writer.
    for t in 0..threads {
        engine
            .exec(
                None,
                &format!("CREATE TABLE {} (id INT, name TEXT)", conc_heap_table(t)),
            )
            .unwrap();
        engine
            .exec(
                None,
                &format!("CREATE TABLE {} (id INT, name TEXT)", conc_index_table(t)),
            )
            .unwrap();
        engine
            .exec(
                None,
                &format!("CREATE INDEX ON {} (name)", conc_index_table(t)),
            )
            .unwrap();
    }

    // Checkpoints fire while the writers run, so a kill can land between
    // CheckpointBegin/End with concurrent writes in flight. ON by default
    // since the Stage T checkpoint/FPI P0 fix; `M2B_CRASH_CONC_CKPT=1`
    // shortens the interval to 20ms for extra stress.
    //
    // RESOLVED ENGINE BUG (Stage T finding, reproduced ~1/3 of runs): with
    // the checkpoint thread enabled, recovery after the kill could emit a
    // SPURIOUS BTreeSplitCLR for an already-committed split
    // (redo_ref_lsn=INVALID, i.e. flagged by the undo-time page scan),
    // double-finishing it and corrupting the parent with a duplicate
    // downlink. Forensics on the preserved failing dir (/tmp/conc_repro_35):
    // `split_commit` appended the BTreeSplitCommit record FIRST and only
    // then pin_mut'ed the parent and the left page; with a checkpoint cycle
    // opened between the split's Copy and its Commit, those pin_muts fired
    // the pages' cycle FPIs at LSNs AFTER the Commit record, capturing
    // PRE-commit images (SPLIT_INCOMPLETE still set). FPI redo's
    // unconditional restore then rolled the page back past the Commit (the
    // FPI patches pd_lsn past the Commit's LSN, so the Commit redo's pd_lsn
    // guard skipped the flag clear / downlink insert). Fixed in
    // pg-am-btree/pg-storage: split_commit now PRE-TOUCHES every page the
    // Commit modifies with a scoped pin_mut (emitting any due cycle FPI)
    // before the record's WAL position is fixed, and the apply re-pins via
    // `BufferPool::pin_mut_without_fpi` so a checkpoint publishing in the
    // pre-touch → re-pin window cannot fire a stale post-commit FPI.
    // Deterministic regression test:
    // pg-am-btree/tests/btree_split_crash.rs
    // `test_btree_split_commit_fpi_precedes_commit_record`.
    let ckpt_interval_ms = if std::env::var("M2B_CRASH_CONC_CKPT").is_ok() {
        20
    } else {
        150
    };
    let stop = Arc::new(AtomicBool::new(false));
    let ckpt = {
        let engine = Arc::clone(&engine);
        let stop = Arc::clone(&stop);
        thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_millis(ckpt_interval_ms));
                if !stop.load(Ordering::Relaxed) {
                    engine.checkpoint().unwrap();
                }
            }
        })
    };

    let mut handles = Vec::with_capacity(threads);
    for t in 0..threads {
        let engine = Arc::clone(&engine);
        let data_dir = data_dir.to_path_buf();
        handles.push(thread::spawn(move || {
            run_conc_worker(&engine, &data_dir, seed, t, ops);
        }));
    }
    for h in handles {
        h.join().expect("concurrent writer panicked");
    }
    stop.store(true, Ordering::Relaxed);
    ckpt.join().expect("checkpoint thread panicked");
}

/// One writer thread: `ops` auto-committed ops against its own two tables,
/// rewriting its own expectation file after each committed op. The final
/// write is MODE full (all ops committed); every intermediate write is
/// MODE mid.
fn run_conc_worker(engine: &Engine, data_dir: &Path, seed: u64, t: usize, ops: usize) {
    let heap_t = conc_heap_table(t);
    let index_t = conc_index_table(t);
    let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9) ^ (t as u64 + 1));
    let mut model = ChildModel {
        tables: [(heap_t.clone(), Vec::new()), (index_t.clone(), Vec::new())]
            .into_iter()
            .collect(),
        extra_seq: 0,
        // Disjoint id space per thread.
        row_seq: (t as i32) * 1_000_000,
    };

    for completed in 1..=ops {
        do_conc_op(engine, &mut rng, &mut model, &heap_t, &index_t);
        let done = completed == ops;
        write_conc_expectation(engine, data_dir, &model, t, done, completed);
    }
}

/// Every arm changes at most one row of one table, so a mid-write kill
/// leaves the recovered state at most one op ahead of the expectation.
fn do_conc_op(engine: &Engine, rng: &mut Rng, model: &mut ChildModel, heap_t: &str, index_t: &str) {
    match rng.below(20) {
        // Heap INSERT (30%).
        0..=5 => {
            let id = model.row_seq;
            model.row_seq += 1;
            engine
                .exec(
                    None,
                    &format!("INSERT INTO {heap_t} VALUES ({id}, 'row-{id}')"),
                )
                .unwrap();
            model.tables.get_mut(heap_t).unwrap().push(id);
        }
        // Indexed INSERT (40%): wide keys keep leaf/root splits in flight.
        6..=13 => {
            let id = model.row_seq;
            model.row_seq += 1;
            let key = ix_key(id);
            engine
                .exec(
                    None,
                    &format!("INSERT INTO {index_t} VALUES ({id}, '{key}')"),
                )
                .unwrap();
            model.tables.get_mut(index_t).unwrap().push(id);
        }
        // HOT UPDATE (10%): only the unindexed `id` changes.
        14..=15 => {
            let ids = model.tables.get_mut(index_t).unwrap();
            if ids.is_empty() {
                return;
            }
            let idx = rng.below(ids.len() as u64) as usize;
            let old_id = ids[idx];
            let new_id = model.row_seq;
            model.row_seq += 1;
            engine
                .exec(
                    None,
                    &format!("UPDATE {index_t} SET id = {new_id} WHERE id = {old_id}"),
                )
                .unwrap();
            ids[idx] = new_id;
        }
        // Non-HOT UPDATE (10%): the indexed `name` changes.
        16..=17 => {
            let ids = model.tables.get(index_t).unwrap();
            if ids.is_empty() {
                return;
            }
            let idx = rng.below(ids.len() as u64) as usize;
            let id = ids[idx];
            let key_seq = model.row_seq;
            model.row_seq += 1;
            let key = ix_key(key_seq);
            engine
                .exec(
                    None,
                    &format!("UPDATE {index_t} SET name = '{key}' WHERE id = {id}"),
                )
                .unwrap();
        }
        // Heap DELETE (10%).
        _ => {
            let ids = model.tables.get_mut(heap_t).unwrap();
            if ids.is_empty() {
                return;
            }
            let idx = rng.below(ids.len() as u64) as usize;
            let id = ids.swap_remove(idx);
            engine
                .exec(None, &format!("DELETE FROM {heap_t} WHERE id = {id}"))
                .unwrap();
        }
    }
}

/// Rewrite this thread's expectation file atomically (tmp + fsync + rename).
fn write_conc_expectation(
    engine: &Engine,
    data_dir: &Path,
    model: &ChildModel,
    t: usize,
    done: bool,
    completed_ops: usize,
) {
    let mut out = String::new();
    out.push_str(if done { "MODE full\n" } else { "MODE mid\n" });
    out.push_str(&format!("OPS {completed_ops}\n"));
    for table in model.tables.keys() {
        out.push_str(&format!("TABLE {table}\n"));
        for (id, name) in scan_table(engine, table) {
            out.push_str(&format!("ROW {table} {id} {name}\n"));
        }
    }
    let tmp = data_dir.join(conc_expectation_tmp(t));
    fs::write(&tmp, &out).unwrap();
    fs::File::open(&tmp).unwrap().sync_all().unwrap();
    fs::rename(&tmp, data_dir.join(conc_expectation_file(t))).unwrap();
}

/// The Stage T crash-automation acceptance: kill -9 rounds against a child
/// running CONCURRENT writers (indexed tables, HOT updates, in-flight
/// splits, checkpoints). `M2B_CRASH_CONC_ROUNDS` defaults to 4 for CI; the
/// acceptance configuration is 1000 (see the module docs).
#[test]
fn m2b_crash_rounds_concurrent() {
    if std::env::var(CHILD_CONC_ENV_VAR).is_ok() {
        return; // we are the child; the entry test does the work
    }
    let rounds: u64 = std::env::var(CONC_ROUNDS_ENV_VAR)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(4);
    let threads = env_usize(CONC_THREADS_ENV_VAR, 4);
    let ops = env_usize(CONC_OPS_ENV_VAR, 60);

    for round in 0..rounds {
        let tmp = tempfile::TempDir::new().unwrap();
        let data_dir = tmp.path().to_path_buf();
        let mut child = spawn_child_at(&data_dir, round, CHILD_CONC_ENV_VAR, CONC_CHILD_TEST_NAME);

        // Never kill before the engine is open.
        let engine_ready = data_dir.join(ENGINE_READY_MARKER);
        let start = Instant::now();
        while !engine_ready.exists() {
            assert!(
                start.elapsed() < Duration::from_secs(30),
                "round {round}: child engine did not open in time"
            );
            thread::sleep(Duration::from_millis(2));
        }

        if round % 4 == 0 {
            // One in four rounds: let the full concurrent workload commit.
            assert!(
                wait_for_marker(&data_dir, Duration::from_secs(300)),
                "round {round}: concurrent child did not finish its workload in time"
            );
        } else {
            // Mid-concurrent-write kill: as soon as the expectation files
            // show a seed-derived number of committed ops in TOTAL.
            let target_ops = 10 + (round as usize * 7) % (threads * ops / 2).max(1);
            let start = Instant::now();
            loop {
                let mut total = 0usize;
                for t in 0..threads {
                    let path = data_dir.join(conc_expectation_file(t));
                    if path.exists() {
                        total += parse_expectation(&fs::read_to_string(path).unwrap()).ops;
                    }
                }
                if total >= target_ops {
                    break;
                }
                assert!(
                    start.elapsed() < Duration::from_secs(300),
                    "round {round}: concurrent child did not reach {target_ops} total ops in time"
                );
                thread::sleep(Duration::from_millis(2));
            }
        }
        child.kill().expect("failed to kill crash child");
        child.wait().expect("failed to reap crash child");

        verify_conc_round(round, &data_dir, threads);
    }
}

fn verify_conc_round(round: u64, data_dir: &Path, threads: usize) {
    let engine = Engine::open(data_dir, EngineConfig::new(data_dir)).unwrap_or_else(|e| {
        preserve_repro_dir(round, data_dir);
        panic!("round {round}: engine failed to reopen: {e}")
    });

    for t in 0..threads {
        let expectation_path = data_dir.join(conc_expectation_file(t));
        assert!(
            expectation_path.exists(),
            "round {round}: expectation file for thread {t} missing — the child never committed its first op"
        );
        let expectation = parse_expectation(&fs::read_to_string(&expectation_path).unwrap());

        if expectation.mid {
            // Prefix-durability for a concurrent writer: the recovered
            // state may be at most ONE op ahead of the expectation — a
            // committed-but-unrecorded insert shows as one extra row, a
            // delete as one missing row, an update as one of each. The
            // divergence budget is per THREAD (one in-flight op total),
            // accumulated across its tables and asserted once.
            let mut missing = 0usize;
            let mut extras = 0usize;
            for (table, expected_rows) in &expectation.tables {
                let rows = scan_table(&engine, table);
                let mut remaining = rows.clone();
                for expected in expected_rows {
                    match remaining.iter().position(|r| r == expected) {
                        Some(pos) => {
                            remaining.swap_remove(pos);
                        }
                        None => missing += 1,
                    }
                }
                extras += remaining.len();
            }
            assert!(
                missing <= 1 && extras <= 1,
                "round {round} thread {t}: recovered state diverges from the expectation by \
                 more than one in-flight op ({missing} missing, {extras} extra)"
            );
        } else {
            for (table, expected_rows) in &expectation.tables {
                let rows = scan_table(&engine, table);
                assert_eq!(
                    &rows, expected_rows,
                    "round {round} thread {t}: table {table} content diverged across crash recovery"
                );
            }
        }
        verify_conc_index(round, t, &engine, &expectation, data_dir);
    }
    engine.shutdown();
}

/// Preserve a failing round's data dir for WAL forensics (the harness
/// author's triage flow kept /tmp/conc_repro_35 the same way).
fn preserve_repro_dir(round: u64, data_dir: &Path) {
    let dst = std::path::PathBuf::from(format!("/tmp/conc_repro_round{round}"));
    let _ = fs::remove_dir_all(&dst);
    if let Err(e) = fs::rename(data_dir, &dst) {
        eprintln!("failed to preserve repro dir {data_dir:?}: {e}");
    } else {
        eprintln!("preserved failing round dir at {dst:?}");
        // The rename moved the dir out of the TempDir; keep the caller's
        // later cleanup happy by recreating an empty dir.
        let _ = fs::create_dir_all(data_dir);
    }
}

/// The per-thread index must agree with the heap after recovery. In mid
/// mode a committed-but-unrecorded update may have replaced one row's key,
/// so expectation-side lookups are only asserted for rows present in BOTH
/// the expectation and the recovered scan, and the one "extra" recovered
/// row gets its own reachability spot-check; `validate` (which catches a
/// split left unfinished by undo) always runs.
fn verify_conc_index(
    round: u64,
    t: usize,
    engine: &Engine,
    expectation: &Expectation,
    data_dir: &Path,
) {
    let table = conc_index_table(t);
    let index = engine
        .btree_index(&table, "name")
        .unwrap_or_else(|e| panic!("round {round} thread {t}: {table} index failed to open: {e}"));
    if let Err(e) = index.validate() {
        preserve_repro_dir(round, data_dir);
        panic!("round {round} thread {t}: {table} index is corrupt after crash recovery: {e}");
    }

    let recovered = scan_table(engine, &table);
    let ix_rows = recovered.len();
    // ~15 wide keys per leaf: past 30 rows the tree cannot still be a
    // single root leaf (keeps a round from passing vacuously with no split
    // recovery exercised).
    if ix_rows > 30 {
        assert!(
            index.tree_level() >= 1,
            "round {round} thread {t}: {ix_rows} rows in {table} but its index never split"
        );
    }

    let expected: Vec<_> = expectation
        .tables
        .get(&table)
        .into_iter()
        .flatten()
        .collect();
    for (id, name) in &expected {
        if !recovered.iter().any(|r| r == &(*id, name.clone())) {
            continue; // the one in-flight op may legitimately rewrite a row
        }
        let found = engine
            .index_lookup(&table, "name", &Datum::Text(name.clone()))
            .unwrap_or_else(|e| panic!("round {round} thread {t}: index lookup failed: {e}"));
        if found.is_none() {
            preserve_repro_dir(round, data_dir);
            panic!("round {round} thread {t}: committed row {id} of {table} is not reachable through its index");
        }
    }

    // Mid mode: the one committed-but-unrecorded row (an "extra" vs the
    // expectation — e.g. the new key of an in-flight update) must ALSO be
    // reachable through the index; validate() alone only proves structure,
    // not heap↔index agreement for that row.
    if expectation.mid {
        for (id, name) in &recovered {
            if expected.iter().any(|r| **r == (*id, name.clone())) {
                continue;
            }
            let found = engine
                .index_lookup(&table, "name", &Datum::Text(name.clone()))
                .unwrap_or_else(|e| panic!("round {round} thread {t}: index lookup failed: {e}"));
            if found.is_none() {
                preserve_repro_dir(round, data_dir);
                panic!("round {round} thread {t}: extra (committed-but-unrecorded) row {id} of {table} \
                     is not reachable through its index");
            }
        }
    }
}
