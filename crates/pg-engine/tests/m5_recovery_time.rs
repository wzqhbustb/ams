//! M5 Stage E: the §11.4/§13.2 recovery-time acceptance — after a 1M-vector
//! build, an immediate checkpoint (the §9.2 hard requirement made flesh:
//! the bulk-load completion point MUST checkpoint, or the <30 s guarantee
//! does not hold), a fixed 100k-insert increment, and a SIGKILL, the time
//! from `Engine::open` to **queryable** must be under 30 s.
//!
//! # 口径 (pinned, tech-selection §9.2/§13.2)
//!
//! The <30 s budget covers ONLY the post-checkpoint incremental window —
//! redo replays the 100k increments' records, not the 1M build's. This test
//! is therefore also the empirical proof that the checkpoint gated the
//! replay window: without it the replay would cover ~3 GB of WAL and blow
//! the budget by minutes.
//!
//! # Harness
//!
//! Same discipline as `m5_hnsw_crash_rounds.rs`: the parent spawns the test
//! binary itself as a child (`M5_RECOVERY_CHILD=1`); the child builds,
//! checkpoints, inserts the increment, writes an atomic expectation file
//! (tmp + fsync + rename) and then sleeps with the engine ALIVE until the
//! parent's SIGKILL. The parent reopens (after removing the stale lock —
//! the documented operator action) and times:
//!
//! 1. `Engine::open` (redo replay happens here),
//! 2. catalog resolution + `open_hnsw_index`,
//! 3. the first successful `hnsw_search` (= "queryable", §11.4).
//!
//! The sum must be < 30 s (env-overridable for experiments via
//! `M5_RECOVERY_BUDGET_SECS`; the acceptance run uses the default).
//!
//! Post-budget (never in the measurement) the parent also runs the §11.3
//! audit and an exact WAL scan reporting the replay-window RECORD COUNT —
//! the benchmarks contract metric (coding-plan Stage E: 恢复时长含重放窗口
//! 记录数), counted with the same `WalReader` redo uses, from the
//! checkpoint LSN (inclusive — the window bytes include the
//! CheckpointBegin/End pair) to the expectation's final LSN, cross-pinned
//! to land exactly on it.
//!
//! # Gate
//!
//! Manual/nightly only (coding-plan CI 清单 #7): without
//! `M5_RECOVERY_BENCH=1` the test skips. A set-but-not-"1" value panics —
//! a mis-set acceptance gate must never silently downgrade to a skip.
//!
//! # Knobs
//!
//! - `M5_RECOVERY_BASE` (default 1_000_000) / `M5_RECOVERY_INCREMENT`
//!   (default 100_000) — scale; small values give a smoke run.
//! - `M5_RECOVERY_POOL_MB` (default 4096) — buffer pool size; the 1M-node
//!   build (~0.8 GB of pages) must fit or construction thrashes the pool
//!   (the default 128 MB pool would turn the build into an eviction
//!   benchmark, which is not what §13.2 measures).
//!
//! # Run
//!
//! ```sh
//! M5_RECOVERY_BENCH=1 cargo test -p pg-engine --test m5_recovery_time --release -- --nocapture
//! ```

use std::fs;
use std::path::Path;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use pg_am_hnsw::{ExpectedParams, HnswParams, Metric, NeighborSelection, NodeId};
use pg_engine::{Engine, EngineConfig, Oid};

const BENCH_ENV_VAR: &str = "M5_RECOVERY_BENCH";
const CHILD_ENV_VAR: &str = "M5_RECOVERY_CHILD";
const DIR_ENV_VAR: &str = "M5_RECOVERY_DIR";
const BASE_ENV_VAR: &str = "M5_RECOVERY_BASE";
const INCR_ENV_VAR: &str = "M5_RECOVERY_INCREMENT";
const POOL_ENV_VAR: &str = "M5_RECOVERY_POOL_MB";
const BUDGET_ENV_VAR: &str = "M5_RECOVERY_BUDGET_SECS";
const CHILD_TEST_NAME: &str = "m5_recovery_time_child_entry";

const ENGINE_READY_MARKER: &str = "engine-ready";
const READY_MARKER: &str = "ready-to-die";
const PROGRESS_FILE: &str = "progress.txt";
const EXPECTATION_FILE: &str = "expectation.txt";
const EXPECTATION_TMP: &str = "expectation.tmp";

const DIM: u16 = 128;
const DEFAULT_BASE: u64 = 1_000_000;
const DEFAULT_INCREMENT: u64 = 100_000;
const DEFAULT_POOL_MB: u64 = 4096;
const DEFAULT_BUDGET_SECS: u64 = 30;

/// Fixed seeds: the acceptance numbers must be comparable across runs.
const VECTOR_SEED: u64 = 42;
const INDEX_SEED: u64 = 42;

/// xorshift64* — deterministic PRNG (verbatim from m5_hnsw_crash_rounds.rs).
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

/// The vector stream — parent and child must derive it byte-identically
/// (verbatim from m5_hnsw_crash_rounds.rs).
fn vector_at(seed: u64, seq: u64) -> Vec<f32> {
    let mut r = Rng(seed ^ 0x9E37_79B9_7F4A_7C15u64.wrapping_mul(seq + 1));
    (0..DIM as usize)
        .map(|_| ((r.next() % 2000) as f32 - 1000.0) / 250.0)
        .collect()
}

fn params() -> HnswParams {
    HnswParams::default()
}

fn expected_params() -> ExpectedParams {
    let p = params();
    ExpectedParams {
        dim: DIM,
        m: p.m(),
        m_max0: p.m_max0(),
        ef_construction: p.ef_construction(),
        ef_search_default: p.ef_search_default(),
        metric: Metric::L2,
        selection: NeighborSelection::Heuristic,
    }
}

fn env_u64(name: &str, default: u64) -> u64 {
    match std::env::var(name) {
        Ok(v) => v
            .parse()
            .unwrap_or_else(|_| panic!("{name} must be a positive integer, got {v:?}")),
        Err(_) => default,
    }
}

fn engine_config(data_dir: &Path) -> EngineConfig {
    let mut cfg = EngineConfig::new(data_dir);
    let pool_mb = env_u64(POOL_ENV_VAR, DEFAULT_POOL_MB);
    cfg.storage.buffer_pool_size = (pool_mb as usize) * 1024 * 1024;
    cfg
}

// ---------------------------------------------------------------------------
// Child process
// ---------------------------------------------------------------------------

/// Child entry point: build → checkpoint → increment, then sleep until
/// killed.
#[test]
fn m5_recovery_time_child_entry() {
    if std::env::var(CHILD_ENV_VAR).is_err() {
        return;
    }
    let data_dir = std::env::var(DIR_ENV_VAR).expect("data dir required");
    let base = env_u64(BASE_ENV_VAR, DEFAULT_BASE);
    let incr = env_u64(INCR_ENV_VAR, DEFAULT_INCREMENT);
    run_child(Path::new(&data_dir), base, incr);
}

/// Never returns: the workload runs, then the child sleeps until the
/// parent's SIGKILL. The engine and index MUST stay in scope until the
/// kill — returning early would drop them, and `DataDirLock::drop` would
/// remove the lock file (the same `-> !` contract as the crash-rounds
/// child).
fn run_child(data_dir: &Path, base: u64, incr: u64) -> ! {
    let engine = Engine::open(data_dir, engine_config(data_dir)).unwrap();
    fs::write(data_dir.join(ENGINE_READY_MARKER), b"").unwrap();

    let (oid, meta_page) = engine
        .create_hnsw_index(
            params(),
            DIM,
            Metric::L2,
            NeighborSelection::Heuristic,
            INDEX_SEED,
        )
        .unwrap();
    let outcome = engine
        .open_hnsw_index(meta_page, &expected_params())
        .unwrap();
    assert!(
        outcome.warnings.is_empty(),
        "open warnings must be empty on the creation-matched parameters"
    );
    let mut index = outcome.index;

    // Phase 1: the bulk load. Progress is reported coarsely (plain write,
    // no fsync — it is a liveness signal for the parent's log, not part of
    // the durability contract).
    for i in 0..base {
        let id = engine
            .hnsw_insert(&mut index, &vector_at(VECTOR_SEED, i))
            .unwrap();
        assert_eq!(id, NodeId(i as u32), "dense NodeId allocation");
        if i > 0 && i % 10_000 == 0 {
            fs::write(data_dir.join(PROGRESS_FILE), format!("build {i}/{base}\n")).unwrap();
        }
    }

    // §9.2 hard requirement, acceptance 落实: the bulk-load completion
    // point checkpoints IMMEDIATELY, truncating the redo replay window to
    // the incremental tail. Without this call the <30 s budget does not
    // hold (and this test's result is the empirical proof that it does).
    // The progress line keeps the parent's 30 s reports meaningful through
    // the checkpoint's own (minutes-long) flush.
    fs::write(
        data_dir.join(PROGRESS_FILE),
        format!("checkpoint after {base}\n"),
    )
    .unwrap();
    // The CheckpointBegin LSN: the checkpoint's FIRST WAL action is the
    // CheckpointBegin append (checkpoint.rs), and this thread is the only
    // appender, so the pre-call clock position is exactly the begin
    // record's start. The replay window (and the record count below) is
    // measured from here — CheckpointBegin/End included, matching redo's
    // scan window (Stage E review: capturing it AFTER checkpoint() would
    // silently start at the post-checkpoint tail instead).
    let checkpoint_lsn = engine.storage().wal_writer().current_lsn();
    engine.checkpoint().unwrap();

    // Phase 2: the fixed post-checkpoint increment — this is the window
    // redo will replay after the kill.
    for i in base..base + incr {
        let id = engine
            .hnsw_insert(&mut index, &vector_at(VECTOR_SEED, i))
            .unwrap();
        assert_eq!(id, NodeId(i as u32), "dense NodeId allocation");
        if i > 0 && i % 10_000 == 0 {
            fs::write(
                data_dir.join(PROGRESS_FILE),
                format!("increment {}/{}\n", i - base, incr),
            )
            .unwrap();
        }
    }
    let final_lsn = engine.storage().wal_writer().current_lsn();

    // Workload complete — publish the expectation atomically (tmp + fsync +
    // rename, crash-rounds discipline), signal, then wait for the kill with
    // the engine ALIVE.
    let out = format!(
        "BASE {base}\nINCR {incr}\nOID {}\nCHECKPOINT_LSN {}\nFINAL_LSN {}\n",
        oid.0, checkpoint_lsn.0, final_lsn.0
    );
    let tmp = data_dir.join(EXPECTATION_TMP);
    fs::write(&tmp, &out).unwrap();
    fs::File::open(&tmp).unwrap().sync_all().unwrap();
    fs::rename(&tmp, data_dir.join(EXPECTATION_FILE)).unwrap();
    fs::write(data_dir.join(READY_MARKER), b"").unwrap();
    loop {
        thread::sleep(Duration::from_secs(1));
    }
}

// ---------------------------------------------------------------------------
// Parent harness
// ---------------------------------------------------------------------------

struct Expectation {
    base: u64,
    incr: u64,
    oid: u64,
    checkpoint_lsn: u64,
    final_lsn: u64,
}

fn parse_expectation(text: &str) -> Expectation {
    let (mut base, mut incr, mut oid, mut ckpt, mut fin) = (None, None, None, None, None);
    for line in text.lines() {
        let parts: Vec<&str> = line.splitn(2, ' ').collect();
        match parts.as_slice() {
            ["BASE", n] => base = Some(n.parse().expect("BASE line must carry a number")),
            ["INCR", n] => incr = Some(n.parse().expect("INCR line must carry a number")),
            ["OID", n] => oid = Some(n.parse().expect("OID line must carry a number")),
            ["CHECKPOINT_LSN", n] => {
                ckpt = Some(n.parse().expect("CHECKPOINT_LSN must carry a number"))
            }
            ["FINAL_LSN", n] => fin = Some(n.parse().expect("FINAL_LSN must carry a number")),
            other => panic!("malformed expectation line: {other:?}"),
        }
    }
    Expectation {
        base: base.expect("expectation must carry a BASE line"),
        incr: incr.expect("expectation must carry an INCR line"),
        oid: oid.expect("expectation must carry an OID line"),
        checkpoint_lsn: ckpt.expect("expectation must carry a CHECKPOINT_LSN line"),
        final_lsn: fin.expect("expectation must carry a FINAL_LSN line"),
    }
}

fn spawn_child(data_dir: &Path) -> std::process::Child {
    let mut cmd = Command::new(std::env::current_exe().expect("test binary path"));
    cmd.arg("--test-threads=1")
        .arg(CHILD_TEST_NAME)
        .arg("--exact")
        .env(CHILD_ENV_VAR, "1")
        .env(DIR_ENV_VAR, data_dir.as_os_str())
        // Forward the scale/pool knobs to the child (it reads its own env).
        .envs(
            [BASE_ENV_VAR, INCR_ENV_VAR, POOL_ENV_VAR]
                .iter()
                .filter_map(|k| std::env::var(k).ok().map(|v| (k, v))),
        )
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    cmd.spawn().expect("failed to spawn recovery-bench child")
}

/// The §11.4/§13.2 acceptance: 1M build → immediate checkpoint → 100k
/// increment → SIGKILL → `Engine::open` to queryable in < 30 s. Manual /
/// nightly only; skipped unless `M5_RECOVERY_BENCH=1`.
/// RAII reaper for the bench child: any early exit from the parent (the
/// 12 h timeout, a dead-child panic, a failed assertion) still kills and
/// reaps it — a leaked child keeps a multi-GiB buffer pool fsyncing
/// against a deleted TempDir forever (Stage E review).
struct ChildGuard {
    child: Option<std::process::Child>,
}

impl ChildGuard {
    fn spawn(data_dir: &Path) -> Self {
        Self {
            child: Some(spawn_child(data_dir)),
        }
    }

    fn try_wait(&mut self) -> Option<std::process::ExitStatus> {
        self.child
            .as_mut()
            .expect("child already reaped")
            .try_wait()
            .expect("failed to poll child")
    }

    /// The normal path: loud on kill/reap failure (unchanged semantics).
    /// The child stays owned by the guard until the reap SUCCEEDS — if
    /// kill/wait fails and we panic, `Drop` still retries the kill+reap
    /// (`take()` first would disarm the guard on exactly the failure path
    /// it exists for — Stage E review).
    fn kill_and_reap(&mut self) {
        let child = self.child.as_mut().expect("child already reaped");
        child.kill().expect("failed to kill recovery-bench child");
        child.wait().expect("failed to reap recovery-bench child");
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

#[test]
fn m5_recovery_time() {
    if std::env::var(CHILD_ENV_VAR).is_ok() {
        return; // we are the child; the entry test does the work
    }
    match std::env::var(BENCH_ENV_VAR) {
        Ok(v) if v == "1" => {}
        Ok(v) => panic!("{BENCH_ENV_VAR} must be exactly \"1\" when set, got {v:?} — a mis-set acceptance gate must never silently skip"),
        Err(std::env::VarError::NotUnicode(_)) => {
            panic!("{BENCH_ENV_VAR} is set but not valid Unicode")
        }
        Err(std::env::VarError::NotPresent) => {
            eprintln!(
                "skipping §13.2 recovery-time acceptance (set {BENCH_ENV_VAR}=1 to run; manual/nightly only)"
            );
            return;
        }
    }
    let base = env_u64(BASE_ENV_VAR, DEFAULT_BASE);
    let incr = env_u64(INCR_ENV_VAR, DEFAULT_INCREMENT);
    let budget = Duration::from_secs(env_u64(BUDGET_ENV_VAR, DEFAULT_BUDGET_SECS));
    assert!(base >= 1 && incr >= 1, "base/increment must be >= 1");

    let tmp = tempfile::TempDir::new().unwrap();
    let data_dir = tmp.path().to_path_buf();
    let mut child = ChildGuard::spawn(&data_dir);

    // Wait for the workload to finish. The timeout is deliberately huge:
    // a 1.1M-insert build with a per-insert success-boundary fsync takes
    // wall-clock hours on slow disks; the parent reports progress so a
    // stuck child is distinguishable from a slow one.
    let start = Instant::now();
    let mut last_report = Instant::now();
    while !data_dir.join(READY_MARKER).exists() {
        // Fail fast on a dead child: its stdout/stderr are swallowed, so a
        // panic mid-build would otherwise cost the full 12 h timeout to
        // discover.
        if let Some(status) = child.try_wait() {
            let progress = fs::read_to_string(data_dir.join(PROGRESS_FILE))
                .unwrap_or_else(|_| "starting".to_string());
            panic!(
                "child exited before ready-to-die (status {status}); last progress: {}",
                progress.trim()
            );
        }
        assert!(
            start.elapsed() < Duration::from_secs(12 * 3600),
            "child did not finish the workload within 12 h"
        );
        if last_report.elapsed() > Duration::from_secs(30) {
            let progress = fs::read_to_string(data_dir.join(PROGRESS_FILE))
                .unwrap_or_else(|_| "starting".to_string());
            eprintln!(
                "[{:?}] child progress: {}",
                start.elapsed(),
                progress.trim()
            );
            last_report = Instant::now();
        }
        thread::sleep(Duration::from_millis(500));
    }
    let build_elapsed = start.elapsed();
    child.kill_and_reap();

    // The SIGKILLed child leaves `{data_dir}/lock` behind; its PRESENCE is
    // the pin that we killed a live engine (same discipline as the crash
    // rounds), then the documented operator action removes it.
    assert!(
        data_dir.join("lock").exists(),
        "lock file missing after SIGKILL — the child's engine was already dropped (the kill hit a torn-down process, not a live engine)"
    );
    let _ = fs::remove_file(data_dir.join("lock"));

    let expectation = parse_expectation(
        &fs::read_to_string(data_dir.join(EXPECTATION_FILE))
            .expect("expectation file missing — the child never completed the workload"),
    );
    assert_eq!(expectation.base, base, "expectation/base mismatch");
    assert_eq!(expectation.incr, incr, "expectation/increment mismatch");
    let replay_window_bytes = expectation.final_lsn - expectation.checkpoint_lsn;

    // ---- The measured window: Engine::open → queryable (§11.4) ----
    let t0 = Instant::now();
    let engine = Engine::open(&data_dir, engine_config(&data_dir))
        .expect("engine failed to reopen after SIGKILL");
    let t_engine_open = t0.elapsed();

    let t1 = Instant::now();
    let meta_page = engine
        .hnsw_index_first_page(Oid(expectation.oid))
        .unwrap()
        .expect("catalog lost the index OID");
    let outcome = engine
        .open_hnsw_index(meta_page, &expected_params())
        .unwrap();
    assert!(
        outcome.warnings.is_empty(),
        "unexpected open warnings: {:?}",
        outcome.warnings
    );
    let index = outcome.index;
    let t_index_open = t1.elapsed();

    let t2 = Instant::now();
    let hits = engine
        .hnsw_search(
            &index,
            &vector_at(VECTOR_SEED ^ 0x0A11_CE55_0A11_CE55, 0),
            10,
            None,
        )
        .expect("first post-recovery query failed");
    assert_eq!(
        hits.len(),
        ((base + incr) as usize).min(10),
        "first query must return k hits (or all n when the smoke scale is tiny)"
    );
    let t_first_query = t2.elapsed();
    let total = t0.elapsed();

    // ---- Post-timing sanity (NOT part of the budget): exact state ----
    let t3 = Instant::now();
    let report = index.audit(engine.storage().buffer_pool()).unwrap();
    let t_audit = t3.elapsed();
    let n = base + incr;
    assert_eq!(report.node_count, n, "audit node_count");
    assert_eq!(report.live_count, n, "every node LIVE");
    assert_eq!(report.initializing_count, 0, "no ghosts");
    assert_eq!(report.orphan_entry_count, 0, "no orphans");
    assert_eq!(
        report.hidden_high_level_count, 0,
        "no hidden high-level nodes"
    );
    assert_eq!(report.tombstoned_count, 0, "M5 has no tombstone producer");
    assert_eq!(index.hwm(), n, "hwm cross-pin");

    // ---- Post-timing (not in budget): the exact replay-window RECORD
    // count — the benchmarks contract metric (module header). Counted
    // from the checkpoint LSN inclusive; the scan must land exactly on
    // the expectation's final LSN (the window is self-consistent).
    let t4 = Instant::now();
    let mut reader = pg_storage::wal::WalReader::open_at(
        data_dir.join("wal"),
        engine_config(&data_dir).storage.wal_segment_size,
        pg_storage::types::Lsn(expectation.checkpoint_lsn),
    )
    .expect("WAL reader must open at the checkpoint LSN");
    let mut replay_records = 0u64;
    while reader.current_lsn().0 < expectation.final_lsn {
        match reader.next_record() {
            Ok(Some(_)) => replay_records += 1,
            Ok(None) => break,
            Err(e) => panic!("WAL scan inside the replay window failed: {e}"),
        }
    }
    assert_eq!(
        reader.current_lsn().0,
        expectation.final_lsn,
        "the WAL scan must land exactly on the expectation's final LSN ({} < {})",
        reader.current_lsn().0,
        expectation.final_lsn
    );
    let t_wal_scan = t4.elapsed();

    eprintln!("=== M5 §13.2 recovery-time measurement ===");
    eprintln!("scale: base {base} + increment {incr} = {n} vectors (dim {DIM})");
    eprintln!("build+increment wall clock (child): {build_elapsed:?}");
    eprintln!(
        "replay window: {} bytes (LSN {} → {})",
        replay_window_bytes, expectation.checkpoint_lsn, expectation.final_lsn
    );
    eprintln!("Engine::open (redo replay): {t_engine_open:?}");
    eprintln!("catalog resolve + open_hnsw_index: {t_index_open:?}");
    eprintln!("first hnsw_search (queryable): {t_first_query:?}");
    eprintln!("TOTAL open-to-queryable: {total:?} (budget {budget:?})");
    eprintln!("post-timing audit (not in budget): {t_audit:?}");
    eprintln!(
        "replay window records: {replay_records} (exact WAL scan, not in budget: {t_wal_scan:?})"
    );

    assert!(
        total < budget,
        "§13.2 breached: open-to-queryable took {total:?} >= {budget:?} — the budget only ever \
         covers the post-checkpoint incremental window (§9.2), so this means either the \
         checkpoint did not gate the replay window or redo got slower"
    );

    engine.shutdown();
}
