//! M5 Stage D slice 3: SIGKILL crash-rounds automation for the page-resident
//! HNSW index (tech-selection §8.1/§8.2 crash windows, §11.1 same-platform
//! bitwise reproducibility, §11.3 audit).
//!
//! Harness shape follows `m2b_crash_rounds.rs`: the parent spawns the test
//! binary itself as a child (`M5_CRASH_CHILD=1`), the child runs a
//! deterministic insert workload (seeded by the round number) through the
//! engine's HNSW API and is then SIGKILLed; the parent reopens the data
//! directory and validates against an atomic expectation file
//! (`expectation.txt`, tmp + fsync + rename).
//!
//! # What is verified each round
//!
//! Every `hnsw_insert` is durable at its own success boundary (the §8.1
//! boundary-① `flush_to`), so the durable state is always a PREFIX of the
//! insert stream plus at most one in-flight insert's residue. The child
//! rewrites the expectation after every insert (MODE/OPS/OID). The parent:
//!
//! - reopens, resolves the index through the catalog
//!   (`hnsw_index_first_page` → `open_hnsw_index`), and runs the §11.3
//!   audit — it must PASS on every residue shape (audit failure = red);
//! - **full mode** (even rounds, kill after the workload completed): exact
//!   state — node_count == OPS, every node LIVE, zero residues — and a
//!   BITWISE twin comparison: the in-memory `Hnsw` replayed over the same
//!   vector stream must answer a query grid with identical
//!   `(NodeId, distance.to_bits())` sequences;
//! - **mid mode** (odd rounds, kill at a seed-derived PROGRESS point):
//!   prefix durability + residue bounds — live ∈ {n, n+1}, hwm - live <= 1,
//!   at most one INITIALIZING / one orphan / one hidden-high-level entry,
//!   and the reachable-residue consistency pins (ghost ⟹ hwm == n+1,
//!   orphan ⟹ hwm == n, hidden ⟹ ghost). With zero residue the bitwise
//!   twin comparison still runs; with a residue the ghost's half-written
//!   edges legitimately diverge from any clean replay, so only functional
//!   sanity (queries answer, exact hit count) is asserted — noted here as
//!   deliberate, not a gap.
//!
//! Timing note (same accepted shape as m2b): the mid-mode kill lands within
//! one parent poll (2 ms) of the expectation reaching the target. Each child
//! insert costs exactly two fsyncs (the §8.1 boundary-① WAL flush plus the
//! expectation `sync_all`), so the {n, n+1} bounds hold whenever one
//! insert-plus-expectation cycle costs more than the poll window — true on
//! every real-disk environment we run on (an insert is ~ms). On a
//! hypothetical sub-millisecond-fsync setup (tmpfs, very fast NVMe) a 2 ms
//! window could fit several inserts and the mid-mode bounds could flake;
//! the knobs then are a wider bound or a slower poll. Registered, accepted
//! (m2b carries the same premise).
//!
//! # Kill timing
//!
//! Even rounds wait for the child's `ready-to-die` marker (full workload,
//! then killed). Odd rounds wait for `engine-ready` and then kill as soon as
//! the expectation shows `30 + (round % 60)` committed ops. Kills are never
//! delivered before `engine-ready`. A SIGKILLed child leaves
//! `{data_dir}/lock` behind; the parent removes it before reopening (the
//! m2b precedent).
//!
//! # Rounds
//!
//! `M5_CRASH_ROUNDS` controls the round count. The default of 25 is the CI
//! configuration; the acceptance configuration is 1000 rounds, run manually:
//!
//! ```sh
//! M5_CRASH_ROUNDS=1000 cargo test -p pg-engine --test m5_hnsw_crash_rounds --release -- --nocapture
//! ```

use std::fs;
use std::path::Path;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use pg_am_hnsw::{ExpectedParams, Hnsw, HnswParams, Metric, NeighborSelection, NodeId};
use pg_engine::{Engine, EngineConfig, Oid};

const CHILD_ENV_VAR: &str = "M5_CRASH_CHILD";
const DIR_ENV_VAR: &str = "M5_CRASH_DIR";
const SEED_ENV_VAR: &str = "M5_CRASH_SEED";
const ROUNDS_ENV_VAR: &str = "M5_CRASH_ROUNDS";
const CHILD_TEST_NAME: &str = "m5_hnsw_crash_child_entry";

const READY_MARKER: &str = "ready-to-die";
const ENGINE_READY_MARKER: &str = "engine-ready";
const EXPECTATION_FILE: &str = "expectation.txt";
const EXPECTATION_TMP: &str = "expectation.tmp";

const DIM: u16 = 128;

/// xorshift64* — deterministic PRNG so each round's stream is reproducible
/// (verbatim from m2b_crash_rounds.rs).
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

/// The vector stream — parent and child must derive it byte-identically.
fn vector_at(seed: u64, seq: u64) -> Vec<f32> {
    let mut r = Rng(seed ^ 0x9E37_79B9_7F4A_7C15u64.wrapping_mul(seq + 1));
    (0..DIM as usize)
        .map(|_| ((r.next() % 2000) as f32 - 1000.0) / 250.0)
        .collect()
}

/// Selection alternates by seed parity (Heuristic on even, Simple on odd).
fn selection_of(seed: u64) -> NeighborSelection {
    if seed % 2 == 0 {
        NeighborSelection::Heuristic
    } else {
        NeighborSelection::Simple
    }
}

/// The index's level-draw seed.
fn index_seed(seed: u64) -> u64 {
    seed ^ 0x5EED_5EED_5EED_5EED
}

fn expected_params(seed: u64) -> ExpectedParams {
    let p = HnswParams::default();
    ExpectedParams {
        dim: DIM,
        m: p.m(),
        m_max0: p.m_max0(),
        ef_construction: p.ef_construction(),
        ef_search_default: p.ef_search_default(),
        metric: Metric::L2,
        selection: selection_of(seed),
    }
}

// ---------------------------------------------------------------------------
// Child process
// ---------------------------------------------------------------------------

/// Child entry point: runs the seeded insert workload, then sleeps until
/// killed.
#[test]
fn m5_hnsw_crash_child_entry() {
    if std::env::var(CHILD_ENV_VAR).is_err() {
        return;
    }
    let data_dir = std::env::var(DIR_ENV_VAR).expect("data dir required");
    let seed: u64 = std::env::var(SEED_ENV_VAR)
        .expect("seed required")
        .parse()
        .expect("seed is u64");
    run_child(Path::new(&data_dir), seed);
}

/// Never returns: the workload runs, then the child sleeps until the
/// parent's SIGKILL. The engine and index MUST stay in scope until the
/// kill — returning early would drop them, and `DataDirLock::drop` would
/// remove the lock file (and any orderly teardown would soften the
/// "crash of a live engine" shape this harness exists to exercise).
fn run_child(data_dir: &Path, seed: u64) -> ! {
    let engine = Engine::open(data_dir, EngineConfig::new(data_dir)).unwrap();
    fs::write(data_dir.join(ENGINE_READY_MARKER), b"").unwrap();

    let (oid, meta_page) = engine
        .create_hnsw_index(
            HnswParams::default(),
            DIM,
            Metric::L2,
            selection_of(seed),
            index_seed(seed),
        )
        .unwrap();
    let outcome = engine
        .open_hnsw_index(meta_page, &expected_params(seed))
        .unwrap();
    assert!(
        outcome.warnings.is_empty(),
        "open warnings must be empty on the creation-matched parameters"
    );
    let mut index = outcome.index;

    let mut rng = Rng(seed | 0x9E37_79B9_7F4A_7C15);
    let op_count = 120 + rng.below(60);
    let mid = seed % 2 == 1;
    for i in 0..op_count {
        // Periodic checkpoints: they truncate the replay window, so kills
        // exercise both the with- and without-checkpoint recovery shapes.
        if i > 0 && i % 47 == 0 {
            engine.checkpoint().unwrap();
        }
        let id = engine.hnsw_insert(&mut index, &vector_at(seed, i)).unwrap();
        // Dense allocation pinned: the i-th insert MUST get NodeId(i).
        assert_eq!(id, NodeId(i as u32), "dense NodeId allocation");
        write_expectation(data_dir, mid, i + 1, oid);
    }

    // Workload complete — signal, then wait for the kill with the engine
    // ALIVE (see the function's `-> !` contract above).
    fs::write(data_dir.join(READY_MARKER), b"").unwrap();
    loop {
        thread::sleep(Duration::from_secs(1));
    }
}

fn write_expectation(data_dir: &Path, mid: bool, ops: u64, oid: Oid) {
    let out = format!(
        "MODE {}\nOPS {ops}\nOID {}\n",
        if mid { "mid" } else { "full" },
        oid.0
    );
    let tmp = data_dir.join(EXPECTATION_TMP);
    fs::write(&tmp, &out).unwrap();
    fs::File::open(&tmp).unwrap().sync_all().unwrap();
    // A SIGKILL landing between the sync and the rename leaves a stale
    // `expectation.tmp` behind; the parent only ever reads
    // `expectation.txt`, so the residue is harmless.
    fs::rename(&tmp, data_dir.join(EXPECTATION_FILE)).unwrap();
}

// ---------------------------------------------------------------------------
// Parent harness
// ---------------------------------------------------------------------------

struct Expectation {
    mid: bool,
    ops: u64,
    oid: u64,
}

fn parse_expectation(text: &str) -> Expectation {
    let (mut mid, mut ops, mut oid) = (None, None, None);
    for line in text.lines() {
        let parts: Vec<&str> = line.splitn(2, ' ').collect();
        match parts.as_slice() {
            ["MODE", mode] => {
                mid = Some(match *mode {
                    "full" => false,
                    "mid" => true,
                    other => panic!("unknown expectation mode {other}"),
                })
            }
            ["OPS", n] => ops = Some(n.parse().expect("OPS line must carry a number")),
            ["OID", n] => oid = Some(n.parse().expect("OID line must carry a number")),
            other => panic!("malformed expectation line: {other:?}"),
        }
    }
    Expectation {
        mid: mid.expect("expectation must start with a MODE line"),
        ops: ops.expect("expectation must carry an OPS line"),
        oid: oid.expect("expectation must carry an OID line"),
    }
}

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

/// The M5 Stage D slice-3 crash automation: `M5_CRASH_ROUNDS` kill -9 +
/// reopen cycles against the page-resident HNSW index (default 25; 1000
/// for acceptance).
#[test]
fn m5_hnsw_crash_rounds() {
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
        let mut child = spawn_child(&data_dir, round);

        if round % 2 == 0 {
            // Even rounds: let the full workload commit, then kill.
            wait_for_file(
                &data_dir.join(READY_MARKER),
                Duration::from_secs(120),
                "the ready-to-die marker",
                round,
            );
        } else {
            // Odd rounds: kill at a PROGRESS point — as soon as the
            // expectation shows the seed-derived number of committed ops.
            wait_for_file(
                &data_dir.join(ENGINE_READY_MARKER),
                Duration::from_secs(30),
                "the engine-ready marker",
                round,
            );
            let target_ops = 30 + (round % 60);
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

        // The SIGKILLed child left its `{data_dir}/lock` behind; play the
        // documented operator action before reopening (m2b precedent). The
        // lock's PRESENCE is itself the pin that we killed a live engine:
        // the child keeps the engine in scope until the kill (the
        // `run_child` `-> !` contract), so `DataDirLock::drop` never runs.
        assert!(
            data_dir.join("lock").exists(),
            "round {round}: lock file missing after SIGKILL — the child's engine was already dropped (the kill hit a torn-down process, not a live engine)"
        );
        let _ = std::fs::remove_file(data_dir.join("lock"));

        verify_round(round, &data_dir);
    }
}

/// The in-memory twin replayed over `vector_at(seed, 0..n)` — the §11.1
/// same-platform bitwise reference.
fn build_twin(seed: u64, n: u64) -> Hnsw {
    let mut twin = Hnsw::new_with_neighbor_selection(
        DIM,
        Metric::L2,
        HnswParams::default(),
        index_seed(seed),
        selection_of(seed),
    )
    .unwrap();
    for i in 0..n {
        let id = twin.insert(&vector_at(seed, i)).unwrap();
        assert_eq!(id, NodeId(i as u32), "twin: dense NodeId stream");
    }
    twin
}

/// The bitwise query grid: the page-resident index and the in-memory twin
/// must answer every query with identical `(NodeId, distance)` bit
/// sequences.
fn assert_bitwise_twin(engine: &Engine, index: &pg_am_hnsw::HnswIndex, seed: u64, n: u64) {
    let twin = build_twin(seed, n);
    let query_seed = seed ^ 0x0A11_CE55_0A11_CE55;
    for j in 0..4u64 {
        let q = vector_at(query_seed, j);
        for k in [1usize, 5, 15] {
            let expected: Vec<(u32, u64)> = twin
                .search(&q, k, None)
                .unwrap()
                .iter()
                .map(|(id, d)| (id.0, d.to_bits()))
                .collect();
            let got: Vec<(u32, u64)> = engine
                .hnsw_search(index, &q, k, None)
                .unwrap()
                .iter()
                .map(|(id, d)| (id.0, d.to_bits()))
                .collect();
            assert_eq!(
                got, expected,
                "bitwise twin mismatch at query {j}, k = {k} (n = {n}, seed {seed})"
            );
        }
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
    let n = expectation.ops;

    // Catalog resolution → open (warnings must be empty) → §11.3 audit.
    let meta_page = engine
        .hnsw_index_first_page(Oid(expectation.oid))
        .unwrap()
        .unwrap_or_else(|| panic!("round {round}: catalog lost the index OID"));
    let outcome = engine
        .open_hnsw_index(meta_page, &expected_params(round))
        .unwrap_or_else(|e| panic!("round {round}: open_hnsw_index failed: {e}"));
    // The child created and opened with these exact parameters — any
    // warning (e.g. ef_search_default mismatch) means the meta page or the
    // expectation check drifted.
    assert!(
        outcome.warnings.is_empty(),
        "round {round}: unexpected open warnings: {:?}",
        outcome.warnings
    );
    let index = outcome.index;
    let report = index
        .audit(engine.storage().buffer_pool())
        .unwrap_or_else(|e| {
            panic!("round {round}: §11.3 audit failed on the recovered graph: {e}")
        });
    // Cross-pin the two INDEPENDENT chain walks: the handle's hwm (derived at
    // open) and the audit's node_count (the audit deliberately re-walks the
    // directory chain itself, trusting no handle cache). Nothing mutates
    // between open and audit, so any divergence is a bug in one of the walks.
    assert_eq!(
        report.node_count,
        index.hwm(),
        "round {round}: audit chain-derived hwm disagrees with the open handle's hwm"
    );

    if !expectation.mid {
        // Full mode: the workload completed; the state is exact.
        assert_eq!(report.node_count, n, "round {round}: node_count");
        assert_eq!(report.live_count, n, "round {round}: every node LIVE");
        assert_eq!(report.initializing_count, 0, "round {round}");
        assert_eq!(report.orphan_entry_count, 0, "round {round}");
        assert_eq!(report.hidden_high_level_count, 0, "round {round}");
        assert_eq!(report.tombstoned_count, 0, "round {round}");
        assert_eq!(index.hwm(), n, "round {round}: hwm");
        assert_bitwise_twin(&engine, &index, round, n);
    } else {
        // Mid mode: prefix durability + the single-in-flight-insert
        // residue bounds.
        let live = report.live_count;
        let hwm = index.hwm();
        assert!(
            live == n || live == n + 1,
            "round {round}: live {live} outside {{n, n+1}} (n = {n})"
        );
        assert!(
            hwm == n || hwm == n + 1,
            "round {round}: hwm {hwm} outside {{n, n+1}} (n = {n})"
        );
        assert!(live <= hwm && hwm - live <= 1, "round {round}: live/hwm");
        assert!(report.initializing_count <= 1, "round {round}");
        assert!(report.orphan_entry_count <= 1, "round {round}");
        assert!(report.hidden_high_level_count <= 1, "round {round}");
        assert_eq!(
            report.tombstoned_count, 0,
            "round {round}: M5 has no tombstone producer"
        );
        // Reachable-residue consistency pins (the only shapes a single
        // in-flight insert can leave: clean / ghost / orphan / ghost+hidden).
        if report.initializing_count == 1 {
            assert_eq!(
                hwm,
                n + 1,
                "round {round}: a ghost implies the mapping was published"
            );
        }
        if report.orphan_entry_count == 1 {
            assert_eq!(
                hwm, n,
                "round {round}: an orphan implies the mapping was never published"
            );
        }
        if report.hidden_high_level_count == 1 {
            assert_eq!(
                report.initializing_count, 1,
                "round {round}: a hidden high-level node is always the unpublished in-flight insert"
            );
        }

        let zero_residue = report.initializing_count == 0
            && report.orphan_entry_count == 0
            && report.hidden_high_level_count == 0;
        if zero_residue {
            // A clean prefix: the bitwise twin comparison still applies
            // (m = live ∈ {n, n+1}).
            assert_bitwise_twin(&engine, &index, round, live);
        } else {
            // The ghost's half-written edges legitimately diverge from any
            // clean replay — functional sanity only (module header note).
            // live >= 30 (the progress floor) guarantees enough hits.
            let query_seed = round ^ 0x0A11_CE55_0A11_CE55;
            for j in 0..2u64 {
                let q = vector_at(query_seed, j);
                let hits = engine.hnsw_search(&index, &q, 5, None).unwrap();
                assert_eq!(hits.len(), 5, "round {round}: functional sanity");
            }
        }
    }

    engine.shutdown();
}
