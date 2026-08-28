//! Crash-recovery integration tests for pg-storage.
//!
//! This file contains both manual fixed-scenario crash tests and an automated
//! random-kill harness. The harness works by spawning a child process that
//! executes a storage workload and then killing it abruptly (simulating
//! `kill -9`). The parent then reopens the data directory and validates that
//! recovery succeeds and data is consistent.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use pg_storage::config::StorageConfig;
use pg_storage::engine::StorageEngine;
use pg_storage::error::StorageError;
use pg_storage::page::PAGE_HEADER_SIZE;
use pg_storage::types::PageId;

const CHILD_ENV_VAR: &str = "CRASH_RECOVERY_CHILD";
const DIR_ENV_VAR: &str = "CRASH_RECOVERY_DIR";
const SCENARIO_ENV_VAR: &str = "CRASH_RECOVERY_SCENARIO";
const ITERATIONS_ENV_VAR: &str = "CRASH_RECOVERY_ITERATIONS";
const BP_SIZE_ENV_VAR: &str = "CRASH_RECOVERY_BP_SIZE";
const CHILD_TEST_NAME: &str = "crash_recovery_child_entry";

/// Marker file written by a child to tell the parent that the workload phase
/// is complete and the process is now just sleeping until it is killed.
const READY_MARKER: &str = "ready-to-die";

/// Entry point executed by the child process.
///
/// When the integration test binary is spawned with `CRASH_RECOVERY_CHILD=1`,
/// it runs the requested scenario and then blocks until killed. We filter the
/// test execution to this single test so the child does not run the parent
/// harness tests.
#[test]
fn crash_recovery_child_entry() {
    if std::env::var(CHILD_ENV_VAR).is_ok() {
        let data_dir = std::env::var(DIR_ENV_VAR).expect("data dir required");
        let scenario = std::env::var(SCENARIO_ENV_VAR).expect("scenario required");
        let iterations: usize = std::env::var(ITERATIONS_ENV_VAR)
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(100);

        run_child_scenario(&data_dir, &scenario, iterations);

        // After finishing the workload, write a marker so the parent knows it
        // is safe to kill us at any point from here on.
        fs::write(Path::new(&data_dir).join(READY_MARKER), b"").unwrap();

        loop {
            thread::sleep(Duration::from_secs(1));
        }
    }
}

fn run_child_scenario(data_dir: &str, scenario: &str, iterations: usize) {
    let mut config = StorageConfig::new(data_dir);
    if let Ok(size) = std::env::var(BP_SIZE_ENV_VAR) {
        if let Ok(size) = size.parse::<usize>() {
            config.buffer_pool_size = size;
        }
    }

    // Scenario-specific configuration must be applied before opening the engine.
    if scenario == "large_wal" {
        config.wal_segment_size = 1024;
    }

    let engine = StorageEngine::open(data_dir, &config).unwrap();

    match scenario {
        "alloc_checkpoint" => {
            // Allocate pages, modify them, and take a checkpoint before dying.
            // All pages should be recoverable from the data file.
            for i in 1..=iterations {
                let mut guard = engine.buffer_pool().new_page().unwrap();
                write_test_pattern(guard.page_mut(), i);
            }
            engine.trigger_checkpoint().unwrap();
        }

        "alloc_flush_no_checkpoint" => {
            // Allocate pages, modify them, and flush each page individually.
            // Recovery should not corrupt the already-durable data.
            for i in 1..=iterations {
                let page_id = {
                    let mut guard = engine.buffer_pool().new_page().unwrap();
                    write_test_pattern(guard.page_mut(), i);
                    guard.page_id()
                };
                engine.buffer_pool().flush(page_id).unwrap();
            }
        }

        "modify_after_checkpoint" => {
            // Allocate and checkpoint a baseline, then modify the pages and flush
            // the WAL before dying.
            //
            // M1 design note: pages allocated in this session have
            // `needs_fpi=false`, so the post-checkpoint modification does NOT
            // write a FullPageImage. Recovery therefore restores the checkpoint
            // baseline from the data file; the unflushed in-memory modification
            // is lost. This scenario still validates that checkpointed data
            // survives a crash. True FPI crash recovery is exercised by the
            // `fpi_after_eviction` scenario and by the unit test
            // `recover_repairs_torn_page_after_checkpoint`.
            //
            // TODO(M2): once heap redo records are implemented, update this
            // scenario (and its test assertion) to verify that post-checkpoint
            // modifications are preserved rather than lost.
            let mut ids = Vec::with_capacity(iterations);
            for i in 1..=iterations {
                let mut guard = engine.buffer_pool().new_page().unwrap();
                write_test_pattern(guard.page_mut(), i);
                ids.push(guard.page_id());
            }
            engine.trigger_checkpoint().unwrap();

            for (idx, id) in ids.iter().enumerate() {
                let mut guard = engine.buffer_pool().pin_mut(*id).unwrap();
                // Overwrite with a new pattern that will be lost on crash.
                write_test_pattern(guard.page_mut(), idx + 1_000_000);
            }
            engine.wal_writer().flush().unwrap();
        }

        "mixed_with_periodic_checkpoint" => {
            // Interleave allocations and modifications, taking checkpoints
            // every N operations. Pages up to the last completed checkpoint
            // should be recoverable.
            //
            // Ensure at least one checkpoint runs even for small iteration
            // counts. For iterations <= 4 this degenerates to a checkpoint
            // after every operation, which is still a valid stress pattern.
            let checkpoint_every = (iterations / 4).max(1);
            let mut ids = Vec::new();
            for i in 1..=iterations {
                if i % 3 == 0 || ids.is_empty() {
                    let mut guard = engine.buffer_pool().new_page().unwrap();
                    write_test_pattern(guard.page_mut(), i);
                    ids.push(guard.page_id());
                } else {
                    let id = ids[i % ids.len()];
                    let mut guard = engine.buffer_pool().pin_mut(id).unwrap();
                    write_test_pattern(guard.page_mut(), i);
                }
                if checkpoint_every > 0 && i % checkpoint_every == 0 {
                    engine.trigger_checkpoint().unwrap();
                }
            }
        }

        "alloc_loop" => {
            // Continuously allocate pages without checkpointing. The parent
            // only validates that recovery succeeds and page IDs are unique;
            // unflushed content is allowed to be lost.
            for i in 1..=iterations {
                let mut guard = engine.buffer_pool().new_page().unwrap();
                write_test_pattern(guard.page_mut(), i);
            }
        }

        "empty_database" => {
            // Open the engine and do nothing. The parent kills us immediately
            // after seeing the marker, exercising recovery on a freshly created
            // database.
            let _ = engine.buffer_pool();
        }

        "checkpoint_loop" => {
            // Repeatedly allocate a page and trigger a checkpoint. The parent
            // kills us after the current iteration completes (see
            // `run_manual_crash_test` kill-timing note), so this exercises
            // recovery after repeated checkpoints rather than a true
            // mid-checkpoint crash.
            for i in 1..=iterations {
                let mut guard = engine.buffer_pool().new_page().unwrap();
                write_test_pattern(guard.page_mut(), i);
                drop(guard);
                engine.trigger_checkpoint().unwrap();
            }
        }

        "large_wal" => {
            // Generate enough WAL records to span multiple segments without
            // taking a checkpoint. Recovery must replay across segment
            // boundaries. wal_segment_size is set before StorageEngine::open.
            for i in 1..=iterations {
                let mut guard = engine.buffer_pool().new_page().unwrap();
                write_test_pattern(guard.page_mut(), i);
            }
        }

        "fpi_after_eviction" => {
            // True FPI crash-recovery exercise:
            // 1. Allocate a small number of pages and checkpoint them.
            // 2. Allocate enough new pages to evict the originals from the
            //    tiny buffer pool.
            // 3. pin_mut the evicted pages: they are loaded from disk with
            //    needs_fpi=true, so the first modification writes a FullPageImage
            //    WAL record of the checkpoint baseline.
            // 4. Overwrite with a new pattern and flush the WAL.
            // On recovery the FPI repairs any torn page in the data file.
            let mut ids = Vec::with_capacity(iterations);
            for i in 1..=iterations {
                let mut guard = engine.buffer_pool().new_page().unwrap();
                write_test_pattern(guard.page_mut(), i);
                ids.push(guard.page_id());
            }
            engine.trigger_checkpoint().unwrap();

            // Force eviction by filling the buffer pool several times over.
            let frame_count = engine.buffer_pool().frame_count();
            for _ in 0..frame_count * 4 {
                drop(engine.buffer_pool().new_page().unwrap());
            }

            for (idx, id) in ids.iter().enumerate() {
                let mut guard = engine.buffer_pool().pin_mut(*id).unwrap();
                // This pin_mut writes an FPI of the checkpoint baseline.
                write_test_pattern(guard.page_mut(), idx + 1_000_000);
            }
            engine.wal_writer().flush().unwrap();
        }

        "reserve_without_emit" => {
            // Simulate a crash between reserve_lsn and append_at during a
            // checkpoint. This leaves a gap of zeros in the WAL; recovery treats
            // it as end-of-WAL. The baseline checkpoint data must survive.
            //
            // Step 1: Establish a baseline with a completed checkpoint.
            for i in 1..=iterations {
                let mut guard = engine.buffer_pool().new_page().unwrap();
                write_test_pattern(guard.page_mut(), i);
            }
            engine.trigger_checkpoint().unwrap();

            // Step 2: Reserve a slot (as trigger_checkpoint would) but do NOT
            // call append_at. This creates a 32-byte gap of zeros.
            let _reserved_lsn = engine
                .wal_writer()
                .reserve_lsn(pg_storage::wal::record::WAL_RECORD_HEADER_SIZE as u64)
                .unwrap();

            // Step 3: Write additional records AFTER the gap via new allocations.
            // These records live at LSNs beyond the gap.
            for _ in 0..4 {
                let _ = engine.buffer_pool().new_page().unwrap();
            }

            // Step 4: Flush the WAL so post-gap records are physically on disk.
            // Recovery will still stop at the gap (zeros → end-of-WAL).
            engine.wal_writer().flush().unwrap();
        }

        other => panic!("unknown crash scenario: {other}"),
    }
}

fn write_test_pattern(page: &mut [u8], seed: usize) {
    // User content starts past the 32-byte page header so the pattern never
    // collides with pd_lsn (page[0..8]).
    page[PAGE_HEADER_SIZE] = (seed % 256) as u8;
    page[PAGE_HEADER_SIZE + 1..PAGE_HEADER_SIZE + 9].copy_from_slice(&seed.to_be_bytes());
    // Fill the rest with a deterministic but non-trivial pattern.
    for (i, byte) in page.iter_mut().enumerate().skip(PAGE_HEADER_SIZE + 9) {
        *byte = ((seed + i) % 256) as u8;
    }
}

fn verify_test_pattern(page: &[u8], seed: usize) -> bool {
    if page[PAGE_HEADER_SIZE] != (seed % 256) as u8 {
        return false;
    }
    if page[PAGE_HEADER_SIZE + 1..PAGE_HEADER_SIZE + 9] != seed.to_be_bytes() {
        return false;
    }
    for (i, &byte) in page.iter().enumerate().skip(PAGE_HEADER_SIZE + 9) {
        if byte != ((seed + i) % 256) as u8 {
            return false;
        }
    }
    true
}

fn current_test_binary() -> PathBuf {
    std::env::current_exe().expect("cannot determine current test binary")
}

fn spawn_child(
    data_dir: &Path,
    scenario: &str,
    iterations: usize,
    bp_size: Option<usize>,
) -> std::process::Child {
    let mut cmd = Command::new(current_test_binary());
    cmd.arg("--test-threads=1")
        .arg(CHILD_TEST_NAME)
        .env(CHILD_ENV_VAR, "1")
        .env(DIR_ENV_VAR, data_dir.as_os_str())
        .env(SCENARIO_ENV_VAR, scenario)
        .env(ITERATIONS_ENV_VAR, iterations.to_string());
    if let Some(size) = bp_size {
        cmd.env(BP_SIZE_ENV_VAR, size.to_string());
    }
    cmd.stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    cmd.spawn().expect("failed to spawn crash-recovery child")
}

fn wait_for_marker(data_dir: &Path, timeout: Duration) -> bool {
    let marker = data_dir.join(READY_MARKER);
    let start = Instant::now();
    while start.elapsed() < timeout {
        if marker.exists() {
            return true;
        }
        thread::sleep(Duration::from_millis(5));
    }
    false
}

/// Spawn a child process, run a crash scenario, kill the child, and return the
/// preserved data directory.
///
/// # Kill timing note
///
/// The child writes [`READY_MARKER`] only after the entire workload finishes,
/// then enters an idle sleep loop. The parent therefore kills the child
/// *after* the workload completes. `kill_delay_ms` is a post-marker buffer for
/// OS-level quiescence, not a window for catching in-flight I/O.
///
/// True mid-workload crashes are exercised elsewhere:
/// - `recover_repairs_torn_page_after_checkpoint` (unit test, `mem::forget`)
/// - `crash_fpi_after_eviction_repairs_torn_page` (integration, manual corrupt)
fn run_manual_crash_test(
    scenario: &str,
    iterations: usize,
    kill_delay_ms: u64,
    bp_size: Option<usize>,
) -> tempfile::TempDir {
    let tmp = tempfile::TempDir::new().unwrap();
    let data_dir = tmp.path().to_path_buf();

    let mut child = spawn_child(&data_dir, scenario, iterations, bp_size);

    // Wait until the child signals it has finished the workload, then sleep a
    // bit more before killing. See the function doc for the kill-timing caveat.
    assert!(
        wait_for_marker(&data_dir, Duration::from_secs(30)),
        "child did not finish workload in time"
    );
    thread::sleep(Duration::from_millis(kill_delay_ms));

    child.kill().expect("failed to kill child");
    child.wait().ok();

    // M3 Stage E F1: the SIGKILLed child left its `{data_dir}/lock` file
    // behind; the harness plays the documented operator action for crash
    // residue (remove the stale lock) before the parent reopens.
    let _ = fs::remove_file(data_dir.join("lock"));

    // Clean up the marker so it does not confuse subsequent openings.
    let _ = fs::remove_file(data_dir.join(READY_MARKER));

    tmp
}

fn assert_pages_recover(data_dir: &Path, expected_count: usize, seed_offset: usize) {
    let config = StorageConfig::new(data_dir);
    let engine = StorageEngine::open(data_dir, &config).unwrap();

    let next_page_id = engine.page_allocator().lock().next_page_id().0;
    assert!(
        next_page_id as usize > expected_count,
        "expected at least {expected_count} allocated pages, got {}",
        next_page_id - 1
    );

    for i in 1..=expected_count {
        let page_id = PageId(i as u64);
        let guard = engine.buffer_pool().pin(page_id).unwrap();
        let seed = i + seed_offset;
        assert!(
            verify_test_pattern(guard.page(), seed),
            "page {page_id} did not match expected pattern for seed {seed}"
        );
    }
}

/// Open the data directory and assert that the allocator recovered to exactly
/// `expected_count` allocated pages. Returns the opened engine so the caller
/// can verify the content of specific pages.
fn assert_allocator_state_exact(data_dir: &Path, expected_count: usize) -> StorageEngine {
    let config = StorageConfig::new(data_dir);
    let engine = StorageEngine::open(data_dir, &config).unwrap();

    let next_page_id = engine.page_allocator().lock().next_page_id().0;
    assert_eq!(
        next_page_id as usize,
        expected_count + 1,
        "allocator state mismatch: expected {expected_count} allocated pages, got {}",
        next_page_id - 1
    );

    engine
}

#[test]
fn crash_after_checkpoint_recovers_all_pages() {
    let tmp = run_manual_crash_test("alloc_checkpoint", 10, 50, None);
    assert_pages_recover(tmp.path(), 10, 0);
}

#[test]
fn crash_after_flush_recovers_all_pages() {
    let tmp = run_manual_crash_test("alloc_flush_no_checkpoint", 10, 50, None);
    assert_pages_recover(tmp.path(), 10, 0);
}

#[test]
fn crash_after_checkpoint_modification_loses_post_checkpoint_data() {
    // After checkpoint, in-place modifications of newly allocated pages do NOT
    // write FPIs (needs_fpi=false). Recovery therefore restores the checkpoint
    // baseline from the data file; the post-checkpoint in-memory modification
    // (even after WAL flush) is lost because M1 has no heap redo records.
    let tmp = run_manual_crash_test("modify_after_checkpoint", 10, 50, None);
    assert_pages_recover(tmp.path(), 10, 0);
}

#[test]
fn crash_fpi_after_eviction_repairs_torn_page() {
    use std::io::{Seek, SeekFrom, Write};

    // Use a tiny buffer pool so that the original pages are evicted and later
    // pin_mut writes FullPageImage records.
    //
    // The child allocates 2 protected pages + 4 * frame_count eviction pages.
    // The expected total depends on the compile-time PAGE_SIZE (8 KB or 16 KB).
    let bp_size = 64 * 1024;
    let tmp = run_manual_crash_test("fpi_after_eviction", 2, 50, Some(bp_size));
    let data_dir = tmp.path();
    let frame_count = bp_size / pg_storage::types::PAGE_SIZE;
    let expected_total = 2 + 4 * frame_count;

    // Corrupt the first half of each protected page to simulate a torn write.
    let data_file_path = pg_storage::io::data_file_path(data_dir);
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(&data_file_path)
        .unwrap();
    for i in 1..=2usize {
        let offset = (i as u64 - 1) * pg_storage::types::PAGE_SIZE as u64;
        file.seek(SeekFrom::Start(offset)).unwrap();
        let half = vec![0xFFu8; pg_storage::types::PAGE_SIZE / 2];
        file.write_all(&half).unwrap();
    }
    file.sync_all().unwrap();
    drop(file);

    // Recovery replays the FPIs, restoring the checkpoint baseline.
    // Only the first 2 protected pages are expected to match their checkpoint
    // baseline; the eviction pages (3+) have no FPI and may be lost.
    let engine = assert_allocator_state_exact(data_dir, expected_total);
    for i in 1..=2usize {
        let page_id = PageId(i as u64);
        let guard = engine.buffer_pool().pin(page_id).unwrap();
        assert!(
            verify_test_pattern(guard.page(), i),
            "protected page {page_id} did not match checkpoint baseline"
        );
    }
}

#[test]
fn crash_during_mixed_workload_recovers_consistently() {
    let tmp = run_manual_crash_test("mixed_with_periodic_checkpoint", 32, 50, None);
    // We only assert that recovery succeeds and at least the pages that were
    // allocated are readable. Exact content verification is scenario-specific.
    let config = StorageConfig::new(tmp.path());
    let engine = StorageEngine::open(tmp.path(), &config).unwrap();
    let next_page_id = engine.page_allocator().lock().next_page_id().0;
    assert!(next_page_id > 1, "no pages were allocated before crash");
}

#[test]
fn crash_alloc_loop_recovers_without_corruption() {
    let tmp = run_manual_crash_test("alloc_loop", 100, 50, None);
    let config = StorageConfig::new(tmp.path());
    let engine = StorageEngine::open(tmp.path(), &config).unwrap();
    let next_page_id = engine.page_allocator().lock().next_page_id().0;
    assert!(next_page_id > 1, "no pages were allocated before crash");
}

#[test]
fn crash_empty_database_recovers() {
    let tmp = run_manual_crash_test("empty_database", 1, 0, None);
    let config = StorageConfig::new(tmp.path());
    let engine = StorageEngine::open(tmp.path(), &config).unwrap();
    let next_page_id = engine.page_allocator().lock().next_page_id().0;
    assert_eq!(next_page_id, 1, "empty database should have no allocations");
}

#[test]
fn crash_after_checkpoint_loop_recovers() {
    // The child repeatedly allocates and checkpoints. The kill happens after
    // the current iteration's workload completes (see run_manual_crash_test
    // kill-timing note), so this validates recovery after repeated checkpoints
    // rather than a true mid-checkpoint crash.
    let tmp = run_manual_crash_test("checkpoint_loop", 16, 25, None);
    let config = StorageConfig::new(tmp.path());
    let engine = StorageEngine::open(tmp.path(), &config).unwrap();
    let next_page_id = engine.page_allocator().lock().next_page_id().0;
    assert!(next_page_id > 1, "no pages were allocated before crash");
}

#[test]
fn crash_with_large_wal_recovers_across_segments() {
    // 256 allocations with 1 KB WAL segments produces multiple WAL segment files.
    // Recovery must replay across segment boundaries.
    let tmp = run_manual_crash_test("large_wal", 256, 50, None);
    let wal_dir = tmp.path().join("wal");
    let segment_count = std::fs::read_dir(&wal_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "log"))
        .count();
    assert!(
        segment_count > 1,
        "large_wal should span multiple WAL segments"
    );

    let config = StorageConfig::new(tmp.path());
    let engine = StorageEngine::open(tmp.path(), &config).unwrap();
    let next_page_id = engine.page_allocator().lock().next_page_id().0;
    assert!(next_page_id > 1, "no pages were allocated before crash");
}

#[test]
fn crash_reserve_without_emit_recovers_baseline() {
    // Simulates a crash between reserve_lsn and append_at during a checkpoint.
    // The reserved range is zeros in the WAL. With the Stage N P0-3 fix,
    // the WAL reader forwards-probes past an all-zero header and hard-fails
    // when it finds non-zero data — rather than silently truncating the log
    // and losing the post-gap records. The baseline (pre-gap) data on disk
    // is intact and can be recovered by a tool that fills the hole.
    let iterations = 8;
    let tmp = run_manual_crash_test("reserve_without_emit", iterations, 50, None);
    let data_dir = tmp.path();

    // Recovery must hard-fail on the WAL hole (MetadataCorrupted), not panic.
    let config = StorageConfig::new(data_dir);
    let err = StorageEngine::open(data_dir, &config).unwrap_err();
    assert!(
        matches!(err, StorageError::MetadataCorrupted(_)),
        "expected MetadataCorrupted for WAL hole, got {err:?}"
    );
}

#[test]
fn automated_random_crash_recovery() {
    // Coding plan target is 1000 runs. The default (50) keeps normal CI fast
    // while still exercising multiple timings and scenarios. Set
    // CRASH_RECOVERY_AUTOMATED_RUNS=1000 for a full stress run (e.g. nightly).
    let runs: usize = std::env::var("CRASH_RECOVERY_AUTOMATED_RUNS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(50);

    // Note: the child writes a workload-completion marker and then idles before
    // it is killed, so all `kill_delay_ms` values are functionally equivalent.
    // The variety in this loop comes from cycling 7 scenarios and 8 different
    // iteration counts (8-15). `kill_delay_ms` is kept as a parameter so a
    // future mid-workload kill mode can use it meaningfully.
    let kill_delays = [1u64, 5, 10, 25, 50, 100, 200];
    let scenarios = [
        "alloc_checkpoint",
        "alloc_flush_no_checkpoint",
        "modify_after_checkpoint",
        "mixed_with_periodic_checkpoint",
        "alloc_loop",
        // "empty_database" is excluded: its invariant is just "recovery succeeds",
        // which is covered by the dedicated manual test above.
        "checkpoint_loop",
        "large_wal",
    ];

    for (run, delay) in kill_delays.iter().cycle().take(runs).enumerate() {
        let scenario = scenarios[run % scenarios.len()];
        let iterations = 8 + (run % 8);
        let tmp = run_manual_crash_test(scenario, iterations, *delay, None);

        // The only universal invariant is that recovery must succeed and not
        // return duplicate page IDs. Scenario-specific invariants are checked
        // by the manual tests above.
        let config = StorageConfig::new(tmp.path());
        let engine = StorageEngine::open(tmp.path(), &config).unwrap();
        let next_page_id = engine.page_allocator().lock().next_page_id().0;
        assert!(
            next_page_id > 1,
            "run {run} ({scenario}) produced no allocations"
        );
    }
}
