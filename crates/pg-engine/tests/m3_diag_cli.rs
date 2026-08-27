//! M3 Stage E (coding-plan S1; ROADMAP.md:217) acceptance: `pg-diag` —
//! the command-line diagnostics exit over the §6.2 introspection APIs.
//!
//! Covered:
//!
//! - fixture-based output: with a real active transaction + lock wait in
//!   process, `diag::txn_report` / `diag::locks_report` (the exact text the
//!   `pg-diag` binary prints) contain the expected XIDs, horizon, wait-for
//!   edge, and per-table granted/waiter state;
//! - CLI smoke: the compiled `pg-diag` binary runs both subcommands against
//!   a data directory and exits 0. Because the binary opens its OWN engine
//!   on the directory, it reports that process's freshly recovered, idle
//!   state — the documented M3 single-process boundary (live cross-process
//!   diagnostics is Phase 4a via pg-wire).
//!
//! Acceptance: `cargo test -p pg-engine --test m3_diag_cli`

use std::process::Command;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use pg_engine::{diag, Engine, EngineConfig};
use pg_txn::LockMode;
use tempfile::TempDir;

/// The compiled binary under test (same package, so cargo provides it).
const PG_DIAG: &str = env!("CARGO_BIN_EXE_pg-diag");

/// Watchdog budget for the polling loop (a regression FAILS, never hangs).
const WATCHDOG: Duration = Duration::from_secs(120);

fn wait_until(cond: impl Fn() -> bool, what: &str) {
    let deadline = Instant::now() + WATCHDOG;
    while !cond() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn txn_and_locks_reports_show_live_fixture() {
    let tmp = TempDir::new().unwrap();
    let engine = Arc::new(Engine::open(tmp.path(), EngineConfig::new(tmp.path())).unwrap());
    engine.exec(None, "CREATE TABLE t (id INT)").unwrap();
    let oid = engine.describe_table("t").unwrap().oid;

    let txn_a = engine.begin_txn().unwrap();
    let xid_a = txn_a.xid();
    engine
        .exec(Some(&txn_a), "INSERT INTO t VALUES (1)")
        .unwrap();

    let worker = {
        let engine = Arc::clone(&engine);
        thread::spawn(move || {
            let txn_b = engine.begin_txn().unwrap();
            let xid_b = txn_b.xid();
            engine
                .lock_manager()
                .acquire(xid_b, oid, LockMode::AccessExclusive)
                .expect("acquire after holder commits");
            txn_b.abort().unwrap();
            xid_b
        })
    };
    wait_until(
        || {
            engine
                .table_lock_state(oid)
                .is_some_and(|s| !s.waiters.is_empty())
        },
        "txn_b to queue for AccessExclusive",
    );
    let xid_b = engine.table_lock_state(oid).unwrap().waiters[0].0;

    // `txn` report: both active XIDs, the horizon pinned at xid_a, and the
    // two hit-rate lines.
    let report = diag::txn_report(&engine);
    assert!(
        report.contains(&format!("active_xids=[{}, {}]", xid_a.0, xid_b.0)),
        "{report}"
    );
    assert!(
        report.contains(&format!("oldest_snapshot_xmin={}", xid_a.0)),
        "{report}"
    );
    assert!(report.contains("clog_hit_rate="), "{report}");
    assert!(report.contains("buffer_pool_hit_rate="), "{report}");

    // `locks` report: the wait-for edge and the contended table's state.
    let report = diag::locks_report(&engine);
    assert!(
        report.contains(&format!("waiter={} -> holder={}", xid_b.0, xid_a.0)),
        "{report}"
    );
    assert!(report.contains(&format!("table={}", oid.0)), "{report}");
    assert!(
        report.contains(&format!("granted=[{}:RowExclusive]", xid_a.0)),
        "{report}"
    );
    assert!(
        report.contains(&format!("waiters=[{}:AccessExclusive]", xid_b.0)),
        "{report}"
    );

    // Cleanup: commit the holder, the worker acquires and aborts.
    txn_a.commit().unwrap();
    worker.join().expect("worker panicked");

    // Quiescent engine: empty graph, no contended tables, empty active set.
    let report = diag::locks_report(&engine);
    assert!(report.contains("wait_for_edges=[]"), "{report}");
    assert!(report.contains("table_lock_states=[]"), "{report}");
    let report = diag::txn_report(&engine);
    assert!(report.contains("active_xids=[]"), "{report}");
}

/// CLI smoke: the binary runs both subcommands and exits 0. The directory
/// is fresh (created by the open), so the report shows the tool's own idle
/// engine — the M3 single-process boundary, asserted explicitly.
#[test]
fn cli_runs_both_subcommands() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path().join("db");

    let out = Command::new(PG_DIAG)
        .args(["--data-dir"])
        .arg(&dir)
        .arg("txn")
        .output()
        .expect("failed to spawn pg-diag");
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8(out.stdout).unwrap();
    assert!(stdout.contains("active_xids=[]"), "{stdout}");
    assert!(stdout.contains("oldest_snapshot_xmin="), "{stdout}");
    assert!(stdout.contains("clog_hit_rate="), "{stdout}");

    let out = Command::new(PG_DIAG)
        .args(["--data-dir"])
        .arg(&dir)
        .arg("locks")
        .output()
        .expect("failed to spawn pg-diag");
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8(out.stdout).unwrap();
    assert!(stdout.contains("wait_for_edges=[]"), "{stdout}");
    assert!(stdout.contains("table_lock_states=[]"), "{stdout}");

    // Usage error: no subcommand → non-zero exit.
    let out = Command::new(PG_DIAG)
        .arg("--data-dir")
        .arg(&dir)
        .output()
        .unwrap();
    assert!(!out.status.success());
}

/// F1 (Stage E review): a second PROCESS opening a live engine's data
/// directory fails cleanly — `pg-diag` pointed at a running server's
/// directory exits non-zero with "already in use", instead of two engines
/// reading and writing the same files uncoordinated. After the holder
/// closes, the directory opens normally (clean close releases the lock).
#[test]
fn pg_diag_against_live_engine_dir_is_rejected() {
    let tmp = TempDir::new().unwrap();
    let engine = Engine::open(tmp.path(), EngineConfig::new(tmp.path())).unwrap();

    let out = Command::new(PG_DIAG)
        .arg("--data-dir")
        .arg(tmp.path())
        .arg("txn")
        .output()
        .expect("failed to spawn pg-diag");
    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("already in use"), "{stderr}");

    drop(engine);
    let out = Command::new(PG_DIAG)
        .arg("--data-dir")
        .arg(tmp.path())
        .arg("txn")
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}
