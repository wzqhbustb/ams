//! M3 Stage E review F1: exclusive data-directory lock acceptance.
//!
//! Covered:
//!
//! - a lock file held by a FOREIGN pid (a second process — the
//!   pg-diag-vs-running-server hazard) makes `StorageEngine::open` fail
//!   cleanly with an actionable "already in use" error; removing the stale
//!   file unblocks the open (the documented first-version limitation: no
//!   kill(pid,0) liveness check under the zero-new-dependency rule);
//! - a clean close releases the lock and reopening works;
//! - the in-process crash-test idiom (`mem::forget` + reopen, same pid)
//!   keeps working — the stale same-pid lock is reclaimed (see the
//!   `data_dir_lock` module docs for why same-pid conflicts are reclaimed
//!   rather than rejected).
//!
//! The genuine cross-process double-open case is covered end-to-end at the
//! engine level: `pg-engine/tests/m3_diag_cli.rs` runs the `pg-diag` binary
//! against a live engine's data directory.
//!
//! Acceptance: `cargo test -p pg-storage --test data_dir_lock`

use pg_storage::config::StorageConfig;
use pg_storage::engine::StorageEngine;
use pg_storage::error::StorageError;
use tempfile::TempDir;

fn open(tmp: &TempDir) -> pg_storage::error::Result<StorageEngine> {
    StorageEngine::open(tmp.path(), &StorageConfig::new(tmp.path()))
}

#[test]
fn foreign_pid_lock_is_rejected_with_instructions() {
    let tmp = TempDir::new().unwrap();
    // Fabricate a live foreign holder (a real second process is covered by
    // the pg-diag end-to-end test in pg-engine).
    std::fs::write(
        tmp.path().join("lock"),
        format!("pid={}\n", std::process::id() + 1_000_000),
    )
    .unwrap();
    let err = open(&tmp).unwrap_err();
    match err {
        StorageError::InvalidOperation(msg) => {
            assert!(msg.contains("already in use"), "{msg}");
            assert!(msg.contains("remove the stale lock file"), "{msg}");
        }
        other => panic!("expected InvalidOperation, got {other:?}"),
    }

    // Manual cleanup (the documented operator action) unblocks the open.
    std::fs::remove_file(tmp.path().join("lock")).unwrap();
    let _engine = open(&tmp).unwrap();
}

#[test]
fn clean_close_releases_lock_for_reopen() {
    let tmp = TempDir::new().unwrap();
    let first = open(&tmp).unwrap();
    drop(first);
    assert!(!tmp.path().join("lock").exists());
    let _second = open(&tmp).unwrap();
}

#[test]
fn forget_then_reopen_reclaims_same_pid_lock() {
    // The suite's kill -9 idiom (~100 crash-recovery tests): forget the
    // engine (no Drop, lock file left behind) and reopen in the SAME
    // process. The recorded pid is our own, so the lock is reclaimed.
    let tmp = TempDir::new().unwrap();
    let engine = open(&tmp).unwrap();
    std::mem::forget(engine);
    assert!(tmp.path().join("lock").exists());
    let _reopened = open(&tmp).unwrap();
}
