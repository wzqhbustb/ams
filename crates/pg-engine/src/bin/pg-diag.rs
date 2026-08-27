//! `pg-diag` — command-line engine diagnostics (M3 Stage E, coding-plan S1;
//! ROADMAP.md:217 "可通过命令行工具诊断事务和锁问题").
//!
//! ```text
//! pg-diag [--data-dir <DIR>] <txn|locks>
//! ```
//!
//! - `txn`: active XIDs, the vacuum horizon (`oldest_snapshot_xmin`), and
//!   the CLOG / buffer-pool hit rates.
//! - `locks`: the wait-for graph (row-lock + table-lock edges, the same
//!   composition the deadlock detector consumes) and every contended
//!   table's granted set / FIFO wait queue.
//!
//! All output is assembled from the §6.2 introspection APIs; the rendering
//! lives in [`pg_engine::diag`] so tests can assert the exact text
//! in-process.
//!
//! # M3 boundary — single-process diagnostics
//!
//! The tool opens its OWN [`Engine`] on the data directory, so it reports
//! that process's (freshly recovered, idle) state: a separate `pg-diag`
//! process cannot see a running server's live transactions or lock waits.
//! Pointing it at a data directory a live server has open fails cleanly
//! with "data directory already in use" — the storage engine takes an
//! exclusive `lock` file at open (M3 Stage E review F1; a stale lock left
//! by a crashed process must be removed by hand, which the error message
//! states). Live cross-process diagnostics is Phase 4a, exposing the same
//! §6.2 surface through pg-wire (coding-plan "遗留与归队", S1).
//!
//! Note: opening a directory whose previous engine did NOT shut down
//! cleanly triggers full crash recovery (loser undo, checkpoint) — a WRITE
//! side effect. Diagnostics are read-only in intent but not in mechanism;
//! a read-only mode is Phase 4a scope.

use std::path::PathBuf;
use std::process::ExitCode;

use pg_engine::{diag, Engine, EngineConfig};

fn main() -> ExitCode {
    match run() {
        Ok(report) => {
            print!("{report}");
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("pg-diag: error: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<String, String> {
    let mut data_dir = PathBuf::from("data");
    let mut subcommand = None;
    let mut argv = std::env::args().skip(1);
    while let Some(arg) = argv.next() {
        match arg.as_str() {
            "--data-dir" => {
                let value = argv
                    .next()
                    .ok_or_else(|| "--data-dir requires a value".to_string())?;
                data_dir = PathBuf::from(value);
            }
            "-h" | "--help" => return Err(usage()),
            _ if arg.starts_with('-') => {
                return Err(format!("unknown option {arg}\n{}", usage()));
            }
            "txn" | "locks" => {
                if subcommand.is_some() {
                    return Err(format!("multiple subcommands\n{}", usage()));
                }
                subcommand = Some(arg);
            }
            _ => return Err(format!("unknown argument {arg:?}\n{}", usage())),
        }
    }
    let subcommand = subcommand.ok_or_else(usage)?;

    let engine = Engine::open(&data_dir, EngineConfig::new(&data_dir))
        .map_err(|e| format!("cannot open {}: {e}", data_dir.display()))?;
    match subcommand.as_str() {
        "txn" => Ok(diag::txn_report(&engine)),
        "locks" => Ok(diag::locks_report(&engine)),
        _ => unreachable!("subcommand is validated above"),
    }
}

fn usage() -> String {
    "usage: pg-diag [--data-dir <DIR>] <txn|locks>".to_string()
}
