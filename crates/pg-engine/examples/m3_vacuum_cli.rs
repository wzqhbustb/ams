//! Manual offline-vacuum runner (M3 Stage D `Engine::vacuum`).
//!
//! The SQL layer deliberately exposes no `VACUUM` command (minimal parser);
//! vacuum is a typed-API maintenance operation. This example is the manual
//! entry point:
//!
//! ```sh
//! cargo run -p pg-engine --example m3_vacuum_cli -- <data-dir> <table>
//! ```
//!
//! OFFLINE MODEL: the pass takes `AccessExclusive` on the table, and the
//! `DataDirLock` means the data directory must not be held by a running
//! `pg-server` — stop the server first, vacuum, then restart it.

use std::path::PathBuf;
use std::process::ExitCode;

use pg_engine::{Engine, EngineConfig};

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let (dir, table) = match (args.next(), args.next()) {
        (Some(dir), Some(table)) => (PathBuf::from(dir), table),
        _ => {
            eprintln!("usage: m3_vacuum_cli <data-dir> <table>");
            return ExitCode::from(2);
        }
    };

    let engine = match Engine::open(&dir, EngineConfig::new(&dir)) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("m3_vacuum_cli: engine open failed: {e}");
            return ExitCode::FAILURE;
        }
    };
    match engine.vacuum(&table) {
        Ok(stats) => println!("vacuum {table}: {stats:?}"),
        Err(e) => {
            eprintln!("m3_vacuum_cli: vacuum {table} failed: {e}");
            return ExitCode::FAILURE;
        }
    }
    engine.shutdown();
    ExitCode::SUCCESS
}
