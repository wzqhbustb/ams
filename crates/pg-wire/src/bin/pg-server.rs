//! Minimal fixed-port wire server for the manual client matrix
//! (tech-selection §7.4 / coding-plan N6): psql / psycopg2 / node-postgres
//! runs need a server on a known address; the CI gate (`wire_clients.rs`)
//! binds ephemeral ports instead.
//!
//! Usage: `pg-server <addr> <data-dir>` — e.g.
//! `pg-server 127.0.0.1:55432 /tmp/pg_rust_matrix`.

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use pg_engine::{Engine, EngineConfig};
use pg_wire::Server;

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let (addr, dir) = match (args.next(), args.next()) {
        (Some(addr), Some(dir)) => (addr, PathBuf::from(dir)),
        _ => {
            eprintln!("usage: pg-server <addr> <data-dir>");
            return ExitCode::from(2);
        }
    };

    let engine = match Engine::open(&dir, EngineConfig::new(&dir)) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("pg-server: engine open failed: {e}");
            return ExitCode::FAILURE;
        }
    };
    let server = match Server::bind(&addr, Arc::new(engine)) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("pg-server: bind {addr} failed: {e}");
            return ExitCode::FAILURE;
        }
    };
    match server.local_addr() {
        Ok(a) => eprintln!("pg-server: listening on {a} (data dir {})", dir.display()),
        Err(e) => eprintln!("pg-server: listening on {addr} (local_addr failed: {e})"),
    }
    if let Err(e) = server.serve() {
        eprintln!("pg-server: accept loop failed: {e}");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}
