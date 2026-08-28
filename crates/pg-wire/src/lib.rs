//! pg-wire — minimal PostgreSQL v3 wire protocol server (Phase 1 M3 Stage F).
//!
//! Scope (tech-selection §7.1): protocol codec + connection management +
//! SQL passthrough ONLY — no execution logic lives here. Statements are
//! handed to [`pg_engine::Engine::exec`]; `BEGIN`/`COMMIT`/`ROLLBACK` are intercepted
//! at this layer and mapped to the engine's programmatic transaction API
//! (`Engine::begin_txn` / `TxnHandle::commit` / `TxnHandle::abort`), which
//! is the only way the engine accepts transaction control.
//!
//! Protocol coverage (§7.2 minimal closed set):
//!
//! - startup: StartupMessage → AuthenticationOk (trust) → ParameterStatus
//!   (minimal set) → ReadyForQuery; SSLRequest → `'N'`, GSSENCRequest →
//!   `'N'`, CancelRequest → close;
//! - Simple Query (`'Q'`) only — multi-statement strings execute in order,
//!   the first error aborts the rest of the string but not the connection;
//! - results: RowDescription + text-format DataRow + CommandComplete
//!   (`SELECT n` / `INSERT 0 n` / `UPDATE n` / `DELETE n` tags);
//! - errors → ErrorResponse + ReadyForQuery; Terminate (`'X'`) closes
//!   cleanly.
//!
//! Non-goals (§7.2): Extended Query, COPY, authentication, CancelRequest
//! handling, TLS.
//!
//! Threading (§7.3): `std::net::TcpListener`, one std thread per
//! connection, `Arc<Engine>` shared across all of them. Each connection
//! thread exclusively owns its [`pg_engine::TxnHandle`] (`!Sync` by way of
//! its `RefCell<Snapshot>` — the model fits exactly).

#![warn(missing_docs)]
#![warn(rust_2018_idioms)]

pub mod codec;
pub mod error;
mod server;
mod session;
pub mod types;

pub use error::{Result, WireError};
pub use server::Server;
pub use session::{split_statements, Session};

// O5 (tech-selection §7.3): pin `Engine: Send + Sync` as a compile-time
// property instead of relying on field-structure deduction — the
// thread-per-connection model shares one `Arc<Engine>` across every
// connection thread and is unsound the day that bound stops holding. Bare
// fn, no `static_assertions` crate (zero new runtime deps, §10).
#[allow(dead_code)]
fn assert_engine_send_sync() {
    fn require_send_sync<T: Send + Sync>() {}
    require_send_sync::<pg_engine::Engine>();
}
