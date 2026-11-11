//! pg-wire — PostgreSQL v3 wire protocol server (Phase 4a Stage C).
//!
//! Scope: protocol codec + connection management + SQL passthrough — no
//! execution logic lives here. Statements use `Engine::exec` or
//! `Engine::exec_with_params`; `BEGIN`/`COMMIT`/`ROLLBACK` are intercepted
//! and mapped to the engine's programmatic transaction API.
//!
//! Protocol coverage:
//!
//! - startup negotiation and trust-mode AuthenticationOk;
//! - Simple Query (`'Q'`) with ordered multi-statement strings;
//! - Extended Query Parse/Bind/Describe/Execute/Close/Flush/Sync, including
//!   named and unnamed statement/portal lifecycles and error-until-Sync
//!   recovery;
//! - RowDescription + text or Bind-selected binary DataRow values;
//! - errors → ErrorResponse, with ReadyForQuery at Simple Query/Sync
//!   boundaries; Terminate (`'X'`) closes cleanly.
//!
//! Non-goals: COPY, authentication, CancelRequest handling, TLS, and the
//! complete PostgreSQL type system (Phase 6).
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
