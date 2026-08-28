//! Per-connection session state machine (§7.2/§7.3).
//!
//! A [`Session`] owns the connection's only [`TxnHandle`] (`!Sync` — one
//! handle per connection thread, tech-selection §7.3) and implements:
//!
//! - **Startup negotiation**: SSLRequest/GSSENCRequest → `'N'` (a GSS-less
//!   server must still answer — libpq blocks on the byte; "ignore" in §7.2
//!   means "no encryption", not "no reply"), CancelRequest → close
//!   (unsupported, §7.2 非目标), v3.0 StartupMessage → AuthenticationOk +
//!   minimal ParameterStatus set + ReadyForQuery (trust, no auth).
//! - **Simple Query only**: `'Q'` strings are split into statements and
//!   executed in order; the first error aborts the rest of the string
//!   (PostgreSQL simple-protocol semantics) but never the connection.
//! - **Transaction interception**: `BEGIN`/`COMMIT`/`ROLLBACK` never reach
//!   [`Engine::exec`] (the engine hard-rejects them — transaction control
//!   is a programmatic API). They map to `Engine::begin_txn` /
//!   `TxnHandle::commit` / `TxnHandle::abort` instead. At most one
//!   `TxnHandle` per connection; `BEGIN` inside a transaction errors
//!   (SQLSTATE 25001) without disturbing the live handle.
//!
//! The statement-dispatch core ([`Session::handle_query`]) is transport-
//! agnostic — it appends response messages to a caller buffer — so the
//! codec/session logic is unit-testable without sockets.

use std::io::Write;
use std::net::TcpStream;
use std::sync::Arc;

use pg_engine::sql::Statement;
use pg_engine::{Engine, EngineError, QueryResult, TxnHandle, Value};

use crate::codec::{self, FieldDesc, FrontendMessage, StartupPacket};
use crate::error::{Result, WireError};
use crate::types::{column_type_of, encode_text, wire_type};

/// `(SQLSTATE, message)` pair for one failed statement.
struct StatementError(&'static str, String);

impl StatementError {
    fn engine(err: &EngineError) -> Self {
        Self(WireError::sqlstate_of(err), err.to_string())
    }
}

/// Split a simple-query string into statements on top-level `';'`,
/// honoring single-quoted literals (`''` is an escaped quote).
///
/// The engine parser itself accepts only one statement per parse, so the
/// wire layer owns multi-statement splitting (§7.2 "多语句串按序执行").
/// Comment syntax does not exist in the engine's SQL subset, so no comment
/// skipping is needed here either.
pub fn split_statements(sql: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut in_quote = false;
    let mut chars = sql.char_indices().peekable();
    while let Some((i, c)) = chars.next() {
        match c {
            '\'' if in_quote => {
                // A doubled quote is an escaped quote, not a terminator.
                if chars.peek().map(|&(_, c)| c) == Some('\'') {
                    chars.next();
                } else {
                    in_quote = false;
                }
            }
            '\'' => in_quote = true,
            ';' if !in_quote => {
                parts.push(&sql[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    parts.push(&sql[start..]);
    parts
}

/// Per-connection state: the shared engine plus this connection's
/// (at most one) explicit transaction handle.
pub struct Session {
    engine: Arc<Engine>,
    txn: Option<TxnHandle>,
}

impl Session {
    /// A fresh session over `engine` (no transaction in progress).
    pub fn new(engine: Arc<Engine>) -> Self {
        Self { engine, txn: None }
    }

    /// The `ReadyForQuery` status byte for the current state.
    fn status_byte(&self) -> u8 {
        if self.txn.is_some() {
            codec::STATUS_IN_TXN
        } else {
            codec::STATUS_IDLE
        }
    }

    /// Serve a connected socket until Terminate, clean EOF, or a protocol
    /// violation. Dropping `self` auto-aborts any live transaction
    /// (`TxnHandle::drop`), so a hangup mid-transaction never leaks an XID.
    pub fn run(&mut self, mut stream: &TcpStream) -> Result<()> {
        // ── Startup phase ────────────────────────────────────────────
        let params = loop {
            match codec::read_startup_packet(&mut stream)? {
                // Client connected and went away without a startup packet.
                None => return Ok(()),
                Some(StartupPacket::SslRequest) | Some(StartupPacket::GssEncRequest) => {
                    stream.write_all(b"N")?;
                }
                // CancelRequest is a §7.2 non-goal; real PG closes the
                // cancel connection after consuming it too.
                Some(StartupPacket::CancelRequest { .. }) => return Ok(()),
                Some(StartupPacket::Startup { params }) => break params,
            }
        };

        let mut out = Vec::new();
        codec::authentication_ok(&mut out);
        // Minimal ParameterStatus set (§7.2). `server_version` is what psql
        // and drivers key compatibility off; the rest silence common probes.
        let get = |name: &str| {
            params
                .iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.as_str())
        };
        let statuses: [(&str, &str); 8] = [
            ("server_version", "16.0"),
            ("server_encoding", "UTF8"),
            ("client_encoding", "UTF8"),
            ("DateStyle", "ISO, MDY"),
            ("integer_datetimes", "on"),
            ("standard_conforming_strings", "on"),
            ("is_superuser", "on"),
            ("session_authorization", get("user").unwrap_or("pg_rust")),
        ];
        for (name, value) in statuses {
            codec::parameter_status(&mut out, name, value);
        }
        if let Some(app) = get("application_name") {
            codec::parameter_status(&mut out, "application_name", app);
        }
        codec::ready_for_query(&mut out, codec::STATUS_IDLE);
        stream.write_all(&out)?;

        // ── Regular phase ────────────────────────────────────────────
        loop {
            match codec::read_message(&mut stream)? {
                None | Some(FrontendMessage::Terminate) => return Ok(()),
                Some(FrontendMessage::Query(sql)) => {
                    let mut out = Vec::new();
                    self.handle_query(&sql, &mut out);
                    stream.write_all(&out)?;
                }
                Some(FrontendMessage::Unsupported { tag }) => {
                    let mut out = Vec::new();
                    codec::error_response(
                        &mut out,
                        "ERROR",
                        "0A000",
                        &format!(
                            "frontend message type {} is not supported (simple query protocol only)",
                            tag as char
                        ),
                    );
                    codec::ready_for_query(&mut out, self.status_byte());
                    stream.write_all(&out)?;
                }
            }
        }
    }

    /// Execute one simple-query string (possibly multi-statement),
    /// appending every response message — including the trailing
    /// `ReadyForQuery` — to `out`.
    ///
    /// Statement errors abort the remainder of the string (PostgreSQL
    /// simple-protocol semantics) but leave the connection — and any
    /// explicit transaction — alive.
    pub fn handle_query(&mut self, sql: &str, out: &mut Vec<u8>) {
        let mut any = false;
        for piece in split_statements(sql) {
            let piece = piece.trim();
            if piece.is_empty() {
                continue;
            }
            any = true;
            if let Err(StatementError(sqlstate, message)) = self.handle_statement(piece, out) {
                codec::error_response(out, "ERROR", sqlstate, &message);
                break;
            }
        }
        if !any {
            codec::empty_query_response(out);
        }
        codec::ready_for_query(out, self.status_byte());
    }

    /// Dispatch one already-split statement.
    fn handle_statement(
        &mut self,
        sql: &str,
        out: &mut Vec<u8>,
    ) -> std::result::Result<(), StatementError> {
        let stmt = pg_engine::sql::parse(sql).map_err(|e| StatementError::engine(&e))?;
        match stmt {
            // ── Wire-level transaction interception (§7.2) ──────────
            Statement::Begin => {
                if self.txn.is_some() {
                    // PG issues a WARNING and continues; a hard error is
                    // equally within "报错不破坏现状" and simpler for
                    // clients to observe — the live handle is untouched.
                    return Err(StatementError(
                        "25001",
                        "there is already a transaction in progress".to_string(),
                    ));
                }
                match self.engine.begin_txn() {
                    Ok(handle) => {
                        self.txn = Some(handle);
                        codec::command_complete(out, "BEGIN");
                    }
                    Err(e) => return Err(StatementError::engine(&e)),
                }
            }
            Statement::Commit => match self.txn.take() {
                None => {
                    return Err(StatementError(
                        "25P01",
                        "there is no transaction in progress".to_string(),
                    ));
                }
                Some(handle) => match handle.commit() {
                    Ok(()) => codec::command_complete(out, "COMMIT"),
                    Err(e) => return Err(StatementError::engine(&e)),
                },
            },
            Statement::Rollback => match self.txn.take() {
                None => {
                    return Err(StatementError(
                        "25P01",
                        "there is no transaction in progress".to_string(),
                    ));
                }
                Some(handle) => match handle.abort() {
                    Ok(()) => codec::command_complete(out, "ROLLBACK"),
                    Err(e) => return Err(StatementError::engine(&e)),
                },
            },
            // ── SQL passthrough ─────────────────────────────────────
            _ => {
                let result = self
                    .engine
                    .exec(self.txn.as_ref(), sql)
                    .map_err(|e| StatementError::engine(&e))?;
                self.write_result(&stmt, result, out)?;
            }
        }
        Ok(())
    }

    /// Render an engine result as RowDescription + DataRows +
    /// CommandComplete with the correct tag (§7.2).
    fn write_result(
        &self,
        stmt: &Statement,
        result: QueryResult,
        out: &mut Vec<u8>,
    ) -> std::result::Result<(), StatementError> {
        match result {
            QueryResult::Rows { columns, rows } => {
                let fields = self.field_descs(stmt, &columns, &rows);
                codec::row_description(out, &fields);
                for row in &rows {
                    let mut values: Vec<Option<Vec<u8>>> = Vec::with_capacity(row.len());
                    for v in row {
                        values.push(match v {
                            None => None,
                            Some(d) => Some(
                                encode_text(d)
                                    .map_err(|e| StatementError("XX000", e.to_string()))?,
                            ),
                        });
                    }
                    codec::data_row(
                        out,
                        &values.iter().map(|v| v.as_deref()).collect::<Vec<_>>(),
                    );
                }
                codec::command_complete(out, &format!("SELECT {}", rows.len()));
            }
            QueryResult::Affected(n) => {
                let tag = match stmt {
                    Statement::Insert { .. } => format!("INSERT 0 {n}"),
                    Statement::Update { .. } => format!("UPDATE {n}"),
                    Statement::Delete { .. } => format!("DELETE {n}"),
                    // Affected only comes from DML; keep a sane fallback.
                    _ => format!("OK {n}"),
                };
                codec::command_complete(out, &tag);
            }
            QueryResult::Ok => {
                let tag = match stmt {
                    Statement::CreateTable { .. } => "CREATE TABLE",
                    Statement::CreateIndex { .. } => "CREATE INDEX",
                    _ => "OK",
                };
                codec::command_complete(out, tag);
            }
        }
        Ok(())
    }

    /// Build RowDescription fields: schema-driven typing via
    /// [`Engine::describe_table`] (primary), value-inferred typing as a
    /// fallback for columns the schema lookup can no longer resolve (table
    /// dropped between parse and exec), TEXT as the last resort.
    fn field_descs(
        &self,
        stmt: &Statement,
        columns: &[String],
        rows: &[Vec<Value>],
    ) -> Vec<FieldDesc> {
        let schema = match stmt {
            Statement::Select { table, .. } => self.engine.describe_table(table),
            _ => None,
        };
        columns
            .iter()
            .enumerate()
            .map(|(i, name)| {
                let resolved = schema.as_ref().and_then(|entry| {
                    entry
                        .columns
                        .iter()
                        .enumerate()
                        .find(|(_, c)| &c.name == name)
                        .map(|(attnum, c)| (entry.oid.0 as u32, attnum as i16 + 1, c.col_type))
                });
                let (table_oid, attnum, ty) = match resolved {
                    Some((oid, attnum, ty)) => (oid, attnum, ty),
                    None => {
                        let inferred = rows
                            .iter()
                            .find_map(|row| row.get(i)?.as_ref())
                            .map(column_type_of)
                            .unwrap_or(pg_engine::ColumnType::Text);
                        (0, 0, inferred)
                    }
                };
                let wt = wire_type(ty);
                FieldDesc {
                    name: name.clone(),
                    table_oid,
                    attnum,
                    type_oid: wt.oid,
                    type_len: wt.len,
                    type_modifier: -1,
                    format: 0,
                }
            })
            .collect()
    }
}
