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
//! - **Simple Query**: `'Q'` strings are split into statements and executed
//!   in order; the first error aborts the rest of the string (PostgreSQL
//!   simple-protocol semantics) but never the connection.
//! - **Extended Query**: Parse/Bind/Describe/Execute/Close plus Sync/Flush,
//!   named and unnamed object lifecycles, text/binary values, suspended
//!   portals, and error recovery that ignores messages until Sync.
//! - **Transaction interception**: `BEGIN`/`COMMIT`/`ROLLBACK` never reach
//!   [`Engine::exec`] (the engine hard-rejects them — transaction control
//!   is a programmatic API). They map to `Engine::begin_txn` /
//!   `TxnHandle::commit` / `TxnHandle::abort` instead. At most one
//!   `TxnHandle` per connection; `BEGIN` inside a transaction errors
//!   (SQLSTATE 25001) without disturbing the live handle.
//!
//! The dispatch cores ([`Session::handle_query`] and
//! [`Session::handle_message`]) are transport-agnostic — they append response
//! messages to a caller buffer — so the session logic is testable without
//! sockets.

use std::collections::HashMap;
use std::io::Write;
use std::net::TcpStream;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use pg_engine::sql::{SelectCols, SelectItem, SequenceFunction, Statement};
use pg_engine::{ColumnType, Engine, EngineError, QueryResult, SqlSession, TxnHandle, Value};

static NEXT_SQL_SESSION_ID: AtomicU64 = AtomicU64::new(1);

use crate::codec::{self, FieldDesc, FrontendMessage, ObjectKind, StartupPacket};
use crate::error::{Result, WireError};
use crate::types::{
    column_type_for_oid, column_type_of, decode_parameter, encode_text, encode_value,
    expand_format_codes, wire_type, FORMAT_TEXT,
};

/// `(SQLSTATE, message)` pair for one failed statement.
struct StatementError(&'static str, String);

impl StatementError {
    fn engine(err: &EngineError) -> Self {
        Self(WireError::sqlstate_of(err), err.to_string())
    }

    fn protocol(message: impl Into<String>) -> Self {
        Self("08P01", message.into())
    }

    fn unsupported(message: impl Into<String>) -> Self {
        Self("0A000", message.into())
    }

    fn bind(err: WireError) -> Self {
        match err {
            WireError::Value { sqlstate, message } => Self(sqlstate, message),
            WireError::Protocol(message) => Self::protocol(message),
            WireError::Encode(message) if message.contains("not supported") => {
                Self::unsupported(message)
            }
            WireError::Encode(message) => Self::protocol(message),
            other => Self("XX000", other.to_string()),
        }
    }

    fn result_encoding(err: WireError) -> Self {
        let message = err.to_string();
        if message.contains("not supported") {
            Self::unsupported(message)
        } else {
            Self("XX000", message)
        }
    }
}

enum PreparedStatementKind {
    Sql(Box<Statement>),
    Empty,
}

struct PreparedStatement {
    sql: String,
    stmt: PreparedStatementKind,
    parameter_types: Vec<ColumnType>,
    parameter_oids: Vec<u32>,
}

struct Portal {
    statement_name: String,
    prepared: Arc<PreparedStatement>,
    parameters: Vec<Value>,
    result_formats: Vec<i16>,
    execution: PortalExecution,
}

enum PortalExecution {
    Ready,
    Suspended {
        rows: Vec<Vec<Value>>,
        next_row: usize,
    },
    Done,
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

/// Per-connection state: the shared engine, transaction handle, and extended
/// query objects. Empty names key the unnamed statement and portal in the
/// same maps as their named counterparts.
pub struct Session {
    engine: Arc<Engine>,
    txn: Option<TxnHandle>,
    /// True while an extended-query series owns an implicit transaction that
    /// Sync must commit (success) or roll back (any earlier error).
    implicit_txn: bool,
    /// Whether an explicit transaction block exists. Unlike `txn`, this stays
    /// true after the failed handle has been aborted, preserving 'E' status.
    explicit_txn: bool,
    /// Explicit transaction failed and was aborted; only ROLLBACK or COMMIT
    /// (treated as ROLLBACK) can clear it. ReadyForQuery reports 'E'.
    failed_txn: bool,
    statements: HashMap<String, Arc<PreparedStatement>>,
    portals: HashMap<String, Portal>,
    extended_error: bool,
    sql: SqlSession,
}

impl Session {
    /// A fresh session over `engine` (no transaction in progress).
    pub fn new(engine: Arc<Engine>) -> Self {
        Self {
            engine,
            txn: None,
            implicit_txn: false,
            explicit_txn: false,
            failed_txn: false,
            statements: HashMap::new(),
            portals: HashMap::new(),
            extended_error: false,
            sql: SqlSession::new(NEXT_SQL_SESSION_ID.fetch_add(1, Ordering::Relaxed)),
        }
    }

    /// The `ReadyForQuery` status byte for the current state.
    fn status_byte(&self) -> u8 {
        if self.failed_txn {
            codec::STATUS_FAILED
        } else if self.txn.is_some() {
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
            let message = match codec::read_message(&mut stream) {
                Ok(message) => message,
                Err(WireError::Sync(message)) => {
                    self.extended_error = true;
                    self.fail_explicit_txn();
                    let mut out = Vec::new();
                    codec::error_response(&mut out, "ERROR", "08P01", &message);
                    self.handle_sync(&mut out);
                    stream.write_all(&out)?;
                    continue;
                }
                Err(WireError::SimpleQuery(message)) => {
                    if self.extended_error {
                        // An existing extended-protocol error owns recovery:
                        // discard this malformed Q without a second response.
                        continue;
                    }
                    self.fail_explicit_txn();
                    let rollback = self.finish_implicit_txn(true);
                    let mut out = Vec::new();
                    codec::error_response(&mut out, "ERROR", "08P01", &message);
                    if let Err(StatementError(sqlstate, message)) = rollback {
                        codec::error_response(&mut out, "ERROR", sqlstate, &message);
                    }
                    codec::ready_for_query(&mut out, self.status_byte());
                    stream.write_all(&out)?;
                    continue;
                }
                Err(WireError::Protocol(message)) => {
                    // The frame boundary is known, so body-validation errors
                    // enter the same discard-until-Sync recovery as other
                    // extended-protocol errors instead of dropping the socket.
                    self.extended_error = true;
                    self.fail_explicit_txn();
                    let mut out = Vec::new();
                    codec::error_response(&mut out, "ERROR", "08P01", &message);
                    stream.write_all(&out)?;
                    continue;
                }
                Err(error) => return Err(error),
            };
            match message {
                None | Some(FrontendMessage::Terminate) => return Ok(()),
                Some(message) => {
                    let mut out = Vec::new();
                    self.handle_message(message, &mut out);
                    stream.write_all(&out)?;
                }
            }
        }
    }

    /// Handle one regular-phase message and append its responses to `out`.
    /// Extended-protocol errors suppress every later message except Sync and
    /// Terminate; the socket owner handles the actual Terminate close.
    pub fn handle_message(&mut self, message: FrontendMessage, out: &mut Vec<u8>) {
        match message {
            FrontendMessage::Terminate => {}
            FrontendMessage::Sync => self.handle_sync(out),
            _ if self.extended_error => {}
            FrontendMessage::Flush => {}
            FrontendMessage::Query(sql) => self.handle_query(&sql, out),
            // FunctionCall is a separate legacy command cycle: its clients
            // wait for ReadyForQuery, not for an extended-protocol Sync.
            FrontendMessage::Unsupported { tag: b'F' } => {
                self.fail_explicit_txn();
                let rollback = self.finish_implicit_txn(true);
                codec::error_response(out, "ERROR", "0A000", "FunctionCall is not supported");
                if let Err(StatementError(sqlstate, message)) = rollback {
                    codec::error_response(out, "ERROR", sqlstate, &message);
                }
                codec::ready_for_query(out, self.status_byte());
            }
            FrontendMessage::Unsupported { tag } => {
                self.extended_error = true;
                self.fail_explicit_txn();
                codec::error_response(
                    out,
                    "ERROR",
                    "0A000",
                    &format!("frontend message type {} is not supported", tag as char),
                );
            }
            message => {
                let result = match message {
                    FrontendMessage::Parse {
                        name,
                        query,
                        parameter_type_oids,
                    } => self.handle_parse(&name, &query, &parameter_type_oids, out),
                    FrontendMessage::Bind {
                        portal,
                        statement,
                        parameter_formats,
                        parameters,
                        result_formats,
                    } => self.handle_bind(
                        &portal,
                        &statement,
                        &parameter_formats,
                        &parameters,
                        &result_formats,
                        out,
                    ),
                    FrontendMessage::Describe { kind, name } => {
                        self.handle_describe(kind, &name, out)
                    }
                    FrontendMessage::Execute { portal, max_rows } => {
                        self.handle_execute(&portal, max_rows, out)
                    }
                    FrontendMessage::Close { kind, name } => self.handle_close(kind, &name, out),
                    FrontendMessage::Query(_)
                    | FrontendMessage::Flush
                    | FrontendMessage::Sync
                    | FrontendMessage::Terminate
                    | FrontendMessage::Unsupported { .. } => {
                        unreachable!("outer message dispatch handles this variant")
                    }
                };
                if let Err(StatementError(sqlstate, message)) = result {
                    self.extended_error = true;
                    self.fail_explicit_txn();
                    codec::error_response(out, "ERROR", sqlstate, &message);
                }
            }
        }
    }

    fn handle_sync(&mut self, out: &mut Vec<u8>) {
        let result = self.finish_implicit_txn(self.extended_error);
        self.extended_error = false;
        if self.txn.is_none() {
            self.portals.clear();
        }
        if let Err(StatementError(sqlstate, message)) = result {
            codec::error_response(out, "ERROR", sqlstate, &message);
        }
        codec::ready_for_query(out, self.status_byte());
    }

    fn finish_implicit_txn(&mut self, rollback: bool) -> std::result::Result<(), StatementError> {
        if !self.implicit_txn {
            return Ok(());
        }
        self.implicit_txn = false;
        let Some(handle) = self.txn.take() else {
            return Ok(());
        };
        let result = if rollback {
            handle.abort()
        } else {
            handle.commit()
        };
        result.map_err(|err| StatementError::engine(&err))
    }

    fn ensure_extended_txn(&mut self) -> std::result::Result<(), StatementError> {
        if self.txn.is_none() {
            self.txn = Some(
                self.engine
                    .begin_txn()
                    .map_err(|err| StatementError::engine(&err))?,
            );
            self.implicit_txn = true;
        }
        Ok(())
    }

    fn fail_explicit_txn(&mut self) {
        if self.implicit_txn {
            return;
        }
        if !self.explicit_txn && self.txn.is_none() && !self.failed_txn {
            return;
        }
        if let Some(handle) = self.txn.take() {
            if let Err(error) = handle.abort() {
                tracing::warn!(%error, "failed to abort an explicit transaction after a statement error");
            }
        }
        self.failed_txn = true;
    }

    fn failed_txn_error() -> StatementError {
        StatementError(
            "25P02",
            "current transaction is aborted, commands ignored until end of transaction block"
                .to_string(),
        )
    }

    fn handle_parse(
        &mut self,
        name: &str,
        sql: &str,
        supplied_oids: &[u32],
        out: &mut Vec<u8>,
    ) -> std::result::Result<(), StatementError> {
        if !name.is_empty() && self.statements.contains_key(name) {
            return Err(StatementError(
                "42P05",
                format!("prepared statement {name:?} already exists"),
            ));
        }

        // A new unnamed Parse invalidates the previous statement before any
        // analysis, including a replacement that later fails to parse.
        if name.is_empty() {
            self.statements.remove(name);
            self.portals
                .retain(|_, portal| portal.statement_name != name);
        }
        let stmt = if split_statements(sql)
            .iter()
            .all(|piece| piece.trim().is_empty())
        {
            PreparedStatementKind::Empty
        } else {
            let stmt = pg_engine::sql::parse(sql).map_err(|err| StatementError::engine(&err))?;
            if self.failed_txn && !matches!(stmt, Statement::Commit | Statement::Rollback) {
                return Err(Self::failed_txn_error());
            }
            self.validate_statement_relations(&stmt)?;
            PreparedStatementKind::Sql(Box::new(stmt))
        };
        let inferred = match &stmt {
            PreparedStatementKind::Empty => Vec::new(),
            PreparedStatementKind::Sql(stmt) => self
                .engine
                .parameter_types(stmt)
                .map_err(|err| StatementError::engine(&err))?,
        };
        if supplied_oids.len() > inferred.len() {
            return Err(StatementError::protocol(format!(
                "Parse supplies {} parameter types, but the statement has {} placeholders",
                supplied_oids.len(),
                inferred.len()
            )));
        }
        let (parameter_types, parameter_oids) =
            self.resolve_parameter_types(&inferred, supplied_oids)?;
        let prepared = Arc::new(PreparedStatement {
            sql: sql.to_string(),
            stmt,
            parameter_types,
            parameter_oids,
        });

        self.statements.insert(name.to_string(), prepared);
        codec::parse_complete(out);
        Ok(())
    }

    fn validate_statement_relations(
        &self,
        stmt: &Statement,
    ) -> std::result::Result<(), StatementError> {
        let table = match stmt {
            Statement::Select { table, .. }
            | Statement::Insert { table, .. }
            | Statement::Update { table, .. }
            | Statement::Delete { table, .. }
            | Statement::CreateIndex { table, .. } => Some(table),
            Statement::Explain(inner) => match inner.as_ref() {
                Statement::Select { table, .. } => Some(table),
                _ => None,
            },
            _ => None,
        };
        if let Some(table) = table {
            if self.engine.describe_table(table).is_none() {
                return Err(StatementError(
                    "42P01",
                    format!("table {table:?} does not exist"),
                ));
            }
        }
        Ok(())
    }

    fn resolve_parameter_types(
        &self,
        inferred: &[Option<ColumnType>],
        supplied_oids: &[u32],
    ) -> std::result::Result<(Vec<ColumnType>, Vec<u32>), StatementError> {
        let vector_oid = wire_type(ColumnType::Vector(1)).oid;
        let mut types = Vec::with_capacity(inferred.len());
        for (index, inferred_type) in inferred.iter().enumerate() {
            let supplied = supplied_oids.get(index).copied().unwrap_or(0);
            let vector_dim = match inferred_type {
                Some(ColumnType::Vector(dim)) => *dim,
                _ => 0,
            };
            let ty = if supplied == 0 {
                inferred_type.ok_or_else(|| {
                    StatementError::unsupported(format!(
                        "could not determine a supported type for parameter ${}",
                        index + 1
                    ))
                })?
            } else {
                let supported_oid =
                    matches!(supplied, 20 | 23 | 25 | 114 | 701) || supplied == vector_oid;
                let supplied_type = column_type_for_oid(supplied, vector_dim).map_err(|err| {
                    if supported_oid {
                        StatementError::protocol(err.to_string())
                    } else {
                        StatementError::unsupported(err.to_string())
                    }
                })?;
                if let Some(inferred_type) = inferred_type {
                    if supplied_type != *inferred_type {
                        if supplied_type == ColumnType::Text
                            && matches!(inferred_type, ColumnType::Vector(_))
                        {
                            // pgvector accepts its text representation. Decode
                            // using the inferred VECTOR(dim), while reporting
                            // the client-declared TEXT OID below.
                            *inferred_type
                        } else {
                            return Err(StatementError::protocol(format!(
                                "parameter ${} has inferred type {inferred_type:?}, but OID {supplied} means {supplied_type:?}",
                                index + 1
                            )));
                        }
                    } else {
                        supplied_type
                    }
                } else {
                    supplied_type
                }
            };
            if !matches!(
                ty,
                ColumnType::Int4
                    | ColumnType::Int8
                    | ColumnType::Float8
                    | ColumnType::Text
                    | ColumnType::Jsonb
                    | ColumnType::Vector(_)
            ) {
                return Err(StatementError::unsupported(format!(
                    "Extended Query parameters are not supported for {ty:?}"
                )));
            }
            types.push(ty);
        }
        let oids = types
            .iter()
            .enumerate()
            .map(|(index, ty)| {
                let supplied = supplied_oids.get(index).copied().unwrap_or(0);
                if supplied == 0 {
                    wire_type(*ty).oid
                } else {
                    supplied
                }
            })
            .collect();
        Ok((types, oids))
    }

    fn handle_bind(
        &mut self,
        portal_name: &str,
        statement_name: &str,
        parameter_formats: &[i16],
        parameters: &[Option<Vec<u8>>],
        result_formats: &[i16],
        out: &mut Vec<u8>,
    ) -> std::result::Result<(), StatementError> {
        if !portal_name.is_empty() && self.portals.contains_key(portal_name) {
            return Err(StatementError(
                "42P03",
                format!("portal {portal_name:?} already exists"),
            ));
        }
        if portal_name.is_empty() {
            self.portals.remove(portal_name);
        }
        let prepared = match self.statements.get(statement_name).cloned() {
            Some(prepared) => prepared,
            None => {
                if self.failed_txn {
                    return Err(Self::failed_txn_error());
                }
                return Err(StatementError(
                    "26000",
                    format!("prepared statement {statement_name:?} does not exist"),
                ));
            }
        };
        let transaction_exit = match &prepared.stmt {
            PreparedStatementKind::Sql(stmt) => {
                matches!(stmt.as_ref(), Statement::Commit | Statement::Rollback)
            }
            PreparedStatementKind::Empty => false,
        };
        if self.failed_txn && !transaction_exit {
            return Err(Self::failed_txn_error());
        }
        if parameters.len() != prepared.parameter_types.len() {
            return Err(StatementError::protocol(format!(
                "bind message supplies {} parameters, but prepared statement requires {}",
                parameters.len(),
                prepared.parameter_types.len()
            )));
        }
        let parameter_formats =
            expand_format_codes(parameter_formats, parameters.len(), "parameter")
                .map_err(StatementError::bind)?;
        let mut values = Vec::with_capacity(parameters.len());
        for (index, ((ty, format), bytes)) in prepared
            .parameter_types
            .iter()
            .zip(&parameter_formats)
            .zip(parameters)
            .enumerate()
        {
            let format =
                if prepared.parameter_oids[index] == 25 && matches!(ty, ColumnType::Vector(_)) {
                    // TEXT OID's binary format is still the type's text bytes;
                    // only an actual VECTOR OID selects pgvector's binary layout.
                    FORMAT_TEXT
                } else {
                    *format
                };
            values.push(
                decode_parameter(*ty, format, bytes.as_deref())
                    .map_err(StatementError::bind)
                    .map_err(|err| {
                        StatementError(err.0, format!("parameter ${}: {}", index + 1, err.1))
                    })?,
            );
        }

        let fields = self.statement_field_descs(&prepared.stmt, FORMAT_TEXT)?;
        let result_formats = expand_format_codes(result_formats, fields.len(), "result")
            .map_err(StatementError::bind)?;
        self.portals.insert(
            portal_name.to_string(),
            Portal {
                statement_name: statement_name.to_string(),
                prepared,
                parameters: values,
                result_formats,
                execution: PortalExecution::Ready,
            },
        );
        codec::bind_complete(out);
        Ok(())
    }

    fn handle_describe(
        &self,
        kind: ObjectKind,
        name: &str,
        out: &mut Vec<u8>,
    ) -> std::result::Result<(), StatementError> {
        match kind {
            ObjectKind::Statement => {
                let prepared = self.statements.get(name).ok_or_else(|| {
                    StatementError(
                        "26000",
                        format!("prepared statement {name:?} does not exist"),
                    )
                })?;
                codec::parameter_description(out, &prepared.parameter_oids);
                let fields = self.statement_field_descs(&prepared.stmt, FORMAT_TEXT)?;
                if fields.is_empty() {
                    codec::no_data(out);
                } else {
                    codec::row_description(out, &fields);
                }
            }
            ObjectKind::Portal => {
                let portal = self.portals.get(name).ok_or_else(|| {
                    StatementError("34000", format!("portal {name:?} does not exist"))
                })?;
                let mut fields = self.statement_field_descs(&portal.prepared.stmt, FORMAT_TEXT)?;
                if fields.is_empty() {
                    codec::no_data(out);
                } else {
                    for (field, format) in fields.iter_mut().zip(&portal.result_formats) {
                        field.format = *format;
                    }
                    codec::row_description(out, &fields);
                }
            }
        }
        Ok(())
    }

    fn handle_close(
        &mut self,
        kind: ObjectKind,
        name: &str,
        out: &mut Vec<u8>,
    ) -> std::result::Result<(), StatementError> {
        match kind {
            ObjectKind::Statement => {
                self.statements.remove(name);
                self.portals
                    .retain(|_, portal| portal.statement_name != name);
            }
            ObjectKind::Portal => {
                self.portals.remove(name);
            }
        }
        // Close is idempotent: a missing statement or portal still succeeds.
        codec::close_complete(out);
        Ok(())
    }

    fn handle_execute(
        &mut self,
        name: &str,
        max_rows: u32,
        out: &mut Vec<u8>,
    ) -> std::result::Result<(), StatementError> {
        let mut portal = self
            .portals
            .remove(name)
            .ok_or_else(|| StatementError("34000", format!("portal {name:?} does not exist")))?;
        let ended_transaction = match &portal.prepared.stmt {
            PreparedStatementKind::Sql(stmt) => {
                matches!(stmt.as_ref(), Statement::Commit | Statement::Rollback)
            }
            PreparedStatementKind::Empty => false,
        };
        let result = self.execute_portal(&mut portal, max_rows, out);
        if !ended_transaction {
            self.portals.insert(name.to_string(), portal);
        }
        result
    }

    fn execute_portal(
        &mut self,
        portal: &mut Portal,
        max_rows: u32,
        out: &mut Vec<u8>,
    ) -> std::result::Result<(), StatementError> {
        if matches!(portal.execution, PortalExecution::Done) {
            match &portal.prepared.stmt {
                PreparedStatementKind::Empty => {
                    codec::empty_query_response(out);
                    return Ok(());
                }
                PreparedStatementKind::Sql(stmt)
                    if matches!(
                        stmt.as_ref(),
                        Statement::Select { .. } | Statement::Explain(_)
                    ) =>
                {
                    codec::command_complete(out, "SELECT 0");
                    return Ok(());
                }
                _ => {
                    return Err(StatementError::protocol(
                        "portal cannot be run more than once",
                    ))
                }
            }
        }

        let PreparedStatementKind::Sql(stmt) = &portal.prepared.stmt else {
            portal.execution = PortalExecution::Done;
            codec::empty_query_response(out);
            return Ok(());
        };
        if let Some(result) = self.handle_transaction_control(stmt, out) {
            if result.is_ok() {
                portal.execution = PortalExecution::Done;
            }
            return result;
        }
        if self.failed_txn {
            return Err(Self::failed_txn_error());
        }

        let execution = std::mem::replace(&mut portal.execution, PortalExecution::Done);
        match execution {
            PortalExecution::Ready => {
                if matches!(
                    stmt.as_ref(),
                    Statement::CreateTable { .. }
                        | Statement::CreateIndex { .. }
                        | Statement::CreateSequence { .. }
                        | Statement::CreateGraph { .. }
                ) {
                    return Err(StatementError::unsupported(
                        "DDL in an extended implicit transaction is not supported",
                    ));
                }
                self.ensure_extended_txn()?;
                let result = self
                    .engine
                    .exec_with_params_in_session(
                        &mut self.sql,
                        self.txn.as_ref(),
                        &portal.prepared.sql,
                        &portal.parameters,
                    )
                    .map_err(|err| StatementError::engine(&err))?;
                match result {
                    QueryResult::Rows { rows, .. } => {
                        self.write_portal_rows(portal, rows, 0, max_rows, out)
                    }
                    QueryResult::Affected(n) => {
                        codec::command_complete(out, &affected_tag(stmt, n));
                        Ok(())
                    }
                    QueryResult::Ok => {
                        codec::command_complete(out, ok_tag(stmt));
                        Ok(())
                    }
                }
            }
            PortalExecution::Suspended { rows, next_row } => {
                self.write_portal_rows(portal, rows, next_row, max_rows, out)
            }
            PortalExecution::Done => unreachable!("done portals return above"),
        }
    }

    fn write_portal_rows(
        &self,
        portal: &mut Portal,
        rows: Vec<Vec<Value>>,
        next_row: usize,
        max_rows: u32,
        out: &mut Vec<u8>,
    ) -> std::result::Result<(), StatementError> {
        let remaining = rows.len() - next_row;
        let limit = usize::try_from(max_rows).unwrap_or(usize::MAX);
        let count = if max_rows == 0 {
            remaining
        } else {
            remaining.min(limit)
        };
        let end = next_row + count;
        let mut encoded = Vec::new();
        for row in &rows[next_row..end] {
            if row.len() != portal.result_formats.len() {
                return Err(StatementError::protocol(format!(
                    "result row has {} columns, but Bind selected {} formats",
                    row.len(),
                    portal.result_formats.len()
                )));
            }
            let mut values = Vec::with_capacity(row.len());
            for (value, format) in row.iter().zip(&portal.result_formats) {
                values.push(match value {
                    None => None,
                    Some(datum) => Some(
                        encode_value(datum, *format).map_err(StatementError::result_encoding)?,
                    ),
                });
            }
            codec::data_row(
                &mut encoded,
                &values
                    .iter()
                    .map(|value| value.as_deref())
                    .collect::<Vec<_>>(),
            );
        }
        out.extend_from_slice(&encoded);

        // A nonzero, exactly-full page also suspends; the next Execute then
        // completes with SELECT 0. Only a short page completes immediately.
        if max_rows > 0 && count == limit {
            portal.execution = PortalExecution::Suspended {
                rows,
                next_row: end,
            };
            codec::portal_suspended(out);
        } else {
            portal.execution = PortalExecution::Done;
            codec::command_complete(out, &format!("SELECT {count}"));
        }
        Ok(())
    }

    /// Execute one simple-query string (possibly multi-statement),
    /// appending every response message — including the trailing
    /// `ReadyForQuery` — to `out`.
    ///
    /// Statement errors abort the remainder of the string (PostgreSQL
    /// simple-protocol semantics) but leave the connection — and any
    /// explicit transaction — alive.
    pub fn handle_query(&mut self, sql: &str, out: &mut Vec<u8>) {
        // Simple Query invalidates the unnamed statement, every portal derived
        // from it, and the unnamed portal. Named objects are otherwise kept.
        self.statements.remove("");
        self.portals
            .retain(|name, portal| !name.is_empty() && !portal.statement_name.is_empty());

        // Parse the entire string before executing anything. In particular, a
        // trailing syntax error must not commit earlier statements.
        let mut statements = Vec::new();
        let mut had_error = None;
        for piece in split_statements(sql) {
            let piece = piece.trim();
            if piece.is_empty() {
                continue;
            }
            match pg_engine::sql::parse(piece) {
                Ok(stmt) => statements.push((piece.to_string(), stmt)),
                Err(error) => {
                    had_error = Some(StatementError::engine(&error));
                    break;
                }
            }
        }
        if had_error.is_none() {
            for (sql, stmt) in &statements {
                let is_ddl = matches!(
                    stmt,
                    Statement::CreateTable { .. }
                        | Statement::CreateIndex { .. }
                        | Statement::CreateSequence { .. }
                        | Statement::CreateGraph { .. }
                );
                if is_ddl {
                    if self.failed_txn {
                        had_error = Some(Self::failed_txn_error());
                        break;
                    }
                    if self.implicit_txn {
                        had_error = Some(StatementError::unsupported(
                            "DDL in an extended implicit transaction is not supported",
                        ));
                        break;
                    }
                    if self.txn.is_some() {
                        // Explicit-transaction DDL keeps the engine's loud ban.
                        if let Err(error) = self.handle_statement(sql, out) {
                            had_error = Some(error);
                            break;
                        }
                    } else {
                        match self.engine.exec_in_session(&mut self.sql, None, sql) {
                            Ok(result) => {
                                if let Err(error) = self.write_result(stmt, result, out) {
                                    had_error = Some(error);
                                    break;
                                }
                            }
                            Err(error) => {
                                had_error = Some(StatementError::engine(&error));
                                break;
                            }
                        }
                    }
                } else {
                    if !matches!(stmt, Statement::Commit | Statement::Rollback) {
                        if let Err(error) = self.ensure_extended_txn() {
                            had_error = Some(error);
                            break;
                        }
                    }
                    if let Err(error) = self.handle_statement(sql, out) {
                        had_error = Some(error);
                        break;
                    }
                }
            }
        }
        if statements.is_empty() && had_error.is_none() {
            codec::empty_query_response(out);
        }
        let failed = had_error.is_some();
        if let Some(StatementError(sqlstate, message)) = had_error {
            self.fail_explicit_txn();
            codec::error_response(out, "ERROR", sqlstate, &message);
        }
        if self.implicit_txn {
            // The whole string and any preceding extended series share one
            // implicit transaction: only a clean simple-query cycle commits it.
            if let Err(StatementError(sqlstate, message)) = self.finish_implicit_txn(failed) {
                codec::error_response(out, "ERROR", sqlstate, &message);
            }
        }
        codec::ready_for_query(out, self.status_byte());
        if self.txn.is_none() {
            self.portals.clear();
        }
    }

    /// Dispatch one already-split statement.
    fn handle_statement(
        &mut self,
        sql: &str,
        out: &mut Vec<u8>,
    ) -> std::result::Result<(), StatementError> {
        let stmt = pg_engine::sql::parse(sql).map_err(|e| StatementError::engine(&e))?;
        if let Some(result) = self.handle_transaction_control(&stmt, out) {
            return result;
        }
        if self.failed_txn {
            return Err(Self::failed_txn_error());
        }
        let result = self
            .engine
            .exec_in_session(&mut self.sql, self.txn.as_ref(), sql)
            .map_err(|e| StatementError::engine(&e))?;
        self.write_result(&stmt, result, out)
    }

    fn handle_transaction_control(
        &mut self,
        stmt: &Statement,
        out: &mut Vec<u8>,
    ) -> Option<std::result::Result<(), StatementError>> {
        if self.failed_txn {
            return Some(match stmt {
                Statement::Commit | Statement::Rollback => {
                    self.failed_txn = false;
                    self.explicit_txn = false;
                    self.portals.clear();
                    codec::notice_response(
                        out,
                        "WARNING",
                        "25P01",
                        "there is no transaction in progress",
                    );
                    codec::command_complete(out, "ROLLBACK");
                    Ok(())
                }
                _ => Err(Self::failed_txn_error()),
            });
        }
        let result = match stmt {
            // ── Wire-level transaction interception (§7.2) ──────────
            Statement::Begin => {
                if self.txn.is_some() {
                    if self.implicit_txn {
                        // BEGIN converts the extended series' implicit
                        // transaction into an explicit block in place.
                        self.implicit_txn = false;
                        self.explicit_txn = true;
                        codec::command_complete(out, "BEGIN");
                        Ok(())
                    } else {
                        codec::notice_response(
                            out,
                            "WARNING",
                            "25001",
                            "there is already a transaction in progress",
                        );
                        codec::command_complete(out, "BEGIN");
                        Ok(())
                    }
                } else {
                    match self.engine.begin_txn() {
                        Ok(handle) => {
                            self.txn = Some(handle);
                            self.explicit_txn = true;
                            codec::command_complete(out, "BEGIN");
                            Ok(())
                        }
                        Err(e) => Err(StatementError::engine(&e)),
                    }
                }
            }
            Statement::Commit => match self.txn.take() {
                None => {
                    codec::notice_response(
                        out,
                        "WARNING",
                        "25P01",
                        "there is no transaction in progress",
                    );
                    codec::command_complete(out, "COMMIT");
                    Ok(())
                }
                Some(handle) => {
                    self.implicit_txn = false;
                    self.explicit_txn = false;
                    let result = handle.commit().map_err(|e| StatementError::engine(&e));
                    self.portals.clear();
                    result.map(|()| codec::command_complete(out, "COMMIT"))
                }
            },
            Statement::Rollback => match self.txn.take() {
                None => {
                    codec::notice_response(
                        out,
                        "WARNING",
                        "25P01",
                        "there is no transaction in progress",
                    );
                    codec::command_complete(out, "ROLLBACK");
                    Ok(())
                }
                Some(handle) => {
                    self.implicit_txn = false;
                    self.explicit_txn = false;
                    let result = handle.abort().map_err(|e| StatementError::engine(&e));
                    self.portals.clear();
                    result.map(|()| codec::command_complete(out, "ROLLBACK"))
                }
            },
            _ => return None,
        };
        Some(result)
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
                codec::command_complete(out, &affected_tag(stmt, n));
            }
            QueryResult::Ok => codec::command_complete(out, ok_tag(stmt)),
        }
        Ok(())
    }

    fn statement_field_descs(
        &self,
        stmt: &PreparedStatementKind,
        format: i16,
    ) -> std::result::Result<Vec<FieldDesc>, StatementError> {
        let stmt = match stmt {
            PreparedStatementKind::Empty => return Ok(Vec::new()),
            PreparedStatementKind::Sql(stmt) => stmt,
        };
        let make_field = |name: String, table_oid, attnum, ty| {
            let wt = wire_type(ty);
            FieldDesc {
                name,
                table_oid,
                attnum,
                type_oid: wt.oid,
                type_len: wt.len,
                type_modifier: -1,
                format,
            }
        };
        if let Statement::SequenceQuery { function, .. } = stmt.as_ref() {
            let name = match function {
                SequenceFunction::NextVal => "nextval",
                SequenceFunction::CurrVal => "currval",
            };
            return Ok(vec![make_field(name.to_string(), 0, 0, ColumnType::Int8)]);
        }
        if matches!(stmt.as_ref(), Statement::Explain(_)) {
            return Ok(vec![make_field("plan".to_string(), 0, 0, ColumnType::Text)]);
        }
        let Statement::Select { table, columns, .. } = stmt.as_ref() else {
            return Ok(Vec::new());
        };
        let entry = self
            .engine
            .describe_table(table)
            .ok_or_else(|| StatementError("42P01", format!("table {table:?} does not exist")))?;
        match columns {
            SelectCols::All => Ok(entry
                .columns
                .iter()
                .enumerate()
                .map(|(index, column)| {
                    make_field(
                        column.name.clone(),
                        entry.oid.0 as u32,
                        index as i16 + 1,
                        column.col_type,
                    )
                })
                .collect()),
            SelectCols::Cols(items) => items
                .iter()
                .map(|item| match item {
                    SelectItem::Column(name) => {
                        let (index, column) = entry
                            .columns
                            .iter()
                            .enumerate()
                            .find(|(_, column)| column.name.eq_ignore_ascii_case(name))
                            .ok_or_else(|| {
                                StatementError(
                                    "42601",
                                    format!("no column {name:?} in table {table:?}"),
                                )
                            })?;
                        Ok(make_field(
                            name.clone(),
                            entry.oid.0 as u32,
                            index as i16 + 1,
                            column.col_type,
                        ))
                    }
                    SelectItem::Distance(_) => {
                        Ok(make_field("?column?".to_string(), 0, 0, ColumnType::Float8))
                    }
                    SelectItem::Bm25Score(expr) => Ok(make_field(
                        expr.alias.clone().unwrap_or_else(|| "?column?".to_string()),
                        0,
                        0,
                        ColumnType::Float8,
                    )),
                })
                .collect(),
        }
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
        let (schema, select_items) = match stmt {
            Statement::Select { table, columns, .. } => (
                self.engine.describe_table(table),
                match columns {
                    SelectCols::Cols(items) => Some(items),
                    SelectCols::All => None,
                },
            ),
            _ => (None, None),
        };
        columns
            .iter()
            .enumerate()
            .map(|(i, name)| {
                let computed = matches!(
                    select_items.and_then(|items| items.get(i)),
                    Some(SelectItem::Distance(_) | SelectItem::Bm25Score(_))
                );
                // A computed expression's alias must never be resolved as a
                // same-named base column.
                let resolved = if computed {
                    None
                } else {
                    schema.as_ref().and_then(|entry| {
                        entry
                            .columns
                            .iter()
                            .enumerate()
                            .find(|(_, c)| &c.name == name)
                            .map(|(attnum, c)| (entry.oid.0 as u32, attnum as i16 + 1, c.col_type))
                    })
                };
                let (table_oid, attnum, ty) = match resolved {
                    Some((oid, attnum, ty)) => (oid, attnum, ty),
                    None if matches!(stmt, Statement::SequenceQuery { .. }) => {
                        (0, 0, pg_engine::ColumnType::Int8)
                    }
                    None if matches!(
                        select_items.and_then(|items| items.get(i)),
                        Some(SelectItem::Distance(_) | SelectItem::Bm25Score(_))
                    ) =>
                    {
                        // Computed distance/BM25 columns have a static type
                        // even when the result is empty and no datum can infer it.
                        (0, 0, pg_engine::ColumnType::Float8)
                    }
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

fn affected_tag(stmt: &Statement, n: usize) -> String {
    match stmt {
        Statement::Insert { .. } => format!("INSERT 0 {n}"),
        Statement::Update { .. } => format!("UPDATE {n}"),
        Statement::Delete { .. } => format!("DELETE {n}"),
        // Affected only comes from DML; keep a sane fallback.
        _ => format!("OK {n}"),
    }
}

fn ok_tag(stmt: &Statement) -> &'static str {
    match stmt {
        Statement::CreateTable { .. } => "CREATE TABLE",
        Statement::CreateIndex { .. } => "CREATE INDEX",
        Statement::CreateSequence { .. } => "CREATE SEQUENCE",
        _ => "OK",
    }
}

#[cfg(test)]
mod stage0_bm25_tests {
    use super::*;
    use pg_engine::{Datum, EngineConfig};

    #[test]
    fn empty_and_nonempty_bm25_results_keep_float8_wire_shape() {
        let tmp = tempfile::TempDir::new().unwrap();
        let engine = Arc::new(Engine::open(tmp.path(), EngineConfig::new(tmp.path())).unwrap());
        engine
            .exec(None, "CREATE TABLE agent_memory (id INT4, content TEXT)")
            .unwrap();
        let session = Session::new(engine);
        let stmt = pg_engine::sql::parse(
            "SELECT id, bm25_score(content, 'contract') AS score FROM agent_memory WHERE content @@ 'contract'",
        )
        .unwrap();
        let columns = vec!["id".to_string(), "score".to_string()];

        // Stage 0 does not execute BM25. Supplying the result shape directly
        // tests the same schema-driven Simple/Extended encoder without
        // pretending that query execution already exists.
        let empty = session.field_descs(&stmt, &columns, &[]);
        assert_eq!(empty[0].type_oid, 23);
        assert_eq!(empty[1].name, "score");
        assert_eq!(empty[1].type_oid, 701);
        assert_eq!(empty[1].type_len, 8);

        let rows = vec![vec![Some(Datum::Int4(1)), Some(Datum::Float8(1.25))]];
        let nonempty = session.field_descs(&stmt, &columns, &rows);
        assert_eq!(nonempty[1].type_oid, 701);
        let encoded: Vec<Option<Vec<u8>>> = rows[0]
            .iter()
            .map(|value| {
                value
                    .as_ref()
                    .map(|datum| encode_value(datum, FORMAT_TEXT).unwrap())
            })
            .collect();
        let encoded_refs: Vec<Option<&[u8]>> =
            encoded.iter().map(|value| value.as_deref()).collect();
        let mut bytes = Vec::new();
        codec::data_row(&mut bytes, &encoded_refs);
        assert_eq!(
            bytes,
            [b'D', 0, 0, 0, 19, 0, 2, 0, 0, 0, 1, b'1', 0, 0, 0, 4, b'1', b'.', b'2', b'5',]
        );
        session.engine.shutdown();
    }

    #[test]
    fn bm25_alias_cannot_inherit_base_column_type() {
        let tmp = tempfile::TempDir::new().unwrap();
        let engine = Arc::new(Engine::open(tmp.path(), EngineConfig::new(tmp.path())).unwrap());
        engine
            .exec(None, "CREATE TABLE t (content TEXT, score INT4)")
            .unwrap();
        let session = Session::new(engine);
        let stmt = pg_engine::sql::parse(
            "SELECT bm25_score(content, 'x') AS score FROM t WHERE content @@ 'x'",
        )
        .unwrap();
        let prepared = PreparedStatementKind::Sql(Box::new(stmt.clone()));
        let extended = match session.statement_field_descs(&prepared, FORMAT_TEXT) {
            Ok(fields) => fields,
            Err(_) => panic!("BM25 statement description failed"),
        };
        assert_eq!(extended[0].name, "score");
        assert_eq!(extended[0].type_oid, 701);

        let simple = session.field_descs(&stmt, &["score".to_string()], &[]);
        assert_eq!(simple[0].name, "score");
        assert_eq!(simple[0].type_oid, 701);
        assert_eq!(simple[0].table_oid, 0);
        assert_eq!(simple[0].attnum, 0);
        session.engine.shutdown();
    }
}
