//! Hand-rolled PostgreSQL v3 wire codec (tech-selection §7.2, §10).
//!
//! Framing rules implemented here:
//!
//! - **Startup phase**: packets have no type byte — `Int32 length`
//!   (self-inclusive) + `Int32 code` + body. Dispatched by code:
//!   v3.0 startup (196608), SSLRequest (80877103), GSSENCRequest
//!   (80877104), CancelRequest (80877102).
//! - **Regular phase**: `Int8 tag` + `Int32 length` (self-inclusive,
//!   excluding the tag) + body.
//! - All integers big-endian; strings are NUL-terminated (`cstring`).
//!
//! The decoder covers Simple Query plus the Stage C Extended Query messages
//! (`P/B/D/E/C/H/S`). Unknown tags surface as
//! [`FrontendMessage::Unsupported`] without losing framing sync.
//!
//! Encode side produces the corresponding backend messages. All encoders
//! append to a caller-owned buffer so a whole response batch can be flushed
//! with a single `write_all`.

use std::io::Read;

use crate::error::{Result, WireError};

/// v3.0 protocol version code (`(3 << 16) | 0`).
pub const PROTOCOL_V3: u32 = 196608;
/// SSLRequest magic code; the server answers `'N'` (no TLS, §7.2 非目标).
pub const SSL_REQUEST_CODE: u32 = 80877103;
/// GSSENCRequest magic code; answered `'N'` as well (see session docs).
pub const GSSENC_REQUEST_CODE: u32 = 80877104;
/// CancelRequest magic code; unsupported (§7.2 非目标) — connection closes.
pub const CANCEL_REQUEST_CODE: u32 = 80877102;

/// `ReadyForQuery` status: idle (no explicit transaction).
pub const STATUS_IDLE: u8 = b'I';
/// `ReadyForQuery` status: inside an explicit transaction block.
pub const STATUS_IN_TXN: u8 = b'T';
/// `ReadyForQuery` status: failed explicit transaction block.
pub const STATUS_FAILED: u8 = b'E';

/// Hard cap on any regular-phase message: 64 MiB. The SQL subset has no
/// large-object literals, so a legitimate simple-query string or its biggest
/// parameter payload sits orders of magnitude below this; the cap exists to
/// reject a hostile/absurd length word before any body allocation happens
/// (F1 review finding).
pub const MAX_MESSAGE_LEN: u32 = 64 << 20;

/// Hard cap on a startup packet: PostgreSQL's `MAX_STARTUP_PACKET_LENGTH`
/// (10000) — a startup packet carries only connection parameters, and PG
/// has enforced this exact bound since 8.0.
pub const MAX_STARTUP_PACKET_LEN: u32 = 10000;

/// Read `len - min` body bytes, growing the buffer in 8 KiB chunks as bytes
/// actually arrive.
///
/// F1 (Stage F review): the declared length is NEVER pre-allocated in full
/// — a 5-byte header claiming a 64 MiB body would otherwise pin 64 MiB of
/// memory per connection while blocked in `read_exact` (a memory-DoS
/// vector). Chunked growth bounds the allocation at any instant by the
/// bytes the peer has actually sent (plus one chunk of slack).
fn read_body<R: Read>(r: &mut R, len: u32, min: u32, max: u32) -> Result<Vec<u8>> {
    if !(min..=max).contains(&len) {
        return Err(WireError::Framing(format!(
            "invalid message length {len} (allowed {min}..={max})"
        )));
    }
    let mut remaining = (len - min) as usize;
    let mut body = Vec::with_capacity(remaining.min(8192));
    let mut chunk = [0u8; 8192];
    while remaining > 0 {
        let want = remaining.min(chunk.len());
        // A truncated body is a hard error (never a clean EOF — the length
        // word already promised these bytes).
        let n = r.read(&mut chunk[..want])?;
        if n == 0 {
            return Err(WireError::Io(std::io::Error::from(
                std::io::ErrorKind::UnexpectedEof,
            )));
        }
        body.extend_from_slice(&chunk[..n]);
        remaining -= n;
    }
    Ok(body)
}

/// A startup-phase packet (the only messages without a type byte).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartupPacket {
    /// v3.0 StartupMessage: `(name, value)` parameter pairs in wire order.
    Startup {
        /// Startup parameters (`user`, `database`, `application_name`, ...).
        params: Vec<(String, String)>,
    },
    /// SSLRequest — the session answers `'N'` and keeps reading.
    SslRequest,
    /// GSSENCRequest — answered `'N'` (a GSS-less server must still respond;
    /// libpq blocks waiting for the byte).
    GssEncRequest,
    /// CancelRequest — out of scope (§7.2); the session closes the socket.
    CancelRequest {
        /// Target backend PID (ignored — no cancel support).
        pid: u32,
        /// Target backend secret key (ignored).
        key: u32,
    },
}

/// Object kind shared by Describe and Close.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectKind {
    /// A prepared statement (`'S'`).
    Statement,
    /// A bound portal (`'P'`).
    Portal,
}

/// A regular-phase frontend message of the supported closed set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrontendMessage {
    /// Simple Query (`'Q'`): the query string (may hold multiple
    /// `;`-separated statements; splitting is the session's job).
    Query(String),
    /// Parse (`'P'`): create or replace a prepared statement.
    Parse {
        /// Statement name; empty selects the unnamed statement.
        name: String,
        /// SQL containing optional `$1..$n` placeholders.
        query: String,
        /// Client-supplied parameter type OIDs; zero means infer at Parse.
        parameter_type_oids: Vec<u32>,
    },
    /// Bind (`'B'`): bind parameters and result formats into a portal.
    Bind {
        /// Portal name; empty selects the unnamed portal.
        portal: String,
        /// Source prepared-statement name.
        statement: String,
        /// Parameter format codes, expanded using PostgreSQL's 0/1/N rule.
        parameter_formats: Vec<i16>,
        /// Parameter payloads; `None` is SQL NULL.
        parameters: Vec<Option<Vec<u8>>>,
        /// Result-column format codes, expanded using PostgreSQL's 0/1/N rule.
        result_formats: Vec<i16>,
    },
    /// Describe (`'D'`): describe a statement or portal.
    Describe {
        /// Whether to describe a statement or portal.
        kind: ObjectKind,
        /// Object name.
        name: String,
    },
    /// Execute (`'E'`): execute a portal.
    Execute {
        /// Portal name.
        portal: String,
        /// Maximum rows to return before PortalSuspended; zero means all.
        max_rows: u32,
    },
    /// Close (`'C'`): close a statement or portal.
    Close {
        /// Whether to close a statement or portal.
        kind: ObjectKind,
        /// Object name.
        name: String,
    },
    /// Flush (`'H'`): flush buffered backend output.
    Flush,
    /// Sync (`'S'`): end an extended-query error/recovery cycle.
    Sync,
    /// Terminate (`'X'`): the session closes the connection cleanly.
    Terminate,
    /// Any other tag (FunctionCall, ...). The payload is discarded — the
    /// length word keeps framing in sync.
    Unsupported {
        /// The raw message type byte.
        tag: u8,
    },
}

/// One field of a `RowDescription`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldDesc {
    /// Column name.
    pub name: String,
    /// Table OID (`pg_class.oid`), or 0 when not attributable to a table.
    pub table_oid: u32,
    /// Column attribute number (1-based), or 0 when not applicable.
    pub attnum: i16,
    /// Type OID (see [`crate::types::wire_type`]).
    pub type_oid: u32,
    /// Fixed type width in bytes, or -1 for varlena.
    pub type_len: i16,
    /// Type modifier; always -1 in this subset.
    pub type_modifier: i32,
    /// Format code: 0 = text, 1 = binary.
    pub format: i16,
}

// ─── Frontend decode ───────────────────────────────────────────────────

fn read_exact_or_eof<R: Read>(r: &mut R, buf: &mut [u8]) -> Result<Option<()>> {
    match r.read_exact(buf) {
        Ok(()) => Ok(Some(())),
        // Clean hangup before the first byte of a frame: normal close.
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
        Err(e) => Err(WireError::Io(e)),
    }
}

/// Read one startup-phase packet. Returns `Ok(None)` on a clean EOF before
/// the length word (client connected and went away).
pub fn read_startup_packet<R: Read>(r: &mut R) -> Result<Option<StartupPacket>> {
    let mut hdr = [0u8; 4];
    if read_exact_or_eof(r, &mut hdr)?.is_none() {
        return Ok(None);
    }
    let len = u32::from_be_bytes(hdr);
    // Startup packets are tiny (connection parameters only): the PG
    // MAX_STARTUP_PACKET_LENGTH cap applies, and chunked growth means the
    // declared length is never pre-allocated (F1). The length word is
    // self-inclusive, so len-4 bytes remain: request code (4) + body.
    let mut framed = read_body(r, len, 4, MAX_STARTUP_PACKET_LEN)?;
    if framed.len() < 4 {
        return Err(WireError::Protocol(format!(
            "startup packet length {len} too short for request code"
        )));
    }
    let code = u32::from_be_bytes(framed[0..4].try_into().unwrap());
    let body = framed.split_off(4);
    let packet = match code {
        PROTOCOL_V3 => StartupPacket::Startup {
            params: parse_startup_body(&body)?,
        },
        SSL_REQUEST_CODE => StartupPacket::SslRequest,
        GSSENC_REQUEST_CODE => StartupPacket::GssEncRequest,
        CANCEL_REQUEST_CODE => {
            if body.len() != 8 {
                return Err(WireError::Protocol(
                    "CancelRequest body must be exactly 8 bytes".to_string(),
                ));
            }
            StartupPacket::CancelRequest {
                pid: u32::from_be_bytes(body[0..4].try_into().unwrap()),
                key: u32::from_be_bytes(body[4..8].try_into().unwrap()),
            }
        }
        other => {
            return Err(WireError::Protocol(format!(
                "unsupported protocol version or request code {other}"
            )))
        }
    };
    Ok(Some(packet))
}

/// Parse the `name\0value\0...\0` body of a v3.0 StartupMessage.
pub fn parse_startup_body(body: &[u8]) -> Result<Vec<(String, String)>> {
    let mut params = Vec::new();
    let mut rest = body;
    loop {
        let Some(nul) = rest.iter().position(|&b| b == 0) else {
            return Err(WireError::Protocol(
                "startup packet missing terminator".to_string(),
            ));
        };
        if nul == 0 {
            // The terminator must be the LAST byte of the body.
            if nul + 1 != rest.len() {
                return Err(WireError::Protocol(
                    "trailing bytes after startup terminator".to_string(),
                ));
            }
            return Ok(params);
        }
        let name = std::str::from_utf8(&rest[..nul])
            .map_err(|_| WireError::Protocol("non-UTF8 startup name".to_string()))?
            .to_string();
        rest = &rest[nul + 1..];
        let Some(nul) = rest.iter().position(|&b| b == 0) else {
            return Err(WireError::Protocol(
                "startup parameter missing value".to_string(),
            ));
        };
        let value = std::str::from_utf8(&rest[..nul])
            .map_err(|_| WireError::Protocol("non-UTF8 startup value".to_string()))?
            .to_string();
        rest = &rest[nul + 1..];
        params.push((name, value));
    }
}

/// Bounds-checked cursor for one already-framed message body.
struct BodyReader<'a> {
    body: &'a [u8],
    pos: usize,
}

impl<'a> BodyReader<'a> {
    fn new(body: &'a [u8]) -> Self {
        Self { body, pos: 0 }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(len)
            .ok_or_else(|| WireError::Protocol("message-body offset overflow".to_string()))?;
        if end > self.body.len() {
            return Err(WireError::Protocol(format!(
                "truncated message body: need {len} bytes at offset {}",
                self.pos
            )));
        }
        let bytes = &self.body[self.pos..end];
        self.pos = end;
        Ok(bytes)
    }

    fn byte(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn i16(&mut self) -> Result<i16> {
        Ok(i16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }

    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn i32(&mut self) -> Result<i32> {
        Ok(i32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn count(&mut self, _what: &str) -> Result<usize> {
        Ok(self.u16()? as usize)
    }

    fn cstring(&mut self, what: &str) -> Result<String> {
        let rest = &self.body[self.pos..];
        let Some(nul) = rest.iter().position(|&b| b == 0) else {
            return Err(WireError::Protocol(format!(
                "{what} missing NUL terminator"
            )));
        };
        let value = std::str::from_utf8(&rest[..nul])
            .map_err(|_| WireError::Protocol(format!("{what} is not UTF-8")))?
            .to_string();
        self.pos += nul + 1;
        Ok(value)
    }

    fn finish(self) -> Result<()> {
        if self.pos != self.body.len() {
            return Err(WireError::Protocol(format!(
                "{} trailing bytes after message body",
                self.body.len() - self.pos
            )));
        }
        Ok(())
    }
}

fn object_kind(byte: u8) -> Result<ObjectKind> {
    match byte {
        b'S' => Ok(ObjectKind::Statement),
        b'P' => Ok(ObjectKind::Portal),
        other => Err(WireError::Protocol(format!(
            "invalid Describe/Close object kind 0x{other:02x}"
        ))),
    }
}

/// Read one regular-phase frontend message. Returns `Ok(None)` on a clean
/// EOF before the tag byte (client closed the socket).
pub fn read_message<R: Read>(r: &mut R) -> Result<Option<FrontendMessage>> {
    let mut tag = [0u8; 1];
    if read_exact_or_eof(r, &mut tag)?.is_none() {
        return Ok(None);
    }
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf)?;
    let len = u32::from_be_bytes(len_buf);
    let body = read_body(r, len, 4, MAX_MESSAGE_LEN)?;
    let mut reader = BodyReader::new(&body);
    let msg = match tag[0] {
        b'Q' => {
            let sql = (|| -> Result<String> {
                let sql = reader.cstring("query string")?;
                reader.finish()?;
                Ok(sql)
            })()
            .map_err(|error| match error {
                WireError::Protocol(message) => WireError::SimpleQuery(message),
                other => other,
            })?;
            FrontendMessage::Query(sql)
        }
        b'P' => {
            let name = reader.cstring("statement name")?;
            let query = reader.cstring("query string")?;
            let count = reader.count("parameter type")?;
            let mut parameter_type_oids = Vec::with_capacity(count);
            for _ in 0..count {
                parameter_type_oids.push(reader.u32()?);
            }
            reader.finish()?;
            FrontendMessage::Parse {
                name,
                query,
                parameter_type_oids,
            }
        }
        b'B' => {
            let portal = reader.cstring("portal name")?;
            let statement = reader.cstring("statement name")?;
            let format_count = reader.count("parameter format")?;
            let mut parameter_formats = Vec::with_capacity(format_count);
            for _ in 0..format_count {
                parameter_formats.push(reader.i16()?);
            }
            let parameter_count = reader.count("parameter")?;
            let mut parameters = Vec::with_capacity(parameter_count);
            for _ in 0..parameter_count {
                let len = reader.i32()?;
                let value = match len {
                    -1 => None,
                    0.. => Some(reader.take(len as usize)?.to_vec()),
                    _ => {
                        return Err(WireError::Protocol(format!(
                            "invalid parameter length {len}"
                        )))
                    }
                };
                parameters.push(value);
            }
            let result_count = reader.count("result format")?;
            let mut result_formats = Vec::with_capacity(result_count);
            for _ in 0..result_count {
                result_formats.push(reader.i16()?);
            }
            reader.finish()?;
            FrontendMessage::Bind {
                portal,
                statement,
                parameter_formats,
                parameters,
                result_formats,
            }
        }
        b'D' => {
            let kind = object_kind(reader.byte()?)?;
            let name = reader.cstring("Describe object name")?;
            reader.finish()?;
            FrontendMessage::Describe { kind, name }
        }
        b'E' => {
            let portal = reader.cstring("portal name")?;
            let max_rows = reader.u32()?;
            reader.finish()?;
            FrontendMessage::Execute { portal, max_rows }
        }
        b'C' => {
            let kind = object_kind(reader.byte()?)?;
            let name = reader.cstring("Close object name")?;
            reader.finish()?;
            FrontendMessage::Close { kind, name }
        }
        b'H' => {
            reader.finish()?;
            FrontendMessage::Flush
        }
        b'S' => {
            reader.finish().map_err(|error| match error {
                WireError::Protocol(message) => WireError::Sync(message),
                other => other,
            })?;
            FrontendMessage::Sync
        }
        b'X' => {
            reader.finish()?;
            FrontendMessage::Terminate
        }
        other => FrontendMessage::Unsupported { tag: other },
    };
    Ok(Some(msg))
}

// ─── Backend encode ────────────────────────────────────────────────────

fn put_msg(buf: &mut Vec<u8>, tag: u8, body: &[u8]) {
    buf.push(tag);
    buf.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
    buf.extend_from_slice(body);
}

fn put_cstring(body: &mut Vec<u8>, s: &str) {
    debug_assert!(
        !s.contains('\0'),
        "wire strings must not contain NUL: {s:?}"
    );
    body.extend_from_slice(s.as_bytes());
    body.push(0);
}

/// `AuthenticationOk` — trust mode: the only auth message ever sent (§7.2).
pub fn authentication_ok(buf: &mut Vec<u8>) {
    put_msg(buf, b'R', &0u32.to_be_bytes());
}

/// `ParameterStatus(name, value)`.
pub fn parameter_status(buf: &mut Vec<u8>, name: &str, value: &str) {
    let mut body = Vec::new();
    put_cstring(&mut body, name);
    put_cstring(&mut body, value);
    put_msg(buf, b'S', &body);
}

/// `ReadyForQuery(status)` — `status` is [`STATUS_IDLE`],
/// [`STATUS_IN_TXN`], or [`STATUS_FAILED`] after an explicit-transaction
/// statement error has aborted the engine handle.
pub fn ready_for_query(buf: &mut Vec<u8>, status: u8) {
    put_msg(buf, b'Z', &[status]);
}

/// `ParseComplete` — acknowledges a Parse message.
pub fn parse_complete(buf: &mut Vec<u8>) {
    put_msg(buf, b'1', &[]);
}

/// `BindComplete` — acknowledges a Bind message.
pub fn bind_complete(buf: &mut Vec<u8>) {
    put_msg(buf, b'2', &[]);
}

/// `CloseComplete` — acknowledges a Close message.
pub fn close_complete(buf: &mut Vec<u8>) {
    put_msg(buf, b'3', &[]);
}

/// `ParameterDescription` — the inferred or client-specified statement
/// parameter OIDs, in `$1..$n` order.
pub fn parameter_description(buf: &mut Vec<u8>, type_oids: &[u32]) {
    let mut body = Vec::new();
    body.extend_from_slice(&(type_oids.len() as i16).to_be_bytes());
    for oid in type_oids {
        body.extend_from_slice(&oid.to_be_bytes());
    }
    put_msg(buf, b't', &body);
}

/// `NoData` — a Describe target has no result rows.
pub fn no_data(buf: &mut Vec<u8>) {
    put_msg(buf, b'n', &[]);
}

/// `PortalSuspended` — Execute reached its nonzero row limit.
pub fn portal_suspended(buf: &mut Vec<u8>) {
    put_msg(buf, b's', &[]);
}

/// `RowDescription` for a SELECT result set.
pub fn row_description(buf: &mut Vec<u8>, fields: &[FieldDesc]) {
    let mut body = Vec::new();
    body.extend_from_slice(&(fields.len() as i16).to_be_bytes());
    for f in fields {
        put_cstring(&mut body, &f.name);
        body.extend_from_slice(&f.table_oid.to_be_bytes());
        body.extend_from_slice(&f.attnum.to_be_bytes());
        body.extend_from_slice(&f.type_oid.to_be_bytes());
        body.extend_from_slice(&f.type_len.to_be_bytes());
        body.extend_from_slice(&f.type_modifier.to_be_bytes());
        body.extend_from_slice(&f.format.to_be_bytes());
    }
    put_msg(buf, b'T', &body);
}

/// `DataRow` in text format. `None` is the protocol NULL marker (length -1).
pub fn data_row(buf: &mut Vec<u8>, values: &[Option<&[u8]>]) {
    let mut body = Vec::new();
    body.extend_from_slice(&(values.len() as i16).to_be_bytes());
    for v in values {
        match v {
            None => body.extend_from_slice(&(-1i32).to_be_bytes()),
            Some(bytes) => {
                body.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                body.extend_from_slice(bytes);
            }
        }
    }
    put_msg(buf, b'D', &body);
}

/// `CommandComplete(tag)` — e.g. `SELECT 3`, `INSERT 0 2`, `BEGIN`.
pub fn command_complete(buf: &mut Vec<u8>, tag: &str) {
    let mut body = Vec::new();
    put_cstring(&mut body, tag);
    put_msg(buf, b'C', &body);
}

/// `ErrorResponse` with the three fields every client needs: severity
/// (`S`/`V`), SQLSTATE (`C`), message (`M`).
pub fn error_response(buf: &mut Vec<u8>, severity: &str, sqlstate: &str, message: &str) {
    let mut body = Vec::new();
    body.push(b'S');
    put_cstring(&mut body, severity);
    body.push(b'V');
    put_cstring(&mut body, severity);
    body.push(b'C');
    put_cstring(&mut body, sqlstate);
    body.push(b'M');
    put_cstring(&mut body, message);
    body.push(0);
    put_msg(buf, b'E', &body);
}

/// `NoticeResponse` for non-fatal warnings (`S`/`V`, SQLSTATE `C`, message `M`).
pub fn notice_response(buf: &mut Vec<u8>, severity: &str, sqlstate: &str, message: &str) {
    let mut body = Vec::new();
    body.push(b'S');
    put_cstring(&mut body, severity);
    body.push(b'V');
    put_cstring(&mut body, severity);
    body.push(b'C');
    put_cstring(&mut body, sqlstate);
    body.push(b'M');
    put_cstring(&mut body, message);
    body.push(0);
    put_msg(buf, b'N', &body);
}

/// `EmptyQueryResponse` — answer to an all-whitespace query string.
pub fn empty_query_response(buf: &mut Vec<u8>) {
    put_msg(buf, b'I', &[]);
}
