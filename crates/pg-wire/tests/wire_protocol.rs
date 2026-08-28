//! M3 Stage F (tech-selection §7.2, §7.3) protocol unit tests: codec
//! roundtrips, transaction interception, and type text encoding.
//!
//! Covered:
//!
//! - codec: startup packet framing (v3 / SSLRequest / GSSENCRequest /
//!   CancelRequest), regular-phase framing (`Q` / `X` / unsupported tag),
//!   malformed-frame rejection, golden bytes for every backend encoder of
//!   the minimal set;
//! - transaction interception: BEGIN/COMMIT/ROLLBACK map to
//!   `Engine::begin_txn` / `TxnHandle::commit` / `abort` and NEVER reach
//!   `exec`; BEGIN inside a transaction errors (25001) without disturbing
//!   the live handle; COMMIT/ROLLBACK without a transaction error (25P01);
//! - type text encoding: INT4/INT8 decimal, TEXT as-is, NULL protocol
//!   marker, Timestamptz µs integer, Uuid standard string, Bytea `\x` hex;
//! - multi-statement strings execute in order; the first error aborts the
//!   rest of the string; probe-style statements (`SET`, `SELECT
//!   version()`) error without breaking the session.
//!
//! Acceptance: `cargo test -p pg-wire --test wire_protocol`

use std::io::Cursor;
use std::sync::Arc;

use pg_engine::{ColumnType, Datum, Engine, EngineConfig};
use pg_wire::codec::{
    self, FieldDesc, FrontendMessage, StartupPacket, CANCEL_REQUEST_CODE, GSSENC_REQUEST_CODE,
    PROTOCOL_V3, SSL_REQUEST_CODE, STATUS_IDLE, STATUS_IN_TXN,
};
use pg_wire::types::{column_type_of, encode_text, wire_type};
use pg_wire::{split_statements, Session};
use tempfile::TempDir;

// ─── Test-side message scanner ─────────────────────────────────────────

/// One decoded backend message (tag + body).
struct Msg {
    tag: u8,
    body: Vec<u8>,
}

/// Split a response buffer into its framed backend messages.
fn scan_messages(buf: &[u8]) -> Vec<Msg> {
    let mut msgs = Vec::new();
    let mut rest = buf;
    while !rest.is_empty() {
        let tag = rest[0];
        let len = u32::from_be_bytes(rest[1..5].try_into().unwrap()) as usize;
        let body = rest[5..1 + len].to_vec();
        msgs.push(Msg { tag, body });
        rest = &rest[1 + len..];
    }
    msgs
}

/// The command tag of a CommandComplete message body.
fn command_tag(body: &[u8]) -> &str {
    std::str::from_utf8(&body[..body.len() - 1]).unwrap()
}

/// (sqlstate, message) of an ErrorResponse body.
fn error_fields(body: &[u8]) -> (String, String) {
    let mut sqlstate = String::new();
    let mut message = String::new();
    let mut rest = body;
    while rest[0] != 0 {
        let field = rest[0];
        let nul = rest.iter().position(|&b| b == 0).unwrap();
        let value = std::str::from_utf8(&rest[1..nul]).unwrap().to_string();
        match field {
            b'C' => sqlstate = value,
            b'M' => message = value,
            _ => {}
        }
        rest = &rest[nul + 1..];
    }
    (sqlstate, message)
}

/// Collect the command tags of all CommandComplete messages, in order.
fn command_tags(buf: &[u8]) -> Vec<String> {
    scan_messages(buf)
        .iter()
        .filter(|m| m.tag == b'C')
        .map(|m| command_tag(&m.body).to_string())
        .collect()
}

/// All ErrorResponses in a response buffer.
fn errors(buf: &[u8]) -> Vec<(String, String)> {
    scan_messages(buf)
        .iter()
        .filter(|m| m.tag == b'E')
        .map(|m| error_fields(&m.body))
        .collect()
}

/// The trailing ReadyForQuery status byte.
fn final_status(buf: &[u8]) -> u8 {
    let msgs = scan_messages(buf);
    let last = msgs.last().unwrap();
    assert_eq!(last.tag, b'Z', "last message must be ReadyForQuery");
    last.body[0]
}

fn open_session() -> (TempDir, Arc<Engine>, Session) {
    let tmp = TempDir::new().unwrap();
    let engine = Arc::new(Engine::open(tmp.path(), EngineConfig::new(tmp.path())).unwrap());
    let session = Session::new(Arc::clone(&engine));
    (tmp, engine, session)
}

// ─── Codec: startup phase ──────────────────────────────────────────────

fn startup_bytes(code: u32, body: &[u8]) -> Vec<u8> {
    let len = (8 + body.len()) as u32;
    let mut v = Vec::new();
    v.extend_from_slice(&len.to_be_bytes());
    v.extend_from_slice(&code.to_be_bytes());
    v.extend_from_slice(body);
    v
}

#[test]
fn startup_packet_roundtrip() {
    let mut body = Vec::new();
    for (k, v) in [
        ("user", "alice"),
        ("database", "db1"),
        ("application_name", "psql"),
    ] {
        body.extend_from_slice(k.as_bytes());
        body.push(0);
        body.extend_from_slice(v.as_bytes());
        body.push(0);
    }
    body.push(0);
    let bytes = startup_bytes(PROTOCOL_V3, &body);
    let pkt = read_startup(&bytes).unwrap().unwrap();
    match pkt {
        StartupPacket::Startup { params } => assert_eq!(
            params,
            vec![
                ("user".to_string(), "alice".to_string()),
                ("database".to_string(), "db1".to_string()),
                ("application_name".to_string(), "psql".to_string()),
            ]
        ),
        other => panic!("expected Startup, got {other:?}"),
    }
}

fn read_startup(bytes: &[u8]) -> pg_wire::Result<Option<StartupPacket>> {
    codec::read_startup_packet(&mut Cursor::new(bytes.to_vec()))
}

#[test]
fn startup_special_codes() {
    let ssl = read_startup(&startup_bytes(SSL_REQUEST_CODE, &[])).unwrap();
    assert_eq!(ssl, Some(StartupPacket::SslRequest));
    let gss = read_startup(&startup_bytes(GSSENC_REQUEST_CODE, &[])).unwrap();
    assert_eq!(gss, Some(StartupPacket::GssEncRequest));
    let mut body = Vec::new();
    body.extend_from_slice(&7u32.to_be_bytes());
    body.extend_from_slice(&42u32.to_be_bytes());
    let cancel = read_startup(&startup_bytes(CANCEL_REQUEST_CODE, &body)).unwrap();
    assert_eq!(
        cancel,
        Some(StartupPacket::CancelRequest { pid: 7, key: 42 })
    );
    // Clean EOF before any byte is a normal hangup, not an error.
    assert_eq!(read_startup(&[]).unwrap(), None);
}

#[test]
fn startup_malformed_rejected() {
    // Unknown protocol version.
    assert!(read_startup(&startup_bytes(0x0004_0000, &[0])).is_err());
    // Missing terminator.
    let bad = startup_bytes(PROTOCOL_V3, b"user\0alice\0");
    assert!(read_startup(&bad[..bad.len() - 1]).is_err());
    // Trailing garbage after the terminator.
    let bad = startup_bytes(PROTOCOL_V3, b"\0junk");
    assert!(read_startup(&bad).is_err());
    // Absurd length word.
    let mut bad = Vec::new();
    bad.extend_from_slice(&u32::MAX.to_be_bytes());
    assert!(read_startup(&bad).is_err());
    // Truncated body (length promised more than arrived).
    let mut bad = startup_bytes(PROTOCOL_V3, b"\0");
    bad.truncate(bad.len() - 1);
    assert!(read_startup(&bad).is_err());
}

// ─── Codec: regular phase ──────────────────────────────────────────────

fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut v = vec![tag];
    v.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
    v.extend_from_slice(body);
    v
}

fn read_msg(bytes: &[u8]) -> pg_wire::Result<Option<FrontendMessage>> {
    codec::read_message(&mut Cursor::new(bytes.to_vec()))
}

#[test]
fn query_and_terminate_roundtrip() {
    let msg = read_msg(&frame(b'Q', b"SELECT 1\0")).unwrap();
    assert_eq!(msg, Some(FrontendMessage::Query("SELECT 1".to_string())));
    let msg = read_msg(&frame(b'X', &[])).unwrap();
    assert_eq!(msg, Some(FrontendMessage::Terminate));
    // Extended-protocol tags stay framed but surface as Unsupported.
    let msg = read_msg(&frame(b'P', b"\0\0\0\0\0")).unwrap();
    assert_eq!(msg, Some(FrontendMessage::Unsupported { tag: b'P' }));
    // Clean EOF is a normal close.
    assert_eq!(read_msg(&[]).unwrap(), None);
}

#[test]
fn message_malformed_rejected() {
    // Query string without NUL terminator.
    assert!(read_msg(&frame(b'Q', b"SELECT 1")).is_err());
    // Non-UTF8 query string.
    assert!(read_msg(&frame(b'Q', &[0xff, 0xfe, 0])).is_err());
    // Length word below the self-inclusive minimum.
    assert!(read_msg(&[b'Q', 0, 0, 0, 2]).is_err());
    // Truncated body.
    let mut short = frame(b'Q', b"SELECT 1\0");
    short.truncate(short.len() - 3);
    assert!(read_msg(&short).is_err());
}

/// F1 (Stage F review): an absurd declared length must be REJECTED by the
/// cap — never honored by a pre-allocation. The header alone (5 bytes, no
/// body) is enough input: if the decoder tried to read the promised body
/// it would surface an I/O error instead of a protocol error.
#[test]
fn oversized_declared_length_rejected_before_allocation() {
    // 1 GiB claim (the old cap): rejected outright now.
    let err = read_msg(&[b'Q', 0x40, 0x00, 0x00, 0x04]).unwrap_err();
    assert!(
        matches!(err, pg_wire::WireError::Protocol(_)),
        "expected Protocol error, got {err:?}"
    );
    // One past the 64 MiB cap: rejected.
    let len = (codec::MAX_MESSAGE_LEN + 1).to_be_bytes();
    let err = read_msg(&[b'Q', len[0], len[1], len[2], len[3]]).unwrap_err();
    assert!(matches!(err, pg_wire::WireError::Protocol(_)));
    // At the cap but body never arrives: an I/O error (the cap passed),
    // and the chunked reader allocated nothing beyond one 8 KiB chunk.
    let len = codec::MAX_MESSAGE_LEN.to_be_bytes();
    let err = read_msg(&[b'Q', len[0], len[1], len[2], len[3]]).unwrap_err();
    assert!(matches!(err, pg_wire::WireError::Io(_)), "got {err:?}");

    // Startup packets: the PG MAX_STARTUP_PACKET_LENGTH cap (10000).
    let len = (codec::MAX_STARTUP_PACKET_LEN + 1).to_be_bytes();
    let err = read_startup(&len).unwrap_err();
    assert!(matches!(err, pg_wire::WireError::Protocol(_)));
    // Exactly at the cap with a truncated body: cap passes, I/O errors.
    let len = codec::MAX_STARTUP_PACKET_LEN.to_be_bytes();
    let err = read_startup(&len).unwrap_err();
    assert!(matches!(err, pg_wire::WireError::Io(_)), "got {err:?}");
}

/// F4 (Stage F review): bytes after the query string's NUL terminator are
/// a framing bug in the peer, not payload to silently drop (PG: "invalid
/// string in message").
#[test]
fn query_trailing_bytes_after_nul_rejected() {
    let err = read_msg(&frame(b'Q', b"SELECT 1\0junk")).unwrap_err();
    assert!(
        matches!(err, pg_wire::WireError::Protocol(_)),
        "expected Protocol error, got {err:?}"
    );
}

// ─── Codec: backend encoders (golden bytes) ────────────────────────────

#[test]
fn backend_encoder_golden_bytes() {
    let mut buf = Vec::new();
    codec::authentication_ok(&mut buf);
    assert_eq!(buf, [b'R', 0, 0, 0, 8, 0, 0, 0, 0]);

    buf.clear();
    codec::ready_for_query(&mut buf, STATUS_IDLE);
    assert_eq!(buf, [b'Z', 0, 0, 0, 5, b'I']);

    buf.clear();
    codec::parameter_status(&mut buf, "server_version", "16.0");
    let mut expected = vec![b'S', 0, 0, 0, 24];
    expected.extend_from_slice(b"server_version\x0016.0\0");
    assert_eq!(buf, expected);

    buf.clear();
    codec::command_complete(&mut buf, "SELECT 3");
    let mut expected = vec![b'C', 0, 0, 0, 13];
    expected.extend_from_slice(b"SELECT 3\0");
    assert_eq!(buf, expected);

    buf.clear();
    codec::empty_query_response(&mut buf);
    assert_eq!(buf, [b'I', 0, 0, 0, 4]);

    buf.clear();
    codec::error_response(&mut buf, "ERROR", "42P01", "no such table");
    let msgs = scan_messages(&buf);
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].tag, b'E');
    assert_eq!(
        error_fields(&msgs[0].body),
        ("42P01".to_string(), "no such table".to_string())
    );
}

#[test]
fn row_description_and_data_row_structure() {
    let fields = vec![
        FieldDesc {
            name: "id".to_string(),
            table_oid: 16384,
            attnum: 1,
            type_oid: 23,
            type_len: 4,
            type_modifier: -1,
            format: 0,
        },
        FieldDesc {
            name: "name".to_string(),
            table_oid: 16384,
            attnum: 2,
            type_oid: 25,
            type_len: -1,
            type_modifier: -1,
            format: 0,
        },
    ];
    let mut buf = Vec::new();
    codec::row_description(&mut buf, &fields);
    let msgs = scan_messages(&buf);
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].tag, b'T');
    let body = &msgs[0].body;
    assert_eq!(i16::from_be_bytes(body[0..2].try_into().unwrap()), 2);
    // Walk the two field descriptors.
    let mut rest = &body[2..];
    let mut names = Vec::new();
    let mut oids = Vec::new();
    for _ in 0..2 {
        let nul = rest.iter().position(|&b| b == 0).unwrap();
        names.push(std::str::from_utf8(&rest[..nul]).unwrap().to_string());
        rest = &rest[nul + 1..];
        assert_eq!(u32::from_be_bytes(rest[0..4].try_into().unwrap()), 16384);
        rest = &rest[4..];
        rest = &rest[2..]; // attnum
        oids.push(u32::from_be_bytes(rest[0..4].try_into().unwrap()));
        rest = &rest[4..];
        rest = &rest[2..]; // typlen
        assert_eq!(i32::from_be_bytes(rest[0..4].try_into().unwrap()), -1);
        rest = &rest[4..];
        assert_eq!(i16::from_be_bytes(rest[0..2].try_into().unwrap()), 0);
        rest = &rest[2..];
    }
    assert_eq!(names, ["id", "name"]);
    assert_eq!(oids, [23, 25]);
    assert!(rest.is_empty());

    // DataRow: value, NULL (-1 length), value.
    let mut buf = Vec::new();
    codec::data_row(&mut buf, &[Some(b"42"), None, Some(b"alice")]);
    let msgs = scan_messages(&buf);
    assert_eq!(msgs[0].tag, b'D');
    let body = &msgs[0].body;
    assert_eq!(i16::from_be_bytes(body[0..2].try_into().unwrap()), 3);
    assert_eq!(i32::from_be_bytes(body[2..6].try_into().unwrap()), 2);
    assert_eq!(&body[6..8], b"42");
    assert_eq!(i32::from_be_bytes(body[8..12].try_into().unwrap()), -1);
    assert_eq!(i32::from_be_bytes(body[12..16].try_into().unwrap()), 5);
    assert_eq!(&body[16..21], b"alice");
    assert_eq!(body.len(), 21);
}

// ─── Type text encoding (§7.2) ─────────────────────────────────────────

#[test]
fn type_text_encoding_roundtrips() {
    assert_eq!(encode_text(&Datum::Int4(-42)).unwrap(), b"-42");
    assert_eq!(
        encode_text(&Datum::Int8(i64::MAX)).unwrap(),
        b"9223372036854775807"
    );
    assert_eq!(
        encode_text(&Datum::Text("héllo".to_string())).unwrap(),
        "héllo".as_bytes()
    );
    assert_eq!(
        encode_text(&Datum::Timestamptz(1_700_000_000_123_456)).unwrap(),
        b"1700000000123456"
    );
    let uuid = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
    assert_eq!(
        encode_text(&Datum::Uuid(uuid)).unwrap(),
        b"550e8400-e29b-41d4-a716-446655440000"
    );
    assert_eq!(
        encode_text(&Datum::Bytea(vec![0x00, 0x0f, 0xde, 0xad, 0xbe, 0xef])).unwrap(),
        b"\\x000fdeadbeef"
    );
    // Empty bytea still carries the \x marker (PG text format).
    assert_eq!(encode_text(&Datum::Bytea(vec![])).unwrap(), b"\\x");
}

#[test]
fn wire_type_mapping_matches_encoding() {
    // The reported OID must match the bytes on the wire (types.rs docs).
    assert_eq!(
        wire_type(ColumnType::Int4),
        pg_wire::types::WireType { oid: 23, len: 4 }
    );
    assert_eq!(
        wire_type(ColumnType::Int8),
        pg_wire::types::WireType { oid: 20, len: 8 }
    );
    assert_eq!(
        wire_type(ColumnType::Text),
        pg_wire::types::WireType { oid: 25, len: -1 }
    );
    assert_eq!(
        wire_type(ColumnType::Bytea),
        pg_wire::types::WireType { oid: 17, len: -1 }
    );
    // µs integer is presented as INT8; the standard UUID string as TEXT.
    assert_eq!(
        wire_type(ColumnType::Timestamptz),
        pg_wire::types::WireType { oid: 20, len: 8 }
    );
    assert_eq!(
        wire_type(ColumnType::Uuid),
        pg_wire::types::WireType { oid: 25, len: -1 }
    );

    assert_eq!(column_type_of(&Datum::Int4(1)), ColumnType::Int4);
    assert_eq!(
        column_type_of(&Datum::Timestamptz(0)),
        ColumnType::Timestamptz
    );
}

#[test]
fn null_is_protocol_null_marker() {
    // NULL at the value level encodes as the -1 length word, never as text.
    let mut buf = Vec::new();
    codec::data_row(&mut buf, &[None]);
    let body = &scan_messages(&buf)[0].body;
    assert_eq!(i32::from_be_bytes(body[2..6].try_into().unwrap()), -1);
}

// ─── Statement splitting ───────────────────────────────────────────────

#[test]
fn split_statements_respects_string_literals() {
    assert_eq!(split_statements(""), [""]);
    assert_eq!(split_statements("SELECT 1"), ["SELECT 1"]);
    assert_eq!(
        split_statements("INSERT INTO t VALUES ('a;b'); SELECT * FROM t;"),
        ["INSERT INTO t VALUES ('a;b')", " SELECT * FROM t", ""]
    );
    // Doubled quote is an escape, not a terminator.
    assert_eq!(
        split_statements("INSERT INTO t VALUES ('a'';b'); SELECT * FROM t"),
        ["INSERT INTO t VALUES ('a'';b')", " SELECT * FROM t"]
    );
}

// ─── Transaction interception (§7.2) ───────────────────────────────────

/// F8 (Stage F review): a connection dying mid-transaction must reclaim
/// the XID — `TxnHandle::drop` auto-aborts, so nothing lingers in the
/// active set to pin the vacuum horizon.
#[test]
fn drop_session_mid_txn_reclaims_xid() {
    let (_tmp, engine, mut session) = open_session();
    let mut out = Vec::new();
    session.handle_query("BEGIN; INSERT INTO t VALUES (1)", &mut out);
    // (INSERT errors — table does not exist — but the BEGIN already took
    // a live handle, which is all this test needs.)
    assert_eq!(engine.active_xids().len(), 1);
    drop(session); // connection hangup
    assert_eq!(engine.active_xids().len(), 0);
}

/// F8 (Stage F review): CancelRequest is consumed at the session level by
/// closing the connection (codec-level decode is covered above).
#[test]
fn cancel_request_closes_connection() {
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};

    let (_tmp, engine, _session) = open_session();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut session = Session::new(engine);
        session.run(&stream)
    });

    let mut client = TcpStream::connect(addr).unwrap();
    client
        .set_read_timeout(Some(std::time::Duration::from_secs(10)))
        .unwrap();
    // CancelRequest: len=16, code 80877102, pid, key.
    let mut pkt = Vec::new();
    pkt.extend_from_slice(&16u32.to_be_bytes());
    pkt.extend_from_slice(&codec::CANCEL_REQUEST_CODE.to_be_bytes());
    pkt.extend_from_slice(&7u32.to_be_bytes());
    pkt.extend_from_slice(&42u32.to_be_bytes());
    client.write_all(&pkt).unwrap();
    // The server closes without answering: the client reads clean EOF.
    let mut buf = [0u8; 1];
    assert_eq!(client.read(&mut buf).unwrap(), 0);
    server.join().unwrap().unwrap();
}

#[test]
fn txn_lifecycle_maps_to_programmatic_api() {
    let (_tmp, engine, mut session) = open_session();
    let mut out = Vec::new();

    session.handle_query("CREATE TABLE t (id INT)", &mut out);
    assert_eq!(command_tags(&out), ["CREATE TABLE"]);
    assert_eq!(final_status(&out), STATUS_IDLE);

    // BEGIN → Engine::begin_txn (one XID enters the active set).
    out.clear();
    session.handle_query("BEGIN", &mut out);
    assert_eq!(command_tags(&out), ["BEGIN"]);
    assert_eq!(final_status(&out), STATUS_IN_TXN);
    assert_eq!(engine.active_xids().len(), 1);

    // Work inside the txn, then COMMIT → TxnHandle::commit.
    out.clear();
    session.handle_query("INSERT INTO t VALUES (1); COMMIT", &mut out);
    assert_eq!(command_tags(&out), ["INSERT 0 1", "COMMIT"]);
    assert_eq!(final_status(&out), STATUS_IDLE);
    assert_eq!(engine.active_xids().len(), 0);

    // The commit is durable and visible to a fresh statement.
    out.clear();
    session.handle_query("SELECT * FROM t", &mut out);
    assert_eq!(command_tags(&out), ["SELECT 1"]);

    // The intercepted statements never reached exec: query stats (which
    // record EVERY exec'd statement) hold no BEGIN/COMMIT entries.
    let entries = engine.query_stats().entries();
    let recorded: Vec<&str> = entries.iter().map(|e| e.query.as_str()).collect();
    assert!(
        !recorded.contains(&"BEGIN"),
        "BEGIN must not reach exec: {recorded:?}"
    );
    assert!(
        !recorded.contains(&"COMMIT"),
        "COMMIT must not reach exec: {recorded:?}"
    );
}

#[test]
fn rollback_undoes_the_transaction() {
    let (_tmp, _engine, mut session) = open_session();
    let mut out = Vec::new();
    session.handle_query(
        "CREATE TABLE t (id INT); INSERT INTO t VALUES (1)",
        &mut out,
    );

    out.clear();
    session.handle_query("BEGIN; INSERT INTO t VALUES (2); ROLLBACK", &mut out);
    assert_eq!(command_tags(&out), ["BEGIN", "INSERT 0 1", "ROLLBACK"]);
    assert_eq!(final_status(&out), STATUS_IDLE);

    out.clear();
    session.handle_query("SELECT * FROM t", &mut out);
    assert_eq!(command_tags(&out), ["SELECT 1"]); // only the committed row
}

#[test]
fn begin_inside_txn_errors_without_breaking_state() {
    let (_tmp, _engine, mut session) = open_session();
    let mut out = Vec::new();
    session.handle_query("CREATE TABLE t (id INT)", &mut out);

    out.clear();
    session.handle_query("BEGIN; BEGIN", &mut out);
    let errs = errors(&out);
    assert_eq!(errs.len(), 1);
    assert_eq!(errs[0].0, "25001");
    // The live handle is untouched: still in a transaction...
    assert_eq!(final_status(&out), STATUS_IN_TXN);
    // ...and still usable.
    out.clear();
    session.handle_query("INSERT INTO t VALUES (7); COMMIT", &mut out);
    assert_eq!(command_tags(&out), ["INSERT 0 1", "COMMIT"]);
}

#[test]
fn commit_and_rollback_without_txn_error() {
    let (_tmp, _engine, mut session) = open_session();
    let mut out = Vec::new();
    session.handle_query("COMMIT", &mut out);
    assert_eq!(errors(&out)[0].0, "25P01");
    assert_eq!(final_status(&out), STATUS_IDLE);
    out.clear();
    session.handle_query("ROLLBACK", &mut out);
    assert_eq!(errors(&out)[0].0, "25P01");
}

// ─── Multi-statement / error semantics ─────────────────────────────────

#[test]
fn multi_statement_executes_in_order_auto_commit() {
    let (_tmp, _engine, mut session) = open_session();
    let mut out = Vec::new();
    session.handle_query(
        "CREATE TABLE t (id INT, name TEXT); INSERT INTO t VALUES (1, 'a'), (2, 'b'); UPDATE t SET name = 'c' WHERE id = 2; SELECT * FROM t",
        &mut out,
    );
    assert_eq!(
        command_tags(&out),
        ["CREATE TABLE", "INSERT 0 2", "UPDATE 1", "SELECT 2"]
    );
    assert_eq!(errors(&out), vec![]);
    // One ReadyForQuery for the whole string.
    assert_eq!(
        scan_messages(&out).iter().filter(|m| m.tag == b'Z').count(),
        1
    );
}

#[test]
fn statement_error_aborts_rest_of_string_but_not_session() {
    let (_tmp, _engine, mut session) = open_session();
    let mut out = Vec::new();
    session.handle_query("CREATE TABLE t (id INT)", &mut out);

    out.clear();
    session.handle_query(
        "INSERT INTO t VALUES (1); SELECT * FROM nosuch; INSERT INTO t VALUES (2)",
        &mut out,
    );
    // First statement applied, second failed (undefined table), third skipped.
    assert_eq!(command_tags(&out), ["INSERT 0 1"]);
    let errs = errors(&out);
    assert_eq!(errs.len(), 1);
    assert_eq!(errs[0].0, "42P01");

    // The session is fully usable afterwards.
    out.clear();
    session.handle_query("INSERT INTO t VALUES (3); SELECT * FROM t", &mut out);
    assert_eq!(command_tags(&out), ["INSERT 0 1", "SELECT 2"]);
}

#[test]
fn probe_statements_error_without_breaking_session() {
    let (_tmp, _engine, mut session) = open_session();
    let mut out = Vec::new();
    // Driver/psql startup probes outside the SQL subset.
    for probe in [
        "SET client_encoding = 'UTF8'",
        "SELECT version()",
        "SET search_path = public",
    ] {
        out.clear();
        session.handle_query(probe, &mut out);
        assert_eq!(errors(&out).len(), 1, "probe must error: {probe}");
        // Parse errors map to the syntax-error class.
        assert_eq!(errors(&out)[0].0, "42601");
        assert_eq!(final_status(&out), STATUS_IDLE);
    }
    // Connection still works.
    out.clear();
    session.handle_query("CREATE TABLE t (id INT)", &mut out);
    assert_eq!(command_tags(&out), ["CREATE TABLE"]);
}

#[test]
fn empty_query_gets_empty_query_response() {
    let (_tmp, _engine, mut session) = open_session();
    let mut out = Vec::new();
    session.handle_query("", &mut out);
    let msgs = scan_messages(&out);
    assert_eq!(msgs.len(), 2);
    assert_eq!(msgs[0].tag, b'I');
    assert_eq!(msgs[1].tag, b'Z');
    // Whitespace-and-semicolons only is empty too.
    out.clear();
    session.handle_query("  ; ; ", &mut out);
    assert_eq!(scan_messages(&out)[0].tag, b'I');
}

// ─── End-to-end result rendering (typed rows over the codec) ───────────

#[test]
fn select_result_carries_schema_types_and_text_values() {
    let (_tmp, engine, mut session) = open_session();
    let mut out = Vec::new();
    session.handle_query(
        "CREATE TABLE t (id INT, big BIGINT, name TEXT, ts TIMESTAMPTZ, u UUID, b BYTEA)",
        &mut out,
    );
    // The SQL subset has no bytea/timestamptz/uuid literals: seed those
    // through the typed API (the wire never gains execution logic).
    engine
        .insert(
            "t",
            &[
                Some(Datum::Int4(7)),
                Some(Datum::Int8(-9_000_000_000)),
                Some(Datum::Text("alice".to_string())),
                Some(Datum::Timestamptz(1_234_567)),
                Some(Datum::Uuid(
                    uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap(),
                )),
                Some(Datum::Bytea(vec![0xde, 0xad])),
            ],
        )
        .unwrap();
    engine
        .insert("t", &[Some(Datum::Int4(8)), None, None, None, None, None])
        .unwrap();

    out.clear();
    session.handle_query("SELECT * FROM t ORDER BY id", &mut out);
    let msgs = scan_messages(&out);
    assert_eq!(msgs[0].tag, b'T');
    // RowDescription reports schema types with the encoding-matched OIDs.
    let body = &msgs[0].body;
    let mut rest = &body[2..];
    let mut oids = Vec::new();
    for _ in 0..6 {
        let nul = rest.iter().position(|&b| b == 0).unwrap();
        rest = &rest[nul + 1..];
        assert_ne!(u32::from_be_bytes(rest[0..4].try_into().unwrap()), 0); // real table oid
        rest = &rest[4..];
        rest = &rest[2..];
        oids.push(u32::from_be_bytes(rest[0..4].try_into().unwrap()));
        rest = &rest[4 + 2 + 4 + 2..];
    }
    assert_eq!(oids, [23, 20, 25, 20, 25, 17]);

    let rows: Vec<&Msg> = msgs.iter().filter(|m| m.tag == b'D').collect();
    assert_eq!(rows.len(), 2);
    // First row: every type in its §7.2 text form.
    let mut vals = Vec::new();
    let mut rest = &rows[0].body[2..];
    for _ in 0..6 {
        let len = i32::from_be_bytes(rest[0..4].try_into().unwrap());
        rest = &rest[4..];
        if len >= 0 {
            vals.push(Some(
                std::str::from_utf8(&rest[..len as usize])
                    .unwrap()
                    .to_string(),
            ));
            rest = &rest[len as usize..];
        } else {
            vals.push(None);
        }
    }
    assert_eq!(
        vals,
        [
            Some("7".to_string()),
            Some("-9000000000".to_string()),
            Some("alice".to_string()),
            Some("1234567".to_string()),
            Some("550e8400-e29b-41d4-a716-446655440000".to_string()),
            Some("\\xdead".to_string()),
        ]
    );
    // Second row: NULLs are protocol null markers.
    let body = &rows[1].body;
    let mut rest = &body[2..];
    let mut nulls = 0;
    for _ in 0..6 {
        let len = i32::from_be_bytes(rest[0..4].try_into().unwrap());
        rest = &rest[4..];
        if len < 0 {
            nulls += 1;
        } else {
            rest = &rest[len as usize..];
        }
    }
    assert_eq!(nulls, 5);
    assert_eq!(command_tags(&out), ["SELECT 2"]);
}
