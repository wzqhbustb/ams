//! Phase 4a Stage C transport-independent Extended Query tests.

use std::sync::Arc;

use pg_engine::{Engine, EngineConfig, ExecutionPath};
use pg_wire::codec::{FieldDesc, FrontendMessage, ObjectKind, STATUS_IDLE, STATUS_IN_TXN};
use pg_wire::types::{FORMAT_BINARY, FORMAT_TEXT};
use pg_wire::Session;
use tempfile::TempDir;

#[derive(Debug)]
struct Msg {
    tag: u8,
    body: Vec<u8>,
}

fn scan_messages(buf: &[u8]) -> Vec<Msg> {
    let mut messages = Vec::new();
    let mut rest = buf;
    while !rest.is_empty() {
        assert!(rest.len() >= 5, "truncated backend header");
        let tag = rest[0];
        let len = u32::from_be_bytes(rest[1..5].try_into().unwrap()) as usize;
        assert!(len >= 4 && rest.len() > len, "truncated backend body");
        messages.push(Msg {
            tag,
            body: rest[5..len + 1].to_vec(),
        });
        rest = &rest[len + 1..];
    }
    messages
}

fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut bytes = vec![tag];
    bytes.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
    bytes.extend_from_slice(body);
    bytes
}

fn tags(buf: &[u8]) -> Vec<u8> {
    scan_messages(buf).iter().map(|msg| msg.tag).collect()
}

fn error_field(body: &[u8], wanted: u8) -> String {
    let mut rest = body;
    while rest[0] != 0 {
        let field = rest[0];
        let nul = rest.iter().position(|&byte| byte == 0).unwrap();
        let value = std::str::from_utf8(&rest[1..nul]).unwrap().to_string();
        if field == wanted {
            return value;
        }
        rest = &rest[nul + 1..];
    }
    panic!("missing ErrorResponse field {}", wanted as char);
}

fn assert_error(buf: &[u8], sqlstate: &str) {
    let messages = scan_messages(buf);
    assert_eq!(
        messages.len(),
        1,
        "expected only ErrorResponse: {messages:?}"
    );
    assert_eq!(messages[0].tag, b'E');
    assert_eq!(error_field(&messages[0].body, b'C'), sqlstate);
}

fn command_tags(buf: &[u8]) -> Vec<String> {
    scan_messages(buf)
        .iter()
        .filter(|msg| msg.tag == b'C')
        .map(|msg| {
            assert_eq!(msg.body.last(), Some(&0));
            std::str::from_utf8(&msg.body[..msg.body.len() - 1])
                .unwrap()
                .to_string()
        })
        .collect()
}

fn row_description(body: &[u8]) -> Vec<FieldDesc> {
    let count = i16::from_be_bytes(body[0..2].try_into().unwrap()) as usize;
    let mut fields = Vec::with_capacity(count);
    let mut rest = &body[2..];
    for _ in 0..count {
        let nul = rest.iter().position(|&byte| byte == 0).unwrap();
        let name = std::str::from_utf8(&rest[..nul]).unwrap().to_string();
        rest = &rest[nul + 1..];
        let field = FieldDesc {
            name,
            table_oid: u32::from_be_bytes(rest[0..4].try_into().unwrap()),
            attnum: i16::from_be_bytes(rest[4..6].try_into().unwrap()),
            type_oid: u32::from_be_bytes(rest[6..10].try_into().unwrap()),
            type_len: i16::from_be_bytes(rest[10..12].try_into().unwrap()),
            type_modifier: i32::from_be_bytes(rest[12..16].try_into().unwrap()),
            format: i16::from_be_bytes(rest[16..18].try_into().unwrap()),
        };
        fields.push(field);
        rest = &rest[18..];
    }
    assert!(rest.is_empty());
    fields
}

fn row_values(body: &[u8]) -> Vec<Option<Vec<u8>>> {
    let count = i16::from_be_bytes(body[0..2].try_into().unwrap()) as usize;
    let mut values = Vec::with_capacity(count);
    let mut rest = &body[2..];
    for _ in 0..count {
        let len = i32::from_be_bytes(rest[0..4].try_into().unwrap());
        rest = &rest[4..];
        if len == -1 {
            values.push(None);
        } else {
            assert!(len >= 0);
            values.push(Some(rest[..len as usize].to_vec()));
            rest = &rest[len as usize..];
        }
    }
    assert!(rest.is_empty());
    values
}

fn parameter_oids(body: &[u8]) -> Vec<u32> {
    let count = i16::from_be_bytes(body[0..2].try_into().unwrap()) as usize;
    assert_eq!(body.len(), 2 + count * 4);
    body[2..]
        .chunks_exact(4)
        .map(|oid| u32::from_be_bytes(oid.try_into().unwrap()))
        .collect()
}

fn open_session() -> (TempDir, Arc<Engine>, Session) {
    let tmp = TempDir::new().unwrap();
    let engine = Arc::new(Engine::open(tmp.path(), EngineConfig::new(tmp.path())).unwrap());
    let session = Session::new(Arc::clone(&engine));
    (tmp, engine, session)
}

fn send(session: &mut Session, message: FrontendMessage) -> Vec<u8> {
    let mut out = Vec::new();
    session.handle_message(message, &mut out);
    out
}

fn simple(session: &mut Session, sql: &str) -> Vec<u8> {
    let mut out = Vec::new();
    session.handle_query(sql, &mut out);
    assert!(!tags(&out).contains(&b'E'), "simple query failed: {sql}");
    out
}

fn parse(name: &str, query: &str, oids: Vec<u32>) -> FrontendMessage {
    FrontendMessage::Parse {
        name: name.to_string(),
        query: query.to_string(),
        parameter_type_oids: oids,
    }
}

fn bind(
    portal: &str,
    statement: &str,
    parameter_formats: Vec<i16>,
    parameters: Vec<Option<Vec<u8>>>,
    result_formats: Vec<i16>,
) -> FrontendMessage {
    FrontendMessage::Bind {
        portal: portal.to_string(),
        statement: statement.to_string(),
        parameter_formats,
        parameters,
        result_formats,
    }
}

fn describe(kind: ObjectKind, name: &str) -> FrontendMessage {
    FrontendMessage::Describe {
        kind,
        name: name.to_string(),
    }
}

fn execute(portal: &str, max_rows: u32) -> FrontendMessage {
    FrontendMessage::Execute {
        portal: portal.to_string(),
        max_rows,
    }
}

fn close(kind: ObjectKind, name: &str) -> FrontendMessage {
    FrontendMessage::Close {
        kind,
        name: name.to_string(),
    }
}

fn sync(session: &mut Session, status: u8) {
    assert_eq!(
        send(session, FrontendMessage::Sync),
        [b'Z', 0, 0, 0, 5, status]
    );
}

#[test]
fn parse_bind_describe_execute_sync_golden() {
    let (_tmp, _engine, mut session) = open_session();
    simple(
        &mut session,
        "CREATE TABLE t (id INT, name TEXT); INSERT INTO t VALUES (7, 'alice')",
    );

    assert_eq!(
        send(
            &mut session,
            parse("s", "SELECT id, name FROM t WHERE id = $1", vec![0]),
        ),
        [b'1', 0, 0, 0, 4]
    );
    let out = send(&mut session, describe(ObjectKind::Statement, "s"));
    let messages = scan_messages(&out);
    assert_eq!(tags(&out), [b't', b'T']);
    assert_eq!(messages[0].body, [0, 1, 0, 0, 0, 23]);
    let fields = row_description(&messages[1].body);
    assert_eq!(fields.len(), 2);
    assert_eq!(fields[0].name, "id");
    assert_eq!(fields[1].name, "name");
    assert_eq!(fields[0].type_oid, 23);
    assert_eq!(fields[1].type_oid, 25);
    assert!(fields.iter().all(|field| field.format == FORMAT_TEXT));
    assert!(fields.iter().all(|field| field.table_oid != 0));
    assert_eq!(fields[0].attnum, 1);
    assert_eq!(fields[1].attnum, 2);

    assert_eq!(
        send(
            &mut session,
            bind(
                "p",
                "s",
                vec![FORMAT_BINARY],
                vec![Some(7i32.to_be_bytes().to_vec())],
                vec![FORMAT_BINARY, FORMAT_TEXT],
            ),
        ),
        [b'2', 0, 0, 0, 4]
    );
    let out = send(&mut session, describe(ObjectKind::Portal, "p"));
    let messages = scan_messages(&out);
    assert_eq!(tags(&out), [b'T']);
    let formats: Vec<i16> = row_description(&messages[0].body)
        .iter()
        .map(|field| field.format)
        .collect();
    assert_eq!(formats, [FORMAT_BINARY, FORMAT_TEXT]);

    // Describing a portal never changes the statement's all-text description.
    let out = send(&mut session, describe(ObjectKind::Statement, "s"));
    let messages = scan_messages(&out);
    assert!(row_description(&messages[1].body)
        .iter()
        .all(|field| field.format == FORMAT_TEXT));

    let out = send(&mut session, execute("p", 0));
    let mut row = vec![0, 2];
    row.extend_from_slice(&4i32.to_be_bytes());
    row.extend_from_slice(&7i32.to_be_bytes());
    row.extend_from_slice(&5i32.to_be_bytes());
    row.extend_from_slice(b"alice");
    let mut expected = frame(b'D', &row);
    expected.extend_from_slice(&frame(b'C', b"SELECT 1\0"));
    assert_eq!(out, expected, "Execute emits no RowDescription");
    sync(&mut session, STATUS_IDLE);

    // Sync ends the implicit transaction and its portal, not the statement.
    assert_error(&send(&mut session, execute("p", 0)), "34000");
    sync(&mut session, STATUS_IDLE);
    assert_eq!(
        tags(&send(&mut session, describe(ObjectKind::Statement, "s"),)),
        [b't', b'T']
    );
}

#[test]
fn named_duplicates_and_unnamed_replacement() {
    let (_tmp, _engine, mut session) = open_session();
    simple(
        &mut session,
        "CREATE TABLE t (id INT); INSERT INTO t VALUES (1), (2)",
    );
    send(
        &mut session,
        parse("first", "SELECT * FROM t WHERE id = 1", vec![]),
    );
    assert_error(
        &send(
            &mut session,
            parse("first", "SELECT * FROM t WHERE id = 2", vec![]),
        ),
        "42P05",
    );
    sync(&mut session, STATUS_IDLE);
    send(
        &mut session,
        parse("second", "SELECT * FROM t WHERE id = 2", vec![]),
    );

    send(&mut session, bind("p", "first", vec![], vec![], vec![]));
    assert_error(
        &send(&mut session, bind("p", "second", vec![], vec![], vec![])),
        "42P03",
    );
    sync(&mut session, STATUS_IDLE);

    // Rebinding the unnamed portal replaces it; named objects are untouched.
    send(&mut session, bind("", "first", vec![], vec![], vec![]));
    send(&mut session, bind("", "second", vec![], vec![], vec![]));
    let out = send(&mut session, execute("", 0));
    let messages = scan_messages(&out);
    assert_eq!(row_values(&messages[0].body), [Some(b"2".to_vec())]);
    assert_eq!(command_tags(&out), ["SELECT 1"]);

    // Reparsing the unnamed statement replaces its dependent portals.
    send(
        &mut session,
        parse("", "SELECT * FROM t WHERE id = 1", vec![]),
    );
    send(&mut session, bind("", "", vec![], vec![], vec![]));
    send(
        &mut session,
        parse("", "SELECT * FROM t WHERE id = 2", vec![]),
    );
    assert_error(&send(&mut session, execute("", 0)), "34000");
    sync(&mut session, STATUS_IDLE);
    send(&mut session, bind("", "", vec![], vec![], vec![]));
    let out = send(&mut session, execute("", 0));
    assert_eq!(
        row_values(&scan_messages(&out)[0].body),
        [Some(b"2".to_vec())]
    );

    // Both named statements survived all unnamed replacements.
    assert_eq!(
        tags(&send(
            &mut session,
            describe(ObjectKind::Statement, "first"),
        )),
        [b't', b'T']
    );
    assert_eq!(
        tags(&send(
            &mut session,
            describe(ObjectKind::Statement, "second"),
        )),
        [b't', b'T']
    );
}

#[test]
fn parse_and_bind_validation_uses_protocol_sqlstates() {
    let (_tmp, _engine, mut session) = open_session();
    simple(&mut session, "CREATE TABLE t (id INT, name TEXT)");
    let sql = "SELECT * FROM t WHERE id = $1";
    for (message, sqlstate) in [
        (parse("", sql, vec![23, 23]), "08P01"),
        (parse("", sql, vec![1022]), "0A000"),
        (parse("", sql, vec![20]), "08P01"),
        (parse("", "SELECT * FROM t WHERE id = $2", vec![]), "0A000"),
    ] {
        assert_error(&send(&mut session, message), sqlstate);
        sync(&mut session, STATUS_IDLE);
    }

    // An explicitly typed unused gap is valid and is still bound by position.
    send(
        &mut session,
        parse("", "SELECT * FROM t WHERE id = $2", vec![23, 23]),
    );
    assert_error(
        &send(
            &mut session,
            bind("", "", vec![2], vec![None, Some(b"1".to_vec())], vec![]),
        ),
        "22023",
    );
    sync(&mut session, STATUS_IDLE);
    assert_error(
        &send(
            &mut session,
            bind(
                "",
                "",
                vec![FORMAT_BINARY, FORMAT_BINARY],
                vec![None, Some(b"1".to_vec())],
                vec![],
            ),
        ),
        "22P03",
    );
    sync(&mut session, STATUS_IDLE);
    send(
        &mut session,
        bind("", "", vec![], vec![None, Some(b"1".to_vec())], vec![]),
    );
    assert_eq!(
        command_tags(&send(&mut session, execute("", 0))),
        ["SELECT 0"]
    );

    send(&mut session, parse("", "SELECT id, name FROM t", vec![]));
    assert_error(
        &send(&mut session, bind("", "", vec![], vec![], vec![0, 1, 0])),
        "08P01",
    );
    sync(&mut session, STATUS_IDLE);
    assert_error(
        &send(&mut session, bind("", "missing", vec![], vec![], vec![])),
        "26000",
    );
    sync(&mut session, STATUS_IDLE);
}

#[test]
fn null_parameter_and_result_use_protocol_null() {
    let (_tmp, _engine, mut session) = open_session();
    simple(&mut session, "CREATE TABLE t (id INT, name TEXT)");
    send(
        &mut session,
        parse("", "INSERT INTO t VALUES ($1, $2)", vec![]),
    );
    send(
        &mut session,
        bind(
            "",
            "",
            vec![FORMAT_TEXT, FORMAT_BINARY],
            vec![Some(b"3".to_vec()), None],
            vec![],
        ),
    );
    let out = send(&mut session, execute("", 0));
    assert_eq!(out, frame(b'C', b"INSERT 0 1\0"));

    send(
        &mut session,
        parse("", "SELECT id, name FROM t WHERE id = $1", vec![]),
    );
    send(
        &mut session,
        bind(
            "",
            "",
            vec![FORMAT_BINARY],
            vec![Some(3i32.to_be_bytes().to_vec())],
            vec![],
        ),
    );
    let out = send(&mut session, execute("", 0));
    let messages = scan_messages(&out);
    assert_eq!(tags(&out), [b'D', b'C']);
    assert_eq!(row_values(&messages[0].body), [Some(b"3".to_vec()), None]);
}

#[test]
fn vector_binary_description_and_data_row_golden() {
    let (_tmp, _engine, mut session) = open_session();
    simple(
        &mut session,
        "CREATE TABLE vectors (id INT, embedding VECTOR(2)); INSERT INTO vectors VALUES (1, [1,2.5])",
    );
    send(
        &mut session,
        parse(
            "v",
            "SELECT id, embedding FROM vectors WHERE id = $1",
            vec![],
        ),
    );
    let out = send(&mut session, describe(ObjectKind::Statement, "v"));
    let messages = scan_messages(&out);
    assert_eq!(parameter_oids(&messages[0].body), [23]);
    let fields = row_description(&messages[1].body);
    assert_eq!(fields[1].type_oid, 9877);
    assert_eq!(fields[1].type_len, -1);
    assert_eq!(fields[1].format, FORMAT_TEXT);

    send(
        &mut session,
        bind(
            "vp",
            "v",
            vec![FORMAT_BINARY],
            vec![Some(1i32.to_be_bytes().to_vec())],
            vec![FORMAT_BINARY],
        ),
    );
    let out = send(&mut session, describe(ObjectKind::Portal, "vp"));
    let fields = row_description(&scan_messages(&out)[0].body);
    assert_eq!(fields[1].type_oid, 9877);
    assert!(fields.iter().all(|field| field.format == FORMAT_BINARY));

    let out = send(&mut session, execute("vp", 0));
    let mut row = vec![0, 2];
    row.extend_from_slice(&4i32.to_be_bytes());
    row.extend_from_slice(&1i32.to_be_bytes());
    row.extend_from_slice(&12i32.to_be_bytes());
    row.extend_from_slice(&2u16.to_be_bytes());
    row.extend_from_slice(&0u16.to_be_bytes());
    row.extend_from_slice(&1.0f32.to_be_bytes());
    row.extend_from_slice(&2.5f32.to_be_bytes());
    let mut expected = frame(b'D', &row);
    expected.extend_from_slice(&frame(b'C', b"SELECT 1\0"));
    assert_eq!(out, expected);
}

#[test]
fn parameterized_ann_uses_vector_oid_and_binary_float8() {
    let (_tmp, engine, mut session) = open_session();
    simple(
        &mut session,
        "CREATE TABLE ann (id INT, embedding VECTOR(2)); \
         CREATE INDEX ann_idx ON ann USING hnsw (embedding vector_l2_ops); \
         INSERT INTO ann VALUES (1, [0,0]), (2, [3,4])",
    );
    let sql = "SELECT id, embedding <-> $1 FROM ann \
               ORDER BY embedding <-> $1 ASC LIMIT 2";
    send(&mut session, parse("ann", sql, vec![0]));
    let out = send(&mut session, describe(ObjectKind::Statement, "ann"));
    let messages = scan_messages(&out);
    assert_eq!(parameter_oids(&messages[0].body), [9877]);
    let fields = row_description(&messages[1].body);
    assert_eq!(fields[0].type_oid, 23);
    assert_eq!(fields[1].type_oid, 701);

    let mut vector = Vec::new();
    vector.extend_from_slice(&2u16.to_be_bytes());
    vector.extend_from_slice(&0u16.to_be_bytes());
    vector.extend_from_slice(&0.0f32.to_be_bytes());
    vector.extend_from_slice(&0.0f32.to_be_bytes());
    send(
        &mut session,
        bind(
            "ap",
            "ann",
            vec![FORMAT_BINARY],
            vec![Some(vector)],
            vec![FORMAT_TEXT, FORMAT_BINARY],
        ),
    );
    let out = send(&mut session, execute("ap", 0));
    let messages = scan_messages(&out);
    assert_eq!(tags(&out), [b'D', b'D', b'C']);
    let first = row_values(&messages[0].body);
    let second = row_values(&messages[1].body);
    assert_eq!(first[0], Some(b"1".to_vec()));
    assert_eq!(first[1].as_deref(), Some(0.0f64.to_be_bytes().as_slice()));
    assert_eq!(second[0], Some(b"2".to_vec()));
    assert_eq!(second[1].as_deref(), Some(5.0f64.to_be_bytes().as_slice()));
    assert_eq!(command_tags(&out), ["SELECT 2"]);
    assert_eq!(
        engine.query_stats().entries().last().unwrap().path,
        ExecutionPath::IndexLookup
    );
}

#[test]
fn extended_error_ignores_everything_until_sync() {
    let (_tmp, _engine, mut session) = open_session();
    simple(&mut session, "CREATE TABLE t (id INT)");
    assert_error(&send(&mut session, execute("missing", 0)), "34000");
    for message in [
        FrontendMessage::Query("INSERT INTO t VALUES (1)".to_string()),
        parse("", "SELECT * FROM t", vec![]),
        FrontendMessage::Flush,
        close(ObjectKind::Statement, "missing"),
        FrontendMessage::Unsupported { tag: b'F' },
        FrontendMessage::Terminate,
    ] {
        assert!(send(&mut session, message).is_empty());
    }
    sync(&mut session, STATUS_IDLE);
    let out = simple(&mut session, "SELECT * FROM t");
    assert_eq!(command_tags(&out), ["SELECT 0"]);

    let out = send(&mut session, FrontendMessage::Unsupported { tag: b'F' });
    let messages = scan_messages(&out);
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0].tag, b'E');
    assert_eq!(error_field(&messages[0].body, b'C'), "0A000");
    assert_eq!(messages[1].tag, b'Z');
    assert_eq!(messages[1].body, [STATUS_IDLE]);
    assert_error(
        &send(&mut session, parse("", "SELECT * FROM missing", vec![])),
        "42P01",
    );
    sync(&mut session, STATUS_IDLE);
}

#[test]
fn close_and_flush_do_not_substitute_for_sync() {
    let (_tmp, _engine, mut session) = open_session();
    simple(&mut session, "CREATE TABLE t (id INT)");
    send(&mut session, parse("s", "SELECT * FROM t", vec![]));
    send(&mut session, bind("p", "s", vec![], vec![], vec![]));
    assert!(send(&mut session, FrontendMessage::Flush).is_empty());
    assert_eq!(
        tags(&send(&mut session, describe(ObjectKind::Portal, "p"))),
        [b'T']
    );

    assert_eq!(
        send(&mut session, close(ObjectKind::Statement, "s")),
        [b'3', 0, 0, 0, 4]
    );
    assert_error(
        &send(&mut session, describe(ObjectKind::Portal, "p")),
        "34000",
    );
    assert!(send(&mut session, FrontendMessage::Flush).is_empty());
    assert!(send(&mut session, close(ObjectKind::Statement, "s")).is_empty());
    sync(&mut session, STATUS_IDLE);
    assert_eq!(
        send(&mut session, close(ObjectKind::Statement, "s")),
        [b'3', 0, 0, 0, 4]
    );
    sync(&mut session, STATUS_IDLE);
    assert_eq!(
        send(&mut session, close(ObjectKind::Portal, "missing")),
        [b'3', 0, 0, 0, 4]
    );
    sync(&mut session, STATUS_IDLE);

    send(&mut session, parse("s", "SELECT * FROM t", vec![]));
    send(&mut session, bind("p", "s", vec![], vec![], vec![]));
    assert_eq!(
        send(&mut session, close(ObjectKind::Portal, "p")),
        [b'3', 0, 0, 0, 4]
    );
    assert_error(&send(&mut session, execute("p", 0)), "34000");
}

#[test]
fn execute_suspends_and_resumes_without_row_description() {
    let (_tmp, _engine, mut session) = open_session();
    simple(
        &mut session,
        "CREATE TABLE t (id INT); INSERT INTO t VALUES (1), (2), (3)",
    );
    send(
        &mut session,
        parse("s", "SELECT * FROM t ORDER BY id", vec![]),
    );
    send(&mut session, bind("p", "s", vec![], vec![], vec![]));

    let out = send(&mut session, execute("p", 2));
    let messages = scan_messages(&out);
    assert_eq!(tags(&out), [b'D', b'D', b's']);
    assert_eq!(row_values(&messages[0].body), [Some(b"1".to_vec())]);
    assert_eq!(row_values(&messages[1].body), [Some(b"2".to_vec())]);
    assert!(messages[2].body.is_empty());

    let out = send(&mut session, execute("p", 2));
    let messages = scan_messages(&out);
    assert_eq!(tags(&out), [b'D', b'C']);
    assert_eq!(row_values(&messages[0].body), [Some(b"3".to_vec())]);
    // PostgreSQL's final completion count is for this Execute, not the total.
    assert_eq!(command_tags(&out), ["SELECT 1"]);

    let out = send(&mut session, execute("p", 0));
    assert_eq!(out, frame(b'C', b"SELECT 0\0"));
    sync(&mut session, STATUS_IDLE);
    assert_error(&send(&mut session, execute("p", 0)), "34000");
}

#[test]
fn extended_txn_control_clears_portals_and_preserves_statements() {
    let (_tmp, engine, mut session) = open_session();
    simple(&mut session, "CREATE TABLE t (id INT)");
    send(&mut session, parse("begin", "BEGIN", vec![]));
    send(&mut session, bind("pb", "begin", vec![], vec![], vec![]));
    assert_eq!(
        send(&mut session, execute("pb", 0)),
        frame(b'C', b"BEGIN\0")
    );
    sync(&mut session, STATUS_IN_TXN);

    send(
        &mut session,
        parse("insert", "INSERT INTO t VALUES (1), (2)", vec![]),
    );
    send(&mut session, bind("pi", "insert", vec![], vec![], vec![]));
    assert_eq!(
        command_tags(&send(&mut session, execute("pi", 0))),
        ["INSERT 0 2"]
    );
    send(&mut session, parse("select", "SELECT * FROM t", vec![]));
    send(&mut session, bind("ps", "select", vec![], vec![], vec![]));
    assert_eq!(tags(&send(&mut session, execute("ps", 1))), [b'D', b's']);

    send(&mut session, parse("rollback", "ROLLBACK", vec![]));
    send(&mut session, bind("pr", "rollback", vec![], vec![], vec![]));
    assert_eq!(
        send(&mut session, execute("pr", 0)),
        frame(b'C', b"ROLLBACK\0")
    );
    sync(&mut session, STATUS_IDLE);
    assert_error(&send(&mut session, execute("ps", 0)), "34000");
    sync(&mut session, STATUS_IDLE);
    assert_eq!(
        command_tags(&simple(&mut session, "SELECT * FROM t")),
        ["SELECT 0"]
    );
    assert_eq!(
        tags(&send(
            &mut session,
            describe(ObjectKind::Statement, "insert"),
        )),
        [b't', b'n']
    );

    // Reuse the preserved transaction statements, then commit this time.
    send(&mut session, bind("pb2", "begin", vec![], vec![], vec![]));
    send(&mut session, execute("pb2", 0));
    sync(&mut session, STATUS_IN_TXN);
    send(&mut session, bind("pi2", "insert", vec![], vec![], vec![]));
    send(&mut session, execute("pi2", 0));
    send(&mut session, parse("commit", "COMMIT", vec![]));
    send(&mut session, bind("pc", "commit", vec![], vec![], vec![]));
    assert_eq!(
        send(&mut session, execute("pc", 0)),
        frame(b'C', b"COMMIT\0")
    );
    sync(&mut session, STATUS_IDLE);
    assert_eq!(
        command_tags(&simple(&mut session, "SELECT * FROM t")),
        ["SELECT 2"]
    );

    let stats = engine.query_stats().entries();
    let recorded: Vec<&str> = stats.iter().map(|entry| entry.query.as_str()).collect();
    for control in ["BEGIN", "COMMIT", "ROLLBACK"] {
        assert!(
            !recorded.contains(&control),
            "{control} reached Engine::exec"
        );
    }
}

#[test]
fn implicit_extended_transaction_rolls_back_on_error_and_commits_on_sync() {
    let (_tmp, _engine, mut session) = open_session();
    simple(&mut session, "CREATE TABLE t (id INT)");
    send(
        &mut session,
        parse("i1", "INSERT INTO t VALUES (1)", vec![]),
    );
    send(&mut session, bind("p1", "i1", vec![], vec![], vec![]));
    send(&mut session, execute("p1", 0));
    send(
        &mut session,
        parse("i2", "INSERT INTO t VALUES ($1)", vec![23]),
    );
    assert_error(
        &send(
            &mut session,
            bind(
                "p2",
                "i2",
                vec![],
                vec![Some(b"not-an-int".to_vec())],
                vec![],
            ),
        ),
        "22P02",
    );
    sync(&mut session, STATUS_IDLE);
    assert_eq!(
        command_tags(&simple(&mut session, "SELECT * FROM t")),
        ["SELECT 0"]
    );

    send(
        &mut session,
        parse("i3", "INSERT INTO t VALUES (2)", vec![]),
    );
    send(&mut session, bind("p3", "i3", vec![], vec![], vec![]));
    send(&mut session, execute("p3", 0));
    sync(&mut session, STATUS_IDLE);
    assert_eq!(
        command_tags(&simple(&mut session, "SELECT * FROM t")),
        ["SELECT 1"]
    );
}

#[test]
fn empty_and_explain_statements_describe_and_execute_correctly() {
    let (_tmp, _engine, mut session) = open_session();
    simple(&mut session, "CREATE TABLE t (id INT)");
    send(&mut session, parse("empty", "", vec![]));
    assert_eq!(
        tags(&send(
            &mut session,
            describe(ObjectKind::Statement, "empty")
        )),
        [b't', b'n']
    );
    send(&mut session, bind("pe", "empty", vec![], vec![], vec![]));
    assert_eq!(send(&mut session, execute("pe", 0)), frame(b'I', &[]));

    send(
        &mut session,
        parse("explain", "EXPLAIN SELECT * FROM t", vec![]),
    );
    let out = send(&mut session, describe(ObjectKind::Statement, "explain"));
    assert_eq!(tags(&out), [b't', b'T']);
    let messages = scan_messages(&out);
    let fields = row_description(&messages[1].body);
    assert_eq!(fields.len(), 1);
    assert_eq!(fields[0].name, "plan");
    assert_eq!(fields[0].type_oid, 25);
    send(&mut session, bind("px", "explain", vec![], vec![], vec![]));
    let out = send(&mut session, execute("px", 0));
    assert!(!tags(&out).contains(&b'E'), "EXPLAIN failed: {out:?}");
    assert_eq!(tags(&out), [b'D', b'C']);
    sync(&mut session, STATUS_IDLE);
}

#[test]
fn failed_parse_and_simple_query_invalidate_unnamed_objects() {
    let (_tmp, _engine, mut session) = open_session();
    simple(&mut session, "CREATE TABLE t (id INT)");
    send(&mut session, parse("", "SELECT * FROM t", vec![]));
    assert_error(
        &send(&mut session, parse("", "not valid SQL", vec![])),
        "42601",
    );
    sync(&mut session, STATUS_IDLE);
    assert_error(
        &send(&mut session, describe(ObjectKind::Statement, "")),
        "26000",
    );
    sync(&mut session, STATUS_IDLE);

    send(&mut session, parse("", "SELECT * FROM t", vec![]));
    send(&mut session, bind("", "", vec![], vec![], vec![]));
    send(&mut session, execute("", 0));
    simple(&mut session, "SELECT * FROM t");
    assert_error(
        &send(&mut session, describe(ObjectKind::Statement, "")),
        "26000",
    );
    sync(&mut session, STATUS_IDLE);
    assert_error(&send(&mut session, execute("", 0)), "34000");
    sync(&mut session, STATUS_IDLE);
}

#[test]
fn exactly_full_execute_page_suspends_once() {
    let (_tmp, _engine, mut session) = open_session();
    simple(
        &mut session,
        "CREATE TABLE t (id INT); INSERT INTO t VALUES (1), (2)",
    );
    send(
        &mut session,
        parse("s", "SELECT * FROM t ORDER BY id", vec![]),
    );
    send(&mut session, bind("p", "s", vec![], vec![], vec![]));
    let out = send(&mut session, execute("p", 2));
    assert_eq!(tags(&out), [b'D', b'D', b's']);
    let out = send(&mut session, execute("p", 2));
    assert_eq!(out, frame(b'C', b"SELECT 0\0"));
    sync(&mut session, STATUS_IDLE);
}

#[test]
fn extended_ddl_is_rejected_before_any_side_effect() {
    let (_tmp, _engine, mut session) = open_session();
    send(
        &mut session,
        parse("ct", "CREATE TABLE extended_probe (id INT)", vec![]),
    );
    send(&mut session, bind("pct", "ct", vec![], vec![], vec![]));
    assert_error(&send(&mut session, execute("pct", 0)), "0A000");
    sync(&mut session, STATUS_IDLE);
    let out = send(
        &mut session,
        FrontendMessage::Query("SELECT * FROM extended_probe".to_string()),
    );
    let messages = scan_messages(&out);
    assert_eq!(messages[0].tag, b'E');
    assert_eq!(error_field(&messages[0].body, b'C'), "42P01");
    assert_eq!(messages[1].tag, b'Z');

    simple(&mut session, "CREATE TABLE t (id INT)");
    send(&mut session, parse("ci", "CREATE INDEX ON t (id)", vec![]));
    send(&mut session, bind("pci", "ci", vec![], vec![], vec![]));
    assert_error(&send(&mut session, execute("pci", 0)), "0A000");
    sync(&mut session, STATUS_IDLE);

    send(
        &mut session,
        parse("cg", "CREATE GRAPH extended_graph", vec![]),
    );
    send(&mut session, bind("pcg", "cg", vec![], vec![], vec![]));
    let out = send(&mut session, execute("pcg", 0));
    assert_error(&out, "0A000");
    let messages = scan_messages(&out);
    assert!(
        error_field(&messages[0].body, b'M').contains("DDL in an extended implicit transaction")
    );
    sync(&mut session, STATUS_IDLE);
}

#[test]
fn row_locking_select_rejects_later_ddl_without_self_wedging() {
    for clause in ["FOR UPDATE", "FOR SHARE"] {
        let (_tmp, _engine, mut session) = open_session();
        simple(
            &mut session,
            "CREATE TABLE t (id INT); INSERT INTO t VALUES (1)",
        );
        send(
            &mut session,
            parse("s", &format!("SELECT * FROM t {clause}"), vec![]),
        );
        send(&mut session, bind("ps", "s", vec![], vec![], vec![]));
        send(&mut session, execute("ps", 0));
        send(&mut session, parse("ci", "CREATE INDEX ON t (id)", vec![]));
        send(&mut session, bind("pci", "ci", vec![], vec![], vec![]));
        assert_error(&send(&mut session, execute("pci", 0)), "0A000");
        sync(&mut session, STATUS_IDLE);
    }
}

#[test]
fn simple_query_error_rolls_back_preceding_extended_series() {
    let (_tmp, _engine, mut session) = open_session();
    simple(&mut session, "CREATE TABLE t (id INT)");
    send(&mut session, parse("i", "INSERT INTO t VALUES (1)", vec![]));
    send(&mut session, bind("p", "i", vec![], vec![], vec![]));
    send(&mut session, execute("p", 0));
    let out = send(
        &mut session,
        FrontendMessage::Query("not valid SQL".to_string()),
    );
    let messages = scan_messages(&out);
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0].tag, b'E');
    assert_eq!(error_field(&messages[0].body, b'C'), "42601");
    assert_eq!(messages[1].tag, b'Z');
    assert_eq!(
        command_tags(&simple(&mut session, "SELECT * FROM t")),
        ["SELECT 0"]
    );
}

#[test]
fn semicolon_only_parse_is_an_empty_statement() {
    let (_tmp, _engine, mut session) = open_session();
    send(&mut session, parse("empty", "; ;", vec![]));
    assert_eq!(
        tags(&send(
            &mut session,
            describe(ObjectKind::Statement, "empty")
        )),
        [b't', b'n']
    );
    send(&mut session, bind("p", "empty", vec![], vec![], vec![]));
    assert_eq!(send(&mut session, execute("p", 0)), frame(b'I', &[]));
    sync(&mut session, STATUS_IDLE);
}

#[test]
fn explicit_error_enters_failed_state_and_commit_rolls_back() {
    let (_tmp, _engine, mut session) = open_session();
    simple(&mut session, "CREATE TABLE t (id INT)");
    simple(&mut session, "BEGIN");
    simple(&mut session, "INSERT INTO t VALUES (1)");
    let out = send(
        &mut session,
        FrontendMessage::Query("SELECT * FROM missing".to_string()),
    );
    assert_eq!(scan_messages(&out).last().unwrap().body, [b'E']);

    let out = send(
        &mut session,
        FrontendMessage::Query("INSERT INTO t VALUES (2)".to_string()),
    );
    let messages = scan_messages(&out);
    assert_eq!(messages[0].tag, b'E');
    assert_eq!(error_field(&messages[0].body, b'C'), "25P02");
    assert_eq!(messages[1].body, [b'E']);

    let out = send(&mut session, FrontendMessage::Query("COMMIT".to_string()));
    assert_eq!(command_tags(&out), ["ROLLBACK"]);
    assert_eq!(scan_messages(&out).last().unwrap().body, [b'I']);
    assert_eq!(
        command_tags(&simple(&mut session, "SELECT * FROM t")),
        ["SELECT 0"]
    );
}

#[test]
fn failed_state_rejects_parse_bind_and_simple_ddl() {
    let (_tmp, _engine, mut session) = open_session();
    simple(&mut session, "CREATE TABLE t (id INT)");
    simple(&mut session, "BEGIN");
    let out = send(
        &mut session,
        FrontendMessage::Query("SELECT * FROM missing".to_string()),
    );
    assert_eq!(scan_messages(&out).last().unwrap().body, [b'E']);

    assert_error(
        &send(&mut session, parse("s", "SELECT * FROM t", vec![])),
        "25P02",
    );
    sync(&mut session, b'E');
    assert_error(
        &send(&mut session, bind("p", "s", vec![], vec![], vec![])),
        "25P02",
    );
    sync(&mut session, b'E');

    let out = send(
        &mut session,
        FrontendMessage::Query("CREATE TABLE leaked (id INT)".to_string()),
    );
    let messages = scan_messages(&out);
    assert_eq!(messages[0].tag, b'E');
    assert_eq!(error_field(&messages[0].body, b'C'), "25P02");
    assert_eq!(messages[1].body, [b'E']);
    send(&mut session, parse("exit", "COMMIT", vec![]));
    send(&mut session, bind("pexit", "exit", vec![], vec![], vec![]));
    let out = send(&mut session, execute("pexit", 0));
    assert_eq!(command_tags(&out), ["ROLLBACK"]);
    sync(&mut session, STATUS_IDLE);

    let out = send(
        &mut session,
        FrontendMessage::Query("SELECT * FROM leaked".to_string()),
    );
    let messages = scan_messages(&out);
    assert_eq!(messages[0].tag, b'E');
    assert_eq!(error_field(&messages[0].body, b'C'), "42P01");
}

#[test]
fn failed_state_allows_extended_rollback() {
    let (_tmp, _engine, mut session) = open_session();
    simple(&mut session, "CREATE TABLE t (id INT)");
    simple(&mut session, "BEGIN");
    let out = send(
        &mut session,
        FrontendMessage::Query("SELECT * FROM missing".to_string()),
    );
    assert_eq!(scan_messages(&out).last().unwrap().body, [b'E']);
    send(&mut session, parse("exit", "ROLLBACK", vec![]));
    send(&mut session, bind("pexit", "exit", vec![], vec![], vec![]));
    let out = send(&mut session, execute("pexit", 0));
    let messages = scan_messages(&out);
    assert_eq!(messages[0].tag, b'N');
    assert_eq!(error_field(&messages[0].body, b'C'), "25P01");
    assert_eq!(command_tags(&out), ["ROLLBACK"]);
    assert_error(&send(&mut session, execute("pexit", 0)), "34000");
    sync(&mut session, STATUS_IDLE);
}

#[test]
fn function_call_enters_failed_state() {
    let (_tmp, _engine, mut session) = open_session();
    simple(&mut session, "CREATE TABLE t (id INT)");
    simple(&mut session, "BEGIN");
    simple(&mut session, "INSERT INTO t VALUES (1)");
    let out = send(&mut session, FrontendMessage::Unsupported { tag: b'F' });
    let messages = scan_messages(&out);
    assert_eq!(messages[0].tag, b'E');
    assert_eq!(error_field(&messages[0].body, b'C'), "0A000");
    assert_eq!(messages[1].body, [b'E']);
    let out = send(&mut session, FrontendMessage::Query("COMMIT".to_string()));
    assert_eq!(command_tags(&out), ["ROLLBACK"]);
    assert_eq!(scan_messages(&out).last().unwrap().body, [b'I']);
    assert_eq!(
        command_tags(&simple(&mut session, "SELECT * FROM t")),
        ["SELECT 0"]
    );
}

#[test]
fn simple_multi_statement_is_atomic_and_fully_parsed_first() {
    let (_tmp, _engine, mut session) = open_session();
    simple(&mut session, "CREATE TABLE t (id INT)");
    let out = send(
        &mut session,
        FrontendMessage::Query(
            "INSERT INTO t VALUES (1); SELECT * FROM missing; INSERT INTO t VALUES (2)".to_string(),
        ),
    );
    assert_eq!(scan_messages(&out).last().unwrap().body, [b'I']);
    assert_eq!(
        command_tags(&simple(&mut session, "SELECT * FROM t")),
        ["SELECT 0"]
    );

    let out = send(
        &mut session,
        FrontendMessage::Query("BEGIN; INSERT INTO t VALUES (1); COMMIT; SELCT".to_string()),
    );
    assert_eq!(scan_messages(&out).last().unwrap().body, [b'I']);
    assert_eq!(
        command_tags(&simple(&mut session, "SELECT * FROM t")),
        ["SELECT 0"]
    );
}

#[test]
fn bm25_statement_and_portal_descriptions_are_float8() {
    let (_tmp, _engine, mut session) = open_session();
    simple(
        &mut session,
        "CREATE TABLE agent_memory (id INT4, content TEXT)",
    );
    assert_eq!(
        send(
            &mut session,
            parse(
                "s",
                "SELECT id, bm25_score(content, $1) AS score FROM agent_memory WHERE content @@ $1",
                vec![0],
            ),
        ),
        [b'1', 0, 0, 0, 4]
    );
    let out = send(&mut session, describe(ObjectKind::Statement, "s"));
    let messages = scan_messages(&out);
    assert_eq!(tags(&out), [b't', b'T']);
    assert_eq!(parameter_oids(&messages[0].body), [25]);
    let fields = row_description(&messages[1].body);
    assert_eq!(fields.len(), 2);
    assert_eq!(fields[0].type_oid, 23);
    assert_eq!(fields[1].name, "score");
    assert_eq!(fields[1].type_oid, 701);
    assert_eq!(fields[1].table_oid, 0);
    assert_eq!(fields[1].attnum, 0);

    assert_eq!(
        send(
            &mut session,
            bind(
                "p",
                "s",
                vec![FORMAT_TEXT],
                vec![Some(b"contract".to_vec())],
                vec![FORMAT_BINARY],
            ),
        ),
        [b'2', 0, 0, 0, 4]
    );
    let out = send(&mut session, describe(ObjectKind::Portal, "p"));
    let messages = scan_messages(&out);
    assert_eq!(tags(&out), [b'T']);
    let fields = row_description(&messages[0].body);
    assert_eq!(fields[1].type_oid, 701);
    assert_eq!(fields[1].format, FORMAT_BINARY);
}

#[test]
fn fulltext_parameter_rejects_non_text_column_before_describe() {
    let (_tmp, _engine, mut session) = open_session();
    simple(
        &mut session,
        "CREATE TABLE agent_memory (id INT4, content TEXT)",
    );
    let out = send(
        &mut session,
        parse("bad", "SELECT * FROM agent_memory WHERE id @@ $1", vec![0]),
    );
    let messages = scan_messages(&out);
    assert_eq!(tags(&out), [b'E']);
    assert_eq!(error_field(&messages[0].body, b'C'), "42601");
    assert!(error_field(&messages[0].body, b'M').contains("not a TEXT column"));
    sync(&mut session, STATUS_IDLE);
}
