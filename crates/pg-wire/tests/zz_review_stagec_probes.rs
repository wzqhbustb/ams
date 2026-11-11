//! Deep-review probes for Stage C (scratch; not part of the deliverable set).
//! Each probe pins a protocol edge against PostgreSQL's documented behavior.

use std::sync::Arc;

use pg_engine::{Engine, EngineConfig};
use pg_wire::codec::{FrontendMessage, STATUS_FAILED, STATUS_IDLE, STATUS_IN_TXN};
use pg_wire::Session;
use tempfile::TempDir;

fn open_session() -> (TempDir, Arc<Engine>, Session) {
    let tmp = TempDir::new().unwrap();
    let engine = Arc::new(Engine::open(tmp.path(), EngineConfig::new(tmp.path())).unwrap());
    let session = Session::new(Arc::clone(&engine));
    (tmp, engine, session)
}

fn scan_messages(buf: &[u8]) -> Vec<(u8, Vec<u8>)> {
    let mut msgs = Vec::new();
    let mut rest = buf;
    while rest.len() >= 5 {
        let tag = rest[0];
        let len = u32::from_be_bytes(rest[1..5].try_into().unwrap()) as usize;
        let body = rest[5..5 + len - 4].to_vec();
        msgs.push((tag, body));
        rest = &rest[5 + len - 4..];
    }
    assert!(rest.is_empty(), "trailing bytes after scan");
    msgs
}

fn send(session: &mut Session, message: FrontendMessage) -> Vec<u8> {
    let mut out = Vec::new();
    session.handle_message(message, &mut out);
    out
}

fn simple(session: &mut Session, sql: &str) -> Vec<u8> {
    send(session, FrontendMessage::Query(sql.to_string()))
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

fn execute(portal: &str, max_rows: u32) -> FrontendMessage {
    FrontendMessage::Execute {
        portal: portal.to_string(),
        max_rows,
    }
}

fn sync(session: &mut Session) -> (Vec<u8>, u8) {
    let out = send(session, FrontendMessage::Sync);
    let msgs = scan_messages(&out);
    let status = msgs.last().unwrap().1[0];
    (out, status)
}

fn error_field(body: &[u8], wanted: u8) -> String {
    let mut rest = body;
    while !rest.is_empty() {
        let field = rest[0];
        let nul = rest[1..].iter().position(|&b| b == 0).unwrap();
        let value = String::from_utf8(rest[1..1 + nul].to_vec()).unwrap();
        if field == wanted {
            return value;
        }
        rest = &rest[nul + 2..];
    }
    panic!("field {} not found", wanted as char)
}

fn assert_error(buf: &[u8], sqlstate: &str) {
    let msgs = scan_messages(buf);
    let err = msgs
        .iter()
        .find(|(tag, _)| *tag == b'E')
        .unwrap_or_else(|| {
            panic!(
                "expected ErrorResponse, got {:?}",
                msgs.iter().map(|(t, _)| t).collect::<Vec<_>>()
            )
        });
    assert_eq!(error_field(&err.1, b'C'), sqlstate);
}

// T1: simple query syntax error inside an explicit transaction → 'E' state,
// then COMMIT reports ROLLBACK and leaves the session idle.
#[test]
fn simple_query_error_in_explicit_txn_enters_failed_and_commit_rolls_back() {
    let (_tmp, _engine, mut session) = open_session();
    simple(&mut session, "CREATE TABLE t (id INT)");
    simple(&mut session, "BEGIN");
    assert!(!simple(&mut session, "SELECT * FROM t").is_empty());
    let out = simple(&mut session, "SELECT bad syntax here");
    assert_error(&out, "42601");
    let msgs = scan_messages(&out);
    assert_eq!(msgs.last().unwrap().1, [STATUS_FAILED]);

    // In failed state, plain statements are refused with 25P02.
    let out = simple(&mut session, "SELECT * FROM t");
    assert_error(&out, "25P02");
    assert_eq!(scan_messages(&out).last().unwrap().1, [STATUS_FAILED]);

    // COMMIT in failed state acts as ROLLBACK and emits the same WARNING
    // as the idle path: "there is no transaction in progress" (25P01).
    let out = simple(&mut session, "COMMIT");
    let msgs = scan_messages(&out);
    let (tag, body) = msgs.first().expect("WARNING notice required");
    assert_eq!(*tag, b'N');
    assert_eq!(error_field(body, b'C'), "25P01");
    let tags: Vec<String> = msgs
        .iter()
        .filter(|(t, _)| *t == b'C')
        .map(|(_, b)| String::from_utf8(b.clone()).unwrap())
        .collect();
    assert_eq!(tags, ["ROLLBACK\0"]);
    assert_eq!(msgs.last().unwrap().1, [STATUS_IDLE]);

    // Session is usable again.
    let out = simple(
        &mut session,
        "CREATE TABLE u (id INT); INSERT INTO u VALUES (1)",
    );
    let msgs = scan_messages(&out);
    assert!(msgs.iter().any(|(t, _)| *t == b'C'));
}

// T2: failed Parse of the unnamed statement destroys the previous one
// (implementation choice; pin it so a change is deliberate).
#[test]
fn failed_unnamed_parse_still_invalidates_previous_statement() {
    let (_tmp, _engine, mut session) = open_session();
    simple(&mut session, "CREATE TABLE t (id INT)");
    send(&mut session, parse("", "SELECT * FROM t", vec![]));
    let out = send(&mut session, parse("", "SELECT * FROM missing", vec![]));
    assert_error(&out, "42P01");
    // The previous unnamed statement is gone: Bind must fail with 26000.
    let (_out, _status) = sync(&mut session);
    let out = send(&mut session, bind("p", "", vec![], vec![], vec![]));
    assert_error(&out, "26000");
    let (_out, status) = sync(&mut session);
    assert_eq!(status, STATUS_IDLE);
}

// T3: TEXT-OID(25) declared vector parameter (pgvector compatibility path,
// untested in the deliverable suite).
#[test]
fn text_declared_vector_parameter_decodes_and_binary_format_uses_text_bytes() {
    let (_tmp, _engine, mut session) = open_session();
    simple(
        &mut session,
        "CREATE TABLE t (id INT, v VECTOR(2)); INSERT INTO t VALUES (1, [3,4])",
    );
    send(
        &mut session,
        parse(
            "s",
            "SELECT id FROM t ORDER BY v <-> $1 ASC LIMIT 1",
            vec![25],
        ),
    );
    let out = send(
        &mut session,
        bind("p", "s", vec![0], vec![Some(b"[0,0]".to_vec())], vec![]),
    );
    assert!(
        out.windows(4).any(|w| w == b"2\0\0\0"),
        "expected BindComplete"
    );
    let out = send(&mut session, execute("p", 0));
    let msgs = scan_messages(&out);
    assert!(msgs.iter().any(|(t, _)| *t == b'D'), "expected a DataRow");
    let (_out, _status) = sync(&mut session);

    // Binary format code with TEXT-declared OID must still be text bytes.
    send(
        &mut session,
        parse(
            "s2",
            "SELECT id FROM t ORDER BY v <-> $1 ASC LIMIT 1",
            vec![25],
        ),
    );
    send(
        &mut session,
        bind("p2", "s2", vec![1], vec![Some(b"[0,0]".to_vec())], vec![]),
    );
    let out = send(&mut session, execute("p2", 0));
    assert!(scan_messages(&out).iter().any(|(t, _)| *t == b'D'));
    let (_out, _status) = sync(&mut session);
}

// T4: repeated Execute of a completed non-SELECT portal is refused.
#[test]
fn repeated_execute_of_completed_insert_portal_fails() {
    let (_tmp, _engine, mut session) = open_session();
    simple(&mut session, "CREATE TABLE t (id INT)");
    send(&mut session, parse("s", "INSERT INTO t VALUES (1)", vec![]));
    send(&mut session, bind("p", "s", vec![], vec![], vec![]));
    let out = send(&mut session, execute("p", 0));
    assert!(scan_messages(&out).iter().any(|(t, _)| *t == b'C'));
    let out = send(&mut session, execute("p", 0));
    assert_error(&out, "08P01");
    let (_out, status) = sync(&mut session);
    assert_eq!(status, STATUS_IDLE);
}

// T5: named portal survives Sync inside an explicit transaction, dies at COMMIT.
#[test]
fn named_portal_lives_through_sync_in_explicit_txn_dies_at_commit() {
    let (_tmp, _engine, mut session) = open_session();
    simple(
        &mut session,
        "CREATE TABLE t (id INT); INSERT INTO t VALUES (1)",
    );
    simple(&mut session, "BEGIN");
    send(&mut session, parse("s", "SELECT * FROM t", vec![]));
    send(&mut session, bind("p", "s", vec![], vec![], vec![]));
    send(&mut session, execute("p", 0));
    let (_out, status) = sync(&mut session);
    assert_eq!(status, STATUS_IN_TXN);
    // Portal still alive after Sync.
    let out = send(&mut session, execute("p", 0));
    let msgs = scan_messages(&out);
    assert!(msgs.iter().any(|(t, _)| *t == b'C')); // SELECT 0 completion
    simple(&mut session, "COMMIT");
    let out = send(&mut session, execute("p", 0));
    assert_error(&out, "34000");
    let (_out, status) = sync(&mut session);
    assert_eq!(status, STATUS_IDLE);
}

// T6: Parse in failed state: COMMIT allowed, others 25P02.
#[test]
fn parse_in_failed_txn_blocks_everything_but_transaction_exit() {
    let (_tmp, _engine, mut session) = open_session();
    simple(&mut session, "CREATE TABLE t (id INT)");
    simple(&mut session, "BEGIN");
    let out = simple(&mut session, "SELECT bad");
    assert_error(&out, "42601");

    let out = send(&mut session, parse("s", "SELECT * FROM t", vec![]));
    assert_error(&out, "25P02");
    let (_out, _status) = sync(&mut session);
    let out = send(&mut session, parse("c", "COMMIT", vec![]));
    assert!(
        out.windows(4).any(|w| w == b"1\0\0\0"),
        "ParseComplete expected"
    );
    send(&mut session, bind("p", "c", vec![], vec![], vec![]));
    let out = send(&mut session, execute("p", 0));
    let msgs = scan_messages(&out);
    let tags: Vec<String> = msgs
        .iter()
        .filter(|(t, _)| *t == b'C')
        .map(|(_, b)| String::from_utf8(b.clone()).unwrap())
        .collect();
    assert_eq!(tags, ["ROLLBACK\0"]);
    let (_out, status) = sync(&mut session);
    assert_eq!(status, STATUS_IDLE);
}

// T7: simple query in failed state does not leak a throwaway transaction.
#[test]
fn failed_state_simple_query_leaves_engine_idle() {
    let (_tmp, engine, mut session) = open_session();
    simple(&mut session, "CREATE TABLE t (id INT)");
    simple(&mut session, "BEGIN");
    simple(&mut session, "INSERT INTO t VALUES (1)");
    let out = simple(&mut session, "SELECT bad");
    assert_error(&out, "42601");
    assert!(!simple(&mut session, "SELECT 1").is_empty());
    let (_, status) = sync(&mut session);
    assert_eq!(status, STATUS_FAILED);
    // Engine must have no live transaction handle left over.
    assert!(
        engine.txn_manager().active_xids().is_empty(),
        "engine leaked a txn"
    );
}

// T8: integer text parameters reject surrounding whitespace (PG int4in trims).
#[test]
fn int_text_parameter_whitespace_behavior_pinned() {
    let (_tmp, _engine, mut session) = open_session();
    simple(&mut session, "CREATE TABLE t (id INT)");
    send(
        &mut session,
        parse("s", "SELECT * FROM t WHERE id = $1", vec![]),
    );
    let out = send(
        &mut session,
        bind("p", "s", vec![0], vec![Some(b" 5".to_vec())], vec![]),
    );
    // Current behavior: rejected (22P02). PG accepts; pin so the deviation is deliberate.
    assert_error(&out, "22P02");
    let (_out, _status) = sync(&mut session);
}

// T9: lowercase "infinity" (PG accepts case-insensitively).
#[test]
fn float8_infinity_spellings_pinned() {
    let (_tmp, _engine, mut session) = open_session();
    simple(&mut session, "CREATE TABLE t (x FLOAT8)");
    send(
        &mut session,
        parse("s", "SELECT * FROM t WHERE x < $1", vec![]),
    );
    for (text, ok) in [
        ("Infinity", true),
        ("inf", true),
        ("infinity", true), // Rust f64::from_str is case-insensitive; matches PG
    ] {
        let out = send(
            &mut session,
            bind(
                "p",
                "s",
                vec![0],
                vec![Some(text.as_bytes().to_vec())],
                vec![],
            ),
        );
        let bound = out.windows(4).any(|w| w == b"2\0\0\0");
        assert_eq!(bound, ok, "unexpected Bind outcome for {text:?}");
        let (_o, _s) = sync(&mut session);
    }
}

// T10: Bind with result format for a zero-column statement is refused.
#[test]
fn bind_result_format_count_mismatch_refused() {
    let (_tmp, _engine, mut session) = open_session();
    simple(&mut session, "CREATE TABLE t (id INT)");
    send(&mut session, parse("s", "INSERT INTO t VALUES (1)", vec![]));
    let out = send(&mut session, bind("p", "s", vec![], vec![], vec![0, 1]));
    assert_error(&out, "08P01");
    let (_out, _status) = sync(&mut session);
}
