//! Phase 4a Stage F: sequence behavior across Simple and Extended Query.

use std::sync::Arc;

use pg_engine::{Engine, EngineConfig};
use pg_wire::codec::{FrontendMessage, ObjectKind};
use pg_wire::Session;
use tempfile::TempDir;

fn open(dir: &std::path::Path) -> Arc<Engine> {
    Arc::new(Engine::open(dir, EngineConfig::new(dir)).unwrap())
}

fn simple(session: &mut Session, sql: &str) -> Vec<u8> {
    let mut out = Vec::new();
    session.handle_query(sql, &mut out);
    out
}

fn extended(session: &mut Session, sql: &str) -> Vec<u8> {
    let mut out = Vec::new();
    for message in [
        FrontendMessage::Parse {
            name: "s".to_string(),
            query: sql.to_string(),
            parameter_type_oids: vec![],
        },
        FrontendMessage::Bind {
            portal: "p".to_string(),
            statement: "s".to_string(),
            parameter_formats: vec![],
            parameters: vec![],
            result_formats: vec![],
        },
        FrontendMessage::Execute {
            portal: "p".to_string(),
            max_rows: 0,
        },
        FrontendMessage::Sync,
    ] {
        session.handle_message(message, &mut out);
    }
    out
}

fn text(out: &[u8]) -> String {
    String::from_utf8_lossy(out).into_owned()
}

#[test]
fn sequence_currval_survives_extended_sync_and_rollback() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    let mut session = Session::new(Arc::clone(&engine));
    assert!(text(&simple(&mut session, "CREATE SEQUENCE s")).contains("CREATE SEQUENCE"));
    assert!(text(&simple(&mut session, "SELECT nextval('s')")).contains("1"));
    assert!(text(&extended(&mut session, "SELECT nextval('s')")).contains("2"));
    assert!(text(&simple(&mut session, "SELECT currval('s')")).contains("2"));

    let failed = simple(
        &mut session,
        "BEGIN; SELECT nextval('s'); SELECT * FROM missing",
    );
    assert!(text(&failed).contains("ERROR"));
    assert!(text(&simple(&mut session, "COMMIT")).contains("ROLLBACK"));
    assert!(text(&simple(&mut session, "SELECT currval('s')")).contains("3"));
    assert!(text(&simple(&mut session, "SELECT nextval('s')")).contains("4"));
    engine.shutdown();
}

#[test]
fn currval_is_isolated_per_connection() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    let mut first = Session::new(Arc::clone(&engine));
    let mut second = Session::new(Arc::clone(&engine));
    simple(&mut first, "CREATE SEQUENCE s");
    simple(&mut first, "SELECT nextval('s')");
    assert!(text(&simple(&mut first, "SELECT currval('s')")).contains("1"));
    assert!(text(&simple(&mut second, "SELECT currval('s')")).contains("currval is not defined"));
    engine.shutdown();
}

#[test]
fn serial_defaults_work_through_simple_query() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    let mut session = Session::new(Arc::clone(&engine));
    simple(&mut session, "CREATE TABLE items (id SERIAL, note TEXT)");
    simple(&mut session, "INSERT INTO items (note) VALUES ('a'), ('b')");
    simple(
        &mut session,
        "INSERT INTO items (id, note) VALUES (DEFAULT, 'c'), (NULL, 'd')",
    );
    let rows = engine.scan("items", None).unwrap();
    let ids = rows
        .iter()
        .map(|(_, values)| values[0].clone())
        .collect::<Vec<_>>();
    assert_eq!(
        ids,
        vec![
            Some(pg_engine::Datum::Int4(1)),
            Some(pg_engine::Datum::Int4(2)),
            Some(pg_engine::Datum::Int4(3)),
            None,
        ]
    );
    assert!(text(&simple(&mut session, "SELECT currval('items_id_seq')")).contains("3"));
    engine.shutdown();
}

#[test]
fn sequence_query_describe_is_int8() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    let mut session = Session::new(Arc::clone(&engine));
    simple(&mut session, "CREATE SEQUENCE s");
    let mut out = Vec::new();
    for message in [
        FrontendMessage::Parse {
            name: "q".to_string(),
            query: "SELECT currval('s')".to_string(),
            parameter_type_oids: vec![],
        },
        FrontendMessage::Describe {
            kind: ObjectKind::Statement,
            name: "q".to_string(),
        },
    ] {
        session.handle_message(message, &mut out);
    }
    assert!(out
        .windows(4)
        .any(|window| window == 20u32.to_be_bytes().as_slice()));
    engine.shutdown();
}
