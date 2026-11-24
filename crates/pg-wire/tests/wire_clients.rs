//! M3 Stage F (tech-selection §7.4) client-acceptance tests: the CI hard
//! gate is **rust-postgres** driving a real server over TCP (N6 —
//! coding-plan Stage F). The other three clients of the §7.4 matrix are
//! run MANUALLY and their results are archived at Stage G (benchmark doc +
//! stage_spec), not gated here.
//!
//! Manual matrix commands (server fixture: any test below binds
//! `127.0.0.1:0`; for manual runs bind a fixed port, e.g. a tiny bin or
//! `cargo test`-spawned instance on 127.0.0.1:55432 over a temp data dir):
//!
//! ```sh
//! # psql (probes like `\d` are NOT promised — §11 R3; basic CRUD is):
//! psql "host=127.0.0.1 port=55432 user=pg_rust dbname=pg_rust" \
//!   -c "CREATE TABLE t (id INT, name TEXT)" \
//!   -c "INSERT INTO t VALUES (1, 'a')" -c "SELECT * FROM t" \
//!   -c "UPDATE t SET name = 'b' WHERE id = 1" -c "DELETE FROM t WHERE id = 1" \
//!   -c "BEGIN" -c "INSERT INTO t VALUES (2, 'x')" -c "COMMIT"
//!
//! # psycopg2 (autocommit off exercises BEGIN/COMMIT interception;
//! # set autocommit FIRST for DDL — the engine rejects DDL inside explicit
//! # transactions (M2b boundary), and psycopg2's default implicit-txn wraps
//! # every statement; archived matrix result: docs/phase1-m3-benchmarks.md):
//! python3 - <<'PY'
//! import psycopg2
//! c = psycopg2.connect("host=127.0.0.1 port=55432 user=pg_rust dbname=pg_rust")
//! c.autocommit = True
//! cur = c.cursor()
//! cur.execute("CREATE TABLE t (id INT, name TEXT)")
//! cur.execute("INSERT INTO t VALUES (1, 'a')")
//! cur.execute("SELECT * FROM t"); print(cur.fetchall())
//! cur.execute("UPDATE t SET name = 'b' WHERE id = 1")
//! cur.execute("DELETE FROM t WHERE id = 1")
//! PY
//!
//! # node-postgres:
//! node -e '
//! const { Client } = require("pg");
//! (async () => {
//!   const c = new Client({ host: "127.0.0.1", port: 55432, user: "pg_rust", database: "pg_rust" });
//!   await c.connect();
//!   await c.query("CREATE TABLE t (id INT, name TEXT)");
//!   await c.query("INSERT INTO t VALUES (1, '"'"'a'"'"')");
//!   console.log((await c.query("SELECT * FROM t")).rows);
//!   await c.query("BEGIN"); await c.query("UPDATE t SET name = '"'"'b'"'"' WHERE id = 1"); await c.query("COMMIT");
//!   await c.query("DELETE FROM t WHERE id = 1"); await c.end();
//! })();'
//! ```
//!
//! Covered here (rust-postgres hard gate):
//!
//! - connect + full CRUD + BEGIN/COMMIT/ROLLBACK over a real socket;
//! - multi-statement strings execute in order;
//! - startup-probe statements (`SET`, `SELECT version()`) error WITHOUT
//!   killing the connection (§7.4 / §11 R3);
//! - all six heap types text-round-trip, NULL as protocol null;
//! - 4 concurrent clients on one engine (watchdog-protected, §12.2 并发面).
//!
//! Acceptance: `cargo test -p pg-wire --test wire_clients`

use std::io::{Read, Write};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use pg_engine::{Datum, Engine, EngineConfig};
use pg_wire::codec::{FrontendMessage, STATUS_FAILED, STATUS_IDLE};
use pg_wire::types::FORMAT_BINARY;
use pg_wire::{Server, Session};
use postgres::types::Type;
use postgres::{Client, NoTls, SimpleQueryMessage};
use tempfile::TempDir;

/// Overall watchdog bound for a single client test — generous so a slow CI
/// runner never trips it, tight enough that a regression cannot hang.
const WATCHDOG: Duration = Duration::from_secs(120);

/// Run `f` in a supervisor thread and fail on timeout instead of hanging
/// (same pattern as pg-am-btree's btree_concurrent.rs watchdog).
fn run_with_watchdog<F>(name: &str, f: F)
where
    F: FnOnce() + Send + 'static,
{
    let (tx, rx) = mpsc::channel();
    let name = name.to_string();
    thread::spawn(move || {
        f();
        let _ = tx.send(());
    });
    match rx.recv_timeout(WATCHDOG) {
        Ok(()) => {}
        Err(mpsc::RecvTimeoutError::Timeout) => panic!("{name}: deadlocked or ran too long"),
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            panic!("{name}: client thread panicked (see above)")
        }
    }
}

/// Spin up a server on an ephemeral port over a fresh temp data dir.
/// The serve loop runs on a detached thread for the rest of the process.
fn spawn_server() -> (TempDir, Arc<Engine>, u16) {
    let tmp = TempDir::new().unwrap();
    let engine = Arc::new(Engine::open(tmp.path(), EngineConfig::new(tmp.path())).unwrap());
    let server = Server::bind("127.0.0.1:0", Arc::clone(&engine)).unwrap();
    let port = server.local_addr().unwrap().port();
    thread::spawn(move || {
        if let Err(e) = server.serve() {
            eprintln!("pg-wire serve loop ended: {e}");
        }
    });
    (tmp, engine, port)
}

fn connect(port: u16) -> Client {
    Client::connect(
        &format!("host=127.0.0.1 port={port} user=pg_rust dbname=pg_rust connect_timeout=10"),
        NoTls,
    )
    .unwrap()
}

/// Pull the text cells out of a simple-query response.
fn rows_of(msgs: &[SimpleQueryMessage]) -> Vec<Vec<Option<String>>> {
    msgs.iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|i| row.get(i).map(|s: &str| s.to_string()))
                    .collect(),
            ),
            _ => None,
        })
        .collect()
}

/// The affected-row counts of all CommandComplete messages, in order.
fn completions_of(msgs: &[SimpleQueryMessage]) -> Vec<u64> {
    msgs.iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::CommandComplete(n) => Some(*n),
            _ => None,
        })
        .collect()
}

#[test]
fn rust_postgres_full_crud_and_txn_control() {
    run_with_watchdog("crud+txn", || {
        let (_tmp, _engine, port) = spawn_server();
        let mut client = connect(port);

        // DDL + DML through the simple protocol.
        client
            .batch_execute("CREATE TABLE t (id INT, name TEXT)")
            .unwrap();
        client
            .batch_execute("INSERT INTO t VALUES (1, 'alice'), (2, NULL)")
            .unwrap();

        // SELECT returns text cells; NULL is a protocol null.
        let msgs = client.simple_query("SELECT * FROM t ORDER BY id").unwrap();
        assert_eq!(
            rows_of(&msgs),
            vec![
                vec![Some("1".to_string()), Some("alice".to_string())],
                vec![Some("2".to_string()), None],
            ]
        );
        assert_eq!(completions_of(&msgs), [2]);

        // UPDATE / DELETE tags carry the affected-row counts.
        let msgs = client
            .simple_query("UPDATE t SET name = 'bob' WHERE id = 1")
            .unwrap();
        assert_eq!(completions_of(&msgs), [1]);
        let msgs = client.simple_query("DELETE FROM t WHERE id = 2").unwrap();
        assert_eq!(completions_of(&msgs), [1]);

        // Explicit transaction: BEGIN intercept → work → COMMIT.
        client.batch_execute("BEGIN").unwrap();
        client
            .batch_execute("INSERT INTO t VALUES (3, 'carol')")
            .unwrap();
        client.batch_execute("COMMIT").unwrap();
        let msgs = client.simple_query("SELECT * FROM t ORDER BY id").unwrap();
        assert_eq!(rows_of(&msgs).len(), 2);

        // ROLLBACK discards the transaction's writes.
        client.batch_execute("BEGIN").unwrap();
        client.batch_execute("DELETE FROM t").unwrap();
        client.batch_execute("ROLLBACK").unwrap();
        let msgs = client.simple_query("SELECT * FROM t").unwrap();
        assert_eq!(completions_of(&msgs), [2]);

        // Multi-statement string executes in order (§7.2).
        client
            .batch_execute("INSERT INTO t VALUES (4, 'd'); UPDATE t SET name = 'dd' WHERE id = 4; DELETE FROM t WHERE id = 3")
            .unwrap();
        let msgs = client.simple_query("SELECT * FROM t ORDER BY id").unwrap();
        assert_eq!(
            rows_of(&msgs),
            vec![
                vec![Some("1".to_string()), Some("bob".to_string())],
                vec![Some("4".to_string()), Some("dd".to_string())]
            ]
        );
    });
}

#[test]
fn rust_postgres_inverted_backfill_and_bm25_end_to_end() {
    run_with_watchdog("inverted+BM25", || {
        let (_tmp, engine, port) = spawn_server();
        let mut client = connect(port);
        client
            .batch_execute("CREATE TABLE agent_memory (id INT4, content TEXT)")
            .unwrap();
        client
            .batch_execute(
                "INSERT INTO agent_memory VALUES (1, 'rust contract'), (2, 'other document')",
            )
            .unwrap();
        client
            .batch_execute("CREATE INDEX content_idx ON agent_memory USING inverted (content)")
            .unwrap();

        let msgs = client
            .simple_query(
                "SELECT id, bm25_score(content, 'contract') AS score FROM agent_memory WHERE content @@ 'contract' ORDER BY bm25_score(content, 'contract') DESC",
            )
            .unwrap();
        let rows = rows_of(&msgs);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0].as_deref(), Some("1"));
        assert!(rows[0][1].as_ref().unwrap().parse::<f64>().unwrap() > 0.0);

        let rows = client
            .query(
                "SELECT id, bm25_score(content, $1) AS score FROM agent_memory WHERE content @@ $1 ORDER BY bm25_score(content, $1) DESC",
                &[&"contract"],
            )
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get::<_, i32>(0), 1);
        assert!(rows[0].get::<_, f64>(1) > 0.0);
        assert_eq!(
            engine.query_stats().entries().last().unwrap().path,
            pg_engine::ExecutionPath::IndexLookup
        );
    });
}

#[test]
fn rust_postgres_probe_statements_error_but_connection_survives() {
    run_with_watchdog("probes", || {
        let (_tmp, _engine, port) = spawn_server();
        let mut client = connect(port);

        // Driver/psql startup probes outside the SQL subset (§7.4 / R3).
        assert!(client
            .batch_execute("SET client_encoding = 'UTF8'")
            .is_err());
        assert!(client.simple_query("SELECT version()").is_err());
        assert!(client.simple_query("SET search_path = public").is_err());

        // The connection must still be fully usable.
        client.batch_execute("CREATE TABLE t (id INT)").unwrap();
        client.batch_execute("INSERT INTO t VALUES (1)").unwrap();
        let msgs = client.simple_query("SELECT * FROM t").unwrap();
        assert_eq!(rows_of(&msgs), vec![vec![Some("1".to_string())]]);

        // Unknown tables error too — and the connection STILL survives.
        assert!(client.simple_query("SELECT * FROM nosuch").is_err());
        let msgs = client.simple_query("SELECT * FROM t").unwrap();
        assert_eq!(completions_of(&msgs), [1]);
    });
}

#[test]
fn rust_postgres_all_types_text_roundtrip() {
    run_with_watchdog("types", || {
        let (_tmp, engine, port) = spawn_server();
        // Seed types the SQL literal subset cannot express via the typed
        // API (the wire layer never grows execution logic — §7.1).
        engine
            .exec(
                None,
                "CREATE TABLE t (i4 INT, i8 BIGINT, s TEXT, ts TIMESTAMPTZ, u UUID, b BYTEA)",
            )
            .unwrap();
        engine
            .insert(
                "t",
                &[
                    Some(Datum::Int4(-7)),
                    Some(Datum::Int8(9_223_372_036_854_775_000)),
                    Some(Datum::Text("héllo wörld".to_string())),
                    Some(Datum::Timestamptz(1_700_000_000_123_456)),
                    Some(Datum::Uuid(
                        uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap(),
                    )),
                    Some(Datum::Bytea(vec![0x00, 0xff, 0x10])),
                ],
            )
            .unwrap();

        let mut client = connect(port);
        let msgs = client.simple_query("SELECT * FROM t").unwrap();
        assert_eq!(
            rows_of(&msgs),
            vec![vec![
                Some("-7".to_string()),
                Some("9223372036854775000".to_string()),
                Some("héllo wörld".to_string()),
                // Timestamptz is the µs integer, Uuid the standard string,
                // Bytea the \x hex form (§7.2).
                Some("1700000000123456".to_string()),
                Some("550e8400-e29b-41d4-a716-446655440000".to_string()),
                Some("\\x00ff10".to_string()),
            ]]
        );
    });
}

#[test]
fn rust_postgres_four_concurrent_clients() {
    run_with_watchdog("4 clients", || {
        let (_tmp, _engine, port) = spawn_server();
        let mut handles = Vec::new();
        for c in 0..4u32 {
            handles.push(thread::spawn(move || {
                let mut client = connect(port);
                let table = format!("t_{c}");
                client
                    .batch_execute(&format!("CREATE TABLE {table} (id INT, name TEXT)"))
                    .unwrap();
                for i in 0..10 {
                    client
                        .batch_execute(&format!("INSERT INTO {table} VALUES ({i}, 'n{i}')"))
                        .unwrap();
                }
                // Interleave an explicit transaction per client.
                client.batch_execute("BEGIN").unwrap();
                client
                    .batch_execute(&format!("UPDATE {table} SET name = 'x' WHERE id = 0"))
                    .unwrap();
                client.batch_execute("COMMIT").unwrap();
                client
                    .batch_execute(&format!("DELETE FROM {table} WHERE id > 7"))
                    .unwrap();
                let msgs = client
                    .simple_query(&format!("SELECT * FROM {table}"))
                    .unwrap();
                assert_eq!(completions_of(&msgs), [8], "client {c}");
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    });
}

#[test]
fn rust_postgres_extended_typed_crud_ann_explain_and_recovery() {
    run_with_watchdog("extended typed driver", || {
        let (_tmp, engine, port) = spawn_server();
        let mut client = connect(port);
        client
            .batch_execute("CREATE TABLE typed (id INT, big BIGINT, score FLOAT8, name TEXT)")
            .unwrap();
        let insert = client
            .prepare("INSERT INTO typed VALUES ($1, $2, $3, $4)")
            .unwrap();
        assert_eq!(
            client
                .execute(&insert, &[&1i32, &10i64, &1.25f64, &"one"])
                .unwrap(),
            1
        );
        assert_eq!(
            client
                .execute(&insert, &[&2i32, &20i64, &-2.5f64, &Option::<&str>::None])
                .unwrap(),
            1
        );

        let select = client
            .prepare("SELECT id, big, score, name FROM typed WHERE id = $1")
            .unwrap();
        let rows = client.query(&select, &[&2i32]).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get::<_, i32>(0), 2);
        assert_eq!(rows[0].get::<_, i64>(1), 20);
        assert_eq!(rows[0].get::<_, f64>(2), -2.5);
        assert_eq!(rows[0].get::<_, Option<String>>(3), None);

        let update = client
            .prepare("UPDATE typed SET name = $1 WHERE id = $2")
            .unwrap();
        assert_eq!(client.execute(&update, &[&"updated", &1i32]).unwrap(), 1);
        let updated = client
            .prepare("SELECT name FROM typed WHERE id = $1")
            .unwrap();
        assert_eq!(
            client.query(&updated, &[&1i32]).unwrap()[0].get::<_, String>(0),
            "updated"
        );
        let delete = client.prepare("DELETE FROM typed WHERE id = $1").unwrap();
        assert_eq!(client.execute(&delete, &[&1i32]).unwrap(), 1);
        let remaining = client.prepare("SELECT id FROM typed").unwrap();
        assert_eq!(client.query(&remaining, &[]).unwrap().len(), 1);

        let explain = client.prepare("EXPLAIN SELECT * FROM typed").unwrap();
        let plans = client.query(&explain, &[]).unwrap();
        assert!(!plans.is_empty());
        assert!(!plans[0].get::<_, String>(0).is_empty());

        assert!(client.prepare("SELECT * FROM missing").is_err());
        let after_error = client
            .simple_query("SELECT id FROM typed ORDER BY id")
            .unwrap();
        assert_eq!(completions_of(&after_error), [1]);

        client
            .batch_execute("CREATE TABLE ann (id INT, v VECTOR(2))")
            .unwrap();
        client
            .batch_execute("CREATE INDEX ann_idx ON ann USING hnsw (v vector_l2_ops)")
            .unwrap();
        let insert_ann = client
            .prepare_typed("INSERT INTO ann VALUES ($1, $2)", &[Type::INT4, Type::TEXT])
            .unwrap();
        client.execute(&insert_ann, &[&1i32, &"[1,0]"]).unwrap();
        client.execute(&insert_ann, &[&2i32, &"[0,1]"]).unwrap();

        let ann_query = client
            .prepare_typed(
                "SELECT id FROM ann ORDER BY v <-> $1 ASC LIMIT 1",
                &[Type::TEXT],
            )
            .unwrap();
        let ann_rows = client.query(&ann_query, &[&"[1,0]"]).unwrap();
        assert_eq!(ann_rows[0].get::<_, i32>(0), 1);
        assert_eq!(
            engine.query_stats().entries().last().unwrap().path,
            pg_engine::ExecutionPath::IndexLookup
        );
    });
}

fn read_backend_frame(stream: &mut std::net::TcpStream) -> (u8, Vec<u8>) {
    let mut tag = [0u8; 1];
    stream.read_exact(&mut tag).unwrap();
    let mut len = [0u8; 4];
    stream.read_exact(&mut len).unwrap();
    let len = u32::from_be_bytes(len) as usize;
    assert!(len >= 4);
    let mut body = vec![0u8; len - 4];
    stream.read_exact(&mut body).unwrap();
    (tag[0], body)
}

fn write_frontend_frame(stream: &mut std::net::TcpStream, tag: u8, body: &[u8]) {
    stream.write_all(&[tag]).unwrap();
    stream
        .write_all(&((body.len() + 4) as u32).to_be_bytes())
        .unwrap();
    stream.write_all(body).unwrap();
}

#[test]
fn malformed_bind_body_recovers_at_sync_over_real_socket() {
    run_with_watchdog("malformed Bind recovery", || {
        let (_tmp, _engine, port) = spawn_server();
        let mut stream = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut startup = Vec::new();
        startup.extend_from_slice(b"user\0pg_rust\0database\0pg_rust\0\0");
        let mut packet = Vec::new();
        packet.extend_from_slice(&((startup.len() + 8) as u32).to_be_bytes());
        packet.extend_from_slice(&196_608u32.to_be_bytes());
        packet.extend_from_slice(&startup);
        stream.write_all(&packet).unwrap();
        loop {
            let (tag, _) = read_backend_frame(&mut stream);
            if tag == b'Z' {
                break;
            }
        }

        let mut bind = Vec::new();
        bind.extend_from_slice(b"p\0s\0");
        bind.extend_from_slice(&0u16.to_be_bytes()); // parameter format count
        bind.extend_from_slice(&1u16.to_be_bytes()); // parameter count
        bind.extend_from_slice(&(-2i32).to_be_bytes()); // invalid length
        bind.extend_from_slice(&0u16.to_be_bytes()); // result format count
        write_frontend_frame(&mut stream, b'B', &bind);
        write_frontend_frame(&mut stream, b'S', &[]);

        let mut saw_error = false;
        let mut saw_ready = false;
        while !saw_ready {
            let (tag, body) = read_backend_frame(&mut stream);
            match tag {
                b'E' => {
                    saw_error = true;
                    assert!(body.windows(5).any(|window| window == b"08P01"));
                }
                b'Z' => {
                    saw_ready = true;
                    assert_eq!(body, [b'I']);
                }
                other => panic!("unexpected backend message {}", other as char),
            }
        }
        assert!(saw_error);
    });
}

#[test]
fn malformed_simple_query_completes_without_sync_over_real_socket() {
    run_with_watchdog("malformed Simple Query recovery", || {
        let (_tmp, _engine, port) = spawn_server();
        let mut stream = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut startup = Vec::new();
        startup.extend_from_slice(b"user\0pg_rust\0database\0pg_rust\0\0");
        let mut packet = Vec::new();
        packet.extend_from_slice(&((startup.len() + 8) as u32).to_be_bytes());
        packet.extend_from_slice(&196_608u32.to_be_bytes());
        packet.extend_from_slice(&startup);
        stream.write_all(&packet).unwrap();
        loop {
            let (tag, _) = read_backend_frame(&mut stream);
            if tag == b'Z' {
                break;
            }
        }

        write_frontend_frame(&mut stream, b'Q', b"SELECT 1\0junk");
        let mut saw_error = false;
        let mut saw_ready = false;
        while !saw_ready {
            let (tag, body) = read_backend_frame(&mut stream);
            match tag {
                b'E' => {
                    saw_error = true;
                    assert!(body.windows(5).any(|window| window == b"08P01"));
                }
                b'Z' => {
                    saw_ready = true;
                    assert_eq!(body, [b'I']);
                }
                other => panic!("unexpected backend message {}", other as char),
            }
        }
        assert!(saw_error);
    });
}

#[test]
fn malformed_simple_query_is_discarded_during_extended_error() {
    run_with_watchdog("malformed Q during extended error", || {
        let (_tmp, _engine, port) = spawn_server();
        let mut stream = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut startup = Vec::new();
        startup.extend_from_slice(b"user\0pg_rust\0database\0pg_rust\0\0");
        let mut packet = Vec::new();
        packet.extend_from_slice(&((startup.len() + 8) as u32).to_be_bytes());
        packet.extend_from_slice(&196_608u32.to_be_bytes());
        packet.extend_from_slice(&startup);
        stream.write_all(&packet).unwrap();
        loop {
            let (tag, _) = read_backend_frame(&mut stream);
            if tag == b'Z' {
                break;
            }
        }

        let mut execute = Vec::new();
        execute.extend_from_slice(b"missing\0");
        execute.extend_from_slice(&0u32.to_be_bytes());
        write_frontend_frame(&mut stream, b'E', &execute);
        let (tag, body) = read_backend_frame(&mut stream);
        assert_eq!(tag, b'E');
        assert!(body.windows(5).any(|window| window == b"34000"));

        write_frontend_frame(&mut stream, b'Q', b"SELECT 1\0junk");
        write_frontend_frame(&mut stream, b'S', &[]);
        let (tag, body) = read_backend_frame(&mut stream);
        assert_eq!(tag, b'Z', "malformed Q must be discarded until Sync");
        assert_eq!(body, [b'I']);
    });
}

#[test]
fn malformed_sync_emits_error_and_exactly_one_ready_for_query() {
    run_with_watchdog("malformed Sync recovery", || {
        let (_tmp, _engine, port) = spawn_server();
        let mut stream = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut startup = Vec::new();
        startup.extend_from_slice(b"user\0pg_rust\0database\0pg_rust\0\0");
        let mut packet = Vec::new();
        packet.extend_from_slice(&((startup.len() + 8) as u32).to_be_bytes());
        packet.extend_from_slice(&196_608u32.to_be_bytes());
        packet.extend_from_slice(&startup);
        stream.write_all(&packet).unwrap();
        loop {
            let (tag, _) = read_backend_frame(&mut stream);
            if tag == b'Z' {
                break;
            }
        }

        write_frontend_frame(&mut stream, b'S', b"x");
        let (tag, body) = read_backend_frame(&mut stream);
        assert_eq!(tag, b'E');
        assert!(body.windows(5).any(|window| window == b"08P01"));
        let (tag, body) = read_backend_frame(&mut stream);
        assert_eq!(tag, b'Z');
        assert_eq!(body, [b'I']);
    });
}

#[test]
fn malformed_sync_rolls_back_implicit_transaction() {
    let tmp = TempDir::new().unwrap();
    let engine = Arc::new(Engine::open(tmp.path(), EngineConfig::new(tmp.path())).unwrap());
    let mut session = Session::new(Arc::clone(&engine));
    let mut out = Vec::new();
    session.handle_query("CREATE TABLE t (id INT)", &mut out);
    session.handle_message(
        FrontendMessage::Parse {
            name: "i".to_string(),
            query: "INSERT INTO t VALUES (1)".to_string(),
            parameter_type_oids: vec![],
        },
        &mut out,
    );
    session.handle_message(
        FrontendMessage::Bind {
            portal: "p".to_string(),
            statement: "i".to_string(),
            parameter_formats: vec![],
            parameters: vec![],
            result_formats: vec![],
        },
        &mut out,
    );
    session.handle_message(
        FrontendMessage::Execute {
            portal: "p".to_string(),
            max_rows: 0,
        },
        &mut out,
    );
    // A malformed Sync sets extended_error before recovery. Simulate that
    // same failed state with an ordinary extended error, then Sync.
    session.handle_message(
        FrontendMessage::Parse {
            name: "bad".to_string(),
            query: "SELECT * FROM missing".to_string(),
            parameter_type_oids: vec![],
        },
        &mut out,
    );
    out.clear();
    session.handle_message(FrontendMessage::Sync, &mut out);
    assert_eq!(out.last(), Some(&STATUS_IDLE));

    out.clear();
    session.handle_query("SELECT * FROM t", &mut out);
    assert!(out.windows(8).any(|window| window == b"SELECT 0"));
}

#[test]
fn malformed_bind_enters_explicit_failed_state() {
    let tmp = TempDir::new().unwrap();
    let engine = Arc::new(Engine::open(tmp.path(), EngineConfig::new(tmp.path())).unwrap());
    let mut session = Session::new(Arc::clone(&engine));
    let mut out = Vec::new();
    session.handle_query("CREATE TABLE t (id INT)", &mut out);
    session.handle_query("BEGIN", &mut out);
    session.handle_message(
        FrontendMessage::Parse {
            name: "s".to_string(),
            query: "SELECT * FROM t WHERE id = $1".to_string(),
            parameter_type_oids: vec![23],
        },
        &mut out,
    );
    session.handle_message(
        FrontendMessage::Bind {
            portal: "p".to_string(),
            statement: "s".to_string(),
            parameter_formats: vec![FORMAT_BINARY],
            parameters: vec![Some(b"1".to_vec())],
            result_formats: vec![],
        },
        &mut out,
    );
    assert!(out.windows(5).any(|window| window == b"22P03"));
    out.clear();
    session.handle_message(FrontendMessage::Sync, &mut out);
    assert_eq!(out.last(), Some(&STATUS_FAILED));

    out.clear();
    session.handle_query("INSERT INTO t VALUES (2)", &mut out);
    assert!(out.windows(5).any(|window| window == b"25P02"));
    assert_eq!(out.last(), Some(&STATUS_FAILED));

    out.clear();
    session.handle_query("COMMIT", &mut out);
    assert!(out.windows(8).any(|window| window == b"ROLLBACK"));
    assert_eq!(out.last(), Some(&STATUS_IDLE));
}

// FunctionCall failure semantics are covered by the Session-level regression
// `function_call_enters_failed_state` in wire_extended.rs.
