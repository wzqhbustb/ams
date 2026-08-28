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

use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use pg_engine::{Datum, Engine, EngineConfig};
use pg_wire::Server;
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
