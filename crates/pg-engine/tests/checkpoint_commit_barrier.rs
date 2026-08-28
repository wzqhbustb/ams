//! M2c Stage P: the commit/checkpoint barrier now lives inside
//! `TxnManager` (sunk down from pg-engine's `commit_barrier` field), and the
//! checkpoint coordinator takes its write guard internally via
//! `set_commit_barrier`. These tests pin the post-sink contract:
//!
//! - concurrent auto-commit DML + `Engine::checkpoint` never deadlock and
//!   every committed row survives (the barrier read/write guards compose);
//! - a commit issued through the `txn_manager()` back door — undefined
//!   behavior before the sink — is now checkpoint-safe by construction and
//!   its row survives a checkpoint + reopen.

use std::sync::Arc;
use std::thread;

use pg_engine::{Engine, EngineConfig, QueryResult};
use tempfile::TempDir;

fn open(dir: &std::path::Path) -> Engine {
    Engine::open(dir, EngineConfig::new(dir)).unwrap()
}

fn row_count(engine: &Engine, table: &str) -> usize {
    match engine
        .exec(None, &format!("SELECT * FROM {table}"))
        .unwrap()
    {
        QueryResult::Rows { rows, .. } => rows.len(),
        other => panic!("expected Rows, got {other:?}"),
    }
}

/// Concurrent auto-commit inserts racing `Engine::checkpoint`: the sunken
/// barrier serializes each commit's hard order against the checkpoint's
/// CLOG flush without deadlocking, and every committed insert is visible
/// afterwards.
#[test]
fn checkpoint_racing_commits_neither_deadlocks_nor_loses_rows() {
    const WRITERS: usize = 4;
    const INSERTS_PER_WRITER: usize = 20;

    let tmp = TempDir::new().unwrap();
    let engine = Arc::new(open(tmp.path()));
    engine
        .exec(None, "CREATE TABLE t (id INT, name TEXT)")
        .unwrap();

    let mut handles = Vec::new();
    for w in 0..WRITERS {
        let engine = Arc::clone(&engine);
        handles.push(thread::spawn(move || {
            for i in 0..INSERTS_PER_WRITER {
                let id = (w * INSERTS_PER_WRITER + i) as i64;
                engine
                    .exec(None, &format!("INSERT INTO t VALUES ({id}, 'w{w}')"))
                    .unwrap();
            }
        }));
    }

    // Checkpoint repeatedly while the writers run. If the barrier wiring
    // were broken (e.g. the write guard taken while a commit holds a lock
    // the checkpoint needs), this would wedge here.
    for _ in 0..10 {
        engine.checkpoint().unwrap();
    }

    for h in handles {
        h.join().unwrap();
    }
    // A final checkpoint after all commits, then verify the count.
    engine.checkpoint().unwrap();
    assert_eq!(row_count(&engine, "t"), WRITERS * INSERTS_PER_WRITER);
}

/// The Stage L caveat is resolved: committing directly through
/// `Engine::txn_manager()` (no engine-level guard) concurrent with a
/// checkpoint is safe — the manager self-guards. The row written under a
/// directly-committed XID must survive a checkpoint and a reopen.
#[test]
fn direct_txn_manager_commit_is_checkpoint_safe() {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine
        .exec(None, "CREATE TABLE t (id INT, name TEXT)")
        .unwrap();

    // Begin + write through the engine (so the heap row carries `xid`),
    // but COMMIT through the raw manager — the former back door.
    let handle = engine.begin_txn().unwrap();
    let xid = handle.xid();
    engine
        .exec(Some(&handle), "INSERT INTO t VALUES (1, 'backdoor')")
        .unwrap();
    // Dropping the handle without commit would auto-abort; take the
    // commit into our own hands instead.
    std::mem::forget(handle);

    // Race the direct commit against checkpoints. Without the sunken
    // barrier this was the undefined interleaving; now the manager's
    // internal read guard serializes it against the coordinator's write
    // guard.
    let engine_ref = &engine;
    thread::scope(|s| {
        s.spawn(|| {
            for _ in 0..5 {
                engine_ref.checkpoint().unwrap();
            }
        });
        engine_ref.txn_manager().commit_txn(xid).unwrap();
    });
    // The back-door commit does not release table locks (the LockManager
    // is an engine-layer concept): the forgotten handle's INSERT took
    // RowExclusive on `t` under `xid`, so release it explicitly here —
    // otherwise the grant would linger until engine drop (harmless in this
    // test, a wedge for any later DDL in real code).
    engine.lock_manager().release_all(xid);

    engine.checkpoint().unwrap();
    drop(engine);

    let engine = open(tmp.path());
    assert_eq!(
        row_count(&engine, "t"),
        1,
        "direct commit must survive checkpoint + reopen"
    );
}
