//! Diagnostics report rendering (M3 Stage E, tech-selection §6.2 + coding
//! plan S1).
//!
//! Pure formatting over the §6.2 introspection APIs. The `pg-diag` binary
//! (`src/bin/pg-diag.rs`) is a thin shell over these functions so the CLI's
//! exact output is testable in-process against live fixtures (active
//! transactions, real lock waits) — an independent `pg-diag` process would
//! see only its own freshly-opened engine, which is the documented M3
//! boundary (single-process diagnostics; live cross-process diagnostics is
//! Phase 4a via pg-wire).

use std::fmt::Write;

use crate::Engine;

/// Render the `pg-diag txn` report: active XIDs, the vacuum horizon, and
/// the CLOG / buffer-pool hit rates (§6.2).
pub fn txn_report(engine: &Engine) -> String {
    let xids: Vec<u64> = engine.active_xids().iter().map(|x| x.0).collect();
    let mut out = String::new();
    let _ = writeln!(out, "active_xids={xids:?}");
    let _ = writeln!(
        out,
        "oldest_snapshot_xmin={}",
        engine.oldest_snapshot_xmin().0
    );
    let _ = writeln!(out, "clog_hit_rate={:.4}", engine.clog_hit_rate());
    let _ = writeln!(
        out,
        "buffer_pool_hit_rate={:.4}",
        engine.buffer_pool_hit_rate()
    );
    out
}

/// Render the `pg-diag locks` report: the full wait-for graph (row-lock
/// edges + table-lock edges, exactly what the deadlock detector consumes)
/// plus the contended tables' granted sets and FIFO wait queues (§6.2).
///
/// Tables with grants but an empty wait queue contribute no wait-for edges
/// and are not listed — [`pg_txn::LockManager::table_lock_states`] filters
/// them out by design.
///
/// # Non-atomic snapshot (M3 Stage E review F4)
///
/// The edge list and the per-table states come from two SEPARATE snapshots
/// taken a moment apart (the same is true inside
/// [`pg_txn::wait_for_edges`], which locks its two sources in turn). Under
/// concurrent lock traffic the two halves can transiently disagree — an
/// edge whose waiter has already been granted, or a table queue whose
/// edge is gone. That is inherent to lock-free observation and acceptable
/// for diagnostics; the deadlock detector lives with the same property.
pub fn locks_report(engine: &Engine) -> String {
    let mut out = String::new();
    let edges = engine.wait_edges();
    if edges.is_empty() {
        let _ = writeln!(out, "wait_for_edges=[]");
    } else {
        let _ = writeln!(out, "wait_for_edges:");
        for (waiter, holder) in edges {
            let _ = writeln!(out, "  waiter={} -> holder={}", waiter.0, holder.0);
        }
    }
    let states = engine.lock_manager().table_lock_states();
    if states.is_empty() {
        let _ = writeln!(out, "table_lock_states=[]");
    } else {
        let _ = writeln!(out, "table_lock_states:");
        for (table, state) in states {
            let granted: Vec<String> = state
                .granted
                .iter()
                .map(|(x, m)| format!("{}:{m:?}", x.0))
                .collect();
            let waiters: Vec<String> = state
                .waiters
                .iter()
                .map(|(x, m)| format!("{}:{m:?}", x.0))
                .collect();
            let _ = writeln!(
                out,
                "  table={} granted=[{}] waiters=[{}]",
                table.0,
                granted.join(", "),
                waiters.join(", ")
            );
        }
    }
    out
}
