//! pg_rust storage engine — Phase 1 M1.
//!
//! This crate implements the physical storage layer:
//! - Page allocation
//! - Write-ahead logging (WAL)
//! - Buffer pool
//! - LSN clock
//! - Checkpoint / recovery
//!
//! M2 additions so far (Stage 0a): page headers with the authoritative
//! `pd_lsn` ([`mod@page`]), the commit-status abstraction ([`clog`]), and the
//! redo-dispatch registry used by recovery ([`recovery`]).
//!
//! M3 Stage G adds two **interface reservations** (contracts only, no
//! implementation — tech-selection §8/§9): [`segment`] for the segment-based
//! storages of Phase 3/5, and [`tier2`] for asynchronous Tier 2 index
//! followership (WAL tailing + per-index freshness watermarks).
//!
//! M2c Stage Q adds the [`sync`] alias layer: production builds re-export
//! `parking_lot` / `std::sync::atomic` unchanged, while `--features loom`
//! (test-only model builds) swaps in loom's instrumented primitives so
//! exhaustive interleaving models can drive the real buffer-pool/WAL latch
//! choreography. See the [`sync`] module docs for what is stubbed under loom.
//!
//! It intentionally does **not** expose a generic "File Manager" abstraction.
//! File-management responsibilities live inside the components that own the
//! crash-safety invariants.

#![warn(missing_docs)]
#![warn(rust_2018_idioms)]

pub mod analysis;
pub mod buffer_pool;
pub mod checkpoint;
pub mod clog;
pub mod config;
mod data_dir_lock;
pub mod engine;
pub mod error;
pub mod freelist_meta;
pub mod io;
pub mod lsn_clock;
pub mod oid;
pub mod page;
pub mod page_allocator;
pub mod positioned_file;
pub mod recovery;
pub mod segment;
pub mod superblock;
pub mod sync;
pub mod tier2;
pub mod txn_id;
pub mod types;
pub mod wal;

/// Initialize a global `tracing` subscriber for tests.
///
/// Applications using `pg-storage` as a library should set up their own
/// subscriber instead of calling this function.
#[cfg(test)]
pub(crate) fn init_test_logging() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logging_initializes_without_panic() {
        // Smoke test that ensures the test workspace builds and the tracing
        // subscriber can be initialized.
        init_test_logging();
    }
}
