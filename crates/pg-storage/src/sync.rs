//! Synchronization-primitive alias layer (Stage Q).
//!
//! # Why this exists
//!
//! `loom` 0.7 has no `parking_lot` integration, so parking_lot locks cannot
//! be scheduled by loom's exhaustive interleaving explorer. To model-test the
//! real latch choreography (Blink crabbing, coupled right hops, the
//! optimistic/pessimistic write paths in `pg-am-btree::index`), every lock
//! that participates in that choreography is imported from **here** instead
//! of directly from `parking_lot` / `std::sync`:
//!
//! - **Production (`not(loom)`)**: these names are plain re-exports of
//!   `parking_lot` and `std::sync::atomic` — zero wrapper, zero cost, and the
//!   public API types (e.g. `Arc<Mutex<PageAllocator>>`) remain exactly the
//!   parking_lot types downstream crates already use.
//! - **Model builds (`--features loom`, which sets `--cfg loom` via
//!   `build.rs`)**: thin wrappers around `loom::sync` primitives with the
//!   parking_lot *method* shape (`lock()` returns a bare guard, `try_lock()`
//!   returns `Option`), so call sites compile unchanged in both worlds.
//!
//! `Arc` is deliberately **not** aliased: `loom::sync::Arc` cannot coerce to
//! `Arc<dyn Trait>` on stable Rust, and aliasing it would cascade a
//! `cfg(loom)` split into every downstream crate (`pg-txn`, `pg-engine`,
//! `pg-am-*`). Reference counting is not part of the race surface loom needs
//! to explore, so `std::sync::Arc` stays everywhere.
//!
//! # What is stubbed under `cfg(loom)`
//!
//! - [`crate::wal::writer::WalWriter`] does not spawn its background
//!   group-commit worker; `WalWriter::flush_to` marks the target LSN synced
//!   inline, without any fsync (loom models must not block on real I/O, and
//!   durability is not what the models check).
//! - `crate::buffer_pool::BufferPool::flush_frame` performs the
//!   dirty/rec_lsn/needs_fpi state transitions but skips the data-file write
//!   and fsync. Loom models must size the pool so no eviction happens: an
//!   evicted page reloaded from disk would read zeros, since nothing was ever
//!   written.
//! - Setup-path fsyncs are no-ops ([`crate::io::sync_dir`],
//!   [`crate::io::write_atomic`]'s temp-file fsync, and the WAL segment
//!   preallocation fsync): durability is meaningless inside a model, and a
//!   real F_FULLFSYNC (~ms each) per iteration makes exploring thousands of
//!   interleavings prohibitively slow. Segment-file writes themselves are
//!   kept (buffered, no fsync).
//!
//! # Caveat
//!
//! loom primitives **panic when used outside `loom::model`**. In a
//! `--features loom` build, only the loom model tests
//! (`pg-am-btree/tests/btree_loom.rs`) may *run*; the other test binaries
//! still compile but must not be executed under the feature. Production
//! (default-feature) builds and tests are completely unaffected.
//!
//! # The alias rule (enforced in CI)
//!
//! **Every lock that participates in the buffer-pool / WAL latch
//! choreography, or that crosses a crate boundary (a type another crate
//! locks through `pg-storage`'s API), MUST be imported from
//! `crate::sync`**, never directly from `parking_lot` or `std::sync`. A
//! direct import compiles fine in production but silently drops that lock
//! out of the loom schedule space — the model tests then explore a
//! choreography that is not the real one. This is enforced by a CI grep
//! guard (`.github/workflows/ci.yml`, loom job): `git grep "use
//! parking_lot" -- crates/pg-storage/src` must match only this file.
//! Locks with no concurrency semantics under test (purely local,
//! single-thread-scope guards) may use `std`/`parking_lot` directly, but
//! when in doubt, alias.

#[cfg(not(loom))]
pub use parking_lot::{Condvar, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
#[cfg(not(loom))]
pub use std::sync::atomic;

#[cfg(loom)]
pub use loom::sync::atomic;
#[cfg(loom)]
pub use loom::sync::{MutexGuard, RwLockReadGuard, RwLockWriteGuard};

#[cfg(loom)]
pub use loom_wrappers::{Condvar, Mutex, RwLock};

#[cfg(loom)]
mod loom_wrappers {
    use super::{MutexGuard, RwLockReadGuard, RwLockWriteGuard};

    /// `loom::sync::Mutex` behind the parking_lot method shape:
    /// `lock()` unwraps the `LockResult`, `try_lock()` maps to `Option`.
    /// (parking_lot never poisons; the unwrap is the loom analogue.)
    pub struct Mutex<T: ?Sized> {
        inner: loom::sync::Mutex<T>,
    }

    impl<T> Mutex<T> {
        /// Create a new mutex, mirroring `parking_lot::Mutex::new`.
        pub fn new(value: T) -> Self {
            Self {
                inner: loom::sync::Mutex::new(value),
            }
        }
    }

    impl<T: ?Sized> Mutex<T> {
        /// Lock, unwrapping loom's `LockResult` (parking_lot shape).
        pub fn lock(&self) -> MutexGuard<'_, T> {
            self.inner.lock().expect("loom mutex poisoned")
        }

        /// Try to lock, mapping loom's `TryLockResult` to `Option`.
        pub fn try_lock(&self) -> Option<MutexGuard<'_, T>> {
            self.inner.try_lock().ok()
        }
    }

    impl<T: std::fmt::Debug> std::fmt::Debug for Mutex<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            std::fmt::Debug::fmt(&self.inner, f)
        }
    }

    /// `loom::sync::RwLock` behind the parking_lot method shape.
    /// `loom::sync::RwLock` wrapped in parking_lot's method shape.
    ///
    /// Note: `try_read` / `try_write` are NOT exposed (the Mutex wrapper
    /// exposes `try_lock` because `try_pin_resident` needs it). No current
    /// loom-scheduled path calls them — but `Frame::content` is an RwLock
    /// in production, so any future loom model of buffer-pool concurrency
    /// (eviction, `try_pin_resident`-style probes) will need them; add them
    /// here when such a model lands.
    pub struct RwLock<T> {
        inner: loom::sync::RwLock<T>,
    }

    impl<T> RwLock<T> {
        /// Create a new rwlock, mirroring `parking_lot::RwLock::new`.
        pub fn new(value: T) -> Self {
            Self {
                inner: loom::sync::RwLock::new(value),
            }
        }
    }

    impl<T> RwLock<T> {
        /// Read-lock, unwrapping loom's `LockResult`.
        pub fn read(&self) -> RwLockReadGuard<'_, T> {
            self.inner.read().expect("loom rwlock poisoned")
        }

        /// Write-lock, unwrapping loom's `LockResult`.
        pub fn write(&self) -> RwLockWriteGuard<'_, T> {
            self.inner.write().expect("loom rwlock poisoned")
        }
    }

    impl<T: std::fmt::Debug> std::fmt::Debug for RwLock<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            std::fmt::Debug::fmt(&self.inner, f)
        }
    }

    /// `loom::sync::Condvar` reduced to what the model build needs.
    ///
    /// `wait` / `wait_for` are intentionally absent: the WAL group-commit
    /// worker (their only consumer) does not run under loom — `flush_to`
    /// completes synchronously inline — so no loom code path ever blocks on
    /// a condition variable. This matches the `flush_frame` loom variant,
    /// which asserts a single flusher (`debug_assert!(!meta.flushing)`).
    ///
    /// Assumption to revisit: if a future model schedules `flush_frame`
    /// concurrency (e.g. checkpoint group-fsync coalescing), waiters WILL
    /// need `wait` — loom schedules condvar waits fine; only the producer
    /// (the worker thread) is absent today. Add it here when that model
    /// lands.
    #[derive(Debug)]
    pub struct Condvar {
        inner: loom::sync::Condvar,
    }

    impl Condvar {
        /// Create a new condvar.
        pub fn new() -> Self {
            Self {
                inner: loom::sync::Condvar::new(),
            }
        }

        /// Wake one waiter (a no-op under loom; kept so `append` compiles).
        pub fn notify_one(&self) {
            self.inner.notify_one();
        }

        /// Wake all waiters (a no-op under loom; kept so error paths compile).
        pub fn notify_all(&self) {
            self.inner.notify_all();
        }
    }

    impl Default for Condvar {
        fn default() -> Self {
            Self::new()
        }
    }
}
