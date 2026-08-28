//! Exclusive data-directory lock (M3 Stage E review F1).
//!
//! Opening a data directory creates `{data_dir}/lock` with `create_new`
//! (O_EXCL) and holds it for the storage engine's lifetime; `Drop` removes
//! it. A second PROCESS opening the same directory — e.g. `pg-diag`
//! pointed at a running server — gets a clean "already in use" error
//! naming the holder's pid, instead of two engines reading and writing the
//! same files uncoordinated.
//!
//! # Why not flock(2)
//!
//! The zero-new-dependency rule (tech-selection §10) excludes `fs2` and a
//! direct `libc` edge, and std has no file-lock API at MSRV 1.86
//! (`File::lock` stabilizes in 1.89). An O_EXCL lock file carrying the
//! holder's pid is the portable minimum that works identically on macOS
//! and Linux.
//!
//! # Known limitations (accepted first version, documented)
//!
//! - **Crash residue**: a killed (kill -9) process leaves the file behind.
//!   The next opener reports it and names the manual fix (remove the
//!   file). Automatic stale detection — `kill(pid, 0)`, the shape of PG's
//!   `postmaster.pid` protocol — needs `libc`, so it is deferred.
//! - **Same-pid conflicts are reclaimed, not rejected**: the test suite's
//!   crash idiom is `mem::forget(engine)` followed by a reopen IN THE SAME
//!   PROCESS (~100 crash-recovery tests), which any unconditional
//!   same-process exclusion would break (a forgotten engine's `Drop` never
//!   runs, so neither a lock file nor a held flock fd would be released).
//!   A lock file whose recorded pid is our own is therefore treated as
//!   stale and overwritten with a warning. The cost: same-process
//!   double-open of a LIVE engine is not prevented — a programming error
//!   outside F1's scope (F1's hazard is a second process).

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::error::{Result, StorageError};

/// Lock file name inside the data directory.
pub(crate) const LOCK_FILE_NAME: &str = "lock";

/// Held for the lifetime of a [`StorageEngine`](crate::engine::StorageEngine);
/// see the module docs.
#[derive(Debug)]
pub(crate) struct DataDirLock {
    path: PathBuf,
    _file: File,
}

impl DataDirLock {
    /// Acquire the directory lock, or fail with an actionable error.
    pub(crate) fn acquire(data_dir: &Path) -> Result<Self> {
        let path = data_dir.join(LOCK_FILE_NAME);
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(file) => Ok(Self::held(path, file)),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                let holder = std::fs::read_to_string(&path).unwrap_or_default();
                if holder_pid(&holder) == Some(std::process::id()) {
                    // In-process crash-test idiom (module docs): the
                    // previous engine was `mem::forget`ed, so its lock was
                    // never released. Reclaim.
                    tracing::warn!(
                        dir = %data_dir.display(),
                        "reclaiming same-process stale data-directory lock"
                    );
                    let file = OpenOptions::new()
                        .write(true)
                        .truncate(true)
                        .open(&path)
                        .map_err(StorageError::Io)?;
                    return Ok(Self::held(path, file));
                }
                let holder = holder.trim();
                Err(StorageError::InvalidOperation(format!(
                    "data directory {} is already in use (lock file {} held by {}); \
                     if the holder crashed, remove the stale lock file and retry",
                    data_dir.display(),
                    path.display(),
                    if holder.is_empty() {
                        "an unknown process".to_string()
                    } else {
                        holder.to_string()
                    },
                )))
            }
            Err(e) => Err(StorageError::Io(e)),
        }
    }

    /// Common tail: record our pid in the held file (informational only —
    /// read back solely for error messages and the same-pid check).
    fn held(path: PathBuf, mut file: File) -> Self {
        let _ = writeln!(file, "pid={}", std::process::id());
        Self { path, _file: file }
    }
}

impl Drop for DataDirLock {
    fn drop(&mut self) {
        // Best effort: a failure here (e.g. the directory is already gone)
        // leaves a stale file, which `acquire` reports with instructions.
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Parse the `pid=<n>` line written by [`DataDirLock::held`].
fn holder_pid(contents: &str) -> Option<u32> {
    contents.trim().strip_prefix("pid=")?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn second_acquire_same_process_reclaims_stale_file() {
        // The crash-test idiom: forget the guard (its Drop never runs),
        // re-acquire in the same process → same pid → reclaimed.
        let tmp = tempfile::TempDir::new().unwrap();
        let first = DataDirLock::acquire(tmp.path()).unwrap();
        std::mem::forget(first);
        assert!(tmp.path().join(LOCK_FILE_NAME).exists());
        let second = DataDirLock::acquire(tmp.path()).unwrap();
        drop(second);
        assert!(!tmp.path().join(LOCK_FILE_NAME).exists());
    }

    #[test]
    fn foreign_pid_lock_file_is_rejected_with_instructions() {
        let tmp = tempfile::TempDir::new().unwrap();
        let dir = tmp.path();
        std::fs::write(
            dir.join(LOCK_FILE_NAME),
            format!("pid={}", std::process::id() + 1_000_000),
        )
        .unwrap();
        let err = DataDirLock::acquire(dir).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("already in use"), "{msg}");
        assert!(msg.contains("remove the stale lock file"), "{msg}");
        // Removing the residue unblocks the open.
        std::fs::remove_file(dir.join(LOCK_FILE_NAME)).unwrap();
        assert!(DataDirLock::acquire(dir).is_ok());
    }
}
