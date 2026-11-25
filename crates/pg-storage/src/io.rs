//! Low-level I/O helpers used by the storage engine.
//!
//! These helpers encapsulate the crash-safe file operations required by M1:
//! directory creation, atomic file replacement, file preallocation, and
//! directory fsync.

use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::error::{Result, StorageError};

/// File name of the data file inside `{data_dir}/data/`.
///
/// Centralized here so BufferPool, PageAllocator, recovery, and tests all
/// refer to the same on-disk path via [`data_file_path`].
pub const DATA_FILE_NAME: &str = "datafile";

/// Build the absolute path of the data file for a given data directory.
pub fn data_file_path(data_dir: &Path) -> PathBuf {
    data_dir.join("data").join(DATA_FILE_NAME)
}

/// Create the database directory and its standard subdirectories.
///
/// Layout:
///
/// ```text
/// {data_dir}/
/// ├── data/
/// ├── wal/
/// ├── meta/
/// ├── clog/
/// └── tmp/
/// ```
///
/// `clog/` is created from Stage C onward; it holds the transaction commit
/// status (CLOG) segments used from M2b.
///
/// The parent directory is fsynced after creation.
pub fn ensure_data_dir(data_dir: &Path) -> Result<()> {
    fs::create_dir_all(data_dir)?;
    for sub in ["data", "wal", "meta", "clog", "tmp"] {
        fs::create_dir_all(data_dir.join(sub))?;
    }
    sync_dir(data_dir)?;
    Ok(())
}

/// Return the temporary path used by [`write_atomic`] for `path`.
///
/// Cleanup code must use this helper instead of reconstructing the suffix so
/// crash leftovers stay recognizable if the atomic-write convention changes.
pub fn atomic_temp_path(path: &Path) -> PathBuf {
    let mut tmp_name = path.as_os_str().to_os_string();
    tmp_name.push(".pg_rust_tmp");
    PathBuf::from(tmp_name)
}

/// Atomically write `contents` to `path`.
///
/// The implementation writes to a temporary file in the same directory,
/// fsyncs it, then renames it over `path`. Finally the parent directory
/// is fsynced so the rename is durable.
///
/// This is the standard crash-safe pattern for updating small metadata files.
///
/// // SAFETY: M1 uses a single-writer for superblock / freelist metadata, so
/// the fixed temp name `{path}.pg_rust_tmp` cannot collide with another
/// concurrent caller. Do not use this helper for multi-writer paths without
/// adding a unique temp suffix.
pub fn write_atomic(path: &Path, contents: &[u8]) -> Result<()> {
    let tmp = atomic_temp_path(path);

    // Ensure the parent directory exists. The caller is still responsible for
    // fsyncing higher-level directory state if needed.
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(StorageError::Io)?;
    }

    // Create/truncate the temp file. Any leftover temp file from a previous
    // crash is safely overwritten here.
    let mut file = File::create(&tmp).map_err(StorageError::Io)?;
    file.write_all(contents).map_err(StorageError::Io)?;
    // Skipped under cfg(loom): model builds do not check crash durability,
    // and a real fsync per setup dominates interleaving-exploration time.
    #[cfg(not(loom))]
    file.sync_all().map_err(StorageError::Io)?;
    drop(file);

    fs::rename(&tmp, path).map_err(StorageError::Io)?;

    if let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }

    Ok(())
}

/// Preallocate a file to `new_size` bytes.
///
/// M1 uses the portable `File::set_len` (ftruncate) path. Real
/// `fallocate()` semantics and `O_DIRECT` are Phase 7b optimization work.
pub fn preallocate_file(file: &File, new_size: u64) -> Result<()> {
    file.set_len(new_size).map_err(StorageError::Io)?;
    Ok(())
}

/// Fsync a directory so that directory metadata operations (e.g., rename)
/// are durable.
///
/// Under `cfg(loom)` this is a no-op: loom models check latch choreography,
/// not crash durability, and a real directory fsync per setup makes
/// thousands of interleavings prohibitively slow (see `crate::sync` docs).
#[cfg(all(unix, not(loom)))]
pub fn sync_dir(path: &Path) -> Result<()> {
    let dir = File::open(path).map_err(StorageError::Io)?;
    dir.sync_all().map_err(StorageError::Io)?;
    Ok(())
}

/// On non-Unix platforms (and in loom model builds) directory fsync is a
/// no-op.
#[cfg(any(not(unix), loom))]
pub fn sync_dir(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use tempfile::TempDir;

    #[test]
    fn ensure_data_dir_creates_subdirs() {
        let tmp = TempDir::new().unwrap();
        ensure_data_dir(tmp.path()).unwrap();
        assert!(tmp.path().join("data").is_dir());
        assert!(tmp.path().join("wal").is_dir());
        assert!(tmp.path().join("meta").is_dir());
        assert!(tmp.path().join("clog").is_dir());
        assert!(tmp.path().join("tmp").is_dir());
    }

    #[test]
    fn write_atomic_creates_file() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("test.txt");
        write_atomic(&path, b"hello world").unwrap();

        let mut file = File::open(&path).unwrap();
        let mut contents = String::new();
        file.read_to_string(&mut contents).unwrap();
        assert_eq!(contents, "hello world");
        assert!(!tmp.path().join("test.txt.pg_rust_tmp").exists());
    }

    #[test]
    fn write_atomic_overwrites_existing_file() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("test.txt");
        fs::write(&path, "old").unwrap();
        write_atomic(&path, b"new").unwrap();

        assert_eq!(fs::read_to_string(&path).unwrap(), "new");
    }

    #[test]
    fn preallocate_file_extends_file() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("data.dat");
        let file = File::create(&path).unwrap();
        preallocate_file(&file, 4096).unwrap();
        drop(file);

        assert_eq!(fs::metadata(&path).unwrap().len(), 4096);
    }
}
