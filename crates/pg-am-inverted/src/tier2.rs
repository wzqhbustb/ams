//! Tier 2 WAL tailing and freshness watermarks.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use pg_storage::error::{Result as StorageResult, StorageError};
use pg_storage::tier2::WalTailReader;
use pg_storage::types::Lsn;
use pg_storage::wal::reader::WalReader;
use pg_storage::wal::record::WalRecord;
use pg_storage::wal::WalWriter;

/// Freshness state used by the inverted planner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Watermark {
    /// In-process WAL prefix consumed by the index, including write buffer.
    pub consumed_lsn: Lsn,
    /// Durable WAL prefix recovered after segment metadata publication.
    pub durable_applied_lsn: Lsn,
}

/// Point-in-time WAL consumer lag and freshness metrics.
#[derive(Debug, Clone)]
pub struct LagMetrics {
    /// Flushed data-bearing records not yet applied by this consumer.
    /// Administrative `SegmentSeal`/`SegmentMerge` records are excluded because
    /// they do not change the query-visible document set.
    pub wal_lag_records: u64,
    /// Bytes of the data-bearing records in `wal_lag_records`, excluding
    /// administrative segment markers.
    pub wal_lag_bytes: u64,
    /// WAL prefix applied by this consumer.
    pub applied_lsn: Lsn,
    /// Current durable WAL frontier.
    pub synced_lsn: Lsn,
    /// Last time the consumer made progress.
    pub last_progress_at: Instant,
    /// Non-negative wall-clock seconds since `last_progress_at`.
    pub watermark_lag_seconds: f64,
}

/// Pull-based WAL tail reader bounded by the flush frontier captured when the
/// iterator is created. At most one record is decoded per `next` call.
pub struct FlushedWalTail {
    wal_dir: PathBuf,
    segment_size: u64,
    wal: Arc<WalWriter>,
}

impl FlushedWalTail {
    /// Create a tail reader over one storage engine's WAL directory and writer.
    pub fn new(wal_dir: impl AsRef<Path>, segment_size: u64, wal: Arc<WalWriter>) -> Self {
        Self {
            wal_dir: wal_dir.as_ref().to_path_buf(),
            segment_size,
            wal,
        }
    }
}

fn open_tail_reader(wal_dir: &Path, segment_size: u64, start: Lsn) -> StorageResult<WalReader> {
    match WalReader::open_at(wal_dir, segment_size, start) {
        Ok(reader) => Ok(reader),
        Err(StorageError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
            Err(StorageError::MetadataCorrupted(format!(
                "WAL recycled before inverted replay point {start}; rebuild required"
            )))
        }
        Err(error) => Err(error),
    }
}

impl WalTailReader for FlushedWalTail {
    fn tail_from(&self, start: Lsn) -> Box<dyn Iterator<Item = StorageResult<WalRecord>> + '_> {
        let start = if start.is_valid() { start } else { Lsn::FIRST };
        let frontier = self.wal.synced_lsn();
        let (reader, error) = match open_tail_reader(&self.wal_dir, self.segment_size, start) {
            Ok(reader) => (Some(reader), None),
            Err(error) => (None, Some(error)),
        };
        Box::new(FlushedWalTailIter {
            reader,
            frontier,
            error,
            done: false,
        })
    }
}

struct FlushedWalTailIter {
    reader: Option<WalReader>,
    frontier: Lsn,
    error: Option<StorageError>,
    done: bool,
}

impl Iterator for FlushedWalTailIter {
    type Item = StorageResult<WalRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        if let Some(error) = self.error.take() {
            self.done = true;
            return Some(Err(error));
        }
        let record = match self.reader.as_mut()?.next_record() {
            Ok(Some(record)) => record,
            Ok(None) => {
                self.done = true;
                return None;
            }
            Err(error) => {
                self.done = true;
                return Some(Err(error));
            }
        };
        if record.lsn.advance(record.record_size() as u64) > self.frontier {
            self.done = true;
            return None;
        }
        Some(Ok(record))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pg_storage::config::StorageConfig;
    use pg_storage::types::PageId;

    #[test]
    fn tail_reads_only_flushed_records_across_segments_and_resumes() {
        let tmp = tempfile::TempDir::new().unwrap();
        let mut config = StorageConfig::new(tmp.path());
        config.wal_segment_size = 1024;
        config.wal_group_commit_batch_size = 1024;
        config.wal_group_commit_timeout_ms = 60_000;
        let wal = Arc::new(WalWriter::open(tmp.path(), &config).unwrap());
        let mut appended = Vec::new();
        for page in 1..=40 {
            appended.push(
                wal.append(WalRecord::page_alloc(PageId(page)).unwrap())
                    .unwrap(),
            );
        }
        let tail = FlushedWalTail::new(
            tmp.path().join("wal"),
            config.wal_segment_size,
            Arc::clone(&wal),
        );
        assert!(tail.tail_from(Lsn::FIRST).next().is_none());

        wal.flush().unwrap();
        let records = tail
            .tail_from(Lsn::FIRST)
            .collect::<StorageResult<Vec<_>>>()
            .unwrap();
        assert_eq!(records.len(), appended.len());
        assert!(records.windows(2).all(|pair| pair[0].lsn < pair[1].lsn));
        assert_eq!(records[0].lsn, appended[0]);

        let resume_at = records[19]
            .lsn
            .advance(records[19].encode().unwrap().len() as u64);
        let resumed = tail
            .tail_from(resume_at)
            .collect::<StorageResult<Vec<_>>>()
            .unwrap();
        assert_eq!(resumed.len(), 20);
        assert_eq!(resumed[0].lsn, records[20].lsn);
    }

    #[test]
    fn recycled_start_segment_is_a_loud_rebuild_error() {
        let tmp = tempfile::TempDir::new().unwrap();
        let mut config = StorageConfig::new(tmp.path());
        config.wal_group_commit_batch_size = 1024;
        config.wal_group_commit_timeout_ms = 60_000;
        let wal = Arc::new(WalWriter::open(tmp.path(), &config).unwrap());
        wal.append(WalRecord::page_alloc(PageId(1)).unwrap())
            .unwrap();
        wal.flush().unwrap();
        std::fs::remove_file(
            tmp.path()
                .join("wal")
                .join(pg_storage::wal::segment::wal_filename(0)),
        )
        .unwrap();
        let tail = FlushedWalTail::new(
            tmp.path().join("wal"),
            config.wal_segment_size,
            Arc::clone(&wal),
        );
        let error = tail.tail_from(Lsn::FIRST).next().unwrap().unwrap_err();
        assert!(matches!(error, StorageError::MetadataCorrupted(_)));
        assert!(error.to_string().contains("rebuild required"));
    }
}
