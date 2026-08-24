//! WAL reader for sequential record access.
//!
//! The reader treats the WAL as a single logical byte stream split across fixed-
//! size segment files. It can start at any aligned LSN and reads records
//! sequentially, automatically opening the next segment file when required.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use crate::error::{Result, StorageError};
use crate::types::{align_up, Lsn, LSN_ALIGNMENT};
use crate::wal::record::{WalRecord, WAL_RECORD_HEADER_SIZE};
use crate::wal::segment::wal_filename;

/// A sequential WAL reader.
///
/// `WalReader` is not thread-safe; the caller is expected to serialize access
/// or create one reader per thread.
#[derive(Debug)]
pub struct WalReader {
    wal_dir: PathBuf,
    segment_size: u64,
    current_segment_id: u64,
    current_file: File,
    current_lsn: Lsn,
}

impl WalReader {
    /// Open a reader starting at [`Lsn::FIRST`] in `wal_dir`.
    pub fn open(wal_dir: impl AsRef<Path>, segment_size: u64) -> Result<Self> {
        Self::open_at(wal_dir, segment_size, Lsn::FIRST)
    }

    /// Open a reader starting at `start_lsn`.
    ///
    /// `start_lsn` must be valid, aligned, and point to the first byte of a
    /// record (typically a checkpoint redo point). If `start_lsn` falls in the
    /// middle of a record, decoding will fail. The caller is responsible for
    /// ensuring that the corresponding segment file exists.
    pub fn open_at(wal_dir: impl AsRef<Path>, segment_size: u64, start_lsn: Lsn) -> Result<Self> {
        if !start_lsn.is_valid() || start_lsn.0 % LSN_ALIGNMENT != 0 {
            return Err(StorageError::InvalidConfig(format!(
                "invalid WAL start LSN {start_lsn}"
            )));
        }
        if segment_size == 0 || segment_size % LSN_ALIGNMENT != 0 {
            return Err(StorageError::InvalidConfig(format!(
                "invalid WAL segment size {segment_size}"
            )));
        }

        let wal_dir = wal_dir.as_ref().to_path_buf();
        let segment_id = start_lsn.segment_id(segment_size);
        let file = Self::open_segment_file(&wal_dir, segment_id)?;

        Ok(Self {
            wal_dir,
            segment_size,
            current_segment_id: segment_id,
            current_file: file,
            current_lsn: start_lsn,
        })
    }

    /// Return the LSN at which the next record read will begin.
    pub fn current_lsn(&self) -> Lsn {
        self.current_lsn
    }

    /// Read the next record from the WAL.
    ///
    /// Returns `Ok(None)` when the end of the durable WAL is reached. An all-
    /// zero header (uninitialized padding) is treated as a clean end-of-WAL
    /// marker, and so is a CRC-failing record that is provably a torn tail
    /// (header pins it to this position, nothing but zeros after it — see
    /// [`Self::is_torn_tail`]): a crash can interrupt the writer mid-record,
    /// and the preallocated segment makes the unwritten remainder read back
    /// as zeros instead of a short read. Any other decode or CRC failure is
    /// returned as an error.
    pub fn next_record(&mut self) -> Result<Option<WalRecord>> {
        let start_lsn = self.current_lsn;
        let mut header = [0u8; WAL_RECORD_HEADER_SIZE];
        if !self.try_read_exact(&mut header)? {
            return Ok(None);
        }

        // An all-zero header means we have reached the uninitialized tail of
        // the WAL — or a zero-filled hole left by a non-atomic reserve+append
        // (Stage N review, P0-3). Before treating it as end-of-WAL, probe
        // forward one header: if that candidate is also all zeros, we're at
        // the preallocated tail. If it is non-zero, we've hit a hole — a
        // crash filled a reserved slot that was never written, and the
        // records after it are silently lost if we return Ok(None) here.
        // Hard-fail instead.
        if header.iter().all(|&b| b == 0) {
            let probe_pos = start_lsn.0 + WAL_RECORD_HEADER_SIZE as u64;
            let mut probe = [0u8; WAL_RECORD_HEADER_SIZE];
            if self.try_read_exact_at(probe_pos, &mut probe)? && probe.iter().any(|&b| b != 0) {
                return Err(StorageError::MetadataCorrupted(format!(
                    "WAL hole detected: zero-filled header at {:?} followed by non-zero data at {:?}; \
                     an unflushed reserved slot truncated the log",
                    start_lsn, Lsn(probe_pos)
                )));
            }
            self.current_lsn = start_lsn;
            return Ok(None);
        }

        let payload_len = u16::from_le_bytes(header[26..28].try_into().unwrap()) as usize;
        let total = align_up(WAL_RECORD_HEADER_SIZE + payload_len, 8);

        let mut buf = Vec::with_capacity(total);
        buf.extend_from_slice(&header);

        if total > WAL_RECORD_HEADER_SIZE {
            let mut rest = vec![0u8; total - WAL_RECORD_HEADER_SIZE];
            if !self.try_read_exact(&mut rest)? {
                // The record is torn: the header was written but the payload
                // could not be fully read. Treat this as the end of the WAL and
                // roll back the read position to the start of the torn record.
                self.current_lsn = start_lsn;
                return Ok(None);
            }
            buf.extend_from_slice(&rest);
        }

        let decoded = match WalRecord::decode(&buf) {
            Ok(ok) => ok,
            Err(e) => {
                // A CRC mismatch at the very END of the durable log is a torn
                // record, not corruption: a crash can interrupt the writer's
                // `write_all` mid-record (kill -9 keeps the page cache, so the
                // partially written prefix survives), and because segment
                // files are preallocated the unwritten remainder reads back
                // as zeros — complete enough to fail the CRC instead of the
                // short-read check above. `is_torn_tail` accepts this shape
                // only when the header pins the record to this exact position
                // AND nothing but zeros follows it, so genuine mid-file
                // corruption (valid records after the bad one) still
                // hard-fails. Roll back like any other torn record.
                let record_end = Lsn(start_lsn.0 + total as u64);
                if matches!(e, StorageError::WalCorrupted(_))
                    && self.is_torn_tail(start_lsn, record_end, &header)?
                {
                    self.current_lsn = start_lsn;
                    return Ok(None);
                }
                return Err(e);
            }
        };
        let (record, consumed) = decoded;
        debug_assert_eq!(consumed, total);
        // current_lsn was already advanced by try_read_exact to the byte
        // immediately following the record.
        Ok(Some(record))
    }

    /// Return an iterator over records starting from the current position.
    pub fn records(&mut self) -> WalRecordIter<'_> {
        WalRecordIter { reader: self }
    }

    // TODO(Phase 3): implement `tail_follow(start_lsn: Lsn)` returning an async
    // `Stream<Item = Result<WalRecord>>` for streaming WAL to replicas /
    // indexers. M1 only needs one-shot recovery reads.

    fn open_segment_file(wal_dir: &Path, segment_id: u64) -> Result<File> {
        let path = wal_dir.join(wal_filename(segment_id));
        OpenOptions::new()
            .read(true)
            .open(&path)
            .map_err(StorageError::Io)
    }

    /// Fill `buf` starting from `self.current_lsn`, automatically opening the
    /// next segment file when the current segment boundary is crossed.
    ///
    /// Returns `Ok(false)` when the WAL ends before `buf` can be completely
    /// filled (missing segment file or EOF). This is treated as a clean
    /// end-of-WAL, which recovery code will use to discard any torn/partial
    /// record. Returns an error only on I/O failures.
    fn try_read_exact(&mut self, buf: &mut [u8]) -> Result<bool> {
        let mut read = 0;
        while read < buf.len() {
            let pos = Lsn(self.current_lsn.0 + read as u64);
            let segment_id = pos.segment_id(self.segment_size);
            let offset = pos.segment_offset(self.segment_size);

            if segment_id != self.current_segment_id {
                let path = self.wal_dir.join(wal_filename(segment_id));
                if !path.exists() {
                    return Ok(false);
                }
                self.current_file = Self::open_segment_file(&self.wal_dir, segment_id)?;
                self.current_segment_id = segment_id;
            }

            let remaining_in_segment = (self.segment_size - offset) as usize;
            let chunk = std::cmp::min(remaining_in_segment, buf.len() - read);
            self.current_file
                .seek(SeekFrom::Start(offset))
                .map_err(StorageError::Io)?;
            let n = self
                .current_file
                .read(&mut buf[read..read + chunk])
                .map_err(StorageError::Io)?;
            // `read()` may return fewer than `chunk` bytes. The loop will retry
            // within the same segment; the seek at the top of each iteration
            // positions the fd cursor at the correct offset before the next
            // read. If this is ever refactored to use `pread()`, the retry
            // path must account for the new offset.
            if n == 0 {
                return Ok(false);
            }
            read += n;
        }

        self.current_lsn = Lsn(self.current_lsn.0 + buf.len() as u64);
        Ok(true)
    }

    /// Like [`Self::try_read_exact`] but reads from `pos` without advancing
    /// `self.current_lsn`. Used by the zero-hole forward probe — the reader
    /// peeks ahead while keeping its position at the start of the candidate
    /// hole so that `next_record` can roll back cleanly.
    fn try_read_exact_at(&mut self, pos: u64, buf: &mut [u8]) -> Result<bool> {
        let mut read = 0;
        while read < buf.len() {
            let p = Lsn(pos + read as u64);
            let segment_id = p.segment_id(self.segment_size);
            let offset = p.segment_offset(self.segment_size);

            if segment_id != self.current_segment_id {
                let path = self.wal_dir.join(wal_filename(segment_id));
                if !path.exists() {
                    return Ok(false);
                }
                self.current_file = Self::open_segment_file(&self.wal_dir, segment_id)?;
                self.current_segment_id = segment_id;
            }

            let remaining_in_segment = (self.segment_size - offset) as usize;
            let chunk = std::cmp::min(remaining_in_segment, buf.len() - read);
            self.current_file
                .seek(SeekFrom::Start(offset))
                .map_err(StorageError::Io)?;
            let n = self
                .current_file
                .read(&mut buf[read..read + chunk])
                .map_err(StorageError::Io)?;
            if n == 0 {
                return Ok(false);
            }
            read += n;
        }
        Ok(true)
    }

    /// Decide whether a CRC-failing record at `start_lsn` is a torn tail
    /// rather than genuine mid-file corruption.
    ///
    /// Two conditions must both hold:
    ///
    /// - `header` pins the record to this exact position: its self-reported
    ///   LSN equals `start_lsn`. A crash-torn write keeps the record's
    ///   leading bytes (record starts are 8-byte aligned and a short
    ///   `write()` transfers a prefix), so the first 8 bytes — the LSN field
    ///   — survive whenever the header is readable at all. A mid-file bit
    ///   flip, in contrast, lands anywhere and usually breaks this check.
    /// - Every byte from `record_end` (the record's end per its own length
    ///   prefix) to the end of the durable WAL is zero. A torn record is the
    ///   last thing the writer touched, so nothing may follow it; a single
    ///   non-zero byte afterwards means real records exist beyond the bad
    ///   one and the log is genuinely corrupt.
    ///
    /// Known limitation (Stage T review): the LSN field survives a torn
    /// write but the `payload_len` field may not — a bit flip that INFLATES
    /// `payload_len` pushes `record_end` past the real tail, so the zero
    /// scan starts inside valid records further back; if the flipped record
    /// sits within ~64KB of the durable end (u16 length), condition 2 sees
    /// only zeros and valid committed records after the corrupt one are
    /// silently truncated instead of hard-failing. Accepted because the CRC
    /// gate makes the odds negligible and PG's tail-CRC-failure-as-EOF has
    /// the same shape; the structural fix is a header self-CRC (PG's xl_crc
    /// pattern), deferred.
    ///
    /// Reads via the same segment-hopping logic as the sequential path but
    /// never advances `current_lsn`.
    fn is_torn_tail(&mut self, start_lsn: Lsn, record_end: Lsn, header: &[u8]) -> Result<bool> {
        let header_lsn = Lsn(u64::from_le_bytes(header[0..8].try_into().unwrap()));
        if header_lsn != start_lsn {
            return Ok(false);
        }

        let mut pos = record_end.0;
        let mut buf = [0u8; 64 * 1024];
        loop {
            let p = Lsn(pos);
            let segment_id = p.segment_id(self.segment_size);
            let offset = p.segment_offset(self.segment_size);

            if segment_id != self.current_segment_id {
                let path = self.wal_dir.join(wal_filename(segment_id));
                if !path.exists() {
                    // No further segments: the durable log ends here.
                    return Ok(true);
                }
                self.current_file = Self::open_segment_file(&self.wal_dir, segment_id)?;
                self.current_segment_id = segment_id;
            }

            let chunk = std::cmp::min((self.segment_size - offset) as usize, buf.len());
            self.current_file
                .seek(SeekFrom::Start(offset))
                .map_err(StorageError::Io)?;
            let n = self
                .current_file
                .read(&mut buf[..chunk])
                .map_err(StorageError::Io)?;
            if n == 0 {
                return Ok(true);
            }
            if buf[..n].iter().any(|&b| b != 0) {
                return Ok(false);
            }
            pos += n as u64;
        }
    }
}

/// Iterator over WAL records.
pub struct WalRecordIter<'a> {
    reader: &'a mut WalReader,
}

impl Iterator for WalRecordIter<'_> {
    type Item = Result<WalRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.reader.next_record() {
            Ok(None) => None,
            Ok(Some(record)) => Some(Ok(record)),
            Err(e) => Some(Err(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    use crate::config::StorageConfig;
    use crate::types::PageId;
    use crate::wal::record::WalRecordType;
    use crate::wal::writer::WalWriter;
    use tempfile::TempDir;

    fn writer_config(tmp: &TempDir) -> StorageConfig {
        let mut cfg = StorageConfig::new(tmp.path());
        cfg.wal_group_commit_timeout_ms = 1;
        cfg.wal_group_commit_batch_size = 1;
        cfg.wal_segment_size = 1024;
        cfg
    }

    #[test]
    fn read_single_record() {
        let tmp = TempDir::new().unwrap();
        let cfg = writer_config(&tmp);
        let writer = WalWriter::open(tmp.path(), &cfg).unwrap();
        let lsn = writer
            .append(WalRecord::page_alloc(PageId(42)).unwrap())
            .unwrap();

        let mut reader = WalReader::open(tmp.path().join("wal"), cfg.wal_segment_size).unwrap();
        let record = reader.next_record().unwrap().unwrap();
        assert_eq!(record.record_type, WalRecordType::PageAlloc);
        assert_eq!(record.lsn, lsn);
        assert!(reader.next_record().unwrap().is_none());
    }

    #[test]
    fn read_multiple_records_preserves_order() {
        let tmp = TempDir::new().unwrap();
        let cfg = writer_config(&tmp);
        let writer = WalWriter::open(tmp.path(), &cfg).unwrap();

        let count: usize = 100;
        let mut lsns = Vec::new();
        for i in 0..count {
            let lsn = writer
                .append(WalRecord::page_alloc(PageId(i as u64 + 1)).unwrap())
                .unwrap();
            lsns.push(lsn);
        }

        let mut reader = WalReader::open(tmp.path().join("wal"), cfg.wal_segment_size).unwrap();
        let mut read = 0;
        while let Some(record) = reader.next_record().unwrap() {
            assert_eq!(record.lsn, lsns[read]);
            read += 1;
        }
        assert_eq!(read, count);
    }

    #[test]
    fn read_across_segment_boundary() {
        let tmp = TempDir::new().unwrap();
        let mut cfg = writer_config(&tmp);
        cfg.wal_segment_size = 256;
        let writer = WalWriter::open(tmp.path(), &cfg).unwrap();

        // Write enough records to cross into a second segment.
        let n: usize = 20;
        for i in 0..n {
            writer
                .append(WalRecord::page_alloc(PageId(i as u64 + 1)).unwrap())
                .unwrap();
        }

        let mut reader = WalReader::open(tmp.path().join("wal"), cfg.wal_segment_size).unwrap();
        let mut count = 0;
        while reader.next_record().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, n);
    }

    #[test]
    fn read_from_start_lsn() {
        let tmp = TempDir::new().unwrap();
        let cfg = writer_config(&tmp);
        let writer = WalWriter::open(tmp.path(), &cfg).unwrap();

        let lsn1 = writer
            .append(WalRecord::page_alloc(PageId(1)).unwrap())
            .unwrap();
        writer
            .append(WalRecord::page_alloc(PageId(2)).unwrap())
            .unwrap();

        let mut reader =
            WalReader::open_at(tmp.path().join("wal"), cfg.wal_segment_size, lsn1).unwrap();
        let record = reader.next_record().unwrap().unwrap();
        assert_eq!(record.lsn, lsn1);
        assert!(reader.next_record().unwrap().is_some());
        assert!(reader.next_record().unwrap().is_none());
    }

    #[test]
    fn read_rejects_corrupted_crc() {
        let tmp = TempDir::new().unwrap();
        let cfg = writer_config(&tmp);
        let writer = WalWriter::open(tmp.path(), &cfg).unwrap();
        let lsn = writer
            .append(WalRecord::page_alloc(PageId(1)).unwrap())
            .unwrap();

        // Corrupt the first byte of the record (part of the LSN).
        let path = tmp.path().join("wal").join("wal-00000001.log");
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let offset = lsn.segment_offset(cfg.wal_segment_size);
        let mut buf = [0u8; 1];
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.read_exact(&mut buf).unwrap();
        buf[0] ^= 0xff;
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.write_all(&buf).unwrap();
        drop(file);

        let mut reader = WalReader::open(tmp.path().join("wal"), cfg.wal_segment_size).unwrap();
        assert!(reader.next_record().is_err());
    }

    #[test]
    fn read_empty_wal_returns_none() {
        let tmp = TempDir::new().unwrap();
        let cfg = writer_config(&tmp);
        // Create the WAL directory and an empty segment file.
        let wal_dir = tmp.path().join("wal");
        std::fs::create_dir(&wal_dir).unwrap();
        File::create(wal_dir.join("wal-00000001.log")).unwrap();

        let mut reader = WalReader::open(&wal_dir, cfg.wal_segment_size).unwrap();
        assert!(reader.next_record().unwrap().is_none());
    }

    #[test]
    fn read_torn_payload_returns_none() {
        let tmp = TempDir::new().unwrap();
        let cfg = writer_config(&tmp);
        let writer = WalWriter::open(tmp.path(), &cfg).unwrap();
        let lsn = writer
            .append(WalRecord::full_page_image(PageId(1), vec![0xAB; 256]).unwrap())
            .unwrap();
        drop(writer);

        // Truncate the file so that the header is intact but the payload is
        // only partially present. This simulates a crash after the header was
        // written but before the full record was fsynced.
        let path = tmp.path().join("wal").join("wal-00000001.log");
        let record_start = lsn.segment_offset(cfg.wal_segment_size);
        let truncate_len = record_start + WAL_RECORD_HEADER_SIZE as u64 + 4;
        let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(truncate_len).unwrap();
        drop(file);

        let mut reader = WalReader::open(tmp.path().join("wal"), cfg.wal_segment_size).unwrap();
        assert!(reader.next_record().unwrap().is_none());
        // The torn record should be discarded; current_lsn rolls back to the
        // start of the torn record.
        assert_eq!(reader.current_lsn(), lsn);
    }

    #[test]
    fn read_torn_header_returns_none() {
        let tmp = TempDir::new().unwrap();
        let cfg = writer_config(&tmp);
        let writer = WalWriter::open(tmp.path(), &cfg).unwrap();
        let lsn = writer
            .append(WalRecord::page_alloc(PageId(1)).unwrap())
            .unwrap();
        drop(writer);

        // Truncate the file mid-header.
        let path = tmp.path().join("wal").join("wal-00000001.log");
        let record_start = lsn.segment_offset(cfg.wal_segment_size);
        let truncate_len = record_start + (WAL_RECORD_HEADER_SIZE as u64) / 2;
        let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(truncate_len).unwrap();
        drop(file);

        let mut reader = WalReader::open(tmp.path().join("wal"), cfg.wal_segment_size).unwrap();
        assert!(reader.next_record().unwrap().is_none());
        assert_eq!(reader.current_lsn(), lsn);
    }

    #[test]
    fn read_iterator_matches_next_record() {
        let tmp = TempDir::new().unwrap();
        let cfg = writer_config(&tmp);
        let writer = WalWriter::open(tmp.path(), &cfg).unwrap();

        for i in 0..5 {
            writer
                .append(WalRecord::page_alloc(PageId(i + 1)).unwrap())
                .unwrap();
        }

        let mut reader = WalReader::open(tmp.path().join("wal"), cfg.wal_segment_size).unwrap();
        let from_iter: Vec<_> = reader.records().map(|r| r.unwrap().record_type).collect();
        assert_eq!(from_iter, vec![WalRecordType::PageAlloc; 5]);
    }

    #[test]
    fn read_torn_tail_record_with_zero_remainder_returns_none() {
        // Simulates a kill -9 landing mid-`write_all`: the last record's
        // leading bytes (full header + a payload prefix) are durable, and
        // because the segment file is preallocated the unwritten remainder
        // reads back as zeros. The CRC therefore fails on a complete-looking
        // record — but this is a torn tail and must be a clean end-of-WAL,
        // not a hard corruption error (Stage T crash-test failure).
        let tmp = TempDir::new().unwrap();
        let cfg = writer_config(&tmp);
        let writer = WalWriter::open(tmp.path(), &cfg).unwrap();
        let lsn1 = writer
            .append(WalRecord::page_alloc(PageId(1)).unwrap())
            .unwrap();
        let fpi_lsn = writer
            .append(WalRecord::full_page_image(PageId(2), vec![0xAB; 256]).unwrap())
            .unwrap();
        writer.flush_to(fpi_lsn).unwrap();
        drop(writer);

        // Zero everything from mid-FPI-payload onward: the torn tail shape.
        let path = tmp.path().join("wal").join("wal-00000001.log");
        let mut file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        let torn_from =
            fpi_lsn.segment_offset(cfg.wal_segment_size) + WAL_RECORD_HEADER_SIZE as u64 + 16;
        let file_len = file.metadata().unwrap().len();
        assert!(torn_from < file_len);
        file.seek(SeekFrom::Start(torn_from)).unwrap();
        file.write_all(&vec![0u8; (file_len - torn_from) as usize])
            .unwrap();
        drop(file);

        let mut reader = WalReader::open(tmp.path().join("wal"), cfg.wal_segment_size).unwrap();
        let first = reader.next_record().unwrap().unwrap();
        assert_eq!(first.lsn, lsn1);
        // The torn FPI is discarded as a clean end-of-WAL, rolling the read
        // position back to its start.
        assert!(reader.next_record().unwrap().is_none());
        assert_eq!(reader.current_lsn(), fpi_lsn);
    }

    #[test]
    fn read_crc_failure_with_records_after_still_errors() {
        // A CRC-failing record that is NOT at the tail — valid records follow
        // it — is genuine mid-file corruption and must remain a hard error.
        let tmp = TempDir::new().unwrap();
        let cfg = writer_config(&tmp);
        let writer = WalWriter::open(tmp.path(), &cfg).unwrap();
        writer
            .append(WalRecord::page_alloc(PageId(1)).unwrap())
            .unwrap();
        let mid_lsn = writer
            .append(WalRecord::page_alloc(PageId(2)).unwrap())
            .unwrap();
        writer
            .append(WalRecord::page_alloc(PageId(3)).unwrap())
            .unwrap();
        writer.flush().unwrap();
        drop(writer);

        // Flip one padding byte of the middle record (header stays intact).
        let path = tmp.path().join("wal").join("wal-00000001.log");
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let offset = mid_lsn.segment_offset(cfg.wal_segment_size) + WAL_RECORD_HEADER_SIZE as u64;
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.write_all(&[0xFF]).unwrap();
        drop(file);

        let mut reader = WalReader::open(tmp.path().join("wal"), cfg.wal_segment_size).unwrap();
        assert!(reader.next_record().unwrap().is_some());
        assert!(reader.next_record().is_err());
    }
}
