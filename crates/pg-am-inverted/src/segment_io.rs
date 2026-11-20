//! Bounded-memory streaming codecs for segment posting maps.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Read, Seek, Write};
use std::path::{Path, PathBuf};

use pg_storage::types::Tid;

use crate::error::{InvertedError, Result};
use crate::posting::Posting;
use crate::recovery::{FORMAT_VERSION, HEADER_SIZE, MAGIC, MAX_METADATA_BYTES, TRAILER_SIZE};

/// One posting-map term and its sorted document entries. Bincode encodes a
/// sequence of key/value pairs identically to the equivalent `BTreeMap`.
pub(crate) type TermEntries = (String, Vec<(Tid, Posting)>);

/// A posting reader that retains only the current term's posting list.
pub(crate) struct PostingsReader {
    payload: std::io::Take<File>,
    remaining_terms: u64,
    previous_term: Option<String>,
}

impl PostingsReader {
    /// Validate the complete frame and open a streaming cursor over its terms.
    pub(crate) fn open(path: &Path) -> Result<Self> {
        validate_frame(path)?;
        let mut file = File::open(path).map_err(|error| storage(path, error))?;
        file.seek(std::io::SeekFrom::Start(HEADER_SIZE as u64))
            .map_err(|error| storage(path, error))?;
        let payload_len = file.metadata().map_err(|error| storage(path, error))?.len() as usize
            - HEADER_SIZE
            - TRAILER_SIZE;
        let mut payload = file.take(payload_len as u64);
        let remaining_terms = decode::<u64>(path, &mut payload)?;
        if remaining_terms > payload_len as u64 {
            return Err(InvertedError::Corrupted(format!(
                "posting term count {remaining_terms} exceeds payload size {payload_len}"
            )));
        }
        Ok(Self {
            payload,
            remaining_terms,
            previous_term: None,
        })
    }

    /// Decode the next term without materializing any later posting lists.
    pub(crate) fn next_term(&mut self) -> Result<Option<TermEntries>> {
        if self.remaining_terms == 0 {
            if self.payload.limit() != 0 {
                return Err(InvertedError::Corrupted(
                    "posting payload has trailing bytes".to_string(),
                ));
            }
            return Ok(None);
        }
        let path = Path::new("postings payload");
        let (term, documents) = decode::<TermEntries>(path, &mut self.payload)?;
        self.remaining_terms -= 1;
        if self
            .previous_term
            .as_ref()
            .is_some_and(|previous| *previous >= term)
        {
            return Err(InvertedError::Corrupted(
                "posting terms are not strictly increasing".to_string(),
            ));
        }
        if term.is_empty() || documents.is_empty() {
            return Err(InvertedError::Corrupted(
                "posting map contains an empty term or posting list".to_string(),
            ));
        }
        let mut previous_tid = None;
        for (tid, posting) in &documents {
            if previous_tid.is_some_and(|previous| previous >= *tid) {
                return Err(InvertedError::Corrupted(format!(
                    "posting TIDs for {term:?} are not strictly increasing"
                )));
            }
            if *tid != posting.tid {
                return Err(InvertedError::Corrupted(format!(
                    "posting map key {tid} disagrees with posting TID {:?}",
                    posting.tid
                )));
            }
            posting.validate()?;
            previous_tid = Some(*tid);
        }
        self.previous_term = Some(term.clone());
        Ok(Some((term, documents)))
    }
}

/// A streaming posting writer. Entries are spooled once, then framed and
/// atomically renamed without constructing the complete encoded map in memory.
pub(crate) struct PostingsWriter {
    final_path: PathBuf,
    temporary_path: PathBuf,
    spool_path: PathBuf,
    spool: File,
    term_count: u64,
    spool_len: usize,
    previous_term: Option<String>,
    published: bool,
}

impl PostingsWriter {
    /// Create an unpublished posting output and its temporary spool.
    pub(crate) fn create(path: &Path) -> Result<Self> {
        let parent = path.parent().ok_or_else(|| {
            InvertedError::InvalidArgument(format!("{} has no parent directory", path.display()))
        })?;
        std::fs::create_dir_all(parent).map_err(|error| storage(parent, error))?;
        let temporary_path = path.with_extension("tmp");
        let spool_path = path.with_extension("spool");
        let spool = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&spool_path)
            .map_err(|error| storage(&spool_path, error))?;
        Ok(Self {
            final_path: path.to_path_buf(),
            temporary_path,
            spool_path,
            spool,
            term_count: 0,
            spool_len: 0,
            previous_term: None,
            published: false,
        })
    }

    /// Append one sorted, non-empty term and its posting list.
    pub(crate) fn write_term(
        &mut self,
        term: &str,
        documents: &BTreeMap<Tid, Posting>,
    ) -> Result<()> {
        if term.is_empty() || documents.is_empty() {
            return Err(InvertedError::InvalidArgument(
                "cannot write an empty term or posting list".to_string(),
            ));
        }
        if self
            .previous_term
            .as_deref()
            .is_some_and(|previous| previous >= term)
        {
            return Err(InvertedError::InvalidArgument(
                "posting terms must be strictly increasing".to_string(),
            ));
        }
        let bytes = bincode::serde::encode_to_vec((term, documents), bincode::config::standard())
            .map_err(|error| InvertedError::InvalidArgument(error.to_string()))?;
        self.spool_len = self.spool_len.checked_add(bytes.len()).ok_or_else(|| {
            InvertedError::Capacity("posting payload length overflows usize".to_string())
        })?;
        if self.spool_len > MAX_METADATA_BYTES {
            return Err(InvertedError::Capacity(format!(
                "metadata payload exceeds {MAX_METADATA_BYTES} bytes"
            )));
        }
        self.spool
            .write_all(&bytes)
            .map_err(|error| storage(&self.spool_path, error))?;
        self.term_count = self.term_count.checked_add(1).ok_or_else(|| {
            InvertedError::InvalidArgument("posting term count overflows u64".to_string())
        })?;
        self.previous_term = Some(term.to_string());
        Ok(())
    }

    /// Finish the canonical frame, fsync it, and atomically publish the target.
    pub(crate) fn finish(mut self) -> Result<()> {
        self.spool
            .flush()
            .map_err(|error| storage(&self.spool_path, error))?;
        self.spool
            .sync_all()
            .map_err(|error| storage(&self.spool_path, error))?;
        let count_bytes =
            bincode::serde::encode_to_vec(self.term_count, bincode::config::standard())
                .map_err(|error| InvertedError::InvalidArgument(error.to_string()))?;
        let payload_len = count_bytes
            .len()
            .checked_add(self.spool_len)
            .ok_or_else(|| {
                InvertedError::Capacity("posting payload length overflows usize".to_string())
            })?;
        if payload_len > MAX_METADATA_BYTES {
            return Err(InvertedError::Capacity(format!(
                "metadata payload exceeds {MAX_METADATA_BYTES} bytes"
            )));
        }
        let payload_len = u32::try_from(payload_len)
            .map_err(|_| InvertedError::Capacity("metadata payload exceeds u32".to_string()))?;
        let mut output = File::create(&self.temporary_path)
            .map_err(|error| storage(&self.temporary_path, error))?;
        output
            .write_all(MAGIC)
            .and_then(|_| output.write_all(&FORMAT_VERSION.to_le_bytes()))
            .and_then(|_| output.write_all(&payload_len.to_le_bytes()))
            .map_err(|error| storage(&self.temporary_path, error))?;
        output
            .write_all(&count_bytes)
            .map_err(|error| storage(&self.temporary_path, error))?;
        let mut crc = crc32fast::Hasher::new();
        crc.update(&count_bytes);
        self.spool
            .seek(std::io::SeekFrom::Start(0))
            .map_err(|error| storage(&self.spool_path, error))?;
        let mut buffer = [0u8; 8192];
        loop {
            let count = self
                .spool
                .read(&mut buffer)
                .map_err(|error| storage(&self.spool_path, error))?;
            if count == 0 {
                break;
            }
            output
                .write_all(&buffer[..count])
                .map_err(|error| storage(&self.temporary_path, error))?;
            crc.update(&buffer[..count]);
        }
        output
            .write_all(&crc.finalize().to_le_bytes())
            .map_err(|error| storage(&self.temporary_path, error))?;
        output
            .sync_all()
            .map_err(|error| storage(&self.temporary_path, error))?;
        std::fs::rename(&self.temporary_path, &self.final_path)
            .map_err(|error| storage(&self.final_path, error))?;
        let parent = self.final_path.parent().ok_or_else(|| {
            InvertedError::InvalidArgument(format!(
                "{} has no parent directory",
                self.final_path.display()
            ))
        })?;
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| storage(parent, error))?;
        std::fs::remove_file(&self.spool_path).map_err(|error| storage(&self.spool_path, error))?;
        self.published = true;
        Ok(())
    }
}

impl Drop for PostingsWriter {
    fn drop(&mut self) {
        if !self.published {
            let _ = std::fs::remove_file(&self.temporary_path);
            let _ = std::fs::remove_file(&self.spool_path);
        }
    }
}

fn validate_frame(path: &Path) -> Result<()> {
    let mut file = File::open(path).map_err(|error| storage(path, error))?;
    let file_len = file.metadata().map_err(|error| storage(path, error))?.len() as usize;
    if file_len < HEADER_SIZE + TRAILER_SIZE {
        return Err(InvertedError::Corrupted(format!(
            "metadata is truncated: {file_len} bytes"
        )));
    }
    let mut header = [0u8; HEADER_SIZE];
    file.read_exact(&mut header)
        .map_err(|error| storage(path, error))?;
    if &header[..4] != MAGIC {
        return Err(InvertedError::Corrupted(
            "metadata magic mismatch".to_string(),
        ));
    }
    let version = u16::from_le_bytes([header[4], header[5]]);
    if version != FORMAT_VERSION {
        return Err(InvertedError::Corrupted(format!(
            "unsupported metadata version {version}"
        )));
    }
    let declared = u32::from_le_bytes([header[6], header[7], header[8], header[9]]) as usize;
    if declared > MAX_METADATA_BYTES {
        return Err(InvertedError::Corrupted(format!(
            "metadata payload length {declared} exceeds {MAX_METADATA_BYTES}"
        )));
    }
    if file_len != HEADER_SIZE + declared + TRAILER_SIZE {
        return Err(InvertedError::Corrupted(format!(
            "metadata length mismatch: header declares {} bytes, got {file_len}",
            HEADER_SIZE + declared + TRAILER_SIZE
        )));
    }
    let mut crc = crc32fast::Hasher::new();
    let mut remaining = declared;
    let mut buffer = [0u8; 8192];
    while remaining > 0 {
        let read_len = remaining.min(buffer.len());
        let count = file
            .read(&mut buffer[..read_len])
            .map_err(|error| storage(path, error))?;
        if count == 0 {
            return Err(InvertedError::Corrupted(
                "posting payload is truncated".to_string(),
            ));
        }
        crc.update(&buffer[..count]);
        remaining -= count;
    }
    let mut trailer = [0u8; TRAILER_SIZE];
    file.read_exact(&mut trailer)
        .map_err(|error| storage(path, error))?;
    if crc.finalize() != u32::from_le_bytes(trailer) {
        return Err(InvertedError::Corrupted(
            "metadata CRC32 mismatch".to_string(),
        ));
    }
    Ok(())
}

fn decode<T: for<'de> serde::Deserialize<'de>>(path: &Path, reader: &mut impl Read) -> Result<T> {
    bincode::serde::decode_from_std_read(
        reader,
        bincode::config::standard().with_limit::<MAX_METADATA_BYTES>(),
    )
    .map_err(|error| InvertedError::Corrupted(format!("cannot decode {}: {error}", path.display())))
}

fn storage(path: &Path, error: std::io::Error) -> InvertedError {
    InvertedError::Storage(format!("cannot access {}: {error}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use pg_storage::types::PageId;

    #[test]
    fn streaming_posting_frame_matches_frozen_map_encoding() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("1.postings");
        let tid = Tid {
            page_id: PageId(1),
            slot_id: 1,
        };
        let postings = BTreeMap::from([(
            "rust".to_string(),
            BTreeMap::from([(
                tid,
                Posting {
                    tid,
                    term_freq: 1,
                    positions: vec![0],
                },
            )]),
        )]);
        let mut writer = PostingsWriter::create(&path).unwrap();
        for (term, documents) in &postings {
            writer.write_term(term, documents).unwrap();
        }
        writer.finish().unwrap();
        assert_eq!(
            std::fs::read(&path).unwrap(),
            crate::recovery::encode(&postings).unwrap()
        );
        let mut reader = PostingsReader::open(&path).unwrap();
        let (term, documents) = reader.next_term().unwrap().unwrap();
        assert_eq!(term, "rust");
        assert_eq!(
            documents.into_iter().collect::<BTreeMap<_, _>>(),
            postings["rust"]
        );
        assert!(reader.next_term().unwrap().is_none());
    }

    #[test]
    fn streaming_reader_rejects_unsorted_terms_and_duplicate_tids() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("bad.postings");
        let first = Tid {
            page_id: PageId(1),
            slot_id: 1,
        };
        let second = Tid {
            page_id: PageId(1),
            slot_id: 2,
        };
        let posting = |tid| Posting {
            tid,
            term_freq: 1,
            positions: vec![0],
        };
        let unsorted = vec![
            ("z".to_string(), vec![(first, posting(first))]),
            ("a".to_string(), vec![(second, posting(second))]),
        ];
        std::fs::write(&path, crate::recovery::encode(&unsorted).unwrap()).unwrap();
        let mut reader = PostingsReader::open(&path).unwrap();
        assert!(reader.next_term().unwrap().is_some());
        assert!(matches!(
            reader.next_term(),
            Err(InvertedError::Corrupted(_))
        ));

        let duplicates = vec![(
            "rust".to_string(),
            vec![(first, posting(first)), (first, posting(first))],
        )];
        std::fs::write(&path, crate::recovery::encode(&duplicates).unwrap()).unwrap();
        let mut reader = PostingsReader::open(&path).unwrap();
        assert!(matches!(
            reader.next_term(),
            Err(InvertedError::Corrupted(_))
        ));
    }

    #[test]
    fn streaming_writer_rejects_invalid_order_and_cleans_temporary_files() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("1.postings");
        let tid = Tid {
            page_id: PageId(1),
            slot_id: 1,
        };
        let documents = BTreeMap::from([(
            tid,
            Posting {
                tid,
                term_freq: 1,
                positions: vec![0],
            },
        )]);
        let mut writer = PostingsWriter::create(&path).unwrap();
        writer.write_term("z", &documents).unwrap();
        assert!(matches!(
            writer.write_term("a", &documents),
            Err(InvertedError::InvalidArgument(_))
        ));
        drop(writer);
        assert!(!path.with_extension("tmp").exists());
        assert!(!path.with_extension("spool").exists());
        assert!(!path.exists());
    }
}
