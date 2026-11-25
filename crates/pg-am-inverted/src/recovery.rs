//! CRC-framed metadata codecs used by manifest and segment files.

use pg_storage::metadata::MetadataFrame;
use serde::{Deserialize, Serialize};

use crate::error::{InvertedError, Result};

pub(crate) const MAGIC: &[u8; 4] = b"PGIV";
pub(crate) const FORMAT_VERSION: u16 = 1;
pub(crate) const HEADER_SIZE: usize = 10;
pub(crate) const TRAILER_SIZE: usize = 4;
pub(crate) const MAX_METADATA_BYTES: usize = 16 * 1024 * 1024;
const FRAME: MetadataFrame<MAX_METADATA_BYTES> = MetadataFrame::new(*MAGIC, FORMAT_VERSION);

/// Serialize a value in the versioned metadata frame and append a CRC32
/// trailer. The payload length is bounded before it can reach a decoder.
pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    FRAME.encode(value).map_err(|error| {
        if error.contains("exceeds") {
            InvertedError::Capacity(error)
        } else {
            InvertedError::InvalidArgument(error)
        }
    })
}

/// Decode a value, checking magic, format version, declared and actual
/// lengths, the metadata bound, its CRC32 trailer, and bincode's nested
/// read/allocation limit.
pub fn decode<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T> {
    FRAME.decode(bytes).map_err(InvertedError::Corrupted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SegmentManifest;
    use pg_storage::types::Lsn;

    fn manifest_bytes() -> Vec<u8> {
        SegmentManifest {
            generation: 1,
            active_segment_ids: Vec::new(),
            applied_lsn: Lsn::INVALID,
        }
        .encode()
        .unwrap()
    }

    #[test]
    fn rejects_bad_magic_version_lengths_and_crc() {
        let valid = manifest_bytes();
        for bytes in [Vec::new(), valid[..HEADER_SIZE + TRAILER_SIZE - 1].to_vec()] {
            assert!(matches!(
                SegmentManifest::decode(&bytes),
                Err(InvertedError::Corrupted(_))
            ));
        }

        let mut bad_magic = valid.clone();
        bad_magic[0] ^= 1;
        assert!(matches!(
            SegmentManifest::decode(&bad_magic),
            Err(InvertedError::Corrupted(_))
        ));

        let mut bad_version = valid.clone();
        bad_version[4..6].copy_from_slice(&2u16.to_le_bytes());
        assert!(matches!(
            SegmentManifest::decode(&bad_version),
            Err(InvertedError::Corrupted(_))
        ));

        let mut bad_length = valid.clone();
        bad_length[6..10].copy_from_slice(&0u32.to_le_bytes());
        assert!(matches!(
            SegmentManifest::decode(&bad_length),
            Err(InvertedError::Corrupted(_))
        ));

        let mut excessive_length = valid.clone();
        excessive_length[6..10].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(matches!(
            SegmentManifest::decode(&excessive_length),
            Err(InvertedError::Corrupted(_))
        ));

        let mut bad_crc = valid;
        bad_crc[HEADER_SIZE] ^= 1;
        assert!(matches!(
            SegmentManifest::decode(&bad_crc),
            Err(InvertedError::Corrupted(_))
        ));
    }

    fn framed(payload: Vec<u8>) -> Vec<u8> {
        let mut bytes = MAGIC.to_vec();
        bytes.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        bytes.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&payload);
        bytes.extend_from_slice(&crc32fast::hash(&payload).to_le_bytes());
        bytes
    }

    #[test]
    fn malicious_inner_lengths_are_bounded_without_panicking() {
        let mut vec_payload = vec![1, 253];
        vec_payload.extend_from_slice(&u64::MAX.to_le_bytes());
        vec_payload.push(0);
        let vec_bytes = framed(vec_payload);
        let result = std::panic::catch_unwind(|| SegmentManifest::decode(&vec_bytes));
        assert!(result.is_ok(), "decoder panicked on an inner Vec length");
        assert!(matches!(result.unwrap(), Err(InvertedError::Corrupted(_))));

        let mut string_payload = vec![253];
        string_payload.extend_from_slice(&u64::MAX.to_le_bytes());
        let string_bytes = framed(string_payload);
        let result = std::panic::catch_unwind(|| {
            crate::recovery::decode::<crate::AnalyzerConfig>(&string_bytes)
        });
        assert!(result.is_ok(), "decoder panicked on an inner String length");
        assert!(matches!(result.unwrap(), Err(InvertedError::Corrupted(_))));
    }

    #[test]
    fn oversized_frames_are_capacity_errors() {
        let error = encode(&vec![0u8; MAX_METADATA_BYTES + 1]).unwrap_err();
        assert!(matches!(error, InvertedError::Capacity(_)));
    }
}
