//! CRC-framed metadata codecs used by manifest and segment files.

use serde::{Deserialize, Serialize};

use crate::error::{InvertedError, Result};

pub(crate) const MAGIC: &[u8; 4] = b"PGIV";
pub(crate) const FORMAT_VERSION: u16 = 1;
pub(crate) const HEADER_SIZE: usize = 10;
pub(crate) const TRAILER_SIZE: usize = 4;
pub(crate) const MAX_METADATA_BYTES: usize = 16 * 1024 * 1024;

/// Serialize a value in the versioned metadata frame and append a CRC32
/// trailer. The payload length is bounded before it can reach a decoder.
pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let payload = bincode::serde::encode_to_vec(value, bincode::config::standard())
        .map_err(|error| InvertedError::InvalidArgument(error.to_string()))?;
    if payload.len() > MAX_METADATA_BYTES {
        return Err(InvertedError::Capacity(format!(
            "metadata payload exceeds {MAX_METADATA_BYTES} bytes"
        )));
    }
    let payload_len = u32::try_from(payload.len())
        .map_err(|_| InvertedError::Capacity("metadata payload exceeds u32".to_string()))?;
    let crc = crc32fast::hash(&payload);
    let mut bytes = Vec::with_capacity(HEADER_SIZE + payload.len() + TRAILER_SIZE);
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    bytes.extend_from_slice(&payload_len.to_le_bytes());
    bytes.extend_from_slice(&payload);
    bytes.extend_from_slice(&crc.to_le_bytes());
    Ok(bytes)
}

/// Decode a value, checking magic, format version, declared and actual
/// lengths, the metadata bound, its CRC32 trailer, and bincode's nested
/// read/allocation limit.
pub fn decode<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T> {
    if bytes.len() < HEADER_SIZE + TRAILER_SIZE {
        return Err(InvertedError::Corrupted(format!(
            "metadata is truncated: {} bytes",
            bytes.len()
        )));
    }
    if &bytes[..4] != MAGIC {
        return Err(InvertedError::Corrupted(
            "metadata magic mismatch".to_string(),
        ));
    }
    let version = u16::from_le_bytes(bytes[4..6].try_into().expect("two-byte version"));
    if version != FORMAT_VERSION {
        return Err(InvertedError::Corrupted(format!(
            "unsupported metadata version {version}"
        )));
    }
    let declared = u32::from_le_bytes(bytes[6..10].try_into().expect("four-byte length")) as usize;
    if declared > MAX_METADATA_BYTES {
        return Err(InvertedError::Corrupted(format!(
            "metadata payload length {declared} exceeds {MAX_METADATA_BYTES}"
        )));
    }
    let expected_len = HEADER_SIZE + declared + TRAILER_SIZE;
    if bytes.len() != expected_len {
        return Err(InvertedError::Corrupted(format!(
            "metadata length mismatch: header declares {expected_len} bytes, got {}",
            bytes.len()
        )));
    }
    let payload = &bytes[HEADER_SIZE..HEADER_SIZE + declared];
    let trailer = &bytes[HEADER_SIZE + declared..];
    let expected_crc = u32::from_le_bytes(trailer.try_into().expect("four-byte CRC trailer"));
    if crc32fast::hash(payload) != expected_crc {
        return Err(InvertedError::Corrupted(
            "metadata CRC32 mismatch".to_string(),
        ));
    }
    let (value, consumed) = bincode::serde::decode_from_slice(
        payload,
        bincode::config::standard().with_limit::<{ MAX_METADATA_BYTES }>(),
    )
    .map_err(|error| InvertedError::Corrupted(error.to_string()))?;
    if consumed != payload.len() {
        return Err(InvertedError::Corrupted(format!(
            "metadata has {} trailing bytes",
            payload.len() - consumed
        )));
    }
    Ok(value)
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
