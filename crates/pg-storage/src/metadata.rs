//! Shared bounded version+CRC metadata framing used by AM side files.

use serde::{Deserialize, Serialize};

/// A versioned metadata frame with a four-byte magic, little-endian length,
/// and CRC32 trailer. `MAX` bounds both the declared payload and nested
/// bincode read/allocation budgets.
#[derive(Debug, Clone, Copy)]
pub struct MetadataFrame<const MAX: usize> {
    magic: [u8; 4],
    version: u16,
}

impl<const MAX: usize> MetadataFrame<MAX> {
    /// Freeze a frame identity and format version.
    pub const fn new(magic: [u8; 4], version: u16) -> Self {
        Self { magic, version }
    }

    /// Serialize a value with bounded payload length and a CRC32 trailer.
    pub fn encode<T: Serialize>(&self, value: &T) -> Result<Vec<u8>, String> {
        let payload = bincode::serde::encode_to_vec(value, bincode::config::standard())
            .map_err(|error| error.to_string())?;
        if payload.len() > MAX {
            return Err(format!("metadata payload exceeds {MAX} bytes"));
        }
        let payload_len =
            u32::try_from(payload.len()).map_err(|_| "metadata payload exceeds u32".to_string())?;
        let crc = crc32fast::hash(&payload);
        let mut bytes = Vec::with_capacity(10 + payload.len() + 4);
        bytes.extend_from_slice(&self.magic);
        bytes.extend_from_slice(&self.version.to_le_bytes());
        bytes.extend_from_slice(&payload_len.to_le_bytes());
        bytes.extend_from_slice(&payload);
        bytes.extend_from_slice(&crc.to_le_bytes());
        Ok(bytes)
    }

    /// Decode a frame after checking magic, version, exact length, CRC32,
    /// nested decoder limits, and trailing payload bytes.
    pub fn decode<T: for<'de> Deserialize<'de>>(&self, bytes: &[u8]) -> Result<T, String> {
        if bytes.len() < 14 {
            return Err(format!("metadata is truncated: {} bytes", bytes.len()));
        }
        if bytes[..4] != self.magic {
            return Err("metadata magic mismatch".to_string());
        }
        let version = u16::from_le_bytes(bytes[4..6].try_into().expect("two-byte version"));
        if version != self.version {
            return Err(format!("unsupported metadata version {version}"));
        }
        let declared =
            u32::from_le_bytes(bytes[6..10].try_into().expect("four-byte length")) as usize;
        if declared > MAX {
            return Err(format!("metadata payload length {declared} exceeds {MAX}"));
        }
        let expected_len = 10 + declared + 4;
        if bytes.len() != expected_len {
            return Err(format!(
                "metadata length mismatch: expected {expected_len}, got {}",
                bytes.len()
            ));
        }
        let payload = &bytes[10..10 + declared];
        let expected_crc = u32::from_le_bytes(
            bytes[10 + declared..]
                .try_into()
                .expect("four-byte CRC trailer"),
        );
        if crc32fast::hash(payload) != expected_crc {
            return Err("metadata CRC32 mismatch".to_string());
        }
        let (value, consumed) = bincode::serde::decode_from_slice(
            payload,
            bincode::config::standard().with_limit::<MAX>(),
        )
        .map_err(|error| error.to_string())?;
        if consumed != payload.len() {
            return Err(format!(
                "metadata has {} trailing bytes",
                payload.len() - consumed
            ));
        }
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FRAME: MetadataFrame<1024> = MetadataFrame::new(*b"TEST", 1);

    #[test]
    fn frame_roundtrip_and_negative_matrix() {
        let value = vec![1u32, 2, 3];
        let bytes = FRAME.encode(&value).unwrap();
        assert_eq!(FRAME.decode::<Vec<u32>>(&bytes).unwrap(), value);

        let mut bad_magic = bytes.clone();
        bad_magic[0] ^= 1;
        assert!(FRAME.decode::<Vec<u32>>(&bad_magic).is_err());
        let mut bad_version = bytes.clone();
        bad_version[4..6].copy_from_slice(&2u16.to_le_bytes());
        assert!(FRAME.decode::<Vec<u32>>(&bad_version).is_err());
        let mut bad_length = bytes.clone();
        bad_length[6..10].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(FRAME.decode::<Vec<u32>>(&bad_length).is_err());
        let mut bad_crc = bytes;
        bad_crc[10] ^= 1;
        assert!(FRAME.decode::<Vec<u32>>(&bad_crc).is_err());
    }

    #[test]
    fn malicious_inner_lengths_are_bounded_without_panicking() {
        let mut payload = vec![1, 253];
        payload.extend_from_slice(&u64::MAX.to_le_bytes());
        let mut bytes = b"TEST".to_vec();
        bytes.extend_from_slice(&1u16.to_le_bytes());
        bytes.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&payload);
        bytes.extend_from_slice(&crc32fast::hash(&payload).to_le_bytes());
        let result = std::panic::catch_unwind(|| FRAME.decode::<Vec<u32>>(&bytes));
        assert!(result.is_ok());
        assert!(result.unwrap().is_err());
    }
}
