//! Frozen order-preserving keys for the identity, out, and in B+Tree indexes.

use pg_am_btree::key::{encode_i32, encode_i64};

use crate::error::Result;
use crate::model::GraphItem;

/// Byte length of `record_type:u32 + id:u64` identity keys.
pub const GRAPH_IDENTITY_KEY_BYTES: usize = 12;
/// Byte length of adjacency keys with a neighbor and edge ID.
pub const GRAPH_ADJACENCY_KEY_BYTES: usize = 20;

/// The three single-column byte-key indexes attached to every graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphKeyKind {
    /// `(record_type,id)`: node existence and exact edge-ID deletion.
    Identity,
    /// `(record_type,src,id)`: outgoing adjacency.
    Out,
    /// `(record_type,dst,id)`: incoming adjacency.
    In,
}

/// Stateless encoder for GraphItem B+Tree keys.
#[derive(Debug, Clone, Copy, Default)]
pub struct GraphKeyCodec;

impl GraphKeyCodec {
    /// Encode an identity key or exact identity prefix.
    pub fn identity(record_type: i32, id: i64) -> [u8; GRAPH_IDENTITY_KEY_BYTES] {
        let mut bytes = [0; GRAPH_IDENTITY_KEY_BYTES];
        bytes[..4].copy_from_slice(&encode_i32(record_type));
        bytes[4..].copy_from_slice(&encode_i64(id));
        bytes
    }

    /// Encode an outgoing adjacency key.
    pub fn out(record_type: i32, source_node_id: i64, id: i64) -> [u8; GRAPH_ADJACENCY_KEY_BYTES] {
        adjacency(record_type, source_node_id, id)
    }

    /// Encode an incoming adjacency key. `in` is a Rust keyword, so the API
    /// uses the explicit `inbound` spelling while the index kind remains `In`.
    pub fn inbound(
        record_type: i32,
        target_node_id: i64,
        id: i64,
    ) -> [u8; GRAPH_ADJACENCY_KEY_BYTES] {
        adjacency(record_type, target_node_id, id)
    }

    /// Encode one GraphItem for the selected index.
    pub fn encode(kind: GraphKeyKind, item: &GraphItem) -> Result<Vec<u8>> {
        item.validate()?;
        let bytes = match kind {
            GraphKeyKind::Identity => Self::identity(item.record_type, item.id).to_vec(),
            GraphKeyKind::Out => Self::out(item.record_type, item.src, item.id).to_vec(),
            GraphKeyKind::In => Self::inbound(item.record_type, item.dst, item.id).to_vec(),
        };
        Ok(bytes)
    }
}

fn adjacency(record_type: i32, neighbor_id: i64, id: i64) -> [u8; GRAPH_ADJACENCY_KEY_BYTES] {
    let mut bytes = [0; GRAPH_ADJACENCY_KEY_BYTES];
    bytes[..4].copy_from_slice(&encode_i32(record_type));
    bytes[4..12].copy_from_slice(&encode_i64(neighbor_id));
    bytes[12..].copy_from_slice(&encode_i64(id));
    bytes
}
