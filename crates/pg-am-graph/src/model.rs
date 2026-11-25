//! Frozen GraphItem, graph metadata, and traversal result shapes.

use pg_am_heap::tuple::{decode_tuple, encode_tuple, ColumnType, Datum, TupleHeader};
use pg_storage::types::{Oid, PageId, Tid};
use serde::{Deserialize, Serialize};

use crate::error::{GraphError, Result};
use crate::recovery;

/// Graph metadata format version stored both in the frame and payload.
pub const GRAPH_METADATA_FORMAT_VERSION: u16 = 1;
/// `record_type` value for a node row.
pub const GRAPH_RECORD_NODE: i32 = 1;
/// `record_type` value for an edge row.
pub const GRAPH_RECORD_EDGE: i32 = 2;
/// Maximum traversal depth accepted by the Stage 0 SQL and search contracts.
pub const MAX_GRAPH_DEPTH: u32 = 3;

/// Schema of the single heap relation shared by nodes and edges.
pub const GRAPH_ITEM_COLUMNS: [ColumnType; 5] = [
    ColumnType::Int4,
    ColumnType::Int8,
    ColumnType::Int8,
    ColumnType::Int8,
    ColumnType::Jsonb,
];

/// One node or edge stored in the graph's heap relation.
///
/// Node rows have `id == src == dst`. Edge rows use `id` as their edge ID and
/// `src`/`dst` as endpoint node IDs. Every index lookup must include
/// `record_type`; equal numeric IDs do not make nodes and edges interchangeable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphItem {
    /// Node (`1`) or edge (`2`) discriminator.
    pub record_type: i32,
    /// Node ID or edge ID, depending on `record_type`.
    pub id: i64,
    /// Source node ID; equal to `id` for nodes.
    pub src: i64,
    /// Destination node ID; equal to `id` for nodes.
    pub dst: i64,
    /// Original, validated JSON text. Encoding does not canonicalize it.
    pub properties: String,
}

impl GraphItem {
    /// Construct a validated node row.
    pub fn node(node_id: i64, properties: impl Into<String>) -> Result<Self> {
        let item = Self {
            record_type: GRAPH_RECORD_NODE,
            id: node_id,
            src: node_id,
            dst: node_id,
            properties: properties.into(),
        };
        item.validate()?;
        Ok(item)
    }

    /// Construct a validated edge row.
    pub fn edge(
        edge_id: i64,
        source_node_id: i64,
        target_node_id: i64,
        properties: impl Into<String>,
    ) -> Result<Self> {
        let item = Self {
            record_type: GRAPH_RECORD_EDGE,
            id: edge_id,
            src: source_node_id,
            dst: target_node_id,
            properties: properties.into(),
        };
        item.validate()?;
        Ok(item)
    }

    /// Validate the row discriminator, node shape, and JSONB property text.
    pub fn validate(&self) -> Result<()> {
        match self.record_type {
            GRAPH_RECORD_NODE if self.id == self.src && self.id == self.dst => {}
            GRAPH_RECORD_NODE => {
                return Err(GraphError::InvalidArgument(
                    "graph node requires id = src = dst".to_string(),
                ));
            }
            GRAPH_RECORD_EDGE => {}
            other => {
                return Err(GraphError::InvalidArgument(format!(
                    "unknown graph record_type {other}"
                )));
            }
        }
        validate_json(&self.properties)
    }

    /// Encode this row using the common heap tuple and TOAST-compatible
    /// varlena format. External properties are resolved by the heap reader.
    pub fn encode(&self, header: TupleHeader) -> Result<Vec<u8>> {
        self.validate()?;
        encode_tuple(
            header,
            &GRAPH_ITEM_COLUMNS,
            &[
                Some(Datum::Int4(self.record_type)),
                Some(Datum::Int8(self.id)),
                Some(Datum::Int8(self.src)),
                Some(Datum::Int8(self.dst)),
                Some(Datum::Jsonb(self.properties.clone())),
            ],
        )
        .map_err(|error| GraphError::InvalidArgument(error.to_string()))
    }

    /// Decode an inline GraphItem and its heap header.
    ///
    /// TOAST pointers must be detoasted by the heap layer before this format
    /// adapter is called; this method never mistakes a pointer for JSON text.
    pub fn decode(bytes: &[u8]) -> Result<(TupleHeader, Self)> {
        let (header, values) = decode_tuple(bytes, &GRAPH_ITEM_COLUMNS)
            .map_err(|error| GraphError::Corrupted(error.to_string()))?;
        let [Some(Datum::Int4(record_type)), Some(Datum::Int8(id)), Some(Datum::Int8(src)), Some(Datum::Int8(dst)), Some(Datum::Jsonb(properties))] =
            values.as_slice()
        else {
            return Err(GraphError::Corrupted(
                "GraphItem contains NULL, external, or mistyped attributes".to_string(),
            ));
        };
        let item = Self {
            record_type: *record_type,
            id: *id,
            src: *src,
            dst: *dst,
            properties: properties.clone(),
        };
        item.validate()
            .map_err(|error| GraphError::Corrupted(error.to_string()))?;
        Ok((header, item))
    }
}

/// Persisted identity of one graph. Edge IDs are owned exclusively by the
/// referenced sequence's high-water mark; no second counter is stored here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphMetadata {
    /// Payload format version; must match [`GRAPH_METADATA_FORMAT_VERSION`].
    pub format_version: u16,
    /// Graph OID, also the internal heap relation OID.
    pub oid: Oid,
    /// Unique user-visible graph name.
    pub name: String,
    /// Whether edges have direction.
    pub directed: bool,
    /// First heap page of the GraphItem relation.
    pub first_page: PageId,
    /// Owning edge-ID sequence.
    pub edge_sequence_oid: Oid,
}

impl GraphMetadata {
    /// Construct metadata using the current format version.
    pub fn new(
        oid: Oid,
        name: impl Into<String>,
        directed: bool,
        first_page: PageId,
        edge_sequence_oid: Oid,
    ) -> Result<Self> {
        let metadata = Self {
            format_version: GRAPH_METADATA_FORMAT_VERSION,
            oid,
            name: name.into(),
            directed,
            first_page,
            edge_sequence_oid,
        };
        metadata.validate()?;
        Ok(metadata)
    }

    /// Validate persisted identifiers and the inner format version.
    pub fn validate(&self) -> Result<()> {
        if self.format_version != GRAPH_METADATA_FORMAT_VERSION {
            return Err(GraphError::InvalidArgument(format!(
                "unsupported graph metadata payload version {}",
                self.format_version
            )));
        }
        if self.oid == Oid::INVALID
            || self.first_page == PageId::INVALID
            || self.edge_sequence_oid == Oid::INVALID
        {
            return Err(GraphError::InvalidArgument(
                "graph metadata contains an invalid OID or page".to_string(),
            ));
        }
        if self.name.is_empty() {
            return Err(GraphError::InvalidArgument(
                "graph name must not be empty".to_string(),
            ));
        }
        Ok(())
    }

    /// Encode metadata in the bounded version+CRC side-file frame.
    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        recovery::encode(self)
    }

    /// Decode and validate one metadata side file.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let metadata: Self = recovery::decode(bytes)?;
        metadata
            .validate()
            .map_err(|error| GraphError::Corrupted(error.to_string()))?;
        Ok(metadata)
    }
}

/// Direction of adjacency expansion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphDirection {
    /// Follow `src -> dst` using the out key codec.
    Out,
    /// Follow `dst -> src` using the in key codec.
    In,
    /// Merge and deduplicate both adjacency directions.
    Both,
}

/// Internal traversal result. SQL currently projects only
/// `(node_id, depth, path)`; `source_tid` remains available for future Fusion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphHit {
    /// Heap TID of the visible GraphItem node row.
    pub source_tid: Tid,
    /// Reached node ID.
    pub node_id: i64,
    /// BFS shortest-path depth or DFS preorder depth.
    pub depth: i32,
    /// JSONB array text containing every node ID from start through this hit.
    pub path: String,
}

impl GraphHit {
    /// Construct a result with validated JSON array path text.
    pub fn new(source_tid: Tid, node_id: i64, depth: i32, path: &[i64]) -> Result<Self> {
        if depth < 0 || depth > MAX_GRAPH_DEPTH as i32 {
            return Err(GraphError::InvalidArgument(format!(
                "graph depth {depth} is outside 0..={MAX_GRAPH_DEPTH}"
            )));
        }
        if path.is_empty() {
            return Err(GraphError::InvalidArgument(
                "graph path must contain its start node".to_string(),
            ));
        }
        let path = serde_json::to_string(path)
            .map_err(|error| GraphError::InvalidArgument(error.to_string()))?;
        Ok(Self {
            source_tid,
            node_id,
            depth,
            path,
        })
    }
}

/// Validate JSON without normalizing its original text.
pub fn validate_json(text: &str) -> Result<()> {
    serde_json::from_str::<serde_json::Value>(text)
        .map(|_| ())
        .map_err(|error| GraphError::InvalidArgument(format!("invalid JSONB: {error}")))
}
