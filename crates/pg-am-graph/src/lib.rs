//! Phase 5b-1 graph access-method format and lifecycle contracts.
//!
//! Stage 0 freezes GraphItem, identity/out/in key bytes, side-file recovery,
//! traversal result/budget shapes, and explicit unsupported AM operations.
//! Stage A adds DDL/DML through the pg-engine graph adapter; BFS/DFS and
//! physical GC execution belong to later stages.

#![warn(missing_docs)]
#![warn(rust_2018_idioms)]

pub mod error;
pub mod key;
pub mod model;
pub mod recovery;
pub mod store;
pub mod traversal;

pub use error::{GraphError, Result};
pub use key::{GraphKeyCodec, GraphKeyKind};
pub use model::{
    GraphDirection, GraphHit, GraphItem, GraphMetadata, GRAPH_ITEM_COLUMNS,
    GRAPH_METADATA_FORMAT_VERSION, GRAPH_RECORD_EDGE, GRAPH_RECORD_NODE, MAX_GRAPH_DEPTH,
};
pub use store::{GraphStore, GRAPH_AM_OID};
pub use traversal::{GraphTraversalAlgorithm, GraphTraversalOptions, GraphTraversalRequest};

#[cfg(test)]
mod tests {
    use super::*;
    use pg_am_heap::access_method::{
        AccessMethod, BuildContext, DeleteContext, InsertContext, ScanContext,
    };
    use pg_am_heap::tuple::{ColumnType, Datum, TupleHeader};
    use pg_storage::clog::NoOpClogAccessor;
    use pg_storage::types::{Oid, PageId, Tid, TxnId};
    use pg_txn::Snapshot;

    fn header() -> TupleHeader {
        TupleHeader::new(
            TxnId(7),
            TxnId::INVALID,
            0,
            [0; 16],
            Tid {
                page_id: PageId(42),
                slot_id: 1,
            },
            0,
        )
    }

    fn metadata() -> GraphMetadata {
        GraphMetadata::new(Oid(20_000), "memory", true, PageId(42), Oid(20_001)).unwrap()
    }

    #[test]
    fn graph_item_golden_tuple_roundtrip_and_record_discrimination() {
        let node = GraphItem::node(7, r#"{"kind":"agent","n":1}"#).unwrap();
        let bytes = node.encode(header()).unwrap();
        let expected = vec![
            7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 42, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 5, 0, 64,
            0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 7, 0,
            0, 0, 0, 0, 0, 0, 26, 0, 0, 0, 123, 34, 107, 105, 110, 100, 34, 58, 34, 97, 103, 101,
            110, 116, 34, 44, 34, 110, 34, 58, 49, 125,
        ];
        assert_eq!(bytes, expected);
        let (_, decoded) = GraphItem::decode(&bytes).unwrap();
        assert_eq!(decoded, node);

        let same_id_edge = GraphItem::edge(7, 7, 7, "{}").unwrap();
        assert_ne!(node.record_type, same_id_edge.record_type);
        assert_ne!(
            GraphKeyCodec::encode(GraphKeyKind::Identity, &node).unwrap(),
            GraphKeyCodec::encode(GraphKeyKind::Identity, &same_id_edge).unwrap()
        );
    }

    #[test]
    fn graph_key_golden_bytes_and_adjacency_order() {
        let edge = GraphItem::edge(-2, -1, 3, "{}").unwrap();
        assert_eq!(
            GraphKeyCodec::encode(GraphKeyKind::Identity, &edge).unwrap(),
            vec![0x80, 0x00, 0x00, 0x02, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe,]
        );
        assert_eq!(
            GraphKeyCodec::encode(GraphKeyKind::Out, &edge).unwrap(),
            vec![
                0x80, 0x00, 0x00, 0x02, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f, 0xff,
                0xff, 0xff, 0xff, 0xff, 0xff, 0xfe,
            ]
        );
        assert_eq!(
            GraphKeyCodec::encode(GraphKeyKind::In, &edge).unwrap(),
            vec![
                0x80, 0x00, 0x00, 0x02, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x7f, 0xff,
                0xff, 0xff, 0xff, 0xff, 0xff, 0xfe,
            ]
        );

        let first = GraphKeyCodec::out(GRAPH_RECORD_EDGE, 9, 1);
        let second = GraphKeyCodec::out(GRAPH_RECORD_EDGE, 9, 2);
        let other_neighbor = GraphKeyCodec::out(GRAPH_RECORD_EDGE, 10, 0);
        assert!(first < second && second < other_neighbor);
        let node = GraphKeyCodec::out(GRAPH_RECORD_NODE, 9, 9);
        assert!(node < first, "record_type prefix excludes node self-rows");
    }

    #[test]
    fn metadata_roundtrip_and_side_file_reopen() {
        let tmp = tempfile::TempDir::new().unwrap();
        let store = GraphStore::create(tmp.path(), metadata()).unwrap();
        let reopened = GraphStore::open(tmp.path(), Oid(20_000)).unwrap().unwrap();
        assert_eq!(reopened.metadata(), store.metadata());
        let bytes = std::fs::read(GraphStore::metadata_path(tmp.path(), Oid(20_000))).unwrap();
        assert_eq!(
            bytes,
            vec![
                80, 71, 71, 82, 1, 0, 16, 0, 0, 0, 1, 251, 32, 78, 6, 109, 101, 109, 111, 114, 121,
                1, 42, 251, 33, 78, 69, 200, 248, 184,
            ]
        );
        assert!(GraphStore::open(tmp.path(), Oid(20_002)).unwrap().is_none());
        assert_eq!(
            GraphStore::metadata_path(tmp.path(), Oid(20_000)),
            tmp.path().join("graph/20000/metadata.bin")
        );
    }

    #[test]
    fn create_is_idempotent_and_never_overwrites_existing_metadata() {
        let tmp = tempfile::TempDir::new().unwrap();
        let original = GraphStore::create(tmp.path(), metadata()).unwrap();
        GraphStore::create(tmp.path(), metadata()).unwrap();
        let original_bytes =
            std::fs::read(GraphStore::metadata_path(tmp.path(), Oid(20_000))).unwrap();
        let conflicting = GraphMetadata {
            name: "different".to_string(),
            edge_sequence_oid: Oid(20_099),
            ..metadata()
        };
        assert!(GraphStore::create(tmp.path(), conflicting).is_err());
        let after = std::fs::read(GraphStore::metadata_path(tmp.path(), Oid(20_000))).unwrap();
        assert_eq!(after, original_bytes);
        assert_eq!(
            GraphStore::open(tmp.path(), Oid(20_000))
                .unwrap()
                .unwrap()
                .metadata(),
            original.metadata()
        );

        std::fs::write(
            GraphStore::metadata_path(tmp.path(), Oid(20_000)),
            b"corrupt",
        )
        .unwrap();
        assert!(GraphStore::create(tmp.path(), metadata()).is_err());
    }

    #[test]
    fn metadata_rejects_bad_magic_version_length_crc_and_inner_version() {
        let valid = metadata().encode().unwrap();
        let mut bad_magic = valid.clone();
        bad_magic[0] ^= 1;
        assert!(matches!(
            GraphMetadata::decode(&bad_magic),
            Err(GraphError::Corrupted(_))
        ));

        let mut bad_version = valid.clone();
        bad_version[4..6].copy_from_slice(&2u16.to_le_bytes());
        assert!(matches!(
            GraphMetadata::decode(&bad_version),
            Err(GraphError::Corrupted(_))
        ));
        let mut bad_length = valid.clone();
        bad_length[6..10].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(matches!(
            GraphMetadata::decode(&bad_length),
            Err(GraphError::Corrupted(_))
        ));
        let mut truncated = valid.clone();
        truncated.pop();
        assert!(matches!(
            GraphMetadata::decode(&truncated),
            Err(GraphError::Corrupted(_))
        ));
        let mut bad_crc = valid;
        bad_crc[10] ^= 1;
        assert!(matches!(
            GraphMetadata::decode(&bad_crc),
            Err(GraphError::Corrupted(_))
        ));

        let mut wrong_inner = metadata();
        wrong_inner.format_version += 1;
        let framed = recovery::encode(&wrong_inner).unwrap();
        assert!(matches!(
            GraphMetadata::decode(&framed),
            Err(GraphError::Corrupted(_))
        ));
    }

    #[test]
    fn malicious_nested_lengths_are_bounded_without_panicking() {
        let mut payload = vec![1, 0, 253];
        payload.extend_from_slice(&u64::MAX.to_le_bytes());
        let mut framed = recovery::MAGIC.to_vec();
        framed.extend_from_slice(&recovery::FORMAT_VERSION.to_le_bytes());
        framed.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        framed.extend_from_slice(&payload);
        framed.extend_from_slice(&crc32fast::hash(&payload).to_le_bytes());
        let result = std::panic::catch_unwind(|| GraphMetadata::decode(&framed));
        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), Err(GraphError::Corrupted(_))));
    }

    #[test]
    fn invalid_json_shapes_depth_and_traversal_are_loud() {
        assert!(GraphItem::node(1, "not json").is_err());
        let mut malformed = GraphItem::node(1, "{}").unwrap();
        malformed.src = 2;
        assert!(malformed.validate().is_err());
        assert!(GraphTraversalRequest::new(
            GraphTraversalAlgorithm::Bfs,
            1,
            GraphDirection::Out,
            4,
            GraphTraversalOptions::default(),
        )
        .is_err());
        assert_eq!(GraphTraversalOptions::default().max_results, 100_000);
        assert_eq!(
            GraphTraversalOptions::default().max_visited_nodes,
            1_000_000
        );
        assert_eq!(
            GraphTraversalOptions::default().max_response_bytes,
            16 * 1024 * 1024
        );

        let hit = GraphHit::new(
            Tid {
                page_id: PageId(42),
                slot_id: 1,
            },
            2,
            1,
            &[1, 2],
        )
        .unwrap();
        assert_eq!(hit.path, "[1,2]");
        assert_eq!(hit.source_tid.slot_id, 1);
    }

    #[test]
    fn access_method_contract_is_explicit_and_never_fakes_success() {
        let tmp = tempfile::TempDir::new().unwrap();
        let store = GraphStore::new(tmp.path(), metadata()).unwrap();
        let snapshot = Snapshot::everything();
        let clog = NoOpClogAccessor;
        let rel = pg_am_heap::RelationDesc {
            rel_oid: Oid(20_000),
            first_page: PageId(42),
            columns: &GRAPH_ITEM_COLUMNS,
        };
        store.build(BuildContext { rel }).unwrap();
        assert!(store
            .insert(InsertContext {
                rel,
                snapshot: &snapshot,
                tuple: &[],
                out_tid: None,
            })
            .is_err());
        assert!(store
            .scan(ScanContext {
                rel,
                snapshot: &snapshot,
                clog: &clog,
            })
            .is_err());
        assert!(store
            .delete(DeleteContext {
                rel,
                snapshot: &snapshot,
                tid: Tid {
                    page_id: PageId(42),
                    slot_id: 1,
                },
                clog: &clog,
            })
            .is_err());
        assert_eq!(store.freshness(), None);
        assert_eq!(store.name(), "graph");
        assert_eq!(GRAPH_AM_OID, Oid(9880));
        let request = GraphTraversalRequest::new(
            GraphTraversalAlgorithm::Bfs,
            1,
            GraphDirection::Out,
            3,
            GraphTraversalOptions::default(),
        )
        .unwrap();
        assert!(matches!(
            store.search(&request),
            Err(GraphError::Unsupported(_))
        ));

        let wrong_rel = pg_am_heap::RelationDesc {
            columns: &[ColumnType::Int4],
            ..rel
        };
        assert!(store.build(BuildContext { rel: wrong_rel }).is_err());
        let _ = Datum::Jsonb("{}".to_string());
    }
}
