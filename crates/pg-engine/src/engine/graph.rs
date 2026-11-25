//! Phase 5b-1 Stage A graph registry, DDL, DML, and derived-index maintenance.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;

use pg_am_btree::BTreeAM;
use pg_am_graph::{
    GraphItem, GraphKeyCodec, GraphKeyKind, GraphMetadata, GraphStore, GRAPH_AM_OID,
    GRAPH_ITEM_COLUMNS, GRAPH_RECORD_EDGE, GRAPH_RECORD_NODE,
};
use pg_am_heap::access_method::{DeleteContext, InsertContext, UpdateContext};
use pg_am_heap::tuple::{ColumnType, Datum};
use pg_am_heap::{AccessMethod, UpdatableAM};
use pg_catalog::builtin_types::BYTEA_OID;
use pg_catalog::system_tables::{
    SystemTableDef, PG_ATTRIBUTE, PG_CLASS, PG_INDEX, PG_RELPAGES, RELKIND_INDEX,
};
use pg_storage::types::{Oid, PageId, Tid, TxnId};
use pg_txn::{LockMode, Snapshot};

use crate::engine::{
    column_types, relation_desc, tuple_header, Engine, IndexEntry, TableEntry, Value,
};
use crate::planner::AmKind;
use crate::sequence::{sequence_class_row, SequenceManager};
use crate::sql::{Literal, Statement};
use crate::{EngineError, QueryResult, Result};

const GRAPH_KEY_KINDS: [GraphKeyKind; 3] =
    [GraphKeyKind::Identity, GraphKeyKind::Out, GraphKeyKind::In];
const GRAPH_ITEM_COLUMN_NAMES: [&str; 5] = ["record_type", "id", "src", "dst", "properties"];

/// One live graph registration. The side file owns graph identity; the table
/// entry and indexes are recovered from the ordinary catalogs.
#[derive(Debug, Clone)]
pub(crate) struct GraphEntry {
    pub(crate) metadata: GraphMetadata,
    pub(crate) relation_name: String,
    pub(crate) table: TableEntry,
}

impl Engine {
    pub(crate) fn execute_graph_auto(&self, stmt: Statement) -> Result<QueryResult> {
        if let Statement::CreateGraph { name, directed } = stmt {
            return self.create_graph(&name, directed).map(|_| QueryResult::Ok);
        }
        self.auto_commit(|snap| {
            let xid = snap.current_xid();
            self.execute_graph_dml(snap, xid, stmt)
        })
    }

    pub(crate) fn execute_graph_txn(
        &self,
        snap: &Snapshot,
        xid: TxnId,
        stmt: Statement,
    ) -> Result<QueryResult> {
        // Graph integrity constraints must see the state after the table-lock
        // wait, including this transaction's own writes. Ordinary SELECTs keep
        // the explicit transaction's original SI snapshot.
        self.execute_graph_dml(snap, xid, stmt)
    }

    fn execute_graph_dml(
        &self,
        snap: &Snapshot,
        xid: TxnId,
        stmt: Statement,
    ) -> Result<QueryResult> {
        match stmt {
            Statement::GraphAddNode {
                graph,
                node_id,
                properties,
            } => {
                let node_id = graph_i64(&node_id, "graph node ID")?;
                let properties = graph_json(&properties)?;
                let graph = self.lock_graph(xid, &graph, LockMode::AccessExclusive)?;
                let (snap, _re_guard) = self.graph_snapshot_after_lock(snap, xid);
                self.graph_add_node_inner(&snap, &graph, node_id, properties)
                    .map(QueryResult::Affected)
            }
            Statement::GraphAddEdge {
                graph,
                source_node_id,
                target_node_id,
                properties,
            } => {
                let source_node_id = graph_i64(&source_node_id, "graph source node ID")?;
                let target_node_id = graph_i64(&target_node_id, "graph target node ID")?;
                let properties = graph_json(&properties)?;
                let graph = self.lock_graph(xid, &graph, LockMode::AccessExclusive)?;
                let (snap, _re_guard) = self.graph_snapshot_after_lock(snap, xid);
                self.graph_add_edge_inner(&snap, &graph, source_node_id, target_node_id, properties)
                    .map(QueryResult::Affected)
            }
            Statement::GraphUpdateNode {
                graph,
                node_id,
                properties,
            } => {
                let node_id = graph_i64(&node_id, "graph node ID")?;
                let properties = graph_json(&properties)?;
                let graph = self.lock_graph(xid, &graph, LockMode::RowExclusive)?;
                let (snap, _re_guard) = self.graph_snapshot_after_lock(snap, xid);
                self.graph_update_node_inner(&snap, &graph, node_id, properties)
                    .map(QueryResult::Affected)
            }
            Statement::GraphDeleteNode { graph, node_id } => {
                let node_id = graph_i64(&node_id, "graph node ID")?;
                let graph = self.lock_graph(xid, &graph, LockMode::AccessExclusive)?;
                let (snap, _re_guard) = self.graph_snapshot_after_lock(snap, xid);
                self.graph_delete_node_inner(&snap, &graph, node_id)
                    .map(QueryResult::Affected)
            }
            Statement::GraphDeleteEdge { graph, edge_id } => {
                let edge_id = graph_i64(&edge_id, "graph edge ID")?;
                let graph = self.lock_graph(xid, &graph, LockMode::RowExclusive)?;
                let (snap, _re_guard) = self.graph_snapshot_after_lock(snap, xid);
                self.graph_delete_edge_inner(&snap, &graph, edge_id)
                    .map(QueryResult::Affected)
            }
            Statement::GraphBfs(_) | Statement::GraphDfs(_) => Err(EngineError::Unsupported(
                "graph traversal execution is implemented in Stage B".to_string(),
            )),
            Statement::CreateGraph { .. } => Err(EngineError::InvalidArgument(
                "CREATE GRAPH must run as a standalone DDL statement".to_string(),
            )),
            _ => Err(EngineError::InvalidArgument(
                "not a graph statement".to_string(),
            )),
        }
    }

    fn graph_snapshot_after_lock(
        &self,
        snap: &Snapshot,
        xid: TxnId,
    ) -> (Snapshot, Option<pg_txn::SnapshotGuard>) {
        let (mut snapshot, guard) = self.txn.snapshot(xid);
        // Refresh the visibility horizon without rewinding the command counter:
        // earlier writes in the same explicit transaction must remain visible.
        snapshot.set_curcid(snap.curcid());
        (snapshot, Some(guard))
    }

    /// Create an empty graph, its three byte-key indexes, edge sequence, and
    /// side-file registration in one DDL transaction.
    pub(crate) fn create_graph(&self, name: &str, directed: bool) -> Result<()> {
        if self.graphs.read().contains_key(name) {
            return Err(EngineError::GraphExists(name.to_string()));
        }
        let _ddl = self.ddl_lock.lock();
        if self.graphs.read().contains_key(name) {
            return Err(EngineError::GraphExists(name.to_string()));
        }

        let graph_oid = self.catalog.oid_allocator().alloc();
        let toast_oid = self.catalog.oid_allocator().alloc();
        let sequence_oid = self.catalog.oid_allocator().alloc();
        let index_oids = [
            self.catalog.oid_allocator().alloc(),
            self.catalog.oid_allocator().alloc(),
            self.catalog.oid_allocator().alloc(),
        ];
        let relation_name = graph_relation_name(graph_oid);
        let sequence_name = graph_sequence_name(graph_oid);
        let sequence_def = self.sequence_relation()?;
        for (index_oid, kind) in index_oids.into_iter().zip(GRAPH_KEY_KINDS) {
            self.ensure_graph_index_catalog_room(graph_oid, index_oid, kind)?;
        }
        for (def, row) in [
            (&PG_CLASS, sequence_class_row(sequence_oid, &sequence_name)),
            (
                &sequence_def,
                vec![
                    Some(Datum::Int8(sequence_oid.0 as i64)),
                    Some(Datum::Text(sequence_name.clone())),
                    Some(Datum::Int8(0)),
                ],
            ),
        ] {
            let columns = def.column_types();
            let tuple = pg_am_heap::tuple::encode_tuple(tuple_header(0), &columns, &row)?;
            self.ensure_catalog_room(def, tuple.len())?;
        }

        let created = RefCell::new(None);
        let result = self.auto_commit_with_hooks(
            |snap| {
                self.lock_oid(snap.current_xid(), graph_oid, LockMode::AccessExclusive)?;
                let schema = GRAPH_ITEM_COLUMN_NAMES
                    .into_iter()
                    .zip(GRAPH_ITEM_COLUMNS)
                    .map(|(name, col_type)| crate::ColumnDef {
                        name: name.to_string(),
                        col_type,
                    })
                    .collect::<Vec<_>>();
                let (first_page, toast) = self.create_table_inner(
                    snap,
                    graph_oid,
                    Some(toast_oid),
                    &relation_name,
                    &schema,
                )?;
                let toast = toast.ok_or_else(|| {
                    EngineError::Corrupted("graph TOAST relation was not created".to_string())
                })?;
                self.heap.register_toast_relation(graph_oid, toast)?;
                let table = TableEntry {
                    oid: graph_oid,
                    first_page,
                    columns: schema,
                    defaults: vec![None; GRAPH_ITEM_COLUMNS.len()],
                    toast: Some(toast),
                };

                let btree = BTreeAM::new(
                    std::sync::Arc::clone(self.storage.buffer_pool()),
                    std::sync::Arc::clone(self.storage.wal_writer()),
                );
                for (index_oid, kind) in index_oids.into_iter().zip(GRAPH_KEY_KINDS) {
                    let index = btree.create_index(index_oid, ColumnType::Bytea)?;
                    let meta_page = index.meta_page();
                    self.create_graph_index_catalog_rows(
                        snap, graph_oid, index_oid, kind, meta_page,
                    )?;
                    self.indexes.write().push(IndexEntry {
                        kind: AmKind::Graph,
                        inverted: None,
                        index_oid,
                        table_oid: graph_oid,
                        column: graph_key_column(kind).to_string(),
                        key_type: ColumnType::Bytea,
                        meta_page,
                        map_page: None,
                    });
                }

                self.insert_catalog_row(
                    snap,
                    &PG_CLASS,
                    &sequence_class_row(sequence_oid, &sequence_name),
                )?;
                self.insert_catalog_row(
                    snap,
                    &sequence_def,
                    &[
                        Some(Datum::Int8(sequence_oid.0 as i64)),
                        Some(Datum::Text(sequence_name.clone())),
                        Some(Datum::Int8(0)),
                    ],
                )?;
                let metadata =
                    GraphMetadata::new(graph_oid, name, directed, first_page, sequence_oid)
                        .map_err(graph_error)?;
                GraphStore::create(self.storage.data_dir(), metadata.clone())
                    .map_err(graph_error)?;
                *created.borrow_mut() = Some(GraphEntry {
                    metadata,
                    relation_name: relation_name.clone(),
                    table,
                });
                Ok(())
            },
            || {
                let graph = created
                    .borrow_mut()
                    .take()
                    .expect("successful graph creation installs registry metadata");
                self.sequences.states.lock().insert(
                    sequence_oid,
                    crate::sequence::SequenceState {
                        name: sequence_name.clone(),
                        high_water: 0,
                    },
                );
                self.registry
                    .write()
                    .insert(relation_name.clone(), graph.table.clone());
                self.graphs.write().insert(name.to_string(), graph);
                Ok(())
            },
            || {
                self.heap.unregister_toast_relation(graph_oid);
                self.heap.drop_relation(graph_oid);
                self.heap.drop_relation(toast_oid);
                self.indexes
                    .write()
                    .retain(|index| !index_oids.contains(&index.index_oid));
                self.registry.write().remove(&relation_name);
                self.graphs.write().remove(name);
                self.sequences.states.lock().remove(&sequence_oid);
                remove_graph_directory(self.storage.data_dir(), graph_oid);
            },
        );
        result
    }

    pub(crate) fn graph_add_node_inner(
        &self,
        snap: &Snapshot,
        graph: &GraphEntry,
        node_id: i64,
        properties: String,
    ) -> Result<usize> {
        if self
            .graph_lookup_identity(snap, graph, GRAPH_RECORD_NODE, node_id)?
            .is_some()
        {
            return Err(EngineError::InvalidArgument(format!(
                "graph node {node_id} already exists"
            )));
        }
        let item = GraphItem::node(node_id, properties).map_err(graph_error)?;
        self.graph_insert_item(snap, graph, &item)?;
        Ok(1)
    }

    pub(crate) fn graph_add_edge_inner(
        &self,
        snap: &Snapshot,
        graph: &GraphEntry,
        source_node_id: i64,
        target_node_id: i64,
        properties: String,
    ) -> Result<usize> {
        for node_id in [source_node_id, target_node_id] {
            if self
                .graph_lookup_identity(snap, graph, GRAPH_RECORD_NODE, node_id)?
                .is_none()
            {
                return Err(EngineError::InvalidArgument(format!(
                    "graph edge endpoint {node_id} does not exist"
                )));
            }
        }
        let edge_id = self.nextval_oid(graph.metadata.edge_sequence_oid)?;
        let item = GraphItem::edge(edge_id, source_node_id, target_node_id, properties)
            .map_err(graph_error)?;
        self.graph_insert_item(snap, graph, &item)?;
        Ok(1)
    }

    pub(crate) fn graph_update_node_inner(
        &self,
        snap: &Snapshot,
        graph: &GraphEntry,
        node_id: i64,
        properties: String,
    ) -> Result<usize> {
        let (tid, old_item) = self
            .graph_lookup_identity(snap, graph, GRAPH_RECORD_NODE, node_id)?
            .ok_or_else(|| {
                EngineError::InvalidArgument(format!("graph node {node_id} does not exist"))
            })?;
        let new_item = GraphItem::node(node_id, properties).map_err(graph_error)?;
        self.graph_update_item(snap, graph, tid, &old_item, &new_item)?;
        Ok(1)
    }

    pub(crate) fn graph_delete_edge_inner(
        &self,
        snap: &Snapshot,
        graph: &GraphEntry,
        edge_id: i64,
    ) -> Result<usize> {
        let (tid, item) = self
            .graph_lookup_identity(snap, graph, GRAPH_RECORD_EDGE, edge_id)?
            .ok_or_else(|| {
                EngineError::InvalidArgument(format!("graph edge {edge_id} does not exist"))
            })?;
        self.graph_delete_item(snap, graph, tid, &item)?;
        Ok(1)
    }

    pub(crate) fn graph_delete_node_inner(
        &self,
        snap: &Snapshot,
        graph: &GraphEntry,
        node_id: i64,
    ) -> Result<usize> {
        let (node_tid, node) = self
            .graph_lookup_identity(snap, graph, GRAPH_RECORD_NODE, node_id)?
            .ok_or_else(|| {
                EngineError::InvalidArgument(format!("graph node {node_id} does not exist"))
            })?;
        let mut edge_tids = BTreeMap::new();
        for kind in [GraphKeyKind::Out, GraphKeyKind::In] {
            edge_tids.extend(self.graph_adjacent_edges(snap, graph, kind, node_id)?);
        }
        for (tid, edge) in edge_tids {
            self.graph_delete_item(snap, graph, tid, &edge)?;
        }
        self.graph_delete_item(snap, graph, node_tid, &node)?;
        Ok(1)
    }

    fn graph_insert_item(
        &self,
        snap: &Snapshot,
        graph: &GraphEntry,
        item: &GraphItem,
    ) -> Result<()> {
        let col_types = column_types(&graph.table);
        let values = graph_values(item);
        let tuple =
            self.encode_row_with_toast(snap, &graph.table, &col_types, &values, None, None)?;
        let mut out_tid = Tid {
            page_id: PageId::INVALID,
            slot_id: 0,
        };
        self.heap.insert(InsertContext {
            rel: relation_desc(&graph.table, &col_types),
            snapshot: snap,
            tuple: &tuple,
            out_tid: Some(&mut out_tid),
        })?;
        for kind in GRAPH_KEY_KINDS {
            let key = GraphKeyCodec::encode(kind, item).map_err(graph_error)?;
            let entry = self.graph_index(graph, kind)?;
            self.open_btree(&entry)?.insert(&key, out_tid)?;
            self.record_index_undo(
                snap.current_xid(),
                &entry,
                key,
                out_tid,
                crate::engine::IndexUndoOp::Inserted,
            );
        }
        Ok(())
    }

    fn graph_update_item(
        &self,
        snap: &Snapshot,
        graph: &GraphEntry,
        old_tid: Tid,
        _old_item: &GraphItem,
        new_item: &GraphItem,
    ) -> Result<()> {
        let col_types = column_types(&graph.table);
        let (old_raw_values, _) =
            self.read_row_by_tid_with_node_id(&graph.table, &col_types, old_tid)?;
        let tuple = self.encode_row_with_toast(
            snap,
            &graph.table,
            &col_types,
            &graph_values(new_item),
            None,
            Some(&old_raw_values),
        )?;
        let mut out_tid = Tid {
            page_id: PageId::INVALID,
            slot_id: 0,
        };
        self.heap.update(UpdateContext {
            rel: relation_desc(&graph.table, &col_types),
            snapshot: snap,
            old_tid,
            new_tuple: &tuple,
            out_tid: Some(&mut out_tid),
            clog: self.clog.as_ref(),
            hot_eligible: true,
        })?;
        if out_tid.page_id != old_tid.page_id {
            // Keep the old entry so another transaction can still find the
            // committed version while this update is in flight. Visibility
            // filtering and later vacuum own retirement of dead entries.
            for kind in GRAPH_KEY_KINDS {
                let entry = self.graph_index(graph, kind)?;
                let new_key = GraphKeyCodec::encode(kind, new_item).map_err(graph_error)?;
                self.open_btree(&entry)?.insert(&new_key, out_tid)?;
                self.record_index_undo(
                    snap.current_xid(),
                    &entry,
                    new_key,
                    out_tid,
                    crate::engine::IndexUndoOp::Inserted,
                );
            }
        }
        Ok(())
    }

    fn graph_delete_item(
        &self,
        snap: &Snapshot,
        graph: &GraphEntry,
        tid: Tid,
        _item: &GraphItem,
    ) -> Result<()> {
        let col_types = column_types(&graph.table);
        // Logical deletion is already MVCC-filtered by graph lookups. Keep the
        // physical entry until vacuum so concurrent readers never lose the
        // last committed version of this row.
        self.heap.delete(DeleteContext {
            rel: relation_desc(&graph.table, &col_types),
            snapshot: snap,
            tid,
            clog: self.clog.as_ref(),
        })?;
        Ok(())
    }

    fn graph_lookup_identity(
        &self,
        snap: &Snapshot,
        graph: &GraphEntry,
        record_type: i32,
        id: i64,
    ) -> Result<Option<(Tid, GraphItem)>> {
        let key = GraphKeyCodec::identity(record_type, id);
        let entry = self.graph_index(graph, GraphKeyKind::Identity)?;
        for tid in self.open_btree(&entry)?.lookup_all(&key)? {
            let Some(visible_tid) = self.heap_tuple_visible(snap, tid)? else {
                continue;
            };
            let values =
                self.read_row_by_tid(snap, &graph.table, &column_types(&graph.table), visible_tid)?;
            let item = graph_item_from_values(&values)?;
            if item.record_type == record_type && item.id == id {
                return Ok(Some((visible_tid, item)));
            }
        }
        Ok(None)
    }

    fn graph_adjacent_edges(
        &self,
        snap: &Snapshot,
        graph: &GraphEntry,
        kind: GraphKeyKind,
        node_id: i64,
    ) -> Result<BTreeMap<Tid, GraphItem>> {
        let entry = self.graph_index(graph, kind)?;
        let start = match kind {
            GraphKeyKind::Out => GraphKeyCodec::out(GRAPH_RECORD_EDGE, node_id, i64::MIN),
            GraphKeyKind::In => GraphKeyCodec::inbound(GRAPH_RECORD_EDGE, node_id, i64::MIN),
            GraphKeyKind::Identity => {
                return Err(EngineError::InvalidArgument(
                    "identity is not an adjacency index".to_string(),
                ))
            }
        };
        let mut edges = BTreeMap::new();
        for (key, tid) in self.open_btree(&entry)?.range_scan(Some(&start), None)? {
            if key.len() != start.len() {
                return Err(EngineError::Corrupted(format!(
                    "graph adjacency index contains a malformed {}-byte key",
                    key.len()
                )));
            }
            if key[..12] != start[..12] {
                break;
            }
            let Some(visible_tid) = self.heap_tuple_visible(snap, tid)? else {
                continue;
            };
            let values =
                self.read_row_by_tid(snap, &graph.table, &column_types(&graph.table), visible_tid)?;
            let item = graph_item_from_values(&values)?;
            let matches = match kind {
                GraphKeyKind::Out => item.src == node_id,
                GraphKeyKind::In => item.dst == node_id,
                GraphKeyKind::Identity => unreachable!(),
            };
            if item.record_type == GRAPH_RECORD_EDGE && matches {
                edges.insert(visible_tid, item);
            }
        }
        Ok(edges)
    }

    pub(crate) fn lock_graph(&self, xid: TxnId, name: &str, mode: LockMode) -> Result<GraphEntry> {
        let relation_name = self.graph_entry(name)?.relation_name.clone();
        let table = self.lock_table_entry(xid, &relation_name, mode)?;
        let mut graph = self.graph_entry(name)?;
        graph.table = table;
        Ok(graph)
    }

    pub(crate) fn graph_entry(&self, name: &str) -> Result<GraphEntry> {
        self.graphs
            .read()
            .get(name)
            .cloned()
            .ok_or_else(|| EngineError::InvalidArgument(format!("graph {name:?} does not exist")))
    }

    fn graph_index(&self, graph: &GraphEntry, kind: GraphKeyKind) -> Result<IndexEntry> {
        graph_index_from_slice(&self.indexes.read(), graph.metadata.oid, kind)
    }
}

pub(crate) fn is_graph_relation(engine: &Engine, entry: &TableEntry) -> bool {
    engine
        .indexes
        .read()
        .iter()
        .any(|index| index.table_oid == entry.oid && index.kind == AmKind::Graph)
}

pub(crate) fn graph_i64(literal: &Literal, operand: &str) -> Result<i64> {
    match literal {
        Literal::Int(value) => Ok(*value),
        _ => Err(EngineError::InvalidArgument(format!(
            "{operand} must be an INT8 literal"
        ))),
    }
}

pub(crate) fn graph_json(literal: &Literal) -> Result<String> {
    let Literal::Str(text) = literal else {
        return Err(EngineError::InvalidArgument(
            "graph properties must be a JSONB text literal, not SQL NULL".to_string(),
        ));
    };
    pg_am_graph::model::validate_json(text).map_err(graph_error)?;
    Ok(text.clone())
}

pub(crate) fn graph_item_from_values(values: &[Value]) -> Result<GraphItem> {
    let [Some(Datum::Int4(record_type)), Some(Datum::Int8(id)), Some(Datum::Int8(src)), Some(Datum::Int8(dst)), Some(Datum::Jsonb(properties))] =
        values
    else {
        return Err(EngineError::Corrupted(
            "GraphItem contains NULL, external, or mistyped attributes".to_string(),
        ));
    };
    let item = GraphItem {
        record_type: *record_type,
        id: *id,
        src: *src,
        dst: *dst,
        properties: properties.clone(),
    };
    item.validate()
        .map_err(|error| EngineError::Corrupted(format!("invalid GraphItem: {error}")))?;
    Ok(item)
}

pub(crate) fn graph_key_kind_from_column(column: &str) -> Option<GraphKeyKind> {
    match column {
        "id" => Some(GraphKeyKind::Identity),
        "src" => Some(GraphKeyKind::Out),
        "dst" => Some(GraphKeyKind::In),
        _ => None,
    }
}

pub(crate) fn recover_graph_registry(
    data_dir: &Path,
    tables: &HashMap<String, TableEntry>,
    indexes: &[IndexEntry],
    sequences: &SequenceManager,
) -> Result<HashMap<String, GraphEntry>> {
    let root = data_dir.join("graph");
    let entries = match std::fs::read_dir(&root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(HashMap::new()),
        Err(error) => return Err(EngineError::Storage(error.into())),
    };
    let mut graphs = HashMap::new();
    for entry in entries {
        let entry = entry.map_err(|error| EngineError::Storage(error.into()))?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let Some(oid) = path
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| name.parse::<u64>().ok())
            .map(Oid)
        else {
            continue;
        };
        let Some(store) = GraphStore::open(data_dir, oid).map_err(graph_error)? else {
            continue;
        };
        let metadata = store.metadata().clone();
        let Some((relation_name, table)) =
            tables.iter().find(|(_, table)| table.oid == metadata.oid)
        else {
            continue;
        };
        validate_graph_table(&metadata, relation_name, table)?;
        for kind in GRAPH_KEY_KINDS {
            graph_index_from_slice(indexes, metadata.oid, kind)?;
        }
        let sequence = sequences
            .states
            .lock()
            .get(&metadata.edge_sequence_oid)
            .cloned();
        let Some(sequence) = sequence else {
            return Err(EngineError::Corrupted(format!(
                "graph {} references missing edge sequence {}",
                metadata.oid, metadata.edge_sequence_oid
            )));
        };
        if sequence.name != graph_sequence_name(metadata.oid) {
            return Err(EngineError::Corrupted(format!(
                "graph {} edge sequence has unexpected name {:?}",
                metadata.oid, sequence.name
            )));
        }
        let graph = GraphEntry {
            relation_name: relation_name.clone(),
            table: table.clone(),
            metadata: metadata.clone(),
        };
        if graphs.insert(metadata.name.clone(), graph).is_some() {
            return Err(EngineError::Corrupted(format!(
                "duplicate graph name {:?}",
                metadata.name
            )));
        }
    }
    // A live Graph catalog relation without its side file is corruption, not
    // an empty registry. Only side files with no catalog owner are orphans.
    let expected = indexes
        .iter()
        .filter(|index| index.kind == AmKind::Graph)
        .map(|index| index.table_oid)
        .chain(tables.iter().filter_map(|(relation_name, table)| {
            let graph_oid = graph_oid_from_relation_name(relation_name)?;
            (table.oid == graph_oid && graph_schema_matches(table)).then_some(graph_oid)
        }))
        .collect::<HashSet<_>>();
    for graph_oid in expected {
        if !graphs.values().any(|graph| graph.metadata.oid == graph_oid) {
            return Err(EngineError::Corrupted(format!(
                "graph catalog relation {graph_oid} has no side file"
            )));
        }
    }
    Ok(graphs)
}

pub(crate) fn remove_orphan_graph_directories(
    data_dir: &Path,
    graphs: &HashMap<String, GraphEntry>,
) {
    let live = graphs
        .values()
        .map(|graph| graph.metadata.oid)
        .collect::<HashSet<_>>();
    let root = data_dir.join("graph");
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let Some(oid) = path
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| name.parse::<u64>().ok())
            .map(Oid)
        else {
            continue;
        };
        if !live.contains(&oid) {
            remove_graph_directory(data_dir, oid);
        }
    }
}

fn graph_schema_matches(table: &TableEntry) -> bool {
    table
        .columns
        .iter()
        .map(|column| (column.name.as_str(), column.col_type))
        .eq(GRAPH_ITEM_COLUMN_NAMES.into_iter().zip(GRAPH_ITEM_COLUMNS))
}

fn validate_graph_table(
    metadata: &GraphMetadata,
    relation_name: &str,
    table: &TableEntry,
) -> Result<()> {
    let schema_matches = graph_schema_matches(table);
    if relation_name != graph_relation_name(metadata.oid)
        || table.first_page != metadata.first_page
        || !schema_matches
    {
        return Err(EngineError::Corrupted(format!(
            "graph {} side file disagrees with its heap relation",
            metadata.oid
        )));
    }
    Ok(())
}

fn graph_index_from_slice(
    indexes: &[IndexEntry],
    graph_oid: Oid,
    kind: GraphKeyKind,
) -> Result<IndexEntry> {
    let column = graph_key_column(kind);
    let mut matching = indexes.iter().filter(|index| {
        index.table_oid == graph_oid && index.kind == AmKind::Graph && index.column == column
    });
    let index = matching.next().ok_or_else(|| {
        EngineError::Corrupted(format!("graph {graph_oid} is missing its {column} index"))
    })?;
    if matching.next().is_some() || index.key_type != ColumnType::Bytea {
        return Err(EngineError::Corrupted(format!(
            "graph {graph_oid} has duplicate or mistyped {column} indexes"
        )));
    }
    Ok(index.clone())
}

fn graph_values(item: &GraphItem) -> Vec<Value> {
    vec![
        Some(Datum::Int4(item.record_type)),
        Some(Datum::Int8(item.id)),
        Some(Datum::Int8(item.src)),
        Some(Datum::Int8(item.dst)),
        Some(Datum::Jsonb(item.properties.clone())),
    ]
}

fn graph_key_column(kind: GraphKeyKind) -> &'static str {
    match kind {
        GraphKeyKind::Identity => "id",
        GraphKeyKind::Out => "src",
        GraphKeyKind::In => "dst",
    }
}

fn graph_key_name(kind: GraphKeyKind) -> &'static str {
    match kind {
        GraphKeyKind::Identity => "identity",
        GraphKeyKind::Out => "out",
        GraphKeyKind::In => "in",
    }
}

fn graph_relation_name(graph_oid: Oid) -> String {
    format!("__graph_{}", graph_oid.0)
}

fn graph_oid_from_relation_name(name: &str) -> Option<Oid> {
    let oid = name.strip_prefix("__graph_")?.parse::<u64>().ok()?;
    (oid != Oid::INVALID.0).then_some(Oid(oid))
}

fn graph_sequence_name(graph_oid: Oid) -> String {
    format!("__graph_{}_edges", graph_oid.0)
}

fn graph_error(error: pg_am_graph::GraphError) -> EngineError {
    use pg_am_graph::GraphError;
    match error {
        GraphError::Unsupported(message) => EngineError::Unsupported(message.to_string()),
        GraphError::Corrupted(message) => EngineError::Corrupted(message),
        GraphError::InvalidArgument(message) | GraphError::Capacity(message) => {
            EngineError::InvalidArgument(message)
        }
        GraphError::Storage(message) => EngineError::Storage(pg_storage::error::StorageError::Io(
            std::io::Error::other(message),
        )),
    }
}

fn remove_graph_directory(data_dir: &Path, graph_oid: Oid) {
    let path = data_dir.join("graph").join(graph_oid.0.to_string());
    if let Err(error) = std::fs::remove_dir_all(&path) {
        if error.kind() != std::io::ErrorKind::NotFound {
            tracing::warn!(%error, path = %path.display(), "failed to remove graph directory");
        }
    }
}

impl Engine {
    fn ensure_graph_index_catalog_room(
        &self,
        graph_oid: Oid,
        index_oid: Oid,
        kind: GraphKeyKind,
    ) -> Result<()> {
        for (def, row) in graph_index_catalog_rows(graph_oid, index_oid, kind, PageId(0)) {
            let columns = def.column_types();
            let tuple = pg_am_heap::tuple::encode_tuple(tuple_header(0), &columns, &row)?;
            self.ensure_catalog_room(def, tuple.len())?;
        }
        Ok(())
    }

    fn create_graph_index_catalog_rows(
        &self,
        snap: &Snapshot,
        graph_oid: Oid,
        index_oid: Oid,
        kind: GraphKeyKind,
        meta_page: PageId,
    ) -> Result<()> {
        for (def, row) in graph_index_catalog_rows(graph_oid, index_oid, kind, meta_page) {
            self.insert_catalog_row(snap, def, &row)?;
        }
        Ok(())
    }
}

fn graph_index_catalog_rows(
    graph_oid: Oid,
    index_oid: Oid,
    kind: GraphKeyKind,
    meta_page: PageId,
) -> [(&'static SystemTableDef, Vec<Value>); 4] {
    let key_name = graph_key_name(kind);
    [
        (
            &PG_CLASS,
            vec![
                Some(Datum::Int8(index_oid.0 as i64)),
                Some(Datum::Text(format!("{}_{key_name}_idx", graph_oid.0))),
                Some(Datum::Text(RELKIND_INDEX.to_string())),
                Some(Datum::Int4(1)),
                Some(Datum::Int8(0)),
                Some(Datum::Int8(GRAPH_AM_OID.0 as i64)),
            ],
        ),
        (
            &PG_ATTRIBUTE,
            vec![
                Some(Datum::Int8(index_oid.0 as i64)),
                Some(Datum::Text(graph_key_column(kind).to_string())),
                Some(Datum::Int8(BYTEA_OID.raw().0 as i64)),
                Some(Datum::Int4(-1)),
                Some(Datum::Int4(1)),
                Some(Datum::Int4(0)),
                Some(Datum::Int4(1)),
            ],
        ),
        (
            &PG_INDEX,
            vec![
                Some(Datum::Int8(index_oid.0 as i64)),
                Some(Datum::Int8(graph_oid.0 as i64)),
                Some(Datum::Int4(1)),
                Some(Datum::Int4(0)),
                Some(Datum::Int4(0)),
            ],
        ),
        (
            &PG_RELPAGES,
            vec![
                Some(Datum::Int8(index_oid.0 as i64)),
                Some(Datum::Int8(meta_page.0 as i64)),
                Some(Datum::Int8(meta_page.0 as i64)),
                Some(Datum::Int8(1)),
            ],
        ),
    ]
}
