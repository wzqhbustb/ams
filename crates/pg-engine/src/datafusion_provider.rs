//! Optional DataFusion AP integration (Phase 4a Stage E).
//!
//! The self-owned TP/ANN executor remains authoritative for visibility. This
//! module only routes an explicit AP query through DataFusion, converting its
//! Arrow batches back to engine values. SnapshotGuards live in the provider
//! and execution plan until DataFusion drops the resulting stream.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

const JSONB_FIELD_METADATA_KEY: &str = "pg_rust.column_type";
const JSONB_FIELD_METADATA_VALUE: &str = "jsonb";

use async_trait::async_trait;
use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, FixedSizeBinaryArray, Float32Array, Float64Array, Int32Array,
    Int64Array, ListArray, StringArray, TimestampMicrosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DFSchema, ScalarValue};
use datafusion::datasource::TableType;
use datafusion::error::DataFusionError;
use datafusion::execution::context::{SQLOptions, SessionContext, TaskContext};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::{
    utils::merge_schema, BinaryExpr, Expr, ExprSchemable, LogicalPlan, Operator,
    TableProviderFilterPushDown,
};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::display::{DisplayAs, DisplayFormatType};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{ExecutionPlan, Partitioning, PlanProperties};
use futures::stream;
use pg_am_heap::tuple::{ColumnType, Datum};
use pg_txn::{LockMode, Snapshot, SnapshotGuard};

use crate::engine::{filter_to_predicate, Engine, QueryResult, TableEntry, Value};
use crate::error::{EngineError, Result};
use crate::sql::{CmpOp, Filter as SqlFilter, Literal};

fn df_error(error: impl std::fmt::Display) -> DataFusionError {
    DataFusionError::Execution(error.to_string())
}

fn engine_error(error: impl std::fmt::Display) -> EngineError {
    EngineError::InvalidArgument(format!("DataFusion query failed: {error}"))
}

/// Execute one explicit AP query. Default `exec` / TP / ANN paths never call
/// this entry point, so their behavior is unchanged when the feature is off.
pub(crate) async fn execute_datafusion(
    engine: &Arc<Engine>,
    txn: Option<&crate::engine::TxnHandle>,
    sql: &str,
) -> Result<QueryResult> {
    let statement = crate::sql::parse(sql).ok();
    if let Some(crate::sql::Statement::Select {
        table,
        order_by: Some(crate::sql::OrderByKind::Column(order_by)),
        ..
    }) = &statement
    {
        let entry = engine.describe_table(table).ok_or_else(|| {
            EngineError::InvalidPredicate(format!("table {table:?} does not exist"))
        })?;
        if entry.columns.iter().any(|column| {
            column.name.eq_ignore_ascii_case(&order_by.column)
                && column.col_type == ColumnType::Jsonb
        }) {
            return Err(EngineError::Unsupported(format!(
                "ORDER BY JSONB column {:?} is not supported",
                order_by.column
            )));
        }
    }
    if let Some(crate::sql::Statement::Select {
        table,
        filter,
        extra_filter,
        ..
    }) = &statement
    {
        let entry = engine.describe_table(table).ok_or_else(|| {
            EngineError::InvalidPredicate(format!("table {table:?} does not exist"))
        })?;
        for filter in [filter, extra_filter].into_iter().flatten() {
            if entry.columns.iter().any(|column| {
                column.name.eq_ignore_ascii_case(&filter.column)
                    && column.col_type == ColumnType::Jsonb
            }) {
                return Err(EngineError::Unsupported(format!(
                    "JSONB predicate on column {:?} is not supported",
                    filter.column
                )));
            }
        }
    }
    let (columns, order_by, lock) = match statement {
        Some(crate::sql::Statement::Select {
            columns,
            order_by,
            lock,
            ..
        }) => (columns, order_by, lock),
        Some(_) => {
            return Err(EngineError::Unsupported(
                "DataFusion AP only supports SELECT statements".to_string(),
            ))
        }
        // DataFusion owns richer AP grammar. The Mini SQL parser is only a
        // guard for ANN, row locks, and non-SELECT statements, never a filter
        // on which AP queries may execute.
        None => (crate::sql::SelectCols::All, None, None),
    };
    if lock.is_some() {
        return Err(EngineError::Unsupported(
            "DataFusion AP does not support FOR UPDATE / FOR SHARE".to_string(),
        ));
    }
    if matches!(order_by, Some(crate::sql::OrderByKind::Expr(_)))
        || matches!(
            columns,
            crate::sql::SelectCols::Cols(ref items)
                if items.iter().any(|item| matches!(item, crate::sql::SelectItem::Distance(_)))
        )
    {
        return Err(EngineError::Unsupported(
            "ANN queries stay on the self-owned TP executor".to_string(),
        ));
    }

    let (snapshot, guard) = match txn {
        Some(handle) => {
            // Explicit AP transactions take AccessShare on every relation that
            // a JOIN might touch, not only the first FROM table.
            for (name, _) in engine.table_entries() {
                engine.lock_table_entry(handle.xid(), &name, LockMode::AccessShare)?;
            }
            handle.advance_curcid();
            (handle.snapshot(), None)
        }
        None => {
            let (mut snapshot, guard) = engine
                .txn_manager()
                .snapshot(pg_storage::types::TxnId::INVALID);
            snapshot.advance_curcid();
            (snapshot, Some(Arc::new(guard)))
        }
    };
    let context = SessionContext::new();
    // Register every live table so JOIN and subquery resolution works instead
    // of being limited to the first FROM table.
    for (name, table_entry) in engine.table_entries() {
        let provider = PgTableProvider::new(
            Arc::clone(engine),
            name.clone(),
            table_entry.clone(),
            snapshot.clone(),
            guard.clone(),
        );
        context
            .register_table(name.clone(), Arc::new(provider))
            .map_err(engine_error)?;
    }
    let plan = context
        .state()
        .create_logical_plan(sql)
        .await
        .map_err(engine_error)?;
    SQLOptions::new().verify_plan(&plan).map_err(engine_error)?;
    reject_jsonb_operations(&plan).map_err(|error| EngineError::Unsupported(error.to_string()))?;
    let plan = rewrite_uuid_string_predicates(plan).map_err(engine_error)?;
    let frame = context
        .execute_logical_plan(plan)
        .await
        .map_err(engine_error)?;
    let result_schema = Arc::clone(frame.schema().inner());
    let batches = frame.collect().await.map_err(engine_error)?;
    // The context owns the provider and its guard. Dropping it here closes the
    // query owner before any subsequent vacuum can run.
    drop(context);
    batches_to_query_result(&batches, &result_schema)
}

fn reject_jsonb_operations(plan: &LogicalPlan) -> datafusion::common::Result<()> {
    plan.clone()
        .transform_up_with_subqueries(|plan| {
            let schema = expression_schema(&plan);
            if matches!(plan, LogicalPlan::Distinct(_) | LogicalPlan::Union(_))
                && (schema_has_jsonb(&schema) || schema_has_jsonb(plan.schema()))
            {
                return Err(DataFusionError::Plan(
                    "JSONB set operations are not supported".to_string(),
                ));
            }
            if let LogicalPlan::Projection(projection) = &plan {
                for expr in &projection.expr {
                    validate_jsonb_projection(expr, &schema)?;
                }
                return Ok(Transformed::no(plan));
            }
            if let LogicalPlan::Join(join) = &plan {
                for (left, right) in &join.on {
                    validate_jsonb_expression(left, &schema)?;
                    validate_jsonb_expression(right, &schema)?;
                }
            }
            if let LogicalPlan::Sort(sort) = &plan {
                for sort_expr in &sort.expr {
                    validate_jsonb_expression(&sort_expr.expr, &schema)?;
                }
            }
            if let LogicalPlan::Aggregate(aggregate) = &plan {
                for expr in aggregate.group_expr.iter().chain(&aggregate.aggr_expr) {
                    validate_jsonb_expression(expr, &schema)?;
                }
            }
            plan.map_expressions(|expr| {
                validate_jsonb_expression(&expr, &schema)?;
                Ok(Transformed::no(expr))
            })
        })
        .map(|_| ())
}

// JSONB is transported as Utf8, so any operation other than returning a column
// unchanged or testing it for NULL would silently acquire TEXT semantics.
fn validate_jsonb_projection(expr: &Expr, schema: &DFSchema) -> datafusion::common::Result<()> {
    match expr {
        Expr::Column(column) if is_jsonb_column(column, schema) => Ok(()),
        Expr::Alias(alias) => validate_jsonb_projection(&alias.expr, schema),
        _ => validate_jsonb_expression(expr, schema),
    }
}

fn validate_jsonb_expression(expr: &Expr, schema: &DFSchema) -> datafusion::common::Result<()> {
    if let Expr::Column(column) = expr {
        if is_jsonb_column(column, schema) {
            return Err(DataFusionError::Plan(
                "JSONB operation is not supported".to_string(),
            ));
        }
        return Ok(());
    }
    if let Expr::IsNull(inner) | Expr::IsNotNull(inner) = expr {
        if let Expr::Column(column) = &**inner {
            if is_jsonb_column(column, schema) {
                return Ok(());
            }
        }
    }
    if let Expr::WindowFunction(function) = expr {
        for expr in &function.params.partition_by {
            validate_jsonb_expression(expr, schema)?;
        }
        for sort_expr in &function.params.order_by {
            validate_jsonb_expression(&sort_expr.expr, schema)?;
        }
    }
    expr.clone()
        .map_children(|child| {
            validate_jsonb_expression(&child, schema)?;
            Ok(Transformed::no(child))
        })
        .map(|_| ())
}

fn expression_schema(plan: &LogicalPlan) -> DFSchema {
    let schema = merge_schema(&plan.inputs());
    if schema.fields().is_empty() {
        plan.schema().as_ref().clone()
    } else {
        schema
    }
}

fn is_jsonb_column(column: &datafusion::common::Column, schema: &DFSchema) -> bool {
    schema
        .index_of_column(column)
        .map(|index| is_jsonb_field(schema.field(index)))
        .unwrap_or(false)
}

fn schema_has_jsonb(schema: &DFSchema) -> bool {
    schema.fields().iter().any(|field| is_jsonb_field(field))
}

fn is_jsonb_field(field: &Field) -> bool {
    field
        .metadata()
        .get(JSONB_FIELD_METADATA_KEY)
        .map(String::as_str)
        == Some(JSONB_FIELD_METADATA_VALUE)
}

fn rewrite_uuid_string_predicates(plan: LogicalPlan) -> datafusion::common::Result<LogicalPlan> {
    plan.transform_up_with_subqueries(|plan| {
        if !matches!(&plan, LogicalPlan::Filter(_) | LogicalPlan::TableScan(_)) {
            return Ok(Transformed::no(plan));
        }
        let mut schema = merge_schema(&plan.inputs());
        if let LogicalPlan::TableScan(scan) = &plan {
            let source_schema = DFSchema::try_from_qualified_schema(
                scan.table_name.clone(),
                &scan.source.schema(),
            )?;
            schema.merge(&source_schema);
        }
        plan.map_expressions(|expr| {
            expr.transform_up(|expr| rewrite_uuid_string_comparison(expr, &schema))
        })
    })
    .map(|transformed| transformed.data)
}

fn rewrite_uuid_string_comparison(
    expr: Expr,
    schema: &DFSchema,
) -> datafusion::common::Result<Transformed<Expr>> {
    let Expr::BinaryExpr(mut binary) = expr else {
        return Ok(Transformed::no(expr));
    };
    if !matches!(binary.op, Operator::Eq | Operator::NotEq) {
        return Ok(Transformed::no(Expr::BinaryExpr(binary)));
    }

    let mut transformed = false;
    if let Some(literal) = converted_uuid_literal(&binary.left, &binary.right, schema)? {
        binary.right = Box::new(literal);
        transformed = true;
    }
    if let Some(literal) = converted_uuid_literal(&binary.right, &binary.left, schema)? {
        binary.left = Box::new(literal);
        transformed = true;
    }

    let expr = Expr::BinaryExpr(binary);
    Ok(if transformed {
        Transformed::yes(expr)
    } else {
        Transformed::no(expr)
    })
}

fn converted_uuid_literal(
    uuid_side: &Expr,
    literal_side: &Expr,
    schema: &DFSchema,
) -> datafusion::common::Result<Option<Expr>> {
    let Expr::Literal(scalar, metadata) = literal_side else {
        return Ok(None);
    };
    let value = match scalar {
        ScalarValue::Utf8(Some(value))
        | ScalarValue::LargeUtf8(Some(value))
        | ScalarValue::Utf8View(Some(value)) => value,
        _ => return Ok(None),
    };
    if uuid_side.get_type(schema)? != DataType::FixedSizeBinary(16) {
        return Ok(None);
    }

    let uuid = uuid::Uuid::parse_str(value).map_err(|error| {
        DataFusionError::Plan(format!("invalid UUID string literal {value:?}: {error}"))
    })?;
    Ok(Some(Expr::Literal(
        ScalarValue::FixedSizeBinary(16, Some(uuid.as_bytes().to_vec())),
        metadata.clone(),
    )))
}

struct PgTableProvider {
    engine: Arc<Engine>,
    table: String,
    entry: TableEntry,
    snapshot: Snapshot,
    _guard: Option<Arc<SnapshotGuard>>,
    schema: SchemaRef,
}

impl PgTableProvider {
    fn new(
        engine: Arc<Engine>,
        table: String,
        entry: TableEntry,
        snapshot: Snapshot,
        guard: Option<Arc<SnapshotGuard>>,
    ) -> Self {
        let schema = Arc::new(Schema::new(
            entry
                .columns
                .iter()
                .map(|column| {
                    arrow_field(column)
                        .unwrap_or_else(|| Field::new(&column.name, DataType::Utf8, true))
                })
                .collect::<Vec<_>>(),
        ));
        Self {
            engine,
            table,
            entry,
            snapshot,
            _guard: guard,
            schema,
        }
    }
}

impl fmt::Debug for PgTableProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PgTableProvider")
            .field("table", &self.entry.oid)
            .finish()
    }
}

#[async_trait]
impl TableProvider for PgTableProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::common::Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|expr| {
                if df_filter(expr, &self.entry).is_some() {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let filters = filters
            .iter()
            .filter_map(|expr| df_filter(expr, &self.entry))
            .collect::<Vec<_>>();
        Ok(Arc::new(PgExecutionPlan::new(
            Arc::clone(&self.engine),
            self.table.clone(),
            self.entry.clone(),
            self.snapshot.clone(),
            self._guard.clone(),
            ScanOptions {
                projection: projection.cloned(),
                limit,
            },
            filters,
        )))
    }
}

struct ScanOptions {
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
}

struct PgExecutionPlan {
    engine: Arc<Engine>,
    table: String,
    entry: TableEntry,
    snapshot: Snapshot,
    _guard: Option<Arc<SnapshotGuard>>,
    projection: Option<Vec<usize>>,
    filters: Vec<SqlFilter>,
    limit: Option<usize>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl PgExecutionPlan {
    fn new(
        engine: Arc<Engine>,
        table: String,
        entry: TableEntry,
        snapshot: Snapshot,
        guard: Option<Arc<SnapshotGuard>>,
        options: ScanOptions,
        filters: Vec<SqlFilter>,
    ) -> Self {
        let schema = projected_schema(&entry, options.projection.as_deref());
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            engine,
            table,
            entry,
            snapshot,
            _guard: guard,
            projection: options.projection,
            filters,
            limit: options.limit,
            schema,
            properties,
        }
    }

    async fn run(self) -> datafusion::common::Result<RecordBatch> {
        let Self {
            engine,
            table,
            entry,
            snapshot,
            projection,
            filters,
            limit,
            schema,
            ..
        } = self;
        let first = filters.first().cloned();
        let mut rows = crate::executor::execute(
            &engine, &snapshot, &table, &entry, &first, &None, &None, &None,
        )
        .map_err(df_error)?
        .rows;
        for filter in filters.iter().skip(1) {
            let predicate = filter_to_predicate(&engine, &entry, filter).map_err(df_error)?;
            let index = predicate.col_index();
            rows.retain(|(_, values)| predicate.matches(&values[index]));
        }
        if let Some(limit) = limit {
            rows.truncate(limit);
        }

        let indices = projection.unwrap_or_else(|| (0..entry.columns.len()).collect::<Vec<_>>());
        let arrays = indices
            .iter()
            .map(|index| arrow_column(&entry, &rows, *index))
            .collect::<datafusion::common::Result<Vec<_>>>()?;
        if rows.is_empty() {
            return Ok(RecordBatch::new_empty(schema));
        }
        let options = RecordBatchOptions::new().with_row_count(Some(rows.len()));
        RecordBatch::try_new_with_options(schema, arrays, &options).map_err(df_error)
    }
}

impl fmt::Debug for PgExecutionPlan {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PgExecutionPlan")
            .field("table", &self.entry.oid)
            .field("filters", &self.filters.len())
            .field("limit", &self.limit)
            .finish()
    }
}

impl ExecutionPlan for PgExecutionPlan {
    fn name(&self) -> &str {
        "PgHeapScan"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        self.properties.as_ref()
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(format!(
                "PgHeapScan has one partition, got {partition}"
            )));
        }
        let schema = self.schema();
        let this = self.clone();
        let stream = stream::once(this.run());
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

impl DisplayAs for PgExecutionPlan {
    fn fmt_as(
        &self,
        _format: DisplayFormatType,
        formatter: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        formatter.write_str("PgHeapScan")
    }
}

impl Clone for PgExecutionPlan {
    fn clone(&self) -> Self {
        Self {
            engine: Arc::clone(&self.engine),
            table: self.table.clone(),
            entry: self.entry.clone(),
            snapshot: self.snapshot.clone(),
            _guard: self._guard.clone(),
            projection: self.projection.clone(),
            filters: self.filters.clone(),
            limit: self.limit,
            schema: Arc::clone(&self.schema),
            properties: Arc::clone(&self.properties),
        }
    }
}

fn arrow_field(column: &crate::engine::ColumnDef) -> Option<Field> {
    let data_type = match column.col_type {
        ColumnType::Int4 => DataType::Int32,
        ColumnType::Int8 => DataType::Int64,
        ColumnType::Float8 => DataType::Float64,
        ColumnType::Timestamptz => DataType::Timestamp(TimeUnit::Microsecond, None),
        ColumnType::Uuid => DataType::FixedSizeBinary(16),
        ColumnType::Text => DataType::Utf8,
        ColumnType::Bytea => DataType::Binary,
        // Utf8 is only the Arrow transport. Field metadata preserves JSONB's
        // engine type so result decoding cannot silently turn it into TEXT.
        ColumnType::Jsonb => DataType::Utf8,
        ColumnType::Vector(_) => {
            DataType::List(Arc::new(Field::new("item", DataType::Float32, true)))
        }
    };
    let field = Field::new(&column.name, data_type, true);
    Some(if column.col_type == ColumnType::Jsonb {
        field.with_metadata(HashMap::from([(
            JSONB_FIELD_METADATA_KEY.to_string(),
            JSONB_FIELD_METADATA_VALUE.to_string(),
        )]))
    } else {
        field
    })
}

fn projected_schema(entry: &TableEntry, projection: Option<&[usize]>) -> SchemaRef {
    let indices = projection
        .map(|indices| indices.to_vec())
        .unwrap_or_else(|| (0..entry.columns.len()).collect());
    Arc::new(Schema::new(
        indices
            .into_iter()
            .filter_map(|index| entry.columns.get(index).and_then(arrow_field))
            .collect::<Vec<_>>(),
    ))
}

fn arrow_column(
    entry: &TableEntry,
    rows: &[(pg_storage::types::Tid, Vec<Value>)],
    index: usize,
) -> datafusion::common::Result<ArrayRef> {
    let column = entry
        .columns
        .get(index)
        .ok_or_else(|| df_error(format!("projection column {index} is out of range")))?;
    match column.col_type {
        ColumnType::Int4 => Ok(Arc::new(Int32Array::from(
            rows.iter()
                .map(|(_, values)| {
                    values
                        .get(index)
                        .ok_or_else(|| df_error(format!("row has no column {index}")))?
                        .as_ref()
                        .map(|datum| match datum {
                            Datum::Int4(value) => Ok(*value),
                            other => Err(df_error(format!("expected INT4, got {other:?}"))),
                        })
                        .transpose()
                })
                .collect::<datafusion::common::Result<Vec<_>>>()?,
        ))),
        ColumnType::Int8 => Ok(Arc::new(Int64Array::from(
            rows.iter()
                .map(|(_, values)| {
                    values
                        .get(index)
                        .ok_or_else(|| df_error(format!("row has no column {index}")))?
                        .as_ref()
                        .map(|datum| match datum {
                            Datum::Int8(value) => Ok(*value),
                            other => Err(df_error(format!("expected INT8, got {other:?}"))),
                        })
                        .transpose()
                })
                .collect::<datafusion::common::Result<Vec<_>>>()?,
        ))),
        ColumnType::Float8 => Ok(Arc::new(Float64Array::from(
            rows.iter()
                .map(|(_, values)| {
                    values
                        .get(index)
                        .ok_or_else(|| df_error(format!("row has no column {index}")))?
                        .as_ref()
                        .map(|datum| match datum {
                            Datum::Float8(value) => Ok(*value),
                            other => Err(df_error(format!("expected FLOAT8, got {other:?}"))),
                        })
                        .transpose()
                })
                .collect::<datafusion::common::Result<Vec<_>>>()?,
        ))),
        ColumnType::Timestamptz => Ok(Arc::new(TimestampMicrosecondArray::from(
            rows.iter()
                .map(|(_, values)| {
                    values
                        .get(index)
                        .ok_or_else(|| df_error(format!("row has no column {index}")))?
                        .as_ref()
                        .map(|datum| match datum {
                            Datum::Timestamptz(value) => Ok(*value),
                            other => Err(df_error(format!("expected TIMESTAMPTZ, got {other:?}"))),
                        })
                        .transpose()
                })
                .collect::<datafusion::common::Result<Vec<_>>>()?,
        ))),
        ColumnType::Uuid => {
            let values = rows
                .iter()
                .map(|(_, values)| {
                    values
                        .get(index)
                        .ok_or_else(|| df_error(format!("row has no column {index}")))?
                        .as_ref()
                        .map(|datum| match datum {
                            Datum::Uuid(value) => Ok(*value.as_bytes()),
                            other => Err(df_error(format!("expected UUID, got {other:?}"))),
                        })
                        .transpose()
                })
                .collect::<datafusion::common::Result<Vec<_>>>()?;
            Ok(Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(values.into_iter(), 16)
                    .map_err(df_error)?,
            ))
        }
        ColumnType::Text => Ok(Arc::new(StringArray::from(
            rows.iter()
                .map(|(_, values)| {
                    values
                        .get(index)
                        .ok_or_else(|| df_error(format!("row has no column {index}")))?
                        .as_ref()
                        .map(|datum| match datum {
                            Datum::Text(value) => Ok(value.clone()),
                            other => Err(df_error(format!("expected TEXT, got {other:?}"))),
                        })
                        .transpose()
                })
                .collect::<datafusion::common::Result<Vec<_>>>()?,
        ))),
        ColumnType::Bytea => Ok(Arc::new(BinaryArray::from(
            rows.iter()
                .map(|(_, values)| {
                    values
                        .get(index)
                        .ok_or_else(|| df_error(format!("row has no column {index}")))?
                        .as_ref()
                        .map(|datum| match datum {
                            Datum::Bytea(value) => Ok(value.as_slice()),
                            other => Err(df_error(format!("expected BYTEA, got {other:?}"))),
                        })
                        .transpose()
                })
                .collect::<datafusion::common::Result<Vec<_>>>()?,
        ))),
        ColumnType::Jsonb => Ok(Arc::new(StringArray::from(
            rows.iter()
                .map(|(_, values)| {
                    values
                        .get(index)
                        .ok_or_else(|| df_error(format!("row has no column {index}")))?
                        .as_ref()
                        .map(|datum| match datum {
                            // Heap decode already validated this JSONB. Avoid
                            // parsing it a second time on the Arrow scan path;
                            // `array_to_value` validates again at the result boundary.
                            Datum::Jsonb(value) => Ok(value.clone()),
                            other => Err(df_error(format!("expected JSONB, got {other:?}"))),
                        })
                        .transpose()
                })
                .collect::<datafusion::common::Result<Vec<_>>>()?,
        ))),
        ColumnType::Vector(_) => {
            let lists = rows
                .iter()
                .map(|(_, values)| {
                    values
                        .get(index)
                        .ok_or_else(|| df_error(format!("row has no column {index}")))?
                        .as_ref()
                        .map(|datum| match datum {
                            Datum::Vector(value) => Ok(value.clone()),
                            other => Err(df_error(format!("expected VECTOR, got {other:?}"))),
                        })
                        .transpose()
                })
                .collect::<datafusion::common::Result<Vec<_>>>()?;
            let lists = lists
                .into_iter()
                .map(|value| value.map(|vector| vector.into_iter().map(Some).collect::<Vec<_>>()))
                .collect::<Vec<_>>();
            Ok(Arc::new(
                ListArray::from_iter_primitive::<Float32Type, _, _>(lists),
            ))
        }
    }
}

use datafusion::arrow::datatypes::Float32Type;

fn batches_to_query_result(batches: &[RecordBatch], schema: &SchemaRef) -> Result<QueryResult> {
    let fields = schema.fields();
    let columns = fields
        .iter()
        .map(|field| field.name().to_string())
        .collect::<Vec<_>>();
    let mut rows = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            let mut values = Vec::with_capacity(batch.num_columns());
            for (index, array) in batch.columns().iter().enumerate() {
                let col_type = arrow_column_type(&fields[index])?;
                values.push(array_to_value(array, row, col_type)?);
            }
            rows.push(values);
        }
    }
    Ok(QueryResult::Rows { columns, rows })
}

fn arrow_column_type(field: &Field) -> Result<ColumnType> {
    if field
        .metadata()
        .get(JSONB_FIELD_METADATA_KEY)
        .map(String::as_str)
        == Some(JSONB_FIELD_METADATA_VALUE)
    {
        if field.data_type() != &DataType::Utf8 {
            return Err(EngineError::Unsupported(
                "JSONB Arrow field must use Utf8 transport".to_string(),
            ));
        }
        return Ok(ColumnType::Jsonb);
    }
    match field.data_type() {
        DataType::Int32 => Ok(ColumnType::Int4),
        DataType::Int64 => Ok(ColumnType::Int8),
        DataType::Float64 => Ok(ColumnType::Float8),
        DataType::Timestamp(TimeUnit::Microsecond, _) => Ok(ColumnType::Timestamptz),
        DataType::Utf8 => Ok(ColumnType::Text),
        DataType::FixedSizeBinary(16) => Ok(ColumnType::Uuid),
        DataType::Binary => Ok(ColumnType::Bytea),
        DataType::List(_) => Ok(ColumnType::Vector(0)),
        other => Err(EngineError::Unsupported(format!(
            "unsupported Arrow result type {other:?}"
        ))),
    }
}

fn array_to_value(array: &ArrayRef, row: usize, col_type: ColumnType) -> Result<Value> {
    if array.is_null(row) {
        return Ok(None);
    }
    match col_type {
        ColumnType::Int4 => {
            let array = array.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                EngineError::Corrupted("Arrow Int32 array downcast failed".to_string())
            })?;
            Ok(Some(Datum::Int4(array.value(row))))
        }
        ColumnType::Int8 => {
            let array = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                EngineError::Corrupted("Arrow Int64 array downcast failed".to_string())
            })?;
            Ok(Some(Datum::Int8(array.value(row))))
        }
        ColumnType::Float8 => {
            let array = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    EngineError::Corrupted("Arrow Float64 array downcast failed".to_string())
                })?;
            Ok(Some(Datum::Float8(array.value(row))))
        }
        ColumnType::Timestamptz => {
            let array = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| {
                    EngineError::Corrupted("Arrow timestamp array downcast failed".to_string())
                })?;
            Ok(Some(Datum::Timestamptz(array.value(row))))
        }
        ColumnType::Text => {
            let array = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    EngineError::Corrupted("Arrow Utf8 array downcast failed".to_string())
                })?;
            Ok(Some(Datum::Text(array.value(row).to_string())))
        }
        ColumnType::Jsonb => {
            let array = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    EngineError::Corrupted("Arrow JSONB Utf8 array downcast failed".to_string())
                })?;
            let value = array.value(row).to_string();
            serde_json::from_str::<serde_json::Value>(&value)
                .map_err(|error| EngineError::Corrupted(format!("invalid Arrow JSONB: {error}")))?;
            Ok(Some(Datum::Jsonb(value)))
        }
        ColumnType::Uuid => {
            let array = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| {
                    EngineError::Corrupted("Arrow UUID array downcast failed".to_string())
                })?;
            let uuid = uuid::Uuid::from_slice(array.value(row))
                .map_err(|error| EngineError::Corrupted(format!("invalid UUID bytes: {error}")))?;
            Ok(Some(Datum::Uuid(uuid)))
        }
        ColumnType::Bytea => {
            let array = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    EngineError::Corrupted("Arrow Binary array downcast failed".to_string())
                })?;
            Ok(Some(Datum::Bytea(array.value(row).to_vec())))
        }
        ColumnType::Vector(_) => {
            let array = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                EngineError::Corrupted("Arrow List array downcast failed".to_string())
            })?;
            let values = array.value(row);
            let values = values
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| {
                    EngineError::Corrupted("Arrow Float32 list downcast failed".to_string())
                })?;
            Ok(Some(Datum::Vector(
                (0..values.len()).map(|index| values.value(index)).collect(),
            )))
        }
    }
}

fn df_filter(expr: &Expr, entry: &TableEntry) -> Option<SqlFilter> {
    let Expr::BinaryExpr(BinaryExpr { left, op, right }) = expr else {
        return None;
    };
    let (column, scalar, reversed) = match (&**left, &**right) {
        (Expr::Column(column), Expr::Literal(scalar, _)) => (column, scalar, false),
        (Expr::Literal(scalar, _), Expr::Column(column)) => (column, scalar, true),
        _ => return None,
    };
    if entry.columns.iter().any(|candidate| {
        candidate.name.eq_ignore_ascii_case(&column.name) && candidate.col_type == ColumnType::Jsonb
    }) {
        // JSONB has no comparison or JSON-specific predicate pushdown in this
        // release. Utf8 is only the Arrow transport, never a TEXT coercion.
        return None;
    }
    let op = match (op, reversed) {
        (Operator::Eq, _) => CmpOp::Eq,
        (Operator::Lt, false) => CmpOp::Lt,
        (Operator::Gt, false) => CmpOp::Gt,
        (Operator::Lt, true) => CmpOp::Gt,
        (Operator::Gt, true) => CmpOp::Lt,
        _ => return None,
    };
    let value = match scalar {
        ScalarValue::Null => Literal::Null,
        ScalarValue::Int32(Some(value)) => Literal::Int(i64::from(*value)),
        ScalarValue::Int64(Some(value)) => Literal::Int(*value),
        ScalarValue::Float64(Some(value)) => Literal::Float(*value),
        ScalarValue::Utf8(Some(value)) | ScalarValue::LargeUtf8(Some(value)) => {
            Literal::Str(value.clone())
        }
        _ => return None,
    };
    Some(SqlFilter {
        column: column.name.clone(),
        op,
        value,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arrow_column_returns_execution_errors_for_mismatched_datums() {
        let cases = [
            (ColumnType::Int4, "INT4"),
            (ColumnType::Int8, "INT8"),
            (ColumnType::Float8, "FLOAT8"),
            (ColumnType::Timestamptz, "TIMESTAMPTZ"),
            (ColumnType::Uuid, "UUID"),
            (ColumnType::Text, "TEXT"),
            (ColumnType::Bytea, "BYTEA"),
            (ColumnType::Jsonb, "JSONB"),
            (ColumnType::Vector(2), "VECTOR"),
        ];
        for (col_type, expected) in cases {
            let entry = TableEntry {
                oid: pg_storage::types::Oid::INVALID,
                first_page: pg_storage::types::PageId::INVALID,
                columns: vec![crate::engine::ColumnDef {
                    name: "value".to_string(),
                    col_type,
                }],
                defaults: vec![None],
                toast: None,
            };
            let datum = if col_type == ColumnType::Int4 {
                Datum::Text("wrong".to_string())
            } else {
                Datum::Int4(1)
            };
            let rows = [(
                pg_storage::types::Tid {
                    page_id: pg_storage::types::PageId::INVALID,
                    slot_id: 0,
                },
                vec![Some(datum)],
            )];
            let error = arrow_column(&entry, &rows, 0).unwrap_err();
            match error {
                DataFusionError::Execution(message) => {
                    assert!(message.contains(expected), "{message}")
                }
                other => panic!("expected Execution error for {expected}: {other}"),
            }
        }
    }

    #[test]
    fn jsonb_predicates_are_not_exact_pushdowns() {
        let entry = TableEntry {
            oid: pg_storage::types::Oid::INVALID,
            first_page: pg_storage::types::PageId::INVALID,
            columns: vec![
                crate::engine::ColumnDef {
                    name: "payload".to_string(),
                    col_type: ColumnType::Jsonb,
                },
                crate::engine::ColumnDef {
                    name: "content".to_string(),
                    col_type: ColumnType::Text,
                },
            ],
            defaults: vec![None, None],
            toast: None,
        };
        let predicate = |column| {
            Expr::BinaryExpr(BinaryExpr {
                left: Box::new(Expr::Column(datafusion::common::Column::new_unqualified(
                    column,
                ))),
                op: Operator::Eq,
                right: Box::new(Expr::Literal(
                    ScalarValue::Utf8(Some("{}".to_string())),
                    None,
                )),
            })
        };
        assert!(df_filter(&predicate("payload"), &entry).is_none());
        assert!(df_filter(&predicate("content"), &entry).is_some());
    }

    #[test]
    fn native_jsonb_comparisons_are_rejected_from_field_metadata() {
        let schema = DFSchema::try_from_qualified_schema(
            "events",
            &Schema::new(vec![
                Field::new("payload", DataType::Utf8, true).with_metadata(HashMap::from([(
                    JSONB_FIELD_METADATA_KEY.to_string(),
                    JSONB_FIELD_METADATA_VALUE.to_string(),
                )])),
                Field::new("content", DataType::Utf8, true),
            ]),
        )
        .unwrap();
        let column = |name| Expr::Column(datafusion::common::Column::new(Some("events"), name));
        let predicate = |name| {
            Expr::BinaryExpr(BinaryExpr {
                left: Box::new(column(name)),
                op: Operator::Eq,
                right: Box::new(Expr::Literal(
                    ScalarValue::Utf8(Some("{}".to_string())),
                    None,
                )),
            })
        };
        assert!(validate_jsonb_expression(&predicate("payload"), &schema).is_err());
        assert!(validate_jsonb_expression(&predicate("content"), &schema).is_ok());
        assert!(validate_jsonb_projection(&column("payload"), &schema).is_ok());
        assert!(
            validate_jsonb_expression(&Expr::IsNotNull(Box::new(column("payload"))), &schema)
                .is_ok()
        );
    }

    #[test]
    fn non_utf8_jsonb_arrow_shape_is_unsupported_not_corruption() {
        let field = Field::new("payload", DataType::Int32, true).with_metadata(HashMap::from([(
            JSONB_FIELD_METADATA_KEY.to_string(),
            JSONB_FIELD_METADATA_VALUE.to_string(),
        )]));
        assert!(matches!(
            arrow_column_type(&field),
            Err(EngineError::Unsupported(_))
        ));
    }
}
