// DataFusion bindings

use async_trait::async_trait;
use base64::decode;
use bytes::Bytes;

use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::execution::context::SessionState;
use datafusion::execution::DiskManager;

use datafusion::logical_plan::plan::Projection;
use datafusion::logical_plan::{DFField, DropTable, Expr};

use crate::datafusion::parser::{DFParser, Statement as DFStatement};
use crate::wasm_udf::wasm::create_udf_from_wasm;
use futures::{StreamExt, TryStreamExt};

use hashbrown::HashMap;
use hex::encode;
use object_store::memory::InMemory;
use object_store::{path::Path, ObjectStore};
use sha2::Digest;
use sha2::Sha256;
use sqlparser::ast::{
    ColumnDef as SQLColumnDef, ColumnOption, DataType as SQLDataType, Ident, Statement,
    TableFactor, TableWithJoins,
};
use std::io::Read;

use std::iter::zip;
use std::sync::Arc;

pub use datafusion::error::{DataFusionError as Error, Result};
use datafusion::{
    arrow::{
        datatypes::{
            DataType, Field, Schema, SchemaRef, TimeUnit, DECIMAL_DEFAULT_SCALE,
            DECIMAL_MAX_PRECISION,
        },
        record_batch::RecordBatch,
    },
    datasource::file_format::{parquet::ParquetFormat, FileFormat},
    error::DataFusionError,
    execution::context::TaskContext,
    logical_plan::{
        plan::Extension, Column, CreateCatalog, CreateCatalogSchema, CreateMemoryTable,
        DFSchema, LogicalPlan, ToDFSchema,
    },
    parquet::{arrow::ArrowWriter, file::properties::WriterProperties},
    physical_plan::{
        coalesce_partitions::CoalescePartitionsExec, empty::EmptyExec,
        EmptyRecordBatchStream, ExecutionPlan, SendableRecordBatchStream, Statistics,
    },
    prelude::SessionContext,
    sql::{planner::SqlToRel, TableReference},
};

use crate::catalog::RegionCatalog;
use crate::data_types::{TableId, TableVersionId};
use crate::nodes::{CreateFunction, SeafowlExtensionNode};
use crate::provider::{RegionColumn, SeafowlRegion, SeafowlTable};
use crate::wasm_udf::data_types::{get_volatility, get_wasm_type, CreateFunctionDetails};
use crate::{
    catalog::TableCatalog,
    data_types::DatabaseId,
    nodes::{Assignment, CreateTable, Delete, Insert, Update},
    schema::Schema as SeafowlSchema,
};

// Copied from datafusion::sql::utils (private)

/// Returns a validated `DataType` for the specified precision and
/// scale
fn make_decimal_type(precision: Option<u64>, scale: Option<u64>) -> Result<DataType> {
    // postgres like behavior
    let (precision, scale) = match (precision, scale) {
        (Some(p), Some(s)) => (p as usize, s as usize),
        (Some(p), None) => (p as usize, 0),
        (None, Some(_)) => {
            return Err(DataFusionError::Internal(
                "Cannot specify only scale for decimal data type".to_string(),
            ))
        }
        (None, None) => (DECIMAL_MAX_PRECISION, DECIMAL_DEFAULT_SCALE),
    };

    // Arrow decimal is i128 meaning 38 maximum decimal digits
    if precision > DECIMAL_MAX_PRECISION || scale > precision {
        return Err(DataFusionError::Internal(format!(
            "For decimal(precision, scale) precision must be less than or equal to 38 and scale can't be greater than precision. Got ({}, {})",
            precision, scale
        )));
    } else {
        Ok(DataType::Decimal(precision, scale))
    }
}

// Normalize an identifier to a lowercase string unless the identifier is quoted.
fn normalize_ident(id: &Ident) -> String {
    match id.quote_style {
        Some(_) => id.value.clone(),
        None => id.value.to_ascii_lowercase(),
    }
}

// Copied from SqlRel (private there)
fn build_schema(columns: Vec<SQLColumnDef>) -> Result<Schema> {
    let mut fields = Vec::with_capacity(columns.len());

    for column in columns {
        let data_type = make_data_type(&column.data_type)?;
        let allow_null = column
            .options
            .iter()
            .any(|x| x.option == ColumnOption::Null);
        fields.push(Field::new(
            &normalize_ident(&column.name),
            data_type,
            allow_null,
        ));
    }

    Ok(Schema::new(fields))
}

/// Maps the SQL type to the corresponding Arrow `DataType`
fn make_data_type(sql_type: &SQLDataType) -> Result<DataType> {
    match sql_type {
        SQLDataType::BigInt(_) => Ok(DataType::Int64),
        SQLDataType::Int(_) => Ok(DataType::Int32),
        SQLDataType::SmallInt(_) => Ok(DataType::Int16),
        SQLDataType::Char(_) | SQLDataType::Varchar(_) | SQLDataType::Text => {
            Ok(DataType::Utf8)
        }
        SQLDataType::Decimal(precision, scale) => make_decimal_type(*precision, *scale),
        SQLDataType::Float(_) => Ok(DataType::Float32),
        SQLDataType::Real => Ok(DataType::Float32),
        SQLDataType::Double => Ok(DataType::Float64),
        SQLDataType::Boolean => Ok(DataType::Boolean),
        SQLDataType::Date => Ok(DataType::Date32),
        SQLDataType::Time => Ok(DataType::Time64(TimeUnit::Millisecond)),
        SQLDataType::Timestamp => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        _ => Err(DataFusionError::NotImplemented(format!(
            "The SQL data type {:?} is not implemented",
            sql_type
        ))),
    }
}

/// End copied functions

fn compound_identifier_to_column(ids: &[Ident]) -> Result<Column> {
    // OK, this one is partially taken from the planner for SQLExpr::CompoundIdentifier
    let mut var_names: Vec<_> = ids.iter().map(normalize_ident).collect();
    match (var_names.pop(), var_names.pop()) {
        (Some(name), Some(relation)) if var_names.is_empty() => Ok(Column {
            relation: Some(relation),
            name,
        }),
        _ => Err(DataFusionError::NotImplemented(format!(
            "Unsupported compound identifier '{:?}'",
            var_names,
        ))),
    }
}

/// Load the Statistics for a Parquet file in memory
async fn get_parquet_file_statistics_bytes(
    data: Bytes,
    schema: SchemaRef,
) -> Result<Statistics> {
    // DataFusion's methods for this are all private (see fetch_statistics / summarize_min_max)
    // and require the ObjectStore abstraction since they are normally used in the context
    // of a TableProvider sending a Range request to object storage to get min/max values
    // for a Parquet file. We are currently interested in getting statistics for a temporary
    // file we just wrote out, before uploading it to object storage.

    // A more fancy way to get this working would be making an ObjectStore
    // that serves as a write-through cache so that we can use it both when downloading and uploading
    // Parquet files.

    // Create a dummy object store pointing to our temporary directory (we don't know if
    // DiskManager will always put all files in the same dir)
    let dummy_object_store: Arc<dyn ObjectStore> = Arc::from(InMemory::new());
    let dummy_path = Path::from("data");
    dummy_object_store.put(&dummy_path, data).await.unwrap();

    let parquet = ParquetFormat::default();
    let meta = dummy_object_store
        .head(&dummy_path)
        .await
        .expect("Temporary object not found");
    let stats = parquet
        .infer_stats(&dummy_object_store, schema, &meta)
        .await?;
    Ok(stats)
}

/// Serialize data for the physical region index from Parquet file statistics
fn build_region_columns(
    region_stats: &Statistics,
    schema: SchemaRef,
) -> Vec<RegionColumn> {
    // TODO PhysicalRegionColumn might not be the right data structure here (lacks ID etc)
    match &region_stats.column_statistics {
        Some(column_statistics) => zip(column_statistics, schema.fields())
            .map(|(stats, column)| {
                let min_value = stats
                    .min_value
                    .as_ref()
                    .map(|m| m.to_string().as_bytes().into());
                let max_value = stats
                    .max_value
                    .as_ref()
                    .map(|m| m.to_string().as_bytes().into());

                RegionColumn {
                    name: Arc::from(column.name().to_string()),
                    r#type: Arc::from(column.data_type().to_json().to_string()),
                    min_value: Arc::new(min_value),
                    max_value: Arc::new(max_value),
                }
            })
            .collect(),
        None => schema
            .fields()
            .iter()
            .map(|column| RegionColumn {
                name: Arc::from(column.name().to_string()),
                r#type: Arc::from(column.data_type().to_json().to_string()),
                min_value: Arc::new(None),
                max_value: Arc::new(None),
            })
            .collect(),
    }
}

pub struct DefaultSeafowlContext {
    pub inner: SessionContext,
    pub table_catalog: Arc<dyn TableCatalog>,
    pub region_catalog: Arc<dyn RegionCatalog>,
    pub database: String,
    pub database_id: DatabaseId,
}

/// Create an ExecutionPlan that doesn't produce any results.
/// This is used for queries that are actually run before we produce the plan,
/// since they have to manipulate catalog metadata or use async to write to it.
fn make_dummy_exec() -> Arc<dyn ExecutionPlan> {
    Arc::new(EmptyExec::new(false, SchemaRef::new(Schema::empty())))
}

/// Execute a plan and upload the results to object storage as Parquet files, indexing them.
/// Partially taken from DataFusion's plan_to_parquet with some additions (file stats, using a DiskManager)
pub async fn plan_to_object_store(
    state: &SessionState,
    plan: &Arc<dyn ExecutionPlan>,
    store: Arc<dyn ObjectStore>,
    disk_manager: Arc<DiskManager>,
) -> Result<Vec<SeafowlRegion>> {
    let mut tasks = vec![];
    for i in 0..plan.output_partitioning().partition_count() {
        let physical = plan.clone();
        let task_ctx = Arc::new(TaskContext::from(state));
        let store = store.clone();

        let partition_file = disk_manager.create_tmp_file()?;
        // Maintain a second handle to the file (the first one is consumed by ArrowWriter)
        // We'll close this handle at the end of the task, dropping the file.
        let mut partition_file_handle = partition_file.reopen().map_err(|_| {
            DataFusionError::Execution("Error with temporary Parquet file".to_string())
        })?;

        // let partition_file_path = partition_file.path().to_owned();

        let writer_properties = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(
            partition_file,
            physical.schema(),
            Some(writer_properties.clone()),
        )?;
        let stream = physical.execute(i, task_ctx)?;

        let handle: tokio::task::JoinHandle<Result<SeafowlRegion>> =
            tokio::task::spawn(async move {
                stream
                    .map(|batch| writer.write(&batch?))
                    .try_collect()
                    .await
                    .map_err(DataFusionError::from)?;
                writer.close().map_err(DataFusionError::from).map(|_| ())?;

                // TODO: the object_store crate doesn't support multi-part uploads / uploading a file
                // from a local path. This means we have to read the file back into memory in full.
                // https://github.com/influxdata/object_store_rs/issues/9
                //
                // Another implication is that we could just keep everything in memory (point ArrowWriter to a byte buffer,
                // call get_parquet_file_statistics on that, upload the file) and run the output routine for each partition
                // sequentially.

                let mut buf = Vec::new();
                partition_file_handle
                    .read_to_end(&mut buf)
                    .expect("Error reading the temporary file");
                let data = Bytes::from(buf);

                // Index the Parquet file (get its min-max values)
                let region_stats =
                    get_parquet_file_statistics_bytes(data.clone(), physical.schema())
                        .await?;

                let columns = build_region_columns(&region_stats, physical.schema());

                let mut hasher = Sha256::new();
                hasher.update(&data);
                let hash_str = encode(hasher.finalize());
                let object_storage_id = hash_str + ".parquet";
                store
                    .put(&Path::from(object_storage_id.clone()), data)
                    .await?;

                let region = SeafowlRegion {
                    object_storage_id: Arc::from(object_storage_id),
                    row_count: region_stats
                        .num_rows
                        .expect("Error counting rows in the written file")
                        .try_into()
                        .expect("row count greater than 2147483647"),
                    columns: Arc::new(columns),
                };

                Ok(region)
            });
        tasks.push(handle);
    }

    futures::future::join_all(tasks)
        .await
        .into_iter()
        .map(|x| x.unwrap_or_else(|e| Err(DataFusionError::External(Box::new(e)))))
        .collect()
}

#[async_trait]
pub trait SeafowlContext: Send + Sync {
    /// Reload the context to apply / pick up new schema changes
    async fn reload_schema(&self);
    async fn create_logical_plan(&self, sql: &str) -> Result<LogicalPlan>;
    async fn plan_query(&self, sql: &str) -> Result<Arc<dyn ExecutionPlan>>;
    async fn create_physical_plan(
        &self,
        plan: &LogicalPlan,
    ) -> Result<Arc<dyn ExecutionPlan>>;
    async fn collect(
        &self,
        physical_plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Vec<RecordBatch>>;
}

impl DefaultSeafowlContext {
    pub fn inner(&self) -> &SessionContext {
        &self.inner
    }
    /// Resolve a table reference into a Seafowl table
    fn try_get_seafowl_table(
        &self,
        table_name: impl Into<String> + std::fmt::Debug,
    ) -> Result<SeafowlTable> {
        let table_name = table_name.into();
        let table_ref = TableReference::from(table_name.as_str());
        let resolved_ref = table_ref.resolve(&self.database, "public");
        let table_provider = self
            .inner
            .catalog(resolved_ref.catalog)
            .ok_or_else(|| {
                Error::Plan(format!(
                    "failed to resolve catalog: {}",
                    resolved_ref.catalog
                ))
            })?
            .schema(resolved_ref.schema)
            .ok_or_else(|| {
                Error::Plan(format!("failed to resolve schema: {}", resolved_ref.schema))
            })?
            .table(resolved_ref.table)
            .ok_or_else(|| {
                Error::Plan(format!(
                    "'{}.{}.{}' not found",
                    resolved_ref.catalog, resolved_ref.schema, resolved_ref.table
                ))
            })?;
        let seafowl_table = match table_provider.as_any().downcast_ref::<SeafowlTable>() {
            Some(seafowl_table) => Ok(seafowl_table),
            None => Err(Error::Plan(format!(
                "'{:?}' is a read-only table",
                table_name
            ))),
        }?;
        Ok(seafowl_table.clone())
    }

    async fn exec_create_table(
        &self,
        name: &str,
        schema: &Arc<DFSchema>,
    ) -> Result<(TableId, TableVersionId)> {
        let table_ref = TableReference::from(name);
        let resolved_ref = table_ref.resolve(&self.database, "public");
        let schema_name = resolved_ref.schema;
        let table_name = resolved_ref.table;

        let sf_schema = SeafowlSchema {
            arrow_schema: Arc::new(schema.as_ref().into()),
        };
        let collection_id = self
            .table_catalog
            .get_collection_id_by_name(&self.database, schema_name)
            .await
            .ok_or_else(|| {
                Error::Plan(format!("Schema {:?} does not exist!", schema_name))
            })?;
        Ok(self
            .table_catalog
            .create_table(collection_id, table_name, sf_schema)
            .await)
    }

    async fn execute_stream(
        &self,
        physical_plan: Arc<dyn ExecutionPlan>,
    ) -> Result<SendableRecordBatchStream> {
        match physical_plan.output_partitioning().partition_count() {
            0 => Ok(Box::pin(EmptyRecordBatchStream::new(
                physical_plan.schema(),
            ))),
            1 => self.execute_stream_partitioned(&physical_plan, 0).await,
            _ => {
                let plan: Arc<dyn ExecutionPlan> =
                    Arc::new(CoalescePartitionsExec::new(physical_plan));
                self.execute_stream_partitioned(&plan, 0).await
            }
        }
    }

    async fn execute_stream_partitioned(
        &self,
        physical_plan: &Arc<dyn ExecutionPlan>,
        partition: usize,
    ) -> Result<SendableRecordBatchStream> {
        let task_context = Arc::new(TaskContext::from(self.inner()));
        physical_plan.execute(partition, task_context)
    }
}

#[async_trait]
impl SeafowlContext for DefaultSeafowlContext {
    async fn reload_schema(&self) {
        // TODO: this loads all collection/table names into memory, so creating tables within the same
        // session won't reflect the changes without recreating the context.
        self.inner.register_catalog(
            &self.database,
            Arc::new(self.table_catalog.load_database(self.database_id).await),
        );
    }

    async fn create_logical_plan(&self, sql: &str) -> Result<LogicalPlan> {
        let mut statements = DFParser::parse_sql(sql)?;

        if statements.len() != 1 {
            return Err(Error::NotImplemented(
                "The context currently only supports a single SQL statement".to_string(),
            ));
        }

        let state = self.inner.state.read().clone();
        let query_planner = SqlToRel::new(&state);

        match statements.pop_front().unwrap() {
            DFStatement::Statement(s) => match *s {
                // Delegate SELECT / EXPLAIN to the basic DataFusion logical planner
                // (though note EXPLAIN [our custom query] will mean we have to implement EXPLAIN ourselves)
                Statement::Explain { .. }
                | Statement::Query { .. }
                | Statement::ShowVariable { .. }
                | Statement::ShowColumns { .. }
                | Statement::CreateView { .. }
                | Statement::CreateSchema { .. }
                | Statement::CreateDatabase { .. }
                | Statement::Drop { .. } => query_planner.sql_statement_to_plan(*s),

                // CREATE TABLE (create empty table with columns)
                Statement::CreateTable {
                    query: None,
                    name,
                    columns,
                    constraints,
                    table_properties,
                    with_options,
                    if_not_exists,
                    or_replace: _,
                    ..
                } if constraints.is_empty()
                    && table_properties.is_empty()
                    && with_options.is_empty() =>
                {
                    let cols = build_schema(columns)?;
                    Ok(LogicalPlan::Extension(Extension {
                        node: Arc::new(SeafowlExtensionNode::CreateTable(CreateTable {
                            schema: cols.to_dfschema_ref()?,
                            name: name.to_string(),
                            if_not_exists,
                            output_schema: Arc::new(DFSchema::empty())
                        })),
                    }))
                }

                // Other CREATE TABLE: SqlToRel only allows CreateTableAs statements and makes
                // a CreateMemoryTable node. We're fine with that, but we'll execute it differently.
                Statement::CreateTable { .. } => query_planner.sql_statement_to_plan(*s),

                // This DML is defined by us
                Statement::Insert {
                    table_name,
                    columns,
                    source,
                    ..
                } => {
                    let table_name = table_name.to_string();

                    let seafowl_table = self.try_get_seafowl_table(table_name)?;

                    // Get a list of columns we're inserting into and schema we
                    // have to cast `source` into
                    // INSERT INTO table (col_3, col_4) VALUES (1, 2)
                    let table_schema = seafowl_table.schema.arrow_schema.clone().to_dfschema()?;

                    let target_schema = if columns.is_empty() {
                        // Empty means we're inserting into all columns of the table
                        seafowl_table.schema.arrow_schema.clone().to_dfschema()?
                    } else {
                        let fields = columns.iter().map(|c|
                            Ok(table_schema.field_with_unqualified_name(&normalize_ident(c))?.clone())).collect::<Result<Vec<DFField>>>()?;
                        DFSchema::new_with_metadata(fields, table_schema.metadata().clone())?
                    };

                    let plan = query_planner.query_to_plan(*source, &mut HashMap::new())?;

                    // Check the length
                    if plan.schema().fields().len() != target_schema.fields().len() {
                        return Err(Error::Plan(
                            format!("Unexpected number of columns in VALUES: expected {:?}, got {:?}", target_schema.fields().len(), plan.schema().fields().len())
                        ))
                    }

                    // Check we can cast from the values in the INSERT to the actual table schema
                    target_schema.check_arrow_schema_type_compatible(&((**plan.schema()).clone().into()))?;

                    // Make a projection around the input plan to rename the columns / change the schema
                    // (it doesn't seem to actually do casts at runtime, but ArrowWriter should forcefully
                    // cast the columns when we're writing to Parquet)

                    // TODO: we might need to pad out the result with NULL columns so that it has _exactly_
                    // the same shape as the rest of the table
                    let plan = LogicalPlan::Projection(Projection {
                        expr: target_schema.fields().iter().zip(plan.schema().field_names()).map(|(table_field, query_field_name)| {
                            // Generate CAST (source_col AS table_col_type) AS table_col
                            // If the type is the same, this will be optimized out.
                            Expr::Cast{
                                expr: Box::new(Expr::Column(Column::from_name(query_field_name))),
                                data_type: table_field.data_type().clone()
                            }.alias(table_field.name())
                        }).collect(),
                        input: Arc::new(plan),
                        schema: Arc::new(target_schema),
                        alias: None,
                    });

                    Ok(LogicalPlan::Extension(Extension {
                        node: Arc::new(SeafowlExtensionNode::Insert(Insert {
                            // TODO we might not need the whole table (we're currently cloning it in
                            // try_get_seafowl_table)
                            table: Arc::new(seafowl_table),
                            input: Arc::new(plan),
                            output_schema: Arc::new(DFSchema::empty())
                        })),
                    }))
                }
                Statement::Update {
                    table: TableWithJoins {relation: TableFactor::Table { name, alias: None, args: None, with_hints }, joins },
                    assignments,
                    from: None,
                    selection,
                }
                // We only support the most basic form of UPDATE (no aliases or FROM or joins)
                    if with_hints.is_empty() && joins.is_empty()
                => {
                    // Scan through the original table (with selection) and:
                    // SELECT [for each col, "col AS col" if not an assignment, otherwise "expr AS col"]
                    //   FROM original_table WHERE [selection]
                    // Somehow also split the result by existing partition boundaries and leave unchanged partitions alone

                    // TODO we need to load the table object here in order to validate the UPDATE clauses
                    let table_schema: DFSchema = DFSchema::empty();

                    let selection_expr = match selection {
                        None => None,
                        Some(expr) => Some(query_planner.sql_to_rex(expr, &table_schema, &mut HashMap::new())?),
                    };

                    let assignments_expr = assignments.iter().map(|a| {
                        Ok(Assignment { column: compound_identifier_to_column(&a.id)?, expr: query_planner.sql_to_rex(a.value.clone(), &table_schema, &mut HashMap::new())? })
                    }).collect::<Result<Vec<Assignment>>>()?;

                    Ok(LogicalPlan::Extension(Extension {
                        node: Arc::new(SeafowlExtensionNode::Update(Update {
                            name: name.to_string(),
                            selection: selection_expr,
                            assignments: assignments_expr,
                            output_schema: Arc::new(DFSchema::empty())
                        })),
                    }))
                }
                Statement::Delete {
                    table_name,
                    selection,
                } => {
                    // Same as Update but we just filter out the selection
                    let table_schema: DFSchema = DFSchema::empty();

                    let selection_expr = match selection {
                        None => None,
                        Some(expr) => Some(query_planner.sql_to_rex(expr, &table_schema, &mut HashMap::new())?),
                    };

                    Ok(LogicalPlan::Extension(Extension {
                        node: Arc::new(SeafowlExtensionNode::Delete(Delete {
                            name: table_name.to_string(),
                            selection: selection_expr,
                            output_schema: Arc::new(DFSchema::empty())
                        })),
                    }))
                },
                Statement::CreateFunction {
                    temporary: false,
                    name,
                    class_name,
                    using: None,
                } => {
                    // We abuse the fact that in CREATE FUNCTION AS [class_name], class_name can be an arbitrary string
                    // and so we can get the user to put some JSON in there
                    let function_details: CreateFunctionDetails = serde_json::from_str(&class_name)
                        .map_err(|e| {
                            Error::Execution(format!("Error parsing UDF details: {:?}", e))
                        })?;

                        Ok(LogicalPlan::Extension(Extension {
                            node: Arc::new(SeafowlExtensionNode::CreateFunction(CreateFunction {
                                name: name.to_string(),
                                details: function_details,
                                output_schema: Arc::new(DFSchema::empty())
                            })),
                        }))
                    }
                _ => Err(Error::NotImplemented(format!(
                    "Unsupported SQL statement: {:?}",
                    sql
                ))),
            },
            DFStatement::DescribeTable(s) => query_planner.describe_table_to_plan(s),
            // Stub out the standard DataFusion CREATE EXTERNAL TABLE statements since we don't support them
            DFStatement::CreateExternalTable(_) => {
                return Err(Error::NotImplemented(format!(
                    "Unsupported SQL statement: {:?}",
                    sql
                )))
            }
        }
    }

    async fn plan_query(&self, sql: &str) -> Result<Arc<dyn ExecutionPlan>> {
        let logical_plan = self.create_logical_plan(sql).await?;

        // Similarly to DataFrame::sql, run certain logical plans outside of the actual execution flow
        // and produce a dummy physical plan instead
        match logical_plan {
            LogicalPlan::CreateExternalTable(_) => {
                // We're not supposed to reach this since we filtered it out above
                panic!("No plan for CreateExternalTable");
            }
            LogicalPlan::CreateCatalogSchema(CreateCatalogSchema {
                schema_name,
                if_not_exists: _,
                schema: _,
            }) => {
                // CREATE SCHEMA
                // Create a schema and register it
                self.table_catalog
                    .create_collection(self.database_id, &schema_name)
                    .await;
                Ok(make_dummy_exec())
            }
            LogicalPlan::CreateCatalog(CreateCatalog {
                catalog_name,
                if_not_exists: _,
                schema: _,
            }) => {
                // CREATE DATABASE
                self.table_catalog.create_database(&catalog_name).await;
                Ok(make_dummy_exec())
            }
            // TODO DROP DATABASE / SCHEMA
            LogicalPlan::CreateMemoryTable(CreateMemoryTable {
                name,
                input,
                if_not_exists: _,
                or_replace: _,
            }) => {
                // This is actually CREATE TABLE AS
                let physical = self.create_physical_plan(&input).await?;

                // Execute the plan and write it out to temporary Parquet files.
                let disk_manager = self.inner.runtime_env().disk_manager.clone();

                let object_store_url = ObjectStoreUrl::parse("seafowl://").unwrap();
                let store = self
                    .inner
                    .runtime_env()
                    .object_store(object_store_url.clone())?;

                let regions = plan_to_object_store(
                    &self.inner.state(),
                    &physical,
                    store,
                    disk_manager,
                )
                .await?;

                // Create an empty table with an empty version
                let (_, table_version_id) = self
                    .exec_create_table(&name, &physical.schema().to_dfschema_ref()?)
                    .await?;

                // Attach the regions to the empty table
                let region_ids = self.region_catalog.create_regions(regions).await;
                self.region_catalog
                    .append_regions_to_table(region_ids, table_version_id)
                    .await;

                Ok(make_dummy_exec())
            }
            LogicalPlan::DropTable(DropTable {
                name,
                if_exists: _,
                schema: _,
            }) => {
                // DROP TABLE
                let table = self.try_get_seafowl_table(name)?;
                self.table_catalog.drop_table(table.table_id).await;
                Ok(make_dummy_exec())
            }
            LogicalPlan::CreateView(_) => {
                // CREATE VIEW
                Ok(make_dummy_exec())
            }
            LogicalPlan::Extension(Extension { ref node }) => {
                // Other custom nodes we made like CREATE TABLE/INSERT/UPDATE/DELETE/ALTER
                match SeafowlExtensionNode::from_dynamic(node) {
                    Some(sfe_node) => match sfe_node {
                        SeafowlExtensionNode::CreateTable(CreateTable {
                            schema,
                            name,
                            ..
                        }) => {
                            self.exec_create_table(name, schema).await?;

                            Ok(make_dummy_exec())
                        }
                        SeafowlExtensionNode::Insert(Insert { table, input, .. }) => {
                            let physical = self.create_physical_plan(input).await?;

                            // Execute the plan and write it out to temporary Parquet files.
                            let disk_manager =
                                self.inner.runtime_env().disk_manager.clone();

                            let object_store_url =
                                ObjectStoreUrl::parse("seafowl://").unwrap();
                            let store = self
                                .inner
                                .runtime_env()
                                .object_store(object_store_url.clone())?;

                            let regions = plan_to_object_store(
                                &self.inner.state(),
                                &physical,
                                store,
                                disk_manager,
                            )
                            .await?;

                            // Duplicate the table version into a new one
                            let new_version_id = self
                                .table_catalog
                                .create_new_table_version(table.table_version_id)
                                .await;

                            // Attach the regions to the table
                            let region_ids =
                                self.region_catalog.create_regions(regions).await;
                            self.region_catalog
                                .append_regions_to_table(region_ids, new_version_id)
                                .await;

                            Ok(make_dummy_exec())
                        }
                        SeafowlExtensionNode::Update(_) => {
                            // Some kind of a node that combines Filter + Projection?
                            //
                            //
                            //
                            //    Union
                            //     |  |
                            // Filter Projection
                            //    |    |
                            //    |   Filter
                            //    |    |
                            // TableScan

                            // Pass an "initial partition id" in the batch (or ask our TableScan for what each
                            // partition is pointing to)
                            //
                            // If a partition is missing: it stayed the same
                            // If a partition didn't change (the filter didn't match anything): it stayed the same
                            //    - but how do we find that out? we need to read through the whole partition to
                            //      make sure nothing get updated, so that means we need to buffer the result
                            //      on disk; could we just hash it at the end to see if it changed?
                            // If a partition is empty: delete it
                            //
                            // So:
                            //
                            //   - do a scan through Case (projection) around TableScan (with the filter)
                            //   - for each output partition:
                            //     - gather it in a temporary file and hash it
                            //     - find out from the ExecutionPlan which original file it belonged to
                            //     - if it's the same: do nothing
                            //     - if it's changed: replace that table version object; upload it
                            //     - files corresponding to partitions that never got output won't get updated
                            //
                            // This also assumes one Parquet file <> one partition

                            // - Duplicate the table (new version)
                            // - replace regions that are changed (but we don't know the table_region i.e. which entry to
                            // repoint to our new region)?
                            Ok(make_dummy_exec())
                        }
                        SeafowlExtensionNode::Delete(_) => {
                            // - Duplicate the table (new version)
                            // - Similar to UPDATE, but just a filter

                            // upload new files
                            // replace regions (sometimes we delete them)

                            // really we want to be able to load all regions + cols for a table and then
                            // write that thing back to the db (set table regions)
                            Ok(make_dummy_exec())
                        }
                        SeafowlExtensionNode::CreateFunction(CreateFunction {
                            name,
                            details,
                            output_schema: _,
                        }) => {
                            let function_code = decode(&details.data).map_err(|e| {
                                Error::Execution(format!(
                                    "Error decoding the UDF: {:?}",
                                    e
                                ))
                            })?;

                            let _function = create_udf_from_wasm(
                                name,
                                &function_code,
                                &details.entrypoint,
                                details.input_types.iter().map(get_wasm_type).collect(),
                                get_wasm_type(&details.return_type),
                                get_volatility(&details.volatility),
                            )?;
                            // TODO we don't persist the function here in the database, so it'll get
                            // deleted every time we recreate the context
                            // also this requires &mut self
                            // self.inner.register_udf(function);
                            Ok(make_dummy_exec())
                        }
                    },
                    None => self.inner.create_physical_plan(&logical_plan).await,
                }
            }
            _ => self.inner.create_physical_plan(&logical_plan).await,
        }
    }

    async fn create_physical_plan(
        &self,
        plan: &LogicalPlan,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let physical_plan = self.inner.create_physical_plan(plan).await?;
        Ok(physical_plan)
    }

    // Copied from DataFusion's physical_plan
    async fn collect(
        &self,
        physical_plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Vec<RecordBatch>> {
        let stream = self.execute_stream(physical_plan).await?;
        stream.err_into().try_collect().await
    }
}

#[cfg(test)]
pub mod test_utils {
    use std::sync::Arc;

    use mockall::predicate;
    use object_store::memory::InMemory;

    use crate::{
        catalog::{MockRegionCatalog, MockTableCatalog, TableCatalog},
        provider::{SeafowlCollection, SeafowlDatabase},
    };

    use datafusion::{arrow::datatypes::{
        DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    }, prelude::SessionConfig};

    use std::collections::HashMap as StdHashMap;

    use super::*;

    pub fn make_session() -> SessionContext {
        let session_config = SessionConfig::new().with_information_schema(true);

        let context = SessionContext::with_config(session_config);
        let object_store = Arc::new(InMemory::new());
        context
            .runtime_env()
            .register_object_store("seafowl", "", object_store);
        context
    }

    pub async fn mock_context() -> DefaultSeafowlContext {
        mock_context_with_catalog_assertions(|_| (), |_| ()).await
    }

    pub async fn mock_context_with_catalog_assertions<FR, FT>(
        mut setup_region_catalog: FR,
        mut setup_table_catalog: FT,
    ) -> DefaultSeafowlContext
    where
        FR: FnMut(&mut MockRegionCatalog),
        FT: FnMut(&mut MockTableCatalog),
    {
        let session = make_session();
        let arrow_schema = ArrowSchema::new(vec![
            ArrowField::new("date", ArrowDataType::Date64, false),
            ArrowField::new("value", ArrowDataType::Float64, false),
        ]);

        let mut region_catalog = MockRegionCatalog::new();

        region_catalog
            .expect_load_table_regions()
            .with(predicate::eq(1))
            .returning(|_| {
                vec![SeafowlRegion {
                    object_storage_id: Arc::from("some-file.parquet"),
                    row_count: 3,
                    columns: Arc::new(vec![]),
                }]
            });

        setup_region_catalog(&mut region_catalog);

        let region_catalog_ptr = Arc::new(region_catalog);

        let singleton_table = SeafowlTable {
            name: Arc::from("some_table"),
            schema: Arc::new(SeafowlSchema {
                arrow_schema: Arc::new(arrow_schema.clone()),
            }),
            table_id: 0,
            table_version_id: 0,
            catalog: region_catalog_ptr.clone(),
        };
        let tables =
            StdHashMap::from([(Arc::from("some_table"), Arc::from(singleton_table))]);
        let collections = StdHashMap::from([(
            Arc::from("testcol"),
            Arc::from(SeafowlCollection {
                name: Arc::from("testcol"),
                tables,
            }),
        )]);

        let mut table_catalog = MockTableCatalog::new();
        table_catalog
            .expect_load_database()
            .with(predicate::eq(0))
            .returning(move |_| SeafowlDatabase {
                name: Arc::from("testdb"),
                collections: collections.clone(),
            });

        session
            .register_catalog("testdb", Arc::new(table_catalog.load_database(0).await));

        setup_table_catalog(&mut table_catalog);

        let object_store = Arc::new(InMemory::new());
        session
            .runtime_env()
            .register_object_store("seafowl", "", object_store);

        DefaultSeafowlContext {
            inner: session,
            table_catalog: Arc::new(table_catalog),
            region_catalog: region_catalog_ptr,
            database: "testdb".to_string(),
            database_id: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::execution::disk_manager::DiskManagerConfig;
    use mockall::predicate;
    use object_store::memory::InMemory;

    use crate::context::test_utils::mock_context_with_catalog_assertions;

    use datafusion::assert_batches_eq;

    use super::*;

    use super::test_utils::mock_context;

    #[tokio::test]
    async fn test_plan_to_object_storage() {
        let sf_context = mock_context().await;

        // Make a SELECT VALUES(...) query
        let execution_plan = sf_context
            .plan_query(
                r#"
                SELECT * FROM (VALUES
                    ('2022-01-01', 42, 'one'),
                    ('2022-01-02', 12, 'two'))
                AS t(timestamp, integer, varchar);"#,
            )
            .await
            .unwrap();

        let object_store = Arc::new(InMemory::new());
        let disk_manager = DiskManager::try_new(DiskManagerConfig::new()).unwrap();
        let regions = plan_to_object_store(
            &sf_context.inner.state(),
            &execution_plan,
            object_store,
            disk_manager,
        )
        .await
        .unwrap();

        assert_eq!(regions.len(), 1);

        let region = regions.get(0).unwrap();
        // TODO figure out why:
        //   - timestamp didn't get converted
        //   - utf8 didn't get indexed
        assert_eq!(
            *region,
            SeafowlRegion {
                object_storage_id: Arc::from(
                    "d52a8584a60b598ad0ffa11d185c3ca800b7ddb47ea448d0072b6bf7a5a209e1.parquet"
                        .to_string()
                ),
                row_count: 2,
                columns: Arc::new(vec![
                    RegionColumn {
                        name: Arc::from("timestamp".to_string()),
                        r#type: Arc::from("{\"name\":\"utf8\"}".to_string()),
                        min_value: Arc::new(None),
                        max_value: Arc::new(None)
                    },
                    RegionColumn {
                        name: Arc::from("integer".to_string()),
                        r#type: Arc::from(
                            "{\"name\":\"int\",\"bitWidth\":64,\"isSigned\":true}".to_string()
                        ),
                        min_value: Arc::new(Some([49, 50].to_vec())),
                        max_value: Arc::new(Some([52, 50].to_vec()))
                    },
                    RegionColumn {
                        name: Arc::from("varchar".to_string()),
                        r#type: Arc::from("{\"name\":\"utf8\"}".to_string()),
                        min_value: Arc::new(None),
                        max_value: Arc::new(None)
                    }
                ])
            }
        );
    }

    #[tokio::test]
    async fn test_plan_insert_normal() {
        let sf_context = mock_context().await;

        let plan = sf_context
            .create_logical_plan(
                "INSERT INTO testcol.some_table (date, value) VALUES('2022-01-01T12:00:00', 42)",
            )
            .await
            .unwrap();

        assert_eq!(
            format!("{:?}", plan),
            "Insert: some_table\
            \n  Projection: CAST(#column1 AS Date64) AS date, CAST(#column2 AS Float64) AS value\
            \n    Values: (Utf8(\"2022-01-01T12:00:00\"), Int64(42))"
        );
    }

    #[tokio::test]
    async fn test_plan_insert_renaming() {
        let sf_context = mock_context().await;

        let plan = sf_context
            // TODO: we need to do FROM testdb since it's not set as a default?
            .create_logical_plan(
                "INSERT INTO testcol.some_table (date, value)
                SELECT \"date\" AS my_date, \"value\" AS my_value FROM testdb.testcol.some_table",
            )
            .await
            .unwrap();

        assert_eq!(format!("{:?}", plan), "Insert: some_table\
        \n  Projection: CAST(#my_date AS Date64) AS date, CAST(#my_value AS Float64) AS value\
        \n    Projection: #testdb.testcol.some_table.date AS my_date, #testdb.testcol.some_table.value AS my_value\
        \n      TableScan: testdb.testcol.some_table");
    }

    #[tokio::test]
    async fn test_plan_insert_all() {
        let sf_context = mock_context().await;

        let plan = sf_context
            .create_logical_plan(
                "INSERT INTO testcol.some_table VALUES('2022-01-01T12:00:00', 42)",
            )
            .await
            .unwrap();

        assert_eq!(
            format!("{:?}", plan),
            "Insert: some_table\
            \n  Projection: CAST(#column1 AS Date64) AS date, CAST(#column2 AS Float64) AS value\
            \n    Values: (Utf8(\"2022-01-01T12:00:00\"), Int64(42))"
        );
    }

    #[tokio::test]
    async fn test_plan_insert_type_mismatch() {
        let sf_context = mock_context().await;

        // Try inserting a timestamp into a number (note this will work fine for inserting
        // e.g. Utf-8 into numbers at plan time but should fail at execution time if the value
        // doesn't convert)
        let err = sf_context
            .create_logical_plan("INSERT INTO testcol.some_table SELECT '2022-01-01', to_timestamp('2022-01-01T12:00:00')")
            .await.unwrap_err();
        assert_eq!(err.to_string(), "Error during planning: Column totimestamp(Utf8(\"2022-01-01T12:00:00\")) (type: Timestamp(Nanosecond, None)) is not compatible with column value (type: Float64)");
    }

    #[tokio::test]
    async fn test_plan_insert_values_wrong_number() {
        let sf_context = mock_context().await;

        let err = sf_context
            .create_logical_plan(
                "INSERT INTO testcol.some_table VALUES('2022-01-01T12:00:00')",
            )
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "Error during planning: Unexpected number of columns in VALUES: expected 2, got 1"
        );
    }

    #[tokio::test]
    async fn test_plan_insert_values_duplicate_columns() {
        let sf_context = mock_context().await;

        let err = sf_context
            .create_logical_plan("INSERT INTO testcol.some_table(date, date, value) VALUES('2022-01-01T12:00:00', '2022-01-01T12:00:00', 42)")
            .await.unwrap_err();
        assert_eq!(
            err.to_string(),
            "Schema error: Schema contains duplicate unqualified field name 'date'"
        );
    }

    #[tokio::test]
    async fn test_preexec_insert() {
        let sf_context = mock_context_with_catalog_assertions(
            |regions| {
                regions
                    .expect_create_regions()
                    .withf(|regions| {
                        // TODO: the ergonomics of these mocks are pretty bad, standard with(predicate::eq(...)) doesn't
                        // show the actual value so we have to resort to this.
                        dbg!(regions);
                        *regions
                            == vec![SeafowlRegion {
                                object_storage_id: Arc::from("fadd2ca2b9675ebce722cddc4a4fc05159a644fdeb50893d411c49d58ab52778.parquet"),
                                row_count: 1,
                                columns: Arc::new(vec![
                                    RegionColumn {
                                        name: Arc::from("date"),
                                        r#type: Arc::from("{\"name\":\"date\",\"unit\":\"MILLISECOND\"}"),
                                        min_value: Arc::new(None),
                                        max_value: Arc::new(None),
                                    },
                                    RegionColumn {
                                        name: Arc::from("value"),
                                        r#type: Arc::from("{\"name\":\"floatingpoint\",\"precision\":\"DOUBLE\"}"),
                                        min_value: Arc::new(Some(
                                            vec![
                                                52,
                                                50,
                                            ],
                                        )),
                                        max_value: Arc::new(Some(
                                            vec![
                                                52,
                                                50,
                                            ],
                                        )),
                                    },
                                ],)
                            },]
                    })
                    .return_const(vec![2]);

                // NB: even though this result isn't consumed by the caller, we need
                // to return a unit here, otherwise this will fail pretending the
                // expectation failed.
                regions
                    .expect_append_regions_to_table()
                    .with(predicate::eq(vec![2]), predicate::eq(1)).return_const(());
            },
            |tables| {
                tables
                    .expect_create_new_table_version()
                    .with(predicate::eq(0))
                    .return_const(1);
            },
        )
        .await;

        sf_context
            .plan_query(
                "INSERT INTO testcol.some_table (date, value) VALUES('2022-01-01T12:00:00', 42)",
            )
            .await
            .unwrap();

        let object_store_url = ObjectStoreUrl::parse("seafowl://").unwrap();
        let store = sf_context
            .inner
            .runtime_env()
            .object_store(object_store_url.clone())
            .unwrap();
        let uploaded_objects = store
            .list(None)
            .await
            .unwrap()
            .map_ok(|meta| meta.location)
            .try_collect::<Vec<Path>>()
            .await
            .unwrap();
        assert_eq!(
            uploaded_objects,
            vec![Path::from(
                "fadd2ca2b9675ebce722cddc4a4fc05159a644fdeb50893d411c49d58ab52778.parquet"
            )]
        );
    }

    #[tokio::test]
    #[ignore = "Currently fails, see https://github.com/splitgraph/seafowl/issues/17"]
    async fn test_register_udf() -> Result<()> {
        let sf_context = mock_context().await;

        // Source: https://gist.github.com/going-digital/02e46c44d89237c07bc99cd440ebfa43
        sf_context.collect(sf_context.plan_query(
            r#"CREATE FUNCTION sintau AS '
            {
                "entrypoint": "sintau",
                "language": "wasm",
                "input_types": ["f32"],
                "return_type": "f32",
                "data": "AGFzbQEAAAABDQJgAX0BfWADfX9/AX0DBQQAAAABBQQBAUREBxgDBnNpbnRhdQAABGV4cDIAAQRsb2cyAAIKjgEEKQECfUMAAAA/IgIgACAAjpMiACACk4siAZMgAZZBAEEYEAMgAiAAk5gLGQAgACAAjiIAk0EYQSwQA7wgAKhBF3RqvgslAQF/IAC8IgFBF3ZB/wBrsiABQQl0s0MAAIBPlUEsQcQAEAOSCyIBAX0DQCADIACUIAEqAgCSIQMgAUEEaiIBIAJrDQALIAMLC0oBAEEAC0Q/x2FC2eATQUuqKsJzsqY9QAHJQH6V0DZv+V88kPJTPSJndz6sZjE/HQCAP/clMD0D/T++F6bRPkzcNL/Tgrg//IiKNwBqBG5hbWUBHwQABnNpbnRhdQEEZXhwMgIEbG9nMgMIZXZhbHBvbHkCNwQAAwABeAECeDECBGhhbGYBAQABeAICAAF4AQJ4aQMEAAF4AQVzdGFydAIDZW5kAwZyZXN1bHQDCQEDAQAEbG9vcA=="
            }';"#,
        )
        .await?).await?;

        let results = sf_context
            .collect(
                sf_context
                    .plan_query(
                        "
        SELECT v, ROUND(sintau(CAST(v AS REAL)) * 100) AS sintau
        FROM (VALUES (0.1), (0.2), (0.3), (0.4), (0.5)) d (v)",
                    )
                    .await?,
            )
            .await?;

        let expected = vec![
            "+-----+------------------------------+",
            "| v   | sintau |",
            "+-----+------------------------------+",
            "| 0.1 | 0.5877828                    |",
            "| 0.2 | 0.95106226                   |",
            "| 0.3 | 0.95106226                   |",
            "| 0.4 | 0.5877828                    |",
            "| 0.5 | 0.0000062162862              |",
            "+-----+------------------------------+",
        ];

        assert_batches_eq!(expected, &results);

        Ok(())
    }
}
