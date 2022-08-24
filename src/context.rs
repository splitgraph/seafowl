// DataFusion bindings

use async_trait::async_trait;
use base64::decode;
use bytes::Bytes;
use datafusion::datasource::TableProvider;
use datafusion::sql::ResolvedTableReference;

use std::fs::File;

use datafusion::datasource::file_format::avro::{AvroFormat, DEFAULT_AVRO_EXTENSION};
use datafusion::datasource::file_format::csv::{CsvFormat, DEFAULT_CSV_EXTENSION};
use datafusion::datasource::file_format::json::{JsonFormat, DEFAULT_JSON_EXTENSION};
use datafusion::datasource::file_format::parquet::DEFAULT_PARQUET_EXTENSION;
use datafusion::datasource::listing::ListingOptions;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::execution::context::SessionState;
use datafusion::execution::DiskManager;

use datafusion::logical_plan::plan::Projection;
use datafusion::logical_plan::{CreateExternalTable, DFField, DropTable, Expr, FileType};

use crate::datafusion::parser::{DFParser, Statement as DFStatement};
use crate::datafusion::utils::{
    build_schema, compound_identifier_to_column, normalize_ident,
};
use crate::object_store::http::try_prepare_http_url;
use crate::wasm_udf::wasm::create_udf_from_wasm;
use futures::{StreamExt, TryStreamExt};
use hashbrown::HashMap;
use hex::encode;
#[cfg(test)]
use mockall::automock;
use object_store::memory::InMemory;
use object_store::{path::Path, ObjectStore};
use sha2::Digest;
use sha2::Sha256;
use sqlparser::ast::{
    AlterTableOperation, ObjectType, Statement, TableFactor, TableWithJoins,
};
use std::io::Read;

use std::iter::zip;
use std::sync::Arc;

pub use datafusion::error::{DataFusionError as Error, Result};
use datafusion::{
    arrow::{
        datatypes::{Schema, SchemaRef},
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
use tempfile::NamedTempFile;

use crate::catalog::{PartitionCatalog, DEFAULT_SCHEMA, STAGING_SCHEMA};
use crate::data_types::{TableId, TableVersionId};
use crate::nodes::{CreateFunction, DropSchema, RenameTable, SeafowlExtensionNode};
use crate::provider::{PartitionColumn, SeafowlPartition, SeafowlTable};
use crate::wasm_udf::data_types::{get_volatility, get_wasm_type, CreateFunctionDetails};
use crate::{
    catalog::{FunctionCatalog, TableCatalog},
    data_types::DatabaseId,
    nodes::{Assignment, CreateTable, Delete, Insert, Update},
    schema::Schema as SeafowlSchema,
};

// Scheme used for URLs referencing the object store that we use to register
// with DataFusion's object store registry.
pub const INTERNAL_OBJECT_STORE_SCHEME: &str = "seafowl";

pub fn internal_object_store_url() -> ObjectStoreUrl {
    ObjectStoreUrl::parse(format!("{}://", INTERNAL_OBJECT_STORE_SCHEME)).unwrap()
}

fn quote_ident(val: &str) -> String {
    val.replace('"', "\"\"")
}

fn reference_to_name(reference: &ResolvedTableReference) -> String {
    format!(
        "{}.{}.{}",
        quote_ident(reference.catalog),
        quote_ident(reference.schema),
        quote_ident(reference.table)
    )
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

/// Serialize data for the physical partition index from Parquet file statistics
fn build_partition_columns(
    partition_stats: &Statistics,
    schema: SchemaRef,
) -> Vec<PartitionColumn> {
    // TODO PartitionColumn might not be the right data structure here (lacks ID etc)
    match &partition_stats.column_statistics {
        // NB: Here we may end up with `null_count` being None, but DF pruning algorithm demands that
        // the null count field be not nullable itself. Consequently for any such cases the
        // pruning will fail, and we will default to using all partitions.
        Some(column_statistics) => zip(column_statistics, schema.fields())
            .map(|(stats, column)| {
                // TODO: the to_string will discard the timezone for Timestamp* values, and will
                // therefore hinder the ability to recreate them once needed for partition pruning.
                // However, since DF stats rely on Parquet stats that problem won't come up until
                // 1) Parquet starts collecting stats for Timestamp* types (`parquet::file::statistics::Statistics` enum)
                // 2) DF pattern matches those types in `summarize_min_max`.
                let min_value = stats
                    .min_value
                    .as_ref()
                    .map(|m| m.to_string().as_bytes().into());
                let max_value = stats
                    .max_value
                    .as_ref()
                    .map(|m| m.to_string().as_bytes().into());

                PartitionColumn {
                    name: Arc::from(column.name().to_string()),
                    r#type: Arc::from(column.data_type().to_json().to_string()),
                    min_value: Arc::new(min_value),
                    max_value: Arc::new(max_value),
                    null_count: stats.null_count.map(|nc| nc as i32),
                }
            })
            .collect(),
        None => schema
            .fields()
            .iter()
            .map(|column| PartitionColumn {
                name: Arc::from(column.name().to_string()),
                r#type: Arc::from(column.data_type().to_json().to_string()),
                min_value: Arc::new(None),
                max_value: Arc::new(None),
                null_count: None,
            })
            .collect(),
    }
}

pub struct DefaultSeafowlContext {
    pub inner: SessionContext,
    pub table_catalog: Arc<dyn TableCatalog>,
    pub partition_catalog: Arc<dyn PartitionCatalog>,
    pub function_catalog: Arc<dyn FunctionCatalog>,
    pub database: String,
    pub database_id: DatabaseId,
    pub max_partition_size: i64,
}

/// Create an ExecutionPlan that doesn't produce any results.
/// This is used for queries that are actually run before we produce the plan,
/// since they have to manipulate catalog metadata or use async to write to it.
fn make_dummy_exec() -> Arc<dyn ExecutionPlan> {
    Arc::new(EmptyExec::new(false, SchemaRef::new(Schema::empty())))
}

/// Open a temporary file to write partition and return a handle and a writer for it.
fn temp_partition_file_writer(
    disk_manager: Arc<DiskManager>,
    arrow_schema: SchemaRef,
) -> Result<(File, ArrowWriter<NamedTempFile>)> {
    let partition_file = disk_manager.create_tmp_file()?;
    // Maintain a second handle to the file (the first one is consumed by ArrowWriter)
    // We'll close this handle (and hence drop the file) upon reading the partition contents, just
    // prior to uploading them.
    let partition_file_handle = partition_file.reopen().map_err(|_| {
        DataFusionError::Execution("Error with temporary Parquet file".to_string())
    })?;

    let writer_properties = WriterProperties::builder().build();
    let writer =
        ArrowWriter::try_new(partition_file, arrow_schema, Some(writer_properties))?;
    Ok((partition_file_handle, writer))
}

/// Execute a plan and upload the results to object storage as Parquet files, indexing them.
/// Partially taken from DataFusion's plan_to_parquet with some additions (file stats, using a DiskManager)
pub async fn plan_to_object_store(
    state: &SessionState,
    plan: &Arc<dyn ExecutionPlan>,
    store: Arc<dyn ObjectStore>,
    disk_manager: Arc<DiskManager>,
    max_partition_size: i64,
) -> Result<Vec<SeafowlPartition>> {
    let mut current_partition_size = 0;
    let (mut current_partition_file_handle, mut writer) =
        temp_partition_file_writer(disk_manager.clone(), plan.schema())?;
    let mut partition_file_handles = vec![current_partition_file_handle];
    let mut tasks = vec![];

    // Iterate over Datafusion partitions and rechuhk them into Seafowl partitions, since we want to
    // enforce a pre-defined partition size limit, which is not guaranteed by DF.
    for i in 0..plan.output_partitioning().partition_count() {
        let task_ctx = Arc::new(TaskContext::from(state));
        let mut stream = plan.execute(i, task_ctx)?;

        while let Some(batch) = stream.next().await {
            let mut batch = batch?;
            let mut leftover_partition_capacity =
                (max_partition_size - current_partition_size) as usize;

            while batch.num_rows() > leftover_partition_capacity {
                if leftover_partition_capacity > 0 {
                    // Fill up the remaining capacity in the slice
                    writer
                        .write(&batch.slice(0, leftover_partition_capacity))
                        .map_err(DataFusionError::from)?;
                    // Trim away the part that made it to the current partition
                    batch = batch.slice(
                        leftover_partition_capacity,
                        batch.num_rows() - leftover_partition_capacity,
                    );
                }

                // Roll-over into the next partition: close partition writer, reset partition size
                // counter and open new temp file + writer.
                writer.close().map_err(DataFusionError::from).map(|_| ())?;
                current_partition_size = 0;
                leftover_partition_capacity = max_partition_size as usize;

                (current_partition_file_handle, writer) =
                    temp_partition_file_writer(disk_manager.clone(), plan.schema())?;
                partition_file_handles.push(current_partition_file_handle);
            }

            current_partition_size += batch.num_rows() as i64;
            writer.write(&batch).map_err(DataFusionError::from)?;
        }
    }
    writer.close().map_err(DataFusionError::from).map(|_| ())?;

    for mut partition_file_handle in partition_file_handles {
        let physical = plan.clone();
        let store = store.clone();
        let handle: tokio::task::JoinHandle<Result<SeafowlPartition>> =
            tokio::task::spawn(async move {
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
                let partition_stats =
                    get_parquet_file_statistics_bytes(data.clone(), physical.schema())
                        .await?;

                let columns =
                    build_partition_columns(&partition_stats, physical.schema());

                let mut hasher = Sha256::new();
                hasher.update(&data);
                let hash_str = encode(hasher.finalize());
                let object_storage_id = hash_str + ".parquet";
                store
                    .put(&Path::from(object_storage_id.clone()), data)
                    .await?;

                let partition = SeafowlPartition {
                    object_storage_id: Arc::from(object_storage_id),
                    row_count: partition_stats
                        .num_rows
                        .expect("Error counting rows in the written file")
                        .try_into()
                        .expect("row count greater than 2147483647"),
                    columns: Arc::new(columns),
                };

                Ok(partition)
            });
        tasks.push(handle);
    }

    futures::future::join_all(tasks)
        .await
        .into_iter()
        .map(|x| x.unwrap_or_else(|e| Err(DataFusionError::External(Box::new(e)))))
        .collect()
}

#[cfg_attr(test, automock)]
#[async_trait]
pub trait SeafowlContext: Send + Sync {
    /// Create a logical plan for a query
    async fn create_logical_plan(&self, sql: &str) -> Result<LogicalPlan>;

    /// Create a physical plan for a query.
    /// This runs `create_logical_plan` and then `create_physical_plan`.
    /// Note that for some statements like INSERT, this will also execute
    /// the query.
    async fn plan_query(&self, sql: &str) -> Result<Arc<dyn ExecutionPlan>>;

    /// Create a physical plan from a logical plan.
    /// Note that for some statements like INSERT, this will also execute
    /// the query.
    async fn create_physical_plan(
        &self,
        plan: &LogicalPlan,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    /// Execute a plan, producing a vector of results.
    async fn collect(
        &self,
        physical_plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Vec<RecordBatch>>;

    /// Execute a plan, outputting its results to a table.
    async fn plan_to_table(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        schema_name: String,
        table_name: String,
    ) -> Result<bool>;
}

impl DefaultSeafowlContext {
    pub fn inner(&self) -> &SessionContext {
        &self.inner
    }

    /// Reload the context to apply / pick up new schema changes
    async fn reload_schema(&self) -> Result<()> {
        // DataFusion's table catalog interface is not async, which means that we aren't really
        // supposed to perform IO when loading a list of tables in a schema / list of schemas.
        // This means that we need to know what tables we have before planning a query. We hence
        // load the whole schema for a single database into memory before every query (otherwise
        // writes applied by a different Seafowl instance won't be visible by us).

        // This does incur a latency cost to every query.

        self.inner.register_catalog(
            &self.database,
            Arc::new(self.table_catalog.load_database(self.database_id).await?),
        );

        // Register all functions in the database
        self.function_catalog
            .get_all_functions_in_database(self.database_id)
            .await?
            .iter()
            .try_for_each(|f| self.register_function(&f.name, &f.details))
    }

    /// Get a provider for a given table, return Err if it doesn't exist
    fn get_table_provider(
        &self,
        table_name: impl Into<String>,
    ) -> Result<Arc<dyn TableProvider>> {
        let table_name = table_name.into();
        let table_ref = TableReference::from(table_name.as_str());

        let resolved_ref = table_ref.resolve(&self.database, DEFAULT_SCHEMA);

        self.inner
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
            })
    }

    fn get_internal_object_store(&self) -> Arc<dyn ObjectStore> {
        let object_store_url = internal_object_store_url();
        self.inner
            .runtime_env()
            .object_store(object_store_url)
            .unwrap()
    }

    /// Resolve a table reference into a Seafowl table
    fn try_get_seafowl_table(
        &self,
        table_name: impl Into<String> + std::fmt::Debug,
    ) -> Result<SeafowlTable> {
        let table_name = table_name.into();
        let table_provider = self.get_table_provider(&table_name)?;

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
        let resolved_ref = table_ref.resolve(&self.database, DEFAULT_SCHEMA);
        let schema_name = resolved_ref.schema;
        let table_name = resolved_ref.table;

        let sf_schema = SeafowlSchema {
            arrow_schema: Arc::new(schema.as_ref().into()),
        };
        let collection_id = self
            .table_catalog
            .get_collection_id_by_name(&self.database, schema_name)
            .await?
            .ok_or_else(|| {
                Error::Plan(format!("Schema {:?} does not exist!", schema_name))
            })?;
        Ok(self
            .table_catalog
            .create_table(collection_id, table_name, &sf_schema)
            .await?)
    }

    fn register_function(
        &self,
        name: &str,
        details: &CreateFunctionDetails,
    ) -> Result<()> {
        let function_code = decode(&details.data)
            .map_err(|e| Error::Execution(format!("Error decoding the UDF: {:?}", e)))?;

        let function = create_udf_from_wasm(
            name,
            &function_code,
            &details.entrypoint,
            details.input_types.iter().map(get_wasm_type).collect(),
            get_wasm_type(&details.return_type),
            get_volatility(&details.volatility),
        )?;
        let mut mut_session_ctx = self.inner.clone();
        mut_session_ctx.register_udf(function);

        Ok(())
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

    // Execute the plan, repartition to Parquet files, upload them to object store and add metadata
    // records for table/partitions.
    async fn execute_plan_to_table(
        &self,
        physical_plan: &Arc<dyn ExecutionPlan>,
        name: Option<String>,
        from_table_version: Option<TableVersionId>,
    ) -> Result<bool> {
        let disk_manager = self.inner.runtime_env().disk_manager.clone();
        let store = self.get_internal_object_store();

        let partitions = plan_to_object_store(
            &self.inner.state(),
            physical_plan,
            store,
            disk_manager,
            self.max_partition_size,
        )
        .await?;

        // Create/Update table metadata
        let new_table_version_id;
        match (name, from_table_version) {
            (Some(name), _) => {
                // Create an empty table with an empty version
                (_, new_table_version_id) = self
                    .exec_create_table(&name, &physical_plan.schema().to_dfschema_ref()?)
                    .await?;
            }
            (_, Some(from_table_version)) => {
                // Duplicate the table version into a new one
                new_table_version_id = self
                    .table_catalog
                    .create_new_table_version(from_table_version)
                    .await?;
            }
            _ => {
                return Err(Error::Internal(
                    "Either name or source table version need to be supplied".to_string(),
                ));
            }
        }

        // Attach the partitions to the table
        let partition_ids = self.partition_catalog.create_partitions(partitions).await?;
        self.partition_catalog
            .append_partitions_to_table(partition_ids, new_table_version_id)
            .await?;

        Ok(true)
    }
}

#[async_trait]
impl SeafowlContext for DefaultSeafowlContext {
    async fn create_logical_plan(&self, sql: &str) -> Result<LogicalPlan> {
        // Reload the schema before planning a query
        self.reload_schema().await?;

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
                | Statement::Drop { object_type: ObjectType::Table, .. } => query_planner.sql_statement_to_plan(*s),

                | Statement::Drop { object_type: ObjectType::Schema,
                    if_exists: _,
                    names,
                    cascade: _,
                    purge: _, } => {
                        let name = names.first().unwrap().to_string();

                        Ok(LogicalPlan::Extension(Extension {
                            node: Arc::new(SeafowlExtensionNode::DropSchema(DropSchema { name, output_schema: Arc::new(DFSchema::empty()) }))
                        }))
                    },


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
                },

                // ALTER TABLE ... RENAME TO
                Statement::AlterTable { name, operation: AlterTableOperation::RenameTable {table_name: new_name }} => {
                    let table_name = name.to_string();
                    let table = self.try_get_seafowl_table(table_name)?;

                    if self.get_table_provider(new_name.to_string()).is_ok() {
                        return Err(Error::Plan(
                            format!("Target table {:?} already exists", new_name.to_string())
                        ))
                    }

                    Ok(LogicalPlan::Extension(Extension {
                        node: Arc::new(SeafowlExtensionNode::RenameTable(RenameTable {
                            table: Arc::from(table),
                            new_name: new_name.to_string(),
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
                    // Get the actual table schema, since DF needs to validate unqualified columns
                    // (i.e. ones referenced only by column name, lacking the relation name)
                    let table_name = table_name.to_string();
                    let seafowl_table = self.try_get_seafowl_table(&table_name)?;
                    let table_schema = seafowl_table.schema.arrow_schema.clone().to_dfschema()?;

                    let selection_expr = match selection {
                        None => None,
                        Some(expr) => Some(query_planner.sql_to_rex(expr, &table_schema, &mut HashMap::new())?),
                    };

                    Ok(LogicalPlan::Extension(Extension {
                        node: Arc::new(SeafowlExtensionNode::Delete(Delete {
                            name: table_name,
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
            DFStatement::CreateExternalTable(c) => {
                query_planner.external_table_to_plan(c)
            }
        }
    }

    async fn plan_query(&self, sql: &str) -> Result<Arc<dyn ExecutionPlan>> {
        let logical_plan = self.create_logical_plan(sql).await?;
        self.create_physical_plan(&logical_plan).await
    }

    async fn create_physical_plan(
        &self,
        plan: &LogicalPlan,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Similarly to DataFrame::sql, run certain logical plans outside of the actual execution flow
        // and produce a dummy physical plan instead
        match plan {
            // CREATE EXTERNAL TABLE copied from DataFusion's source code
            // It uses ListingTable which queries data at a given location using the ObjectStore
            // abstraction (URL: scheme://some-path.to.file.parquet) and it's easier to reuse this
            // mechanism in our case too.
            LogicalPlan::CreateExternalTable(CreateExternalTable {
                ref schema,
                ref name,
                ref location,
                ref file_type,
                ref has_header,
                ref delimiter,
                ref table_partition_cols,
                ref if_not_exists,
            }) => {
                // Check that the TableReference doesn't have a database/schema in it.
                // We create all external tables in the staging schema (backed by DataFusion's
                // in-memory schema provider) instead.
                let reference: TableReference = name.as_str().into();
                let resolved_reference =
                    reference.resolve(&self.database, STAGING_SCHEMA);

                if resolved_reference.catalog != self.database
                    || resolved_reference.schema != STAGING_SCHEMA
                {
                    return Err(DataFusionError::Plan(format!(
                        "Can only create external tables in the staging schema.
                        Omit the schema/database altogether or use {}.{}.{}",
                        &self.database, STAGING_SCHEMA, resolved_reference.table
                    )));
                }

                // Replace the table name with the fully qualified one that has our staging schema
                let name = &reference_to_name(&resolved_reference);

                let location =
                    try_prepare_http_url(location).unwrap_or_else(|| location.into());

                // Disallow the seafowl:// scheme (which is registered with DataFusion as our internal
                // object store but shouldn't be accessible via CREATE EXTERNAL TABLE)
                if location
                    .starts_with(format!("{}://", INTERNAL_OBJECT_STORE_SCHEME).as_str())
                {
                    return Err(DataFusionError::Plan(format!(
                        "Invalid URL scheme for location {:?}",
                        location
                    )));
                }

                let (file_format, file_extension) = match file_type {
                    FileType::CSV => (
                        Arc::new(
                            CsvFormat::default()
                                .with_has_header(*has_header)
                                .with_delimiter(*delimiter as u8),
                        ) as Arc<dyn FileFormat>,
                        DEFAULT_CSV_EXTENSION,
                    ),
                    FileType::Parquet => (
                        Arc::new(ParquetFormat::default()) as Arc<dyn FileFormat>,
                        DEFAULT_PARQUET_EXTENSION,
                    ),
                    FileType::Avro => (
                        Arc::new(AvroFormat::default()) as Arc<dyn FileFormat>,
                        DEFAULT_AVRO_EXTENSION,
                    ),
                    FileType::NdJson => (
                        Arc::new(JsonFormat::default()) as Arc<dyn FileFormat>,
                        DEFAULT_JSON_EXTENSION,
                    ),
                };
                let table = self.inner.table(name.as_str());
                match (if_not_exists, table) {
                    (true, Ok(_)) => Ok(make_dummy_exec()),
                    (_, Err(_)) => {
                        // TODO make schema in CreateExternalTable optional instead of empty
                        let provided_schema = if schema.fields().is_empty() {
                            None
                        } else {
                            Some(Arc::new(schema.as_ref().to_owned().into()))
                        };
                        let options = ListingOptions {
                            format: file_format,
                            collect_stat: false,
                            file_extension: file_extension.to_owned(),
                            target_partitions: self
                                .inner
                                .copied_config()
                                .target_partitions,
                            table_partition_cols: table_partition_cols.clone(),
                        };
                        self.inner
                            .register_listing_table(
                                name,
                                location,
                                options,
                                provided_schema,
                            )
                            .await?;
                        Ok(make_dummy_exec())
                    }
                    (false, Ok(_)) => Err(DataFusionError::Execution(format!(
                        "Table '{:?}' already exists",
                        name
                    ))),
                }
            }
            LogicalPlan::CreateCatalogSchema(CreateCatalogSchema {
                schema_name,
                if_not_exists: _,
                schema: _,
            }) => {
                // CREATE SCHEMA
                // Create a schema and register it
                self.table_catalog
                    .create_collection(self.database_id, schema_name)
                    .await?;
                Ok(make_dummy_exec())
            }
            LogicalPlan::CreateCatalog(CreateCatalog {
                catalog_name: _,
                if_not_exists: _,
                schema: _,
            }) => {
                // CREATE DATABASE: currently unsupported (we can create one but the context
                // is tied to a database and the user can't query a different one)
                return Err(Error::Plan(
                    "Creating new databases is currently unsupported!".to_string(),
                ));
            }
            LogicalPlan::CreateMemoryTable(CreateMemoryTable {
                name,
                input,
                if_not_exists: _,
                or_replace: _,
            }) => {
                // This is actually CREATE TABLE AS
                let physical = self.create_physical_plan(input).await?;

                self.execute_plan_to_table(&physical, Some(name.to_string()), None)
                    .await?;

                Ok(make_dummy_exec())
            }
            LogicalPlan::DropTable(DropTable {
                name,
                if_exists: _,
                schema: _,
            }) => {
                // DROP TABLE
                let table = self.try_get_seafowl_table(name)?;
                self.table_catalog.drop_table(table.table_id).await?;
                Ok(make_dummy_exec())
            }
            LogicalPlan::CreateView(_) => {
                return Err(Error::Plan(
                    "Creating views is currently unsupported!".to_string(),
                ))
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

                            self.execute_plan_to_table(
                                &physical,
                                None,
                                Some(table.table_version_id),
                            )
                            .await?;

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
                            // - replace partitions that are changed (but we don't know the table_partition i.e. which entry to
                            // repoint to our new partition)?
                            Ok(make_dummy_exec())
                        }
                        SeafowlExtensionNode::Delete(_) => {
                            // - Duplicate the table (new version)
                            // - Similar to UPDATE, but just a filter

                            // upload new files
                            // replace partitions (sometimes we delete them)

                            // really we want to be able to load all partitions + cols for a table and then
                            // write that thing back to the db (set table partitions)
                            Ok(make_dummy_exec())
                        }
                        SeafowlExtensionNode::CreateFunction(CreateFunction {
                            name,
                            details,
                            output_schema: _,
                        }) => {
                            self.register_function(name, details)?;

                            // Persist the function in the metadata storage
                            self.function_catalog
                                .create_function(self.database_id, name, details)
                                .await?;

                            Ok(make_dummy_exec())
                        }
                        SeafowlExtensionNode::RenameTable(RenameTable {
                            table,
                            new_name,
                            ..
                        }) => {
                            let table_ref = TableReference::from(new_name.as_str());

                            let (new_table_name, new_schema_id) = match table_ref {
                                // Rename the table (keep same schema)
                                TableReference::Bare { table } => (table, None),
                                // Rename the table / move its schema
                                TableReference::Partial { schema, table } => {
                                    let collection_id = self
                                        .table_catalog
                                        .get_collection_id_by_name(&self.database, schema)
                                        .await?
                                        .ok_or_else(|| {
                                            Error::Plan(format!(
                                                "Schema {:?} does not exist!",
                                                schema
                                            ))
                                        })?;

                                    (table, Some(collection_id))
                                }
                                // Catalog specified: raise an error
                                TableReference::Full { .. } => {
                                    return Err(Error::Plan(
                                        "Changing the table's database is not supported!"
                                            .to_string(),
                                    ))
                                }
                            };

                            self.table_catalog
                                .move_table(table.table_id, new_table_name, new_schema_id)
                                .await?;

                            Ok(make_dummy_exec())
                        }
                        SeafowlExtensionNode::DropSchema(DropSchema { name, .. }) => {
                            if let Some(collection_id) = self
                                .table_catalog
                                .get_collection_id_by_name(&self.database, name)
                                .await?
                            {
                                self.table_catalog.drop_collection(collection_id).await?
                            };

                            Ok(make_dummy_exec())
                        }
                    },
                    None => self.inner.create_physical_plan(plan).await,
                }
            }
            _ => self.inner.create_physical_plan(plan).await,
        }
    }

    // Copied from DataFusion's physical_plan
    async fn collect(
        &self,
        physical_plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Vec<RecordBatch>> {
        let stream = self.execute_stream(physical_plan).await?;
        stream.err_into().try_collect().await
    }

    /// Create a new table and insert data generated by the provided execution plan
    async fn plan_to_table(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        schema_name: String,
        table_name: String,
    ) -> Result<bool> {
        // Reload the schema since `try_get_seafowl_table` relies on using DataFusion's
        // TableProvider interface (which we need to pre-populate with up to date
        // information on our tables)
        self.reload_schema().await?;

        // Ensure the schema exists prior to creating the table
        let (full_table_name, from_table_version) = {
            let new_table_name = format!("{}.{}", schema_name, table_name);

            match self
                .table_catalog
                .get_collection_id_by_name(&self.database, &schema_name)
                .await?
            {
                Some(_) => {
                    if let Ok(table) = self.try_get_seafowl_table(&new_table_name) {
                        // Table exists, see if the schemas match
                        if table.schema.arrow_schema != plan.schema() {
                            return Err(DataFusionError::Execution(
                            format!(
                                "The table {} already exists but has a different schema than the one provided.",
                                new_table_name)
                            )
                        );
                        }

                        // Instead of creating a new table, just insert the data into a new version
                        // of an existing table
                        (None, Some(table.table_version_id))
                    } else {
                        // Table doesn't exist or isn't a Seafowl table
                        // We assume it doesn't exist for now
                        (Some(new_table_name), None)
                    }
                }
                None => {
                    self.table_catalog
                        .create_collection(self.database_id, &schema_name)
                        .await?;

                    (Some(new_table_name), None)
                }
            }
        };

        self.execute_plan_to_table(&plan, full_table_name, from_table_version)
            .await
    }
}

#[cfg(test)]
pub mod test_utils {
    use std::sync::Arc;

    use mockall::predicate;
    use object_store::memory::InMemory;

    use crate::{
        catalog::{
            DefaultCatalog, MockFunctionCatalog, MockPartitionCatalog, MockTableCatalog,
            TableCatalog, DEFAULT_DB, DEFAULT_SCHEMA,
        },
        object_store::http::add_http_object_store,
        provider::{SeafowlCollection, SeafowlDatabase},
        repository::sqlite::SqliteRepository,
    };

    use datafusion::{
        arrow::datatypes::{
            DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
        },
        catalog::schema::MemorySchemaProvider,
        prelude::SessionConfig,
    };

    use std::collections::HashMap as StdHashMap;

    use super::*;

    pub fn make_session() -> SessionContext {
        let session_config = SessionConfig::new()
            .with_information_schema(true)
            .with_default_catalog_and_schema(DEFAULT_DB, DEFAULT_SCHEMA);

        let context = SessionContext::with_config(session_config);
        let object_store = Arc::new(InMemory::new());
        context.runtime_env().register_object_store(
            INTERNAL_OBJECT_STORE_SCHEME,
            "",
            object_store,
        );

        // Register the HTTP object store for external tables
        add_http_object_store(&context);

        context
    }

    /// Build a real (not mocked) in-memory context that uses SQLite
    pub async fn in_memory_context() -> DefaultSeafowlContext {
        let session = make_session();

        let repository = SqliteRepository::try_new("sqlite://:memory:".to_string())
            .await
            .unwrap();
        let catalog = Arc::new(DefaultCatalog::new(Arc::new(repository)));
        let default_db = catalog.create_database(DEFAULT_DB).await.unwrap();
        catalog
            .create_collection(default_db, DEFAULT_SCHEMA)
            .await
            .unwrap();

        DefaultSeafowlContext {
            inner: session,
            table_catalog: catalog.clone(),
            partition_catalog: catalog.clone(),
            function_catalog: catalog,
            database: DEFAULT_DB.to_string(),
            database_id: default_db,
            max_partition_size: 1024 * 1024,
        }
    }

    pub async fn mock_context() -> DefaultSeafowlContext {
        mock_context_with_catalog_assertions(|_| (), |_| ()).await
    }

    pub async fn mock_context_with_catalog_assertions<FR, FT>(
        mut setup_partition_catalog: FR,
        mut setup_table_catalog: FT,
    ) -> DefaultSeafowlContext
    where
        FR: FnMut(&mut MockPartitionCatalog),
        FT: FnMut(&mut MockTableCatalog),
    {
        let session = make_session();
        let arrow_schema = ArrowSchema::new(vec![
            ArrowField::new("date", ArrowDataType::Date64, false),
            ArrowField::new("value", ArrowDataType::Float64, false),
        ]);

        let mut partition_catalog = MockPartitionCatalog::new();

        partition_catalog
            .expect_load_table_partitions()
            .with(predicate::eq(1))
            .returning(|_| {
                Ok(vec![SeafowlPartition {
                    object_storage_id: Arc::from("some-file.parquet"),
                    row_count: 3,
                    columns: Arc::new(vec![]),
                }])
            });

        setup_partition_catalog(&mut partition_catalog);

        let partition_catalog_ptr = Arc::new(partition_catalog);

        let singleton_table = SeafowlTable {
            name: Arc::from("some_table"),
            schema: Arc::new(SeafowlSchema {
                arrow_schema: Arc::new(arrow_schema.clone()),
            }),
            table_id: 0,
            table_version_id: 0,
            catalog: partition_catalog_ptr.clone(),
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
            .returning(move |_| {
                Ok(SeafowlDatabase {
                    name: Arc::from("testdb"),
                    collections: collections.clone(),
                    staging_schema: Arc::new(MemorySchemaProvider::new()),
                })
            });

        let mut function_catalog = MockFunctionCatalog::new();
        function_catalog
            .expect_create_function()
            .returning(move |_, _, _| Ok(1));
        function_catalog
            .expect_get_all_functions_in_database()
            .returning(|_| Ok(vec![]));

        session.register_catalog(
            "testdb",
            Arc::new(table_catalog.load_database(0).await.unwrap()),
        );

        setup_table_catalog(&mut table_catalog);

        let object_store = Arc::new(InMemory::new());
        session.runtime_env().register_object_store(
            INTERNAL_OBJECT_STORE_SCHEME,
            "",
            object_store,
        );

        DefaultSeafowlContext {
            inner: session,
            table_catalog: Arc::new(table_catalog),
            partition_catalog: partition_catalog_ptr,
            function_catalog: Arc::new(function_catalog),
            database: "testdb".to_string(),
            database_id: 0,
            max_partition_size: 2,
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field};
    use std::sync::Arc;

    use datafusion::execution::disk_manager::DiskManagerConfig;
    use mockall::predicate;
    use object_store::memory::InMemory;
    use test_case::test_case;

    use crate::context::test_utils::mock_context_with_catalog_assertions;

    use datafusion::assert_batches_eq;
    use datafusion::from_slice::FromSlice;
    use datafusion::physical_plan::memory::MemoryExec;

    use super::*;

    use super::test_utils::mock_context;

    const PARTITION_1_FILE_NAME: &str =
        "bdd6eef7340866d1ad99ed34ce0fa43c0d06bbed4dbcb027e9a51de48638b3ed.parquet";
    const PARTITION_2_FILE_NAME: &str =
        "2d6cabc587f8a3d8b16a56294e84f9b39fc5fc30a00d98c205ad6be670d205a3.parquet";

    const EXPECTED_INSERT_FILE_NAME: &str =
        "1592625fb7bb063580d94fe2eaf514d55e6b44f1bebd6c7f6b2e79f55477218b.parquet";

    fn to_min_max_value<T: ToString>(item: T) -> Arc<Option<Vec<u8>>> {
        Arc::from(Some(item.to_string().as_bytes().to_vec()))
    }

    #[tokio::test]
    async fn test_plan_to_object_storage() {
        let sf_context = mock_context().await;

        // Make a SELECT VALUES(...) query
        let execution_plan = sf_context
            .plan_query(
                r#"
                SELECT * FROM (VALUES
                    ('2022-01-01', 42, 'one'),
                    ('2022-01-02', 12, 'two'),
                    ('2022-01-03', 32, 'three'),
                    ('2022-01-04', 22, 'four'))
                AS t(timestamp, integer, varchar);"#,
            )
            .await
            .unwrap();

        let object_store = Arc::new(InMemory::new());
        let disk_manager = DiskManager::try_new(DiskManagerConfig::new()).unwrap();
        let partitions = plan_to_object_store(
            &sf_context.inner.state(),
            &execution_plan,
            object_store,
            disk_manager,
            2,
        )
        .await
        .unwrap();

        assert_eq!(partitions.len(), 2);

        // - Timestamp didn't get converted since we'd need to cast the string to Timestamp in query
        // or call a to_timestamp function, but neither is supported in the DF ValueExpr node.
        // - Looks like utf8 is None because DF's summarize_min_max doesn't match ByteArray stats.
        assert_eq!(
            partitions,
            vec![
                SeafowlPartition {
                    object_storage_id: Arc::from(PARTITION_1_FILE_NAME.to_string()),
                    row_count: 2,
                    columns: Arc::new(vec![
                        PartitionColumn {
                            name: Arc::from("timestamp".to_string()),
                            r#type: Arc::from("{\"name\":\"utf8\"}".to_string()),
                            min_value: Arc::new(None),
                            max_value: Arc::new(None),
                            null_count: Some(0),
                        },
                        PartitionColumn {
                            name: Arc::from("integer".to_string()),
                            r#type: Arc::from(
                                "{\"name\":\"int\",\"bitWidth\":64,\"isSigned\":true}"
                                    .to_string()
                            ),
                            min_value: to_min_max_value(12),
                            max_value: to_min_max_value(42),
                            null_count: Some(0),
                        },
                        PartitionColumn {
                            name: Arc::from("varchar".to_string()),
                            r#type: Arc::from("{\"name\":\"utf8\"}".to_string()),
                            min_value: Arc::new(None),
                            max_value: Arc::new(None),
                            null_count: Some(0),
                        }
                    ])
                },
                SeafowlPartition {
                    object_storage_id: Arc::from(PARTITION_2_FILE_NAME.to_string()),
                    row_count: 2,
                    columns: Arc::new(vec![
                        PartitionColumn {
                            name: Arc::from("timestamp".to_string()),
                            r#type: Arc::from("{\"name\":\"utf8\"}".to_string()),
                            min_value: Arc::new(None),
                            max_value: Arc::new(None),
                            null_count: Some(0),
                        },
                        PartitionColumn {
                            name: Arc::from("integer".to_string()),
                            r#type: Arc::from(
                                "{\"name\":\"int\",\"bitWidth\":64,\"isSigned\":true}"
                                    .to_string()
                            ),
                            min_value: to_min_max_value(22),
                            max_value: to_min_max_value(32),
                            null_count: Some(0),
                        },
                        PartitionColumn {
                            name: Arc::from("varchar".to_string()),
                            r#type: Arc::from("{\"name\":\"utf8\"}".to_string()),
                            min_value: Arc::new(None),
                            max_value: Arc::new(None),
                            null_count: Some(0),
                        }
                    ])
                },
            ]
        );
    }

    #[test_case(
        5,
        vec![vec![vec![0, 1, 2], vec![3, 4, 5]], vec![vec![6, 7, 8], vec![9, 10, 11]]],
        vec![vec![0, 1, 2, 3, 4], vec![5, 6, 7, 8, 9], vec![10, 11]],
        vec![
            "012fc5d6c2d7379103280ebc39d5d6bf8b9aae45a75f0b722576b442c24d6784.parquet",
            "8ee4296b8bfcd1a2a013685a73bf755387ce0275b0d49159ee67ca72c8237bc1.parquet",
            "e6628dd3c33c390d34e208c01a30365d9565edef101b6f272c2dff661dc67763.parquet",
        ];
        "record batches smaller than partitions")
    ]
    #[test_case(
        3,
        vec![vec![vec![0, 1, 2], vec![3, 4, 5]], vec![vec![6, 7, 8], vec![9, 10, 11]]],
        vec![vec![0, 1, 2], vec![3, 4, 5], vec![6, 7, 8], vec![9, 10, 11]],
        vec![
            "207f477f85081b79122ba19edbc4612a8ca747470795be1ef9d71518f79d543f.parquet",
            "45f630745bc7238fc30c0e17af635cf7f177d51fb2a1a05781fe809d36fad00d.parquet",
            "ea7504e00f9bf75273ae1f6c7760e600c358fbbe19599f41ad808f9f1d520749.parquet",
            "b4d90e2de27c2ed2db4908bcdbb52e0cd0e205966131d9f064c43a7cd98c8f14.parquet",
        ];
        "record batches same size as partitions")
    ]
    #[test_case(
        2,
        vec![vec![vec![0, 1, 2], vec![3, 4, 5]], vec![vec![6, 7, 8], vec![9, 10, 11]]],
        vec![vec![0, 1], vec![2, 3], vec![4, 5], vec![6, 7], vec![8, 9], vec![10, 11]],
        vec![
            "785bae45499128d4898cb7307e636606ada772f69c2e9ae842e79422beb9e95f.parquet",
            "991242bb627896dfa6820f8e486fcead3059205b8c262951c248210047b981eb.parquet",
            "5c7aded940a2b69bf68e824040dfe1e9c6ffcffe5f7d2cc61208fa96f86337ec.parquet",
            "9bf3793f1a262a5f662ceb50a669fcad19d587bdd016f04cd33a475b90b191d9.parquet",
            "72587d81f4f3a1b2b69377d5a6d302fea796319d6fa1ca777cc3148b63ffb819.parquet",
            "e6628dd3c33c390d34e208c01a30365d9565edef101b6f272c2dff661dc67763.parquet",
        ];
        "record batches larger than partitions")
    ]
    #[test_case(
        3,
        vec![vec![vec![0, 1], vec![2, 3, 4]], vec![vec![5]], vec![vec![6, 7, 8, 9], vec![10, 11]]],
        vec![vec![0, 1, 2], vec![3, 4, 5], vec![6, 7, 8], vec![9, 10, 11]],
        vec![
            "207f477f85081b79122ba19edbc4612a8ca747470795be1ef9d71518f79d543f.parquet",
            "45f630745bc7238fc30c0e17af635cf7f177d51fb2a1a05781fe809d36fad00d.parquet",
            "ea7504e00f9bf75273ae1f6c7760e600c358fbbe19599f41ad808f9f1d520749.parquet",
            "b4d90e2de27c2ed2db4908bcdbb52e0cd0e205966131d9f064c43a7cd98c8f14.parquet",
        ];
        "record batches irregular size")
    ]
    #[tokio::test]
    async fn test_plan_to_object_storage_partition_chunking(
        max_partition_size: i64,
        input_partitions: Vec<Vec<Vec<i32>>>,
        output_partitions: Vec<Vec<i32>>,
        storage_ids: Vec<&str>,
    ) {
        let sf_context = mock_context().await;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "some_number",
            DataType::Int32,
            true,
        )]));

        let df_partitions: Vec<Vec<RecordBatch>> = input_partitions
            .iter()
            .map(|partition| {
                partition
                    .iter()
                    .map(|record_batch| {
                        RecordBatch::try_new(
                            schema.clone(),
                            vec![Arc::new(Int32Array::from_slice(record_batch))],
                        )
                        .unwrap()
                    })
                    .collect()
            })
            .collect();

        // Make a dummy execution plan that will return the data we feed it
        let execution_plan: Arc<dyn ExecutionPlan> = Arc::new(
            MemoryExec::try_new(df_partitions.as_slice(), schema, None).unwrap(),
        );

        let object_store = Arc::new(InMemory::new());
        let disk_manager = DiskManager::try_new(DiskManagerConfig::new()).unwrap();
        let partitions = plan_to_object_store(
            &sf_context.inner.state(),
            &execution_plan,
            object_store,
            disk_manager,
            max_partition_size,
        )
        .await
        .unwrap();

        for i in 0..output_partitions.len() {
            assert_eq!(
                partitions[i],
                SeafowlPartition {
                    object_storage_id: Arc::from(storage_ids[i].to_string()),
                    row_count: output_partitions[i].len() as i32,
                    columns: Arc::new(vec![PartitionColumn {
                        name: Arc::from("some_number"),
                        r#type: Arc::from(
                            r#"{"name":"int","bitWidth":32,"isSigned":true}"#
                        ),
                        min_value: to_min_max_value(
                            output_partitions[i].iter().min().unwrap()
                        ),
                        max_value: to_min_max_value(
                            output_partitions[i].iter().max().unwrap()
                        ),
                        null_count: Some(0),
                    }])
                },
            )
        }
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
            |partitions| {
                partitions
                    .expect_create_partitions()
                    .withf(|partitions| {
                        // TODO: the ergonomics of these mocks are pretty bad, standard with(predicate::eq(...)) doesn't
                        // show the actual value so we have to resort to this.
                        dbg!(partitions);
                        *partitions
                            == vec![SeafowlPartition {
                                object_storage_id: Arc::from(EXPECTED_INSERT_FILE_NAME),
                                row_count: 1,
                                columns: Arc::new(vec![
                                    PartitionColumn {
                                        name: Arc::from("date"),
                                        r#type: Arc::from("{\"name\":\"date\",\"unit\":\"MILLISECOND\"}"),
                                        min_value: Arc::new(None),
                                        max_value: Arc::new(None),
                                        null_count: Some(0),
                                    },
                                    PartitionColumn {
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
                                        null_count: Some(0),
                                    },
                                ],)
                            },]
                    })
                    .return_once(|_| Ok(vec![2]));

                // NB: even though this result isn't consumed by the caller, we need
                // to return a unit here, otherwise this will fail pretending the
                // expectation failed.
                partitions
                    .expect_append_partitions_to_table()
                    .with(predicate::eq(vec![2]), predicate::eq(1)).return_once(|_, _| Ok(()));
            },
            |tables| {
                tables
                    .expect_create_new_table_version()
                    .with(predicate::eq(0))
                    .return_once(|_| Ok(1));
            },
        )
        .await;

        sf_context
            .plan_query(
                "INSERT INTO testcol.some_table (date, value) VALUES('2022-01-01T12:00:00', 42)",
            )
            .await
            .unwrap();

        let store = sf_context.get_internal_object_store();
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
            vec![Path::from(EXPECTED_INSERT_FILE_NAME)]
        );
    }

    #[tokio::test]
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
            "+-----+--------+",
            "| v   | sintau |",
            "+-----+--------+",
            "| 0.1 | 59     |",
            "| 0.2 | 95     |",
            "| 0.3 | 95     |",
            "| 0.4 | 59     |",
            "| 0.5 | 0      |",
            "+-----+--------+",
        ];

        assert_batches_eq!(expected, &results);

        Ok(())
    }
}
