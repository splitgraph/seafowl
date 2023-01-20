// DataFusion bindings

use async_trait::async_trait;
use base64::decode;
use bytes::BytesMut;

use datafusion::datasource::{provider_as_source, TableProvider};
use datafusion::parquet::basic::Compression;
use datafusion::sql::ResolvedTableReference;
use itertools::Itertools;
use object_store::local::LocalFileSystem;
use std::collections::{HashMap, HashSet};
use tokio::fs::File as AsyncFile;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};

use std::fs::File;

use datafusion::datasource::listing::ListingTable;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::execution::context::SessionState;
use datafusion::execution::DiskManager;

use datafusion_proto::protobuf;

use crate::datafusion::parser::{DFParser, Statement as DFStatement};
use crate::datafusion::utils::{build_schema, normalize_ident};
use crate::object_store::http::try_prepare_http_url;
use crate::object_store::wrapped::InternalObjectStore;
use crate::utils::{gc_partitions, group_partitions, hash_file};
use crate::wasm_udf::wasm::create_udf_from_wasm;
use futures::{StreamExt, TryStreamExt};

#[cfg(test)]
use mockall::automock;
use object_store::{path::Path, ObjectStore};

use sqlparser::ast::{
    AlterTableOperation, Ident, ObjectName, ObjectType, SchemaName, Statement,
    TableFactor, TableWithJoins,
};

use arrow_integration_test::field_to_json;
use std::iter::zip;
use std::str::FromStr;
use std::sync::Arc;

use datafusion::common::{DFField, DFSchema, ToDFSchema};
use datafusion::config::ConfigOptions;
use datafusion::datasource::file_format::file_type::{FileCompressionType, FileType};
pub use datafusion::error::{DataFusionError as Error, Result};
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_expr::execution_props::ExecutionProps;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::scalar::ScalarValue;
use datafusion::{
    arrow::{
        datatypes::{Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::file_format::{parquet::ParquetFormat, FileFormat},
    error::DataFusionError,
    execution::context::TaskContext,
    parquet::{arrow::ArrowWriter, file::properties::WriterProperties},
    physical_plan::{
        coalesce_partitions::CoalescePartitionsExec, empty::EmptyExec,
        EmptyRecordBatchStream, ExecutionPlan, SendableRecordBatchStream, Statistics,
    },
    prelude::SessionContext,
    sql::{planner::SqlToRel, TableReference},
};

use datafusion_expr::logical_plan::{
    CreateCatalog, CreateCatalogSchema, CreateExternalTable, CreateMemoryTable,
    DropTable, Extension, LogicalPlan, Projection,
};
use datafusion_expr::{cast, Expr, LogicalPlanBuilder};
use log::{debug, info, warn};
use parking_lot::RwLock;
use prost::Message;
use tempfile::TempPath;
use tokio::sync::Semaphore;

use crate::catalog::{PartitionCatalog, DEFAULT_SCHEMA, STAGING_SCHEMA};
use crate::data_types::{PhysicalPartitionId, TableId, TableVersionId};
use crate::datafusion::visit::VisitorMut;
use crate::provider::{
    project_expressions, PartitionColumn, SeafowlPartition, SeafowlPruningStatistics,
    SeafowlTable,
};
use crate::wasm_udf::data_types::{get_volatility, CreateFunctionDetails};
use crate::{
    catalog::{FunctionCatalog, TableCatalog},
    data_types::DatabaseId,
    nodes::{
        CreateFunction, CreateTable, Delete, DropSchema, Insert, RenameTable,
        SeafowlExtensionNode, Update, Vacuum,
    },
    schema::Schema as SeafowlSchema,
    version::TableVersionProcessor,
};

// Scheme used for URLs referencing the object store that we use to register
// with DataFusion's object store registry.
pub const INTERNAL_OBJECT_STORE_SCHEME: &str = "seafowl";

// Max Parquet row group size, in rows. This is what the ArrowWriter uses to determine how many
// rows to buffer in memory before flushing them out to disk. The default for this is 1024^2, which
// means that we're effectively buffering a whole partition in memory, causing issues on RAM-limited
// environments.
const MAX_ROW_GROUP_SIZE: usize = 65536;

// Just a simple read buffer to reduce the number of syscalls when filling in the part buffer.
const PARTITION_FILE_BUFFER_SIZE: usize = 128 * 1024;
// This denotes the threshold size for an individual multipart request payload prior to upload.
// It dictates the memory usage, as we'll need to to keep each part in memory until sent.
const PARTITION_FILE_MIN_PART_SIZE: usize = 5 * 1024 * 1024;
// Controls how many multipart upload tasks we let run in parallel; this is in part dictated by the
// fact that object store concurrently uploads parts for each of our tasks. That concurrency in
// turn is hard coded to 8 (https://github.com/apache/arrow-rs/blob/master/object_store/src/aws/mod.rs#L145)
// meaning that with 2 partition upload tasks x 8 part upload tasks x 5MB we have 80MB of memory usage
const PARTITION_FILE_UPLOAD_MAX_CONCURRENCY: usize = 2;

pub fn internal_object_store_url() -> ObjectStoreUrl {
    ObjectStoreUrl::parse(format!("{INTERNAL_OBJECT_STORE_SCHEME}://")).unwrap()
}

fn quote_ident(val: &str) -> String {
    val.replace('"', "\"\"")
}

pub fn remove_quotes_from_ident(possibly_quoted_name: &Ident) -> Ident {
    Ident::new(&possibly_quoted_name.value)
}

pub fn remove_quotes_from_idents(column_names: &[Ident]) -> Vec<Ident> {
    column_names.iter().map(remove_quotes_from_ident).collect()
}

pub fn remove_quotes_from_object_name(name: &ObjectName) -> ObjectName {
    ObjectName(remove_quotes_from_idents(&name.0))
}

pub fn remove_quotes_from_schema_name(name: &SchemaName) -> SchemaName {
    match name {
        SchemaName::Simple(schema_name) => {
            SchemaName::Simple(remove_quotes_from_object_name(schema_name))
        }
        SchemaName::UnnamedAuthorization(_) | SchemaName::NamedAuthorization(_, _) => {
            name.to_owned()
        }
    }
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
    path: &std::path::Path,
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

    let tmp_dir = path
        .parent()
        .expect("Temporary Parquet file in the FS root");
    let file_name = path
        .file_name()
        .expect("Temporary Parquet file pointing to a directory")
        .to_string_lossy();

    // Create a dummy object store pointing to our temporary directory
    let dummy_object_store: Arc<dyn ObjectStore> =
        Arc::from(LocalFileSystem::new_with_prefix(tmp_dir)?);
    let dummy_path = Path::from(file_name.to_string());

    let parquet = ParquetFormat::new(Arc::new(RwLock::new(ConfigOptions::new())));
    let meta = dummy_object_store
        .head(&dummy_path)
        .await
        .expect("Temporary object not found");
    let stats = parquet
        .infer_stats(&dummy_object_store, schema, &meta)
        .await?;
    Ok(stats)
}

// Serialise min/max stats in the form of a given ScalarValue using Datafusion protobufs format
pub fn scalar_value_to_bytes(value: &ScalarValue) -> Option<Vec<u8>> {
    match <&ScalarValue as TryInto<protobuf::ScalarValue>>::try_into(value) {
        Ok(proto) => Some(proto.encode_to_vec()),
        Err(error) => {
            warn!("Failed to serialise min/max value {:?}: {}", value, error);
            None
        }
    }
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
                // Since DF stats rely on Parquet stats we won't have stats on  Timestamp* values until
                // 1) Parquet starts collecting stats for them (`parquet::file::statistics::Statistics` enum)
                // 2) DF pattern matches those types in `summarize_min_max`.
                let min_value = stats.min_value.as_ref().and_then(scalar_value_to_bytes);
                let max_value = stats.max_value.as_ref().and_then(scalar_value_to_bytes);

                PartitionColumn {
                    name: Arc::from(column.name().to_string()),
                    r#type: Arc::from(field_to_json(column).to_string()),
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
                r#type: Arc::from(field_to_json(column).to_string()),
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
    pub internal_object_store: Arc<InternalObjectStore>,
    pub database: String,
    pub database_id: DatabaseId,
    pub max_partition_size: u32,
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
) -> Result<(TempPath, ArrowWriter<File>)> {
    let partition_file =
        disk_manager.create_tmp_file("Open a temporary file to write partition")?;

    // Hold on to the path of the file, in case we need to just move it instead of
    // uploading the data to the object store. This can be a consistency/security issue, but the
    // worst someone can do is swap out the file with something else if the original temporary
    // file gets deleted and an attacker creates a temporary file with the same name. In that case,
    // we can end up copying an arbitrary file to the object store, which requires access to the
    // machine anyway (and at that point there's likely other things that the attacker can do, like
    // change the write access control settings).
    let path = partition_file.into_temp_path();

    let file_writer = File::options().write(true).open(&path)?;

    let writer_properties = WriterProperties::builder()
        .set_max_row_group_size(MAX_ROW_GROUP_SIZE)
        .set_compression(Compression::ZSTD)
        .build();
    let writer =
        ArrowWriter::try_new(file_writer, arrow_schema, Some(writer_properties))?;
    Ok((path, writer))
}

/// Execute a plan and upload the results to object storage as Parquet files, indexing them.
/// Partially taken from DataFusion's plan_to_parquet with some additions (file stats, using a DiskManager)
pub async fn plan_to_object_store(
    state: &SessionState,
    plan: &Arc<dyn ExecutionPlan>,
    output_schema: Option<SchemaRef>,
    store: Arc<InternalObjectStore>,
    disk_manager: Arc<DiskManager>,
    max_partition_size: u32,
) -> Result<Vec<SeafowlPartition>> {
    let mut current_partition_size = 0;
    let (mut current_partition_file_path, mut writer) = temp_partition_file_writer(
        disk_manager.clone(),
        output_schema.clone().unwrap_or_else(|| plan.schema()),
    )?;
    let mut partition_file_paths = vec![current_partition_file_path];
    let mut tasks = vec![];

    // Iterate over Datafusion partitions and rechuhk them into Seafowl partitions, since we want to
    // enforce a pre-defined partition size limit, which is not guaranteed by DF.
    for i in 0..plan.output_partitioning().partition_count() {
        let task_ctx = Arc::new(TaskContext::from(state));
        let mut stream = plan.execute(i, task_ctx)?;

        while let Some(batch) = stream.next().await {
            let mut batch = batch?;

            // If the output schema is provided, and the batch is not aligned with it, try to coerce
            // the batch to it (aligning only nullability info).
            // This comes up when the UPDATE has a literal assignment for a nullable field; the used
            // projection plan inherits the nullability from the `Literal`, which in turn just looks
            // at whether the used value is null, disregarding the corresponding column/schema.
            if let Some(schema) = output_schema.clone() {
                if batch.schema() != schema {
                    batch = RecordBatch::try_new(schema, batch.columns().to_vec())?;
                }
            }

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

                (current_partition_file_path, writer) =
                    temp_partition_file_writer(disk_manager.clone(), plan.schema())?;
                partition_file_paths.push(current_partition_file_path);
            }

            current_partition_size += batch.num_rows() as u32;
            writer.write(&batch).map_err(DataFusionError::from)?;
        }
    }
    writer.close().map_err(DataFusionError::from).map(|_| ())?;

    info!("Starting upload of partition objects");

    let sem = Arc::new(Semaphore::new(PARTITION_FILE_UPLOAD_MAX_CONCURRENCY));
    for partition_file_path in partition_file_paths {
        let permit = Arc::clone(&sem).acquire_owned().await.ok();

        let physical = plan.clone();
        let store = store.clone();
        let handle: tokio::task::JoinHandle<Result<SeafowlPartition>> =
            tokio::task::spawn(async move {
                // Move the ownership of the semaphore permit into the task
                let _permit = permit;

                // Index the Parquet file (get its min-max values)
                let partition_stats = get_parquet_file_statistics_bytes(
                    &partition_file_path,
                    physical.schema(),
                )
                .await?;

                let columns =
                    build_partition_columns(&partition_stats, physical.schema());

                let object_storage_id =
                    hash_file(&partition_file_path).await? + ".parquet";

                // For local FS stores, we can just move the file to the target location
                if let Some(result) = store
                    .fast_upload(
                        &partition_file_path,
                        &Path::from(object_storage_id.clone()),
                    )
                    .await
                {
                    result?;
                } else {
                    let file = AsyncFile::open(partition_file_path).await?;
                    let mut reader =
                        BufReader::with_capacity(PARTITION_FILE_BUFFER_SIZE, file);
                    let mut part_buffer =
                        BytesMut::with_capacity(PARTITION_FILE_MIN_PART_SIZE);

                    let location = Path::from(object_storage_id.clone());
                    let (multipart_id, mut writer) =
                        store.inner.put_multipart(&location).await?;

                    let error: std::io::Error;
                    let mut eof_counter = 0;
                    loop {
                        match reader.read_buf(&mut part_buffer).await {
                            Ok(0) if part_buffer.is_empty() => {
                                // We've reached EOF and there are no pending writes to flush.
                                // As per the docs size = 0 doesn't seem to guarantee that we've reached EOF, so we use
                                // a heuristic: if we encounter Ok(0) 3 times in a row it's safe to assume it's EOF.
                                // Another potential workaround is to use `stream_position` + `stream_len` to determine
                                // whether we've reached the end (`stream_len` is nightly-only experimental API atm)
                                eof_counter += 1;
                                if eof_counter >= 3 {
                                    break;
                                } else {
                                    continue;
                                }
                            }
                            Ok(size)
                                if size != 0
                                    && part_buffer.len()
                                        < PARTITION_FILE_MIN_PART_SIZE =>
                            {
                                // Keep filling the part buffer until it surpasses the minimum required size
                                eof_counter = 0;
                                continue;
                            }
                            Ok(_) => {
                                let part_size = part_buffer.len();
                                debug!("Uploading part with {} bytes", part_size);
                                match writer.write_all(&part_buffer[..part_size]).await {
                                    Ok(_) => {
                                        part_buffer.clear();
                                        continue;
                                    }
                                    Err(err) => error = err,
                                }
                            }
                            Err(err) => error = err,
                        }

                        warn!(
                            "Aborting multipart partition upload due to an error: {:?}",
                            error
                        );
                        store
                            .inner
                            .abort_multipart(&location, &multipart_id)
                            .await
                            .ok();
                        return Err(DataFusionError::IoError(error));
                    }

                    writer.shutdown().await?;
                }

                let partition = SeafowlPartition {
                    partition_id: None,
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

pub fn is_read_only(plan: &LogicalPlan) -> bool {
    !matches!(
        plan,
        LogicalPlan::CreateExternalTable(_)
            | LogicalPlan::CreateMemoryTable(_)
            | LogicalPlan::CreateView(_)
            | LogicalPlan::CreateCatalogSchema(_)
            | LogicalPlan::CreateCatalog(_)
            | LogicalPlan::DropTable(_)
            | LogicalPlan::Analyze(_)
            | LogicalPlan::Extension(_)
    )
}

pub fn is_statement_read_only(statement: &DFStatement) -> bool {
    if let DFStatement::Statement(s) = statement {
        matches!(**s, Statement::Query(_) | Statement::Explain { .. })
    } else {
        false
    }
}

#[cfg_attr(test, automock)]
#[async_trait]
pub trait SeafowlContext: Send + Sync {
    /// Parse SQL into one or more statements
    async fn parse_query(&self, sql: &str) -> Result<Vec<DFStatement>>;

    /// Create a logical plan for a query (single-statement SQL)
    async fn create_logical_plan(&self, sql: &str) -> Result<LogicalPlan>;

    /// Create a logical plan for a query from a parsed statement
    async fn create_logical_plan_from_statement(
        &self,
        statement: DFStatement,
    ) -> Result<LogicalPlan>;

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

    // Check that the TableReference doesn't have a database/schema in it.
    // We create all external tables in the staging schema (backed by DataFusion's
    // in-memory schema provider) instead.
    fn resolve_staging_ref<'a>(
        &'a self,
        name: &'a str,
    ) -> Result<ResolvedTableReference<'a>> {
        let reference: TableReference = name.into();
        let resolved_reference = reference.resolve(&self.database, STAGING_SCHEMA);

        if resolved_reference.catalog != self.database
            || resolved_reference.schema != STAGING_SCHEMA
        {
            return Err(DataFusionError::Plan(format!(
                "Can only create external tables in the staging schema.
                        Omit the schema/database altogether or use {}.{}.{}",
                &self.database, STAGING_SCHEMA, resolved_reference.table
            )));
        }

        Ok(resolved_reference)
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

    fn get_internal_object_store(&self) -> Arc<InternalObjectStore> {
        self.internal_object_store.clone()
    }

    /// Resolve a table reference into a Seafowl table
    pub fn try_get_seafowl_table(
        &self,
        table_name: impl Into<String> + std::fmt::Debug,
    ) -> Result<SeafowlTable> {
        let table_name = table_name.into();
        let table_provider = self.get_table_provider(&table_name)?;

        let seafowl_table = match table_provider.as_any().downcast_ref::<SeafowlTable>() {
            Some(seafowl_table) => Ok(seafowl_table),
            None => Err(Error::Plan(format!(
                "'{table_name:?}' is a read-only table"
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
                Error::Plan(format!("Schema {schema_name:?} does not exist!"))
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
            .map_err(|e| Error::Execution(format!("Error decoding the UDF: {e:?}")))?;

        let function = create_udf_from_wasm(
            &details.language,
            name,
            &function_code,
            &details.entrypoint,
            &details.input_types,
            &details.return_type,
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
        output_schema: Option<SchemaRef>,
        name: Option<String>,
        from_table_version: Option<TableVersionId>,
    ) -> Result<TableVersionId> {
        let partition_ids = self
            .execute_plan_to_partitions(physical_plan, output_schema.clone())
            .await?;

        // Create/Update table metadata
        let new_table_version_id;
        match (name, from_table_version) {
            (Some(name), _) => {
                let schema = output_schema.unwrap_or_else(|| physical_plan.schema());
                // Create an empty table with an empty version
                (_, new_table_version_id) = self
                    .exec_create_table(&name, &schema.to_dfschema_ref()?)
                    .await?;
            }
            (_, Some(from_table_version)) => {
                // Duplicate the table version into a new one
                new_table_version_id = self
                    .table_catalog
                    .create_new_table_version(from_table_version, true)
                    .await?;
            }
            _ => {
                return Err(Error::Internal(
                    "Either name or source table version need to be supplied".to_string(),
                ));
            }
        }

        // Attach the partitions to the table
        self.partition_catalog
            .append_partitions_to_table(partition_ids.clone(), new_table_version_id)
            .await?;

        Ok(new_table_version_id)
    }

    // Generate new physical Parquet partition files from the provided plan, upload to object store
    // and persist partition metadata.
    async fn execute_plan_to_partitions(
        &self,
        physical_plan: &Arc<dyn ExecutionPlan>,
        output_schema: Option<SchemaRef>,
    ) -> Result<Vec<PhysicalPartitionId>> {
        let disk_manager = self.inner.runtime_env().disk_manager.clone();
        let store = self.get_internal_object_store();

        // Generate new physical partition objects
        let partitions = plan_to_object_store(
            &self.inner.state(),
            physical_plan,
            output_schema,
            store,
            disk_manager,
            self.max_partition_size,
        )
        .await?;

        debug!(
            "execute_plan_to_partition completed, metrics: {:?}",
            physical_plan.metrics()
        );

        // Record partition metadata to the catalog
        self.partition_catalog
            .create_partitions(partitions)
            .await
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed persisting partition metadata {e:?}"
                ))
            })
    }

    // Copied from DataFusion's source code (private functions)
    async fn create_external_table(
        &self,
        cmd: &CreateExternalTable,
        filter_suffix: bool,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let table_provider: Arc<dyn TableProvider> =
            self.create_custom_table(cmd).await?;

        let table = self.inner.table(cmd.name.as_str());
        match (cmd.if_not_exists, table) {
            (true, Ok(_)) => Ok(make_dummy_exec()),
            (_, Err(_)) => {
                if ["TABLE", "DELTATABLE"].contains(&cmd.file_type.as_str()) {
                    // This is a remote or delta table, register and exit
                    self.inner
                        .register_table(cmd.name.as_str(), table_provider)?;
                    return Ok(make_dummy_exec());
                }

                // This is quite unfortunate, as the DataFusion creates everything we need above, apart from
                // the override of the `file_extension`. There's no way to override the ListingOptions
                // in the created ListingTable, so we just grab the one created and create a new ListingTable
                // with our own `file_extension`.

                // TODO make schema in CreateExternalTable optional instead of empty
                let provided_schema = if cmd.schema.fields().is_empty() {
                    None
                } else {
                    Some(Arc::new(cmd.schema.as_ref().to_owned().into()))
                };

                let file_compression_type = match FileCompressionType::from_str(
                    cmd.file_compression_type.as_str(),
                ) {
                    Ok(t) => t,
                    Err(_) => Err(DataFusionError::Execution(
                        "Only known FileCompressionTypes can be ListingTables!"
                            .to_string(),
                    ))?,
                };

                let file_type = match FileType::from_str(cmd.file_type.as_str()) {
                    Ok(t) => t,
                    Err(_) => Err(DataFusionError::Execution(
                        "Only known FileTypes can be ListingTables!".to_string(),
                    ))?,
                };

                // Change from default DataFusion behaviour: allow disabling filtering by an extension
                let file_extension = if filter_suffix {
                    file_type
                        .get_ext_with_compression(file_compression_type.to_owned())?
                } else {
                    "".to_string()
                };

                let options = table_provider
                    .as_any()
                    .downcast_ref::<ListingTable>()
                    .ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "Couldn't cast table {:?} to ListingTable",
                            table_provider.schema(),
                        ))
                    })?
                    .options()
                    .clone();

                self.inner
                    .register_listing_table(
                        cmd.name.as_str(),
                        cmd.location.clone(),
                        options.with_file_extension(file_extension),
                        provided_schema,
                        cmd.definition.clone(),
                    )
                    .await?;
                Ok(make_dummy_exec())
            }
            (false, Ok(_)) => Err(DataFusionError::Execution(format!(
                "Table '{:?}' already exists",
                cmd.name
            ))),
        }
    }

    // Copied from DataFusion's source code (private functions)
    async fn create_custom_table(
        &self,
        cmd: &CreateExternalTable,
    ) -> Result<Arc<dyn TableProvider>> {
        let state = self.inner.state.read().clone();
        let file_type = cmd.file_type.to_uppercase();
        let factory = &state
            .runtime_env
            .table_factories
            .get(file_type.as_str())
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "Unable to find factory for {}",
                    cmd.file_type
                ))
            })?;
        let table = (*factory).create(&state, cmd).await?;
        Ok(table)
    }
}

#[async_trait]
impl SeafowlContext for DefaultSeafowlContext {
    async fn parse_query(&self, sql: &str) -> Result<Vec<DFStatement>> {
        Ok(DFParser::parse_sql(sql)?.into_iter().collect_vec())
    }

    async fn create_logical_plan_from_statement(
        &self,
        statement: DFStatement,
    ) -> Result<LogicalPlan> {
        // Reload the schema before planning a query
        self.reload_schema().await?;
        let state = self.inner.state.read().clone();
        let query_planner = SqlToRel::new(&state);

        match statement {
            DFStatement::Statement(s) => match *s {
                Statement::Query(mut q) => {
                    // Determine if some of the tables reference a non-latest version using table
                    // function syntax. If so, rename the tables in the query by appending the
                    // explicit version id for the provided timestamp and add it to the schema
                    // provider's map.

                    let mut version_processor = TableVersionProcessor::new(self.database.clone(), DEFAULT_SCHEMA.to_string());
                    version_processor.visit_query(&mut q);

                    if !version_processor.table_versions.is_empty() {
                        // Create a new session context and session state, to avoid potential race
                        // conditions leading to schema provider map leaking into other queries (and
                        // thus polluting e.g. the information_schema output), or even worse reloading
                        // the map and having the versioned query fail during execution.
                        let session_ctx = SessionContext::with_state(state.clone());

                        version_processor.triage_version_ids(self.database.clone(), self.table_catalog.clone()).await?;
                        // We now have table_version_ids for each table with version specified; do another
                        // run over the query AST to rewrite the table.
                        version_processor.visit_query(&mut q);
                        debug!("Time travel query rewritten to: {}", q);

                        let tables_by_version = self
                            .table_catalog
                            .load_tables_by_version(self.database_id, Some(version_processor.table_version_ids())).await?;

                        for ((table, version), table_version_id) in &version_processor.table_versions {
                            if let Some(table_version_id) = table_version_id {
                                let mut name = table.clone();
                                name.0.last_mut().unwrap().value =
                                    version_processor.table_with_version(&name, version);
                                let full_name = name.to_string();
                                let table_ref = TableReference::from(full_name.as_str());
                                let table_provider = tables_by_version[table_version_id].clone();

                                if !session_ctx.table_exist(table_ref)? {
                                    session_ctx.register_table(table_ref, table_provider)?;
                                }
                            }
                        }

                        let state = session_ctx.state.read().clone();
                        let query_planner = SqlToRel::new(&state);
                        return query_planner.sql_statement_to_plan(Statement::Query(q));
                    }

                    query_planner.sql_statement_to_plan(Statement::Query(q))
                },
                Statement::CreateSchema { schema_name, if_not_exists } => query_planner.sql_statement_to_plan(
                    Statement::CreateSchema {
                        schema_name: remove_quotes_from_schema_name(&schema_name),
                        if_not_exists
                    }
                ),
                // Delegate generic queries to the basic DataFusion logical planner
                // (though note EXPLAIN [our custom query] will mean we have to implement EXPLAIN ourselves)
                Statement::Explain { .. }
                | Statement::ShowVariable { .. }
                | Statement::ShowTables { .. }
                | Statement::ShowColumns { .. }
                | Statement::CreateView { .. }
                | Statement::CreateDatabase { .. } => query_planner.sql_statement_to_plan(*s),
                | Statement::Drop { object_type: ObjectType::Table,
                    if_exists,
                    names,
                    cascade,
                    restrict,
                    purge } => query_planner.sql_statement_to_plan(Statement::Drop {
                        object_type: ObjectType::Table,
                        if_exists,
                        names: names.iter().map(remove_quotes_from_object_name).collect(),
                        cascade,
                        restrict,
                        purge }),
                | Statement::Drop { object_type: ObjectType::Schema,
                    if_exists: _,
                    names,
                    cascade: _,
                    purge: _, .. } => {
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
                            name: remove_quotes_from_object_name(&name).to_string(),
                            if_not_exists,
                            output_schema: Arc::new(DFSchema::empty())
                        })),
                    }))
                },

                // ALTER TABLE ... RENAME TO
                Statement::AlterTable { name, operation: AlterTableOperation::RenameTable {table_name: new_name }} => {
                    let old_table_name = remove_quotes_from_object_name(&name).to_string();
                    let new_table_name = remove_quotes_from_object_name(&new_name).to_string();
                    let table = self.try_get_seafowl_table(old_table_name)?;

                    if self.get_table_provider(new_table_name.to_owned()).is_ok() {
                        return Err(Error::Plan(
                            format!("Target table {new_table_name:?} already exists")
                        ))
                    }

                    Ok(LogicalPlan::Extension(Extension {
                        node: Arc::new(SeafowlExtensionNode::RenameTable(RenameTable {
                            table: Arc::from(table),
                            new_name: new_table_name,
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
                        expr: target_schema.fields().iter().zip(plan.schema().fields()).map(|(table_field, query_field)| {
                            // Generate CAST (source_col AS table_col_type) AS table_col
                            // If the type is the same, this will be optimized out.
                            cast(
                                Expr::Column(query_field.qualified_column()),
                                table_field.data_type().clone()
                            ).alias(table_field.name())
                        }).collect(),
                        input: Arc::new(plan),
                        schema: Arc::new(target_schema),
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
                    ..
                }
                // We only support the most basic form of UPDATE (no aliases or FROM or joins)
                    if with_hints.is_empty() && joins.is_empty()
                => {
                    // Scan through the original table (with selection) and:
                    // SELECT [for each col, "col AS col" if not an assignment, otherwise "expr AS col"]
                    //   FROM original_table WHERE [selection]
                    // Also split the result by existing partition boundaries and leave unchanged partitions alone

                    // Get the actual table schema, since DF needs to validate unqualified columns
                    // (i.e. ones referenced only by column name, lacking the relation name)
                    let table_name = name.to_string();
                    let seafowl_table = Arc::new(self.try_get_seafowl_table(&table_name)?);
                    let table_schema = seafowl_table.schema.arrow_schema.clone().to_dfschema()?;

                    let selection_expr = match selection {
                        None => None,
                        Some(expr) => Some(query_planner.sql_to_rex(expr, &table_schema, &mut HashMap::new())?),
                    };

                    let assignment_exprs = assignments.iter().map(|a| {
                        Ok((
                            table_schema.field_with_unqualified_name(&normalize_ident(&a.id[0]))?.name().clone(),
                            query_planner.sql_to_rex(a.value.clone(), &table_schema, &mut HashMap::new())?
                        ))
                    }).collect::<Result<Vec<(String, Expr)>>>()?;

                    let logical_plan = LogicalPlan::Extension(Extension {
                        node: Arc::new(SeafowlExtensionNode::Update(Update {
                            table: seafowl_table.clone(),
                            table_plan: Arc::new(LogicalPlanBuilder::scan(table_name,
                                provider_as_source(seafowl_table),
                                None,
                            )?.build()?),
                            selection: selection_expr,
                            assignments: assignment_exprs,
                            output_schema: Arc::new(DFSchema::empty())
                        })),
                    });

                    // Run the optimizer in order to apply required transformations to the query plan
                    // (e.g. type coercions for the WHERE clause)
                    self.inner.optimize(&logical_plan)
                }
                Statement::Delete {
                    table_name,
                    selection,
                    ..
                } => {
                    // Get the actual table schema, since DF needs to validate unqualified columns
                    // (i.e. ones referenced only by column name, lacking the relation name)
                    let table_name = table_name.to_string();
                    let seafowl_table = Arc::new(self.try_get_seafowl_table(&table_name)?);
                    let table_schema = seafowl_table.schema.arrow_schema.clone().to_dfschema()?;

                    let selection_expr = match selection {
                        None => None,
                        Some(expr) => Some(query_planner.sql_to_rex(expr, &table_schema, &mut HashMap::new())?),
                    };

                    let logical_plan = LogicalPlan::Extension(Extension {
                        node: Arc::new(SeafowlExtensionNode::Delete(Delete {
                            table: seafowl_table.clone(),
                            table_plan: Arc::new(LogicalPlanBuilder::scan(table_name,
                                                                          provider_as_source(seafowl_table),
                                                                          None,
                            )?
                                .build()?),
                            selection: selection_expr,
                            output_schema: Arc::new(DFSchema::empty())
                        })),
                    });

                    // Run the optimizer in order to apply required transformations to the query plan
                    // (e.g. type coercions for the WHERE clause)
                    self.inner.optimize(&logical_plan)
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
                            Error::Execution(format!("Error parsing UDF details: {e:?}"))
                        })?;

                        Ok(LogicalPlan::Extension(Extension {
                            node: Arc::new(SeafowlExtensionNode::CreateFunction(CreateFunction {
                                name: name.to_string(),
                                details: function_details,
                                output_schema: Arc::new(DFSchema::empty())
                            })),
                        }))
                    },
                Statement::Truncate { table_name, partitions} => {
                    let table_name = table_name.to_string();
                    let table_id = if partitions.is_none() && !table_name.is_empty() {
                        match self.try_get_seafowl_table(&table_name) {
                            Ok(seafowl_table) => Some(seafowl_table.table_id),
                            Err(_) => return Err(Error::Internal(format!(
                                "Table with name {table_name} not found"
                            )))
                        }
                    } else {
                        None
                    };

                    Ok(LogicalPlan::Extension(Extension {
                        node: Arc::new(SeafowlExtensionNode::Vacuum(Vacuum {
                            partitions: partitions.is_some(),
                            table_id,
                            output_schema: Arc::new(DFSchema::empty())
                        })),
                    }))
                }
                _ => Err(Error::NotImplemented(format!(
                    "Unsupported SQL statement: {s:?}"
                ))),
            },
            DFStatement::DescribeTable(s) => query_planner.describe_table_to_plan(s),
            DFStatement::CreateExternalTable(c) => {
                query_planner.external_table_to_plan(c)
            }
        }
    }

    async fn create_logical_plan(&self, sql: &str) -> Result<LogicalPlan> {
        let mut statements = self.parse_query(sql).await?;

        if statements.len() != 1 {
            return Err(Error::NotImplemented(
                "The context currently only supports a single SQL statement".to_string(),
            ));
        }

        self.create_logical_plan_from_statement(statements.pop().unwrap())
            .await
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
            LogicalPlan::CreateExternalTable(
                cmd @ CreateExternalTable {
                    ref name,
                    ref location,
                    ..
                },
            ) => {
                let resolved_reference = self.resolve_staging_ref(name)?;

                // Replace the table name with the fully qualified one that has our staging schema
                let mut cmd = cmd.clone();
                cmd.name = reference_to_name(&resolved_reference);

                let (location, is_http) = match try_prepare_http_url(location) {
                    Some(new_loc) => (new_loc, true),
                    None => (location.into(), false),
                };

                // Disallow the seafowl:// scheme (which is registered with DataFusion as our internal
                // object store but shouldn't be accessible via CREATE EXTERNAL TABLE)
                if location
                    .starts_with(format!("{INTERNAL_OBJECT_STORE_SCHEME}://").as_str())
                {
                    return Err(DataFusionError::Plan(format!(
                        "Invalid URL scheme for location {location:?}"
                    )));
                }

                // try_prepare_http_url changes the url in case of the HTTP object store
                // (to route _all_ HTTP URLs to our object store, not just specific hosts),
                // so inject it into the CreateExternalTable command as well.
                cmd.location = location;

                self.create_external_table(&cmd, !is_http).await
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

                self.execute_plan_to_table(&physical, None, Some(name.to_string()), None)
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
                                None,
                                Some(table.table_version_id),
                            )
                            .await?;

                            Ok(make_dummy_exec())
                        }
                        SeafowlExtensionNode::Update(Update {
                            table,
                            selection,
                            assignments,
                            ..
                        }) => {
                            // Load all pre-existing partitions
                            let partitions = self
                                .partition_catalog
                                .load_table_partitions(table.table_version_id)
                                .await?;

                            // By default (e.g. when there is no qualifier/selection, or we somehow
                            // fail to prune partitions) update all partitions
                            let mut partitions_to_update =
                                HashSet::<PhysicalPartitionId>::from_iter(
                                    partitions.iter().map(|p| p.partition_id.unwrap()),
                                );

                            let schema = table.schema().as_ref().clone();
                            let mut selection_expr = None;

                            // Try to scope down partition ids which need to be updated with pruning
                            if let Some(expr) = selection {
                                selection_expr = Some(create_physical_expr(
                                    &expr.clone(),
                                    &schema.clone().to_dfschema()?,
                                    &schema,
                                    &ExecutionProps::new(),
                                )?);

                                match SeafowlPruningStatistics::from_partitions(
                                    partitions.clone(),
                                    table.schema(),
                                ) {
                                    Ok(pruning_stats) => {
                                        partitions_to_update = HashSet::from_iter(
                                                pruning_stats
                                                    .prune(&[expr.clone()])
                                                    .await
                                                    .iter()
                                                    .map(|p| p.partition_id.unwrap()),
                                            );
                                    }
                                    Err(error) => warn!(
                                        "Failed constructing pruning statistics for table {} (version: {}) during UPDATE execution: {}",
                                        table.name, table.table_version_id, error
                                    )
                                }
                            }

                            let mut final_partition_ids =
                                Vec::with_capacity(partitions.len());

                            // Deduplicate assignments (we have to keep them as a vector in order
                            // to keep the order of column name -> expression mapping)
                            let assignment_map = HashMap::from_iter(assignments.clone());

                            let mut update_plan: Arc<dyn ExecutionPlan>;
                            let project_expressions = project_expressions(
                                &schema,
                                &assignment_map,
                                selection_expr,
                            )?;

                            // Iterate over partitions, updating the ones affected by the selection,
                            // while re-using the rest
                            for (keep, group) in
                                group_partitions(partitions, |p: &SeafowlPartition| {
                                    !partitions_to_update
                                        .contains(&p.partition_id.unwrap())
                                })
                            {
                                if keep {
                                    // Inherit the partition(s) as is from the previous
                                    // table version
                                    final_partition_ids.extend(
                                        group.iter().map(|p| p.partition_id.unwrap()),
                                    );
                                    continue;
                                }

                                let scan_plan = table
                                    .partition_scan_plan(
                                        None,
                                        group,
                                        &[],
                                        None,
                                        self.internal_object_store.inner.clone(),
                                    )
                                    .await?;

                                update_plan = Arc::new(ProjectionExec::try_new(
                                    project_expressions.clone(),
                                    scan_plan,
                                )?);

                                final_partition_ids.extend(
                                    self.execute_plan_to_partitions(
                                        &update_plan,
                                        Some(table.schema()),
                                    )
                                    .await?,
                                );
                            }

                            // Create a new blank table version
                            let new_table_version_id = self
                                .table_catalog
                                .create_new_table_version(table.table_version_id, false)
                                .await?;

                            // Link the new table version with the corresponding partitions
                            self.partition_catalog
                                .append_partitions_to_table(
                                    final_partition_ids,
                                    new_table_version_id,
                                )
                                .await?;

                            Ok(make_dummy_exec())
                        }
                        SeafowlExtensionNode::Delete(Delete {
                            table, selection, ..
                        }) => {
                            // If no qualifier is specified we're basically truncating the table;
                            // Make a new (empty) table version and finish.
                            let new_table_version_id = self
                                .table_catalog
                                .create_new_table_version(table.table_version_id, false)
                                .await?;

                            if let Some(expr) = selection {
                                // A WHERE clause has been used; employ it to prune the filtration
                                // down to only a subset of partitions, re-use the rest as is

                                // Load all pre-existing partitions
                                let partitions = self
                                    .partition_catalog
                                    .load_table_partitions(table.table_version_id)
                                    .await?;

                                // To simulate the effect of a WHERE clause from a DELETE, we
                                // need to use the inverse clause in a SELECT when filtering
                                let filter = create_physical_expr(
                                    &expr.clone().not(),
                                    &table.schema.arrow_schema.clone().to_dfschema()?,
                                    table.schema().as_ref(),
                                    &ExecutionProps::new(),
                                )?;

                                let mut final_partition_ids =
                                    Vec::with_capacity(partitions.len());

                                match SeafowlPruningStatistics::from_partitions(
                                    partitions.clone(),
                                    table.schema(),
                                ) {
                                    Ok(pruning_stats) => {
                                        // Determine the set of all partition ids that will need to
                                        // be filtered
                                        let partitions_to_filter =
                                            HashSet::<PhysicalPartitionId>::from_iter(
                                                pruning_stats
                                                    .prune(&[expr.clone()])
                                                    .await
                                                    .iter()
                                                    .map(|p| p.partition_id.unwrap()),
                                            );

                                        for (keep, group) in group_partitions(
                                            partitions,
                                            |p: &SeafowlPartition| {
                                                !partitions_to_filter
                                                    .contains(&p.partition_id.unwrap())
                                            },
                                        ) {
                                            if keep {
                                                // Inherit the partition(s) as is from the previous
                                                // table version
                                                final_partition_ids.extend(
                                                    group
                                                        .iter()
                                                        .map(|p| p.partition_id.unwrap()),
                                                );
                                                continue;
                                            }

                                            // Get the plan which will eliminate the affected rows
                                            let filter_plan = table
                                                .partition_filter_plan(
                                                    group,
                                                    filter.clone(),
                                                    &[expr.clone().not()],
                                                    self.internal_object_store
                                                        .inner
                                                        .clone(),
                                                )
                                                .await?;

                                            debug!(
                                                "Prepared delete filter plan: {:?}",
                                                &filter_plan
                                            );

                                            final_partition_ids.extend(
                                                self.execute_plan_to_partitions(
                                                    &filter_plan,
                                                    None,
                                                )
                                                .await?,
                                            );
                                        }
                                    }
                                    Err(error) => {
                                        warn!(
                                            "Failed constructing pruning statistics for table {} (version: {}) during DELETE execution: {}",
                                            table.name, table.table_version_id, error
                                        );

                                        // Fallback to scan + filter across all partitions
                                        let filter_plan = table
                                            .partition_filter_plan(
                                                partitions,
                                                filter.clone(),
                                                &[expr.clone().not()],
                                                self.internal_object_store.inner.clone(),
                                            )
                                            .await?;

                                        final_partition_ids = self
                                            .execute_plan_to_partitions(
                                                &filter_plan,
                                                None,
                                            )
                                            .await?;
                                    }
                                }

                                // Link the new table version with the corresponding partitions
                                self.partition_catalog
                                    .append_partitions_to_table(
                                        final_partition_ids,
                                        new_table_version_id,
                                    )
                                    .await?;
                            }

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
                                                "Schema {schema:?} does not exist!"
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
                        SeafowlExtensionNode::Vacuum(Vacuum {
                            partitions,
                            table_id,
                            ..
                        }) => {
                            if *partitions {
                                gc_partitions(self).await
                            } else {
                                match self
                                    .table_catalog
                                    .delete_old_table_versions(*table_id)
                                    .await
                                {
                                    Ok(row_count) => {
                                        info!("Deleted {} old table versions, cleaning up partitions", row_count);
                                        gc_partitions(self).await
                                    }
                                    Err(error) => {
                                        return Err(Error::Internal(format!(
                                        "Failed to delete old table versions: {error:?}"
                                    )))
                                    }
                                }
                            }

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
            let new_table_name = format!("{schema_name}.{table_name}");

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
                                "The table {new_table_name} already exists but has a different schema than the one provided.")
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

        self.execute_plan_to_table(&plan, None, full_table_name, from_table_version)
            .await?;

        Ok(true)
    }
}

#[cfg(test)]
pub mod test_utils {
    use std::sync::Arc;

    use mockall::predicate;
    use object_store::memory::InMemory;
    use parking_lot::RwLock;

    use crate::{
        catalog::{
            MockFunctionCatalog, MockPartitionCatalog, MockTableCatalog, TableCatalog,
            DEFAULT_DB, DEFAULT_SCHEMA,
        },
        object_store::http::add_http_object_store,
        provider::{SeafowlCollection, SeafowlDatabase},
    };

    use datafusion::{
        arrow::datatypes::{
            DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
        },
        catalog::schema::MemorySchemaProvider,
        prelude::SessionConfig,
    };

    use crate::config::context::build_context;
    use crate::config::schema;
    use crate::config::schema::{Catalog, SeafowlConfig, Sqlite};
    use crate::system_tables::SystemSchemaProvider;
    use sqlx::sqlite::SqliteJournalMode;
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
        let config = SeafowlConfig {
            object_store: schema::ObjectStore::InMemory(schema::InMemory {}),
            catalog: Catalog::Sqlite(Sqlite {
                dsn: "sqlite://:memory:".to_string(),
                journal_mode: SqliteJournalMode::Wal,
                read_only: false,
            }),
            frontend: Default::default(),
            runtime: Default::default(),
            misc: Default::default(),
        };
        build_context(&config).await.unwrap()
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
                    partition_id: Some(1),
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
                tables: RwLock::new(tables),
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
                    system_schema: Arc::new(SystemSchemaProvider::new(
                        Arc::from("testdb"),
                        Arc::new(MockTableCatalog::new()),
                    )),
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

        let object_store = Arc::new(InternalObjectStore {
            inner: Arc::new(InMemory::new()),
            config: schema::ObjectStore::InMemory(schema::InMemory {}),
        });
        session.runtime_env().register_object_store(
            INTERNAL_OBJECT_STORE_SCHEME,
            "",
            object_store.inner.clone(),
        );

        DefaultSeafowlContext {
            inner: session,
            table_catalog: Arc::new(table_catalog),
            partition_catalog: partition_catalog_ptr,
            function_catalog: Arc::new(function_catalog),
            internal_object_store: object_store,
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
    use tempfile::TempDir;

    use std::sync::Arc;

    use datafusion::execution::disk_manager::DiskManagerConfig;
    use mockall::predicate;
    use object_store::memory::InMemory;
    use rstest::rstest;

    use crate::context::test_utils::mock_context_with_catalog_assertions;

    use crate::config::schema;
    use datafusion::assert_batches_eq;
    use datafusion::from_slice::FromSlice;
    use datafusion::physical_plan::memory::MemoryExec;
    use itertools::Itertools;
    use object_store::local::LocalFileSystem;

    use super::*;

    use super::test_utils::{in_memory_context, mock_context};

    const PARTITION_1_FILE_NAME: &str =
        "90e86b04eb8fd3b0feaa7156282fc45a26932ce1cec905a8f3905eba97532f94.parquet";
    const PARTITION_2_FILE_NAME: &str =
        "0572290deae3900339ce1638395a3b89ca981831bbf9a2c28fbed890139f1a0d.parquet";

    const EXPECTED_INSERT_FILE_NAME: &str =
        "6302ae550c00b5cda9add7eb99071b188d0bfd2835b405447be06f8865d159cc.parquet";

    fn to_min_max_value(value: ScalarValue) -> Arc<Option<Vec<u8>>> {
        Arc::from(scalar_value_to_bytes(&value))
    }

    async fn assert_uploaded_objects(
        object_store: Arc<InternalObjectStore>,
        expected: Vec<Path>,
    ) {
        let actual = object_store
            .inner
            .list(None)
            .await
            .unwrap()
            .map_ok(|meta| meta.location)
            .try_collect::<Vec<Path>>()
            .await
            .map(|p| p.into_iter().sorted().collect_vec())
            .unwrap();
        assert_eq!(expected.into_iter().sorted().collect_vec(), actual);
    }

    #[rstest]
    #[case::in_memory_object_store_standard(false)]
    #[case::local_object_store_test_renames(true)]
    #[tokio::test]
    async fn test_plan_to_object_storage(#[case] is_local: bool) {
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

        let (object_store, _tmpdir) = if is_local {
            // Return tmp_dir to the upper scope so that we don't delete the temporary directory
            // until after the test is done
            let tmp_dir = TempDir::new().unwrap();

            (
                Arc::new(InternalObjectStore {
                    inner: Arc::new(
                        LocalFileSystem::new_with_prefix(tmp_dir.path()).unwrap(),
                    ),
                    config: schema::ObjectStore::Local(schema::Local {
                        data_dir: tmp_dir.path().to_string_lossy().to_string(),
                    }),
                }),
                Some(tmp_dir),
            )
        } else {
            (
                Arc::new(InternalObjectStore {
                    inner: Arc::new(InMemory::new()),
                    config: schema::ObjectStore::InMemory(schema::InMemory {}),
                }),
                None,
            )
        };

        let disk_manager = DiskManager::try_new(DiskManagerConfig::new()).unwrap();
        let partitions = plan_to_object_store(
            &sf_context.inner.state(),
            &execution_plan,
            None,
            object_store.clone(),
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
                    partition_id: None,
                    object_storage_id: Arc::from(PARTITION_1_FILE_NAME.to_string()),
                    row_count: 2,
                    columns: Arc::new(vec![
                        PartitionColumn {
                            name: Arc::from("timestamp".to_string()),
                            r#type: Arc::from("{\"children\":[],\"name\":\"timestamp\",\"nullable\":true,\"type\":{\"name\":\"utf8\"}}".to_string()),
                            min_value: Arc::new(None),
                            max_value: Arc::new(None),
                            null_count: Some(0),
                        },
                        PartitionColumn {
                            name: Arc::from("integer".to_string()),
                            r#type: Arc::from(
                                "{\"children\":[],\"name\":\"integer\",\"nullable\":true,\"type\":{\"bitWidth\":64,\"isSigned\":true,\"name\":\"int\"}}"
                                    .to_string()
                            ),
                            min_value: to_min_max_value(ScalarValue::Int64(Some(12))),
                            max_value: to_min_max_value(ScalarValue::Int64(Some(42))),
                            null_count: Some(0),
                        },
                        PartitionColumn {
                            name: Arc::from("varchar".to_string()),
                            r#type: Arc::from("{\"children\":[],\"name\":\"varchar\",\"nullable\":true,\"type\":{\"name\":\"utf8\"}}".to_string()),
                            min_value: Arc::new(None),
                            max_value: Arc::new(None),
                            null_count: Some(0),
                        }
                    ])
                },
                SeafowlPartition {
                    partition_id: None,
                    object_storage_id: Arc::from(PARTITION_2_FILE_NAME.to_string()),
                    row_count: 2,
                    columns: Arc::new(vec![
                        PartitionColumn {
                            name: Arc::from("timestamp".to_string()),
                            r#type: Arc::from("{\"children\":[],\"name\":\"timestamp\",\"nullable\":true,\"type\":{\"name\":\"utf8\"}}".to_string()),
                            min_value: Arc::new(None),
                            max_value: Arc::new(None),
                            null_count: Some(0),
                        },
                        PartitionColumn {
                            name: Arc::from("integer".to_string()),
                            r#type: Arc::from(
                                "{\"children\":[],\"name\":\"integer\",\"nullable\":true,\"type\":{\"bitWidth\":64,\"isSigned\":true,\"name\":\"int\"}}"
                                    .to_string()
                            ),
                            min_value: to_min_max_value(ScalarValue::Int64(Some(22))),
                            max_value: to_min_max_value(ScalarValue::Int64(Some(32))),
                            null_count: Some(0),
                        },
                        PartitionColumn {
                            name: Arc::from("varchar".to_string()),
                            r#type: Arc::from("{\"children\":[],\"name\":\"varchar\",\"nullable\":true,\"type\":{\"name\":\"utf8\"}}".to_string()),
                            min_value: Arc::new(None),
                            max_value: Arc::new(None),
                            null_count: Some(0),
                        }
                    ])
                },
            ]
        );

        assert_uploaded_objects(
            object_store,
            vec![
                Path::from(PARTITION_1_FILE_NAME.to_string()),
                Path::from(PARTITION_2_FILE_NAME.to_string()),
            ],
        )
        .await;
    }

    #[rstest]
    #[case::record_batches_smaller_than_partitions(
        5,
        vec![vec![vec![0, 1, 2], vec![3, 4, 5]], vec![vec![6, 7, 8], vec![9, 10, 11]]],
        vec![vec![0, 1, 2, 3, 4], vec![5, 6, 7, 8, 9], vec![10, 11]])
    ]
    #[case::record_batches_same_size_as_partitions(
        3,
        vec![vec![vec![0, 1, 2], vec![3, 4, 5]], vec![vec![6, 7, 8], vec![9, 10, 11]]],
        vec![vec![0, 1, 2], vec![3, 4, 5], vec![6, 7, 8], vec![9, 10, 11]])
    ]
    #[case::record_batches_larer_than_partitions(
        2,
        vec![vec![vec![0, 1, 2], vec![3, 4, 5]], vec![vec![6, 7, 8], vec![9, 10, 11]]],
        vec![vec![0, 1], vec![2, 3], vec![4, 5], vec![6, 7], vec![8, 9], vec![10, 11]])
    ]
    #[case::record_batches_of_irregular_size(
        3,
        vec![vec![vec![0, 1], vec![2, 3, 4]], vec![vec![5]], vec![vec![6, 7, 8, 9], vec![10, 11]]],
        vec![vec![0, 1, 2], vec![3, 4, 5], vec![6, 7, 8], vec![9, 10, 11]])
    ]
    #[tokio::test]
    async fn test_plan_to_object_storage_partition_chunking(
        #[case] max_partition_size: u32,
        #[case] input_partitions: Vec<Vec<Vec<i32>>>,
        #[case] output_partitions: Vec<Vec<i32>>,
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

        let object_store = Arc::new(InternalObjectStore {
            inner: Arc::new(InMemory::new()),
            config: schema::ObjectStore::InMemory(schema::InMemory {}),
        });
        let disk_manager = DiskManager::try_new(DiskManagerConfig::new()).unwrap();
        let partitions = plan_to_object_store(
            &sf_context.inner.state(),
            &execution_plan,
            None,
            object_store,
            disk_manager,
            max_partition_size,
        )
        .await
        .unwrap();

        for i in 0..output_partitions.len() {
            assert_eq!(partitions[i].row_count, output_partitions[i].len() as i32);

            assert_eq!(
                partitions[i].columns,
                Arc::new(vec![PartitionColumn {
                    name: Arc::from("some_number"),
                    r#type: Arc::from(
                        r#"{"children":[],"name":"some_number","nullable":true,"type":{"bitWidth":32,"isSigned":true,"name":"int"}}"#
                    ),
                    min_value: to_min_max_value(ScalarValue::Int32(
                        output_partitions[i].iter().min().copied()
                    )),
                    max_value: to_min_max_value(ScalarValue::Int32(
                        output_partitions[i].iter().max().copied()
                    )),
                    null_count: Some(0),
                }])
            );
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
            format!("{plan:?}"),
            "Insert: some_table\
            \n  Projection: CAST(column1 AS Date64) AS date, CAST(column2 AS Float64) AS value\
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

        assert_eq!(format!("{plan:?}"), "Insert: some_table\
        \n  Projection: CAST(my_date AS Date64) AS date, CAST(my_value AS Float64) AS value\
        \n    Projection: testdb.testcol.some_table.date AS my_date, testdb.testcol.some_table.value AS my_value\
        \n      TableScan: testdb.testcol.some_table");
    }

    #[tokio::test]
    async fn test_execute_insert_from_other_table() -> Result<()> {
        let context = Arc::new(in_memory_context().await);
        context
            .collect(
                context
                    .plan_query(
                        "CREATE TABLE test_table (\"key\" INTEGER, value STRING);",
                    )
                    .await?,
            )
            .await?;

        context
            .collect(
                context
                    .plan_query("INSERT INTO test_table VALUES (1, 'one'), (2, 'two');")
                    .await?,
            )
            .await?;

        context
        .collect(
            context
                .plan_query("INSERT INTO test_table(key, value) SELECT * FROM test_table WHERE value = 'two'")
                .await?,
        )
        .await?;

        let results = context
            .collect(
                context
                    .plan_query("SELECT * FROM test_table ORDER BY key ASC")
                    .await?,
            )
            .await?;

        let expected = vec![
            "+-----+-------+",
            "| key | value |",
            "+-----+-------+",
            "| 1   | one   |",
            "| 2   | two   |",
            "| 2   | two   |",
            "+-----+-------+",
        ];
        assert_batches_eq!(expected, &results);

        Ok(())
    }

    #[tokio::test]
    async fn test_execute_insert_from_other_table_schema_qualifier() -> Result<()> {
        let context = Arc::new(in_memory_context().await);
        context
            .collect(
                context
                    .plan_query(
                        "CREATE TABLE test_table (\"key\" INTEGER, value STRING);",
                    )
                    .await?,
            )
            .await?;

        context
            .collect(
                context
                    .plan_query(
                        "INSERT INTO public.test_table VALUES (1, 'one'), (2, 'two');",
                    )
                    .await?,
            )
            .await?;

        context
        .collect(
            context
                .plan_query("INSERT INTO test_table(key, value) SELECT * FROM public.test_table WHERE value = 'two'")
                .await?,
        )
        .await?;

        let results = context
            .collect(
                context
                    .plan_query("SELECT * FROM test_table ORDER BY key ASC")
                    .await?,
            )
            .await?;

        let expected = vec![
            "+-----+-------+",
            "| key | value |",
            "+-----+-------+",
            "| 1   | one   |",
            "| 2   | two   |",
            "| 2   | two   |",
            "+-----+-------+",
        ];
        assert_batches_eq!(expected, &results);

        Ok(())
    }

    async fn get_logical_plan(query: &str) -> String {
        let sf_context = mock_context().await;

        let plan = sf_context.create_logical_plan(query).await.unwrap();
        format!("{plan:?}")
    }

    #[tokio::test]
    async fn test_plan_create_schema_name_in_quotes() {
        assert_eq!(
            get_logical_plan("CREATE SCHEMA schema_name;").await,
            "CreateCatalogSchema: \"schema_name\""
        );
        assert_eq!(
            get_logical_plan("CREATE SCHEMA \"schema_name\";").await,
            "CreateCatalogSchema: \"schema_name\""
        );
    }

    #[tokio::test]
    async fn test_plan_rename_table_name_in_quotes() {
        assert_eq!(
            get_logical_plan("ALTER TABLE \"testcol\".\"some_table\" RENAME TO \"testcol\".\"some_table_2\"").await,
            "RenameTable: some_table to testcol.some_table_2"
        );
    }

    #[tokio::test]
    async fn test_plan_drop_table_name_in_quotes() {
        assert_eq!(
            get_logical_plan("DROP TABLE \"testcol\".\"some_table\"").await,
            "DropTable: \"testcol.some_table\" if not exist:=false"
        );
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
            format!("{plan:?}"),
            "Insert: some_table\
            \n  Projection: CAST(column1 AS Date64) AS date, CAST(column2 AS Float64) AS value\
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
                        assert_eq!(*partitions, vec![SeafowlPartition {
                            partition_id: None,
                            object_storage_id: Arc::from(EXPECTED_INSERT_FILE_NAME),
                                row_count: 1,
                                columns: Arc::new(vec![
                                    PartitionColumn {
                                        name: Arc::from("date"),
                                        r#type: Arc::from("{\"children\":[],\"name\":\"date\",\"nullable\":true,\"type\":{\"name\":\"date\",\"unit\":\"MILLISECOND\"}}"),
                                        min_value: Arc::new(None),
                                        max_value: Arc::new(None),
                                        null_count: Some(0),
                                    },
                                    PartitionColumn {
                                        name: Arc::from("value"),
                                        r#type: Arc::from("{\"children\":[],\"name\":\"value\",\"nullable\":true,\"type\":{\"name\":\"floatingpoint\",\"precision\":\"DOUBLE\"}}"),
                                        min_value: Arc::new(scalar_value_to_bytes(&ScalarValue::Float64(Some(42.0)))),
                                        max_value: Arc::new(scalar_value_to_bytes(&ScalarValue::Float64(Some(42.0)))),
                                        null_count: Some(0),
                                    },
                                ],)
                            },]);
                        true
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
                    .with(predicate::eq(0), predicate::eq(true))
                    .return_once(|_, _| Ok(1));
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
        assert_uploaded_objects(
            store,
            vec![Path::from(EXPECTED_INSERT_FILE_NAME.to_string())],
        )
        .await;
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
                "input_types": ["float"],
                "return_type": "float",
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

    #[tokio::test]
    async fn test_register_invalid_udf() -> Result<()> {
        let sf_context = mock_context().await;

        // Source: https://gist.github.com/going-digital/02e46c44d89237c07bc99cd440ebfa43
        let plan = sf_context
            .plan_query(
                r#"CREATE FUNCTION invalidfn AS '
            {
                "entrypoint": "invalidfn",
                "language": "wasmMessagePack",
                "input_types": ["float"],
                "return_type": "float",
                "data": ""
            }';"#,
            )
            .await;
        assert!(plan.is_err());
        assert!(plan.err().unwrap().to_string().starts_with(
            "Internal error: Error initializing WASM + MessagePack UDF \"invalidfn\": Internal(\"Error loading WASM module: failed to parse WebAssembly module"));
        Ok(())
    }
}
