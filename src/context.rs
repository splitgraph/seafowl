// DataFusion bindings

use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD, Engine};
use bytes::BytesMut;
use std::borrow::Cow;
use std::ops::Not;

use datafusion::datasource::TableProvider;
use datafusion::parquet::basic::{Compression, ZstdLevel};
use itertools::Itertools;
use std::collections::HashMap;
use tokio::fs::File as AsyncFile;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};

use std::fs::File;

use datafusion::execution::context::SessionState;
use datafusion::execution::DiskManager;

use crate::datafusion::parser::{DFParser, Statement as DFStatement};
use crate::datafusion::utils::build_schema;
use crate::object_store::http::try_prepare_http_url;
use crate::object_store::wrapped::InternalObjectStore;
use crate::wasm_udf::wasm::create_udf_from_wasm;
use futures::{StreamExt, TryStreamExt};

#[cfg(test)]
use mockall::automock;
use object_store::path::Path;

use sqlparser::ast::{
    AlterTableOperation, CreateFunctionBody, Expr as SqlExpr, FunctionDefinition,
    ObjectType, Query, Statement, TableFactor, TableWithJoins,
};

use arrow_schema::{DataType, TimeUnit};
use chrono::Duration;
use std::ops::Deref;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use datafusion::common::{DFSchema, FileType};
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
pub use datafusion::error::{DataFusionError as Error, Result};
use datafusion::physical_expr::execution_props::ExecutionProps;
use datafusion::physical_expr::expressions::{cast, Column};
use datafusion::physical_expr::{create_physical_expr, PhysicalExpr};
use datafusion::physical_optimizer::pruning::PruningPredicate;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::sql::parser::{CopyToSource, CopyToStatement};
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
        EmptyRecordBatchStream, ExecutionPlan, SendableRecordBatchStream,
    },
    prelude::SessionContext,
    sql::TableReference,
};
use datafusion_common::OwnedTableReference;

use datafusion_expr::logical_plan::{
    CreateCatalog, CreateCatalogSchema, CreateExternalTable, CreateMemoryTable,
    DropTable, Extension, LogicalPlan, Projection,
};
use datafusion_expr::{DdlStatement, DmlStatement, Filter, WriteOp};
use deltalake::action::{Action, Add, DeltaOperation, Remove, SaveMode};
use deltalake::operations::create::CreateBuilder;
use deltalake::operations::transaction::commit;
use deltalake::operations::vacuum::VacuumBuilder;
use deltalake::writer::create_add;
use deltalake::{DeltaTable, Schema as DeltaSchema};
use log::{debug, info, warn};
use parking_lot::RwLock;
use tempfile::TempPath;
use tokio::sync::Semaphore;
use url::Url;
use uuid::Uuid;

use crate::catalog::{DEFAULT_SCHEMA, STAGING_SCHEMA};
use crate::config::context::{build_object_store, build_state_with_table_factories};
use crate::config::schema;
use crate::config::schema::{GCS, S3};
use crate::datafusion::visit::VisitorMut;
use crate::delta_rs::backports::parquet_scan_from_actions;
#[cfg(test)]
use crate::frontend::http::tests::deterministic_uuid;
use crate::provider::project_expressions;
use crate::utils::gc_databases;
use crate::wasm_udf::data_types::{get_volatility, CreateFunctionDetails};
use crate::{
    catalog::{FunctionCatalog, TableCatalog},
    data_types::DatabaseId,
    nodes::{
        CreateFunction, CreateTable, DropFunction, DropSchema, RenameTable,
        SeafowlExtensionNode, Vacuum,
    },
    schema::Schema as SeafowlSchema,
    version::TableVersionProcessor,
};

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

#[cfg(test)]
fn get_uuid() -> Uuid {
    deterministic_uuid()
}

#[cfg(not(test))]
fn get_uuid() -> Uuid {
    Uuid::new_v4()
}

pub struct DefaultSeafowlContext {
    pub inner: SessionContext,
    pub table_catalog: Arc<dyn TableCatalog>,
    pub function_catalog: Arc<dyn FunctionCatalog>,
    pub internal_object_store: Arc<InternalObjectStore>,
    pub database: String,
    pub database_id: DatabaseId,
    pub all_database_ids: Arc<RwLock<HashMap<String, DatabaseId>>>,
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
        .set_compression(Compression::ZSTD(ZstdLevel::default())) // This defaults to MINIMUM_LEVEL
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
    store: Arc<InternalObjectStore>,
    prefix: String,
    disk_manager: Arc<DiskManager>,
    max_partition_size: u32,
) -> Result<Vec<Add>> {
    let mut current_partition_size = 0;
    let (mut current_partition_file_path, mut writer) =
        temp_partition_file_writer(disk_manager.clone(), plan.schema())?;
    let mut partition_file_paths = vec![current_partition_file_path];
    let mut partition_metadata = vec![];
    let mut tasks = vec![];

    // Iterate over Datafusion partitions and re-chunk them, since we want to enforce a pre-defined
    // partition size limit, which is not guaranteed by DF.
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
                let file_metadata = writer.close().map_err(DataFusionError::from)?;
                partition_metadata.push(file_metadata);

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
    let file_metadata = writer.close().map_err(DataFusionError::from)?;
    partition_metadata.push(file_metadata);

    info!("Starting upload of partition objects");
    let partitions_uuid = get_uuid();

    let sem = Arc::new(Semaphore::new(PARTITION_FILE_UPLOAD_MAX_CONCURRENCY));
    for (part, (partition_file_path, metadata)) in partition_file_paths
        .into_iter()
        .zip(partition_metadata)
        .enumerate()
    {
        let permit = Arc::clone(&sem).acquire_owned().await.ok();

        let store = store.clone();
        let prefix = prefix.clone();
        let handle: tokio::task::JoinHandle<Result<Add>> =
            tokio::task::spawn(async move {
                // Move the ownership of the semaphore permit into the task
                let _permit = permit;

                // This is taken from delta-rs `PartitionWriter::next_data_path`
                let file_name =
                    format!("part-{part:0>5}-{partitions_uuid}-c000.snappy.parquet");
                // NB: in order to exploit the fast upload path for local FS store we need to use
                // the internal object store here. However it is not rooted at the table directory
                // root, so we need to fully qualify the path with the appropriate uuid prefix.
                // On the other hand, when creating deltalake `Add`s below we only need the relative
                // path (just the file name).
                let location = Path::from(prefix).child(file_name.clone());

                let size = tokio::fs::metadata(
                    partition_file_path
                        .to_str()
                        .expect("Temporary Parquet file in the FS root"),
                )
                .await?
                .len() as i64;

                // For local FS stores, we can just move the file to the target location
                if let Some(result) =
                    store.fast_upload(&partition_file_path, &location).await
                {
                    result?;
                } else {
                    let file = AsyncFile::open(partition_file_path).await?;
                    let mut reader =
                        BufReader::with_capacity(PARTITION_FILE_BUFFER_SIZE, file);
                    let mut part_buffer =
                        BytesMut::with_capacity(PARTITION_FILE_MIN_PART_SIZE);

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

                // Create the corresponding Add action; currently we don't support partition columns
                // which simplifies things.
                let add = create_add(&HashMap::default(), file_name, size, &metadata)?;

                Ok(add)
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
        LogicalPlan::Dml(_)
            | LogicalPlan::Ddl(_)
            | LogicalPlan::Analyze(_)
            | LogicalPlan::Extension(_)
            | LogicalPlan::Copy(_)
    )
}

pub fn is_statement_read_only(statement: &DFStatement) -> bool {
    if let DFStatement::Statement(s) = statement {
        matches!(
            **s,
            Statement::Query(_)
                | Statement::Explain { .. }
                | Statement::ShowTables { .. }
        )
    } else {
        false
    }
}

// The only reason to keep this trait around (instead of migrating all the functions directly into
// DefaultSeafowlContext), is that `create_physical_plan` would then be a recursive async function,
// which works for traits, but not for structs: https://stackoverflow.com/a/74737853
//
// The workaround would be to box a future as the return of such functions, which isn't very
// appealing atm (involves heap allocations, and is not very readable).
//
// Alternatively, if we're sure that all recursive calls can be handled by the inner (DataFusion's)
// `create_physical_plan` we could also rewrite the calls explicitly like that, and thus break the
// recursion.
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
}

impl DefaultSeafowlContext {
    // Create a new `DefaultSeafowlContext` with a new inner context scoped to a different default DB
    pub async fn scope_to_database(
        &self,
        name: String,
    ) -> Result<Arc<DefaultSeafowlContext>> {
        let maybe_database_id = self.all_database_ids.read().get(name.as_str()).cloned();
        let database_id = match maybe_database_id {
            Some(db_id) => db_id,
            None => {
                // Perhaps the db was created on another node; try to reload from catalog
                let new_db_ids = self.table_catalog.load_database_ids().await?;
                new_db_ids
                    .get(name.as_str())
                    .cloned()
                    .map(|db_id| {self.all_database_ids.write().insert(name.clone(), db_id); db_id})
                    .ok_or_else(|| {
                        DataFusionError::Plan(format!(
                            "Unknown database {name}; try creating one with CREATE DATABASE first"
                        ))
                    })?
            }
        };

        // Swap the default database in the new internal context's session config
        let session_config = self
            .inner()
            .copied_config()
            .with_default_catalog_and_schema(name.clone(), DEFAULT_SCHEMA);

        let state =
            build_state_with_table_factories(session_config, self.inner().runtime_env());

        Ok(Arc::from(DefaultSeafowlContext {
            inner: SessionContext::with_state(state),
            table_catalog: self.table_catalog.clone(),
            function_catalog: self.function_catalog.clone(),
            internal_object_store: self.internal_object_store.clone(),
            database: name,
            database_id,
            all_database_ids: self.all_database_ids.clone(),
            max_partition_size: self.max_partition_size,
        }))
    }

    pub fn inner(&self) -> &SessionContext {
        &self.inner
    }

    /// Reload the context to apply / pick up new schema changes
    pub(crate) async fn reload_schema(&self) -> Result<()> {
        // DataFusion's catalog provider interface is not async, which means that we aren't really
        // supposed to perform IO when loading the list of schemas. On the other hand, as of DF 16
        // the schema provider allows for async fetching of tables. However, this isn't that helpful,
        // since for a query with multiple tables we'd have multiple separate DB hits to load them,
        // whereas below we load everything we need up front. (Furthermore, table existence and name
        // listing are still sync meaning we'd need the pre-load for them as well.)
        // We hence load all schemas and tables into memory before every query (otherwise writes
        // applied by a different Seafowl instance won't be visible by us).

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

    // Determine if some of the tables reference a non-latest version using table function syntax.
    // If so, rename the tables in the query by appending the explicit version to the name, and add
    // it to the schema provider's map inside a new session state.
    // Should become obsolete once `sqlparser-rs` introduces support for some form of the `AS OF`
    // clause: https://en.wikipedia.org/wiki/SQL:2011.
    async fn rewrite_time_travel_query(&self, q: &mut Query) -> Result<SessionState> {
        let mut version_processor =
            TableVersionProcessor::new(self.database.clone(), DEFAULT_SCHEMA.to_string());
        version_processor.visit_query(q);

        if version_processor.table_versions.is_empty() {
            // No time-travel syntax detected, just return the regular session state
            return Ok(self.inner.state());
        }

        debug!("Time travel query rewritten to: {}", q);

        // Create a new session context and session state, to avoid potential race
        // conditions leading to schema provider map leaking into other queries (and
        // thus polluting e.g. the information_schema output), or even worse reloading
        // the map and having the versioned query fail during execution.
        let session_ctx = SessionContext::with_state(self.inner.state());

        for (table, version) in &version_processor.table_versions {
            let name_with_version =
                TableVersionProcessor::table_with_version(table, version);

            let full_table_name = table.to_string();
            let mut resolved_ref = TableReference::from(full_table_name.as_str())
                .resolve(&self.database, DEFAULT_SCHEMA);

            // We only support datetime DeltaTable version specification for start
            let table_uuid = self.get_table_uuid(resolved_ref.clone()).await?;
            let table_object_store =
                self.internal_object_store.for_delta_table(table_uuid);
            let datetime = TableVersionProcessor::version_to_datetime(version)?;

            let mut delta_table = DeltaTable::new(table_object_store, Default::default());
            delta_table.load_with_datetime(datetime).await?;
            let table_provider_for_version = Arc::from(delta_table);

            resolved_ref.table = Cow::Borrowed(name_with_version.as_str());

            if !session_ctx.table_exist(resolved_ref.clone())? {
                session_ctx.register_table(resolved_ref, table_provider_for_version)?;
            }
        }

        Ok(session_ctx.state())
    }

    // Check that the TableReference doesn't have a database/schema in it.
    // We create all external tables in the staging schema (backed by DataFusion's
    // in-memory schema provider) instead.
    fn resolve_staging_ref(
        &self,
        name: &OwnedTableReference,
    ) -> Result<OwnedTableReference> {
        // NB: Since Datafusion 16.0.0 for external tables the parsed ObjectName is coerced into the
        // `OwnedTableReference::Bare` enum variant, since qualified names are not supported for them
        // (see `external_table_to_plan` in datafusion-sql).
        //
        // This means that any potential catalog/schema references get condensed into the name, so
        // we have to unravel that name here again, and then resolve it properly.
        let reference = TableReference::from(name.to_string());
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

        Ok(TableReference::from(resolved_reference).to_owned_reference())
    }

    /// Get a provider for a given table, return Err if it doesn't exist
    async fn get_table_provider(
        &self,
        table_name: impl Into<String>,
    ) -> Result<Arc<dyn TableProvider>> {
        let table_name = table_name.into();
        let table_ref = TableReference::from(table_name.as_str());

        let resolved_ref = table_ref.resolve(&self.database, DEFAULT_SCHEMA);

        self.inner
            .catalog(&resolved_ref.catalog)
            .ok_or_else(|| {
                Error::Plan(format!(
                    "failed to resolve catalog: {}",
                    resolved_ref.catalog
                ))
            })?
            .schema(&resolved_ref.schema)
            .ok_or_else(|| {
                Error::Plan(format!("failed to resolve schema: {}", resolved_ref.schema))
            })?
            .table(&resolved_ref.table)
            .await
            .ok_or_else(|| {
                Error::Plan(format!(
                    "'{}.{}.{}' not found",
                    resolved_ref.catalog, resolved_ref.schema, resolved_ref.table
                ))
            })
    }

    /// Resolve a table reference into a Delta table
    pub async fn try_get_delta_table<'a>(
        &self,
        table_name: impl Into<TableReference<'a>>,
    ) -> Result<DeltaTable> {
        let table_object_store = self
            .inner
            .table_provider(table_name)
            .await?
            .as_any()
            .downcast_ref::<DeltaTable>()
            .ok_or_else(|| {
                DataFusionError::Execution("Table {table_name} not found".to_string())
            })?
            .object_store();

        // We can't just keep hold of the downcasted ref from above because of
        // `temporary value dropped while borrowed`
        Ok(DeltaTable::new(table_object_store, Default::default()))
    }

    // Parse the uuid from the Delta table uri if available
    pub async fn get_table_uuid<'a>(
        &self,
        name: impl Into<TableReference<'a>>,
    ) -> Result<Uuid> {
        match self
            .inner
            .table_provider(name)
            .await?
            .as_any()
            .downcast_ref::<DeltaTable>()
        {
            None => {
                // TODO: try to load from DB if missing?
                Err(DataFusionError::Execution(
                    "Couldn't fetch table uuid".to_string(),
                ))
            }
            Some(delta_table) => {
                let table_uri = Path::from(delta_table.table_uri());
                let uuid = table_uri.parts().last().ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "Failed parsing the uuid suffix from uri {table_uri} for table {delta_table}"
                    ))
                })?;
                Ok(Uuid::try_parse(uuid.as_ref()).map_err(|err| {
                    DataFusionError::Execution(format!(
                        "Failed parsing uuid from {uuid:?}: {err}"
                    ))
                })?)
            }
        }
    }

    async fn create_delta_table<'a>(
        &self,
        name: impl Into<TableReference<'a>>,
        schema: &Schema,
    ) -> Result<Arc<DeltaTable>> {
        let table_ref: TableReference = name.into();
        let resolved_ref = table_ref.resolve(&self.database, DEFAULT_SCHEMA);
        let schema_name = resolved_ref.schema.clone();
        let table_name = resolved_ref.table.clone();

        let sf_schema = SeafowlSchema {
            arrow_schema: Arc::new(schema.clone()),
        };
        let collection_id = self
            .table_catalog
            .get_collection_id_by_name(&self.database, &schema_name)
            .await?
            .ok_or_else(|| {
                Error::Plan(format!("Schema {schema_name:?} does not exist!"))
            })?;

        let delta_schema = DeltaSchema::try_from(schema)?;

        // TODO: we could be doing this inside the DB itself (i.e. `... DEFAULT gen_random_uuid()`
        // in Postgres and `... DEFAULT (uuid())` in SQLite) however we won't be able to do it until
        // sqlx 0.7 is released (which has libsqlite3-sys > 0.25, with the SQLite version that has
        // the `uuid()` function).
        // Then we could create the table in our catalog first and try to create the delta table itself
        // with the returned uuid (and delete the catalog entry if the object store creation fails).
        // On the other hand that would complicate etag testing logic.
        let table_uuid = get_uuid();
        let table_object_store = self.internal_object_store.for_delta_table(table_uuid);

        // NB: there's also a uuid generated below for table's `DeltaTableMetaData::id`, so it would
        // be nice if those two could match
        let table = Arc::new(
            CreateBuilder::new()
                .with_object_store(table_object_store)
                .with_table_name(&*table_name)
                .with_columns(delta_schema.get_fields().clone())
                .with_comment(format!(
                    "Created by Seafowl version {}",
                    env!("CARGO_PKG_VERSION")
                ))
                .await?,
        );

        // We still persist the table into our own catalog, one reason is us being able to load all
        // tables and their schemas in bulk to satisfy information_schema queries.
        // Another is to keep track of table uuid's, which are used to construct the table uri.
        // We may look into doing this via delta-rs somehow eventually.
        self.table_catalog
            .create_table(collection_id, &table_name, &sf_schema, table_uuid)
            .await?;

        self.inner.register_table(resolved_ref, table.clone())?;
        debug!("Created new table {table}");
        Ok(table)
    }

    // Project incompatible data types if any to delta-rs compatible ones (for now ns -> us)
    async fn coerce_plan(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut incompatible_data_type = false;
        let schema = plan.schema().as_ref().clone();
        let projection = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(id, f)| {
                let col = Arc::new(Column::new(f.name(), id));
                match f.data_type() {
                    DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
                        incompatible_data_type = true;
                        let data_type =
                            DataType::Timestamp(TimeUnit::Microsecond, tz.clone());
                        Ok((cast(col, &schema, data_type)?, f.name().to_string()))
                    }
                    _ => Ok((col as _, f.name().to_string())),
                }
            })
            .collect::<Result<Vec<(Arc<dyn PhysicalExpr>, String)>>>()?;

        if incompatible_data_type {
            Ok(Arc::new(ProjectionExec::try_new(projection, plan)?))
        } else {
            Ok(plan)
        }
    }

    /// Generate the Delta table builder and execute the write
    pub async fn plan_to_delta_table<'a>(
        &self,
        name: impl Into<TableReference<'a>>,
        plan: &Arc<dyn ExecutionPlan>,
    ) -> Result<DeltaTable> {
        let table_uuid = self.get_table_uuid(name).await?;
        let table_object_store = self.internal_object_store.for_delta_table(table_uuid);

        // Upload partition files to table's root directory
        let adds = plan_to_object_store(
            &self.inner.state(),
            plan,
            self.internal_object_store.clone(),
            table_uuid.to_string(),
            self.inner.runtime_env().disk_manager.clone(),
            self.max_partition_size,
        )
        .await?;

        // Commit the write into a new version
        let mut table = DeltaTable::new(table_object_store, Default::default());
        table.load().await?;

        let actions: Vec<Action> = adds.into_iter().map(Action::add).collect();
        let op = DeltaOperation::Write {
            mode: SaveMode::Append,
            partition_by: None,
            predicate: None,
        };
        let version = commit(
            table.object_store().as_ref(),
            &actions,
            op,
            table.get_state(),
            None,
        )
        .await?;

        // TODO: if `DeltaTable::get_version_timestamp` was globally public we could also pass the
        // exact version timestamp, instead of creating one automatically in our own catalog (which
        // could lead to minor timestamp differences).
        self.table_catalog
            .create_new_table_version(table_uuid, version)
            .await?;

        debug!("Written table version {version} for {table}");
        Ok(table)
    }

    fn register_function(
        &self,
        name: &str,
        details: &CreateFunctionDetails,
    ) -> Result<()> {
        let function_code = STANDARD
            .decode(&details.data)
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
        self.inner.register_udf(function);

        Ok(())
    }

    pub(crate) async fn execute_stream(
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

    /// Append data from the provided file, creating a new schema/table if absent
    pub async fn file_to_table(
        &self,
        file_path: String,
        file_type: FileType,
        file_schema: Option<SchemaRef>,
        has_header: bool,
        schema_name: String,
        table_name: String,
    ) -> Result<DeltaTable> {
        // Reload the schema since `try_get_delta_table` relies on using DataFusion's
        // TableProvider interface (which we need to pre-populate with up to date
        // information on our tables)
        self.reload_schema().await?;

        let mut table_schema = None;

        // Check whether table already exists and ensure that the schema exists
        let table_exists = match self
            .inner
            .catalog(&self.database)
            .ok_or_else(|| Error::Plan(format!("Database {} not found!", self.database)))?
            .schema(&schema_name)
        {
            Some(_) => {
                // Schema exists, check if existing table's schema matches the new one
                match self.get_table_provider(&table_name).await {
                    Ok(table) => {
                        table_schema = Some(table.schema());
                        true
                    }
                    Err(_) => false,
                }
            }
            None => {
                // Schema doesn't exist; create one first, and then reload to pick it up
                self.table_catalog
                    .create_collection(self.database_id, &schema_name)
                    .await?;
                self.reload_schema().await?;
                false
            }
        };

        // Create a `ListingTable` that points to the specified file
        let table_path = ListingTableUrl::parse(file_path)?;
        let file_format: Arc<dyn FileFormat> = match file_type {
            FileType::CSV => Arc::new(CsvFormat::default().with_has_header(has_header)),
            FileType::PARQUET => Arc::new(ParquetFormat::default()),
            _ => {
                return Err(Error::Plan(format!(
                    "File type {file_type:?} not supported!"
                )));
            }
        };
        let listing_options = ListingOptions::new(file_format);

        // Resolve the final schema; take the one from the table if present, otherwise take the supplied
        // file schema, otherwise infer the schema from the file
        let schema = match table_schema.or(file_schema) {
            Some(schema) => schema,
            None => {
                listing_options
                    .infer_schema(&self.inner.state(), &table_path)
                    .await?
            }
        };

        let config = ListingTableConfig::new(table_path)
            .with_listing_options(listing_options)
            .with_schema(schema);

        let source = ListingTable::try_new(config)?;

        // Make a scan plan for the listing table, which will be the input for the target table
        let plan = source.scan(&self.inner.state(), None, &[], None).await?;

        let table_ref = TableReference::Full {
            catalog: Cow::from(self.database.clone()),
            schema: Cow::from(schema_name),
            table: Cow::from(table_name),
        };

        if !table_exists {
            self.create_delta_table(table_ref.clone(), plan.schema().as_ref())
                .await?;
        }

        self.plan_to_delta_table(table_ref, &plan).await
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
        // TODO: A couple of possible optimisations here:
        // 1. Do a visit of the statement AST, and then load the metadata for only the referenced identifiers.
        // 2. No need to load metadata for the TableProvider implementation maps when instantiating SqlToRel,
        //    since it's sufficient to have metadata for TableSource implementation in the logical query
        //    planning phase. We could use a lighter structure for that, and implement `ContextProvider` for
        //    it rather than for DefaultSeafowlContext.
        self.reload_schema().await?;

        // Create a mutable clone of the statement so that we can rewrite table names if we encounter
        // time travel syntax.
        // Alternatively, this could be done without the `mut`, except then we'd need to construct
        // and return a new `DFStatement` after the rewrite. In case of `Statement::Query` this is
        // straight-forward as that enum variant contains a struct, however `Statement::Insert` and
        // `Statement::CreateTable` wrap dozens of fields in curly braces, and we'd need to capture
        // and re-populate each of those, so it would be very verbose.
        // TODO: If `sqlparser-rs` encapsulates these fields inside a struct at some point remove
        // the mut and go with @ capture and de-structuring to pass along other fields unchanged.
        let mut stmt = statement.clone();

        match &mut stmt {
            DFStatement::Statement(ref mut s) => match &mut **s {
                Statement::Query(ref mut query) => {
                    let state = self.rewrite_time_travel_query(query).await?;
                    state.statement_to_plan(stmt).await
                }
                // Delegate generic queries to the basic DataFusion logical planner
                // (though note EXPLAIN [our custom query] will mean we have to implement EXPLAIN ourselves)
                Statement::Explain { .. }
                | Statement::ShowVariable { .. }
                | Statement::ShowTables { .. }
                | Statement::ShowColumns { .. }
                | Statement::CreateSchema { .. }
                | Statement::CreateView { .. }
                | Statement::CreateDatabase { .. } => self.inner.state().statement_to_plan(stmt).await,
                Statement::Insert{ ref mut source, .. } => {
                    let state = self.rewrite_time_travel_query(source).await?;
                    let plan = state.statement_to_plan(stmt).await?;
                    state.optimize(&plan)
                }
                Statement::Update {
                    table: TableWithJoins {relation: TableFactor::Table { alias: None, args: None, with_hints, .. }, joins },
                    ..
                }
                // We only support the most basic form of UPDATE (no aliases or FROM or joins)
                    if with_hints.is_empty() && joins.is_empty() => {
                    let state = self.inner.state();
                    let plan = state.statement_to_plan(stmt).await?;
                    state.optimize(&plan)
                },
                Statement::Delete{ .. } => {
                    let state = self.inner.state();
                    let plan = state.statement_to_plan(stmt).await?;
                    state.optimize(&plan)
                }
                Statement::Drop { object_type: ObjectType::Table, .. } => self.inner.state().statement_to_plan(stmt).await,
                Statement::Drop { object_type: ObjectType::Schema,
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
                    let schema = build_schema(columns.to_vec())?;
                    Ok(LogicalPlan::Extension(Extension {
                        node: Arc::new(SeafowlExtensionNode::CreateTable(CreateTable {
                            schema,
                            name: name.to_string(),
                            if_not_exists: *if_not_exists,
                            output_schema: Arc::new(DFSchema::empty())
                        })),
                    }))
                },

                // ALTER TABLE ... RENAME TO
                Statement::AlterTable { name, operation: AlterTableOperation::RenameTable {table_name: new_name }} => {
                    let old_table_name = name.to_string();
                    let new_table_name = new_name.to_string();

                    if self.get_table_provider(old_table_name.to_owned()).await.is_err() {
                        return Err(Error::Plan(
                            format!("Source table {old_table_name:?} doesn't exist")
                        ))
                    } else if self.get_table_provider(new_table_name.to_owned()).await.is_ok() {
                        return Err(Error::Plan(
                            format!("Target table {new_table_name:?} already exists")
                        ))
                    }

                    Ok(LogicalPlan::Extension(Extension {
                        node: Arc::new(SeafowlExtensionNode::RenameTable(RenameTable {
                            old_name: old_table_name,
                            new_name: new_table_name,
                            output_schema: Arc::new(DFSchema::empty())
                        })),
                    }))
                }

                // Other CREATE TABLE: SqlToRel only allows CreateTableAs statements and makes
                // a CreateMemoryTable node. We're fine with that, but we'll execute it differently.
                Statement::CreateTable { query: Some(ref mut input), .. } => {
                    let state = self.rewrite_time_travel_query(input).await?;
                    state.statement_to_plan(stmt).await
                },

                Statement::CreateFunction {
                    or_replace,
                    temporary: false,
                    name,
                    params: CreateFunctionBody { as_: Some( FunctionDefinition::SingleQuotedDef(details) ), .. },
                    ..
                } => {
                    // We abuse the fact that in CREATE FUNCTION AS [class_name], class_name can be an arbitrary string
                    // and so we can get the user to put some JSON in there
                    let function_details: CreateFunctionDetails = serde_json::from_str(details)
                        .map_err(|e| {
                            Error::Execution(format!("Error parsing UDF details: {e:?}"))
                        })?;

                        Ok(LogicalPlan::Extension(Extension {
                            node: Arc::new(SeafowlExtensionNode::CreateFunction(CreateFunction {
                                or_replace: *or_replace,
                                name: name.to_string(),
                                details: function_details,
                                output_schema: Arc::new(DFSchema::empty())
                            })),
                        }))
                    },
                Statement::Truncate { table_name, partitions, .. } => {
                    let table_name = if partitions.is_none() {
                        Some(table_name.to_string())
                    } else {
                        None
                    };

                    let mut database = None;
                    if let Some(sql_exprs) = partitions {
                        if let [SqlExpr::Identifier(name)] = sql_exprs.as_slice() {
                            database = Some(name.value.clone());
                        }
                    }

                    Ok(LogicalPlan::Extension(Extension {
                        node: Arc::new(SeafowlExtensionNode::Vacuum(Vacuum {
                            database,
                            table_name,
                            output_schema: Arc::new(DFSchema::empty())
                        })),
                    }))
                }
                Statement::DropFunction{
                    if_exists,
                    func_desc,
                    option: _
                } => {
                    let func_names: Vec<String> =
                        func_desc.iter().map(|desc| desc.name.to_string()).collect();
                        Ok(LogicalPlan::Extension(Extension {
                            node: Arc::new(SeafowlExtensionNode::DropFunction(DropFunction {
                                if_exists: *if_exists,
                                func_names,
                                output_schema: Arc::new(DFSchema::empty()),
                            }))
                        }))
                }
                _ => Err(Error::NotImplemented(format!(
                    "Unsupported SQL statement: {s:?}"
                ))),
            },
            DFStatement::CopyTo(CopyToStatement { ref mut source, .. }) => {
                let state = if let CopyToSource::Query(ref mut query) = source {
                    self.rewrite_time_travel_query(query).await?
                } else {
                    self.inner.state()
                };
                state.statement_to_plan(stmt).await
            }
            DFStatement::DescribeTableStmt(_) | DFStatement::CreateExternalTable(_) => {
                self.inner.state().statement_to_plan(stmt).await
            }
            DFStatement::Explain(_) => Err(Error::NotImplemented(format!(
                "Unsupported SQL statement: {statement:?}"
            ))),
        }
    }

    async fn create_logical_plan(&self, sql: &str) -> Result<LogicalPlan> {
        let mut statements = self.parse_query(sql).await?;
        println!("create_logical_plan() statements {:?}", statements);

        if statements.len() != 1 {
            return Err(Error::NotImplemented(
                "The context currently only supports a single SQL statement".to_string(),
            ));
        }

        let plan = self.create_logical_plan_from_statement(statements.pop().unwrap())
            .await;
        println!("create_logical_plan() plan {:?}", plan);
        plan
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
            LogicalPlan::Copy(_) => {
                let physical = self.inner.state().create_physical_plan(plan).await?;

                // Eagerly execute the COPY TO plan to align with other DML plans in here.
                self.collect(physical).await?;
                Ok(make_dummy_exec())
            }
            // CREATE EXTERNAL TABLE copied from DataFusion's source code
            // It uses ListingTable which queries data at a given location using the ObjectStore
            // abstraction (URL: scheme://some-path.to.file.parquet) and it's easier to reuse this
            // mechanism in our case too.
            LogicalPlan::Ddl(DdlStatement::CreateExternalTable(
                cmd @ CreateExternalTable {
                    ref name,
                    ref location,
                    ..
                },
            )) => {
                // Replace the table name with the fully qualified one that has our staging schema
                let mut cmd = cmd.clone();
                cmd.name = self.resolve_staging_ref(name)?;

                // try_prepare_http_url changes the url in case of the HTTP object store
                // (to route _all_ HTTP URLs to our object store, not just specific hosts),
                // so inject it into the CreateExternalTable command as well.
                cmd.location = match try_prepare_http_url(location) {
                    Some(new_loc) => new_loc,
                    None => location.into(),
                };

                // If the referenced table is in a cloud object store then register a new
                // corresponding object store dynamically:
                // 1. Using cmd.options if provided,
                // 2. Otherwise use the object store credentials from the config if it matches
                //    the object store kind.
                let table_path = ListingTableUrl::parse(&cmd.location)?;
                let scheme = table_path.scheme();
                let url: &Url = table_path.as_ref();
                if matches!(scheme, "s3" | "gs" | "gcs") {
                    let bucket = url
                        .host_str()
                        .ok_or_else(|| {
                            DataFusionError::Execution(format!(
                                "Not able to parse bucket name from url: {}",
                                url.as_str()
                            ))
                        })?
                        .to_string();

                    let config = if scheme == "s3" {
                        let s3 = if cmd.options.is_empty() && let schema::ObjectStore::S3(s3) = self.internal_object_store.config.clone() {
                            S3{ bucket, ..s3 }
                        } else {
                            S3::from_bucket_and_options(bucket, &cmd.options).map_err(|e| DataFusionError::Execution(e.to_string()))?
                        };
                        schema::ObjectStore::S3(s3)
                    } else {
                        let gcs = if cmd.options.is_empty() && let schema::ObjectStore::GCS(gcs) = self.internal_object_store.config.clone() {
                            GCS{ bucket, ..gcs }
                        } else {
                            GCS::from_bucket_and_options(bucket, &cmd.options)
                        };
                        schema::ObjectStore::GCS(gcs)
                    };

                    let object_store = build_object_store(&config);
                    self.inner
                        .runtime_env()
                        .register_object_store(url, object_store);
                }

                self.inner
                    .execute_logical_plan(LogicalPlan::Ddl(
                        DdlStatement::CreateExternalTable(cmd),
                    ))
                    .await?;
                Ok(make_dummy_exec())
            }
            LogicalPlan::Ddl(DdlStatement::CreateCatalogSchema(
                CreateCatalogSchema {
                    schema_name,
                    if_not_exists: _,
                    schema: _,
                },
            )) => {
                // CREATE SCHEMA
                // Create a schema and register it
                self.table_catalog
                    .create_collection(self.database_id, schema_name)
                    .await?;
                Ok(make_dummy_exec())
            }
            LogicalPlan::Ddl(DdlStatement::CreateCatalog(CreateCatalog {
                catalog_name,
                if_not_exists,
                ..
            })) => {
                if self
                    .table_catalog
                    .get_database_id_by_name(catalog_name)
                    .await?
                    .is_some()
                {
                    if !*if_not_exists {
                        return Err(DataFusionError::Plan(format!(
                            "Database {catalog_name} already exists"
                        )));
                    } else {
                        return Ok(make_dummy_exec());
                    }
                }

                // Persist DB into metadata catalog
                let database_id =
                    self.table_catalog.create_database(catalog_name).await?;

                // Create the corresponding default schema as well
                self.table_catalog
                    .create_collection(database_id, DEFAULT_SCHEMA)
                    .await?;

                // Update the shared in-memory map of DB names -> ids
                self.all_database_ids
                    .write()
                    .insert(catalog_name.clone(), database_id);

                Ok(make_dummy_exec())
            }
            LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(CreateMemoryTable {
                name,
                input,
                if_not_exists: _,
                or_replace: _,
                ..
            })) => {
                // This is actually CREATE TABLE AS
                let plan = self.create_physical_plan(input).await?;
                let plan = self.coerce_plan(plan).await?;

                // First create the table and then insert the data from the subquery
                // TODO: this means we'll have 2 table versions at the end, 1st from the create
                // and 2nd from the insert, while it seems more reasonable that in this case we have
                // only one
                self.create_delta_table(name, plan.schema().as_ref())
                    .await?;
                self.plan_to_delta_table(name, &plan).await?;

                Ok(make_dummy_exec())
            }
            LogicalPlan::Dml(DmlStatement {
                table_name,
                op: WriteOp::InsertInto,
                input,
                ..
            }) => {
                let physical = self.create_physical_plan(input).await?;

                self.plan_to_delta_table(table_name, &physical).await?;

                Ok(make_dummy_exec())
            }
            LogicalPlan::Dml(DmlStatement {
                table_name,
                op: WriteOp::Update,
                input,
                ..
            }) => {
                // Destructure input into projection expressions and the upstream scan/filter plan
                let LogicalPlan::Projection(Projection { expr, input, .. }) = &**input
                else {
                    return Err(DataFusionError::Plan(
                        "Update plan doesn't contain a Projection node".to_string(),
                    ));
                };

                // TODO: Once https://github.com/delta-io/delta-rs/issues/1126 is closed use the
                // native delta-rs UPDATE op

                let mut table = self.try_get_delta_table(table_name).await?;
                table.load().await?;

                let schema_ref = TableProvider::schema(&table);
                let df_schema = DFSchema::try_from_qualified_schema(
                    table_name.table(),
                    schema_ref.as_ref(),
                )?;

                let state = self.inner.state();

                let (selection_expr, removes) =
                    if let LogicalPlan::Filter(Filter { predicate, .. }) = &**input {
                        // A WHERE clause has been used; employ it to prune the update down to only
                        // a subset of files, while inheriting the rest from the previous version
                        let filter_expr = create_physical_expr(
                            &predicate.clone(),
                            &df_schema,
                            schema_ref.as_ref(),
                            &ExecutionProps::new(),
                        )?;

                        let pruning_predicate = PruningPredicate::try_new(
                            filter_expr.clone(),
                            schema_ref.clone(),
                        )?;
                        let prune_map = pruning_predicate.prune(&table)?;

                        let files_to_prune = table
                            .get_state()
                            .files()
                            .iter()
                            .zip(prune_map)
                            .filter_map(
                                |(add, keep)| if keep { Some(add.clone()) } else { None },
                            )
                            .collect::<Vec<Add>>();

                        (Some(filter_expr), files_to_prune)
                    } else {
                        // If no qualifier is specified we're basically updating the whole table.
                        (None, table.get_state().files().clone())
                    };

                let uuid = self.get_table_uuid(table_name).await?;
                let mut actions: Vec<Action> = vec![];
                if !removes.is_empty() {
                    let base_scan = parquet_scan_from_actions(
                        &table,
                        removes.as_slice(),
                        schema_ref.as_ref(),
                        selection_expr.clone(),
                        &state,
                        None,
                        None,
                    )
                    .await?;

                    let projections = project_expressions(
                        expr,
                        &df_schema,
                        schema_ref.as_ref(),
                        selection_expr,
                    )?;

                    // Apply the provided assignments
                    let update_plan: Arc<dyn ExecutionPlan> = Arc::new(
                        ProjectionExec::try_new(projections.clone(), base_scan)?,
                    );

                    // Write the new files with updated data
                    let adds = plan_to_object_store(
                        &state,
                        &update_plan,
                        self.internal_object_store.clone(),
                        uuid.to_string(),
                        self.inner.runtime_env().disk_manager.clone(),
                        self.max_partition_size,
                    )
                    .await?;

                    let deletion_timestamp = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as i64;

                    actions = adds.into_iter().map(Action::add).collect();
                    for remove in removes {
                        actions.push(Action::remove(Remove {
                            path: remove.path,
                            deletion_timestamp: Some(deletion_timestamp),
                            data_change: true,
                            extended_file_metadata: Some(true),
                            partition_values: Some(remove.partition_values),
                            size: Some(remove.size),
                            tags: None,
                            deletion_vector: None,
                        }))
                    }
                }

                let op = DeltaOperation::Write {
                    mode: SaveMode::Append,
                    partition_by: None,
                    predicate: None,
                };
                let version = commit(
                    table.object_store().as_ref(),
                    &actions,
                    op,
                    table.get_state(),
                    None,
                )
                .await?;
                self.table_catalog
                    .create_new_table_version(uuid, version)
                    .await?;

                Ok(make_dummy_exec())
            }
            LogicalPlan::Dml(DmlStatement {
                table_name,
                table_schema,
                op: WriteOp::Delete,
                input,
            }) => {
                // TODO: Once https://github.com/delta-io/delta-rs/pull/1176 is merged use that instead
                let uuid = self.get_table_uuid(table_name).await?;

                let mut table = self.try_get_delta_table(table_name).await?;
                table.load().await?;
                let schema_ref = SchemaRef::from(table_schema.deref().clone());

                let (adds, removes) =
                    if let LogicalPlan::Filter(Filter { predicate, .. }) = &**input {
                        // A WHERE clause has been used; employ it to prune the filtration
                        // down to only a subset of partitions, re-use the rest as is

                        let state = self.inner.state();

                        let prune_expr = create_physical_expr(
                            &predicate.clone(),
                            table_schema,
                            schema_ref.as_ref(),
                            &ExecutionProps::new(),
                        )?;

                        let pruning_predicate =
                            PruningPredicate::try_new(prune_expr, schema_ref.clone())?;
                        let prune_map = pruning_predicate.prune(&table)?;
                        let files_to_prune = table
                            .get_state()
                            .files()
                            .iter()
                            .zip(prune_map)
                            .filter_map(
                                |(add, keep)| if keep { Some(add.clone()) } else { None },
                            )
                            .collect::<Vec<Add>>();

                        if files_to_prune.is_empty() {
                            // The used WHERE clause doesn't match any of the partitions, so we don't
                            // have any additions or removals for the new tables state.
                            (vec![], vec![])
                        } else {
                            // To simulate the effect of a WHERE clause from a DELETE, we need to use the
                            // inverse clause in a scan, when filtering the rows that should remain.
                            let filter_expr = create_physical_expr(
                                &predicate.clone().not(),
                                table_schema,
                                schema_ref.as_ref(),
                                &ExecutionProps::new(),
                            )?;

                            let base_scan = parquet_scan_from_actions(
                                &table,
                                files_to_prune.as_slice(),
                                schema_ref.as_ref(),
                                Some(filter_expr.clone()),
                                &state,
                                None,
                                None,
                            )
                            .await?;

                            let filter_plan: Arc<dyn ExecutionPlan> =
                                Arc::new(FilterExec::try_new(filter_expr, base_scan)?);

                            // Write the filtered out data
                            let adds = plan_to_object_store(
                                &state,
                                &filter_plan,
                                self.internal_object_store.clone(),
                                uuid.to_string(),
                                self.inner.runtime_env().disk_manager.clone(),
                                self.max_partition_size,
                            )
                            .await?;

                            (adds, files_to_prune)
                        }
                    } else {
                        // If no qualifier is specified we're basically truncating the table.
                        // Remove all files.
                        (vec![], table.get_state().files().clone())
                    };

                let deletion_timestamp = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64;

                let mut actions: Vec<Action> =
                    adds.into_iter().map(Action::add).collect();
                for remove in removes {
                    actions.push(Action::remove(Remove {
                        path: remove.path,
                        deletion_timestamp: Some(deletion_timestamp),
                        data_change: true,
                        extended_file_metadata: Some(true),
                        partition_values: Some(remove.partition_values),
                        size: Some(remove.size),
                        tags: None,
                        deletion_vector: None,
                    }))
                }

                let op = DeltaOperation::Delete { predicate: None };
                let version = commit(
                    table.object_store().as_ref(),
                    &actions,
                    op,
                    table.get_state(),
                    None,
                )
                .await?;
                self.table_catalog
                    .create_new_table_version(uuid, version)
                    .await?;

                Ok(make_dummy_exec())
            }
            LogicalPlan::Ddl(DdlStatement::DropTable(DropTable {
                name,
                if_exists: _,
                schema: _,
            })) => {
                let table_ref = TableReference::from(name);
                let resolved_ref = table_ref.resolve(&self.database, DEFAULT_SCHEMA);

                if resolved_ref.schema == STAGING_SCHEMA {
                    // Dropping a staging table is a in-memory only op
                    self.inner.deregister_table(resolved_ref)?;
                    return Ok(make_dummy_exec());
                }

                let table_id = self
                    .table_catalog
                    .get_table_id_by_name(
                        &resolved_ref.catalog,
                        &resolved_ref.schema,
                        &resolved_ref.table,
                    )
                    .await?
                    .ok_or_else(|| {
                        DataFusionError::Execution(format!("Table {name} not found"))
                    })?;

                self.table_catalog.drop_table(table_id).await?;
                Ok(make_dummy_exec())
            }
            LogicalPlan::Ddl(DdlStatement::CreateView(_)) => {
                return Err(Error::Plan(
                    "Creating views is currently unsupported!".to_string(),
                ))
            }
            LogicalPlan::Extension(Extension { ref node }) => {
                // Other custom nodes we made like CREATE TABLE/INSERT/ALTER
                match SeafowlExtensionNode::from_dynamic(node) {
                    Some(sfe_node) => match sfe_node {
                        SeafowlExtensionNode::CreateTable(CreateTable {
                            schema,
                            name,
                            ..
                        }) => {
                            self.create_delta_table(name.as_str(), schema).await?;

                            Ok(make_dummy_exec())
                        }
                        SeafowlExtensionNode::CreateFunction(CreateFunction {
                            name,
                            or_replace,
                            details,
                            output_schema: _,
                        }) => {
                            self.register_function(name, details)?;

                            // Persist the function in the metadata storage
                            self.function_catalog
                                .create_function(
                                    self.database_id,
                                    name,
                                    *or_replace,
                                    details,
                                )
                                .await?;

                            Ok(make_dummy_exec())
                        }
                        SeafowlExtensionNode::DropFunction(DropFunction {
                            if_exists,
                            func_names,
                            output_schema: _,
                        }) => {
                            self.function_catalog
                                .drop_function(self.database_id, *if_exists, func_names)
                                .await?;
                            Ok(make_dummy_exec())
                        }
                        SeafowlExtensionNode::RenameTable(RenameTable {
                            old_name,
                            new_name,
                            ..
                        }) => {
                            // Resolve new table reference
                            let new_table_ref = TableReference::from(new_name.as_str());
                            let resolved_new_ref =
                                new_table_ref.resolve(&self.database, DEFAULT_SCHEMA);
                            if resolved_new_ref.catalog != self.database {
                                return Err(Error::Plan(
                                    "Changing the table's database is not supported!"
                                        .to_string(),
                                ));
                            }

                            // Resolve old table reference and fetch the table id
                            let old_table_ref = TableReference::from(old_name.as_str());
                            let resolved_old_ref =
                                old_table_ref.resolve(&self.database, DEFAULT_SCHEMA);

                            let table_id = self
                                .table_catalog
                                .get_table_id_by_name(
                                    &resolved_old_ref.catalog,
                                    &resolved_old_ref.schema,
                                    &resolved_old_ref.table,
                                )
                                .await?
                                .ok_or_else(|| {
                                    DataFusionError::Execution(format!(
                                        "Table {old_name} not found"
                                    ))
                                })?;

                            // If the old and new table schema is different check that the
                            // corresponding collection already exists
                            let new_schema_id =
                                if resolved_new_ref.schema != resolved_old_ref.schema {
                                    let collection_id = self
                                        .table_catalog
                                        .get_collection_id_by_name(
                                            &self.database,
                                            &resolved_new_ref.schema,
                                        )
                                        .await?
                                        .ok_or_else(|| {
                                            Error::Plan(format!(
                                                "Schema \"{}\" does not exist!",
                                                &resolved_new_ref.schema,
                                            ))
                                        })?;
                                    Some(collection_id)
                                } else {
                                    None
                                };

                            // Finally update our catalog entry
                            self.table_catalog
                                .move_table(
                                    table_id,
                                    &resolved_new_ref.table,
                                    new_schema_id,
                                )
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
                            database,
                            table_name,
                            ..
                        }) => {
                            if database.is_some() {
                                gc_databases(self, database.clone()).await;
                            } else if let Some(table_name) = table_name {
                                let table_ref = TableReference::from(table_name.as_str());
                                let resolved_ref =
                                    table_ref.resolve(&self.database, DEFAULT_SCHEMA);

                                let table_id = self
                                    .table_catalog
                                    .get_table_id_by_name(
                                        &resolved_ref.catalog,
                                        &resolved_ref.schema,
                                        &resolved_ref.table,
                                    )
                                    .await?
                                    .ok_or_else(|| {
                                        DataFusionError::Execution(
                                            "Table {table_name} not found".to_string(),
                                        )
                                    })?;

                                if let Ok(mut delta_table) =
                                    self.try_get_delta_table(resolved_ref).await
                                {
                                    // TODO: The Delta protocol doesn't vacuum old table versions per se, but only files no longer tied to the latest table version.
                                    // This means that the VACUUM could be a no-op, for instance, in the case when append-only writes have been performed.
                                    // Furthermore, even when it does GC some files, there's no API to determine which table versions are still valid; the
                                    // vacuum command doesn't change anything in the `_delta_log` folder: https://github.com/delta-io/delta-rs/issues/1013#issuecomment-1416911514
                                    // In turn, this means that after a vacuum we cannot represent any other version but latest with confidence, so in our own
                                    // catalog we simply delete all table versions older than the latest one.
                                    // This all means that there are potential table versions which are still functional (and can be queried using
                                    // time-travel querying syntax), but are not represented in `system.table_versions` table.
                                    delta_table.load().await?;
                                    let plan = VacuumBuilder::new(
                                        delta_table.object_store(),
                                        delta_table.state.clone(),
                                    )
                                    .with_enforce_retention_duration(false)
                                    .with_retention_period(Duration::hours(0_i64));

                                    let (_, metrics) = plan.await?;
                                    let deleted_files = metrics.files_deleted;
                                    info!("Deleted Delta table tombstones {deleted_files:?}");
                                }

                                match self
                                    .table_catalog
                                    .delete_old_table_versions(table_id)
                                    .await
                                {
                                    Ok(row_count) => {
                                        info!("Deleted {} old table versions", row_count);
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
                    None => self.inner.state().create_physical_plan(plan).await,
                }
            }
            _ => self.inner.state().create_physical_plan(plan).await,
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
}

#[cfg(test)]
pub mod test_utils {
    use std::sync::Arc;

    use crate::config::context::build_context;
    use crate::config::schema;
    use crate::config::schema::{Catalog, SeafowlConfig, Sqlite};
    use sqlx::sqlite::SqliteJournalMode;

    use super::*;

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

    pub async fn in_memory_context_with_test_db() -> Arc<DefaultSeafowlContext> {
        let context = in_memory_context().await;

        // Create new non-default database; we're doing this in catalog only to simulate it taking
        // place on another node
        context
            .table_catalog
            .create_database("testdb")
            .await
            .unwrap();

        let context = context
            .scope_to_database("testdb".to_string())
            .await
            .expect("'testdb' should exist");

        // Create new non-default collection
        context.plan_query("CREATE SCHEMA testcol").await.unwrap();

        // Create table
        context
            .plan_query("CREATE TABLE testcol.some_table (date DATE, value DOUBLE)")
            .await
            .unwrap();

        context
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field};
    use tempfile::TempDir;

    use std::sync::Arc;

    use datafusion::execution::disk_manager::DiskManagerConfig;
    use object_store::memory::InMemory;
    use rstest::rstest;

    use crate::config::schema;
    use datafusion::assert_batches_eq;
    use datafusion::physical_plan::memory::MemoryExec;
    use object_store::local::LocalFileSystem;
    use serde_json::{json, Value};

    use super::*;

    use super::test_utils::{in_memory_context, in_memory_context_with_test_db};

    const PART_0_FILE_NAME: &str =
        "part-00000-01020304-0506-4708-890a-0b0c0d0e0f10-c000.snappy.parquet";
    const PART_1_FILE_NAME: &str =
        "part-00001-01020304-0506-4708-890a-0b0c0d0e0f10-c000.snappy.parquet";

    use crate::testutils::assert_uploaded_objects;

    async fn get_logical_plan(query: &str) -> String {
        let sf_context = mock_context().await;
        let plan = sf_context.create_logical_plan(query).await.unwrap();
        format!("{:?}", plan)
    }

    #[tokio::test]
    async fn test_create_schema_name_in_quotes() {

        assert_eq!(
            get_logical_plan("CREATE SCHEMA \"schema_name\"").await,
            "Create: table_name"
        );
    }

    #[rstest]
    #[case::in_memory_object_store_standard(false)]
    #[case::local_object_store_test_renames(true)]
    #[tokio::test]
    async fn test_plan_to_object_storage(#[case] is_local: bool) {
        let sf_context = in_memory_context().await;

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
                Arc::new(InternalObjectStore::new(
                    Arc::new(LocalFileSystem::new_with_prefix(tmp_dir.path()).unwrap()),
                    schema::ObjectStore::Local(schema::Local {
                        data_dir: tmp_dir.path().to_string_lossy().to_string(),
                    }),
                )),
                Some(tmp_dir),
            )
        } else {
            (
                Arc::new(InternalObjectStore::new(
                    Arc::new(InMemory::new()),
                    schema::ObjectStore::InMemory(schema::InMemory {}),
                )),
                None,
            )
        };

        let table_uuid = Uuid::default();
        let disk_manager = DiskManager::try_new(DiskManagerConfig::new()).unwrap();
        let adds = plan_to_object_store(
            &sf_context.inner.state(),
            &execution_plan,
            object_store.clone(),
            table_uuid.to_string(),
            disk_manager,
            2,
        )
        .await
        .unwrap();

        assert_eq!(adds.len(), 2);
        assert_eq!(
            vec![
                (
                    adds[0].path.clone(),
                    adds[0].size,
                    adds[0].partition_values.is_empty(),
                    adds[0].partition_values_parsed.is_none(),
                    adds[0].data_change,
                    serde_json::from_str::<Value>(
                        adds[0].stats.clone().unwrap().as_str()
                    )
                    .unwrap(),
                ),
                (
                    adds[1].path.clone(),
                    adds[1].size,
                    adds[1].partition_values.is_empty(),
                    adds[1].partition_values_parsed.is_none(),
                    adds[1].data_change,
                    serde_json::from_str::<Value>(
                        adds[1].stats.clone().unwrap().as_str()
                    )
                    .unwrap(),
                )
            ],
            vec![
                (
                    PART_0_FILE_NAME.to_string(),
                    1262,
                    true,
                    true,
                    true,
                    json!({
                        "numRecords": 2,
                        "minValues": {
                            "integer": 12,
                            "timestamp": "2022-01-01",
                            "varchar": "one",
                        },
                        "maxValues": {
                            "integer": 42,
                            "timestamp": "2022-01-02",
                            "varchar": "two",
                        },
                        "nullCount": {
                            "integer": 0,
                            "timestamp": 0,
                            "varchar": 0,
                        },
                    }),
                ),
                (
                    PART_1_FILE_NAME.to_string(),
                    1277,
                    true,
                    true,
                    true,
                    json!({
                        "numRecords": 2,
                        "minValues": {
                            "integer": 22,
                            "timestamp": "2022-01-03",
                            "varchar": "four",
                        },
                        "maxValues": {
                            "integer": 32,
                            "timestamp": "2022-01-04",
                            "varchar": "three",
                        },
                        "nullCount": {
                            "integer": 0,
                            "timestamp": 0,
                            "varchar": 0,
                        },
                    }),
                )
            ]
        );

        assert_uploaded_objects(
            object_store.for_delta_table(table_uuid),
            vec![
                Path::from(PART_0_FILE_NAME.to_string()),
                Path::from(PART_1_FILE_NAME.to_string()),
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
        let sf_context = in_memory_context_with_test_db().await;

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
                            vec![Arc::new(Int32Array::from(record_batch.clone()))],
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

        let object_store = Arc::new(InternalObjectStore::new(
            Arc::new(InMemory::new()),
            schema::ObjectStore::InMemory(schema::InMemory {}),
        ));
        let disk_manager = DiskManager::try_new(DiskManagerConfig::new()).unwrap();
        let adds = plan_to_object_store(
            &sf_context.inner.state(),
            &execution_plan,
            object_store,
            "test".to_string(),
            disk_manager,
            max_partition_size,
        )
        .await
        .unwrap();

        assert_eq!(adds.len(), output_partitions.len(),);

        for i in 0..output_partitions.len() {
            assert_eq!(
                serde_json::from_str::<Value>(adds[i].stats.clone().unwrap().as_str())
                    .unwrap(),
                json!({
                    "numRecords": output_partitions[i].len(),
                    "minValues": {
                        "some_number": output_partitions[i].iter().min(),
                    },
                    "maxValues": {
                        "some_number": output_partitions[i].iter().max(),
                    },
                    "nullCount": {
                        "some_number": 0,
                    },
                }),
            )
        }
    }

    #[tokio::test]
    async fn test_plan_insert_normal() {
        let sf_context = in_memory_context_with_test_db().await;

        let plan = sf_context
            .create_logical_plan(
                "INSERT INTO testcol.some_table (date, value) VALUES('2022-01-01T12:00:00', 42)",
            )
            .await
            .unwrap();

        assert_eq!(
            format!("{plan:?}"),
            "Dml: op=[Insert Into] table=[testcol.some_table]\
            \n  Projection: CAST(column1 AS Date32) AS date, CAST(column2 AS Float64) AS value\
            \n    Values: (Utf8(\"2022-01-01T12:00:00\"), Int64(42))"
        );
    }

    #[tokio::test]
    async fn test_plan_insert_renaming() {
        let sf_context = in_memory_context_with_test_db().await;

        let plan = sf_context
            .create_logical_plan(
                "INSERT INTO testcol.some_table (date, value)
                SELECT \"date\" AS my_date, \"value\" AS my_value FROM testdb.testcol.some_table",
            )
            .await
            .unwrap();

        assert_eq!(format!("{plan:?}"), "Dml: op=[Insert Into] table=[testcol.some_table]\
        \n  Projection: testdb.testcol.some_table.date AS date, testdb.testcol.some_table.value AS value\
        \n    TableScan: testdb.testcol.some_table projection=[date, value]");
    }

    #[tokio::test]
    async fn test_create_table_without_columns_fails() {
        let context = Arc::new(in_memory_context().await);
        let err = context
            .plan_query("CREATE TABLE test_table")
            .await
            .unwrap_err();

        assert!(err
            .to_string()
            .contains("At least one column must be defined to create a table."));
    }

    #[tokio::test]
    async fn test_drop_table_pending_deletion() -> Result<()> {
        let context = Arc::new(in_memory_context().await);
        context
            .plan_query("CREATE TABLE test_table (\"key\" INTEGER, value STRING)")
            .await
            .unwrap();
        context.plan_query("DROP TABLE test_table").await.unwrap();

        let plan = context
            .plan_query("SELECT table_schema, table_name, uuid, deletion_status FROM system.dropped_tables")
            .await
            .unwrap();
        let results = context.collect(plan).await.unwrap();

        let expected = ["+--------------+------------+--------------------------------------+-----------------+",
            "| table_schema | table_name | uuid                                 | deletion_status |",
            "+--------------+------------+--------------------------------------+-----------------+",
            "| public       | test_table | 01020304-0506-4708-890a-0b0c0d0e0f10 | PENDING         |",
            "+--------------+------------+--------------------------------------+-----------------+"];
        assert_batches_eq!(expected, &results);

        Ok(())
    }

    #[tokio::test]
    async fn test_execute_insert_from_other_table() -> Result<()> {
        let context = Arc::new(in_memory_context().await);
        context
            .plan_query("CREATE TABLE test_table (\"key\" INTEGER, value STRING);")
            .await?;

        context
            .plan_query("INSERT INTO test_table VALUES (1, 'one'), (2, 'two');")
            .await?;

        context
                .plan_query("INSERT INTO test_table(key, value) SELECT * FROM test_table WHERE value = 'two'")
                .await?;

        let results = context
            .collect(
                context
                    .plan_query("SELECT * FROM test_table ORDER BY key ASC")
                    .await?,
            )
            .await?;

        let expected = [
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

        let expected = [
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
        let sf_context = in_memory_context_with_test_db().await;

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
            "RenameTable: \"testcol\".\"some_table\" to \"testcol\".\"some_table_2\""
        );
    }

    #[tokio::test]
    async fn test_plan_drop_table_name_in_quotes() {
        assert_eq!(
            get_logical_plan("DROP TABLE \"testcol\".\"some_table\"").await,
            "DropTable: Partial { schema: \"testcol\", table: \"some_table\" } if not exist:=false"
        );
    }

    #[tokio::test]
    async fn test_plan_insert_all() {
        let sf_context = in_memory_context_with_test_db().await;

        let plan = sf_context
            .create_logical_plan(
                "INSERT INTO testcol.some_table VALUES('2022-01-01T12:00:00', 42)",
            )
            .await
            .unwrap();

        assert_eq!(
            format!("{plan:?}"),
            "Dml: op=[Insert Into] table=[testcol.some_table]\
            \n  Projection: CAST(column1 AS Date32) AS date, CAST(column2 AS Float64) AS value\
            \n    Values: (Utf8(\"2022-01-01T12:00:00\"), Int64(42))"
        );
    }

    #[tokio::test]
    async fn test_plan_insert_missing_table() {
        let context = in_memory_context_with_test_db().await;

        let err = context
            .create_logical_plan("INSERT INTO testcol.missing_table VALUES(1, 2, 3)")
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "Error during planning: table 'testdb.testcol.missing_table' not found"
        );
    }

    #[tokio::test]
    async fn test_plan_insert_type_mismatch() {
        let sf_context = in_memory_context_with_test_db().await;

        // Try inserting a timestamp into a number (note this will work fine for inserting
        // e.g. Utf-8 into numbers at plan time but should fail at execution time if the value
        // doesn't convert)
        let err = sf_context
            .create_logical_plan("INSERT INTO testcol.some_table SELECT '2022-01-01', to_timestamp('2022-01-01T12:00:00')")
            .await.unwrap_err();
        assert_eq!(err.to_string(), "Error during planning: Cannot automatically convert Timestamp(Nanosecond, None) to Float64");
    }

    #[tokio::test]
    async fn test_plan_insert_values_wrong_number() {
        let sf_context = in_memory_context_with_test_db().await;

        let err = sf_context
            .create_logical_plan(
                "INSERT INTO testcol.some_table VALUES('2022-01-01T12:00:00')",
            )
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "Error during planning: Column count doesn't match insert query!"
        );
    }

    #[tokio::test]
    async fn test_plan_insert_values_duplicate_columns() {
        let sf_context = in_memory_context_with_test_db().await;

        let err = sf_context
            .create_logical_plan("INSERT INTO testcol.some_table(date, date, value) VALUES('2022-01-01T12:00:00', '2022-01-01T12:00:00', 42)")
            .await.unwrap_err();
        assert_eq!(
            err.to_string(),
            "Schema error: Schema contains duplicate unqualified field name date"
        );
    }

    #[rstest]
    #[case::regular_type_names("float", "float")]
    #[case::legacy_type_names("f32", "f32")]
    #[case::uppercase_type_names("FLOAT", "REAL")]
    #[tokio::test]
    async fn test_register_udf(
        #[case] input_type: &str,
        #[case] return_type: &str,
    ) -> Result<()> {
        let sf_context = in_memory_context().await;

        // Source: https://gist.github.com/going-digital/02e46c44d89237c07bc99cd440ebfa43
        let create_function_stmt = r#"CREATE FUNCTION sintau AS '
        {
            "entrypoint": "sintau",
            "language": "wasm",
            "input_types": ["int"],
            "return_type": "int",
            "data": "AGFzbQEAAAABDQJgAX0BfWADfX9/AX0DBQQAAAABBQQBAUREBxgDBnNpbnRhdQAABGV4cDIAAQRsb2cyAAIKjgEEKQECfUMAAAA/IgIgACAAjpMiACACk4siAZMgAZZBAEEYEAMgAiAAk5gLGQAgACAAjiIAk0EYQSwQA7wgAKhBF3RqvgslAQF/IAC8IgFBF3ZB/wBrsiABQQl0s0MAAIBPlUEsQcQAEAOSCyIBAX0DQCADIACUIAEqAgCSIQMgAUEEaiIBIAJrDQALIAMLC0oBAEEAC0Q/x2FC2eATQUuqKsJzsqY9QAHJQH6V0DZv+V88kPJTPSJndz6sZjE/HQCAP/clMD0D/T++F6bRPkzcNL/Tgrg//IiKNwBqBG5hbWUBHwQABnNpbnRhdQEEZXhwMgIEbG9nMgMIZXZhbHBvbHkCNwQAAwABeAECeDECBGhhbGYBAQABeAICAAF4AQJ4aQMEAAF4AQVzdGFydAIDZW5kAwZyZXN1bHQDCQEDAQAEbG9vcA=="
        }';"#;

        sf_context.plan_query(create_function_stmt).await?;

        // Run the same query again to make sure we raise an error if the function already exists
        let err = sf_context
            .plan_query(create_function_stmt)
            .await
            .unwrap_err();

        assert_eq!(
            err.to_string(),
            "Error during planning: Function \"sintau\" already exists"
        );

        // Now replace the function using proper input/return types
        let replace_function_stmt = format!(
            r#"CREATE OR REPLACE FUNCTION sintau AS '
        {{
            "entrypoint": "sintau",
            "language": "wasm",
            "input_types": ["{input_type}"],
            "return_type": "{return_type}",
            "data": "AGFzbQEAAAABDQJgAX0BfWADfX9/AX0DBQQAAAABBQQBAUREBxgDBnNpbnRhdQAABGV4cDIAAQRsb2cyAAIKjgEEKQECfUMAAAA/IgIgACAAjpMiACACk4siAZMgAZZBAEEYEAMgAiAAk5gLGQAgACAAjiIAk0EYQSwQA7wgAKhBF3RqvgslAQF/IAC8IgFBF3ZB/wBrsiABQQl0s0MAAIBPlUEsQcQAEAOSCyIBAX0DQCADIACUIAEqAgCSIQMgAUEEaiIBIAJrDQALIAMLC0oBAEEAC0Q/x2FC2eATQUuqKsJzsqY9QAHJQH6V0DZv+V88kPJTPSJndz6sZjE/HQCAP/clMD0D/T++F6bRPkzcNL/Tgrg//IiKNwBqBG5hbWUBHwQABnNpbnRhdQEEZXhwMgIEbG9nMgMIZXZhbHBvbHkCNwQAAwABeAECeDECBGhhbGYBAQABeAICAAF4AQJ4aQMEAAF4AQVzdGFydAIDZW5kAwZyZXN1bHQDCQEDAQAEbG9vcA=="
        }}';"#
        );

        sf_context
            .plan_query(replace_function_stmt.as_str())
            .await?;

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

        let expected = [
            "+-----+--------+",
            "| v   | sintau |",
            "+-----+--------+",
            "| 0.1 | 59.0   |",
            "| 0.2 | 95.0   |",
            "| 0.3 | 95.0   |",
            "| 0.4 | 59.0   |",
            "| 0.5 | 0.0    |",
            "+-----+--------+",
        ];

        assert_batches_eq!(expected, &results);

        Ok(())
    }

    #[tokio::test]
    async fn test_register_invalid_udf() -> Result<()> {
        let sf_context = in_memory_context().await;

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

    #[tokio::test]
    async fn test_drop_function() -> Result<()> {
        let sf_context = in_memory_context().await;

        let err = sf_context
            .plan_query(r#"DROP FUNCTION nonexistentfunction"#)
            .await
            .unwrap_err();

        assert_eq!(
            err.to_string(),
            "Error during planning: Function \"nonexistentfunction\" not found"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_drop_function_if_exists() -> Result<()> {
        let sf_context = in_memory_context().await;

        let plan = sf_context
            .plan_query(r#"DROP FUNCTION IF EXISTS nonexistentfunction"#)
            .await;
        assert!(plan.is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn test_create_and_drop_two_functions() -> Result<()> {
        let sf_context = in_memory_context().await;

        let create_function_stmt = r#"CREATE FUNCTION sintau AS '
        {
            "entrypoint": "sintau",
            "language": "wasm",
            "input_types": ["int"],
            "return_type": "int",
            "data": "AGFzbQEAAAABDQJgAX0BfWADfX9/AX0DBQQAAAABBQQBAUREBxgDBnNpbnRhdQAABGV4cDIAAQRsb2cyAAIKjgEEKQECfUMAAAA/IgIgACAAjpMiACACk4siAZMgAZZBAEEYEAMgAiAAk5gLGQAgACAAjiIAk0EYQSwQA7wgAKhBF3RqvgslAQF/IAC8IgFBF3ZB/wBrsiABQQl0s0MAAIBPlUEsQcQAEAOSCyIBAX0DQCADIACUIAEqAgCSIQMgAUEEaiIBIAJrDQALIAMLC0oBAEEAC0Q/x2FC2eATQUuqKsJzsqY9QAHJQH6V0DZv+V88kPJTPSJndz6sZjE/HQCAP/clMD0D/T++F6bRPkzcNL/Tgrg//IiKNwBqBG5hbWUBHwQABnNpbnRhdQEEZXhwMgIEbG9nMgMIZXZhbHBvbHkCNwQAAwABeAECeDECBGhhbGYBAQABeAICAAF4AQJ4aQMEAAF4AQVzdGFydAIDZW5kAwZyZXN1bHQDCQEDAQAEbG9vcA=="
        }';"#;

        let create_function_stmt2 = r#"CREATE FUNCTION sintau2 AS '
        {
            "entrypoint": "sintau",
            "language": "wasm",
            "input_types": ["int"],
            "return_type": "int",
            "data": "AGFzbQEAAAABDQJgAX0BfWADfX9/AX0DBQQAAAABBQQBAUREBxgDBnNpbnRhdQAABGV4cDIAAQRsb2cyAAIKjgEEKQECfUMAAAA/IgIgACAAjpMiACACk4siAZMgAZZBAEEYEAMgAiAAk5gLGQAgACAAjiIAk0EYQSwQA7wgAKhBF3RqvgslAQF/IAC8IgFBF3ZB/wBrsiABQQl0s0MAAIBPlUEsQcQAEAOSCyIBAX0DQCADIACUIAEqAgCSIQMgAUEEaiIBIAJrDQALIAMLC0oBAEEAC0Q/x2FC2eATQUuqKsJzsqY9QAHJQH6V0DZv+V88kPJTPSJndz6sZjE/HQCAP/clMD0D/T++F6bRPkzcNL/Tgrg//IiKNwBqBG5hbWUBHwQABnNpbnRhdQEEZXhwMgIEbG9nMgMIZXZhbHBvbHkCNwQAAwABeAECeDECBGhhbGYBAQABeAICAAF4AQJ4aQMEAAF4AQVzdGFydAIDZW5kAwZyZXN1bHQDCQEDAQAEbG9vcA=="
        }';"#;

        // Create two functions in two separate passes
        sf_context.plan_query(create_function_stmt).await?;
        sf_context.plan_query(create_function_stmt2).await?;

        // Test dropping both functions in one pass
        let plan = sf_context
            .plan_query(r#"DROP FUNCTION sintau, sintau2"#)
            .await;
        assert!(plan.is_ok());
        Ok(())
    }
}
