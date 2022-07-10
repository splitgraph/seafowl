// DataFusion bindings

use bytes::Bytes;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::execution::context::SessionState;
use datafusion::execution::DiskManager;
use futures::{StreamExt, TryStreamExt};

use hashbrown::HashMap;
use hex::encode;
use object_store::memory::InMemory;
use object_store::{local::LocalFileSystem, path::Path, ObjectStore};
use sha2::Digest;
use sha2::Sha256;
use sqlparser::ast::{
    ColumnDef as SQLColumnDef, ColumnOption, DataType as SQLDataType, Ident, Statement,
    TableFactor, TableWithJoins,
};
use std::io::Read;

use std::sync::Arc;
use std::{iter::zip, path::Path as OSPath};

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
        plan::Extension, Column, CreateCatalog, CreateCatalogSchema, CreateMemoryTable, DFSchema,
        LogicalPlan, ToDFSchema,
    },
    parquet::{arrow::ArrowWriter, file::properties::WriterProperties},
    physical_plan::{
        coalesce_partitions::CoalescePartitionsExec, empty::EmptyExec, EmptyRecordBatchStream,
        ExecutionPlan, SendableRecordBatchStream, Statistics,
    },
    prelude::SessionContext,
    sql::{parser::DFParser, planner::SqlToRel, TableReference},
};

use crate::{
    catalog::Catalog,
    data_types::{DatabaseId, PhysicalRegion, PhysicalRegionColumn},
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
        SQLDataType::Char(_) | SQLDataType::Varchar(_) | SQLDataType::Text => Ok(DataType::Utf8),
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

/// Load the Statistics for a Parquet file at a certain path
async fn get_parquet_file_statistics(path: &OSPath, schema: SchemaRef) -> Result<Statistics> {
    // DataFusion's methods for this are all private (see fetch_statistics / summarize_min_max)
    // and require the ObjectStore abstraction since they are normally used in the context
    // of a TableProvider sending a Range request to object storage to get min/max values
    // for a Parquet file. We are currently interested in getting statistics for a temporary
    // file we just wrote out, before uploading it to object storage.

    // A more fancy way to get this working would be making an ObjectStore
    // that serves as a write-through cache so that we can use it both when downloading and uploading
    // Parquet files.
    let directory = path
        .parent()
        .expect("Temporary object store path is a directory / root");
    let file_name = path
        .file_name()
        .expect("Temporary object store path is a root")
        .to_str()
        .expect("Temporary object path isn't Unicode");

    // Create a dummy object store pointing to our temporary directory (we don't know if
    // DiskManager will always put all files in the same dir)
    let dummy_object_store: Arc<dyn ObjectStore> =
        Arc::from(LocalFileSystem::new_with_prefix(directory).expect("creating object store"));
    let parquet = ParquetFormat::default();
    let meta = dummy_object_store
        .head(&Path::from(file_name))
        .await
        .expect("Temporary object not found");
    let stats = parquet
        .infer_stats(&dummy_object_store, schema, &meta)
        .await?;
    Ok(stats)
}

/// Load the Statistics for a Parquet file in memory
async fn get_parquet_file_statistics_bytes(data: Bytes, schema: SchemaRef) -> Result<Statistics> {
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
fn build_region_columns(region_stats: &Statistics, schema: SchemaRef) -> Vec<PhysicalRegionColumn> {
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

                PhysicalRegionColumn {
                    id: 0,
                    physical_region_id: 0,
                    name: column.name().to_string(),
                    r#type: column.data_type().to_json().to_string(),
                    min_value,
                    max_value,
                }
            })
            .collect(),
        None => schema
            .fields()
            .iter()
            .map(|column| PhysicalRegionColumn {
                id: 0,
                physical_region_id: 0,
                name: column.name().to_string(),
                r#type: column.data_type().to_json().to_string(),
                min_value: None,
                max_value: None,
            })
            .collect(),
    }
}

struct SeafowlContext {
    inner: SessionContext,
    catalog: Arc<dyn Catalog>,
    database: String,
    database_id: DatabaseId,
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
    plan: Arc<dyn ExecutionPlan>,
    store: Arc<dyn ObjectStore>,
    disk_manager: Arc<DiskManager>,
) -> Result<Vec<(Vec<PhysicalRegionColumn>, PhysicalRegion)>> {
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

        let handle: tokio::task::JoinHandle<Result<(Vec<PhysicalRegionColumn>, PhysicalRegion)>> =
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
                    get_parquet_file_statistics_bytes(data.clone(), physical.schema()).await?;

                let columns = build_region_columns(&region_stats, physical.schema());

                let mut hasher = Sha256::new();
                hasher.update(&data);
                let hash_str = encode(hasher.finalize());
                let object_storage_id = hash_str + ".parquet";
                store
                    .put(&Path::from(object_storage_id.clone()), data)
                    .await?;

                let region = PhysicalRegion {
                    id: 0,
                    object_storage_id,
                    row_count: region_stats
                        .num_rows
                        .expect("Error counting rows in the written file")
                        .try_into()
                        .expect("row count greater than 2147483647"),
                };

                Ok((columns, region))
            });
        tasks.push(handle);
    }

    futures::future::join_all(tasks)
        .await
        .into_iter()
        .map(|x| x.unwrap_or_else(|e| Err(DataFusionError::External(Box::new(e)))))
        .collect()
}

impl SeafowlContext {
    pub fn inner(&self) -> &SessionContext {
        &self.inner
    }

    pub async fn create_logical_plan(&self, sql: &str) -> Result<LogicalPlan> {
        let mut statements = DFParser::parse_sql(sql)?;

        if statements.len() != 1 {
            return Err(Error::NotImplemented(
                "The context currently only supports a single SQL statement".to_string(),
            ));
        }

        let state = self.inner.state.read().clone();
        let query_planner = SqlToRel::new(&state);

        match statements.pop_front().unwrap() {
            datafusion::sql::parser::Statement::Statement(s) => match *s {
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
                        node: Arc::new(CreateTable {
                            schema: cols.to_dfschema_ref()?,
                            name: name.to_string(),
                            if_not_exists,
                        }),
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
                    let plan = query_planner.query_to_plan(*source, &mut HashMap::new())?;

                    let column_exprs = columns
                        .iter()
                        .map(|id| {
                            Column::from_name(normalize_ident(id))
                        })
                        .collect();

                    Ok(LogicalPlan::Extension(Extension {
                        node: Arc::new(Insert {
                            name: table_name.to_string(),
                            columns: column_exprs,
                            input: Arc::new(plan),
                        }),
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
                        node: Arc::new(Update {
                            name: name.to_string(),
                            selection: selection_expr,
                            assignments: assignments_expr,
                        }),
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
                        node: Arc::new(Delete {
                            name: table_name.to_string(),
                            selection: selection_expr,
                        }),
                    }))
                }
                _ => Err(Error::NotImplemented(format!(
                    "Unsupported SQL statement: {:?}",
                    sql
                ))),
            },
            datafusion::sql::parser::Statement::DescribeTable(s) => {
                query_planner.describe_table_to_plan(s)
            }
            // Stub out the standard DataFusion CREATE EXTERNAL TABLE statements since we don't support them
            datafusion::sql::parser::Statement::CreateExternalTable(_) => {
                return Err(Error::NotImplemented(format!(
                    "Unsupported SQL statement: {:?}",
                    sql
                )))
            }
        }
    }

    pub async fn plan_query(&self, sql: &str) -> Result<Arc<dyn ExecutionPlan>> {
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
                self.catalog
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
                self.catalog.create_database(&catalog_name).await;
                Ok(make_dummy_exec())
            }
            // TODO DROP TABLE / DATABASE / SCHEMA
            LogicalPlan::CreateMemoryTable(CreateMemoryTable {
                name: _,
                input,
                if_not_exists: _,
                or_replace: _,
            }) => {
                // This is actually CREATE TABLE AS
                let physical = self.create_physical_plan(&input).await?;

                // TODO:
                //   - create a new table; get the table version ID
                //   - execute the physical plan
                //   - for each resulting partition:
                //     - write out to parquet
                //     - index (min-max values)
                //     - upload
                //     - write out: physical_region_column, physical_region (get id)
                //     - make a table_region entry; attach to existing version

                // Execute the plan and write it out to temporary Parquet files.
                let disk_manager = self.inner.runtime_env().disk_manager.clone();

                let object_store_url = ObjectStoreUrl::parse("seafowl://").unwrap();
                let store = self
                    .inner
                    .runtime_env()
                    .object_store(object_store_url.clone())?;

                plan_to_object_store(&self.inner.state(), physical, store, disk_manager).await?;

                Ok(make_dummy_exec())
            }
            LogicalPlan::DropTable(_) => {
                // DROP TABLE
                Ok(make_dummy_exec())
            }
            LogicalPlan::CreateView(_) => {
                // CREATE VIEW
                Ok(make_dummy_exec())
            }
            LogicalPlan::Extension(Extension { ref node }) => {
                // Other custom nodes we made like CREATE TABLE/INSERT/UPDATE/DELETE/ALTER
                let any = node.as_any();

                if let Some(CreateTable {
                    schema,
                    name,
                    if_not_exists: _,
                }) = any.downcast_ref::<CreateTable>()
                {
                    let table_ref = TableReference::from(name.as_str());
                    let (schema_name, table_name) = match table_ref {
                        TableReference::Bare { table: _ } => Err(Error::NotImplemented(
                            "Cannot CREATE TABLE without a schema qualifier!".to_string(),
                        )),
                        TableReference::Partial { schema, table } => Ok((schema, table)),
                        TableReference::Full {
                            catalog: _,
                            schema,
                            table,
                        } => Ok((schema, table)),
                    }?;

                    let sf_schema = SeafowlSchema {
                        arrow_schema: Arc::new(schema.as_ref().into()),
                    };

                    let collection_id = self
                        .catalog
                        .get_collection_id_by_name(&self.database, schema_name)
                        .await
                        .ok_or_else(|| {
                            Error::Plan(format!("Schema {:?} does not exist!", schema_name))
                        })?;

                    self.catalog
                        .create_table(collection_id, table_name, sf_schema)
                        .await;

                    Ok(make_dummy_exec())
                } else if let Some(Insert {
                    name: _,
                    columns: _,
                    input: _,
                }) = any.downcast_ref::<Insert>()
                {
                    // Duplicate the existing latest version into a new table
                    // (new table_version, same columns, same table_region)
                    // Proceed as in CREATE TABLE AS
                    Ok(make_dummy_exec())
                } else if let Some(Update {
                    name: _,
                    selection: _,
                    assignments: _,
                }) = any.downcast_ref::<Update>()
                {
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
                } else if let Some(Delete {
                    name: _,
                    selection: _,
                }) = any.downcast_ref::<Delete>()
                {
                    // - Duplicate the table (new version)
                    // - Similar to UPDATE, but just a filter

                    // upload new files
                    // replace regions (sometimes we delete them)

                    // really we want to be able to load all regions + cols for a table and then
                    // write that thing back to the db (set table regions)
                    Ok(make_dummy_exec())
                } else {
                    self.inner.create_physical_plan(&logical_plan).await
                }
            }
            _ => self.inner.create_physical_plan(&logical_plan).await,
        }
    }

    pub async fn create_physical_plan(&self, plan: &LogicalPlan) -> Result<Arc<dyn ExecutionPlan>> {
        let physical_plan = self.inner.create_physical_plan(plan).await?;
        Ok(physical_plan)
    }

    // Copied from DataFusion's physical_plan
    pub async fn collect(&self, physical_plan: Arc<dyn ExecutionPlan>) -> Result<Vec<RecordBatch>> {
        let stream = self.execute_stream(physical_plan).await?;
        stream.err_into().try_collect().await
    }

    pub async fn execute_stream(
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

    pub async fn execute_stream_partitioned(
        &self,
        physical_plan: &Arc<dyn ExecutionPlan>,
        partition: usize,
    ) -> Result<SendableRecordBatchStream> {
        let task_context = Arc::new(TaskContext::from(self.inner()));
        physical_plan.execute(partition, task_context)
    }
}
