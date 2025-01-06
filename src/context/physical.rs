use super::delta::CreateDeltaTableDetails;
use super::LakehouseTableProvider;
use crate::catalog::{DEFAULT_SCHEMA, STAGING_SCHEMA};
use crate::context::delta::plan_to_delta_adds;
use crate::context::SeafowlContext;
use crate::nodes::{
    ConvertTable, CreateFunction, CreateTable, DropFunction, RenameTable,
    SeafowlExtensionNode, Truncate, Vacuum,
};
use crate::object_store::factory::build_object_store;
use crate::object_store::http::try_prepare_http_url;
use crate::object_store::wrapped::InternalObjectStore;
use crate::provider::project_expressions;
use crate::utils::gc_databases;

use arrow_schema::{DataType, Schema, TimeUnit};
use chrono::TimeDelta;
use datafusion::common::DFSchema;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError as Error, Result};
use datafusion::physical_expr::execution_props::ExecutionProps;
use datafusion::physical_expr::expressions::{cast, Column};
use datafusion::physical_expr::{create_physical_expr, PhysicalExpr};
use datafusion::physical_optimizer::pruning::PruningPredicate;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{collect, execute_stream};
use datafusion::{
    arrow::{datatypes::SchemaRef, record_batch::RecordBatch},
    datasource::file_format::{parquet::ParquetFormat, FileFormat},
    error::DataFusionError,
    execution::context::TaskContext,
    physical_plan::{ExecutionPlan, SendableRecordBatchStream},
    sql::TableReference,
};
use datafusion_common::config::TableParquetOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{Column as ColumnExpr, ResolvedTableReference, SchemaReference};
use datafusion_expr::logical_plan::{
    CreateCatalog, CreateCatalogSchema, CreateExternalTable, CreateMemoryTable,
    DropTable, Extension, LogicalPlan, Projection,
};
use datafusion_expr::{
    DdlStatement, DmlStatement, DropCatalogSchema, Expr, Filter, WriteOp,
};
use deltalake::kernel::{Action, Add, Remove};
use deltalake::operations::vacuum::VacuumBuilder;
use deltalake::protocol::{DeltaOperation, SaveMode};
use deltalake::DeltaTable;
use object_store::path::Path;
use std::ops::Deref;
use std::ops::Not;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::info;
use url::Url;

use object_store::ObjectStoreScheme;
use object_store_factory::aws::S3Config;
use object_store_factory::google::GCSConfig;
use object_store_factory::ObjectStoreConfig;

/// Create an ExecutionPlan that doesn't produce any results.
/// This is used for queries that are actually run before we produce the plan,
/// since they have to manipulate catalog metadata or use async to write to it.
fn make_dummy_exec() -> Arc<dyn ExecutionPlan> {
    Arc::new(EmptyExec::new(SchemaRef::new(Schema::empty())))
}

impl SeafowlContext {
    /// Get the default object store used for writes, or error out if it's not configured.
    pub fn get_internal_object_store(&self) -> Result<Arc<InternalObjectStore>> {
        self.internal_object_store
            .clone()
            .ok_or(DataFusionError::Plan(
            "Internal object store isn't configured, CREATE EXTERNAL TABLE not supported"
                .to_string(),
        ))
    }
    pub async fn plan_query(&self, sql: &str) -> Result<Arc<dyn ExecutionPlan>> {
        let logical_plan = self.create_logical_plan(sql).await?;
        self.create_physical_plan(&logical_plan).await
    }

    pub async fn create_physical_plan(
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
                cmd @ CreateExternalTable { .. },
            )) => {
                let internal_object_store = self.get_internal_object_store()?;
                let cmd =
                    self.prepare_create_external_table(cmd, internal_object_store)?;

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
                self.metastore
                    .schemas
                    .create(&self.default_catalog, schema_name)
                    .await?;
                Ok(make_dummy_exec())
            }
            LogicalPlan::Ddl(DdlStatement::CreateCatalog(CreateCatalog {
                catalog_name,
                if_not_exists,
                ..
            })) => {
                if self.metastore.catalogs.get(catalog_name).await.is_ok() {
                    if !*if_not_exists {
                        return Err(DataFusionError::Plan(format!(
                            "Database {catalog_name} already exists"
                        )));
                    } else {
                        return Ok(make_dummy_exec());
                    }
                }

                // Persist DB into metadata catalog
                self.metastore.catalogs.create(catalog_name).await?;

                // Create the corresponding default schema as well
                self.metastore
                    .schemas
                    .create(catalog_name, DEFAULT_SCHEMA)
                    .await?;

                Ok(make_dummy_exec())
            }
            LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(CreateMemoryTable {
                name,
                input,
                if_not_exists,
                or_replace: _,
                ..
            })) => {
                // This is actually CREATE TABLE AS
                if *if_not_exists && self.try_get_delta_table(name.clone()).await.is_ok()
                {
                    // Table already exists
                    return Ok(make_dummy_exec());
                }

                let plan = self.inner.state().create_physical_plan(input).await?;
                let plan = self.coerce_plan(plan).await?;

                // First create the table and then insert the data from the subquery
                // TODO: this means we'll have 2 table versions at the end, 1st from the create
                // and 2nd from the insert, while it seems more reasonable that in this case we have
                // only one
                self.create_delta_table(
                    name.clone(),
                    CreateDeltaTableDetails::EmptyTable(plan.schema().as_ref().clone()),
                )
                .await?;
                self.plan_to_delta_table(name.clone(), &plan).await?;

                Ok(make_dummy_exec())
            }
            LogicalPlan::Dml(DmlStatement {
                table_name,
                op: WriteOp::Insert(_),
                input,
                ..
            }) => {
                let physical = self.inner.state().create_physical_plan(input).await?;
                match self
                    .get_lakehouse_table_provider(table_name.clone())
                    .await?
                {
                    LakehouseTableProvider::Delta(_) => {
                        self.plan_to_delta_table(table_name.clone(), &physical)
                            .await?;
                        Ok(make_dummy_exec())
                    }
                    LakehouseTableProvider::Iceberg(provider) => {
                        let table = provider.table();
                        let table_location = table.metadata().location();
                        let file_io = table.file_io();
                        self.plan_to_iceberg_table(file_io, table_location, &physical)
                            .await?;
                        Ok(make_dummy_exec())
                    }
                }
            }
            LogicalPlan::Dml(DmlStatement {
                table_name,
                op: WriteOp::Update,
                input,
                ..
            }) => {
                let internal_object_store = self.get_internal_object_store()?;
                // Destructure input into projection expressions and the upstream scan/filter plan
                let LogicalPlan::Projection(Projection { expr, input, .. }) = &**input
                else {
                    return Err(DataFusionError::Plan(
                        "Update plan doesn't contain a Projection node".to_string(),
                    ));
                };

                let mut table = self.try_get_delta_table(table_name.clone()).await?;
                table.load().await?;
                let snapshot = table.snapshot()?;

                let schema_ref = TableProvider::schema(&table);
                let df_schema = DFSchema::try_from_qualified_schema(
                    table_name.table(),
                    schema_ref.as_ref(),
                )?;

                let state = self.inner.state();
                let mut filter = vec![];

                let (selection_expr, removes) =
                    if let LogicalPlan::Filter(Filter { predicate, .. }) = &**input {
                        // The scan builder in delta-rs builds physical expressions for pruning
                        // using non-qualified schemas, but the filter expressions in the UPDATE
                        // are qualified thanks to https://github.com/apache/arrow-datafusion/pull/7316
                        //
                        // This leads to a panic unless we strip out the qualifier first.
                        filter.push(
                            predicate
                                .clone()
                                .transform(&|expr| {
                                    Ok(
                                        if let Expr::Column(ColumnExpr {
                                            relation: Some(_),
                                            name,
                                        }) = &expr
                                        {
                                            Transformed::yes(Expr::Column(
                                                ColumnExpr::new_unqualified(name),
                                            ))
                                        } else {
                                            Transformed::no(expr)
                                        },
                                    )
                                })
                                .data()?,
                        );

                        // A WHERE clause has been used; employ it to prune the update down to only
                        // a subset of files, while inheriting the rest from the previous version
                        let filter_expr = create_physical_expr(
                            &predicate.clone(),
                            &df_schema,
                            &ExecutionProps::new(),
                        )?;

                        let pruning_predicate = PruningPredicate::try_new(
                            filter_expr.clone(),
                            schema_ref.clone(),
                        )?;
                        let prune_map = pruning_predicate.prune(snapshot)?;

                        let files_to_prune = snapshot
                            .file_actions()?
                            .iter()
                            .zip(prune_map)
                            .filter_map(
                                |(add, keep)| if keep { Some(add.clone()) } else { None },
                            )
                            .collect::<Vec<Add>>();

                        (Some(filter_expr), files_to_prune)
                    } else {
                        // If no qualifier is specified we're basically updating the whole table.
                        (None, snapshot.file_actions()?)
                    };

                let uuid = self.get_table_uuid(table_name.clone()).await?;
                let mut actions: Vec<Action> = vec![];
                if !removes.is_empty() {
                    let base_scan = table.scan(&state, None, &filter, None).await?;

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
                    let object_store = internal_object_store
                        .get_log_store(&uuid.to_string())
                        .object_store();
                    let local_table_dir =
                        internal_object_store.local_table_dir(&uuid.to_string());
                    let adds = plan_to_delta_adds(
                        &state,
                        &update_plan,
                        object_store,
                        local_table_dir,
                        self.config.misc.max_partition_size,
                    )
                    .await?;

                    let deletion_timestamp = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as i64;

                    actions = adds.into_iter().map(Action::Add).collect();
                    for remove in removes {
                        actions.push(Action::Remove(Remove {
                            path: remove.path,
                            deletion_timestamp: Some(deletion_timestamp),
                            data_change: true,
                            extended_file_metadata: Some(true),
                            partition_values: Some(remove.partition_values),
                            size: Some(remove.size),
                            tags: None,
                            deletion_vector: None,
                            base_row_id: None,
                            default_row_commit_version: None,
                        }))
                    }
                }

                let op = DeltaOperation::Write {
                    mode: SaveMode::Append,
                    partition_by: None,
                    predicate: None,
                };

                let version = self.commit_delta_table(actions, &table, op).await?;

                self.metastore
                    .tables
                    .create_new_version(uuid, version)
                    .await?;

                Ok(make_dummy_exec())
            }
            LogicalPlan::Dml(DmlStatement {
                table_name,
                table_schema,
                op: WriteOp::Delete,
                input,
                ..
            }) => {
                let internal_object_store = self.get_internal_object_store()?;
                let uuid = self.get_table_uuid(table_name.clone()).await?;

                let mut table = self.try_get_delta_table(table_name.clone()).await?;
                table.load().await?;
                let snapshot = table.snapshot()?;
                let schema_ref = SchemaRef::from(table_schema.deref().clone());

                let (adds, removes) =
                    if let LogicalPlan::Filter(Filter { predicate, .. }) = &**input {
                        // A WHERE clause has been used; employ it to prune the filtration
                        // down to only a subset of partitions, re-use the rest as is

                        let state = self.inner.state();

                        let prune_expr = create_physical_expr(
                            &predicate.clone(),
                            table_schema,
                            &ExecutionProps::new(),
                        )?;

                        let pruning_predicate =
                            PruningPredicate::try_new(prune_expr, schema_ref.clone())?;
                        let prune_map = pruning_predicate.prune(snapshot)?;
                        let files_to_prune = snapshot
                            .file_actions()?
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
                                &ExecutionProps::new(),
                            )?;

                            let base_scan = table
                                .scan(&state, None, &[predicate.clone()], None)
                                .await?;

                            let filter_plan: Arc<dyn ExecutionPlan> =
                                Arc::new(FilterExec::try_new(filter_expr, base_scan)?);

                            // Write the filtered out data
                            let object_store = internal_object_store
                                .get_log_store(&uuid.to_string())
                                .object_store();
                            let local_table_dir =
                                internal_object_store.local_table_dir(&uuid.to_string());
                            let adds = plan_to_delta_adds(
                                &state,
                                &filter_plan,
                                object_store,
                                local_table_dir,
                                self.config.misc.max_partition_size,
                            )
                            .await?;

                            (adds, files_to_prune)
                        }
                    } else {
                        // If no qualifier is specified we're basically truncating the table.
                        // Remove all files.
                        (vec![], snapshot.file_actions()?)
                    };

                let deletion_timestamp = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64;

                let mut actions: Vec<Action> =
                    adds.into_iter().map(Action::Add).collect();
                for remove in removes {
                    actions.push(Action::Remove(Remove {
                        path: remove.path,
                        deletion_timestamp: Some(deletion_timestamp),
                        data_change: true,
                        extended_file_metadata: Some(true),
                        partition_values: Some(remove.partition_values),
                        size: Some(remove.size),
                        tags: None,
                        deletion_vector: None,
                        base_row_id: None,
                        default_row_commit_version: None,
                    }))
                }

                let op = DeltaOperation::Delete { predicate: None };

                let version = self.commit_delta_table(actions, &table, op).await?;

                self.metastore
                    .tables
                    .create_new_version(uuid, version)
                    .await?;

                Ok(make_dummy_exec())
            }
            LogicalPlan::Ddl(DdlStatement::DropTable(DropTable {
                name,
                if_exists,
                schema: _,
            })) => {
                if *if_exists && self.try_get_delta_table(name.clone()).await.is_err() {
                    // The table doesn't exist
                    return Ok(make_dummy_exec());
                }

                let resolved_ref = self.resolve_table_ref(name.clone());

                if resolved_ref.schema.as_ref() == STAGING_SCHEMA {
                    // Dropping a staging table is a in-memory only op
                    self.inner.deregister_table(resolved_ref)?;
                    return Ok(make_dummy_exec());
                }

                self.delete_delta_table(name.clone())
                    .await
                    .unwrap_or_else(|e| info!("Failed to cleanup table {name}: {e}"));

                self.metastore
                    .tables
                    .delete(
                        &resolved_ref.catalog,
                        &resolved_ref.schema,
                        &resolved_ref.table,
                    )
                    .await?;
                Ok(make_dummy_exec())
            }
            LogicalPlan::Ddl(DdlStatement::DropCatalogSchema(DropCatalogSchema {
                name,
                if_exists,
                ..
            })) => {
                let schema_name = name.schema_name();

                if let SchemaReference::Full { catalog, .. } = name
                    && catalog.as_ref() != self.default_catalog
                {
                    return Err(DataFusionError::Execution(
                        "Cannot delete schemas in other catalogs".to_string(),
                    ));
                }

                let schema = match self
                    .inner
                    .catalog(&self.default_catalog)
                    .expect("Current catalog exists")
                    .schema(schema_name)
                {
                    None => {
                        if *if_exists {
                            return Ok(make_dummy_exec());
                        } else {
                            return Err(DataFusionError::Execution(format!(
                                "Schema {schema_name} does not exist"
                            )));
                        }
                    }
                    Some(schema) => schema,
                };

                // Delete each table sequentially
                for table_name in schema.table_names() {
                    let table_ref = ResolvedTableReference {
                        catalog: Arc::from(self.default_catalog.as_str()),
                        schema: Arc::from(schema_name),
                        table: Arc::from(table_name),
                    };
                    self.delete_delta_table(table_ref.clone()).await?;

                    self.metastore
                        .tables
                        .delete(&table_ref.catalog, &table_ref.schema, &table_ref.table)
                        .await?;
                }

                self.metastore
                    .schemas
                    .delete(&self.default_catalog, schema_name)
                    .await?;

                Ok(make_dummy_exec())
            }
            LogicalPlan::Ddl(DdlStatement::CreateView(_)) => Err(Error::Plan(
                "Creating views is currently unsupported!".to_string(),
            )),
            LogicalPlan::Extension(Extension { ref node }) => {
                // Other custom nodes we made like CREATE TABLE/INSERT/ALTER
                match SeafowlExtensionNode::from_dynamic(node) {
                    Some(sfe_node) => match sfe_node {
                        SeafowlExtensionNode::ConvertTable(ConvertTable {
                            location,
                            name,
                            ..
                        }) => {
                            self.create_delta_table(
                                name,
                                CreateDeltaTableDetails::FromPath(Path::from(
                                    location.as_str(),
                                )),
                            )
                            .await?;

                            Ok(make_dummy_exec())
                        }
                        SeafowlExtensionNode::CreateTable(CreateTable {
                            schema,
                            name,
                            ..
                        }) => {
                            self.create_delta_table(
                                name.as_str(),
                                CreateDeltaTableDetails::EmptyTable(schema.clone()),
                            )
                            .await?;

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
                            self.metastore
                                .functions
                                .create(&self.default_catalog, name, *or_replace, details)
                                .await?;

                            Ok(make_dummy_exec())
                        }
                        SeafowlExtensionNode::DropFunction(DropFunction {
                            if_exists,
                            func_names,
                            output_schema: _,
                        }) => {
                            self.metastore
                                .functions
                                .delete(&self.default_catalog, *if_exists, func_names)
                                .await?;
                            Ok(make_dummy_exec())
                        }
                        SeafowlExtensionNode::RenameTable(RenameTable {
                            old_name,
                            new_name,
                            ..
                        }) => {
                            // Resolve new table reference
                            let resolved_new_ref = self.resolve_table_ref(new_name);
                            if resolved_new_ref.catalog.as_ref() != self.default_catalog {
                                return Err(Error::Plan(
                                    "Changing the table's database is not supported!"
                                        .to_string(),
                                ));
                            }

                            // Resolve old table reference
                            let resolved_old_ref = self.resolve_table_ref(old_name);

                            // Finally update our catalog entry
                            self.metastore
                                .tables
                                .update(
                                    &resolved_old_ref.catalog,
                                    &resolved_old_ref.schema,
                                    &resolved_old_ref.table,
                                    &resolved_new_ref.catalog,
                                    &resolved_new_ref.schema,
                                    &resolved_new_ref.table,
                                )
                                .await?;
                            Ok(make_dummy_exec())
                        }
                        SeafowlExtensionNode::Truncate(Truncate {
                            table_name, ..
                        }) => {
                            let mut table = self.try_get_delta_table(table_name).await?;
                            table.load().await?;
                            let snapshot = table.snapshot()?;
                            let deletion_timestamp = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_millis()
                                as i64;
                            let actions: Vec<Action> = snapshot
                                .file_actions()?
                                .into_iter()
                                .map(|add_action| {
                                    Action::Remove(Remove {
                                        path: add_action.path,
                                        deletion_timestamp: Some(deletion_timestamp),
                                        data_change: true,
                                        extended_file_metadata: Some(true),
                                        partition_values: Some(
                                            add_action.partition_values,
                                        ),
                                        size: Some(add_action.size),
                                        tags: None,
                                        deletion_vector: None,
                                        base_row_id: None,
                                        default_row_commit_version: None,
                                    })
                                })
                                .collect();

                            let op = DeltaOperation::Delete { predicate: None };
                            self.commit_delta_table(actions, &table, op).await?;

                            Ok(make_dummy_exec())
                        }
                        SeafowlExtensionNode::Vacuum(Vacuum {
                            database,
                            table_name,
                            ..
                        }) => {
                            if database.is_some() {
                                let internal_object_store =
                                    self.get_internal_object_store()?;
                                gc_databases(
                                    self,
                                    internal_object_store,
                                    database.clone(),
                                )
                                .await;
                            } else if let Some(table_name) = table_name {
                                let resolved_ref = self.resolve_table_ref(table_name);

                                if let Ok(mut delta_table) =
                                    self.try_get_delta_table(resolved_ref.clone()).await
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
                                        delta_table.log_store(),
                                        delta_table.snapshot()?.clone(),
                                    )
                                    .with_enforce_retention_duration(false)
                                    .with_retention_period(TimeDelta::zero());

                                    let (_, metrics) = plan.await?;
                                    let deleted_files = metrics.files_deleted;
                                    info!("Deleted Delta table tombstones {deleted_files:?}");
                                }

                                match self
                                    .metastore
                                    .tables
                                    .delete_old_versions(
                                        &resolved_ref.catalog,
                                        &resolved_ref.schema,
                                        &resolved_ref.table,
                                    )
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

    fn prepare_create_external_table(
        &self,
        cmd: &CreateExternalTable,
        internal_object_store: Arc<InternalObjectStore>,
    ) -> Result<CreateExternalTable> {
        let mut cmd = cmd.clone();
        cmd.name = self.resolve_staging_ref(&cmd.name)?;
        cmd.location = match try_prepare_http_url(&cmd.location) {
            Some(new_loc) => new_loc,
            None => cmd.location,
        };
        let table_path = ListingTableUrl::parse(&cmd.location)?;
        let url: &Url = table_path.as_ref();
        let parsed_result = ObjectStoreScheme::parse(url);
        if let Ok((scheme, _)) = parsed_result
            && matches!(
                scheme,
                ObjectStoreScheme::AmazonS3 | ObjectStoreScheme::GoogleCloudStorage
            )
        {
            let bucket = url
                .host_str()
                .ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "Unable to parse bucket name from URL: {}",
                        url.as_str()
                    ))
                })?
                .to_string();

            let config = match scheme {
                ObjectStoreScheme::AmazonS3 => {
                    let s3_config = if cmd.options.is_empty() {
                        if let ObjectStoreConfig::AmazonS3(s3) =
                            &internal_object_store.config
                        {
                            S3Config {
                                bucket: bucket.clone(),
                                ..s3.clone()
                            }
                        } else {
                            return Err(DataFusionError::Execution(
                                "Expected AmazonS3 config".into(),
                            ));
                        }
                    } else {
                        S3Config::from_bucket_and_options(bucket, &mut cmd.options)?
                    };
                    ObjectStoreConfig::AmazonS3(s3_config)
                }
                ObjectStoreScheme::GoogleCloudStorage => {
                    let gcs_config = if cmd.options.is_empty() {
                        if let ObjectStoreConfig::GoogleCloudStorage(gcs) =
                            &internal_object_store.config
                        {
                            GCSConfig {
                                bucket: bucket.clone(),
                                ..gcs.clone()
                            }
                        } else {
                            return Err(DataFusionError::Execution(
                                "Expected GoogleCloudStorage config".into(),
                            ));
                        }
                    } else {
                        GCSConfig::from_bucket_and_options(bucket, &mut cmd.options)?
                    };
                    ObjectStoreConfig::GoogleCloudStorage(gcs_config)
                }
                _ => unreachable!(),
            };

            let object_store =
                build_object_store(&config, &self.config.misc.object_store_cache)?;
            self.inner
                .runtime_env()
                .register_object_store(url, object_store);
        }
        Ok(cmd)
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

    // Copied from DataFusion's physical_plan
    pub async fn collect(
        &self,
        physical_plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Vec<RecordBatch>> {
        let task_context = Arc::new(TaskContext::from(self.inner()));
        collect(physical_plan, task_context).await
    }

    pub async fn execute_stream(
        &self,
        physical_plan: Arc<dyn ExecutionPlan>,
    ) -> Result<SendableRecordBatchStream> {
        let task_context = Arc::new(TaskContext::from(self.inner()));
        execute_stream(physical_plan, task_context)
    }

    /// Append data from the provided file, creating a new schema/table if absent
    pub async fn file_to_table(
        &self,
        file_path: String,
        file_type: &str,
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
            .catalog(&self.default_catalog)
            .ok_or_else(|| {
                Error::Plan(format!("Database {} not found!", self.default_catalog))
            })?
            .schema(&schema_name)
        {
            Some(_) => {
                // Schema exists, check if existing table's schema matches the new one
                match self.inner.table_provider(&table_name).await {
                    Ok(table) => {
                        table_schema = Some(table.schema());
                        true
                    }
                    Err(_) => false,
                }
            }
            None => {
                // Schema doesn't exist; create one first, and then reload to pick it up
                self.metastore
                    .schemas
                    .create(&self.default_catalog, &schema_name)
                    .await?;
                self.reload_schema().await?;
                false
            }
        };

        // Create a `ListingTable` that points to the specified file
        let table_path = ListingTableUrl::parse(file_path)?;
        let file_format: Arc<dyn FileFormat> = match file_type {
            "csv" => Arc::new(CsvFormat::default().with_has_header(has_header)),
            "parquet" => {
                // TODO: We can remove this once delta-rs supports Utf8View
                let mut options = TableParquetOptions::default();
                options.global.schema_force_view_types = false;
                Arc::new(ParquetFormat::default().with_options(options))
            }
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
            catalog: Arc::from(self.default_catalog.as_str()),
            schema: Arc::from(schema_name),
            table: Arc::from(table_name),
        };

        if !table_exists {
            self.create_delta_table(
                table_ref.clone(),
                CreateDeltaTableDetails::EmptyTable(plan.schema().as_ref().clone()),
            )
            .await?;
        }

        self.plan_to_delta_table(table_ref, &plan).await
    }
}

#[cfg(test)]
mod tests {
    use crate::context::test_utils::in_memory_context_with_test_db;
    use datafusion::assert_batches_eq;

    use super::super::test_utils::in_memory_context;
    use super::*;

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

        // We don't actually expect anything now that we're doing eager deletion.
        let expected = [
            "+--------------+------------+------+-----------------+",
            "| table_schema | table_name | uuid | deletion_status |",
            "+--------------+------------+------+-----------------+",
            "+--------------+------------+------+-----------------+",
        ];
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

    #[tokio::test]
    async fn test_plan_insert_type_mismatch() {
        let ctx = in_memory_context_with_test_db().await;

        // Try inserting a string into a date (note this will work fine for inserting
        // e.g. Utf-8 into numbers at plan time but should fail at execution time if the value
        // doesn't convert)
        let plan = ctx
            .create_logical_plan("INSERT INTO testcol.some_table SELECT 'abc', to_timestamp('2022-01-01T12:00:00')")
            .await.unwrap();

        let err = ctx.create_physical_plan(&plan).await.unwrap_err();
        assert_eq!(
            err.to_string(),
            "Arrow error: Cast error: Cannot cast string 'abc' to value of Date32 type"
        );
    }

    #[tokio::test]
    async fn test_create_table_if_not_exists() -> Result<()> {
        let context = Arc::new(in_memory_context().await);

        context
            .plan_query("CREATE TABLE test_table AS VALUES (1, 'one'), (2, 'two')")
            .await?;

        // Ensure it errors out first without the `IF NOT EXISTS` clause
        let err = context
            .plan_query("CREATE TABLE test_table AS VALUES (1, 'one'), (2, 'two')")
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "Error during planning: Table \"test_table\" already exists"
        );

        context
            .plan_query(
                "CREATE TABLE IF NOT EXISTS test_table AS VALUES (1, 'one'), (2, 'two')",
            )
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_drop_table_if_exists() -> Result<()> {
        let context = Arc::new(in_memory_context().await);

        // Ensure it errors out first without the `IF NOT EXISTS` clause
        let err = context
            .plan_query("DROP TABLE test_table")
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "Error during planning: Table \"test_table\" doesn't exist"
        );

        context
            .plan_query("DROP TABLE IF EXISTS test_table")
            .await?;

        Ok(())
    }
}
