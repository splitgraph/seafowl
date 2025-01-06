use crate::context::SeafowlContext;
use crate::sync::metrics::SyncPlanMetrics;
use crate::sync::schema::merge::{merge_schemas, MergeProjection};
use crate::sync::schema::{SyncColumn, SyncSchema};
use crate::sync::utils::{construct_qualifier, get_prune_map};
use crate::sync::writer::{now, DataSyncItem};
use crate::sync::SyncResult;
use arrow::array::RecordBatch;
use arrow_schema::{SchemaBuilder, SchemaRef};
use clade::sync::{ColumnDescriptor, ColumnRole};
use datafusion::catalog::TableProvider;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::{provider_as_source, MemTable};
use datafusion::execution::session_state::{SessionState, SessionStateBuilder};
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_optimizer::pruning::PruningPredicate;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::{
    Column, DFSchema, DFSchemaRef, JoinType, ScalarValue, ToDFSchema,
};
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::{
    col, lit, when, EmptyRelation, Expr, LogicalPlan, LogicalPlanBuilder, Projection,
};
use datafusion_functions_nested::expr_fn::make_array;
use deltalake::delta_datafusion::DeltaTableProvider;
use deltalake::kernel::{Action, Add, Remove};
use deltalake::DeltaTable;
use std::iter::once;
use std::ops::Not;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, info, trace, warn};

use super::SyncError;

pub(super) const LOWER_REL: &str = "__lower_rel";
pub(super) const UPPER_REL: &str = "__upper_rel";
pub(super) const UPSERT_COL: &str = "__upsert_col";
const FINE_GRAINED_PRUNING_ROW_CRITERIA: i64 = 3_000_000;

pub(super) struct SeafowlSyncPlanner {
    context: Arc<SeafowlContext>,
    metrics: SyncPlanMetrics,
}

impl SeafowlSyncPlanner {
    pub fn new(context: Arc<SeafowlContext>) -> Self {
        Self {
            context,
            metrics: Default::default(),
        }
    }

    pub(super) async fn plan_iceberg_syncs(
        &self,
        syncs: &[DataSyncItem],
        table_schema: Arc<arrow_schema::Schema>,
    ) -> SyncResult<Arc<dyn ExecutionPlan>> {
        for sync in syncs {
            for sc in sync.sync_schema.columns() {
                if sc.role() == ColumnRole::OldPk {
                    for batch in &sync.data {
                        let null_count = batch
                            .column_by_name(sc.field().name())
                            .expect("Old PK array must exist")
                            .null_count();
                        if batch.num_rows() != null_count {
                            return Err(SyncError::InvalidMessage {
                                reason: "Old PK for Iceberg contains non-null value"
                                    .to_string(),
                            });
                        }
                    }
                }
            }
        }

        let df_table_schema = DFSchema::try_from(table_schema.clone())?;

        let base_plan =
            LogicalPlanBuilder::new(LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema: Arc::new(df_table_schema),
            }))
            .alias(LOWER_REL)?
            .build()?;

        let base_df = DataFrame::new(self.session_state(), base_plan);

        let (sync_schema, sync_df) = self.squash_syncs(syncs)?;
        let (sync_schema, sync_df) = self.normalize_syncs(&sync_schema, sync_df)?;
        let input_df = self
            .apply_syncs(table_schema, base_df, sync_df, &sync_schema)
            .await?;
        let input_plan = input_df.create_physical_plan().await?;
        Ok(input_plan)
    }

    // Construct a plan for flushing the pending syncs to the provided table.
    // Return the plan and the files that are re-written by it (to be removed from the table state).
    pub(super) async fn plan_delta_syncs(
        &self,
        syncs: &[DataSyncItem],
        table: &DeltaTable,
    ) -> SyncResult<(Arc<dyn ExecutionPlan>, Vec<Action>)> {
        // Use the schema from the object store as a source of truth, since it's not guaranteed
        // that any of the entries has the full column list.
        let full_schema = TableProvider::schema(table);

        let prune_start = Instant::now();
        // Gather previous Add files that (might) need to be re-written.
        let files = self.prune_partitions(syncs, full_schema.clone(), table)?;
        let prune_time = prune_start.elapsed().as_millis();
        info!(
            "Partition pruning found {} files in {prune_time} ms",
            files.len(),
        );
        self.metrics.pruning_time.record(prune_time as f64);
        self.metrics.pruning_files.record(files.len() as f64);

        // Create removes to prune away files that are refuted by the qualifier
        let removes = files
            .iter()
            .map(|add| {
                Action::Remove(Remove {
                    path: add.path.clone(),
                    deletion_timestamp: Some(now() as i64),
                    data_change: true,
                    extended_file_metadata: Some(true),
                    partition_values: Some(add.partition_values.clone()),
                    size: Some(add.size),
                    tags: None,
                    deletion_vector: None,
                    base_row_id: None,
                    default_row_commit_version: None,
                })
            })
            .collect::<Vec<_>>();

        // Create a special Delta table provider that will only hit the above partition files
        let base_scan = Arc::new(
            DeltaTableProvider::try_new(
                table.snapshot()?.clone(),
                table.log_store(),
                Default::default(),
            )?
            .with_files(files),
        );

        // Convert the custom Delta table provider into a base logical plan
        let base_plan =
            LogicalPlanBuilder::scan(LOWER_REL, provider_as_source(base_scan), None)?
                .build()?;

        let base_df = DataFrame::new(self.session_state(), base_plan);
        let (sync_schema, sync_df) = self.squash_syncs(syncs)?;
        let (sync_schema, sync_df) = self.normalize_syncs(&sync_schema, sync_df)?;

        let input_df = self
            .apply_syncs(full_schema, base_df, sync_df, &sync_schema)
            .await?;
        let input_plan = input_df.create_physical_plan().await?;

        Ok((input_plan, removes))
    }

    // Construct a state for physical planning; we omit all analyzer/optimizer rules to increase
    // the stack overflow threshold that occurs during recursive plan tree traversal in DF.
    fn session_state(&self) -> SessionState {
        SessionStateBuilder::new_from_existing(self.context.inner.state())
            .with_analyzer_rules(vec![])
            .with_optimizer_rules(vec![])
            .build()
    }

    // Perform logical squashing of the provided sync batches into a single dataframe/plan, which can
    // then be joined against the base scan
    fn squash_syncs(
        &self,
        syncs: &[DataSyncItem],
    ) -> SyncResult<(SyncSchema, DataFrame)> {
        let first_sync = syncs.first().unwrap();
        let mut sync_schema = first_sync.sync_schema.clone();
        let first_sync_data = first_sync.data.clone();
        let provider = MemTable::try_new(
            first_sync_data.first().unwrap().schema(),
            vec![first_sync_data],
        )?;
        let mut sync_plan = LogicalPlanBuilder::scan(
            LOWER_REL,
            provider_as_source(Arc::new(provider)),
            None,
        )?
        .build()?;

        // Make a plan to squash all syncs into a single change stream
        for sync in &syncs[1..] {
            (sync_schema, sync_plan) = self.merge_syncs(
                &sync_schema,
                sync_plan,
                &sync.sync_schema,
                sync.data.first().unwrap().clone(),
            )?;
        }

        let sync_df = DataFrame::new(self.session_state(), sync_plan);
        Ok((sync_schema, sync_df))
    }

    fn merge_syncs(
        &self,
        lower_schema: &SyncSchema,
        lower_plan: LogicalPlan,
        upper_schema: &SyncSchema,
        upper_data: RecordBatch,
    ) -> SyncResult<(SyncSchema, LogicalPlan)> {
        let upper_append_only = upper_schema
            .columns()
            .iter()
            .find_map(|sc| {
                if sc.role() == ColumnRole::OldPk {
                    let null_count = upper_data
                        .column_by_name(sc.field().name())
                        .expect("Old PK array must exist")
                        .null_count();
                    Some(upper_data.num_rows() == null_count)
                } else {
                    None
                }
            })
            .expect("At least 1 old PK column must exist");

        let provider = MemTable::try_new(upper_data.schema(), vec![vec![upper_data]])?;
        let upper_plan = LogicalPlanBuilder::scan(
            UPPER_REL,
            provider_as_source(Arc::new(provider)),
            None,
        )?;

        if upper_append_only {
            self.union_syncs(lower_schema, lower_plan, upper_schema, upper_plan)
        } else {
            self.join_syncs(lower_schema, lower_plan, upper_schema, upper_plan)
        }
    }

    // Build a plan to union two adjacent syncs into one when the upper sync is append-only, meaning
    // there are no PK chains to resolve.
    fn union_syncs(
        &self,
        lower_schema: &SyncSchema,
        lower_plan: LogicalPlan,
        upper_schema: &SyncSchema,
        upper_plan: LogicalPlanBuilder,
    ) -> SyncResult<(SyncSchema, LogicalPlan)> {
        let (col_desc, merged) =
            merge_schemas(lower_schema, upper_schema, MergeProjection::new_union())?;

        let (low_proj, upp_proj) = merged.union();

        let lower_plan = self.project_expressions(lower_plan, low_proj)?;
        let upper_plan = upper_plan.project(upp_proj)?.build()?;
        let merged_plan = LogicalPlanBuilder::from(lower_plan)
            .union(upper_plan)?
            .build()?;

        Ok((
            SyncSchema::try_new(col_desc, merged_plan.schema().inner().clone(), true)?,
            merged_plan,
        ))
    }

    // Build a plan to join two adjacent syncs into one, to resolve potential PK-chains.
    // It assumes that both syncs were squashed before-hand (i.e. no PK-chains in either one).
    fn join_syncs(
        &self,
        lower_schema: &SyncSchema,
        lower_plan: LogicalPlan,
        upper_schema: &SyncSchema,
        upper_plan: LogicalPlanBuilder,
    ) -> SyncResult<(SyncSchema, LogicalPlan)> {
        let exprs = self.add_sync_flag(upper_plan.schema(), UPPER_REL);
        let upper_plan = upper_plan.project(exprs)?;

        let (lower_join_cols, upper_join_cols): (Vec<String>, Vec<String>) = upper_schema
            .map_columns(ColumnRole::OldPk, |sc| {
                let lower_name = lower_schema
                    .column(sc.name(), ColumnRole::NewPk)
                    .expect("PK columns identical")
                    .field()
                    .name()
                    .clone();
                let upper_name = sc.field().name().clone();
                (lower_name, upper_name)
            })
            .into_iter()
            .unzip();

        // Add the `LOWER_REL` flag to lower plan
        let lower_plan =
            if let LogicalPlan::Projection(Projection {
                mut expr, input, ..
            }) = lower_plan
            {
                // In case of projection just extend the present list of expressions with the flag,
                // saving us from adding one more plan node.
                expr.push(lit(true).alias(LOWER_REL));
                Projection::try_new(expr, input).map(LogicalPlan::Projection)?
            } else {
                let exprs = self.add_sync_flag(lower_plan.schema(), LOWER_REL);
                self.project_expressions(lower_plan, exprs)?
            };

        let join_plan = upper_plan
            .join(
                lower_plan,
                JoinType::Full,
                (upper_join_cols, lower_join_cols),
                None,
            )?
            .build()?;

        // Build the merged projection and column descriptors
        let (col_desc, merged) =
            merge_schemas(lower_schema, upper_schema, MergeProjection::new_join())?;
        let projection = merged.join();

        let merged_plan = self.project_expressions(join_plan, projection)?;

        Ok((
            SyncSchema::try_new(col_desc, merged_plan.schema().inner().clone(), true)?,
            merged_plan,
        ))
    }

    // Normalize the squashed sync, by collapsing old and new pks into a single group,
    // projecting the upsert flag column, and breaking up PK-changing UPDATEs into
    // DELETEs + UPSERTs.
    fn normalize_syncs(
        &self,
        sync_schema: &SyncSchema,
        sync_df: DataFrame,
    ) -> SyncResult<(SyncSchema, DataFrame)> {
        // Pick the first Old/New PK for constructing the cases below
        let (old_pk, new_pk) = sync_schema
            .columns()
            .iter()
            .find_map(|sc| {
                if sc.role() == ColumnRole::OldPk {
                    let old_name = sc.field().name();
                    let new_name = sync_schema
                        .column(sc.name(), ColumnRole::NewPk)
                        .unwrap()
                        .field()
                        .name();
                    Some((col(old_name), col(new_name)))
                } else {
                    None
                }
            })
            .expect("At least 1 old PK column must exist");

        // Filter out any temp rows
        let sync_df = sync_df
            .filter(old_pk.clone().is_null().and(new_pk.clone().is_null()).not())?;

        // Construct the conditions for all possible 4 cases that we want to normalize
        let insert = old_pk.clone().is_null().and(new_pk.clone().is_not_null());
        let pk_preserving_update = old_pk
            .clone()
            .is_not_null()
            .and(new_pk.clone().is_not_null())
            .and(old_pk.clone().eq(new_pk.clone()));
        let pk_changing_update = old_pk
            .clone()
            .is_not_null()
            .and(new_pk.clone().is_not_null())
            .and(old_pk.clone().not_eq(new_pk.clone()));

        let mut projection = vec![];
        let mut unnest_cols = vec![];
        let mut column_descriptors = vec![];
        // Now generate the expressions to project the sync from the "old-new" form
        // +-----------+-----------+----------------------+
        // | old_pk_c1 | new_pk_c1 | value_c2             |
        // +-----------+-----------+----------------------+
        // |           | 1         | insert               |
        // | 2         | 2         | pk-preserving-update |
        // | 3         | 4         | pk-changing-update   |
        // | 5         |           | delete               |
        // +-----------+-----------+----------------------+
        //
        // into the "upsert" form
        //
        // +-----------+----------------------+--------------+
        // | pk_c1     | value_c2             | upsert       |
        // +-----------+----------------------+--------------+
        // | [1]       | insert               | [true]       |
        // | [2]       | pk-preserving-update | [true]       |
        // | [3, 4]    | pk-changing-update   | [false, true]|
        // | [5]       | delete               | [false]      |
        // +-----------+----------------------+--------------+
        for sync_col in sync_schema.columns().iter() {
            if sync_col.role() == ColumnRole::OldPk {
                let old_pk = col(sync_col.field().name());
                let new_pk = col(sync_schema
                    .column(sync_col.name(), ColumnRole::NewPk)
                    .unwrap()
                    .field()
                    .name());

                let name =
                    SyncColumn::canonical_field_name(ColumnRole::NewPk, sync_col.name());
                projection.push(
                    when(insert.clone(), make_array(vec![new_pk.clone()]))
                        .when(
                            pk_preserving_update.clone(),
                            make_array(vec![new_pk.clone()]),
                        )
                        .when(
                            pk_changing_update.clone(),
                            make_array(vec![old_pk.clone(), new_pk.clone()]),
                        )
                        .otherwise(make_array(vec![old_pk.clone()]))? // delete
                        .alias_qualified(Some(UPPER_REL), &name),
                );
                unnest_cols.push(Column::new(Some(UPPER_REL), name.clone()));
                column_descriptors.push(ColumnDescriptor {
                    role: ColumnRole::NewPk as _,
                    name: sync_col.name().to_string(),
                });
            } else if matches!(sync_col.role(), ColumnRole::Changed | ColumnRole::Value) {
                let name =
                    SyncColumn::canonical_field_name(sync_col.role(), sync_col.name());
                projection.push(
                    col(sync_col.field().name())
                        .alias_qualified(Some(UPPER_REL), name.clone()),
                );
                column_descriptors.push(sync_col.column_descriptor());
            }
        }
        projection.push(
            when(insert, make_array(vec![lit(true)]))
                .when(pk_preserving_update, make_array(vec![lit(true)]))
                .when(pk_changing_update, make_array(vec![lit(false), lit(true)]))
                .otherwise(make_array(vec![lit(false)]))? // delete
                .alias(UPSERT_COL),
        );
        unnest_cols.push(Column::from(UPSERT_COL));

        // Construct the normalized dataframe unnesting the above pk and upsert columns
        let (session_state, plan) = sync_df.into_parts();
        let normalized_plan =
            LogicalPlanBuilder::from(self.project_expressions(plan, projection)?)
                .unnest_columns_with_options(unnest_cols.clone(), Default::default())?
                // DataFusion currently swallows the unnested column qualifiers so we need to alias
                // it explicitly
                .alias(UPPER_REL)?
                .build()?;

        // Remove the upsert column when constructing the sync schema
        let mut schema = SchemaBuilder::from(normalized_plan.schema().inner().as_ref());
        schema.remove(normalized_plan.schema().fields().len() - 1);
        // Skip PK validation, as we now have only one PK column in the schema
        let sync_schema =
            SyncSchema::try_new(column_descriptors, Arc::new(schema.finish()), false)?;

        Ok((sync_schema, DataFrame::new(session_state, normalized_plan)))
    }

    // More efficient projection on top of an existing logical plan.
    //
    // We can't use the `DataFrame::select`/`LogicalPlanBuilder::project` projection API since it
    // leads to sub-optimal performance due to a bunch of plan/expression walking/normalization that
    // we don't really need.
    //
    // This becomes increasingly slower as more plan nodes are added, so do it at a lower level
    // instead by deconstruct the present one and adding a vanilla `Projection` on top of it.
    fn project_expressions(
        &self,
        plan: LogicalPlan,
        projection: Vec<Expr>,
    ) -> SyncResult<LogicalPlan> {
        Ok(Projection::try_new(projection, Arc::new(plan))
            .map(LogicalPlan::Projection)?)
    }

    fn add_sync_flag(&self, schema: &DFSchemaRef, sync: &str) -> Vec<Expr> {
        schema
            .columns()
            .into_iter()
            .map(Expr::Column)
            .chain(once(lit(true).alias(sync)))
            .collect()
    }

    async fn apply_syncs(
        &self,
        full_schema: SchemaRef,
        base_df: DataFrame,
        sync_df: DataFrame,
        sync_schema: &SyncSchema,
    ) -> SyncResult<DataFrame> {
        // These differ since the physical column names are reflected in the ColumnDescriptor,
        // while logical column names are found in the arrow fields
        let (base_pk_cols, sync_pk_cols): (Vec<String>, Vec<String>) = sync_schema
            .map_columns(ColumnRole::NewPk, |c| {
                (c.name().to_string(), c.field().name().clone())
            })
            .into_iter()
            .unzip();

        let input_df = base_df
            .with_column(LOWER_REL, lit(true))?
            .join(
                sync_df,
                JoinType::Full,
                &base_pk_cols
                    .iter()
                    .map(|pk| pk.as_str())
                    .collect::<Vec<_>>(),
                &sync_pk_cols
                    .iter()
                    .map(|pk| pk.as_str())
                    .collect::<Vec<_>>(),
                None,
            )?
            .filter(col(UPSERT_COL).is_not_false())?; // Remove deleted rows

        // Normalize the schema, by ordering columns according to the full table schema and
        // projecting the sync data accordingly.
        let projection = full_schema
            .flattened_fields()
            .iter()
            .map(|f| {
                let name = f.name();

                let expr = if let Some(sync_col) = sync_schema
                    .column(name, ColumnRole::Value)
                    .or(sync_schema.column(name, ColumnRole::NewPk))
                {
                    // The column is present in the sync schema...
                    when(
                        col(UPSERT_COL).is_null(),
                        // ...but the row doesn't exist in the sync, so inherit the old value
                        col(Column::new(Some(LOWER_REL), name)),
                    )
                    .otherwise(
                        if let Some(changed_sync_col) =
                            sync_schema.column(name, ColumnRole::Changed)
                        {
                            // ... and there is a `Changed` flag denoting whether the column has changed.
                            when(
                                col(Column::new(
                                    Some(UPPER_REL),
                                    changed_sync_col.field().name(),
                                ))
                                .is_true(),
                                // If it's true take the new value
                                col(Column::new(
                                    Some(UPPER_REL),
                                    sync_col.field().name(),
                                )),
                            )
                            .otherwise(
                                // If it's false take the old value
                                col(Column::new(Some(LOWER_REL), name)),
                            )?
                        } else {
                            // ... and the sync has a new corresponding value without a `Changed` flag
                            col(Column::new(Some(UPPER_REL), sync_col.field().name()))
                        },
                    )?
                } else {
                    when(
                        col(LOWER_REL).is_null(),
                        // Column is not present in the sync schema, and the old row doesn't exist
                        // either, project a NULL
                        lit(ScalarValue::Null.cast_to(f.data_type())?),
                    )
                    .otherwise(
                        // Column is not present in the sync schema, but the old row does exist
                        // so project its value
                        col(Column::new(Some(LOWER_REL), name)),
                    )?
                };

                Ok(expr.alias(name))
            })
            .collect::<datafusion_common::Result<_>>()?;

        let (session_state, plan) = input_df.into_parts();
        let input_plan = self.project_expressions(plan, projection)?;

        Ok(DataFrame::new(session_state, input_plan))
    }

    // Get a list of files that need to be scanned, re-written and removed based on the PK values in
    // the sync items.
    fn prune_partitions(
        &self,
        syncs: &[DataSyncItem],
        full_schema: SchemaRef,
        table: &DeltaTable,
    ) -> SyncResult<Vec<Add>> {
        let snapshot = table.snapshot()?;
        let files = snapshot.file_actions()?;

        // First perform coarse-grained pruning, by only looking at global min-max in the syncs.
        // Generate a qualifier expression for old PKs; we definitely need to overwrite those in case
        // of PK-changing UPDATEs or DELETEs. Note that this can be `None` if it's an all-INSERT
        // syncs vec.
        let old_pk_qualifier = construct_qualifier(syncs, ColumnRole::OldPk)?;
        // Next construct the qualifier for new PKs; these are only needed for idempotence.
        let new_pk_qualifier = construct_qualifier(syncs, ColumnRole::NewPk)?;
        let qualifier = match (old_pk_qualifier, new_pk_qualifier) {
            (Some(old_pk_q), Some(new_pk_q)) => old_pk_q.or(new_pk_q),
            (Some(old_pk_q), None) => old_pk_q,
            (None, Some(new_pk_q)) => new_pk_q,
            _ => panic!("There can be no situation without either old or new PKs"),
        };

        let prune_expr = create_physical_expr(
            &qualifier,
            &full_schema.clone().to_dfschema()?,
            &ExecutionProps::new(),
        )?;
        let pruning_predicate =
            PruningPredicate::try_new(prune_expr, full_schema.clone())?;
        trace!(
            "Constructed pruning predicate: {:?}, schema {}",
            pruning_predicate,
            full_schema
        );

        let mut prune_map = pruning_predicate.prune(snapshot)?;

        let partition_count = prune_map.iter().filter(|p| **p).count();
        let total_rows = files
            .iter()
            .zip(&prune_map)
            .filter_map(|(add, keep)| {
                if *keep {
                    if let Ok(Some(stats)) = add.get_json_stats() {
                        Some(stats.num_records)
                    } else {
                        warn!("Missing log stats for Add {add:?}");
                        None
                    }
                } else {
                    None
                }
            })
            .sum::<i64>();
        debug!("Coarse-grained pruning found {partition_count} partitions with {total_rows} rows in total to re-write");

        // TODO: think of a better heuristic to trigger granular pruning
        // Try granular pruning if total row count is higher than 3M
        if total_rows > FINE_GRAINED_PRUNING_ROW_CRITERIA {
            let prune_start = Instant::now();
            let new_prune_map = get_prune_map(syncs, snapshot)?;
            let new_partition_count = new_prune_map.iter().filter(|p| **p).count();
            info!(
                "Fine-grained pruning scoped out {} partitions in {} ms",
                partition_count - new_partition_count,
                prune_start.elapsed().as_millis()
            );
            prune_map = new_prune_map;
        }

        Ok(files
            .iter()
            .zip(prune_map)
            .filter_map(|(add, keep)| if keep { Some(add.clone()) } else { None })
            .collect::<Vec<Add>>())
    }
}

#[cfg(test)]
mod tests {
    use crate::context::test_utils::in_memory_context;
    use crate::sync::schema::{arrow_to_sync_schema, SyncSchema};
    use crate::sync::writer::DataSyncItem;
    use crate::sync::{
        planner::{SeafowlSyncPlanner, LOWER_REL, UPPER_REL},
        SyncResult,
    };
    use arrow::array::{BooleanArray, Int32Array, StringArray};
    use arrow::record_batch::RecordBatch;
    use arrow_schema::{DataType, Field, Schema};
    use clade::sync::{ColumnDescriptor, ColumnRole};
    use datafusion::dataframe::DataFrame;
    use datafusion::datasource::{provider_as_source, MemTable};
    use datafusion::physical_plan::get_plan_string;
    use datafusion_common::assert_batches_eq;
    use datafusion_expr::{col, LogicalPlanBuilder};
    use rstest::rstest;
    use std::sync::Arc;

    #[rstest]
    #[tokio::test]
    async fn test_sync_merging(#[values(true, false)] union: bool) -> SyncResult<()> {
        let ctx = Arc::new(in_memory_context().await);
        let planner = SeafowlSyncPlanner::new(ctx.clone());

        // Construct lower and upper schema that cover all possible cases
        let lower_schema = Arc::new(Schema::new(vec![
            Field::new("old_pk_c1", DataType::Int32, true),
            Field::new("new_pk_c1", DataType::Int32, true),
            // value only in lower
            Field::new("value_c3", DataType::Utf8, true),
            // value in both
            Field::new("value_c5", DataType::Utf8, true),
            // lower has value and changed
            Field::new("changed_c6", DataType::Boolean, true),
            Field::new("value_c6", DataType::Utf8, true),
            // lower has value but not changed
            Field::new("value_c7", DataType::Utf8, true),
            // value and changed only in lower
            Field::new("changed_c8", DataType::Boolean, true),
            Field::new("value_c8", DataType::Utf8, true),
            // value and changed in both
            Field::new("changed_c10", DataType::Boolean, true),
            Field::new("value_c10", DataType::Utf8, true),
        ]));

        let upper_schema = Arc::new(Schema::new(vec![
            Field::new("old_pk_c1", DataType::Int32, true),
            Field::new("new_pk_c1", DataType::Int32, true),
            // value only in upper
            Field::new("value_c4", DataType::Utf8, true),
            // value in both
            Field::new("value_c5", DataType::Utf8, true),
            // upper has value but not changed
            Field::new("value_c6", DataType::Utf8, true),
            // upper has value and changed
            Field::new("changed_c7", DataType::Boolean, true),
            Field::new("value_c7", DataType::Utf8, true),
            // value and changed only in upper
            Field::new("changed_c9", DataType::Boolean, true),
            Field::new("value_c9", DataType::Utf8, true),
            // value and changed in both
            Field::new("changed_c10", DataType::Boolean, true),
            Field::new("value_c10", DataType::Utf8, true),
        ]));

        // UPDATE, INSERT
        let lower_rows = 7;
        let lower_batch = RecordBatch::try_new(
            lower_schema.clone(),
            vec![
                // Insert 3 rows, Update 3 rows and delete 1 row
                Arc::new(Int32Array::from(vec![
                    None,
                    None,
                    None,
                    Some(4),
                    Some(6),
                    Some(8),
                    Some(10),
                ])),
                Arc::new(Int32Array::from(vec![
                    Some(1),
                    Some(2),
                    Some(3),
                    Some(5),
                    Some(7),
                    Some(9),
                    None,
                ])),
                Arc::new(StringArray::from(vec!["lower_c3"; lower_rows])),
                Arc::new(StringArray::from(vec!["lower_c5"; lower_rows])),
                Arc::new(BooleanArray::from(vec![true; lower_rows])),
                Arc::new(StringArray::from(vec!["lower_c6"; lower_rows])),
                Arc::new(StringArray::from(vec!["lower_c7"; lower_rows])),
                Arc::new(BooleanArray::from(vec![true; lower_rows])),
                Arc::new(StringArray::from(vec!["lower_c8"; lower_rows])),
                Arc::new(BooleanArray::from(vec![true; lower_rows])),
                Arc::new(StringArray::from(vec!["lower_c10"; lower_rows])),
            ],
        )?;

        let upper_batch = if union {
            // Create an append-only upper batch
            let upper_rows = 3;
            RecordBatch::try_new(
                upper_schema.clone(),
                vec![
                    // Insert 3 rows,
                    Arc::new(Int32Array::from(vec![None, None, None])),
                    Arc::new(Int32Array::from(vec![Some(11), Some(12), Some(13)])),
                    Arc::new(StringArray::from(vec!["upper_c4"; upper_rows])),
                    Arc::new(StringArray::from(vec!["upper_c5"; upper_rows])),
                    Arc::new(StringArray::from(vec!["upper_c6"; upper_rows])),
                    Arc::new(BooleanArray::from(vec![false; upper_rows])),
                    Arc::new(StringArray::from(vec!["upper_c7"; upper_rows])),
                    Arc::new(BooleanArray::from(vec![false; upper_rows])),
                    Arc::new(StringArray::from(vec!["upper_c9"; upper_rows])),
                    Arc::new(BooleanArray::from(vec![false; upper_rows])),
                    Arc::new(StringArray::from(vec!["upper_c10"; upper_rows])),
                ],
            )?
        } else {
            let upper_rows = 7;
            RecordBatch::try_new(
                upper_schema.clone(),
                vec![
                    // Insert 1 row,
                    // Update 1 row inserted in lower, 1 row updated in lower and 1 other row,
                    // Delete 1 row inserted in lower, 1 row updated in lower and 1 other row
                    Arc::new(Int32Array::from(vec![
                        None,
                        Some(2),
                        Some(7),
                        Some(14),
                        Some(3),
                        Some(9),
                        Some(16),
                    ])),
                    Arc::new(Int32Array::from(vec![
                        Some(11),
                        Some(12),
                        Some(13),
                        Some(15),
                        None,
                        None,
                        None,
                    ])),
                    Arc::new(StringArray::from(vec!["upper_c4"; upper_rows])),
                    Arc::new(StringArray::from(vec!["upper_c5"; upper_rows])),
                    Arc::new(StringArray::from(vec!["upper_c6"; upper_rows])),
                    Arc::new(BooleanArray::from(vec![false; upper_rows])),
                    Arc::new(StringArray::from(vec!["upper_c7"; upper_rows])),
                    Arc::new(BooleanArray::from(vec![false; upper_rows])),
                    Arc::new(StringArray::from(vec!["upper_c9"; upper_rows])),
                    Arc::new(BooleanArray::from(vec![false; upper_rows])),
                    Arc::new(StringArray::from(vec!["upper_c10"; upper_rows])),
                ],
            )?
        };

        let provider = MemTable::try_new(lower_schema.clone(), vec![vec![lower_batch]])?;
        let lower_plan = LogicalPlanBuilder::scan(
            LOWER_REL,
            provider_as_source(Arc::new(provider)),
            None,
        )?
        .build()?;

        let provider = MemTable::try_new(upper_schema.clone(), vec![vec![upper_batch]])?;
        let upper_plan = LogicalPlanBuilder::scan(
            UPPER_REL,
            provider_as_source(Arc::new(provider)),
            None,
        )?;

        let lower_schema = crate::sync::schema::arrow_to_sync_schema(lower_schema)?;
        let upper_schema = crate::sync::schema::arrow_to_sync_schema(upper_schema)?;
        let (_, merged_plan) = if union {
            planner.union_syncs(&lower_schema, lower_plan, &upper_schema, upper_plan)?
        } else {
            planner.join_syncs(&lower_schema, lower_plan, &upper_schema, upper_plan)?
        };

        let merged_df = DataFrame::new(ctx.inner.state(), merged_plan);

        // Pre-sort the merged batch to keep the order stable
        let results = merged_df
            .sort(vec![
                col("old_pk_c1").sort(true, true),
                col("new_pk_c1").sort(true, true),
            ])?
            .collect()
            .await?;

        let expected = if union {
            [
                "+-----------+-----------+------------+----------+----------+------------+----------+----------+------------+----------+-------------+-----------+------------+----------+------------+------------+----------+",
                "| old_pk_c1 | new_pk_c1 | changed_c3 | value_c3 | value_c5 | changed_c6 | value_c6 | value_c7 | changed_c8 | value_c8 | changed_c10 | value_c10 | changed_c4 | value_c4 | changed_c7 | changed_c9 | value_c9 |",
                "+-----------+-----------+------------+----------+----------+------------+----------+----------+------------+----------+-------------+-----------+------------+----------+------------+------------+----------+",
                "|           | 1         | true       | lower_c3 | lower_c5 | true       | lower_c6 | lower_c7 | true       | lower_c8 | true        | lower_c10 | false      |          | true       | false      |          |",
                "|           | 2         | true       | lower_c3 | lower_c5 | true       | lower_c6 | lower_c7 | true       | lower_c8 | true        | lower_c10 | false      |          | true       | false      |          |",
                "|           | 3         | true       | lower_c3 | lower_c5 | true       | lower_c6 | lower_c7 | true       | lower_c8 | true        | lower_c10 | false      |          | true       | false      |          |",
                "|           | 11        | false      |          | upper_c5 | true       | upper_c6 | upper_c7 | false      |          | false       | upper_c10 | true       | upper_c4 | false      | false      | upper_c9 |",
                "|           | 12        | false      |          | upper_c5 | true       | upper_c6 | upper_c7 | false      |          | false       | upper_c10 | true       | upper_c4 | false      | false      | upper_c9 |",
                "|           | 13        | false      |          | upper_c5 | true       | upper_c6 | upper_c7 | false      |          | false       | upper_c10 | true       | upper_c4 | false      | false      | upper_c9 |",
                "| 4         | 5         | true       | lower_c3 | lower_c5 | true       | lower_c6 | lower_c7 | true       | lower_c8 | true        | lower_c10 | false      |          | true       | false      |          |",
                "| 6         | 7         | true       | lower_c3 | lower_c5 | true       | lower_c6 | lower_c7 | true       | lower_c8 | true        | lower_c10 | false      |          | true       | false      |          |",
                "| 8         | 9         | true       | lower_c3 | lower_c5 | true       | lower_c6 | lower_c7 | true       | lower_c8 | true        | lower_c10 | false      |          | true       | false      |          |",
                "| 10        |           | true       | lower_c3 | lower_c5 | true       | lower_c6 | lower_c7 | true       | lower_c8 | true        | lower_c10 | false      |          | true       | false      |          |",
                "+-----------+-----------+------------+----------+----------+------------+----------+----------+------------+----------+-------------+-----------+------------+----------+------------+------------+----------+",
            ]
        } else {
            [
                "+-----------+-----------+------------+----------+----------+------------+----------+----------+------------+----------+-------------+-----------+------------+----------+------------+------------+----------+",
                "| old_pk_c1 | new_pk_c1 | changed_c3 | value_c3 | value_c5 | changed_c6 | value_c6 | value_c7 | changed_c8 | value_c8 | changed_c10 | value_c10 | changed_c4 | value_c4 | changed_c7 | changed_c9 | value_c9 |",
                "+-----------+-----------+------------+----------+----------+------------+----------+----------+------------+----------+-------------+-----------+------------+----------+------------+------------+----------+",
                "|           |           | true       | lower_c3 | upper_c5 | true       | upper_c6 | upper_c7 | false      | lower_c8 | false       | upper_c10 | true       | upper_c4 | false      | false      | upper_c9 |",
                "|           | 1         | true       | lower_c3 | lower_c5 | true       | lower_c6 | lower_c7 | true       | lower_c8 | true        | lower_c10 | false      |          | true       | false      |          |",
                "|           | 11        | false      |          | upper_c5 | true       | upper_c6 | upper_c7 | false      |          | false       | upper_c10 | true       | upper_c4 | false      | false      | upper_c9 |",
                "|           | 12        | true       | lower_c3 | upper_c5 | true       | upper_c6 | upper_c7 | false      | lower_c8 | false       | upper_c10 | true       | upper_c4 | false      | false      | upper_c9 |",
                "| 4         | 5         | true       | lower_c3 | lower_c5 | true       | lower_c6 | lower_c7 | true       | lower_c8 | true        | lower_c10 | false      |          | true       | false      |          |",
                "| 6         | 13        | true       | lower_c3 | upper_c5 | true       | upper_c6 | upper_c7 | false      | lower_c8 | false       | upper_c10 | true       | upper_c4 | false      | false      | upper_c9 |",
                "| 8         |           | true       | lower_c3 | upper_c5 | true       | upper_c6 | upper_c7 | false      | lower_c8 | false       | upper_c10 | true       | upper_c4 | false      | false      | upper_c9 |",
                "| 10        |           | true       | lower_c3 | lower_c5 | true       | lower_c6 | lower_c7 | true       | lower_c8 | true        | lower_c10 | false      |          | true       | false      |          |",
                "| 14        | 15        | false      |          | upper_c5 | true       | upper_c6 | upper_c7 | false      |          | false       | upper_c10 | true       | upper_c4 | false      | false      | upper_c9 |",
                "| 16        |           | false      |          | upper_c5 | true       | upper_c6 | upper_c7 | false      |          | false       | upper_c10 | true       | upper_c4 | false      | false      | upper_c9 |",
                "+-----------+-----------+------------+----------+----------+------------+----------+----------+------------+----------+-------------+-----------+------------+----------+------------+------------+----------+",
            ]
        };
        assert_batches_eq!(expected, &results);

        Ok(())
    }

    #[tokio::test]
    async fn test_sync_normalizing() -> SyncResult<()> {
        let ctx = Arc::new(in_memory_context().await);
        let planner = SeafowlSyncPlanner::new(ctx.clone());

        let schema = Arc::new(Schema::new(vec![
            Field::new("old_pk_c1", DataType::Int32, true),
            Field::new("new_pk_c1", DataType::Int32, true),
            Field::new("value_c2", DataType::Utf8, true),
        ]));
        let sync_schema = arrow_to_sync_schema(schema.clone())?;

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![None, Some(2), Some(3), Some(5)])),
                Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(4), None])),
                Arc::new(StringArray::from(vec![
                    "insert",
                    "pk-preserving-update",
                    "pk-changing-update",
                    "delete",
                ])),
            ],
        )?;

        let sync_df = ctx.inner.read_batch(batch)?;
        let (_, normalized_sync) = planner.normalize_syncs(&sync_schema, sync_df)?;

        let results = normalized_sync
            .sort(vec![col("new_pk_c1").sort(true, true)])?
            .collect()
            .await?;

        let expected = [
            "+-----------+----------------------+--------------+",
            "| new_pk_c1 | value_c2             | __upsert_col |",
            "+-----------+----------------------+--------------+",
            "| 1         | insert               | true         |",
            "| 2         | pk-preserving-update | true         |",
            "| 3         | pk-changing-update   | false        |",
            "| 4         | pk-changing-update   | true         |",
            "| 5         | delete               | false        |",
            "+-----------+----------------------+--------------+",
        ];
        assert_batches_eq!(expected, &results);

        Ok(())
    }

    #[tokio::test]
    async fn test_one_sync_noncanonical_field() -> SyncResult<()> {
        let ctx = Arc::new(in_memory_context().await);
        let planner = SeafowlSyncPlanner::new(ctx.clone());

        // Use non-canonical field names
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Utf8, true),
        ]));
        let cds = vec![
            ColumnDescriptor {
                role: ColumnRole::OldPk as _,
                name: "c1".to_string(),
            },
            ColumnDescriptor {
                role: ColumnRole::NewPk as _,
                name: "c1".to_string(),
            },
            ColumnDescriptor {
                role: ColumnRole::Value as _,
                name: "c2".to_string(),
            },
        ];

        let sync_schema = SyncSchema::try_new(cds, schema.clone(), true)?;

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![None])),
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["a"])),
            ],
        )?;

        let sync_item = DataSyncItem {
            is_squashed: true,
            tx_ids: Default::default(),
            sync_schema,
            data: vec![batch],
        };

        ctx.plan_query("CREATE TABLE test_table(c1 INT, c2 TEXT)")
            .await?;
        let table = ctx.try_get_delta_table("test_table").await?;

        let (plan, _) = planner.plan_delta_syncs(&[sync_item], &table).await?;

        let mut actual_plan = get_plan_string(&plan);
        actual_plan.iter_mut().for_each(|node| {
            if node.contains("RepartitionExec") {
                // Find the position of the first '(' and truncate to avoid test dependency on hardware
                if let Some(pos) = node.find('(') {
                    node.truncate(pos);
                }
            }
        });
        let expected_plan = vec![
            "ProjectionExec: expr=[CASE WHEN __upsert_col@4 IS NULL THEN c1@0 ELSE new_pk_c1@2 END as c1, CASE WHEN __upsert_col@4 IS NULL THEN c2@1 ELSE value_c2@3 END as c2]",
            "  CoalesceBatchesExec: target_batch_size=8192",
            "    FilterExec: __upsert_col@5 IS DISTINCT FROM false, projection=[c1@0, c2@1, new_pk_c1@3, value_c2@4, __upsert_col@5]",
            "      CoalesceBatchesExec: target_batch_size=8192",
            "        HashJoinExec: mode=Partitioned, join_type=Full, on=[(c1@0, new_pk_c1@0)]",
            "          CoalesceBatchesExec: target_batch_size=8192",
            "            RepartitionExec: partitioning=Hash",
            "              ProjectionExec: expr=[c1@0 as c1, c2@1 as c2, true as __lower_rel]",
            "                DeltaScan",
            "                  ParquetExec: file_groups={1 group: [[]]}, projection=[c1, c2]",
            "          CoalesceBatchesExec: target_batch_size=8192",
            "            RepartitionExec: partitioning=Hash",
            "              UnnestExec",
            "                ProjectionExec: expr=[CASE WHEN a@0 IS NULL AND b@1 IS NOT NULL THEN make_array(b@1) WHEN a@0 IS NOT NULL AND b@1 IS NOT NULL AND a@0 = b@1 THEN make_array(b@1) WHEN a@0 IS NOT NULL AND b@1 IS NOT NULL AND a@0 != b@1 THEN make_array(a@0, b@1) ELSE make_array(a@0) END as new_pk_c1, c@2 as value_c2, CASE WHEN a@0 IS NULL AND b@1 IS NOT NULL THEN make_array(true) WHEN a@0 IS NOT NULL AND b@1 IS NOT NULL AND a@0 = b@1 THEN make_array(true) WHEN a@0 IS NOT NULL AND b@1 IS NOT NULL AND a@0 != b@1 THEN make_array(false, true) ELSE make_array(false) END as __upsert_col]",
            "                  RepartitionExec: partitioning=RoundRobinBatch",
            "                    CoalesceBatchesExec: target_batch_size=8192",
            "                      FilterExec: NOT a@0 IS NULL AND b@1 IS NULL",
            "                        MemoryExec: partitions=1, partition_sizes=[1]",
        ];

        assert_eq!(
            expected_plan, actual_plan,
            "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
            expected_plan, actual_plan
        );

        Ok(())
    }
}
