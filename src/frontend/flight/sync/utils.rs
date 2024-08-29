use crate::frontend::flight::sync::schema::{SyncColumn, SyncSchema};
use crate::frontend::flight::sync::writer::{DataSyncItem, LOWER_SYNC, UPPER_SYNC};
use crate::frontend::flight::sync::SyncResult;
use arrow::array::{new_null_array, Array, ArrayRef, RecordBatch, Scalar, UInt64Array};
use arrow::compute::kernels::cmp::{gt_eq, lt_eq};
use arrow::compute::{and_kleene, bool_or, concat_batches, filter, is_not_null, take};
use arrow_row::{Row, RowConverter, SortField};
use clade::sync::{ColumnDescriptor, ColumnRole};
use datafusion::functions_aggregate::min_max::{MaxAccumulator, MinAccumulator};
use datafusion::physical_optimizer::pruning::PruningStatistics;
use datafusion_common::{Column, DataFusionError, Result, ScalarValue};
use datafusion_expr::{col, lit, when, Accumulator, Expr};
use indexmap::IndexSet;
use std::collections::{HashMap, HashSet, VecDeque};
use tracing::log::warn;

// Compact a set of record batches into a single one, squashing any chain of changes to a given row
// into a single row in the output batch.
// This means that if a row is changed multiple times, only the last change will be reflected in the
// output batch (meaning the last NewPk and Value role columns and the last Value column where the
// accompanying Changed field was `true`).
pub(super) fn squash_batches(
    sync_schema: &SyncSchema,
    data: Vec<RecordBatch>,
) -> Result<RecordBatch> {
    // Concatenate all the record batches into a single one
    let schema = data.first().unwrap().schema();
    let batch = concat_batches(&schema, &data)?;

    // Get columns, sort fields and null arrays for a particular role
    let columns = |role: ColumnRole| -> (Vec<ArrayRef>, (Vec<SortField>, Vec<ArrayRef>)) {
        sync_schema
            .columns()
            .iter()
            .zip(batch.columns().iter())
            .filter_map(|(col, array)| {
                if col.role() == role {
                    let data_type = array.data_type();
                    Some((
                        array.clone(),
                        (
                            SortField::new(data_type.clone()),
                            new_null_array(data_type, 1),
                        ),
                    ))
                } else {
                    None
                }
            })
            .unzip()
    };

    let (old_pk_cols, (sort_fields, nulls)) = columns(ColumnRole::OldPk);
    let (new_pk_cols, _) = columns(ColumnRole::NewPk);

    // NB: we must use the same row converter in order to compare fields, or else the comparison
    // might not make sense:
    // https://github.com/apache/arrow-rs/blob/956fe76731b80f62559e6290cfe6fb360794c3ce/arrow-row/src/lib.rs#L47-L48
    //
    // In our case this also necessitates that we use the same sort fields for both old and new PKs
    // and so need to validate this during `SyncSchema` construction.
    //
    // If we ever need to support PK evolution this could be achieved by projecting any missing
    // columns as nulls before constructing the rows.
    let converter = RowConverter::new(sort_fields)?;
    let old_pks = converter.convert_columns(&old_pk_cols)?;
    let new_pks = converter.convert_columns(&new_pk_cols)?;
    let null_rows = converter.convert_columns(&nulls)?;
    let nulls = null_rows.row(0);

    let (changed_binding, _) = columns(ColumnRole::Changed);
    let changed = changed_binding
        .iter()
        .map(|array| {
            array
                .as_any()
                .downcast_ref::<arrow::array::BooleanArray>()
                .expect("Changed column must be boolean")
        })
        .collect::<Vec<_>>();

    // Iterate through pairs of old and new PKs, keeping track of any change chains that appear,
    // denoting each chain with the same ID, and appending or updating the relevant row id for
    // columns. There are 3 categories of columns for which we track row ids: old PKs, new PKs, and
    // changed columns.
    let mut chain_count = 0;
    let mut column_rows: Vec<(u64, u64, Vec<u64>)> = vec![];
    let mut temp_rows = HashSet::new();
    let mut pk_chains: HashMap<Row, usize> = HashMap::new();

    for (row_id, (old_pk, new_pk)) in old_pks.iter().zip(new_pks.iter()).enumerate() {
        match pk_chains.remove(&old_pk) {
            Some(chain_id) => {
                // An entry exists for this pk chain; do not update the old pk row id, since that
                // hasn't changed
                column_rows[chain_id].1 = row_id as u64;
                column_rows[chain_id]
                    .2
                    .iter_mut()
                    .zip(changed.iter())
                    .for_each(|(row, array)| {
                        if array.value(row_id) {
                            // We ran into a later changed value, use its row id
                            *row = row_id as u64;
                        }
                    });
                if new_pk != nulls {
                    // If the new PK is not null keep pointing to the same chain ID, since
                    // otherwise the chain ends by row deletion, and so we don't need to update
                    // its column_rows anymore
                    pk_chains.insert(new_pk, chain_id);
                } else if old_pks.row(column_rows[chain_id].0 as usize) == nulls {
                    // If both the first old PK is null, and the last new PK is null, we have a
                    // temporary row that doesn't need to be included in the output batch
                    temp_rows.insert(chain_id);
                }
            }
            None => {
                // No entry for the old PK, start a new chain.
                // We use the current row_id for the changed columns below regardless of whether it
                // is actually changed or not, since we'll pick up any actual changes later on.
                column_rows.push((
                    row_id as u64,
                    row_id as u64,
                    changed.iter().map(|_| row_id as u64).collect(),
                ));
                if new_pk != nulls {
                    pk_chains.insert(new_pk, chain_count);
                }
                chain_count += 1;
            }
        };
    }

    // Transpose the changed rows so that we obtain the indices for each column role
    let rows = column_rows.len();
    let (mut old_pks, mut new_pks, mut changed_rows) = (
        Vec::with_capacity(rows),
        Vec::with_capacity(rows),
        VecDeque::with_capacity(rows),
    );
    for (id, (old_pk, new_pk, changed_row)) in column_rows.into_iter().enumerate() {
        if temp_rows.contains(&id) {
            // Exclude temporary rows from the output batch
            continue;
        }

        old_pks.push(old_pk);
        new_pks.push(new_pk);
        for (idx, changed_row) in changed_row.iter().enumerate() {
            match changed_rows.get_mut(idx) {
                None => changed_rows.push_back(vec![*changed_row]),
                Some(v) => v.push(*changed_row),
            };
        }
    }

    // Construct the actual index arrays for each column
    let old_pk_indices = UInt64Array::from(old_pks);
    let new_pk_indices = UInt64Array::from(new_pks);
    let changed_indices = changed_rows
        .iter()
        .map(|rows| UInt64Array::from(rows.clone()))
        .collect::<Vec<_>>();
    let mut changed_pos = 0;
    let mut indices: HashMap<usize, &UInt64Array> = HashMap::new();
    sync_schema
        .columns()
        .iter()
        .enumerate()
        .for_each(|(col_id, col)| {
            match col.role() {
                ColumnRole::OldPk => {
                    indices.insert(col_id, &old_pk_indices);
                }
                ColumnRole::NewPk => {
                    indices.insert(col_id, &new_pk_indices);
                }
                ColumnRole::Changed => {
                    // Insert the indices for both this column...
                    indices.insert(col_id, &changed_indices[0]);

                    // ... as well as for the actual changed Value column
                    let changed_col_id = schema
                        .index_of(
                            sync_schema
                                .column(col.name(), ColumnRole::Value)
                                .unwrap()
                                .field()
                                .name(),
                        )
                        .expect("Field exists");
                    indices.insert(changed_col_id, &changed_indices[0]);
                    changed_pos += 1;
                }
                ColumnRole::Value => {
                    indices.entry(col_id).or_insert(&new_pk_indices);
                }
            }
        });

    // Finally take the designated rows from each old PK, new PK, value and changed columns from the
    // batch
    let batch = RecordBatch::try_new(
        schema.clone(),
        (0..schema.fields().len())
            .map(|col_id| {
                Ok(take(
                    batch.columns()[col_id].as_ref(),
                    indices.get(&col_id).expect("Indices present"),
                    None,
                )?)
            })
            .collect::<Result<_>>()?,
    )?;

    Ok(batch)
}

// Generate a qualifier expression that, when applied to the table, will only return
// rows whose primary keys are affected by the changes in `entry`. This is so that
// we can only read the partitions from Delta Lake that we need to rewrite.
pub(super) fn construct_qualifier(
    syncs: &[DataSyncItem],
    role: ColumnRole,
) -> Result<Option<Expr>> {
    // Initialize the min/max accumulators for the primary key columns needed to prune the table
    // files.
    // The assumption here is that the primary key columns are the same across all syncs.
    let mut min_max_values: Vec<(String, (MinAccumulator, MaxAccumulator))> = syncs
        .first()
        .expect("At least one sync item in the queue")
        .sync_schema
        .columns()
        .iter()
        .filter(|col| col.role() == role)
        .map(|col| {
            Ok((
                col.name().clone(),
                (
                    MinAccumulator::try_new(col.field().data_type())?,
                    MaxAccumulator::try_new(col.field().data_type())?,
                ),
            ))
        })
        .collect::<Result<_>>()?;

    // Collect all min/max stats for PK columns
    for sync in syncs {
        min_max_values
            .iter_mut()
            .try_for_each(|(pk_col, (min_value, max_value))| {
                let field = sync.sync_schema.column(pk_col, role).unwrap().field();

                if let Some(pk_array) = sync.batch.column_by_name(field.name()) {
                    min_value.update_batch(&[pk_array.clone()])?;
                    max_value.update_batch(&[pk_array.clone()])?;
                }
                Ok::<(), DataFusionError>(())
            })?
    }

    // Combine the statistics into a single qualifier expression
    Ok(min_max_values
        .iter_mut()
        .filter_map(|(pk_col, (min_value, max_value))| {
            let min_value = min_value.evaluate().ok()?;
            let max_value = max_value.evaluate().ok()?;
            if !min_value.is_null() && !max_value.is_null() {
                Some(Ok(
                    col(pk_col.as_str()).between(lit(min_value), lit(max_value))
                ))
            } else {
                // There shouldn't be a situation where a max is NULL but min isn't and vice-versa.
                // Moreover, it should be the case that if one column has NULL min/max values, all
                // columns do (i.e. we're processing an all INSERT/DELETE changeset).
                assert!(min_value.is_null() && max_value.is_null());
                None
            }
        })
        .collect::<Result<Vec<Expr>>>()?
        .into_iter()
        .reduce(|e1: Expr, e2| e1.and(e2)))
}

// Go through each sync and each partition and check whether any single row out of non-NULL PKs can
// possibly be found in that partition, and if so add it to the map.
pub(super) fn get_prune_map(
    syncs: &[DataSyncItem],
    pruning_stats: &dyn PruningStatistics,
) -> SyncResult<Vec<bool>> {
    let partition_count = pruning_stats.num_containers();

    // Maps of column, partition -> min max scalar value
    let mut min_values: HashMap<(&str, usize), Scalar<ArrayRef>> = HashMap::new();
    let mut max_values: HashMap<(&str, usize), Scalar<ArrayRef>> = HashMap::new();

    // First gather the stats about the partitions and PK columns
    for col in syncs
        .first()
        .unwrap()
        .sync_schema
        .columns()
        .iter()
        .filter(|col| col.role() == ColumnRole::OldPk)
    {
        let maybe_min_vals = pruning_stats.min_values(&col.name().into());
        let maybe_max_vals = pruning_stats.max_values(&col.name().into());

        if maybe_min_vals.is_none() && maybe_max_vals.is_none() {
            // We have no stats for any partition for this PK column, so short-circuit pruning
            // by returning all files
            warn!(
                "Skipping partition pruning: no min/max stats found for column {}",
                col.name(),
            );
            return Ok(vec![true; partition_count]);
        }

        if let Some(min_vals) = maybe_min_vals {
            for file in 0..partition_count {
                min_values.insert(
                    (col.name().as_str(), file),
                    Scalar::new(min_vals.slice(file, 1)),
                );
            }
        }
        if let Some(max_vals) = maybe_max_vals {
            for file in 0..partition_count {
                max_values.insert(
                    (col.name().as_str(), file),
                    Scalar::new(max_vals.slice(file, 1)),
                );
            }
        }
    }

    // Start off scanning no files
    let mut prune_map = vec![false; partition_count];

    for role in [ColumnRole::OldPk, ColumnRole::NewPk] {
        for sync in syncs {
            for (ind, used) in prune_map.iter_mut().enumerate() {
                // Perform pruning only if we don't know whether the partition is needed yet
                if !*used {
                    let mut non_null_map = None;
                    let mut sync_prune_map = None;
                    for pk_col in sync
                        .sync_schema
                        .columns()
                        .iter()
                        .filter(|col| col.role() == role)
                    {
                        let array =
                            sync.batch.column_by_name(pk_col.field().name()).unwrap();

                        // Scope out any NULL values, which only denote no-PKs when inserting/deleting.
                        // We re-use the same non-null map since there can't be a scenario where
                        // some of the PKs are null and others aren't (i.e. NULL PKs are invalid).
                        let array = match non_null_map {
                            None => {
                                let non_nulls = is_not_null(array)?;
                                let array = filter(array, &non_nulls)?;
                                non_null_map = Some(non_nulls);
                                array
                            }
                            Some(ref non_nulls) => filter(array, non_nulls)?,
                        };

                        if array.is_empty() {
                            // Old PKs for INSERT/ new PKs for DELETE, nothing to prune here
                            continue;
                        }

                        let col_file = (pk_col.name().as_str(), ind);
                        let next_sync_prune_map = match (
                            min_values.get(&col_file),
                            max_values.get(&col_file),
                        ) {
                            (Some(min_value), Some(max_value)) => and_kleene(
                                &gt_eq(&array.as_ref(), min_value)?,
                                &lt_eq(&array.as_ref(), max_value)?,
                            )?,
                            (Some(min_value), None) => gt_eq(&array.as_ref(), min_value)?,
                            (None, Some(max_value)) => lt_eq(&array.as_ref(), max_value)?,
                            _ => unreachable!("Validation ensured against this case"),
                        };

                        match sync_prune_map {
                            None => sync_prune_map = Some(next_sync_prune_map),
                            Some(prev_sync_prune_map) => {
                                sync_prune_map = Some(and_kleene(
                                    &prev_sync_prune_map,
                                    &next_sync_prune_map,
                                )?)
                            }
                        }
                    }

                    if let Some(sync_prune_map) = sync_prune_map
                        && (bool_or(&sync_prune_map) == Some(true)
                            || sync_prune_map.null_count() > 0)
                    {
                        // We've managed to calculate the prune map for all columns and at least one
                        // PK is either definitely in the current partition file, or it's unknown
                        // whether it is in the current file, so in either case we must include
                        // this partition in the scan.
                        *used = true;
                    }
                }
            }
        }
    }

    Ok(prune_map)
}

// Build the merged projection and column descriptors from the sync schemas
pub(super) fn merge_schemas(
    lower_schema: &SyncSchema,
    upper_schema: &SyncSchema,
) -> SyncResult<(Vec<ColumnDescriptor>, Vec<Expr>)> {
    let mut projection = vec![];
    let mut col_desc = IndexSet::new();

    // TODO: implement sync role indexing in SyncSchema to make all the lookups below cheap
    // Project all sync columns from the lower schema
    for lower_sync_col in lower_schema.columns() {
        // Build the expression to project the combined column
        let expr = match lower_sync_col.role() {
            ColumnRole::OldPk => project_old_pk_column(lower_sync_col, upper_schema)?,
            ColumnRole::NewPk => project_new_pk_column(lower_sync_col, upper_schema)?,
            ColumnRole::Changed => {
                project_changed_column(lower_sync_col, upper_schema, SyncPosition::Lower)?
            }
            ColumnRole::Value => project_lower_value_column(
                lower_sync_col,
                lower_schema,
                upper_schema,
                &mut col_desc,
                &mut projection,
            )?,
        };

        col_desc.insert(lower_sync_col.column_descriptor());
        projection.push(expr);
    }

    // Now project any missing sync columns from the upper schema
    for upper_sync_col in upper_schema.columns() {
        let cd = ColumnDescriptor {
            role: upper_sync_col.role() as _,
            name: upper_sync_col.name().clone(),
        };

        if col_desc.contains(&cd) {
            // We've already projected this column from the lower schema
            continue;
        }

        // Build the expression that is going to be used when upper sync column value is missing
        let expr = match upper_sync_col.role() {
            ColumnRole::Changed => {
                project_changed_column(upper_sync_col, lower_schema, SyncPosition::Upper)?
            }
            ColumnRole::Value => project_upper_value_column(
                upper_sync_col,
                lower_schema,
                upper_schema,
                &mut col_desc,
                &mut projection,
            )?,
            _ => continue, // We already picked up PKs from the lower sync
        };

        col_desc.insert(upper_sync_col.column_descriptor());
        projection.push(expr);
    }

    let col_desc = col_desc.into_iter().collect::<Vec<_>>();

    // Finally use a qualified alias for the projection, which will act as a new lower sync in the
    // subsequent sync merge
    let projection = col_desc
        .iter()
        .zip(projection)
        .map(|(cd, expr)| {
            expr.alias_qualified(Some(LOWER_SYNC), SyncColumn::canonical_field_name(cd))
        })
        .collect();

    Ok((col_desc, projection))
}

enum SyncPosition {
    Lower,
    Upper,
}

fn project_old_pk_column(
    lower_old_pk_col: &SyncColumn,
    upper_schema: &SyncSchema,
) -> SyncResult<Expr> {
    let lower_old_pk_field = lower_old_pk_col.field().name();

    let upper_old_pk_col = upper_schema
        .column(lower_old_pk_col.name(), ColumnRole::OldPk)
        .expect("Old Pk columns must be the same across syncs");

    // For un-matched lower sync rows use lower sync old PKs, otherwise
    // for un-matched upper sync rows use upper sync old PKs, otherwise
    // for matched lower-upper sync rows use lower sync old PKs
    Ok(when(
        col(UPPER_SYNC).is_null(),
        col(Column::new(Some(LOWER_SYNC), lower_old_pk_field)),
    )
    .otherwise(
        when(
            col(LOWER_SYNC).is_null(),
            col(Column::new(
                Some(UPPER_SYNC),
                upper_old_pk_col.field().name(),
            )),
        )
        .otherwise(col(Column::new(Some(LOWER_SYNC), lower_old_pk_field)))?,
    )?)
}

fn project_new_pk_column(
    lower_new_pk_col: &SyncColumn,
    upper_schema: &SyncSchema,
) -> SyncResult<Expr> {
    let lower_new_pk_field = lower_new_pk_col.field().name();

    let upper_new_pk_col = upper_schema
        .column(lower_new_pk_col.name(), ColumnRole::NewPk)
        .expect("Old Pk columns must be the same across syncs");

    // For un-matched lower sync rows use lower sync new PKs, otherwise
    // for un-matched and matched upper sync rows use upper sync new PKs
    Ok(when(
        col(UPPER_SYNC).is_null(),
        col(Column::new(Some(LOWER_SYNC), lower_new_pk_field)),
    )
    .otherwise(col(Column::new(
        Some(UPPER_SYNC),
        upper_new_pk_col.field().name(),
    )))?)
}

fn project_changed_column(
    this_changed_col: &SyncColumn,
    other_schema: &SyncSchema,
    sync_type: SyncPosition,
) -> SyncResult<Expr> {
    let (this_sync, that_sync) = match sync_type {
        SyncPosition::Lower => (LOWER_SYNC, UPPER_SYNC),
        SyncPosition::Upper => (UPPER_SYNC, LOWER_SYNC),
    };

    // For un-matched this sync rows use this sync changed values, otherwise ...
    Ok(when(
        col(that_sync).is_null(),
        col(Column::new(
            Some(this_sync),
            this_changed_col.field().name(),
        )),
    )
    .otherwise(match sync_type {
        SyncPosition::Lower => {
            if let Some(upper_changed_col) =
                other_schema.column(this_changed_col.name(), ColumnRole::Changed)
            {
                // ... upper sync has the corresponding `Changed` column, take the upper column value
                col(Column::new(
                    Some(UPPER_SYNC),
                    upper_changed_col.field().name(),
                ))
            } else if other_schema
                .column(this_changed_col.name(), ColumnRole::Value)
                .is_some()
            {
                // ... upper sync doesn't have the `Changed` column at hand, but it does have
                // the corresponding `Value` column, project true to pick those values up
                lit(true)
            } else {
                // ... upper sync has neither the `Changed` nor the associated `Value` column,
                // project false to avoid overriding with NULL values
                lit(false)
            }
        }
        // In case of upper sync always take its values
        SyncPosition::Upper => col(Column::new(
            Some(UPPER_SYNC),
            this_changed_col.field().name(),
        )),
    })?)
}

fn project_lower_value_column(
    lower_value_col: &SyncColumn,
    lower_schema: &SyncSchema,
    upper_schema: &SyncSchema,
    col_desc: &mut IndexSet<ColumnDescriptor>,
    projection: &mut Vec<Expr>,
) -> SyncResult<Expr> {
    if upper_schema
        .column(lower_value_col.name(), ColumnRole::Value)
        .is_none()
        && lower_schema
            .column(lower_value_col.name(), ColumnRole::Changed)
            .is_none()
    {
        // There's no corresponding `Value` sync column in the upper schema, and
        // there's no `Changed` column in the lower schema either, we need to project
        // the `Changed` column that takes all the values from the lower schema and
        // no value from the upper one
        col_desc.insert(ColumnDescriptor {
            role: ColumnRole::Changed as _,
            name: lower_value_col.name().clone(),
        });
        projection
            .push(when(col(LOWER_SYNC).is_null(), lit(false)).otherwise(lit(true))?);
    }

    // For un-matched lower sync rows use lower sync value values, otherwise ...
    Ok(when(
        col(UPPER_SYNC).is_null(),
        col(Column::new(
            Some(LOWER_SYNC),
            lower_value_col.field().name(),
        )),
    )
    .otherwise(
        if let Some(upper_value_col) =
            upper_schema.column(lower_value_col.name(), ColumnRole::Value)
        {
            // ... the upper sync has this `Value` column, take its values
            col(Column::new(
                Some(UPPER_SYNC),
                upper_value_col.field().name(),
            ))
        } else {
            // ... the upper sync doesn't have this column but the lower sync already has
            // the `Changed` column which was/will be included in the output.
            // Project NULLs in its place.
            when(
                col(UPPER_SYNC).is_null(),
                lit(ScalarValue::Null.cast_to(lower_value_col.field().data_type())?),
            )
            .otherwise(col(Column::new(
                Some(LOWER_SYNC),
                lower_value_col.field().name(),
            )))?
        },
    )?)
}

fn project_upper_value_column(
    upper_value_col: &SyncColumn,
    lower_schema: &SyncSchema,
    upper_schema: &SyncSchema,
    col_desc: &mut IndexSet<ColumnDescriptor>,
    projection: &mut Vec<Expr>,
) -> SyncResult<Expr> {
    if lower_schema
        .column(upper_value_col.name(), ColumnRole::Value)
        .is_none()
        && upper_schema
            .column(upper_value_col.name(), ColumnRole::Changed)
            .is_none()
    {
        // There's no corresponding `Value` sync column in the upper schema, and
        // there's no `Changed` column in the lower schema either, we need to project
        // the `Changed` column that takes all the values from the lower schema and
        // no value from the upper one
        col_desc.insert(ColumnDescriptor {
            role: ColumnRole::Changed as _,
            name: upper_value_col.name().clone(),
        });
        projection
            .push(when(col(UPPER_SYNC).is_not_null(), lit(false)).otherwise(lit(true))?);
    }

    Ok(when(
        col(UPPER_SYNC).is_not_null(),
        col(Column::new(
            Some(UPPER_SYNC),
            upper_value_col.field().name(),
        )),
    )
    .otherwise(
        // The upper sync doesn't have this column but the lower sync already has
        // the `Changed` column which was/will be included in the output.
        // Project just NULLs in its place.
        lit(ScalarValue::Null.cast_to(upper_value_col.field().data_type())?),
    )?)
}

#[cfg(test)]
mod tests {
    use crate::frontend::flight::sync::schema::SyncSchema;
    use crate::frontend::flight::sync::utils::{
        construct_qualifier, get_prune_map, squash_batches,
    };
    use crate::frontend::flight::sync::writer::DataSyncItem;
    use arrow::array::{
        Array, ArrayRef, BooleanArray, Float64Array, Int32Array, RecordBatch,
        StringArray, UInt8Array,
    };
    use arrow_schema::{DataType, Field, Schema};
    use clade::sync::{ColumnDescriptor, ColumnRole};
    use datafusion::physical_optimizer::pruning::PruningStatistics;
    use datafusion_common::{assert_batches_eq, Column, ScalarValue};
    use datafusion_expr::{col, lit};
    use itertools::Itertools;
    use rand::distributions::{Alphanumeric, DistString, Distribution, WeightedIndex};
    use rand::seq::IteratorRandom;
    use rand::Rng;
    use rstest::rstest;
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;
    use uuid::Uuid;

    #[test]
    fn test_batch_squashing() -> Result<(), Box<dyn std::error::Error>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("old_c1", DataType::Int32, true),
            Field::new("new_c1", DataType::Int32, true),
            Field::new("value_c2", DataType::Float64, true),
            Field::new("changed_c3", DataType::Boolean, false),
            Field::new("value_c3", DataType::Utf8, true),
        ]));

        let column_descriptors = vec![
            ColumnDescriptor {
                role: ColumnRole::OldPk as i32,
                name: "c1".to_string(),
            },
            ColumnDescriptor {
                role: ColumnRole::NewPk as i32,
                name: "c1".to_string(),
            },
            ColumnDescriptor {
                role: ColumnRole::Value as i32,
                name: "c2".to_string(),
            },
            ColumnDescriptor {
                role: ColumnRole::Changed as i32,
                name: "c3".to_string(),
            },
            ColumnDescriptor {
                role: ColumnRole::Value as i32,
                name: "c3".to_string(),
            },
        ];

        let sync_schema = SyncSchema::try_new(column_descriptors, schema.clone())?;

        // Test a batch with several edge cases with:
        // - multiple changes to the same row
        // - PK change
        // - existing row deletion (UPDATE + DELETE)
        // - temp row (INSERT + UPDATE + DELETE)
        // - changed value set to None
        let (old_c1, new_c1, val_c2, chg_c3, val_c3): (
            Vec<_>,
            Vec<_>,
            Vec<_>,
            Vec<_>,
            Vec<_>,
        ) = vec![
            (None, Some(1), Some(1.0), true, Some("one")), // INSERT
            (None, Some(2), Some(2.0), false, None),       // INSERT
            (None, Some(3), Some(3.0), true, Some("three")), // INSERT
            (Some(1), Some(1), Some(1.1), true, None), // UPDATE and change the last value
            (Some(2), Some(-2), Some(2.1), false, None), // UPDATE PK
            (Some(3), Some(6), None, false, None),     // UPDATE PK
            (Some(4), Some(4), Some(4.4), true, Some("four")),
            (Some(5), Some(5), Some(5.0), false, Some("five")),
            (Some(-2), Some(3), Some(3.0), true, Some("two")), // UPDATE PK
            (Some(6), None, Some(5.5), true, Some("five")),    // DELETE temp row
            (Some(1), Some(2), Some(1.2), false, Some("discarded")),
            (Some(5), None, None, false, None), // DELETE existing row
        ]
        .into_iter()
        .multiunzip();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(old_c1)),
                Arc::new(Int32Array::from(new_c1)),
                Arc::new(Float64Array::from(val_c2)),
                Arc::new(BooleanArray::from(chg_c3)),
                Arc::new(StringArray::from(val_c3)),
            ],
        )?;

        let squashed = squash_batches(&sync_schema, vec![batch.clone()])?;

        let expected = [
            "+--------+--------+----------+------------+----------+",
            "| old_c1 | new_c1 | value_c2 | changed_c3 | value_c3 |",
            "+--------+--------+----------+------------+----------+",
            "|        | 2      | 1.2      | true       |          |",
            "|        | 3      | 3.0      | true       | two      |",
            "| 4      | 4      | 4.4      | true       | four     |",
            "| 5      |        |          | false      | five     |",
            "+--------+--------+----------+------------+----------+",
        ];
        assert_batches_eq!(expected, &[squashed]);

        Ok(())
    }

    #[test]
    fn fuzz_batch_squashing() -> Result<(), Box<dyn std::error::Error>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("old_c1", DataType::UInt8, true),
            Field::new("old_c2", DataType::UInt8, true),
            Field::new("new_c1", DataType::UInt8, true),
            Field::new("new_c2", DataType::UInt8, true),
            Field::new("value_c3", DataType::Float64, true),
            Field::new("changed_c4", DataType::Boolean, false),
            Field::new("value_c4", DataType::Utf8, true),
        ]));

        let column_descriptors = vec![
            ColumnDescriptor {
                role: ColumnRole::OldPk as i32,
                name: "c1".to_string(),
            },
            ColumnDescriptor {
                role: ColumnRole::OldPk as i32,
                name: "c2".to_string(),
            },
            ColumnDescriptor {
                role: ColumnRole::NewPk as i32,
                name: "c1".to_string(),
            },
            ColumnDescriptor {
                role: ColumnRole::NewPk as i32,
                name: "c2".to_string(),
            },
            ColumnDescriptor {
                role: ColumnRole::Value as i32,
                name: "c3".to_string(),
            },
            ColumnDescriptor {
                role: ColumnRole::Changed as i32,
                name: "c4".to_string(),
            },
            ColumnDescriptor {
                role: ColumnRole::Value as i32,
                name: "c4".to_string(),
            },
        ];

        let sync_schema = SyncSchema::try_new(column_descriptors, schema.clone())?;

        let mut rng = rand::thread_rng();
        let row_count = rng.gen_range(1..=1000); // With more than 1000 rows the test becomes slow

        let mut used_pks = HashSet::new();
        let mut free_pks = HashSet::new();
        // Insert all possible pairs of u8 values into the HashSet as available primary keys
        for c1 in 0..=u8::MAX {
            for c2 in 0..=u8::MAX {
                // Randomly assign possible PKs as either free or used
                free_pks.insert((c1, c2));
            }
        }

        #[derive(Clone)]
        enum Action {
            Insert,
            UpdateNonPk,
            UpdatePk,
            Delete,
        }

        let actions = [
            Action::Insert,
            Action::UpdateNonPk,
            Action::UpdatePk,
            Action::Delete,
        ];
        let weights = [3, 3, 2, 2];
        let action_dist = WeightedIndex::new(weights).unwrap();

        let mut insert_count = 0;
        let mut delete_count = 0;

        // Generate a random set of rows with random actions
        let (old_c1, old_c2, new_c1, new_c2, val_c3, chg_c4, val_c4): (
            Vec<_>,
            Vec<_>,
            Vec<_>,
            Vec<_>,
            Vec<_>,
            Vec<_>,
            Vec<_>,
        ) = (0..row_count)
            .map(|_| {
                let val_c3: Option<f64> = rng.gen();
                let chg_c4: bool = rng.gen_range(1..=10) == 1; // 1/10 chance of a changed value
                let val_c4 = if rng.gen_range(1..=10) > 2 {
                    Some(Alphanumeric.sample_string(&mut rng, 10)) // 80 % chance of a random string
                } else {
                    None // 20 % chance of a null string
                };

                let action = if free_pks.is_empty() {
                    Action::Delete
                } else if used_pks.is_empty() {
                    Action::Insert
                } else {
                    actions[action_dist.sample(&mut rng)].clone()
                };

                let (old_c1, old_c2, new_c1, new_c2) = match action {
                    Action::Insert => {
                        insert_count += 1;
                        let new_pk = *free_pks.iter().choose(&mut rng).unwrap();
                        free_pks.remove(&new_pk);
                        used_pks.insert(new_pk);
                        (None, None, Some(new_pk.0), Some(new_pk.1))
                    }
                    Action::UpdateNonPk => {
                        let old_pk = *used_pks.iter().choose(&mut rng).unwrap();
                        // Keep the old primary key
                        (
                            Some(old_pk.0),
                            Some(old_pk.1),
                            Some(old_pk.0),
                            Some(old_pk.1),
                        )
                    }
                    Action::UpdatePk => {
                        let old_pk = *used_pks.iter().choose(&mut rng).unwrap();
                        used_pks.remove(&old_pk);
                        free_pks.insert(old_pk);
                        let new_pk = *free_pks.iter().choose(&mut rng).unwrap();
                        free_pks.remove(&new_pk);
                        used_pks.insert(new_pk);
                        (
                            Some(old_pk.0),
                            Some(old_pk.1),
                            Some(new_pk.0),
                            Some(new_pk.1),
                        )
                    }
                    Action::Delete => {
                        delete_count += 1;
                        let old_pk = *used_pks.iter().choose(&mut rng).unwrap();
                        used_pks.remove(&old_pk);
                        free_pks.insert(old_pk);
                        (Some(old_pk.0), Some(old_pk.1), None, None)
                    }
                };

                (old_c1, old_c2, new_c1, new_c2, val_c3, chg_c4, val_c4)
            })
            .multiunzip();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt8Array::from(old_c1)),
                Arc::new(UInt8Array::from(old_c2)),
                Arc::new(UInt8Array::from(new_c1)),
                Arc::new(UInt8Array::from(new_c2)),
                Arc::new(Float64Array::from(val_c3)),
                Arc::new(BooleanArray::from(chg_c4)),
                Arc::new(StringArray::from(val_c4)),
            ],
        )?;

        let squashed = squash_batches(&sync_schema, vec![batch.clone()])?;
        println!(
            "Squashed PKs from {row_count} to {} rows",
            squashed.num_rows()
        );

        // Since we only ever UPDATE or DELETE rows that were already inserted in the test batch,
        // the number of chains, and thus the expected row count is equal to the difference between
        // INSERT and DELETE count.
        assert_eq!(squashed.num_rows(), insert_count - delete_count);

        Ok(())
    }

    #[test]
    fn test_sync_filter() -> Result<(), Box<dyn std::error::Error>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("old_c1", DataType::Int32, true),
            Field::new("old_c2", DataType::Float64, true),
            Field::new("new_c1", DataType::Int32, true),
            Field::new("new_c2", DataType::Float64, true),
        ]));

        let column_descriptors = vec![
            ColumnDescriptor {
                role: ColumnRole::OldPk as i32,
                name: "c1".to_string(),
            },
            ColumnDescriptor {
                role: ColumnRole::OldPk as i32,
                name: "c2".to_string(),
            },
            ColumnDescriptor {
                role: ColumnRole::NewPk as i32,
                name: "c1".to_string(),
            },
            ColumnDescriptor {
                role: ColumnRole::NewPk as i32,
                name: "c2".to_string(),
            },
        ];

        let sync_schema = SyncSchema::try_new(column_descriptors, schema.clone())?;

        // UPDATE, INSERT
        let batch_1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![Some(1), None])),
                Arc::new(Float64Array::from(vec![Some(1.1), None])),
                Arc::new(Int32Array::from(vec![2, 6])),
                Arc::new(Float64Array::from(vec![2.1, 2.2])),
            ],
        )?;

        // DELETE, UPDATE
        let batch_2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![0, 4])),
                Arc::new(Float64Array::from(vec![3.1, 3.2])),
                Arc::new(Int32Array::from(vec![None, Some(3)])),
                Arc::new(Float64Array::from(vec![None, Some(0.1)])),
            ],
        )?;

        let syncs = &[
            DataSyncItem {
                tx_id: Uuid::new_v4(),
                sync_schema: sync_schema.clone(),
                batch: batch_1,
            },
            DataSyncItem {
                tx_id: Uuid::new_v4(),
                sync_schema,
                batch: batch_2,
            },
        ];

        let old_pk_expr = construct_qualifier(syncs, ColumnRole::OldPk)?;
        let new_pk_expr = construct_qualifier(syncs, ColumnRole::NewPk)?;

        assert_eq!(
            old_pk_expr,
            Some(
                col("c1")
                    .between(lit(0), lit(4))
                    .and(col("c2").between(lit(1.1), lit(3.2)))
            ),
        );

        assert_eq!(
            new_pk_expr,
            Some(
                col("c1")
                    .between(lit(2), lit(6))
                    .and(col("c2").between(lit(0.1), lit(2.2)))
            ),
        );

        Ok(())
    }

    struct TestStatistics {
        pub min_values: HashMap<Column, ArrayRef>,
        pub max_values: HashMap<Column, ArrayRef>,
    }

    impl PruningStatistics for TestStatistics {
        fn min_values(&self, column: &Column) -> Option<ArrayRef> {
            self.min_values.get(column).cloned()
        }

        fn max_values(&self, column: &Column) -> Option<ArrayRef> {
            self.max_values.get(column).cloned()
        }

        fn num_containers(&self) -> usize {
            self.min_values
                .values()
                .next()
                .map(|values| values.len())
                .unwrap_or_default()
        }

        fn null_counts(&self, _column: &Column) -> Option<ArrayRef> {
            None
        }

        fn row_counts(&self, _column: &Column) -> Option<ArrayRef> {
            None
        }

        fn contained(
            &self,
            _column: &Column,
            _values: &HashSet<ScalarValue>,
        ) -> Option<BooleanArray> {
            None
        }
    }

    #[rstest]
    #[case(
        vec![Some(70), None, Some(30)],
        vec![Some(90), Some(40), Some(60)],
        vec![Some("aa"), Some("bb"), None],
        vec![Some("rr"), Some("gg"), Some("a")],
        vec![false, true, false],
    )]
    #[test]
    fn test_sync_pruning(
        #[case] c1_min_values: Vec<Option<i32>>,
        #[case] c1_max_values: Vec<Option<i32>>,
        #[case] c2_min_values: Vec<Option<&str>>,
        #[case] c2_max_values: Vec<Option<&str>>,
        #[case] expected_prune_map: Vec<bool>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("old_c1", DataType::Int32, true),
            Field::new("old_c2", DataType::Utf8, true),
            Field::new("new_c1", DataType::Int32, true),
            Field::new("new_c2", DataType::Utf8, true),
        ]));

        let column_descriptors = vec![
            ColumnDescriptor {
                role: ColumnRole::OldPk as i32,
                name: "c1".to_string(),
            },
            ColumnDescriptor {
                role: ColumnRole::OldPk as i32,
                name: "c2".to_string(),
            },
            ColumnDescriptor {
                role: ColumnRole::NewPk as i32,
                name: "c1".to_string(),
            },
            ColumnDescriptor {
                role: ColumnRole::NewPk as i32,
                name: "c2".to_string(),
            },
        ];

        let sync_schema = SyncSchema::try_new(column_descriptors, schema.clone())?;

        // UPDATE, INSERT
        let batch_1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![Some(20), None])),
                Arc::new(StringArray::from(vec![Some("ddd"), None])),
                Arc::new(Int32Array::from(vec![40, 30])),
                Arc::new(StringArray::from(vec!["ccc", "bbb"])),
            ],
        )?;

        // DELETE, UPDATE
        let batch_2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![10, 50])),
                Arc::new(StringArray::from(vec!["aaa", "eee"])),
                Arc::new(Int32Array::from(vec![None, Some(60)])),
                Arc::new(StringArray::from(vec![None, Some("fff")])),
            ],
        )?;

        let syncs = &[
            DataSyncItem {
                tx_id: Uuid::new_v4(),
                sync_schema: sync_schema.clone(),
                batch: batch_1,
            },
            DataSyncItem {
                tx_id: Uuid::new_v4(),
                sync_schema,
                batch: batch_2,
            },
        ];

        let test_statistics = TestStatistics {
            min_values: HashMap::from([
                (
                    Column::from("c1"),
                    Arc::new(Int32Array::from(c1_min_values)) as ArrayRef,
                ),
                (
                    Column::from("c2"),
                    Arc::new(StringArray::from(c2_min_values)) as ArrayRef,
                ),
            ]),
            max_values: HashMap::from([
                (
                    Column::from("c1"),
                    Arc::new(Int32Array::from(c1_max_values)) as ArrayRef,
                ),
                (
                    Column::from("c2"),
                    Arc::new(StringArray::from(c2_max_values)) as ArrayRef,
                ),
            ]),
        };

        let prune_map = get_prune_map(syncs, &test_statistics).unwrap();

        assert_eq!(prune_map, expected_prune_map);

        Ok(())
    }
}
