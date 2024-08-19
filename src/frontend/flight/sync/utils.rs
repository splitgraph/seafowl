use crate::frontend::flight::sync::schema::SyncSchema;
use crate::frontend::flight::sync::writer::DataSyncItem;
use arrow::array::{new_null_array, Array, ArrayRef, RecordBatch, UInt64Array};
use arrow::compute::{concat_batches, take};
use arrow_row::{Row, RowConverter, SortField};
use clade::sync::ColumnRole;
use datafusion::functions_aggregate::min_max::{MaxAccumulator, MinAccumulator};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::{col, lit, Accumulator, Expr};
use std::collections::{HashMap, HashSet, VecDeque};

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

#[cfg(test)]
mod tests {
    use crate::frontend::flight::sync::schema::SyncSchema;
    use crate::frontend::flight::sync::utils::{construct_qualifier, squash_batches};
    use crate::frontend::flight::sync::writer::DataSyncItem;
    use arrow::array::{
        BooleanArray, Float64Array, Int32Array, RecordBatch, StringArray, UInt8Array,
    };
    use arrow_schema::{DataType, Field, Schema};
    use clade::sync::{ColumnDescriptor, ColumnRole};
    use datafusion_common::assert_batches_eq;
    use datafusion_expr::{col, lit};
    use itertools::Itertools;
    use rand::distributions::{Alphanumeric, DistString, Distribution, WeightedIndex};
    use rand::seq::IteratorRandom;
    use rand::Rng;
    use std::collections::HashSet;
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
}
