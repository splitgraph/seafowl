use crate::frontend::flight::sync::writer::DataSyncCollection;
use arrow::array::{new_null_array, Array, ArrayRef, RecordBatch, UInt64Array};
use arrow::compute::{concat_batches, take};
use arrow_row::{Row, RowConverter, Rows, SortField};
use arrow_schema::SchemaRef;
use clade::sync::{ColumnDescriptor, ColumnRole};
use datafusion::physical_expr::expressions::{MaxAccumulator, MinAccumulator};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::{lit, Accumulator, Expr};
use itertools::Itertools;
use std::collections::{HashMap, VecDeque};

// Get rows out of the provided columns, and a null row for comparisons
fn rows(columns: Vec<ArrayRef>) -> Result<(Rows, Rows)> {
    let (fields, nulls): (Vec<SortField>, Vec<ArrayRef>) = columns
        .iter()
        .map(|array| {
            let data_type = array.data_type();
            (
                SortField::new(data_type.clone()),
                new_null_array(data_type, 1),
            )
        })
        .collect::<Vec<_>>()
        .into_iter()
        .unzip();

    let converter = RowConverter::new(fields)?;
    Ok((
        converter.convert_columns(&columns)?,
        converter.convert_columns(&nulls)?,
    ))
}

// Compact a set of record batches into a single one, squashing any chain of changes to a given row
// into a single row in the output batch.
// This means that if a row is changed multiple times, only the last change will be reflected in the
// output batch (meaning the last NewPk and Value role columns and the last Value column where the
// accompanying Changed field was `true`).
pub(super) fn compact_batches(
    column_descriptors: &[ColumnDescriptor],
    data: Vec<RecordBatch>,
) -> Result<RecordBatch> {
    // Concatenate all the record batches into a single one
    let schema = data.first().unwrap().schema();
    let batch = concat_batches(&schema, &data)?;

    // Get columns for a particular role
    let columns = |role: ColumnRole| -> Vec<ArrayRef> {
        column_descriptors
            .iter()
            .zip(batch.columns().iter())
            .filter_map(|(col_desc, array)| {
                if col_desc.get_role() == role {
                    Some(array.clone())
                } else {
                    None
                }
            })
            .collect()
    };

    let (old_pks, _) = rows(columns(ColumnRole::OldPk))?;
    let (new_pks, new_nulls) = rows(columns(ColumnRole::NewPk))?;

    let changed_binding = columns(ColumnRole::Changed);
    let changed = changed_binding
        .iter()
        .map(|array| {
            array
                .as_any()
                .downcast_ref::<arrow::array::BooleanArray>()
                .expect("Changed column must be boolean")
        })
        .collect::<Vec<_>>();

    // Iterate through pairs of old and new PKs, keeping track of any chains that appear and
    // denoting those with the same chain ID in a new column.
    let mut chain_count = 0;

    // Map of compacted changes, with row ids for the old PK, new PK and changed rows
    let mut column_rows: Vec<(u64, u64, Vec<u64>)> = vec![];
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
                if new_pk != new_nulls.row(0) {
                    // If the new PK is not null keep pointing to the same chain ID, since
                    // otherwise the chain ends by row deletion, and so we don't need to update
                    // its column_rows anymore
                    pk_chains.insert(new_pk, chain_id);
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
                if new_pk != new_nulls.row(0) {
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
    for (old_pk, new_pk, changed_row) in column_rows {
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
    column_descriptors
        .iter()
        .enumerate()
        .for_each(|(col_id, col_desc)| {
            match col_desc.get_role() {
                ColumnRole::OldPk => {
                    indices.insert(col_id, &old_pk_indices);
                }
                ColumnRole::NewPk => {
                    indices.insert(col_id, &new_pk_indices);
                }
                ColumnRole::Changed => {
                    // Insert the indices for both this column as well as for the actual changed
                    // Value column
                    indices.insert(col_id, &changed_indices[0]);
                    indices.insert(
                        schema.index_of(&col_desc.name).expect("Field exists"),
                        &changed_indices[0],
                    );
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
        (0..column_descriptors.len())
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

// Generates a pruning qualifier for the table based on the PK columns and the min/max values
// in the data sync entries.
pub(super) fn construct_qualifier(
    full_schema: SchemaRef,
    entry: &DataSyncCollection,
) -> Result<Expr> {
    // Initialize the min/max accumulators for the primary key columns needed to prune the table
    // files
    let mut min_max_values: HashMap<String, (MinAccumulator, MaxAccumulator)> = entry
        .syncs
        .iter()
        .flat_map(|sync| {
            sync.column_descriptors
                .iter()
                .filter_map(|col_desc| {
                    if matches!(
                        col_desc.get_role(),
                        ColumnRole::OldPk | ColumnRole::NewPk
                    ) {
                        Some(col_desc.name.clone())
                    } else {
                        None
                    }
                })
                .collect::<Vec<String>>()
        })
        .unique()
        .map(|col_name| {
            // The validation in the interface should have ensured these fields exist.
            let f = full_schema.column_with_name(&col_name).unwrap().1;
            Ok((
                col_name,
                (
                    MinAccumulator::try_new(f.data_type())?,
                    MaxAccumulator::try_new(f.data_type())?,
                ),
            ))
        })
        .collect::<Result<_>>()?;

    // Collect all min/max stats for PK columns
    for sync in &entry.syncs {
        min_max_values
            .iter_mut()
            .try_for_each(|(pk_col, (min_value, max_value))| {
                if let Some(pk_array) = sync.batch.column_by_name(pk_col) {
                    min_value.update_batch(&[pk_array.clone()])?;
                    max_value.update_batch(&[pk_array.clone()])?;
                }
                Ok::<(), DataFusionError>(())
            })?
    }

    // Combine the statistics into a single qualifier expression
    Ok(min_max_values
        .iter_mut()
        .map(|(pk_col, (min_value, max_value))| {
            Ok(lit(pk_col)
                .between(lit(min_value.evaluate()?), lit(max_value.evaluate()?)))
        })
        .collect::<Result<Vec<Expr>>>()?
        .into_iter()
        .reduce(|e1: Expr, e2| e1.and(e2))
        .unwrap())
}

#[cfg(test)]
mod tests {
    use crate::frontend::flight::sync::utils::compact_batches;
    use arrow::array::{
        BooleanArray, Float64Array, Int32Array, RecordBatch, StringArray,
    };
    use arrow_schema::{DataType, Field, Schema};
    use clade::sync::{ColumnDescriptor, ColumnRole};
    use datafusion_common::assert_batches_eq;
    use std::sync::Arc;

    #[test]
    fn test_batch_compaction() -> Result<(), Box<dyn std::error::Error>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("old_c1", DataType::Int32, true),
            Field::new("old_c2", DataType::Utf8, true),
            Field::new("new_c1", DataType::Int32, true),
            Field::new("new_c2", DataType::Utf8, true),
            Field::new("value_c3", DataType::Float64, true),
            Field::new("changed_c4", DataType::Boolean, true),
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
                name: "value_c4".to_string(),
            },
            ColumnDescriptor {
                role: ColumnRole::Value as i32,
                name: "c4".to_string(),
            },
        ];

        // Test a batch with several edge cases with changes resulting in chains that
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![
                    None,
                    None,
                    Some(1),
                    Some(2),
                    Some(3),
                    Some(4),
                    Some(1),
                    Some(3),
                ])),
                Arc::new(StringArray::from(vec![
                    None,
                    None,
                    Some("one"),
                    Some("two"),
                    Some("three"),
                    Some("four"),
                    Some("one"),
                    Some("three"),
                ])),
                Arc::new(Int32Array::from(vec![
                    Some(1),
                    Some(2),
                    Some(1),
                    Some(2),
                    Some(3),
                    Some(4),
                    Some(1),
                    None,
                ])),
                Arc::new(StringArray::from(vec![
                    Some("one"),
                    Some("two"),
                    Some("one"),
                    Some("2"),
                    Some("three"),
                    Some("four"),
                    Some("1"),
                    None,
                ])),
                Arc::new(Float64Array::from(vec![
                    Some(1.0),
                    Some(2.0),
                    Some(1.1),
                    Some(2.1),
                    Some(3.3),
                    Some(4.0),
                    Some(1.2),
                    None,
                ])),
                Arc::new(BooleanArray::from(vec![
                    false, true, true, false, true, false, true, false,
                ])),
                Arc::new(StringArray::from(vec![
                    None,
                    Some("two"),
                    Some("one"),
                    None,
                    Some("three"),
                    Some("four"),
                    None,
                    None,
                ])),
            ],
        )?;

        // Pretty print original batch
        let expected = [
            "+--------+--------+--------+--------+----------+------------+----------+",
            "| old_c1 | old_c2 | new_c1 | new_c2 | value_c3 | changed_c4 | value_c4 |",
            "+--------+--------+--------+--------+----------+------------+----------+",
            "|        |        | 1      | one    | 1.0      | false      |          |",
            "|        |        | 2      | two    | 2.0      | true       | two      |",
            "| 1      | one    | 1      | one    | 1.1      | true       | one      |",
            "| 2      | two    | 2      | 2      | 2.1      | false      |          |",
            "| 3      | three  | 3      | three  | 3.3      | true       | three    |",
            "| 4      | four   | 4      | four   | 4.0      | false      | four     |",
            "| 1      | one    | 1      | 1      | 1.2      | true       |          |",
            "| 3      | three  |        |        |          | false      |          |",
            "+--------+--------+--------+--------+----------+------------+----------+",
        ];
        assert_batches_eq!(expected, &[batch.clone()]);

        let compacted = compact_batches(&column_descriptors, vec![batch.clone()])?;

        let expected = [
            "+--------+--------+--------+--------+----------+------------+----------+",
            "| old_c1 | old_c2 | new_c1 | new_c2 | value_c3 | changed_c4 | value_c4 |",
            "+--------+--------+--------+--------+----------+------------+----------+",
            "|        |        | 1      | 1      | 1.2      | true       |          |",
            "|        |        | 2      | 2      | 2.1      | true       | two      |",
            "| 3      | three  |        |        |          | true       | three    |",
            "| 4      | four   | 4      | four   | 4.0      | false      | four     |",
            "+--------+--------+--------+--------+----------+------------+----------+",
        ];
        assert_batches_eq!(expected, &[compacted]);

        Ok(())
    }
}
