use crate::sync::schema::merge::SyncPosition;
use crate::sync::schema::{SyncColumn, SyncSchema};
use crate::sync::SyncResult;
use clade::sync::{ColumnDescriptor, ColumnRole};
use datafusion_common::ScalarValue;
use datafusion_expr::{col, lit, Expr};
use indexmap::IndexSet;

pub(super) fn union_pk_column(
    lower_pk_col: &SyncColumn,
    upper_schema: &SyncSchema,
) -> (Expr, Expr) {
    let upper_pk_col = upper_schema
        .column(lower_pk_col.name(), lower_pk_col.role())
        .expect("Old Pk columns must be the same across syncs");

    (
        col(lower_pk_col.field().name()),
        col(upper_pk_col.field().name()),
    )
}

pub(super) fn union_changed_column(
    this_changed_col: &SyncColumn,
    other_schema: &SyncSchema,
    sync_position: SyncPosition,
) -> (Expr, Expr) {
    if let Some(other_changed_col) =
        other_schema.column(this_changed_col.name(), ColumnRole::Changed)
    {
        // ... other sync has the corresponding `Changed` column, projects both
        match sync_position {
            SyncPosition::Lower => (
                col(this_changed_col.field().name()),
                col(other_changed_col.field().name()),
            ),
            SyncPosition::Upper => {
                unreachable!("Picked up `Changed` column during processing of lower sync")
            }
        }
    } else if other_schema
        .column(this_changed_col.name(), ColumnRole::Value)
        .is_some()
    {
        // ... other sync doesn't have the `Changed` column in the schema, but it does have
        // the corresponding `Value` column
        match sync_position {
            // project true to pick those upper values up
            SyncPosition::Lower => (col(this_changed_col.field().name()), lit(true)),
            // project true to pick those lower values up
            SyncPosition::Upper => (lit(true), col(this_changed_col.field().name())),
        }
    } else {
        // ... other sync has neither the `Changed` nor the associated `Value` column,
        match sync_position {
            // project false to avoid overriding lower values with NULL values
            SyncPosition::Lower => (col(this_changed_col.field().name()), lit(false)),
            // project false to avoid overriding upper values with NULL values
            SyncPosition::Upper => (lit(false), col(this_changed_col.field().name())),
        }
    }
}

pub(super) fn union_value_column(
    this_value_col: &SyncColumn,
    this_schema: &SyncSchema,
    other_schema: &SyncSchema,
    col_desc: &mut IndexSet<ColumnDescriptor>,
    lower: &mut Vec<Expr>,
    upper: &mut Vec<Expr>,
    sync_position: SyncPosition,
) -> SyncResult<(Expr, Expr)> {
    if other_schema
        .column(this_value_col.name(), ColumnRole::Value)
        .is_none()
        && this_schema
            .column(this_value_col.name(), ColumnRole::Changed)
            .is_none()
    {
        // There's no corresponding `Value` sync column in the other schema (so we'll project NULLs),
        // and there's no `Changed` column in this schema either, we need to project the `Changed`
        // column that takes all the values from this sync and no value from the other sync
        col_desc.insert(ColumnDescriptor {
            role: ColumnRole::Changed as _,
            name: this_value_col.name().to_string(),
        });

        lower.push(lit(sync_position == SyncPosition::Lower));
        upper.push(lit(sync_position == SyncPosition::Upper));
    }

    Ok(
        if let Some(other_value_col) =
            other_schema.column(this_value_col.name(), ColumnRole::Value)
        {
            // ... other sync has the corresponding `Changed` column, projects both
            match sync_position {
                SyncPosition::Lower => (
                    col(this_value_col.field().name()),
                    col(other_value_col.field().name()),
                ),
                SyncPosition::Upper => {
                    unreachable!(
                        "Picked up `Value` column during processing of lower sync"
                    )
                }
            }
        } else {
            // ... other sync doesn't have this column but this sync does. Since the corresponding
            // `Changed` column will be added regardless ...
            match sync_position {
                SyncPosition::Lower => (
                    col(this_value_col.field().name()),
                    lit(ScalarValue::Null.cast_to(this_value_col.field().data_type())?),
                ),
                SyncPosition::Upper => (
                    lit(ScalarValue::Null.cast_to(this_value_col.field().data_type())?),
                    col(this_value_col.field().name()),
                ),
            }
        },
    )
}
