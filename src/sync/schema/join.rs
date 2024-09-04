use crate::sync::schema::merge::SyncPosition;
use crate::sync::schema::{SyncColumn, SyncSchema};
use crate::sync::writer::{LOWER_SYNC, UPPER_SYNC};
use crate::sync::SyncResult;
use clade::sync::{ColumnDescriptor, ColumnRole};
use datafusion_common::{Column, ScalarValue};
use datafusion_expr::{col, lit, when, Expr};
use indexmap::IndexSet;

pub(super) fn join_old_pk_column(
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

pub(super) fn join_new_pk_column(
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

pub(super) fn join_changed_column(
    this_changed_col: &SyncColumn,
    other_schema: &SyncSchema,
    sync_position: SyncPosition,
) -> SyncResult<Expr> {
    let (this_sync, other_sync) = match sync_position {
        SyncPosition::Lower => (LOWER_SYNC, UPPER_SYNC),
        SyncPosition::Upper => (UPPER_SYNC, LOWER_SYNC),
    };

    // For un-matched rows in this sync use this sync changed values, otherwise ...
    Ok(when(
        col(other_sync).is_null(),
        col(Column::new(
            Some(this_sync),
            this_changed_col.field().name(),
        )),
    )
    .otherwise(
        if let Some(other_changed_col) =
            other_schema.column(this_changed_col.name(), ColumnRole::Changed)
        {
            // ... other sync has the corresponding `Changed` column, take the upper column value in
            // both cases
            col(Column::new(
                Some(UPPER_SYNC),
                match sync_position {
                    SyncPosition::Lower => other_changed_col.field().name(),
                    SyncPosition::Upper => this_changed_col.field().name(),
                },
            ))
        } else if other_schema
            .column(this_changed_col.name(), ColumnRole::Value)
            .is_some()
        {
            // ... other sync doesn't have the `Changed` column in the schema, but it does have
            // the corresponding `Value` column
            match sync_position {
                // project true to pick those upper values up
                SyncPosition::Lower => lit(true),
                // project true to pick those lower values up only for non-upper rows
                SyncPosition::Upper => {
                    when(col(UPPER_SYNC).is_null(), lit(true)).otherwise(col(
                        Column::new(Some(UPPER_SYNC), this_changed_col.field().name()),
                    ))?
                }
            }
        } else {
            // ... other sync has neither the `Changed` nor the associated `Value` column,
            match sync_position {
                // project false to avoid overriding lower values with NULL values
                SyncPosition::Lower => lit(false),
                // project false to avoid overriding upper values with NULL values
                SyncPosition::Upper => {
                    when(col(UPPER_SYNC).is_null(), lit(false)).otherwise(col(
                        Column::new(Some(UPPER_SYNC), this_changed_col.field().name()),
                    ))?
                }
            }
        },
    )?)
}

pub(super) fn join_value_column(
    this_value_col: &SyncColumn,
    this_schema: &SyncSchema,
    other_schema: &SyncSchema,
    col_desc: &mut IndexSet<ColumnDescriptor>,
    projection: &mut Vec<Expr>,
    sync_position: SyncPosition,
) -> SyncResult<Expr> {
    let (this_sync, other_sync) = match sync_position {
        SyncPosition::Lower => (LOWER_SYNC, UPPER_SYNC),
        SyncPosition::Upper => (UPPER_SYNC, LOWER_SYNC),
    };

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
        projection.push(when(col(this_sync).is_null(), lit(false)).otherwise(lit(true))?);
    }

    // For un-matched rows in this sync use this sync changed values, otherwise ...
    Ok(when(
        col(other_sync).is_null(),
        col(Column::new(Some(this_sync), this_value_col.field().name())),
    )
    .otherwise(
        if let Some(other_value_col) =
            other_schema.column(this_value_col.name(), ColumnRole::Value)
        {
            match sync_position {
                SyncPosition::Lower => {
                    // ... the upper sync has this `Value` column too, take its values
                    col(Column::new(
                        Some(UPPER_SYNC),
                        other_value_col.field().name(),
                    ))
                }
                SyncPosition::Upper => {
                    // ... the lower sync has this `Value` column too, take its values where it
                    // didn't get joined to the upper sync, otherwise take upper sync values
                    when(
                        col(UPPER_SYNC).is_null(),
                        col(Column::new(Some(UPPER_SYNC), this_value_col.field().name())),
                    )
                    .otherwise(col(Column::new(
                        Some(LOWER_SYNC),
                        other_value_col.field().name(),
                    )))?
                }
            }
        } else {
            // ... other sync doesn't have this column but this sync does. Since the corresponding
            // `Changed` column will be added regardless ...
            match sync_position {
                SyncPosition::Lower => {
                    // ... project NULLs in its place for unmatched upper sync rows, otherwise for
                    // the matched rows project the lower sync values.
                    when(
                        col(UPPER_SYNC).is_null(),
                        lit(ScalarValue::Null
                            .cast_to(this_value_col.field().data_type())?),
                    )
                    .otherwise(col(Column::new(
                        Some(LOWER_SYNC),
                        this_value_col.field().name(),
                    )))?
                }
                SyncPosition::Upper => {
                    // ... project upper schema column values both where it matched rows in the
                    // lower sync and where it didn't (i.e. which has NULLs by virtue of the JOIN)
                    col(Column::new(Some(UPPER_SYNC), this_value_col.field().name()))
                }
            }
        },
    )?)
}
