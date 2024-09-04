use crate::sync::schema::join::{
    join_changed_column, join_new_pk_column, join_old_pk_column, join_value_column,
};
use crate::sync::schema::union::{
    union_changed_column, union_pk_column, union_value_column,
};
use crate::sync::schema::{SyncColumn, SyncSchema};
use crate::sync::writer::LOWER_SYNC;
use crate::sync::SyncResult;
use clade::sync::{ColumnDescriptor, ColumnRole};
use datafusion::logical_expr::Expr;
use indexmap::IndexSet;

#[allow(dead_code)]
pub(crate) enum MergeProjection {
    // Used in general when there might be some PK-chains between the lower and upper sync.
    // To be applied after the join operator.
    Join(Vec<Expr>),
    // Used in special cases when we know there are no PK-chains to squash, e.g. when an upper sync
    // is append-only.
    // To be applied to lower/upper sync prior to union.
    Union(Vec<Expr>, Vec<Expr>),
}

#[allow(dead_code)]
impl MergeProjection {
    pub(crate) fn new_join() -> Self {
        MergeProjection::Join(vec![])
    }

    pub(crate) fn new_union() -> Self {
        MergeProjection::Union(vec![], vec![])
    }

    pub(crate) fn join(self) -> Vec<Expr> {
        match self {
            MergeProjection::Join(projection) => projection,
            MergeProjection::Union(_, _) => panic!("No join projection for a union"),
        }
    }

    pub(crate) fn union(self) -> (Vec<Expr>, Vec<Expr>) {
        match self {
            MergeProjection::Join(_) => panic!("No union projection for a join"),
            MergeProjection::Union(lower, upper) => (lower, upper),
        }
    }
}

#[derive(PartialEq)]
pub(super) enum SyncPosition {
    Lower,
    Upper,
}

// Build the merged projection and column descriptors from the sync schemas
pub(crate) fn merge_schemas(
    lower_schema: &SyncSchema,
    upper_schema: &SyncSchema,
    mut merge: MergeProjection,
) -> SyncResult<(Vec<ColumnDescriptor>, MergeProjection)> {
    let mut col_desc = IndexSet::new();

    // Project all sync columns from the lower schema
    for lower_sync_col in lower_schema.columns() {
        // Build the expression to project the combined column
        match &mut merge {
            MergeProjection::Join(ref mut projection) => {
                let expr = match lower_sync_col.role() {
                    ColumnRole::OldPk => {
                        join_old_pk_column(lower_sync_col, upper_schema)?
                    }
                    ColumnRole::NewPk => {
                        join_new_pk_column(lower_sync_col, upper_schema)?
                    }
                    ColumnRole::Changed => join_changed_column(
                        lower_sync_col,
                        upper_schema,
                        SyncPosition::Lower,
                    )?,
                    ColumnRole::Value => join_value_column(
                        lower_sync_col,
                        lower_schema,
                        upper_schema,
                        &mut col_desc,
                        projection,
                        SyncPosition::Lower,
                    )?,
                };
                projection.push(expr);
            }
            MergeProjection::Union(ref mut lower, ref mut upper) => {
                let (low, upp) = match lower_sync_col.role() {
                    ColumnRole::OldPk | ColumnRole::NewPk => {
                        union_pk_column(lower_sync_col, upper_schema)
                    }
                    ColumnRole::Changed => union_changed_column(
                        lower_sync_col,
                        upper_schema,
                        SyncPosition::Lower,
                    ),
                    ColumnRole::Value => union_value_column(
                        lower_sync_col,
                        lower_schema,
                        upper_schema,
                        &mut col_desc,
                        lower,
                        upper,
                        SyncPosition::Lower,
                    )?,
                };

                lower.push(low);
                upper.push(upp);
            }
        }

        col_desc.insert(lower_sync_col.column_descriptor());
    }

    // Now project any missing sync columns from the upper schema
    for upper_sync_col in upper_schema.columns() {
        let cd = upper_sync_col.column_descriptor();

        if col_desc.contains(&cd) {
            // We've already projected this column from the lower schema
            continue;
        }

        // Build the expression that is going to be used when upper sync column value is missing
        match &mut merge {
            MergeProjection::Join(ref mut projection) => {
                let expr = match upper_sync_col.role() {
                    ColumnRole::Changed => join_changed_column(
                        upper_sync_col,
                        lower_schema,
                        SyncPosition::Upper,
                    )?,
                    ColumnRole::Value => join_value_column(
                        upper_sync_col,
                        upper_schema,
                        lower_schema,
                        &mut col_desc,
                        projection,
                        SyncPosition::Upper,
                    )?,
                    _ => continue, // We already picked up PKs from the lower sync
                };
                projection.push(expr);
            }
            MergeProjection::Union(ref mut lower, ref mut upper) => {
                let (low, upp) = match upper_sync_col.role() {
                    ColumnRole::Changed => union_changed_column(
                        upper_sync_col,
                        lower_schema,
                        SyncPosition::Upper,
                    ),
                    ColumnRole::Value => union_value_column(
                        upper_sync_col,
                        upper_schema,
                        lower_schema,
                        &mut col_desc,
                        lower,
                        upper,
                        SyncPosition::Upper,
                    )?,
                    _ => continue, // We already picked up PKs from the lower sync
                };

                lower.push(low);
                upper.push(upp);
            }
        }

        col_desc.insert(upper_sync_col.column_descriptor());
    }

    let col_desc = col_desc.into_iter().collect::<Vec<_>>();

    // Wrap the final projection expression in a qualified alias, since it will act as a new lower
    // sync in the subsequent merge
    let qual_as_lower = |(expr, cd): (&mut Expr, &ColumnDescriptor)| {
        *expr = std::mem::take(expr)
            .alias_qualified(Some(LOWER_SYNC), SyncColumn::canonical_field_name(cd));
    };

    match &mut merge {
        MergeProjection::Join(projection) => projection
            .iter_mut()
            .zip(col_desc.iter())
            .for_each(qual_as_lower),
        MergeProjection::Union(lower, upper) => {
            lower
                .iter_mut()
                .zip(col_desc.iter())
                .for_each(qual_as_lower);
            upper
                .iter_mut()
                .zip(col_desc.iter())
                .for_each(qual_as_lower);
        }
    }

    Ok((col_desc, merge))
}
