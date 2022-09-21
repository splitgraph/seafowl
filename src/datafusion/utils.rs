use datafusion::sql::planner::convert_simple_data_type;

use sqlparser::ast::{ColumnDef as SQLColumnDef, ColumnOption, Ident};

pub use datafusion::error::{DataFusionError as Error, Result};
use datafusion::{
    arrow::datatypes::{Field, Schema},
    error::DataFusionError,
    logical_plan::Column,
};

// Normalize an identifier to a lowercase string unless the identifier is quoted.
pub(crate) fn normalize_ident(id: &Ident) -> String {
    match id.quote_style {
        Some(_) => id.value.clone(),
        None => id.value.to_ascii_lowercase(),
    }
}

// Copied from SqlRel (private there)
pub(crate) fn build_schema(columns: Vec<SQLColumnDef>) -> Result<Schema> {
    let mut fields = Vec::with_capacity(columns.len());

    for column in columns {
        let data_type = convert_simple_data_type(&column.data_type)?;

        // Modified from DataFusion to default to nullable
        let allow_null = !column
            .options
            .iter()
            .any(|x| x.option == ColumnOption::NotNull);
        fields.push(Field::new(
            &normalize_ident(&column.name),
            data_type,
            allow_null,
        ));
    }

    Ok(Schema::new(fields))
}

// This one is partially taken from the planner for SQLExpr::CompoundIdentifier
pub(crate) fn compound_identifier_to_column(ids: &[Ident]) -> Result<Column> {
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
