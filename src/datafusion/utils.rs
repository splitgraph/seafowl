use datafusion::sql::planner::convert_simple_data_type;

use sqlparser::ast::{ColumnDef as SQLColumnDef, ColumnOption, Ident};

use datafusion::arrow::datatypes::{Field, Schema};
pub use datafusion::error::{DataFusionError as Error, Result};

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
