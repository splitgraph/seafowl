use arrow_integration_test::{field_from_json, field_to_json};
use std::sync::Arc;

use datafusion::arrow::datatypes::{
    Field as ArrowField, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef,
};
use datafusion::arrow::error::ArrowError;
use serde_json::{from_str, Value};

#[derive(Debug)]
pub struct Schema {
    pub arrow_schema: ArrowSchemaRef,
}

impl Schema {
    fn field_from_json(json: Value) -> Result<ArrowField, ArrowError> {
        field_from_json(&json)
    }

    pub fn from_column_names_types<'a, I>(columns: I) -> Self
    where
        I: Iterator<Item = (&'a String, &'a String)>,
    {
        let fields: Vec<_> = columns
            .map(|(name, text_type)| {
                let field = Self::field_from_json(from_str(text_type).unwrap()).unwrap();

                ArrowField::new(name, field.data_type().to_owned(), field.is_nullable())
            })
            .collect();

        Self {
            arrow_schema: Arc::new(ArrowSchema::new(fields)),
        }
    }

    pub fn to_column_names_types(&self) -> Vec<(String, String)> {
        self.arrow_schema
            .fields()
            .iter()
            .map(|f| (f.name().clone(), field_to_json(f).to_string()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::DataType as ArrowDataType;

    #[test]
    fn test_schema_roundtripping() {
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("col_1", ArrowDataType::Date64, false),
            ArrowField::new("col_2", ArrowDataType::Float64, false),
            ArrowField::new("col_3", ArrowDataType::Boolean, true),
            ArrowField::new(
                "col_4",
                ArrowDataType::List(Arc::new(ArrowField::new(
                    "child_0",
                    ArrowDataType::Utf8,
                    true,
                ))),
                false,
            ),
        ]));

        let sf_schema = Schema {
            arrow_schema: arrow_schema.clone(),
        };

        let cols = sf_schema.to_column_names_types();
        assert_eq!(
            cols,
            vec![
                ("col_1".to_string(),
                r#"{"children":[],"name":"col_1","nullable":false,"type":{"name":"date","unit":"MILLISECOND"}}"#.to_string()),
                ("col_2".to_string(),
                r#"{"children":[],"name":"col_2","nullable":false,"type":{"name":"floatingpoint","precision":"DOUBLE"}}"#.to_string()),
                ("col_3".to_string(),
                r#"{"children":[],"name":"col_3","nullable":true,"type":{"name":"bool"}}"#.to_string()),
                ("col_4".to_string(),
                r#"{"children":[{"children":[],"name":"child_0","nullable":true,"type":{"name":"utf8"}}],"name":"col_4","nullable":false,"type":{"name":"list"}}"#.to_string()),
            ]
        );

        let roundtrip_schema =
            Schema::from_column_names_types(cols.iter().map(|(n, t)| (n, t)));

        assert_eq!(arrow_schema, roundtrip_schema.arrow_schema);
    }
}
