use std::sync::Arc;

use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    SchemaRef as ArrowSchemaRef,
};
use datafusion::arrow::error::ArrowError;
use serde_json::{from_str, Value};

#[derive(Debug)]
pub struct Schema {
    pub arrow_schema: ArrowSchemaRef,
}

impl Schema {
    fn data_type_from_json(json: Value) -> Result<ArrowDataType, ArrowError> {
        // For some reason, Arrow's DataType has to_json, but from is hidden behind
        // the Arrow Field's serialization. Fake an Arrow field to deserialize our data
        // type back.

        let fake_map = Value::Object(serde_json::Map::from_iter(vec![
            ("name".to_string(), Value::String("".to_string())),
            ("type".to_string(), json),
            ("nullable".to_string(), false.into()),
        ]));

        Ok(ArrowField::from(&fake_map)?.data_type().to_owned())
    }

    pub fn from_column_names_types<'a, I>(columns: I) -> Self
    where
        I: Iterator<Item = (&'a String, &'a String)>,
    {
        let fields = columns
            .map(|(name, text_type)| {
                ArrowField::new(
                    name,
                    Self::data_type_from_json(from_str(text_type).unwrap()).unwrap(),
                    true,
                )
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
            .map(|f| (f.name().clone(), f.data_type().to_json().to_string()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_roundtripping() {
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            // TODO: maybe we do need to serialize nullable?
            ArrowField::new("col_1", ArrowDataType::Date64, true),
            ArrowField::new("col_2", ArrowDataType::Float64, true),
            ArrowField::new("col_3", ArrowDataType::Boolean, true),
        ]));

        let sf_schema = Schema {
            arrow_schema: arrow_schema.clone(),
        };

        let cols = sf_schema.to_column_names_types();
        assert_eq!(
            cols,
            vec![
                (
                    "col_1".to_string(),
                    "{\"name\":\"date\",\"unit\":\"MILLISECOND\"}".to_string()
                ),
                (
                    "col_2".to_string(),
                    "{\"name\":\"floatingpoint\",\"precision\":\"DOUBLE\"}".to_string()
                ),
                ("col_3".to_string(), "{\"name\":\"bool\"}".to_string())
            ]
        );

        let roundtrip_schema =
            Schema::from_column_names_types(cols.iter().map(|(n, t)| (n, t)));

        assert_eq!(arrow_schema, roundtrip_schema.arrow_schema);
    }
}
