use std::sync::Arc;

use datafusion::arrow::datatypes::{
    Field as ArrowField, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef,
};
use serde_json::from_str;

#[derive(Debug)]
pub struct Schema {
    pub arrow_schema: ArrowSchemaRef,
}

impl Schema {
    pub fn from_column_names_types<'a, I>(columns: I) -> Self
    where
        I: Iterator<Item = (&'a String, &'a String)>,
    {
        let fields = columns
            .map(|(name, text_type)| ArrowField::new(name, from_str(text_type).unwrap(), true))
            .collect();

        Self {
            arrow_schema: Arc::new(ArrowSchema::new(fields)),
        }
    }

    pub fn to_column_names_types(&self) -> Vec<(String, String)> {
        self.arrow_schema
            .fields()
            .iter()
            .map(|f| (f.name().clone(), f.to_json().to_string()))
            .collect()
    }
}
