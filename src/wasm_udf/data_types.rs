use core::fmt;

use datafusion::logical_expr::Volatility;
use serde::ser::SerializeSeq;
use serde::de::{Error, Visitor, SeqAccess};
use serde::{Deserialize, Serialize, Serializer, Deserializer};
use strum_macros::{Display, EnumString};
use wasmtime::ValType;

// WASM to DataFusion conversions
pub fn get_wasm_type(t: &sqlparser::ast::DataType) -> Result<ValType, datafusion::error::DataFusionError> {
    // based: on https://www.splitgraph.com/docs/seafowl/reference/types
    match t {
        sqlparser::ast::DataType::Integer(_) => Ok(ValType::I32),
        sqlparser::ast::DataType::BigInt(_) => Ok(ValType::I64),
        sqlparser::ast::DataType::Float(_) => Ok(ValType::F32),
        sqlparser::ast::DataType::Real => Ok(ValType::F32),
        sqlparser::ast::DataType::Double => Ok(ValType::F64),
        _ => Err(datafusion::error::DataFusionError::Internal(String::from("Unsupported datatype for UDF language wasm")))
    }
}

pub fn get_volatility(t: &CreateFunctionVolatility) -> Volatility {
    match t {
        CreateFunctionVolatility::Immutable => Volatility::Immutable,
        CreateFunctionVolatility::Stable => Volatility::Stable,
        CreateFunctionVolatility::Volatile => Volatility::Volatile,
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, EnumString, Display, Clone)]
#[serde(rename_all = "camelCase")]
pub enum CreateFunctionVolatility {
    Immutable,
    Stable,
    Volatile,
}
impl Default for CreateFunctionVolatility {
    fn default() -> Self {
        CreateFunctionVolatility::Volatile
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, EnumString, Display, Clone)]
#[serde(rename_all = "camelCase")]
pub enum CreateFunctionLanguage {
    Wasm
}
impl Default for CreateFunctionLanguage {
    fn default() -> Self {
        CreateFunctionLanguage::Wasm
    }
}

pub fn datatype_to_string(datatype: &sqlparser::ast::DataType) -> String {
    format!("{}", datatype)
}

pub fn string_to_datatype(s: &str) -> Option<sqlparser::ast::DataType> {
    // Note: we could do this smarter using the sqlparser to convert
    // the string to a DataType.
    // based on: https://www.splitgraph.com/docs/seafowl/reference/types
    match s {
        "BIGINT" => Some(sqlparser::ast::DataType::BigInt(None)),
        "INT" => Some(sqlparser::ast::DataType::Int(None)),
        "INTEGER" => Some(sqlparser::ast::DataType::Integer(None)),
        "FLOAT" => Some(sqlparser::ast::DataType::Float(None)),
        "REAL" => Some(sqlparser::ast::DataType::Real),
        "DOUBLE" => Some(sqlparser::ast::DataType::Double),
        // TODO: remaining data types
        _ => None
    }
}

// inspired by https://stackoverflow.com/a/39390561
fn serialize_datatype_vec<S>(datatype_vec: &Vec<sqlparser::ast::DataType>, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut seq = s.serialize_seq(Some(datatype_vec.len()))?;
    for element in datatype_vec {
        let serialized_element = datatype_to_string(element);
        seq.serialize_element(&serialized_element)?;
    }
    seq.end()
}

fn serialize_datatype<S>(datatype: &sqlparser::ast::DataType, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    return s.serialize_str(datatype_to_string(datatype).as_str());
}

struct DataTypeVecDeserializer;

impl<'de> Visitor<'de> for DataTypeVecDeserializer {
    type Value = Vec<sqlparser::ast::DataType>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("Vec<sqlparser::ast::DataType> value sequence.")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut new_obj = Vec::with_capacity(seq.size_hint().unwrap_or_default());
        while let Some(key) = seq.next_element()? as Option<&str> {
            match string_to_datatype(key) {
                Some(dt) => new_obj.push(dt),
                None => return Err(A::Error::custom(format!(
                    "couldnt decode {}", key
                )))
            }
        }

        Ok(new_obj)
    }
}

fn deserialize_datatype_vec<'de, D>(deserializer: D) -> Result<Vec<sqlparser::ast::DataType>, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_seq(DataTypeVecDeserializer)
}

fn deserialize_datatype<'de, D>(deserializer: D) -> Result<sqlparser::ast::DataType, D::Error>
where
    D: Deserializer<'de>,
{
    let s: &str = Deserialize::deserialize(deserializer)?;
    match string_to_datatype(s) {
        Some(d) => Ok(d),
        None => Err(D::Error::custom(format!("unsupported data type {}", s)))
    }
}


#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct CreateFunctionDetails {
    pub entrypoint: String,
    #[serde(default)]
    pub language: CreateFunctionLanguage,
    #[serde(serialize_with = "serialize_datatype_vec", deserialize_with = "deserialize_datatype_vec")]
    pub input_types: Vec<sqlparser::ast::DataType>,
    #[serde(serialize_with = "serialize_datatype", deserialize_with = "deserialize_datatype")]
    pub return_type: sqlparser::ast::DataType,
    pub data: String,
    #[serde(default)]
    pub volatility: CreateFunctionVolatility,
}

#[cfg(test)]

mod tests {
    use super::*;

    #[test]
    fn test_create_function_details_parsing() {
        let details: CreateFunctionDetails = serde_json::from_str(
            r#"{
            "entrypoint": "some_function",
            "language": "wasm",
            "input_types": ["BIGINT", "BIGINT", "BIGINT"],
            "return_type": "BIGINT",
            "data": "AGFzbQEAAAABGAVgA35"
        }"#,
        )
        .unwrap();

        assert_eq!(
            details,
            CreateFunctionDetails {
                entrypoint: "some_function".to_string(),
                language: CreateFunctionLanguage::Wasm,
                input_types: vec![
                    sqlparser::ast::DataType::BigInt(None);3
                ],
                return_type:  sqlparser::ast::DataType::BigInt(None),
                data: "AGFzbQEAAAABGAVgA35".to_string(),
                volatility: CreateFunctionVolatility::Volatile
            }
        )
    }
}
