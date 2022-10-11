use core::fmt;
use datafusion::logical_expr::Volatility;
use serde::de::{Deserializer, Error, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use strum_macros::{Display, EnumString};
use wasmtime::ValType;

// WASM to DataFusion conversions
pub fn get_wasm_type(t: &CreateFunctionDataType) -> ValType {
    match t {
        // temporary support for legacy WASM-native names:
        CreateFunctionDataType::I32 => ValType::I32,
        CreateFunctionDataType::I64 => ValType::I64,
        CreateFunctionDataType::F32 => ValType::F32,
        CreateFunctionDataType::F64 => ValType::F64,
        // Supported DDL type names
        CreateFunctionDataType::INT => ValType::I32,
        CreateFunctionDataType::BIGINT => ValType::I64,
        CreateFunctionDataType::FLOAT => ValType::F32,
        CreateFunctionDataType::REAL => ValType::F32,
        CreateFunctionDataType::DOUBLE => ValType::F64,
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
#[serde(rename_all = "lowercase")]
pub enum CreateFunctionDataType {
    // temporary support for legacy WASM-native names:
    I32,
    I64,
    F32,
    F64,
    // Supported DDL type names
    //SMALLINT
    INT,
    BIGINT,
    //CHAR
    //VARCHAR
    //TEXT
    //DECIMAL(p,s)
    FLOAT,
    REAL,
    DOUBLE,
    //BOOLEAN
    //DATE
    //TIMESTAMP
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
    Wasm,
    WasiMessagePack
}
impl Default for CreateFunctionLanguage {
    fn default() -> Self {
        CreateFunctionLanguage::Wasm
    }
}

fn parse_create_function_data_type(
    raw: &str,
) -> Result<CreateFunctionDataType, strum::ParseError> {
    CreateFunctionDataType::from_str(&raw.to_ascii_uppercase())
}

struct DataTypeVecDeserializer;

impl<'de> Visitor<'de> for DataTypeVecDeserializer {
    type Value = Vec<CreateFunctionDataType>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("Vec<CreateFunctionDataType> value sequence.")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut new_obj = Vec::with_capacity(seq.size_hint().unwrap_or_default());
        while let Some(key) = seq.next_element()? as Option<&str> {
            let parsed = parse_create_function_data_type(key);
            match parsed {
                Ok(dt) => new_obj.push(dt),
                Err(_) => {
                    return Err(A::Error::custom(format!("couldnt decode {}", key)))
                }
            }
        }

        Ok(new_obj)
    }
}

fn deserialize_datatype_vec<'de, D>(
    deserializer: D,
) -> Result<Vec<CreateFunctionDataType>, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_seq(DataTypeVecDeserializer)
}

fn deserialize_datatype<'de, D>(
    deserializer: D,
) -> Result<CreateFunctionDataType, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    parse_create_function_data_type(&s)
        .map_err(|_| D::Error::custom(format!("unsupported data type: {}", s)))
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct CreateFunctionDetails {
    pub entrypoint: String,
    #[serde(default)]
    pub language: CreateFunctionLanguage,
    #[serde(deserialize_with = "deserialize_datatype_vec")]
    pub input_types: Vec<CreateFunctionDataType>,
    #[serde(deserialize_with = "deserialize_datatype")]
    pub return_type: CreateFunctionDataType,
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
            "input_types": ["bigint", "bigint", "bigint"],
            "return_type": "bigint",
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
                    CreateFunctionDataType::BIGINT,
                    CreateFunctionDataType::BIGINT,
                    CreateFunctionDataType::BIGINT
                ],
                return_type: CreateFunctionDataType::BIGINT,
                data: "AGFzbQEAAAABGAVgA35".to_string(),
                volatility: CreateFunctionVolatility::Volatile
            }
        )
    }
}
