use datafusion::logical_expr::Volatility;

use serde::{Deserialize, Serialize};
use strum_macros::{Display, EnumString};
use wasmtime::ValType;

// WASM to DataFusion conversions
pub fn get_wasm_type(t: &CreateFunctionDataType) -> ValType {
    match t {
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
pub enum CreateFunctionDataType {
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
}
impl Default for CreateFunctionLanguage {
    fn default() -> Self {
        CreateFunctionLanguage::Wasm
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct CreateFunctionDetails {
    pub entrypoint: String,
    #[serde(default)]
    pub language: CreateFunctionLanguage,
    pub input_types: Vec<CreateFunctionDataType>,
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
