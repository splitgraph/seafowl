use datafusion::logical_expr::Volatility;

use serde::{Deserialize, Serialize};
use strum_macros::{Display, EnumString};
use wasmtime::ValType;

// WASM to DataFusion conversions
pub fn get_wasm_type(t: &CreateFunctionWASMType) -> ValType {
    match t {
        CreateFunctionWASMType::I32 => ValType::I32,
        CreateFunctionWASMType::I64 => ValType::I64,
        CreateFunctionWASMType::F32 => ValType::F32,
        CreateFunctionWASMType::F64 => ValType::F64,
    }
}

pub fn get_volatility(t: &CreateFunctionVolatility) -> Volatility {
    match t {
        CreateFunctionVolatility::Immutable => Volatility::Immutable,
        CreateFunctionVolatility::Stable => Volatility::Stable,
        CreateFunctionVolatility::Volatile => Volatility::Volatile,
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, EnumString, Display)]
#[serde(rename_all = "camelCase")]
pub enum CreateFunctionWASMType {
    I32,
    I64,
    F32,
    F64,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, EnumString, Display)]
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

#[derive(Serialize, Deserialize, Debug, PartialEq, EnumString, Display)]
#[serde(rename_all = "camelCase")]
pub enum CreateFunctionLanguage {
    Wasm,
}
impl Default for CreateFunctionLanguage {
    fn default() -> Self {
        CreateFunctionLanguage::Wasm
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct CreateFunctionDetails {
    pub entrypoint: String,
    #[serde(default)]
    pub language: CreateFunctionLanguage,
    pub input_types: Vec<CreateFunctionWASMType>,
    pub return_type: CreateFunctionWASMType,
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
            "input_types": ["i64", "i64", "i64"],
            "return_type": "i64",
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
                    CreateFunctionWASMType::I64,
                    CreateFunctionWASMType::I64,
                    CreateFunctionWASMType::I64
                ],
                return_type: CreateFunctionWASMType::I64,
                data: "AGFzbQEAAAABGAVgA35".to_string(),
                volatility: CreateFunctionVolatility::Volatile
            }
        )
    }
}
