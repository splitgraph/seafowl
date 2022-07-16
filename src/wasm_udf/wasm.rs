/// Creating DataFusion UDFs from WASM bytecode
use datafusion::{
    arrow::{
        array::{ArrayRef, Float32Array, Float64Array, Int32Array, Int64Array},
        datatypes::DataType,
    },
    common::DataFusionError,
    logical_expr::{ScalarFunctionImplementation, ScalarUDF, Volatility},
};

use datafusion::prelude::*;
use datafusion::{error::Result, physical_plan::functions::make_scalar_function};

use std::sync::Arc;
use wasmtime::{Instance, Module, Store, Val, ValType};

fn wasm_type_to_arrow_type(t: &ValType) -> Result<DataType> {
    match t {
        ValType::I32 => Ok(DataType::Int32),
        ValType::I64 => Ok(DataType::Int64),
        ValType::F32 => Ok(DataType::Float32),
        ValType::F64 => Ok(DataType::Float64),
        _ => Err(DataFusionError::Internal(format!(
            "Unsupported WASM type: {:?}",
            t
        ))),
    }
}

/// Build a DataFusion scalar function from WASM module bytecode.
/// Don't call this function directly; call create_udf_from_wasm instead
/// (as this function doesn't do some validation)
fn make_scalar_function_from_wasm(
    module_bytes: &[u8],
    function_name: &str,
    input_types: Vec<ValType>,
    return_type: ValType,
) -> Result<ScalarFunctionImplementation> {
    let mut store = Store::<()>::default();
    let module = Module::from_binary(store.engine(), module_bytes)
        .map_err(|e| DataFusionError::Internal(format!("Error loading module: {:?}", e)))?;

    // Pre-flight checks to make sure the function exists
    let instance = Instance::new(&mut store, &module, &[])
        .map_err(|e| DataFusionError::Internal(format!("Error instantiating module: {:?}", e)))?;

    let _func = instance
        .get_func(&mut store, function_name)
        .ok_or_else(|| {
            DataFusionError::Internal(format!("Error loading function {:?}", function_name))
        })?;

    // This function has to be of type Fn instead of FnMut. The function invocation (func.call)
    // needs a mutable context, which forces this closure to be FnMut.
    // This means we have to create a store and load the function inside of this closure, discarding
    // the store after we're done.

    // Capture the function name and the module code
    let function_name = function_name.to_owned();
    let module_bytes = module_bytes.to_owned();
    let inner = move |args: &[ArrayRef]| {
        // Load the function again
        let mut store = Store::<()>::default();

        let module = Module::from_binary(store.engine(), &module_bytes)
            .map_err(|e| DataFusionError::Internal(format!("Error loading module: {:?}", e)))?;

        let instance = Instance::new(&mut store, &module, &[]).map_err(|e| {
            DataFusionError::Internal(format!("Error instantiating module: {:?}", e))
        })?;

        let func = instance
            .get_func(&mut store, &function_name)
            .ok_or_else(|| {
                DataFusionError::Internal(format!("Error loading function {:?}", function_name))
            })?;

        // this is guaranteed by DataFusion based on the function's signature.
        assert_eq!(args.len(), input_types.len());

        // Length of the vectorized array
        let array_len = args.get(0).unwrap().len();

        // Buffer for the results
        let mut results: Vec<Val> = Vec::new();
        results.resize(array_len, Val::null());

        for i in 0..array_len {
            let mut params: Vec<Val> = Vec::with_capacity(args.len());
            // Build a slice of WASM Val values to pass to the function
            for j in 0..args.len() {
                let wasm_val = match input_types.get(j).unwrap() {
                    ValType::I32 => Val::I32(
                        args.get(j)
                            .unwrap()
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .expect("cast failed")
                            .value(i),
                    ),
                    ValType::I64 => Val::I64(
                        args.get(j)
                            .unwrap()
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .expect("cast failed")
                            .value(i),
                    ),
                    ValType::F32 => Val::F32(
                        args.get(j)
                            .unwrap()
                            .as_any()
                            .downcast_ref::<Float32Array>()
                            .expect("cast failed")
                            .value(i)
                            .to_bits(),
                    ),
                    ValType::F64 => Val::F64(
                        args.get(j)
                            .unwrap()
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .expect("cast failed")
                            .value(i)
                            .to_bits(),
                    ),
                    _ => panic!("unexpected type"),
                };
                params.push(wasm_val);
            }

            // Get the function to write its output to a slice of the results' buffer
            func.call(&mut store, &params, &mut results[i..i + 1])
                .map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Error executing function {:?}: {:?}",
                        function_name, e
                    ))
                })?;
        }

        // Convert the results back into Arrow (Arc<dyn Array>)
        // These functions panic on type mismatches, which shouldn't happen because
        // we pre-validated them above (unless the function returns a different type
        // than it advertised)
        let array = match return_type {
            ValType::I32 => Arc::new(
                results
                    .iter()
                    .map(|r| r.unwrap_i32())
                    .collect::<Int32Array>(),
            ) as ArrayRef,
            ValType::I64 => Arc::new(
                results
                    .iter()
                    .map(|r| r.unwrap_i64())
                    .collect::<Int64Array>(),
            ) as ArrayRef,
            ValType::F32 => Arc::new(
                results
                    .iter()
                    .map(|r| r.unwrap_f32())
                    .collect::<Float32Array>(),
            ) as ArrayRef,
            ValType::F64 => Arc::new(
                results
                    .iter()
                    .map(|r| r.unwrap_f64())
                    .collect::<Float64Array>(),
            ) as ArrayRef,
            _ => panic!("unexpected type"),
        };
        Ok(array)
    };

    Ok(make_scalar_function(inner))
}

pub fn create_udf_from_wasm(
    name: &str,
    module_bytes: &[u8],
    function_name: &str,
    input_types: Vec<ValType>,
    return_type: ValType,
    volatility: Volatility,
) -> Result<ScalarUDF> {
    // Convert input/output types. We only support the basic {I,F}{32,64} and not function references / V128
    let df_input_types = input_types
        .iter()
        .map(wasm_type_to_arrow_type)
        .collect::<Result<_>>()?;
    let df_return_type = Arc::new(wasm_type_to_arrow_type(&return_type)?);

    let function =
        make_scalar_function_from_wasm(module_bytes, function_name, input_types, return_type)?;

    Ok(create_udf(
        name,
        df_input_types,
        df_return_type,
        volatility,
        function,
    ))
}

#[cfg(test)]
mod tests {
    use hex::decode;

    use super::*;
    use datafusion::assert_batches_eq;

    #[tokio::test]
    async fn test_wasi_math() {
        // Source: https://gist.github.com/going-digital/02e46c44d89237c07bc99cd440ebfa43
        let bytes = decode(
            "\
0061736d01000000010d0260017d017d60037d7f7f017d03050400000001\
0504010144440718030673696e746175000004657870320001046c6f6732\
00020a8e01042901027d430000003f2202200020008e9322002002938b22\
01932001964100411810032002200093980b1900200020008e2200934118\
412c1003bc2000a84117746abe0b2501017f2000bc220141177641ff006b\
b22001410974b3430000804f95412c41c4001003920b2201017d03402003\
20009420012a0200922103200141046a220120026b0d000b20030b0b4a01\
0041000b443fc76142d9e013414baa2ac273b2a63d4001c9407e95d0366f\
f95f3c90f2533d2267773eac66313f1d00803ff725303d03fd3fbe17a6d1\
3e4cdc34bfd382b83ffc888a37006a046e616d65011f04000673696e7461\
7501046578703202046c6f673203086576616c706f6c7902370400030001\
7801027831020468616c6601010001780202000178010278690304000178\
010573746172740203656e640306726573756c74030901030100046c6f6f\
70",
        )
        .unwrap();

        // Create a table with some floating point values
        let mut ctx = SessionContext::new();
        ctx.sql(
            "CREATE TABLE real_values AS
            -- Cast the values to REAL (Float32) since the function only supports that
            SELECT CAST(v1 AS REAL) AS v1, CAST(v2 AS REAL) AS v2
            FROM (VALUES (0.1, 0.2), (1.2, 2.3), (3.3, 4.3), (5.4, 2.5), (1234.5, 678.8)) d (v1, v2)",
        )
        .await
        .unwrap();

        // sin(2*pi*x)
        let sintau = create_udf_from_wasm(
            "sintau",
            &bytes,
            "sintau",
            vec![ValType::F32],
            ValType::F32,
            Volatility::Immutable,
        )
        .unwrap();

        // 2^x
        let exp2 = create_udf_from_wasm(
            "exp2",
            &bytes,
            "exp2",
            vec![ValType::F32],
            ValType::F32,
            Volatility::Immutable,
        )
        .unwrap();

        // log2(x)
        let log2 = create_udf_from_wasm(
            "log2",
            &bytes,
            "log2",
            vec![ValType::F32],
            ValType::F32,
            Volatility::Immutable,
        )
        .unwrap();

        ctx.register_udf(sintau);
        ctx.register_udf(exp2);
        ctx.register_udf(log2);

        let results = ctx
            .sql(
                "SELECT
                ROUND(sintau(v1) * 1000) AS sv1,
                ROUND(sintau(v2) * 1000) AS sv2,
                ROUND(exp2(v1) * 1000) AS ev1,
                ROUND(exp2(v2) * 1000) AS ev2,
                ROUND(log2(v1) * 1000) AS lv1,
                ROUND(log2(v2) * 1000) AS lv2
            FROM real_values;",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // Sliiiightly sketchy to do this comparison string-to-string, but it seems to stay stable
        // without floating point divergences. We use ROUND(x*1000) here to simulate rounding to 3
        // decimal places, since the DataFusion's ROUND() simply discards the second argument.
        // https://github.com/apache/arrow-datafusion/issues/2420
        let expected = vec![
            "+-----+------+-------+-------+-------+-------+",
            "| sv1 | sv2  | ev1   | ev2   | lv1   | lv2   |",
            "+-----+------+-------+-------+-------+-------+",
            "| 588 | 951  | 1072  | 1149  | -3322 | -2322 |",
            "| 951 | 951  | 2297  | 4925  | 263   | 1202  |",
            "| 951 | 951  | 9849  | 19698 | 1722  | 2104  |",
            "| 588 | 0    | 42224 | 5657  | 2433  | 1322  |",
            "| 0   | -951 | -0    | -0    | 10270 | 9407  |",
            "+-----+------+-------+-------+-------+-------+",
        ];

        assert_batches_eq!(expected, &results);
    }

    #[tokio::test]
    async fn test_wasi_encryption() {
        // Speck64/128 block cipher
        // Original from https://github.com/madmo/speck/; adapted for WASM
        // in github.com/mildbyte/speck-wasm
        let speck = decode(
            "\
0061736d0100000001180560037e7e7e017e6000017f60000060017f0060\
017f017f0308070200000103040104050170010202050601018002800206\
09017f01419088c0020b079b0109066d656d6f7279020013737065636b5f\
656e63727970745f626c6f636b000113737065636b5f646563727970745f\
626c6f636b0002195f5f696e6469726563745f66756e6374696f6e5f7461\
626c6501000b5f696e697469616c697a650000105f5f6572726e6f5f6c6f\
636174696f6e000609737461636b5361766500030c737461636b52657374\
6f726500040a737461636b416c6c6f6300050907010041010b01000af704\
070300010b870201077f230041206b220324002003200237031820032001\
37031020032000370308230041106b2109200341106a2206280200210820\
03200328020822053602002003200328020c220736020403402004410347\
0440200941046a20044102746a2006200441016a22044102746a28020036\
02000c010b0b200741187720056a20087322062005410377732105410021\
0403402004411a470440200941046a200441ff01714103704102746a2207\
200728020041187720086a20047322073602002007200841037773220820\
0520064118776a7322062005410377732105200441016a21040c010b0b20\
0320053602002003200636020420032903002100200341206a240020000b\
c40201087f230041206b2203240020032002370318200320013703102003\
2000370300230041106b2109200341106a22072802002106200320032802\
00220a36020820032003280204220836020c034020044103460440034020\
05411a4604404100210503402005411b470440200941046a411920056b22\
0441187441187541036f4102746a22072004200728020022077320062007\
73411d7722046b41087736020020062008732008200a73411d77220a6b41\
08772108200541016a2105200421060c010b0b2003200a36020820032008\
36020c05200941046a200541ff01714103704102746a2204200428020041\
187720066a200573220436020020042006410377732106200541016a2105\
0c010b0b05200941046a20044102746a2007200441016a22044102746a28\
02003602000c010b0b20032903082100200341206a240020000b04002300\
0b0600200024000b1000230020006b4170712200240020000b0500418008\
0b",
        )
        .unwrap();

        let mut ctx = SessionContext::new();
        ctx.sql(
            "CREATE TABLE int64_values AS
            SELECT *
            FROM (VALUES (123456), (7891011), (12131415), (16171819), (-20212223)) d (v)",
        )
        .await
        .unwrap();

        // speck_encrypt_block(plaintext_block, key_msb, key_lsb)
        let speck_encrypt_block = create_udf_from_wasm(
            "speck_encrypt_block",
            &speck,
            "speck_encrypt_block",
            vec![ValType::I64, ValType::I64, ValType::I64],
            ValType::I64,
            Volatility::Immutable,
        )
        .unwrap();

        // speck_decrypt_block(ciphertext_block, key_msb, key_lsb)
        let speck_decrypt_block = create_udf_from_wasm(
            "speck_decrypt_block",
            &speck,
            "speck_decrypt_block",
            vec![ValType::I64, ValType::I64, ValType::I64],
            ValType::I64,
            Volatility::Immutable,
        )
        .unwrap();
        ctx.register_udf(speck_encrypt_block);
        ctx.register_udf(speck_decrypt_block);

        let results = ctx
        .sql(
            "WITH encrypted AS (
                SELECT
                    v,
                    speck_encrypt_block(CAST(v AS BIGINT), 4522913144885835612, -7379163842329862484) AS encrypted
                FROM int64_values
            ) SELECT
                v,
                encrypted,
                speck_decrypt_block(encrypted, 4522913144885835612, -7379163842329862484) AS decrypted
            FROM encrypted;",
        )
        .await
        .unwrap().collect().await.unwrap();

        let expected = vec![
            "+-----------+---------------------+-----------+",
            "| v         | encrypted           | decrypted |",
            "+-----------+---------------------+-----------+",
            "| 123456    | 5661533298546550503 | 123456    |",
            "| 7891011   | 7528692995910408077 | 7891011   |",
            "| 12131415  | 4835612303979161413 | 12131415  |",
            "| 16171819  | 8992269262659013344 | 16171819  |",
            "| -20212223 | 5068206001593455086 | -20212223 |",
            "+-----------+---------------------+-----------+",
        ];

        assert_batches_eq!(expected, &results);
    }
}
