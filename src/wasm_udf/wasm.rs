use arrow::{
    array::{BooleanArray, PrimitiveArray, StringArray},
    datatypes::ArrowPrimitiveType,
};
/// Creating DataFusion UDFs from WASM bytecode
use datafusion::{
    arrow::{
        array::{ArrayRef, Float32Array, Float64Array, Int32Array, Int64Array},
        datatypes::{DataType, TimeUnit},
    },
    common::DataFusionError,
    logical_expr::{ScalarFunctionImplementation, ScalarUDF, Volatility},
};

use datafusion::prelude::*;
use datafusion::{error::Result, physical_plan::functions::make_scalar_function};

use wasmtime::{Engine, Instance, Memory, Module, Store, TypedFunc, Val, ValType};

use super::data_types::{get_wasm_type, CreateFunctionDataType, CreateFunctionLanguage};

use wasi_common::WasiCtx;
use wasmtime_wasi::sync::WasiCtxBuilder;

use std::sync::Arc;
use std::vec;

use rmp_serde;
use rmp_serde::Serializer;
use serde;

use serde::Serialize;

use rmpv::Value;

const SIZE_BYTE_COUNT: usize = std::mem::size_of::<i32>();

fn sql_type_to_arrow_type(t: &CreateFunctionDataType) -> Result<DataType> {
    match t {
        // legacy WASM-native type names
        CreateFunctionDataType::I32 => Ok(DataType::Int32),
        CreateFunctionDataType::I64 => Ok(DataType::Int64),
        CreateFunctionDataType::F32 => Ok(DataType::Float32),
        CreateFunctionDataType::F64 => Ok(DataType::Float64),
        // DDL type names
        CreateFunctionDataType::SMALLINT => Ok(DataType::Int16),
        CreateFunctionDataType::INT => Ok(DataType::Int32),
        CreateFunctionDataType::BIGINT => Ok(DataType::Int64),
        CreateFunctionDataType::CHAR => Ok(DataType::Utf8),
        CreateFunctionDataType::VARCHAR => Ok(DataType::Utf8),
        CreateFunctionDataType::TEXT => Ok(DataType::Utf8),
        CreateFunctionDataType::DECIMAL { precision, scale } => {
            Ok(DataType::Decimal128(*precision, *scale))
        }
        CreateFunctionDataType::FLOAT => Ok(DataType::Float32),
        CreateFunctionDataType::REAL => Ok(DataType::Float32),
        CreateFunctionDataType::DOUBLE => Ok(DataType::Float64),
        CreateFunctionDataType::BOOLEAN => Ok(DataType::Boolean),
        CreateFunctionDataType::DATE => Ok(DataType::Date32),
        CreateFunctionDataType::TIMESTAMP => {
            Ok(DataType::Timestamp(TimeUnit::Nanosecond, None))
        }
    }
}

fn get_wasm_module_exported_fn<Params, Results>(
    instance: &Instance,
    store: &mut Store<WasiCtx>,
    export_name: &str,
) -> Result<TypedFunc<Params, Results>>
where
    Params: wasmtime::WasmParams,
    Results: wasmtime::WasmResults,
{
    instance
        .get_typed_func::<Params, Results, _>(store, export_name)
        .map_err(|err| {
            DataFusionError::Internal(
                format!("Required export '{export_name:?}' could not be located in WASM module exports: {err:?}"))
        })
}

struct WasmMessagePackUDFInstance {
    store: Store<WasiCtx>,
    alloc: TypedFunc<i32, i32>,
    dealloc: TypedFunc<(i32, i32), ()>,
    udf: TypedFunc<i32, i32>,
    memory: Memory,
}

impl WasmMessagePackUDFInstance {
    pub fn new(module_bytes: &[u8], function_name: &str) -> Result<Self> {
        let engine = Engine::default();
        let mut linker = wasmtime::Linker::new(&engine);
        // Create a WASI context and put it in a Store; all instances in the store
        // share this context. `WasiCtxBuilder` provides a number of ways to
        // configure what the target program will have access to.
        let wasi = WasiCtxBuilder::new().inherit_stderr().build();
        let mut store = Store::new(&engine, wasi);
        // Add both wasi_unstable and wasi_snapshot_preview1 WASI modules
        wasmtime_wasi::add_to_linker(&mut linker, |s| s).map_err(|e| {
            DataFusionError::Internal(format!("Error linking to WASI modules: {e:?}"))
        })?;
        // Instantiate WASM module.
        let module = Module::from_binary(&engine, module_bytes).map_err(|e| {
            DataFusionError::Internal(format!("Error loading WASM module: {e:?}"))
        })?;
        let instance = linker.instantiate(&mut store, &module).map_err(|e| {
            DataFusionError::Internal(format!("Error instantiating WASM modules {e:?}"))
        })?;

        let alloc = get_wasm_module_exported_fn(&instance, &mut store, "alloc")?;
        let dealloc = get_wasm_module_exported_fn(&instance, &mut store, "dealloc")?;
        let udf = get_wasm_module_exported_fn(&instance, &mut store, function_name)?;
        let memory = instance.get_memory(&mut store, "memory").ok_or(
            DataFusionError::Internal(
                "could not find module's exported memory".to_string(),
            ),
        )?;
        Ok(Self {
            store,
            alloc,
            dealloc,
            udf,
            memory,
        })
    }

    fn read_udf_output(&mut self, udf_output_ptr: i32) -> Result<(Value, i32)> {
        let ptr: usize = udf_output_ptr.try_into().unwrap();
        let mut size_buffer = [0u8; SIZE_BYTE_COUNT];
        self.memory
            .read(&self.store, ptr, &mut size_buffer)
            .map_err(|err| {
                DataFusionError::Internal(format!(
                    "Error reading UDF output buffer size: {err:?}"
                ))
            })?;
        let size: usize = i32::from_ne_bytes(size_buffer).try_into().map_err(|e| {
            DataFusionError::Internal(format!(
                "Error interpreting output buffer size as i32: {e:?}"
            ))
        })?;
        let mut output_buffer = vec![0_u8; size];
        self.memory
            .read(
                &self.store,
                ptr + SIZE_BYTE_COUNT,
                output_buffer.as_mut_slice(),
            )
            .map_err(|err| {
                DataFusionError::Internal(format!(
                    "Error reading output buf ({size:?} bytes): {err:?}"
                ))
            })?;
        let output: Value =
            rmp_serde::from_slice(output_buffer.as_ref()).map_err(|err| {
                DataFusionError::Internal(format!(
                    "Error messagepack decoding output buffer: {err:?}"
                ))
            })?;
        // return the entire size of the output buffer (including i32 size prefix) so it can be passed to dealloc() later
        let result: (Value, i32) = (output, (size + SIZE_BYTE_COUNT).try_into().unwrap());
        Ok(result)
    }

    fn write_udf_input(&mut self, input: &Value) -> Result<(i32, i32)> {
        // serialize input using MessagePack
        let mut udf_input_buf: Vec<u8> = vec![];
        input
            .serialize(&mut Serializer::new(&mut udf_input_buf))
            .map_err(|err| {
                DataFusionError::Internal(format!(
                    "Error messagepack serializing input {err:?}"
                ))
            })?;
        // Total input size will be serialized messagepack bytes prepended by the
        // size of the serialized input (one i32 == 4 bytes)
        let udf_input_size: usize = udf_input_buf.len() + SIZE_BYTE_COUNT;
        // allocate WASM memory for input buffer
        let udf_input_ptr = self
            .alloc
            .call(&mut self.store, udf_input_size.try_into().unwrap())
            .map_err(|e| {
                DataFusionError::Internal(format!(
                    "Error allocating input buffer in WASM memory: {e:?}"
                ))
            })?;
        let ptr: usize = udf_input_ptr.try_into().unwrap();
        // write size of input buffer first
        self.memory
            .write(&mut self.store, ptr, &udf_input_buf.len().to_ne_bytes())
            .map_err(|err| {
                DataFusionError::Internal(format!(
                    "Error copying input buffer size to WASM memory: {err:?}"
                ))
            })?;
        // copy input buffer
        self.memory
            .write(
                &mut self.store,
                ptr + SIZE_BYTE_COUNT,
                udf_input_buf.as_ref(),
            )
            .map_err(|err| {
                DataFusionError::Internal(format!(
                    "Error copying input buffer to WASM memory: {err:?}"
                ))
            })?;
        // return the entire size of the output buffer (including i32 size prefix) so it can be passed to dealloc() later
        Ok((udf_input_ptr, udf_input_size.try_into().unwrap()))
    }

    pub fn call(&mut self, input: Vec<Value>) -> Result<Value> {
        let args_array = Value::Array(input);
        let (udf_input_ptr, input_size) = self.write_udf_input(&args_array)?;
        // invoke UDF
        let udf_output_ptr =
            self.udf.call(&mut self.store, udf_input_ptr).map_err(|e| {
                DataFusionError::Internal(format!("Error invoking WASM UDF: {e:?}"))
            })?;
        let (output, output_size) = self.read_udf_output(udf_output_ptr)?;
        // deallocate both input and output buffers
        self.dealloc
            .call(&mut self.store, (udf_input_ptr, input_size))
            .map_err(|e| {
                DataFusionError::Internal(format!(
                    "Error deallocating input buffer: {e:?}"
                ))
            })?;
        self.dealloc
            .call(&mut self.store, (udf_output_ptr, output_size))
            .map_err(|e| {
                DataFusionError::Internal(format!(
                    "Error deallocating output buffer: {e:?}"
                ))
            })?;
        Ok(output)
    }
}

fn get_arrow_value<T>(
    args: &[ArrayRef],
    row_ix: usize,
    col_ix: usize,
) -> Result<T::Native>
where
    T: ArrowPrimitiveType,
{
    args.get(col_ix)
        .unwrap()
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .ok_or_else(|| {
            DataFusionError::Internal(format!(
                "Error casting column {col_ix:?} to array of primitive values"
            ))
        })
        .map(|arr| arr.value(row_ix))
}

fn messagepack_encode_input_value(
    value_type: &CreateFunctionDataType,
    args: &[ArrayRef],
    row_ix: usize,
    col_ix: usize,
) -> Result<Value> {
    match value_type {
        CreateFunctionDataType::SMALLINT => {
            get_arrow_value::<arrow::datatypes::Int16Type>(args, row_ix, col_ix)
                .map(Value::from)
        }
        CreateFunctionDataType::I32 | CreateFunctionDataType::INT => {
            get_arrow_value::<arrow::datatypes::Int32Type>(args, row_ix, col_ix)
                .map(Value::from)
        }
        CreateFunctionDataType::I64 | CreateFunctionDataType::BIGINT => {
            get_arrow_value::<arrow::datatypes::Int64Type>(args, row_ix, col_ix)
                .map(Value::from)
        }
        CreateFunctionDataType::F32
        | CreateFunctionDataType::FLOAT
        | CreateFunctionDataType::REAL => {
            get_arrow_value::<arrow::datatypes::Float32Type>(args, row_ix, col_ix)
                .map(Value::from)
        }
        CreateFunctionDataType::F64 | CreateFunctionDataType::DOUBLE => {
            get_arrow_value::<arrow::datatypes::Float64Type>(args, row_ix, col_ix)
                .map(Value::from)
        }
        CreateFunctionDataType::TEXT
        | CreateFunctionDataType::CHAR
        | CreateFunctionDataType::VARCHAR => match args
            .get(col_ix)
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>(
        ) {
            Some(arr) => Ok(Value::from(arr.value(row_ix))),
            None => Err(DataFusionError::Internal(format!(
                "Error casting column {col_ix:?} to string array"
            ))),
        },
        CreateFunctionDataType::BOOLEAN => match args
            .get(col_ix)
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>(
        ) {
            Some(arr) => Ok(Value::from(arr.value(row_ix))),
            None => Err(DataFusionError::Internal(format!(
                "Error casting column {col_ix:?} to boolean array"
            ))),
        },
        // from: https://github.com/apache/arrow/blob/02c8598d264c839a5b5cf3109bfd406f3b8a6ba5/cpp/src/arrow/type.h#L824
        // Arrow decimals are fixed-point decimal numbers encoded as a scaled
        // integer.  The precision is the number of significant digits that the
        // decimal type can represent; the scale is the number of digits after
        // the decimal point (note the scale can be negative).
        //
        // As an example, `Decimal128Type(7, 3)` can exactly represent the numbers
        // 1234.567 and -1234.567 (encoded internally as the 128-bit integers
        // 1234567 and -1234567, respectively), but neither 12345.67 nor 123.4567.
        //
        // Decimal128Type has a maximum precision of 38 significant digits
        // (also available as Decimal128Type::kMaxPrecision).
        // If higher precision is needed, consider using Decimal256Type.
        CreateFunctionDataType::DECIMAL {
            precision: p,
            scale: s,
        } => get_arrow_value::<arrow::datatypes::Decimal128Type>(args, row_ix, col_ix)
            .map(|val| {
                let low: i64 = val as i64;
                let high: i64 = (val >> 64) as i64;

                Value::Array(vec![
                    Value::from(*p),
                    Value::from(*s),
                    Value::from(high),
                    Value::from(low),
                ])
            }),
        // dates are represented as i32 integer internally, serialize them as such.
        CreateFunctionDataType::DATE => {
            get_arrow_value::<arrow::datatypes::Date32Type>(args, row_ix, col_ix)
                .map(Value::from)
        }
        // timestamps are represented as i64 integers internally, serialize them as such.
        CreateFunctionDataType::TIMESTAMP => get_arrow_value::<
            arrow::datatypes::TimestampNanosecondType,
        >(args, row_ix, col_ix)
        .map(Value::from),
    }
}

fn decode_udf_result_primitive_array<T>(
    encoded_results: &[Value],
    decoder: &dyn Fn(&Value) -> Result<T::Native>,
) -> Result<ArrayRef>
where
    T: ArrowPrimitiveType,
{
    encoded_results
        .iter()
        .map(|i| Some(decoder(i)).transpose())
        .collect::<Result<PrimitiveArray<T>>>()
        .map(|a| Arc::new(a) as ArrayRef)
}

fn messagepack_decode_results(
    return_type: &CreateFunctionDataType,
    encoded_results: &[Value],
) -> Result<ArrayRef> {
    match return_type {
        CreateFunctionDataType::SMALLINT => decode_udf_result_primitive_array::<
            arrow::datatypes::Int16Type,
        >(encoded_results, &|v| {
            v.as_i64()
                .ok_or(DataFusionError::Internal(format!(
                    "Expected to find i64 value, but received {v:?} instead"
                )))
                .and_then(|v_i64| {
                    i16::try_from(v_i64).map_err(|e| {
                        DataFusionError::Internal(format!(
                            "Error converting i64 to i16: {e:?}"
                        ))
                    })
                })
        }),
        CreateFunctionDataType::I32 | CreateFunctionDataType::INT => {
            decode_udf_result_primitive_array::<arrow::datatypes::Int32Type>(
                encoded_results,
                &|v| {
                    v.as_i64()
                        .ok_or(DataFusionError::Internal(format!(
                            "Expected to find i64 value, but received {v:?} instead"
                        )))
                        .and_then(|v_i64| {
                            i32::try_from(v_i64).map_err(|e| {
                                DataFusionError::Internal(format!(
                                    "Error converting i64 to i32: {e:?}"
                                ))
                            })
                        })
                },
            )
        }
        CreateFunctionDataType::I64 | CreateFunctionDataType::BIGINT => {
            decode_udf_result_primitive_array::<arrow::datatypes::Int64Type>(
                encoded_results,
                &|v| {
                    v.as_i64().ok_or(DataFusionError::Internal(format!(
                        "Expected to find i64 value, but received {v:?} instead"
                    )))
                },
            )
        }
        CreateFunctionDataType::CHAR
        | CreateFunctionDataType::VARCHAR
        | CreateFunctionDataType::TEXT => encoded_results
            .iter()
            .map(|i| {
                Some(i.as_str().ok_or(DataFusionError::Internal(format!(
                    "Expected to find string value, received {:?} instead",
                    &i
                ))))
                .transpose()
            })
            .collect::<Result<StringArray>>()
            .map(|a| Arc::new(a) as ArrayRef),
        CreateFunctionDataType::DATE => decode_udf_result_primitive_array::<
            arrow::datatypes::Date32Type,
        >(encoded_results, &|v| {
            v.as_i64()
                .ok_or(DataFusionError::Internal(format!(
                    "Expected to find i64 value, but received {v:?} instead"
                )))
                .and_then(|v_i64| {
                    i32::try_from(v_i64).map_err(|e| {
                        DataFusionError::Internal(format!(
                            "Error converting i64 to i32 (for date): {e:?}"
                        ))
                    })
                })
        }),
        CreateFunctionDataType::TIMESTAMP => decode_udf_result_primitive_array::<
            arrow::datatypes::TimestampNanosecondType,
        >(encoded_results, &|v| {
            v.as_i64().ok_or(DataFusionError::Internal(format!(
                "Expected to find i64 value, but received {v:?} instead"
            )))
        }),
        CreateFunctionDataType::BOOLEAN => encoded_results
            .iter()
            .map(|i| {
                Some(i.as_bool().ok_or(DataFusionError::Internal(format!(
                    "Expected to find string value, received {i:?} instead"
                ))))
                .transpose()
            })
            .collect::<Result<BooleanArray>>()
            .map(|a| Arc::new(a) as ArrayRef),

        CreateFunctionDataType::F64 | CreateFunctionDataType::DOUBLE => {
            decode_udf_result_primitive_array::<arrow::datatypes::Float64Type>(
                encoded_results,
                &|v| {
                    v.as_f64().ok_or(DataFusionError::Internal(format!(
                        "Expected to find f64 value, but received {v:?} instead"
                    )))
                },
            )
        }

        CreateFunctionDataType::F32
        | CreateFunctionDataType::REAL
        | CreateFunctionDataType::FLOAT => decode_udf_result_primitive_array::<
            arrow::datatypes::Float32Type,
        >(encoded_results, &|v| match v {
            Value::F32(n) => Ok(*n),
            _ => Err(DataFusionError::Internal(format!(
                "Expected to find f32 value, but received {v:?} instead"
            ))),
        })
        .map(|a| Arc::new(a) as ArrayRef),
        CreateFunctionDataType::DECIMAL {
            precision: p,
            scale: s,
        } => encoded_results
            .iter()
            .map(|i| {
                Some(
                    i.as_array()
                        .ok_or(DataFusionError::Internal(format!(
                            "Expected to find array containing decimal parts, received {i:?} instead"
                        )))
                        .and_then(|decimal_array| {
                            if decimal_array.len() != 4 {
                                return Err(DataFusionError::Internal(format!("DECIMAL UDF result array should have 4 elements, found {:?} instead.", decimal_array.len())));
                            }
                            decimal_array[0].as_u64()
                                .ok_or(DataFusionError::Internal(format!("Decimal precision expected to be integer, found {:?} instead", decimal_array[0])))
                                .and_then(|p_u64| {
                                    let p_u8:u8 = p_u64.try_into().map_err(|err| DataFusionError::Internal(format!("Couldn't convert 64-bit precision value {p_u64:?} to u8 {err:?}")))?;
                                    if p_u8 != *p {
                                        return Err(DataFusionError::Internal(format!("Expected to receive a decimal with precision {:?}, got {:?} instead.", *p, p_u8)))
                                    }
                                    Ok(p_u8)
                                })?;
                            decimal_array[1].as_u64()
                                .ok_or(DataFusionError::Internal(format!("Decimal scale expected to be integer, found {:?} instead", decimal_array[1])))
                                .and_then(|s_u64| {
                                    let s_i8: i8 = s_u64.try_into().map_err(|err| DataFusionError::Internal(format!("Couldn't convert 64-bit scale value {s_u64:?} to i8 {err:?}")))?;
                                    if s_i8 != *s {
                                        return Err(DataFusionError::Internal(format!("Expected to receive a decimal with scale {:?}, got {:?} instead.", *s, s_i8)))
                                    }
                                    Ok(s_i8)
                                })?;
                            let high = decimal_array[2].as_i64()
                                .ok_or(DataFusionError::Internal(format!("Decimal value high half expected to be integer, found {:?} instead", decimal_array[2])))?;
                            let low = decimal_array[3].as_i64()
                                .ok_or(DataFusionError::Internal(format!("Decimal value low half expected to be integer, found {:?} instead", decimal_array[3])))?;
                            let value:i128 = (low as i128) + ((high as i128) << 64);
                            Ok(value)
                        }),
                )
                .transpose()
            })
            .collect::<Result<arrow::array::Decimal128Array>>()
            .and_then(|a| {
                let arr = (a as arrow::array::Decimal128Array)
                    .with_precision_and_scale(*p, *s)
                    .map_err(|e| DataFusionError::Internal(format!("Error setting precision {:?} scale {:?} for decimal result: {:?}", *p, *s, e)))?;
                Ok(Arc::new(arr) as ArrayRef)
            }),
    }
}

fn make_scalar_function_wasm_messagepack(
    module_bytes: &[u8],
    function_name: &str,
    input_types: Vec<CreateFunctionDataType>,
    return_type: CreateFunctionDataType,
) -> Result<ScalarFunctionImplementation> {
    // Similar to make_scalar_function_from_wasm, this function should verify
    // that the module can be loaded and the UDF export is found before
    // returning a Result.
    let function_name = function_name.to_owned();
    let module_bytes = module_bytes.to_owned();
    let _outer_instance = WasmMessagePackUDFInstance::new(&module_bytes, &function_name)
        .map_err(|err| {
            DataFusionError::Internal(format!(
                "Error initializing WASM + MessagePack UDF {function_name:?}: {err:?}"
            ))
        })?;
    let inner = move |args: &[ArrayRef]| {
        let mut instance = WasmMessagePackUDFInstance::new(&module_bytes, &function_name)
            .map_err(|err| {
                DataFusionError::Internal(format!(
                    "Error initializing WASM + MessagePack UDF {function_name:?}: {err:?}"
                ))
            })?;
        // this is guaranteed by DataFusion based on the function's signature.
        if args.len() != input_types.len() {
            return Err(DataFusionError::Internal(format!(
                "Wrong number of arguments for function {:?}: expected {:?}, received {:?}",
                function_name, input_types.len(), args.len()
            )));
        }

        // Length of the vectorized array
        let array_len = args.get(0).unwrap().len();

        // Buffer for the results
        let mut encoded_results: Vec<Value> = Vec::with_capacity(array_len);

        for row_ix in 0..array_len {
            let mut params: Vec<Value> = Vec::with_capacity(args.len());
            // Build a slice of MessagePack Values which will be serialized
            // as an array.
            for col_ix in 0..args.len() {
                params.push(messagepack_encode_input_value(
                    input_types.get(col_ix).unwrap(),
                    args,
                    row_ix,
                    col_ix,
                )?);
            }

            encoded_results.push(instance.call(params).map_err(|err| {
                DataFusionError::Internal(format!(
                    "Error invoking function {function_name:?}: {err:?}"
                ))
            })?);
        }

        messagepack_decode_results(&return_type, &encoded_results)
    };

    Ok(make_scalar_function(inner))
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
        .map_err(|e| DataFusionError::Internal(format!("Error loading module: {e:?}")))?;

    // Pre-flight checks to make sure the function exists
    let instance = Instance::new(&mut store, &module, &[]).map_err(|e| {
        DataFusionError::Internal(format!("Error instantiating module: {e:?}"))
    })?;

    let _func = instance
        .get_func(&mut store, function_name)
        .ok_or_else(|| {
            DataFusionError::Internal(format!("Error loading function {function_name:?}"))
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

        let module = Module::from_binary(store.engine(), &module_bytes).map_err(|e| {
            DataFusionError::Internal(format!("Error loading module: {e:?}"))
        })?;

        let instance = Instance::new(&mut store, &module, &[]).map_err(|e| {
            DataFusionError::Internal(format!("Error instantiating module: {e:?}"))
        })?;

        let func = instance
            .get_func(&mut store, &function_name)
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "Error loading function {function_name:?}"
                ))
            })?;

        // this is guaranteed by DataFusion based on the function's signature.
        assert_eq!(args.len(), input_types.len());

        // Length of the vectorized array
        let array_len = args.get(0).unwrap().len();

        // Buffer for the results
        let mut results: Vec<Val> = Vec::new();
        results.resize(array_len, Val::null());

        for row_ix in 0..array_len {
            let mut params: Vec<Val> = Vec::with_capacity(args.len());
            // Build a slice of WASM Val values to pass to the function
            for col_ix in 0..args.len() {
                let wasm_val = match input_types.get(col_ix).unwrap() {
                    ValType::I32 => Val::I32(get_arrow_value::<
                        arrow::datatypes::Int32Type,
                    >(args, row_ix, col_ix)?),
                    ValType::I64 => Val::I64(get_arrow_value::<
                        arrow::datatypes::Int64Type,
                    >(args, row_ix, col_ix)?),
                    ValType::F32 => Val::F32(
                        get_arrow_value::<arrow::datatypes::Float32Type>(
                            args, row_ix, col_ix,
                        )?
                        .to_bits(),
                    ),
                    ValType::F64 => Val::F64(
                        get_arrow_value::<arrow::datatypes::Float64Type>(
                            args, row_ix, col_ix,
                        )?
                        .to_bits(),
                    ),
                    _ => panic!("unexpected type"),
                };
                params.push(wasm_val);
            }

            // Get the function to write its output to a slice of the results' buffer
            func.call(&mut store, &params, &mut results[row_ix..row_ix + 1])
                .map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Error executing function {function_name:?}: {e:?}"
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
    language: &CreateFunctionLanguage,
    name: &str,
    module_bytes: &[u8],
    function_name: &str,
    input_types: &Vec<CreateFunctionDataType>,
    return_type: &CreateFunctionDataType,
    volatility: Volatility,
) -> Result<ScalarUDF> {
    let df_input_types = input_types
        .iter()
        .map(sql_type_to_arrow_type)
        .collect::<Result<_>>()?;
    let df_return_type = Arc::new(sql_type_to_arrow_type(return_type)?);

    let function = match language {
        CreateFunctionLanguage::Wasm => {
            let converted_input_types = input_types
                .iter()
                .map(|t| get_wasm_type(t).unwrap())
                .collect();
            make_scalar_function_from_wasm(
                module_bytes,
                function_name,
                // Convert input/output types. We only support the basic {I,F}{32,64} and not function references / V128
                converted_input_types,
                get_wasm_type(return_type)?,
            )?
        }
        CreateFunctionLanguage::WasmMessagePack => make_scalar_function_wasm_messagepack(
            module_bytes,
            function_name,
            input_types.to_owned(),
            return_type.to_owned(),
        )?,
    };

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
    use rstest::rstest;

    #[tokio::test]
    async fn test_wasm_math() {
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
        let ctx = SessionContext::new();
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
            &CreateFunctionLanguage::Wasm,
            "sintau",
            &bytes,
            "sintau",
            &vec![CreateFunctionDataType::F32],
            &CreateFunctionDataType::F32,
            Volatility::Immutable,
        )
        .unwrap();

        // 2^x
        let exp2 = create_udf_from_wasm(
            &CreateFunctionLanguage::Wasm,
            "exp2",
            &bytes,
            "exp2",
            &vec![CreateFunctionDataType::F32],
            &CreateFunctionDataType::F32,
            Volatility::Immutable,
        )
        .unwrap();

        // log2(x)
        let log2 = create_udf_from_wasm(
            &CreateFunctionLanguage::Wasm,
            "log2",
            &bytes,
            "log2",
            &vec![CreateFunctionDataType::F32],
            &CreateFunctionDataType::F32,
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
    async fn test_wasm_encryption() {
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

        let ctx = SessionContext::new();
        ctx.sql(
            "CREATE TABLE int64_values AS
            SELECT *
            FROM (VALUES (123456), (7891011), (12131415), (16171819), (-20212223)) d (v)",
        )
        .await
        .unwrap();

        // speck_encrypt_block(plaintext_block, key_msb, key_lsb)
        let speck_encrypt_block = create_udf_from_wasm(
            &CreateFunctionLanguage::Wasm,
            "speck_encrypt_block",
            &speck,
            "speck_encrypt_block",
            &vec![
                CreateFunctionDataType::I64,
                CreateFunctionDataType::I64,
                CreateFunctionDataType::I64,
            ],
            &CreateFunctionDataType::I64,
            Volatility::Immutable,
        )
        .unwrap();

        // speck_decrypt_block(ciphertext_block, key_msb, key_lsb)
        let speck_decrypt_block = create_udf_from_wasm(
            &CreateFunctionLanguage::Wasm,
            "speck_decrypt_block",
            &speck,
            "speck_decrypt_block",
            &vec![
                CreateFunctionDataType::I64,
                CreateFunctionDataType::I64,
                CreateFunctionDataType::I64,
            ],
            &CreateFunctionDataType::I64,
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

    use std::io::Read;

    // from: https://www.reddit.com/r/rust/comments/dekpl5/how_to_read_binary_data_from_a_file_into_a_vecu8/
    #[allow(clippy::all)]
    fn get_file_as_byte_vec(filename: &String) -> Vec<u8> {
        let mut f = std::fs::File::open(&filename).expect("no file found");
        let metadata = std::fs::metadata(&filename).expect("unable to read metadata");
        let mut buffer = vec![0; metadata.len() as usize];
        f.read(&mut buffer).expect("buffer overflow");
        buffer
    }

    async fn register_wasm_messagepack_udf(
        name: &str,
        input_types: &Vec<CreateFunctionDataType>,
        return_type: &CreateFunctionDataType,
    ) -> Result<SessionContext> {
        let mut wasm_filename = std::env::var("CARGO_MANIFEST_DIR").unwrap();
        wasm_filename.push_str("/resources/test/messagepack_rust.wasm");
        let wasm_module = get_file_as_byte_vec(&wasm_filename);
        let ctx = SessionContext::new();

        create_udf_from_wasm(
            &CreateFunctionLanguage::WasmMessagePack,
            name,
            &wasm_module,
            name,
            input_types,
            return_type,
            Volatility::Immutable,
        )
        .map(|udf| {
            ctx.register_udf(udf);
            ctx
        })
    }

    #[tokio::test]
    async fn test_wasm_module_missing_export() -> Result<()> {
        let err = register_wasm_messagepack_udf(
            "404_this_function_is_not_exported_by_the_wasm_module",
            &vec![
                CreateFunctionDataType::INT.to_owned(),
                CreateFunctionDataType::INT.to_owned(),
            ],
            &CreateFunctionDataType::INT,
        )
        .await;
        assert!(err.is_err());
        assert!(
            err.err().unwrap().to_string().starts_with(
                "Internal error: Error initializing WASM + MessagePack UDF \"404_this_function_is_not_exported_by_the_wasm_module\": Internal(\"Required export '\\\"404_this_function_is_not_exported_by_the_wasm_module\\\"' could not be located in WASM module exports: failed to find function export"));
        Ok(())
    }

    #[tokio::test]
    async fn test_wasm_messagepack_udf_wasm_trap() {
        let ctx = register_wasm_messagepack_udf(
            "panic_if_negative",
            &vec![CreateFunctionDataType::INT],
            &CreateFunctionDataType::INT,
        )
        .await
        .unwrap();

        let results = ctx
            .sql("select panic_if_negative(CAST(-1 AS INT))")
            .await
            .unwrap()
            .collect()
            .await;
        assert!(results.is_err());
        assert!(results.err().unwrap().to_string().starts_with("Internal error: Error invoking function \"panic_if_negative\": Internal(\"Error invoking WASM UDF: Trap"));
    }

    #[tokio::test]
    async fn test_wasm_messagepack_unparseable_output() {
        let ctx = register_wasm_messagepack_udf(
            "write_garbage_output",
            &vec![CreateFunctionDataType::INT],
            &CreateFunctionDataType::INT,
        )
        .await
        .unwrap();

        let results = ctx
            .sql("select write_garbage_output(CAST(1 AS INT))")
            .await
            .unwrap()
            .collect()
            .await;
        assert!(results.is_err());
        assert!(results.err().unwrap().to_string().starts_with("Internal error: Error invoking function \"write_garbage_output\": Internal(\"Error messagepack decoding output buffer"));
    }

    #[tokio::test]
    async fn test_wasm_messagepack_unexpected_response_type() {
        let ctx = register_wasm_messagepack_udf(
            "add_i64",
            &vec![
                CreateFunctionDataType::BIGINT,
                CreateFunctionDataType::BIGINT,
            ],
            &CreateFunctionDataType::TEXT,
        )
        .await
        .unwrap();

        let results = ctx
            .sql("select add_i64(1,2)")
            .await
            .unwrap()
            .collect()
            .await;
        assert!(results.is_err());
        assert!(results.err().unwrap().to_string().starts_with("Internal error: Expected to find string value, received Integer(PosInt(3)) instead"));
    }

    #[tokio::test]
    async fn test_wasm_messagepack_unexpected_arity() {
        let ctx = register_wasm_messagepack_udf(
            "add_i64",
            &vec![
                CreateFunctionDataType::BIGINT,
                CreateFunctionDataType::BIGINT,
            ],
            &CreateFunctionDataType::TEXT,
        )
        .await
        .unwrap();

        let results = ctx
            .sql("select add_i64(1,2,3)")
            .await
            .unwrap()
            .collect()
            .await;
        assert!(results.is_err());
        assert!(results.err().unwrap().to_string().starts_with("Internal error: Wrong number of arguments for function \"add_i64\": expected 2, received 3."));
    }

    #[rstest]
    #[case("add_i64", CreateFunctionDataType::BIGINT)]
    #[case("add_i32", CreateFunctionDataType::INT)]
    #[case("add_i16", CreateFunctionDataType::SMALLINT)]
    #[tokio::test]
    async fn test_wasm_messagepack_add_integers(
        #[case] udf_name: &str,
        #[case] int_type: CreateFunctionDataType,
    ) {
        let type_name = int_type.to_string();
        let ctx = register_wasm_messagepack_udf(
            udf_name,
            &vec![int_type.to_owned(), int_type.to_owned()],
            &int_type,
        )
        .await
        .unwrap();

        ctx.sql(
            format!(
                "CREATE TABLE {}_values AS
            SELECT CAST(v1 AS {}) AS v1, CAST(v2 AS {}) AS v2
            FROM (VALUES (1, 2), (3, 4), (5, 6), (7, 8), (9, 10)) d (v1, v2)
            ",
                type_name.to_lowercase(),
                type_name,
                type_name
            )
            .as_str(),
        )
        .await
        .unwrap();

        let results = ctx
            .sql(
                format!(
                    "SELECT
                    v1,
                    v2,
                    CAST({udf_name}(v1, v2) AS {type_name}) AS sum
            FROM {type_name}_values;"
                )
                .as_str(),
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let expected = vec![
            "+----+----+-----+",
            "| v1 | v2 | sum |",
            "+----+----+-----+",
            "| 1  | 2  | 3   |",
            "| 3  | 4  | 7   |",
            "| 5  | 6  | 11  |",
            "| 7  | 8  | 15  |",
            "| 9  | 10 | 19  |",
            "+----+----+-----+",
        ];

        assert_batches_eq!(expected, &results);
    }

    #[rstest]
    #[case("mul_f32", CreateFunctionDataType::FLOAT)]
    #[case("mul_f64", CreateFunctionDataType::DOUBLE)]
    #[tokio::test]
    async fn test_wasm_messagepack_mul_floating_point(
        #[case] udf_name: &str,
        #[case] float_type: CreateFunctionDataType,
    ) {
        let type_name = float_type.to_string();
        let ctx = register_wasm_messagepack_udf(
            udf_name,
            &vec![float_type.to_owned(), float_type.to_owned()],
            &float_type,
        )
        .await
        .unwrap();

        ctx.sql(
            format!("CREATE TABLE {}_values AS
            SELECT CAST(v1 AS {}) AS v1, CAST(v2 AS {}) AS v2
            FROM (VALUES (0.5, 2.0), (3.5, 4.1), (5.4, 6.2), (7.0, 8.9), (9.1, 10.2)) d (v1, v2)",
            type_name.to_lowercase(), type_name, type_name).as_str(),
        )
        .await
        .unwrap();

        let results = ctx
            .sql(
                format!(
                    "SELECT
                v1,
                v2,
                ROUND({}(v1, v2)) AS product
        FROM {}_values;",
                    udf_name,
                    type_name.to_lowercase()
                )
                .as_str(),
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let expected = vec![
            "+-----+------+---------+",
            "| v1  | v2   | product |",
            "+-----+------+---------+",
            "| 0.5 | 2    | 1       |",
            "| 3.5 | 4.1  | 14      |",
            "| 5.4 | 6.2  | 33      |",
            "| 7   | 8.9  | 62      |",
            "| 9.1 | 10.2 | 93      |",
            "+-----+------+---------+",
        ];

        assert_batches_eq!(expected, &results);
    }

    #[tokio::test]
    async fn test_wasm_messagepack_timestamp() {
        let ctx = register_wasm_messagepack_udf(
            "add_hours",
            &vec![
                CreateFunctionDataType::TIMESTAMP,
                CreateFunctionDataType::INT,
            ],
            &CreateFunctionDataType::TIMESTAMP,
        )
        .await
        .unwrap();

        ctx.sql(
            "CREATE TABLE timestamp_values AS
            SELECT CAST(v1 AS TIMESTAMP) AS v1, CAST(v2 AS INT) AS v2
            FROM (VALUES (1669127920543717000, 2), (1669127920543717000, 4), (0, 6)) d (v1, v2)",
        )
        .await
        .unwrap();

        let results = ctx
            .sql(
                "SELECT
                    v1,
                    v2,
                    add_hours(v1, v2) AS new_ts
            FROM timestamp_values;",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let expected = vec![
            "+----------------------------+----+----------------------------+",
            "| v1                         | v2 | new_ts                     |",
            "+----------------------------+----+----------------------------+",
            "| 2022-11-22T14:38:40.543717 | 2  | 2022-11-22T16:38:40.543717 |",
            "| 2022-11-22T14:38:40.543717 | 4  | 2022-11-22T18:38:40.543717 |",
            "| 1970-01-01T00:00:00        | 6  | 1970-01-01T06:00:00        |",
            "+----------------------------+----+----------------------------+",
        ];

        assert_batches_eq!(expected, &results);
    }

    #[tokio::test]
    async fn test_wasm_messagepack_date() {
        let ctx = register_wasm_messagepack_udf(
            "add_days",
            &vec![CreateFunctionDataType::DATE, CreateFunctionDataType::INT],
            &CreateFunctionDataType::DATE,
        )
        .await
        .unwrap();

        ctx.sql(
            "CREATE TABLE date_values AS
            SELECT CAST(v1 AS DATE) AS v1, CAST(v2 AS INT) AS v2
            FROM (VALUES (0, 2), (19318, 0), (19318, 1)) d (v1, v2)",
        )
        .await
        .unwrap();

        let results = ctx
            .sql(
                "SELECT
                    v1,
                    v2,
                    add_days(v1, v2) AS new_days
            FROM date_values;",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let expected = vec![
            "+------------+----+------------+",
            "| v1         | v2 | new_days   |",
            "+------------+----+------------+",
            "| 1970-01-01 | 2  | 1970-01-03 |",
            "| 2022-11-22 | 0  | 2022-11-22 |",
            "| 2022-11-22 | 1  | 2022-11-23 |",
            "+------------+----+------------+",
        ];

        assert_batches_eq!(expected, &results);
    }

    #[tokio::test]
    async fn test_wasm_messagepack_bool() {
        let ctx = register_wasm_messagepack_udf(
            "xor2",
            &vec![
                CreateFunctionDataType::BOOLEAN,
                CreateFunctionDataType::BOOLEAN,
            ],
            &CreateFunctionDataType::BOOLEAN,
        )
        .await
        .unwrap();

        ctx.sql(
            "CREATE TABLE bool_values AS
            SELECT CAST(v1 AS BOOLEAN) AS v1, CAST(v2 AS BOOLEAN) AS v2
            FROM (VALUES (TRUE, TRUE), (TRUE, FALSE), (FALSE, TRUE), (FALSE, FALSE)) d (v1, v2)",
        )
        .await
        .unwrap();

        let results = ctx
            .sql(
                "SELECT
                    v1,
                    v2,
                    xor2(v1, v2) AS xor_result
            FROM bool_values;",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let expected = vec![
            "+-------+-------+------------+",
            "| v1    | v2    | xor_result |",
            "+-------+-------+------------+",
            "| true  | true  | false      |",
            "| true  | false | true       |",
            "| false | true  | true       |",
            "| false | false | false      |",
            "+-------+-------+------------+",
        ];

        assert_batches_eq!(expected, &results);
    }

    #[tokio::test]
    async fn test_wasm_messagepack_increment_decimal() {
        let ctx = register_wasm_messagepack_udf(
            "increment_decimal",
            &vec![CreateFunctionDataType::DECIMAL {
                precision: 20,
                scale: 2,
            }],
            &CreateFunctionDataType::DECIMAL {
                precision: 20,
                scale: 2,
            },
        )
        .await
        .unwrap();

        ctx.sql(
            "CREATE TABLE decimal_values AS
            SELECT
                CAST(v1 AS DECIMAL(20,2)) AS v1
            FROM (
                VALUES (CAST(0.01 AS DECIMAL(20,2))), (CAST(16691927.02 AS DECIMAL(20,2)))
            ) d (v1)",
        )
        .await
        .unwrap();

        let results = ctx
            .sql(
                "SELECT
                    v1,
                    increment_decimal(v1) AS new_decimal
            FROM decimal_values;",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let expected = vec![
            "+-------------+-------------+",
            "| v1          | new_decimal |",
            "+-------------+-------------+",
            "| 0.01        | 1.01        |",
            "| 16691927.02 | 16691928.02 |",
            "+-------------+-------------+",
        ];

        assert_batches_eq!(expected, &results);
    }

    #[tokio::test]
    async fn test_wasm_messagepack_concat() {
        let ctx = register_wasm_messagepack_udf(
            "concat2",
            &vec![CreateFunctionDataType::TEXT, CreateFunctionDataType::TEXT],
            &CreateFunctionDataType::TEXT,
        )
        .await
        .unwrap();

        ctx.sql(
            "CREATE TABLE text_values AS
            SELECT CAST(s1 AS TEXT) AS s1, CAST(s2 AS TEXT) AS s2
            FROM (VALUES ('foo', 'bar'), ('big', 'int'), ('con', 'gress')) d (s1, s2)",
        )
        .await
        .unwrap();

        let results = ctx
            .sql(
                "SELECT
                s1,
                s2,
                CAST(concat2(s1, s2) AS TEXT) AS concat_result
            FROM text_values;",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let expected = vec![
            "+-----+-------+---------------+",
            "| s1  | s2    | concat_result |",
            "+-----+-------+---------------+",
            "| foo | bar   | foobar        |",
            "| big | int   | bigint        |",
            "| con | gress | congress      |",
            "+-----+-------+---------------+",
        ];

        assert_batches_eq!(expected, &results);
    }
}
