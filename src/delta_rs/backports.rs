use arrow::compute::{cast_with_options, CastOptions};
use arrow_schema::{ArrowError, DataType, Schema, TimeUnit};
use chrono::{NaiveDateTime, TimeZone, Utc};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::execution::context::SessionState;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::{Result, ScalarValue};
use deltalake::kernel::Add;
use deltalake::DeltaTable;
use object_store::path::Path;
use object_store::ObjectMeta;
use std::collections::HashMap;
use std::sync::Arc;
use url::Url;

// Appropriated from https://github.com/delta-io/delta-rs/pull/1176 with minor changes.
// Once the DELETE and UPDATE ops are available through delta-rs this will be obsolete.
/// Create a Parquet scan limited to a set of files
pub async fn parquet_scan_from_actions(
    table: &DeltaTable,
    actions: &[Add],
    schema: &Schema,
    filter_expr: Option<Arc<dyn PhysicalExpr>>,
    state: &SessionState,
    projection: Option<&Vec<usize>>,
    limit: Option<usize>,
) -> Result<Arc<dyn ExecutionPlan>> {
    // TODO we group files together by their partition values. If the table is partitioned
    // and partitions are somewhat evenly distributed, probably not the worst choice ...
    // However we may want to do some additional balancing in case we are far off from the above.
    let mut file_groups: HashMap<Vec<ScalarValue>, Vec<PartitionedFile>> = HashMap::new();
    for action in actions {
        let part = partitioned_file_from_action(action, schema);
        file_groups
            .entry(part.partition_values.clone())
            .or_default()
            .push(part);
    }

    let table_partition_cols = table.metadata()?.partition_columns.clone();
    let file_schema = Arc::new(Schema::new(
        schema
            .fields()
            .iter()
            .filter(|f| !table_partition_cols.contains(f.name()))
            .cloned()
            .collect::<Vec<_>>(),
    ));

    let object_store_url = table.log_store().object_store_url();
    let url: &Url = object_store_url.as_ref();
    state
        .runtime_env()
        .register_object_store(url, table.object_store());

    ParquetFormat::new()
        .create_physical_plan(
            state,
            FileScanConfig {
                object_store_url,
                file_schema,
                file_groups: file_groups.into_values().collect(),
                statistics: table.state.datafusion_table_statistics()?,
                projection: projection.cloned(),
                limit,
                table_partition_cols: table_partition_cols
                    .iter()
                    .map(|c| Ok(schema.field_with_name(c)?.clone()))
                    .collect::<Result<Vec<_>, ArrowError>>()?,
                output_ordering: vec![],
                infinite_source: false,
            },
            (&filter_expr).into(),
        )
        .await
}

// Copied from delta-rs as it's private there; once the DELETE and UPDATE ops
// are available through delta-rs this will be obsolete.
fn partitioned_file_from_action(action: &Add, schema: &Schema) -> PartitionedFile {
    let partition_values = schema
        .fields()
        .iter()
        .filter_map(|f| {
            action.partition_values.get(f.name()).map(|val| match val {
                Some(value) => to_correct_scalar_value(
                    &serde_json::Value::String(value.to_string()),
                    f.data_type(),
                )
                .unwrap_or(ScalarValue::Null),
                None => ScalarValue::Null,
            })
        })
        .collect::<Vec<_>>();

    let ts_secs = action.modification_time / 1000;
    let ts_ns = (action.modification_time % 1000) * 1_000_000;
    let last_modified = Utc.from_utc_datetime(
        &NaiveDateTime::from_timestamp_opt(ts_secs, ts_ns as u32).unwrap(),
    );
    PartitionedFile {
        object_meta: ObjectMeta {
            location: Path::from(action.path.clone()),
            last_modified,
            size: action.size as usize,
            e_tag: None,
            version: None,
        },
        partition_values,
        range: None,
        extensions: None,
    }
}

// Copied from delta-rs as it's private there; once the DELETE and UPDATE ops
// are available through delta-rs this will be obsolete.
fn to_correct_scalar_value(
    stat_val: &serde_json::Value,
    field_dt: &DataType,
) -> Option<ScalarValue> {
    match stat_val {
        serde_json::Value::Array(_) => None,
        serde_json::Value::Object(_) => None,
        serde_json::Value::Null => None,
        serde_json::Value::String(string_val) => match field_dt {
            DataType::Timestamp(_, _) => {
                let time_nanos = ScalarValue::try_from_string(
                    string_val.to_owned(),
                    &DataType::Timestamp(TimeUnit::Nanosecond, None),
                )
                .ok()?;
                let cast_arr = cast_with_options(
                    &time_nanos.to_array().ok()?,
                    field_dt,
                    &CastOptions {
                        safe: false,
                        ..Default::default()
                    },
                )
                .ok()?;
                Some(ScalarValue::try_from_array(&cast_arr, 0).ok()?)
            }
            _ => {
                Some(ScalarValue::try_from_string(string_val.to_owned(), field_dt).ok()?)
            }
        },
        other => match field_dt {
            DataType::Timestamp(_, _) => {
                let time_nanos = ScalarValue::try_from_string(
                    other.to_string(),
                    &DataType::Timestamp(TimeUnit::Nanosecond, None),
                )
                .ok()?;
                let cast_arr = cast_with_options(
                    &time_nanos.to_array().ok()?,
                    field_dt,
                    &CastOptions {
                        safe: false,
                        ..Default::default()
                    },
                )
                .ok()?;
                Some(ScalarValue::try_from_array(&cast_arr, 0).ok()?)
            }
            _ => Some(ScalarValue::try_from_string(other.to_string(), field_dt).ok()?),
        },
    }
}
