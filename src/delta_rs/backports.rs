use arrow::compute::{cast_with_options, CastOptions};
use arrow_schema::{ArrowError, DataType, Schema, TimeUnit};
use chrono::{DateTime, NaiveDateTime, Utc};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::execution::context::{SessionState, TaskContext};
use datafusion::physical_plan::file_format::{partition_type_wrap, FileScanConfig};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::Expr;
use deltalake::action::Add;
use deltalake::delta_datafusion::DeltaDataChecker;
use deltalake::operations::writer::{DeltaWriter, WriterConfig};
use deltalake::storage::DeltaObjectStore;
use deltalake::{DeltaResult, DeltaTable};
use futures::StreamExt;
use object_store::path::{Path, DELIMITER};
use object_store::ObjectMeta;
use std::collections::HashMap;
use std::sync::Arc;
use url::Url;

// Appropriated from https://github.com/delta-io/delta-rs/pull/1176; once the DELETE and UPDATE ops
// are available through delta-rs this will be obsolete.
/// Write the provide ExecutionPlan to the underlying storage
/// The table's invariants are checked during this proccess
pub async fn write_execution_plan(
    table: &DeltaTable,
    state: SessionState,
    plan: Arc<dyn ExecutionPlan>,
    partition_columns: Vec<String>,
    object_store: Arc<DeltaObjectStore>,
    target_file_size: Option<usize>,
    write_batch_size: Option<usize>,
) -> Result<Vec<Add>> {
    let invariants = table
        .get_metadata()
        .and_then(|meta| meta.schema.get_invariants())
        .unwrap_or_default();
    let checker = DeltaDataChecker::new(invariants);

    // Write data to disk
    let mut tasks = vec![];
    for i in 0..plan.output_partitioning().partition_count() {
        let inner_plan = plan.clone();
        let task_ctx = Arc::new(TaskContext::from(&state));

        let config = WriterConfig::new(
            inner_plan.schema(),
            partition_columns.clone(),
            None,
            target_file_size,
            write_batch_size,
        );
        let mut writer = DeltaWriter::new(object_store.clone(), config);
        let checker_stream = checker.clone();
        let mut stream = inner_plan.execute(i, task_ctx)?;
        let handle: tokio::task::JoinHandle<DeltaResult<Vec<Add>>> =
            tokio::task::spawn(async move {
                while let Some(maybe_batch) = stream.next().await {
                    let batch = maybe_batch?;
                    checker_stream.check_batch(&batch).await?;
                    writer.write(&batch).await?;
                }
                writer.close().await
            });

        tasks.push(handle);
    }

    // Collect add actions to add to commit
    Ok(futures::future::join_all(tasks)
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| {
            DataFusionError::Execution(format!(
                "Failed writing to delta table {table}: {err}"
            ))
        })?
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?
        .concat()
        .into_iter()
        .collect::<Vec<_>>())
}

// Appropriated from https://github.com/delta-io/delta-rs/pull/1176 with minor changes.
// Once the DELETE and UPDATE ops are available through delta-rs this will be obsolete.
/// Create a Parquet scan limited to a set of files
pub async fn parquet_scan_from_actions(
    table: &DeltaTable,
    actions: &[Add],
    schema: &Schema,
    filters: &[Expr],
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

    let table_partition_cols = table.get_metadata()?.partition_columns.clone();
    let file_schema = Arc::new(Schema::new(
        schema
            .fields()
            .iter()
            .filter(|f| !table_partition_cols.contains(f.name()))
            .cloned()
            .collect(),
    ));

    let url = Url::parse(&table.table_uri()).unwrap();
    let host = format!(
        "{}-{}{}",
        url.scheme(),
        url.host_str().unwrap_or_default(),
        url.path().replace(DELIMITER, "-").replace(':', "-")
    );
    state
        .runtime_env()
        .register_object_store("delta-rs", &host, table.object_store());
    let object_store_url = ObjectStoreUrl::parse(format!("delta-rs://{host}"))?;

    ParquetFormat::new()
        .create_physical_plan(
            state,
            FileScanConfig {
                object_store_url,
                file_schema,
                file_groups: file_groups.into_values().collect(),
                statistics: table.datafusion_table_statistics(),
                projection: projection.cloned(),
                limit,
                table_partition_cols: table_partition_cols
                    .iter()
                    .map(|c| {
                        Ok((
                            c.to_owned(),
                            partition_type_wrap(
                                schema.field_with_name(c)?.data_type().clone(),
                            ),
                        ))
                    })
                    .collect::<Result<Vec<_>, ArrowError>>()?,
                output_ordering: None,
                infinite_source: false,
            },
            filters,
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
    let last_modified = DateTime::<Utc>::from_utc(
        NaiveDateTime::from_timestamp_opt(ts_secs, ts_ns as u32).unwrap(),
        Utc,
    );
    PartitionedFile {
        object_meta: ObjectMeta {
            location: Path::from(action.path.clone()),
            last_modified,
            size: action.size as usize,
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
                    &time_nanos.to_array(),
                    field_dt,
                    &CastOptions { safe: false },
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
                    &time_nanos.to_array(),
                    field_dt,
                    &CastOptions { safe: false },
                )
                .ok()?;
                Some(ScalarValue::try_from_array(&cast_arr, 0).ok()?)
            }
            _ => Some(ScalarValue::try_from_string(other.to_string(), field_dt).ok()?),
        },
    }
}
