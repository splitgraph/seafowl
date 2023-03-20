use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use assert_unordered::assert_eq_unordered_sort;
use chrono::{TimeZone, Utc};
use datafusion::assert_batches_eq;
use datafusion_common::assert_contains;
use deltalake::DeltaDataTypeVersion;
use futures::TryStreamExt;
use itertools::{sorted, Itertools};
use object_store::path::Path;
use seafowl::catalog::{DEFAULT_DB, DEFAULT_SCHEMA};
#[cfg(feature = "remote-tables")]
use sqlx::{AnyPool, Executor};
#[cfg(feature = "remote-tables")]
use tempfile::{NamedTempFile, TempPath};
use tokio::time::sleep;

#[cfg(feature = "remote-tables")]
use rstest::rstest;
use tempfile::TempDir;

use seafowl::config::context::build_context;
use seafowl::config::schema::load_config_from_string;
use seafowl::context::DefaultSeafowlContext;
use seafowl::context::SeafowlContext;
use seafowl::data_types::{TableVersionId, Timestamp};
use seafowl::repository::postgres::testutils::get_random_schema;
use seafowl::system_tables::SYSTEM_SCHEMA;

// Hack because integration tests do not set cfg(test)
// https://users.rust-lang.org/t/sharing-helper-function-between-unit-and-integration-tests/9941/2
mod ddl;
mod dml;
mod function;
#[path = "../../src/object_store/testutils.rs"]
mod http_testutils;
mod query;
mod query_legacy;
mod vacuum;

// Object store IDs for frequently-used test data
const FILENAME_1: &str =
    "7fbfeeeade71978b4ae82cd3d97b8c1bd9ae7ab9a7a78ee541b66209cfd7722d.parquet";

enum ObjectStoreType {
    Local(String),
    InMemory,
}

/// Make a SeafowlContext that's connected to a real PostgreSQL database
async fn make_context_with_pg(
    object_store_type: ObjectStoreType,
) -> DefaultSeafowlContext {
    let dsn = env::var("DATABASE_URL").unwrap();
    let schema = get_random_schema();

    let object_store_section = match object_store_type {
        ObjectStoreType::Local(data_dir) => {
            format!(
                r#"type = "local"
data_dir = "{}""#,
                data_dir
            )
        }
        ObjectStoreType::InMemory => r#"type = "memory""#.to_string(),
    };

    let config_text = format!(
        r#"
[object_store]
{object_store_section}

[catalog]
type = "postgres"
dsn = "{dsn}"
schema = "{schema}""#
    );

    // Ignore the "in-memory object store / persistent catalog" error in e2e tests (we'll discard
    // the PG instance anyway)
    let config = load_config_from_string(&config_text, true, None).unwrap();
    build_context(&config).await.unwrap()
}

/// Get a batch of results with all tables and columns in a database
async fn list_columns_query(context: &DefaultSeafowlContext) -> Vec<RecordBatch> {
    context
        .collect(
            context
                .plan_query(
                    format!(
                        "SELECT table_schema, table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_catalog = 'default' AND table_schema != '{SYSTEM_SCHEMA}'
        ORDER BY table_name, ordinal_position",
                    )
                    .as_str(),
                )
                .await
                .unwrap(),
        )
        .await
        .unwrap()
}

/// Get a batch of results with all tables in a database
async fn list_tables_query(context: &DefaultSeafowlContext) -> Vec<RecordBatch> {
    context
        .collect(
            context
                .plan_query(
                    format!(
                        "SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_catalog = 'default' AND table_schema != '{SYSTEM_SCHEMA}'
        ORDER BY table_schema, table_name",
                    )
                    .as_str(),
                )
                .await
                .unwrap(),
        )
        .await
        .unwrap()
}

async fn create_table_and_insert(context: &DefaultSeafowlContext, table_name: &str) {
    let plan = context
        .plan_query(
            // SQL injection here, fine for test code
            format!(
                "CREATE TABLE {table_name:} (
            some_time TIMESTAMP,
            some_value REAL,
            some_other_value NUMERIC,
            some_bool_value BOOLEAN,
            some_int_value BIGINT)"
            )
            .as_str(),
        )
        .await
        .unwrap();
    context.collect(plan).await.unwrap();

    // Insert some data (with some columns missing, different order)
    let plan = context
        .plan_query(
            format!(
                "INSERT INTO {table_name:} (some_int_value, some_time, some_value) VALUES
                (1111, '2022-01-01T20:01:01Z', 42),
                (2222, '2022-01-01T20:02:02Z', 43),
                (3333, '2022-01-01T20:03:03Z', 44)"
            )
            .as_str(),
        )
        .await
        .unwrap();
    context.collect(plan).await.unwrap();
}

async fn create_table_and_some_partitions(
    context: &DefaultSeafowlContext,
    table_name: &str,
    delay: Option<Duration>,
) -> (
    HashMap<DeltaDataTypeVersion, Vec<RecordBatch>>,
    HashMap<DeltaDataTypeVersion, Timestamp>,
) {
    let mut version_results = HashMap::<DeltaDataTypeVersion, Vec<RecordBatch>>::new();
    let mut version_timestamps = HashMap::<DeltaDataTypeVersion, Timestamp>::new();

    async fn record_latest_version_snapshot(
        context: &DefaultSeafowlContext,
        version_id: DeltaDataTypeVersion,
        table_name: &str,
        delay: Option<Duration>,
        version_results: &mut HashMap<DeltaDataTypeVersion, Vec<RecordBatch>>,
        version_timestamps: &mut HashMap<DeltaDataTypeVersion, Timestamp>,
    ) {
        if let Some(delay) = delay {
            let plan = context
                .plan_query(format!("SELECT * FROM {table_name}").as_str())
                .await
                .unwrap();
            let results = context.collect(plan).await.unwrap();

            // We do a 2 x 1 second pause here because our version timestamp resolution is 1 second, and
            // we want to be able to disambiguate the different versions
            sleep(delay).await;
            version_results.insert(version_id, results);
            version_timestamps.insert(version_id, Utc::now().timestamp() as Timestamp);
            sleep(delay).await;
        }
    }

    // Creates table with table versions 0 (empty) and 1
    create_table_and_insert(context, table_name).await;
    record_latest_version_snapshot(
        context,
        1 as DeltaDataTypeVersion,
        table_name,
        delay,
        &mut version_results,
        &mut version_timestamps,
    )
    .await;

    // Add another partition for table version 2
    let plan = context
        .plan_query(
            format!("INSERT INTO {table_name} (some_value) VALUES (45), (46), (47)")
                .as_str(),
        )
        .await
        .unwrap();
    context.collect(plan).await.unwrap();
    record_latest_version_snapshot(
        context,
        2 as DeltaDataTypeVersion,
        table_name,
        delay,
        &mut version_results,
        &mut version_timestamps,
    )
    .await;

    // Add another partition for table_version 3
    let plan = context
        .plan_query(
            format!("INSERT INTO {table_name} (some_value) VALUES (46), (47), (48)")
                .as_str(),
        )
        .await
        .unwrap();
    context.collect(plan).await.unwrap();
    record_latest_version_snapshot(
        context,
        3 as DeltaDataTypeVersion,
        table_name,
        delay,
        &mut version_results,
        &mut version_timestamps,
    )
    .await;

    // Add another partition for table_version 4
    let plan = context
        .plan_query(
            format!("INSERT INTO {table_name} (some_value) VALUES (42), (41), (40)")
                .as_str(),
        )
        .await
        .unwrap();
    context.collect(plan).await.unwrap();
    record_latest_version_snapshot(
        context,
        4 as DeltaDataTypeVersion,
        table_name,
        delay,
        &mut version_results,
        &mut version_timestamps,
    )
    .await;

    (version_results, version_timestamps)
}

async fn assert_orphan_partitions(context: Arc<DefaultSeafowlContext>, parts: Vec<&str>) {
    assert_eq_unordered_sort!(
        context
            .partition_catalog
            .get_orphan_partition_store_ids()
            .await
            .unwrap()
            // Turn Vec<String> -> Vec<&str>
            .iter()
            .map(|s| &**s)
            .collect(),
        parts
    );
}

async fn get_partition_count(
    context: Arc<DefaultSeafowlContext>,
    table_version_id: i32,
) -> usize {
    context
        .partition_catalog
        .load_table_partitions(table_version_id as TableVersionId)
        .await
        .unwrap()
        .len()
}
