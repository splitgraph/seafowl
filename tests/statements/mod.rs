use itertools::Itertools;
use std::collections::HashMap;
use std::env;
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use chrono::{TimeZone, Utc};
use datafusion::assert_batches_eq;
use datafusion::datasource::TableProvider;
use datafusion_common::{assert_contains, Result};
use itertools::sorted;

use seafowl::catalog::{DEFAULT_DB, DEFAULT_SCHEMA};
#[cfg(feature = "object-store-gcs")]
use serde_json::json;
#[cfg(feature = "remote-tables")]
use sqlx::{any::install_default_drivers, AnyPool, Executor};
#[cfg(feature = "remote-tables")]
use tempfile::{NamedTempFile, TempPath};
use tokio::fs::create_dir;
use tokio::time::sleep;
use uuid::Uuid;

use rstest::rstest;
use tempfile::TempDir;

use seafowl::config::context::build_context;
use seafowl::config::schema::load_config_from_string;
use seafowl::context::SeafowlContext;
use seafowl::repository::interface::Timestamp;
use seafowl::repository::postgres::testutils::get_random_schema;
use seafowl::system_tables::SYSTEM_SCHEMA;

mod ddl;
mod dml;
mod query;
// Hack because integration tests do not set cfg(test)
// https://users.rust-lang.org/t/sharing-helper-function-between-unit-and-integration-tests/9941/2
mod convert;
#[allow(dead_code)]
#[path = "../../src/testutils.rs"]
mod testutils;
mod time_travel;
mod vacuum;

const FAKE_GCS_CREDS_PATH: &str = "/tmp/fake-gcs-server.json";

enum ObjectStoreType {
    Gcs, // TODO: GCS bucket tests with multipart uploads await github.com/fsouza/fake-gcs-server/issues/852
    Local,
    InMemory,
    // S3 object store with an optional path to the actual data folder
    S3(Option<&'static str>),
}

/// Make a SeafowlContext that's connected to a real PostgreSQL database
async fn make_context_with_pg(
    object_store_type: ObjectStoreType,
) -> (SeafowlContext, Option<TempDir>) {
    let dsn = env::var("DATABASE_URL").unwrap();
    let schema = get_random_schema();

    // We need to return the temp dir in order for it to last throughout the test
    let (object_store_section, maybe_temp_dir) = match object_store_type {
        ObjectStoreType::Local => {
            let temp_dir = TempDir::new().unwrap();
            (
                format!(
                    r#"type = "local"
data_dir = "{}""#,
                    temp_dir.path().display()
                ),
                Some(temp_dir),
            )
        }
        ObjectStoreType::InMemory => (r#"type = "memory""#.to_string(), None),
        ObjectStoreType::S3(path) => (
            format!(
                r#"type = "s3"
access_key_id = "minioadmin"
secret_access_key = "minioadmin"
endpoint = "http://127.0.0.1:9000"
bucket = "seafowl-test-bucket"
{}
[object_store.cache_properties]
ttl = 30
"#,
                if let Some(path) = path {
                    format!("prefix = \"{path}\"")
                } else {
                    "".to_string()
                }
            ),
            None,
        ),
        ObjectStoreType::Gcs => {
            let creds_json = json!({"gcs_base_url": "http://localhost:4443", "disable_oauth": true, "client_email": "", "private_key": "", "private_key_id": ""});
            // gcs_base_url should match docker-compose.yml:fake-gcs-server
            let google_application_credentials_path =
                std::path::Path::new(FAKE_GCS_CREDS_PATH);
            std::fs::write(
                google_application_credentials_path,
                serde_json::to_vec(&creds_json).expect("Unable to serialize creds JSON"),
            )
            .expect("Unable to write application credentials JSON file");
            (
                format!(
                    r#"type = "gcs"
bucket = "seafowl-test-bucket"
google_application_credentials = "{}"
"#,
                    google_application_credentials_path.display()
                ),
                None,
            )
        }
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
    (build_context(&config).await.unwrap(), maybe_temp_dir)
}

/// Get a batch of results with all tables and columns in a database
async fn list_columns_query(context: &SeafowlContext) -> Vec<RecordBatch> {
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
async fn list_tables_query(context: &SeafowlContext) -> Vec<RecordBatch> {
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

pub async fn create_table_and_insert(context: &SeafowlContext, table_name: &str) {
    context
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

    // Insert some data (with some columns missing, different order)
    context
        .plan_query(
            format!(
                "INSERT INTO {table_name:} (some_int_value, some_other_value, some_time, some_value) VALUES
                (1111, 1.0, '2022-01-01T20:01:01Z', 42),
                (2222, 1.0, '2022-01-01T20:02:02Z', 43),
                (3333, 1.0, '2022-01-01T20:03:03Z', 44)"
            )
            .as_str(),
        )
        .await
        .unwrap();
}

pub async fn create_table_and_some_partitions(
    context: &SeafowlContext,
    table_name: &str,
    delay: Option<Duration>,
) -> (HashMap<i64, Vec<RecordBatch>>, HashMap<i64, Timestamp>) {
    let mut version_results = HashMap::<i64, Vec<RecordBatch>>::new();
    let mut version_timestamps = HashMap::<i64, Timestamp>::new();

    async fn record_latest_version_snapshot(
        context: &SeafowlContext,
        version_id: i64,
        table_name: &str,
        delay: Option<Duration>,
        version_results: &mut HashMap<i64, Vec<RecordBatch>>,
        version_timestamps: &mut HashMap<i64, Timestamp>,
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
        1,
        table_name,
        delay,
        &mut version_results,
        &mut version_timestamps,
    )
    .await;

    // Add another partition for table version 2
    context
        .plan_query(
            format!("INSERT INTO {table_name} (some_value, some_other_value) VALUES (45, 2.0), (46, 2.0), (47, 2.0)")
                .as_str(),
        )
        .await
        .unwrap();
    record_latest_version_snapshot(
        context,
        2,
        table_name,
        delay,
        &mut version_results,
        &mut version_timestamps,
    )
    .await;

    // Add another partition for table_version 3
    context
        .plan_query(
            format!("INSERT INTO {table_name} (some_value, some_other_value) VALUES (46, 3.0), (47, 3.0), (48, 3.0)")
                .as_str(),
        )
        .await
        .unwrap();
    record_latest_version_snapshot(
        context,
        3,
        table_name,
        delay,
        &mut version_results,
        &mut version_timestamps,
    )
    .await;

    // Add another partition for table_version 4
    context
        .plan_query(
            format!("INSERT INTO {table_name} (some_value, some_other_value) VALUES (42, 4.0), (41, 4.0), (40, 4.0)")
                .as_str(),
        )
        .await
        .unwrap();
    record_latest_version_snapshot(
        context,
        4,
        table_name,
        delay,
        &mut version_results,
        &mut version_timestamps,
    )
    .await;

    (version_results, version_timestamps)
}

fn timestamp_to_rfc3339(timestamp: Timestamp) -> String {
    Utc.timestamp_opt(timestamp, 0).unwrap().to_rfc3339()
}
