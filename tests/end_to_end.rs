use std::env;
use std::sync::Arc;
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use chrono::{TimeZone, Utc};
use datafusion::{assert_batches_eq, assert_contains};
use futures::TryStreamExt;
use hashbrown::HashMap;
use itertools::Itertools;
use object_store::path::Path;
use tokio::time::sleep;

use seafowl::config::context::build_context;
use seafowl::config::schema::load_config_from_string;
use seafowl::context::DefaultSeafowlContext;
use seafowl::context::SeafowlContext;
use seafowl::data_types::{TableVersionId, Timestamp};
use seafowl::provider::SeafowlPartition;
use seafowl::repository::postgres::testutils::get_random_schema;

// Hack because integration tests do not set cfg(test)
// https://users.rust-lang.org/t/sharing-helper-function-between-unit-and-integration-tests/9941/2
#[path = "../src/object_store/testutils.rs"]
mod http_testutils;

// Object store IDs for frequently-used test data
const FILENAME_1: &str =
    "26e39f1717046023c2a53f69ac4d3fa2f8f790489ddf93a267766407817ad4f0.parquet";
const FILENAME_2: &str =
    "b7ffd2743b5fd11ea026065c9aaefcd827771f9cbf4631989786969b3457ded7.parquet";
const FILENAME_RECHUNKED: &str =
    "370e90c844091607f5711565c638bc8b9acbc19b50f242121a88b93ec1892e6d.parquet";

/// Make a SeafowlContext that's connected to a real PostgreSQL database
/// (but uses an in-memory object store)
async fn make_context_with_pg() -> DefaultSeafowlContext {
    let dsn = env::var("DATABASE_URL").unwrap();
    let schema = get_random_schema();

    let config_text = format!(
        r#"
[object_store]
type = "memory"

[catalog]
type = "postgres"
dsn = "{}"
schema = "{}""#,
        dsn, schema
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
                    "SELECT table_schema, table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_catalog = 'default'
        ORDER BY table_name, ordinal_position",
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
                    "SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_catalog = 'default'
        ORDER BY table_schema, table_name",
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
                "CREATE TABLE {:} (
            some_time TIMESTAMP,
            some_value REAL,
            some_other_value NUMERIC,
            some_bool_value BOOLEAN,
            some_int_value BIGINT)",
                table_name
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
                "INSERT INTO {:} (some_int_value, some_time, some_value) VALUES
                (1111, '2022-01-01T20:01:01Z', 42),
                (2222, '2022-01-01T20:02:02Z', 43),
                (3333, '2022-01-01T20:03:03Z', 44)",
                table_name
            )
            .as_str(),
        )
        .await
        .unwrap();
    context.collect(plan).await.unwrap();
}

// A helper function for asserting contents of a given partition
async fn scan_partition(
    context: &DefaultSeafowlContext,
    projection: Option<Vec<usize>>,
    partition: SeafowlPartition,
    table_name: &str,
) -> Vec<RecordBatch> {
    let table = context.try_get_seafowl_table(table_name).unwrap();
    let plan = table
        .partition_scan_plan(
            &projection,
            vec![partition],
            &[],
            None,
            context.internal_object_store.inner.clone(),
        )
        .await
        .unwrap();

    context.collect(plan).await.unwrap()
}

// Used for checking partition ids making up a given table version
async fn assert_partition_ids(
    context: &DefaultSeafowlContext,
    table_version: TableVersionId,
    expected_partition_ids: Vec<i64>,
) {
    let partitions = context
        .partition_catalog
        .load_table_partitions(table_version)
        .await
        .unwrap();

    let partition_ids: Vec<i64> =
        partitions.iter().map(|p| p.partition_id.unwrap()).collect();
    assert_eq!(partition_ids, expected_partition_ids);
}

#[tokio::test]
async fn test_information_schema() {
    let context = make_context_with_pg().await;

    let plan = context
        .plan_query(
            "SELECT * FROM information_schema.tables ORDER BY table_catalog, table_name",
        )
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();

    let expected = vec![
        "+---------------+--------------------+------------+------------+",
        "| table_catalog | table_schema       | table_name | table_type |",
        "+---------------+--------------------+------------+------------+",
        "| default       | information_schema | columns    | VIEW       |",
        "| default       | information_schema | tables     | VIEW       |",
        "| default       | information_schema | views      | VIEW       |",
        "+---------------+--------------------+------------+------------+",
    ];

    assert_batches_eq!(expected, &results);
}

#[tokio::test]
async fn test_create_table() {
    let context = make_context_with_pg().await;

    let plan = context
        .plan_query(
            "CREATE TABLE test_table (
            some_time TIMESTAMP,
            some_value REAL,
            some_other_value NUMERIC,
            some_bool_value BOOLEAN,
            some_int_value BIGINT)",
        )
        .await
        .unwrap();
    context.collect(plan).await.unwrap();

    // Check table columns
    let results = list_columns_query(&context).await;

    let expected = vec![
        "+--------------+------------+------------------+-----------------------------+",
        "| table_schema | table_name | column_name      | data_type                   |",
        "+--------------+------------+------------------+-----------------------------+",
        "| public       | test_table | some_bool_value  | Boolean                     |",
        "| public       | test_table | some_int_value   | Int64                       |",
        "| public       | test_table | some_other_value | Decimal128(38, 10)          |",
        "| public       | test_table | some_time        | Timestamp(Nanosecond, None) |",
        "| public       | test_table | some_value       | Float32                     |",
        "+--------------+------------+------------------+-----------------------------+",
    ];

    assert_batches_eq!(expected, &results);
}

#[tokio::test]
async fn test_create_table_and_insert() {
    let context = make_context_with_pg().await;

    // TODO: insert into nonexistent table outputs a wrong error (schema "public" does not exist)
    create_table_and_insert(&context, "test_table").await;

    // Check table columns: make sure scanning through our file pads the rest with NULLs
    let plan = context
        .plan_query("SELECT * FROM test_table")
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();

    let expected = vec![
        "+-----------------+----------------+------------------+---------------------+------------+",
        "| some_bool_value | some_int_value | some_other_value | some_time           | some_value |",
        "+-----------------+----------------+------------------+---------------------+------------+",
        "|                 | 1111           |                  | 2022-01-01 20:01:01 | 42         |",
        "|                 | 2222           |                  | 2022-01-01 20:02:02 | 43         |",
        "|                 | 3333           |                  | 2022-01-01 20:03:03 | 44         |",
        "+-----------------+----------------+------------------+---------------------+------------+",
    ];

    assert_batches_eq!(expected, &results);

    // Test some projections and aggregations
    let plan = context
        .plan_query("SELECT MAX(some_time) FROM test_table")
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();

    let expected = vec![
        "+---------------------------+",
        "| MAX(test_table.some_time) |",
        "+---------------------------+",
        "| 2022-01-01 20:03:03       |",
        "+---------------------------+",
    ];

    assert_batches_eq!(expected, &results);

    let plan = context
        .plan_query("SELECT MAX(some_int_value), COUNT(DISTINCT some_bool_value), MAX(some_value) FROM test_table")
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();

    let expected = vec![
        "+--------------------------------+--------------------------------------------+----------------------------+",
        "| MAX(test_table.some_int_value) | COUNT(DISTINCT test_table.some_bool_value) | MAX(test_table.some_value) |",
        "+--------------------------------+--------------------------------------------+----------------------------+",
        "| 3333                           | 0                                          | 44                         |",
        "+--------------------------------+--------------------------------------------+----------------------------+",
    ];

    assert_batches_eq!(expected, &results);
}

#[tokio::test]
async fn test_insert_two_different_schemas() {
    let context = make_context_with_pg().await;
    create_table_and_insert(&context, "test_table").await;

    let plan = context
        .plan_query(
            "INSERT INTO test_table (some_value, some_bool_value, some_other_value) VALUES
                (41, FALSE, 2.15),
                (45, TRUE, 9.12),
                (NULL, FALSE, 44.34)",
        )
        .await
        .unwrap();
    context.collect(plan).await.unwrap();

    let plan = context
        .plan_query("SELECT * FROM test_table")
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();

    let expected = vec![
        "+-----------------+----------------+------------------+---------------------+------------+",
        "| some_bool_value | some_int_value | some_other_value | some_time           | some_value |",
        "+-----------------+----------------+------------------+---------------------+------------+",
        "|                 | 1111           |                  | 2022-01-01 20:01:01 | 42         |",
        "|                 | 2222           |                  | 2022-01-01 20:02:02 | 43         |",
        "|                 | 3333           |                  | 2022-01-01 20:03:03 | 44         |",
        "| false           |                | 2.1500000000     |                     | 41         |",
        "| true            |                | 9.1199999999     |                     | 45         |",
        "| false           |                | 44.3400000000    |                     |            |",
        "+-----------------+----------------+------------------+---------------------+------------+",
    ];
    assert_batches_eq!(expected, &results);
}

#[tokio::test]
async fn test_table_partitioning_and_rechunking() {
    let context = make_context_with_pg().await;

    // Make table versions 1 and 2
    create_table_and_insert(&context, "test_table").await;

    // Make table version 3
    let plan = context
        .plan_query(
            "INSERT INTO test_table (some_int_value, some_value) VALUES
            (4444, 45),
            (5555, 46),
            (6666, 47)",
        )
        .await
        .unwrap();
    context.collect(plan).await.unwrap();

    let partitions = context
        .partition_catalog
        .load_table_partitions(3 as TableVersionId)
        .await
        .unwrap();

    // Ensure we have 2 partitions, originating from 2 INSERTS
    assert_eq!(partitions.len(), 2);
    assert_eq!(
        partitions[0].object_storage_id,
        Arc::from(FILENAME_1.to_string())
    );
    assert_eq!(partitions[0].row_count, 3);
    assert_eq!(partitions[0].columns.len(), 3);
    assert_eq!(
        partitions[1].object_storage_id,
        Arc::from(FILENAME_2.to_string())
    );
    assert_eq!(partitions[1].row_count, 3);
    assert_eq!(partitions[1].columns.len(), 2);

    //
    // Test partition pruning during scans works
    //

    // Assert that only a single partition is going to be used
    let plan = context
        .plan_query(
            "EXPLAIN SELECT some_value, some_int_value FROM test_table WHERE some_value > 45",
        )
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();

    let formatted = arrow::util::pretty::pretty_format_batches(results.as_slice())
        .unwrap()
        .to_string();

    let actual_lines: Vec<&str> = formatted.trim().lines().collect();
    assert_contains!(actual_lines[10], format!("partitions=[{:}]", FILENAME_2));

    // Assert query results
    let plan = context
        .plan_query(
            "SELECT some_value, some_int_value FROM test_table WHERE some_value > 45",
        )
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();

    let expected = vec![
        "+------------+----------------+",
        "| some_value | some_int_value |",
        "+------------+----------------+",
        "| 46         | 5555           |",
        "| 47         | 6666           |",
        "+------------+----------------+",
    ];
    assert_batches_eq!(expected, &results);

    //
    // Re-chunk by creating a new table
    //
    let plan = context
        .plan_query("CREATE TABLE table_rechunked AS SELECT * FROM test_table")
        .await
        .unwrap();
    context.collect(plan).await.unwrap();

    let partitions = context
        .partition_catalog
        .load_table_partitions(4 as TableVersionId)
        .await
        .unwrap();

    // Ensure we have re-chunked the 2 partitions into 1
    assert_eq!(partitions.len(), 1);
    assert_eq!(
        partitions[0].object_storage_id,
        Arc::from(FILENAME_RECHUNKED.to_string())
    );
    assert_eq!(partitions[0].row_count, 6);
    assert_eq!(partitions[0].columns.len(), 5);

    // Ensure table contents
    let plan = context
        .plan_query("SELECT some_value, some_int_value FROM table_rechunked")
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();

    let expected = vec![
        "+------------+----------------+",
        "| some_value | some_int_value |",
        "+------------+----------------+",
        "| 42         | 1111           |",
        "| 43         | 2222           |",
        "| 44         | 3333           |",
        "| 45         | 4444           |",
        "| 46         | 5555           |",
        "| 47         | 6666           |",
        "+------------+----------------+",
    ];
    assert_batches_eq!(expected, &results);
}

#[tokio::test]
async fn test_create_table_as() {
    let context = make_context_with_pg().await;
    create_table_and_insert(&context, "test_table").await;

    let plan = context
        .plan_query(
            "
    CREATE TABLE test_ctas AS (
        WITH cte AS (SELECT
            some_int_value,
            some_value + 5 AS some_value,
            EXTRACT(MINUTE FROM some_time) AS some_minute
        FROM test_table)
        SELECT some_value, some_int_value, some_minute
        FROM cte
        ORDER BY some_value DESC
    )
        ",
        )
        .await
        .unwrap();
    context.collect(plan).await.unwrap();

    let plan = context.plan_query("SELECT * FROM test_ctas").await.unwrap();
    let results = context.collect(plan).await.unwrap();

    let expected = vec![
        "+----------------+-------------+------------+",
        "| some_int_value | some_minute | some_value |",
        "+----------------+-------------+------------+",
        "| 3333           | 3           | 49         |",
        "| 2222           | 2           | 48         |",
        "| 1111           | 1           | 47         |",
        "+----------------+-------------+------------+",
    ];
    assert_batches_eq!(expected, &results);
}

#[tokio::test]
async fn test_create_table_move_and_drop() {
    // Create two tables, insert some data into them

    let context = make_context_with_pg().await;

    for table_name in ["test_table_1", "test_table_2"] {
        create_table_and_insert(&context, table_name).await;
    }

    let results = list_columns_query(&context).await;

    let expected = vec![
        "+--------------+--------------+------------------+-----------------------------+",
        "| table_schema | table_name   | column_name      | data_type                   |",
        "+--------------+--------------+------------------+-----------------------------+",
        "| public       | test_table_1 | some_bool_value  | Boolean                     |",
        "| public       | test_table_1 | some_int_value   | Int64                       |",
        "| public       | test_table_1 | some_other_value | Decimal128(38, 10)          |",
        "| public       | test_table_1 | some_time        | Timestamp(Nanosecond, None) |",
        "| public       | test_table_1 | some_value       | Float32                     |",
        "| public       | test_table_2 | some_bool_value  | Boolean                     |",
        "| public       | test_table_2 | some_int_value   | Int64                       |",
        "| public       | test_table_2 | some_other_value | Decimal128(38, 10)          |",
        "| public       | test_table_2 | some_time        | Timestamp(Nanosecond, None) |",
        "| public       | test_table_2 | some_value       | Float32                     |",
        "+--------------+--------------+------------------+-----------------------------+",
    ];

    assert_batches_eq!(expected, &results);

    // Rename the first table to an already existing name
    assert!(context
        .plan_query("ALTER TABLE test_table_1 RENAME TO test_table_2")
        .await
        .unwrap_err()
        .to_string()
        .contains("Target table \"test_table_2\" already exists"));

    // Rename the first table to a new name
    context
        .collect(
            context
                .plan_query("ALTER TABLE test_table_1 RENAME TO test_table_3")
                .await
                .unwrap(),
        )
        .await
        .unwrap();

    let results = list_tables_query(&context).await;

    let expected = vec![
        "+--------------------+--------------+",
        "| table_schema       | table_name   |",
        "+--------------------+--------------+",
        "| information_schema | columns      |",
        "| information_schema | tables       |",
        "| information_schema | views        |",
        "| public             | test_table_2 |",
        "| public             | test_table_3 |",
        "+--------------------+--------------+",
    ];
    assert_batches_eq!(expected, &results);

    // Move the table into a non-existent schema
    assert!(context
        .plan_query("ALTER TABLE test_table_3 RENAME TO new_schema.test_table_3")
        .await
        .unwrap_err()
        .to_string()
        .contains("Schema \"new_schema\" does not exist!"));

    // Create a schema and move the table to it
    context
        .collect(
            context
                .plan_query("CREATE SCHEMA new_schema")
                .await
                .unwrap(),
        )
        .await
        .unwrap();

    context
        .collect(
            context
                .plan_query("ALTER TABLE test_table_3 RENAME TO new_schema.test_table_3")
                .await
                .unwrap(),
        )
        .await
        .unwrap();

    let results = list_tables_query(&context).await;

    let expected = vec![
        "+--------------------+--------------+",
        "| table_schema       | table_name   |",
        "+--------------------+--------------+",
        "| information_schema | columns      |",
        "| information_schema | tables       |",
        "| information_schema | views        |",
        "| new_schema         | test_table_3 |",
        "| public             | test_table_2 |",
        "+--------------------+--------------+",
    ];
    assert_batches_eq!(expected, &results);

    // Drop test_table_3
    let plan = context
        .plan_query("DROP TABLE new_schema.test_table_3")
        .await
        .unwrap();
    context.collect(plan).await.unwrap();

    let results = list_columns_query(&context).await;

    let expected = vec![
        "+--------------+--------------+------------------+-----------------------------+",
        "| table_schema | table_name   | column_name      | data_type                   |",
        "+--------------+--------------+------------------+-----------------------------+",
        "| public       | test_table_2 | some_bool_value  | Boolean                     |",
        "| public       | test_table_2 | some_int_value   | Int64                       |",
        "| public       | test_table_2 | some_other_value | Decimal128(38, 10)          |",
        "| public       | test_table_2 | some_time        | Timestamp(Nanosecond, None) |",
        "| public       | test_table_2 | some_value       | Float32                     |",
        "+--------------+--------------+------------------+-----------------------------+",
    ];

    assert_batches_eq!(expected, &results);

    // Drop the second table

    let plan = context.plan_query("DROP TABLE test_table_2").await.unwrap();
    context.collect(plan).await.unwrap();

    let results = list_columns_query(&context).await;

    let expected = vec!["++", "++"];

    assert_batches_eq!(expected, &results);
}

#[tokio::test]
async fn test_create_table_drop_schema() {
    let context = make_context_with_pg().await;

    for table_name in ["test_table_1", "test_table_2"] {
        create_table_and_insert(&context, table_name).await;
    }

    // Create a schema and move the table to it
    context
        .collect(
            context
                .plan_query("CREATE SCHEMA new_schema")
                .await
                .unwrap(),
        )
        .await
        .unwrap();

    context
        .collect(
            context
                .plan_query("ALTER TABLE test_table_2 RENAME TO new_schema.test_table_2")
                .await
                .unwrap(),
        )
        .await
        .unwrap();

    let results = list_tables_query(&context).await;

    let expected = vec![
        "+--------------------+--------------+",
        "| table_schema       | table_name   |",
        "+--------------------+--------------+",
        "| information_schema | columns      |",
        "| information_schema | tables       |",
        "| information_schema | views        |",
        "| new_schema         | test_table_2 |",
        "| public             | test_table_1 |",
        "+--------------------+--------------+",
    ];
    assert_batches_eq!(expected, &results);

    // DROP the public schema for the fun of it
    context
        .collect(context.plan_query("DROP SCHEMA public").await.unwrap())
        .await
        .unwrap();

    let results = list_tables_query(&context).await;

    let expected = vec![
        "+--------------------+--------------+",
        "| table_schema       | table_name   |",
        "+--------------------+--------------+",
        "| information_schema | columns      |",
        "| information_schema | tables       |",
        "| information_schema | views        |",
        "| new_schema         | test_table_2 |",
        "+--------------------+--------------+",
    ];
    assert_batches_eq!(expected, &results);

    // DROP the new_schema
    context
        .collect(context.plan_query("DROP SCHEMA new_schema").await.unwrap())
        .await
        .unwrap();

    let results = list_tables_query(&context).await;

    let expected = vec![
        "+--------------------+------------+",
        "| table_schema       | table_name |",
        "+--------------------+------------+",
        "| information_schema | columns    |",
        "| information_schema | tables     |",
        "| information_schema | views      |",
        "+--------------------+------------+",
    ];
    assert_batches_eq!(expected, &results);

    // Recreate the public schema and add a table to it
    context
        .collect(context.plan_query("CREATE SCHEMA public").await.unwrap())
        .await
        .unwrap();

    context
        .collect(
            context
                .plan_query("CREATE TABLE test_table_1 (key INT)")
                .await
                .unwrap(),
        )
        .await
        .unwrap();

    let results = list_tables_query(&context).await;

    let expected = vec![
        "+--------------------+--------------+",
        "| table_schema       | table_name   |",
        "+--------------------+--------------+",
        "| information_schema | columns      |",
        "| information_schema | tables       |",
        "| information_schema | views        |",
        "| public             | test_table_1 |",
        "+--------------------+--------------+",
    ];
    assert_batches_eq!(expected, &results);
}

#[tokio::test]
async fn test_create_table_schema_already_exists() {
    let context = make_context_with_pg().await;

    context
        .collect(
            context
                .plan_query("CREATE TABLE some_table(key INT)")
                .await
                .unwrap(),
        )
        .await
        .unwrap();
    let err = context
        .plan_query("CREATE TABLE some_table(key INT)")
        .await
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "Error during planning: Table \"some_table\" already exists"
    );

    let err = context
        .plan_query("CREATE SCHEMA public")
        .await
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "Error during planning: Schema \"public\" already exists"
    );
}

#[tokio::test]
async fn test_create_table_in_staging_schema() {
    let context = make_context_with_pg().await;
    context
        .collect(
            context
                .plan_query("CREATE TABLE some_table(key INT)")
                .await
                .unwrap(),
        )
        .await
        .unwrap();

    let expected_err = "Error during planning: The staging schema can only be referenced via CREATE EXTERNAL TABLE";

    let err = context
        .plan_query("CREATE TABLE staging.some_table(key INT)")
        .await
        .unwrap_err();

    assert_eq!(err.to_string(), expected_err,);

    let err = context.plan_query("DROP SCHEMA staging").await.unwrap_err();

    assert_eq!(err.to_string(), expected_err,);

    let err = context
        .plan_query("ALTER TABLE some_table RENAME TO staging.some_table")
        .await
        .unwrap_err();

    assert_eq!(err.to_string(), expected_err,);
}

#[tokio::test]
async fn test_create_and_run_function() {
    let context = make_context_with_pg().await;

    let function_query = r#"CREATE FUNCTION sintau AS '
    {
        "entrypoint": "sintau",
        "language": "wasm",
        "input_types": ["f32"],
        "return_type": "f32",
        "data": "AGFzbQEAAAABDQJgAX0BfWADfX9/AX0DBQQAAAABBQQBAUREBxgDBnNpbnRhdQAABGV4cDIAAQRsb2cyAAIKjgEEKQECfUMAAAA/IgIgACAAjpMiACACk4siAZMgAZZBAEEYEAMgAiAAk5gLGQAgACAAjiIAk0EYQSwQA7wgAKhBF3RqvgslAQF/IAC8IgFBF3ZB/wBrsiABQQl0s0MAAIBPlUEsQcQAEAOSCyIBAX0DQCADIACUIAEqAgCSIQMgAUEEaiIBIAJrDQALIAMLC0oBAEEAC0Q/x2FC2eATQUuqKsJzsqY9QAHJQH6V0DZv+V88kPJTPSJndz6sZjE/HQCAP/clMD0D/T++F6bRPkzcNL/Tgrg//IiKNwBqBG5hbWUBHwQABnNpbnRhdQEEZXhwMgIEbG9nMgMIZXZhbHBvbHkCNwQAAwABeAECeDECBGhhbGYBAQABeAICAAF4AQJ4aQMEAAF4AQVzdGFydAIDZW5kAwZyZXN1bHQDCQEDAQAEbG9vcA=="
    }';"#;

    let plan = context.plan_query(function_query).await.unwrap();
    context.collect(plan).await.unwrap();

    let results = context
        .collect(
            context
                .plan_query(
                    "
        SELECT v, ROUND(sintau(CAST(v AS REAL)) * 100) AS sintau
        FROM (VALUES (0.1), (0.2), (0.3), (0.4), (0.5)) d (v)",
                )
                .await
                .unwrap(),
        )
        .await
        .unwrap();

    let expected = vec![
        "+-----+--------+",
        "| v   | sintau |",
        "+-----+--------+",
        "| 0.1 | 59     |",
        "| 0.2 | 95     |",
        "| 0.3 | 95     |",
        "| 0.4 | 59     |",
        "| 0.5 | 0      |",
        "+-----+--------+",
    ];

    assert_batches_eq!(expected, &results);

    // Run the same query again to make sure we raise an error if the function already exists
    let err = context.plan_query(function_query).await.unwrap_err();

    assert_eq!(
        err.to_string(),
        "Error during planning: Function \"sintau\" already exists"
    );
}

#[tokio::test]
async fn test_create_external_table_http() {
    /*
    Test CREATE EXTERNAL TABLE works with an HTTP mock server.

    This also works with https + actual S3 (tested manually)

    SELECT * FROM datafusion.public.supply_chains LIMIT 1 results in:

    bytes_scanned{filename=seafowl-public.s3.eu-west-1.amazonaws.com/tutorial/trase-supply-chains.parquet}=232699
    */

    let (mock_server, _) = http_testutils::make_mock_parquet_server(true).await;
    let url = format!("{}/some/file.parquet", &mock_server.uri());

    let context = make_context_with_pg().await;

    // Try creating a table in a non-staging schema
    let err = context
        .plan_query(
            format!(
                "CREATE EXTERNAL TABLE public.file
        STORED AS PARQUET
        LOCATION '{}'",
                url
            )
            .as_str(),
        )
        .await
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("Can only create external tables in the staging schema"));

    // Create a table normally
    let plan = context
        .plan_query(
            format!(
                "CREATE EXTERNAL TABLE file
        STORED AS PARQUET
        LOCATION '{}'",
                url
            )
            .as_str(),
        )
        .await
        .unwrap();
    context.collect(plan).await.unwrap();

    // Test we see the table in the information_schema
    let results = list_tables_query(&context).await;

    let expected = vec![
        "+--------------------+------------+",
        "| table_schema       | table_name |",
        "+--------------------+------------+",
        "| information_schema | columns    |",
        "| information_schema | tables     |",
        "| information_schema | views      |",
        "| staging            | file       |",
        "+--------------------+------------+",
    ];
    assert_batches_eq!(expected, &results);

    // Test standard query
    let plan = context
        .plan_query("SELECT * FROM staging.file")
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();
    let expected = vec![
        "+-------+",
        "| col_1 |",
        "+-------+",
        "| 1     |",
        "| 2     |",
        "| 3     |",
        "+-------+",
    ];

    assert_batches_eq!(expected, &results);

    // Test we can't hit the Seafowl object store directly via CREATE EXTERNAL TABLE
    let err = context
        .plan_query(
            "CREATE EXTERNAL TABLE internal STORED AS PARQUET LOCATION 'seafowl://file'",
        )
        .await
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("Invalid URL scheme for location \"seafowl://file\""));

    // (also test that the DF object store registry doesn't normalize the case so that people can't
    // bypass this)
    let err = context
        .plan_query(
            "CREATE EXTERNAL TABLE internal STORED AS PARQUET LOCATION 'SeAfOwL://file'",
        )
        .await
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("No suitable object store found for seafowl://file"));
}

#[tokio::test]
async fn test_vacuum_command() {
    let context = Arc::new(make_context_with_pg().await);

    async fn assert_orphan_partitions(
        context: Arc<DefaultSeafowlContext>,
        parts: Vec<&str>,
    ) {
        assert_eq!(
            context
                .partition_catalog
                .get_orphan_partition_store_ids()
                .await
                .unwrap(),
            parts
        );
    }

    let get_object_metas = || async {
        context
            .internal_object_store
            .inner
            .list(None)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
    };

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

    //
    // Create two tables with multiple versions
    //

    // Creates table_1 with table_versions 1 (empty) and 2
    create_table_and_insert(&context, "table_1").await;

    // Make table_1 with table_version 3
    let plan = context
        .plan_query("INSERT INTO table_1 (some_value) VALUES (42)")
        .await
        .unwrap();
    context.collect(plan).await.unwrap();

    // Creates table_2 with table_versions 4 (empty) and 5
    create_table_and_insert(&context, "table_2").await;

    // Make table_2 with table_version 6
    let plan = context
        .plan_query("INSERT INTO table_2 (some_value) VALUES (42), (43), (44)")
        .await
        .unwrap();
    context.collect(plan).await.unwrap();

    // Run vacuum on table_1 to remove previous versions
    context
        .collect(context.plan_query("VACUUM TABLE table_1").await.unwrap())
        .await
        .unwrap();

    // TODO: make more explicit the check for deleted table versions
    for &table_version_id in &[1, 2, 4] {
        assert_eq!(
            get_partition_count(context.clone(), table_version_id).await,
            0
        );
    }
    for &table_version_id in &[3, 5, 6] {
        assert!(get_partition_count(context.clone(), table_version_id).await > 0);
    }

    // Run vacuum on all tables; table_2 will now also lose all but the latest version
    context
        .collect(context.plan_query("VACUUM TABLES").await.unwrap())
        .await
        .unwrap();

    // Check table versions cleared up from partition counts
    for &table_version_id in &[1, 2, 4, 5] {
        assert_eq!(
            get_partition_count(context.clone(), table_version_id).await,
            0
        );
    }
    for &table_version_id in &[3, 6] {
        assert!(get_partition_count(context.clone(), table_version_id).await > 0);
    }

    // Drop tables to leave orphan partitions around
    context
        .collect(context.plan_query("DROP TABLE table_1").await.unwrap())
        .await
        .unwrap();
    context
        .collect(context.plan_query("DROP TABLE table_2").await.unwrap())
        .await
        .unwrap();

    // Check we have orphan partitions
    // NB: we have duplicates here which is expected, see: https://github.com/splitgraph/seafowl/issues/5
    let orphans = vec![
        FILENAME_1,
        "a02146c8f6164a0f59526381549a3a8c752a4aa7de5f073e44904bf95833961e.parquet",
        FILENAME_1,
        "824f53285c216022db3ae5e07f032cec9f77d9598b0321cec7c03a23f6d36e87.parquet",
    ];

    assert_orphan_partitions(context.clone(), orphans.clone()).await;
    let object_metas = get_object_metas().await;
    assert_eq!(object_metas.len(), 3);
    for (ind, &orphan) in orphans
        .into_iter()
        .unique()
        .sorted()
        .collect::<Vec<&str>>()
        .iter()
        .enumerate()
    {
        assert_eq!(object_metas[ind].location, Path::from(orphan));
    }

    // Run vacuum on partitions
    context
        .collect(context.plan_query("VACUUM PARTITIONS").await.unwrap())
        .await
        .unwrap();

    // Ensure no orphan partitions are left
    assert_orphan_partitions(context.clone(), vec![]).await;
    let object_metas = get_object_metas().await;
    assert_eq!(object_metas.len(), 0);
}

#[tokio::test]
async fn test_delete_statement() {
    let context = make_context_with_pg().await;

    // Creates table with table_versions 1 (empty) and 2
    create_table_and_insert(&context, "test_table").await;

    // Add another partition for table_version 3
    let plan = context
        .plan_query("INSERT INTO test_table (some_value) VALUES (45), (46), (47)")
        .await
        .unwrap();
    context.collect(plan).await.unwrap();

    // Add another partition for table_version 4
    let plan = context
        .plan_query("INSERT INTO test_table (some_value) VALUES (46), (47), (48)")
        .await
        .unwrap();
    context.collect(plan).await.unwrap();

    // Add another partition for table_version 5
    let plan = context
        .plan_query("INSERT INTO test_table (some_value) VALUES (42), (41), (40)")
        .await
        .unwrap();
    context.collect(plan).await.unwrap();

    // We have 4 partitions from 4 INSERTS
    assert_partition_ids(&context, 5, vec![1, 2, 3, 4]).await;

    //
    // Execute DELETE affecting partitions 2, 3 and creating table_version 6
    //
    let plan = context
        .plan_query("DELETE FROM test_table WHERE some_value > 46")
        .await
        .unwrap();
    context.collect(plan).await.unwrap();

    assert_partition_ids(&context, 6, vec![1, 4, 5]).await;

    let partitions = context
        .partition_catalog
        .load_table_partitions(6 as TableVersionId)
        .await
        .unwrap();

    // Assert result of the new partition with id 5
    let results =
        scan_partition(&context, Some(vec![4]), partitions[2].clone(), "test_table")
            .await;
    let expected = vec![
        "+------------+",
        "| some_value |",
        "+------------+",
        "| 45         |",
        "| 46         |",
        "| 46         |",
        "+------------+",
    ];
    assert_batches_eq!(expected, &results);

    // Verify results
    let plan = context
        .plan_query("SELECT some_value FROM test_table")
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();

    let expected = vec![
        "+------------+",
        "| some_value |",
        "+------------+",
        "| 42         |",
        "| 43         |",
        "| 44         |",
        "| 42         |",
        "| 41         |",
        "| 40         |",
        "| 45         |",
        "| 46         |",
        "| 46         |",
        "+------------+",
    ];
    assert_batches_eq!(expected, &results);

    //
    // Execute a no-op DELETE, leaving the new table version the same as the prior one
    //

    let plan = context
        .plan_query("DELETE FROM test_table WHERE some_value < 35")
        .await
        .unwrap();
    context.collect(plan).await.unwrap();

    assert_partition_ids(&context, 7, vec![1, 4, 5]).await;

    //
    // Add another partition for table_version 8
    //
    let plan = context
        .plan_query("INSERT INTO test_table (some_value) VALUES (48), (49), (50)")
        .await
        .unwrap();
    context.collect(plan).await.unwrap();

    assert_partition_ids(&context, 8, vec![1, 4, 5, 6]).await;

    //
    // Execute DELETE not affecting only partition with id 4, while trimming/combining the rest
    //
    let plan = context
        .plan_query("DELETE FROM test_table WHERE some_value IN (43, 45, 49)")
        .await
        .unwrap();
    context.collect(plan).await.unwrap();

    assert_partition_ids(&context, 9, vec![4, 7, 8]).await;

    // Verify new partition contents
    let partitions = context
        .partition_catalog
        .load_table_partitions(9 as TableVersionId)
        .await
        .unwrap();

    let results =
        scan_partition(&context, Some(vec![4]), partitions[1].clone(), "test_table")
            .await;
    let expected = vec![
        "+------------+",
        "| some_value |",
        "+------------+",
        "| 42         |",
        "| 44         |",
        "+------------+",
    ];
    assert_batches_eq!(expected, &results);

    let results =
        scan_partition(&context, Some(vec![4]), partitions[2].clone(), "test_table")
            .await;
    let expected = vec![
        "+------------+",
        "| some_value |",
        "+------------+",
        "| 46         |",
        "| 46         |",
        "| 48         |",
        "| 50         |",
        "+------------+",
    ];
    assert_batches_eq!(expected, &results);

    //
    // Execute DELETE with multiple qualifiers
    //
    let plan = context
        .plan_query("DELETE FROM test_table WHERE some_value < 41 OR some_value > 46")
        .await
        .unwrap();
    context.collect(plan).await.unwrap();

    assert_partition_ids(&context, 10, vec![7, 9, 10]).await;

    // Verify new partition contents
    let partitions = context
        .partition_catalog
        .load_table_partitions(10 as TableVersionId)
        .await
        .unwrap();

    let results =
        scan_partition(&context, Some(vec![4]), partitions[1].clone(), "test_table")
            .await;
    let expected = vec![
        "+------------+",
        "| some_value |",
        "+------------+",
        "| 42         |",
        "| 41         |",
        "+------------+",
    ];
    assert_batches_eq!(expected, &results);

    let results =
        scan_partition(&context, Some(vec![4]), partitions[2].clone(), "test_table")
            .await;
    let expected = vec![
        "+------------+",
        "| some_value |",
        "+------------+",
        "| 46         |",
        "| 46         |",
        "+------------+",
    ];
    assert_batches_eq!(expected, &results);

    //
    // Execute blank DELETE, without qualifiers
    //
    let plan = context.plan_query("DELETE FROM test_table").await.unwrap();
    context.collect(plan).await.unwrap();

    assert_partition_ids(&context, 11, vec![]).await;

    // Verify results
    let plan = context
        .plan_query("SELECT some_value FROM test_table")
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();

    assert!(results.is_empty());
}

#[tokio::test]
async fn test_update_statement() {
    let context = make_context_with_pg().await;

    // Creates table with table_versions 1 (empty) and 2
    create_table_and_insert(&context, "test_table").await;

    // Add another partition for table_version 3
    let plan = context
        .plan_query("INSERT INTO test_table (some_value) VALUES (45), (46), (47)")
        .await
        .unwrap();
    context.collect(plan).await.unwrap();

    // Add another partition for table_version 4
    let plan = context
        .plan_query("INSERT INTO test_table (some_value) VALUES (46), (47), (48)")
        .await
        .unwrap();
    context.collect(plan).await.unwrap();

    // Add another partition for table_version 5
    let plan = context
        .plan_query("INSERT INTO test_table (some_value) VALUES (42), (41), (40)")
        .await
        .unwrap();
    context.collect(plan).await.unwrap();

    // We have 4 partitions from 4 INSERTS
    assert_partition_ids(&context, 5, vec![1, 2, 3, 4]).await;

    //
    // Execute UPDATE with a selection, affecting partitions 1 and 4, and creating table_version 6
    //
    let plan = context
        .plan_query("UPDATE test_table SET some_time = '2022-01-01 21:21:21Z', some_int_value = 5555, some_value = some_value - 10 WHERE some_value IN (41, 42, 43)")
        .await
        .unwrap();
    context.collect(plan).await.unwrap();

    assert_partition_ids(&context, 6, vec![2, 3, 5, 6]).await;

    // Verify new partition contents
    let partitions = context
        .partition_catalog
        .load_table_partitions(6 as TableVersionId)
        .await
        .unwrap();

    let results =
        scan_partition(&context, None, partitions[2].clone(), "test_table").await;
    let expected = vec![
        "+-----------------+----------------+------------------+---------------------+------------+",
        "| some_bool_value | some_int_value | some_other_value | some_time           | some_value |",
        "+-----------------+----------------+------------------+---------------------+------------+",
        "|                 | 5555           |                  | 2022-01-01 21:21:21 | 32         |",
        "|                 | 5555           |                  | 2022-01-01 21:21:21 | 33         |",
        "|                 | 3333           |                  | 2022-01-01 20:03:03 | 44         |",
        "+-----------------+----------------+------------------+---------------------+------------+",
    ];
    assert_batches_eq!(expected, &results);

    let results =
        scan_partition(&context, None, partitions[3].clone(), "test_table").await;
    let expected = vec![
        "+-----------------+----------------+------------------+---------------------+------------+",
        "| some_bool_value | some_int_value | some_other_value | some_time           | some_value |",
        "+-----------------+----------------+------------------+---------------------+------------+",
        "|                 | 5555           |                  | 2022-01-01 21:21:21 | 32         |",
        "|                 | 5555           |                  | 2022-01-01 21:21:21 | 31         |",
        "|                 |                |                  |                     | 40         |",
        "+-----------------+----------------+------------------+---------------------+------------+"
    ];
    assert_batches_eq!(expected, &results);

    //
    // Execute UPDATE that doesn't change anything
    //
    let plan = context
        .plan_query("UPDATE test_table SET some_bool_value = TRUE WHERE some_value = 200")
        .await
        .unwrap();
    context.collect(plan).await.unwrap();

    assert_partition_ids(&context, 7, vec![2, 3, 5, 6]).await;

    //
    // Execute UPDATE that causes an error during planning/execution, to test that the subsequent
    // UPDATE works correctly
    //
    let err = context
        .plan_query("UPDATE test_table SET some_other_value = 'nope'")
        .await
        .unwrap_err();

    assert!(err
        .to_string()
        .contains("Unsupported CAST from Utf8 to Decimal128(38, 10)"));

    //
    // Execute complex UPDATE (redundant assignment and a case assignment) without a selection,
    // creating new table_version with a single new partition
    //
    let plan = context
        .plan_query(
            "UPDATE test_table SET some_bool_value = FALSE, some_bool_value = (some_int_value = 5555), some_value = 42, \
            some_other_value = CASE WHEN some_int_value = 5555 THEN 5.555 WHEN some_int_value = 3333 THEN 3.333 ELSE 0 END"
        )
        .await
        .unwrap();
    context.collect(plan).await.unwrap();

    assert_partition_ids(&context, 8, vec![7]).await;

    // Verify results
    let plan = context
        .plan_query("SELECT * FROM test_table")
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();

    let expected = vec![
        "+-----------------+----------------+------------------+---------------------+------------+",
        "| some_bool_value | some_int_value | some_other_value | some_time           | some_value |",
        "+-----------------+----------------+------------------+---------------------+------------+",
        "|                 |                | 0.0000000000     |                     | 42         |",
        "|                 |                | 0.0000000000     |                     | 42         |",
        "|                 |                | 0.0000000000     |                     | 42         |",
        "|                 |                | 0.0000000000     |                     | 42         |",
        "|                 |                | 0.0000000000     |                     | 42         |",
        "|                 |                | 0.0000000000     |                     | 42         |",
        "| true            | 5555           | 5.5550000000     | 2022-01-01 21:21:21 | 42         |",
        "| true            | 5555           | 5.5550000000     | 2022-01-01 21:21:21 | 42         |",
        "| false           | 3333           | 3.3330000000     | 2022-01-01 20:03:03 | 42         |",
        "| true            | 5555           | 5.5550000000     | 2022-01-01 21:21:21 | 42         |",
        "| true            | 5555           | 5.5550000000     | 2022-01-01 21:21:21 | 42         |",
        "|                 |                | 0.0000000000     |                     | 42         |",
        "+-----------------+----------------+------------------+---------------------+------------+",
    ];
    assert_batches_eq!(expected, &results);
}

#[tokio::test]
async fn test_update_statement_errors() {
    let context = make_context_with_pg().await;

    // Creates table with table_versions 1 (empty) and 2
    create_table_and_insert(&context, "test_table").await;

    //
    // Execute UPDATE that references a nonexistent column in the assignment or in the selection,
    // or results in a type mismatch
    //
    let err = context
        .plan_query("UPDATE test_table SET nonexistent = 42 WHERE some_value = 32")
        .await
        .unwrap_err();

    assert!(err
        .to_string()
        .contains("Schema error: No field named 'nonexistent'"));

    let err = context
        .plan_query("UPDATE test_table SET some_value = 42 WHERE nonexistent = 32")
        .await
        .unwrap_err();

    assert!(err
        .to_string()
        .contains("Schema error: No field named 'nonexistent'"));

    let err = context
        .plan_query("UPDATE test_table SET some_int_value = 'nope'")
        .await
        .unwrap_err();

    assert!(err
        .to_string()
        .contains("Cannot cast string 'nope' to value of Int64 type"));
}

#[tokio::test]
async fn test_table_time_travel() {
    let context = make_context_with_pg().await;
    let mut version_results = HashMap::<TableVersionId, Vec<RecordBatch>>::new();
    let mut version_timestamps = HashMap::<TableVersionId, Timestamp>::new();

    async fn record_result_and_timestamp(
        context: &DefaultSeafowlContext,
        version_id: TableVersionId,
        version_results: &mut HashMap<TableVersionId, Vec<RecordBatch>>,
        version_timestamps: &mut HashMap<TableVersionId, Timestamp>,
    ) {
        let plan = context
            .plan_query("SELECT * FROM test_table")
            .await
            .unwrap();
        let results = context.collect(plan).await.unwrap();

        // We do a 2 x 1 second pause here because our version timestamp resolution is 1 second, and
        // we want to be able to verify the disambiguate the versions below
        sleep(Duration::from_secs(1)).await;
        version_results.insert(version_id, results);
        version_timestamps.insert(version_id, Utc::now().timestamp() as Timestamp);
        sleep(Duration::from_secs(1)).await;
    }

    // Creates table with table_versions 1 (empty) and 2
    create_table_and_insert(&context, "test_table").await;
    record_result_and_timestamp(
        &context,
        2 as TableVersionId,
        &mut version_results,
        &mut version_timestamps,
    )
    .await;

    // Add another partition for table_version 3
    let plan = context
        .plan_query("INSERT INTO test_table (some_value) VALUES (45), (46), (47)")
        .await
        .unwrap();
    context.collect(plan).await.unwrap();
    record_result_and_timestamp(
        &context,
        3 as TableVersionId,
        &mut version_results,
        &mut version_timestamps,
    )
    .await;

    // Add another partition for table_version 4
    let plan = context
        .plan_query("INSERT INTO test_table (some_value) VALUES (46), (47), (48)")
        .await
        .unwrap();
    context.collect(plan).await.unwrap();
    record_result_and_timestamp(
        &context,
        4 as TableVersionId,
        &mut version_results,
        &mut version_timestamps,
    )
    .await;

    // Add another partition for table_version 5
    let plan = context
        .plan_query("INSERT INTO test_table (some_value) VALUES (42), (41), (40)")
        .await
        .unwrap();
    context.collect(plan).await.unwrap();

    //
    // Now use the recorded timestamps to query specific earlier table versions and compare them to
    // the recorded results for that version.
    //

    async fn query_table_version(
        context: &DefaultSeafowlContext,
        version_id: TableVersionId,
        version_results: &HashMap<TableVersionId, Vec<RecordBatch>>,
        version_timestamps: &HashMap<TableVersionId, Timestamp>,
    ) {
        let timestamp = Utc
            .timestamp(version_timestamps[&version_id], 0)
            .to_rfc3339();
        let plan = context
            .plan_query(format!("SELECT * FROM test_table('{}')", timestamp).as_str())
            .await
            .unwrap();
        let results = context.collect(plan).await.unwrap();

        assert_eq!(version_results[&version_id], results);
    }

    query_table_version(
        &context,
        2 as TableVersionId,
        &version_results,
        &version_timestamps,
    )
    .await;
    query_table_version(
        &context,
        3 as TableVersionId,
        &version_results,
        &version_timestamps,
    )
    .await;
    query_table_version(
        &context,
        4 as TableVersionId,
        &version_results,
        &version_timestamps,
    )
    .await;
}
