mod http;

use std::env;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use datafusion::assert_batches_eq;

use seafowl::config::context::build_context;
use seafowl::config::schema::load_config_from_string;
use seafowl::context::DefaultSeafowlContext;
use seafowl::context::SeafowlContext;
use seafowl::data_types::TableVersionId;
use seafowl::repository::postgres::testutils::get_random_schema;

/// Make a SeafowlContext that's connected to a real PostgreSQL database
/// (but uses an in-memory object store)
async fn make_context_with_pg() -> DefaultSeafowlContext {
    let dsn = env::var("DATABASE_URL").unwrap();

    let config_text = format!(
        r#"
[object_store]
type = "memory"

[catalog]
type = "postgres"
dsn = "{}"
schema = "{}""#,
        dsn,
        get_random_schema()
    );

    // Ignore the "in-memory object store / persistent catalog" error in e2e tests (we'll discard
    // the PG instance anyway)
    let config = load_config_from_string(&config_text, true).unwrap();
    build_context(&config).await
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

    // reregister / reload the catalog
    context.reload_schema().await;

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

    context.reload_schema().await;
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
        "+---------------+--------------------+------------+------------+------------+",
        "| table_catalog | table_schema       | table_name | table_type | definition |",
        "+---------------+--------------------+------------+------------+------------+",
        "| datafusion    | information_schema | columns    | VIEW       |            |",
        "| datafusion    | information_schema | tables     | VIEW       |            |",
        "| default       | information_schema | columns    | VIEW       |            |",
        "| default       | information_schema | tables     | VIEW       |            |",
        "+---------------+--------------------+------------+------------+------------+",
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

    // reregister / reload the catalog
    context.reload_schema().await;

    // Check table columns
    let results = list_columns_query(&context).await;

    let expected = vec![
        "+--------------+------------+------------------+-----------------------------+",
        "| table_schema | table_name | column_name      | data_type                   |",
        "+--------------+------------+------------------+-----------------------------+",
        "| public       | test_table | some_bool_value  | Boolean                     |",
        "| public       | test_table | some_int_value   | Int64                       |",
        "| public       | test_table | some_other_value | Decimal(38, 10)             |",
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

    context.reload_schema().await;

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
        .await;

    // Ensure we have 2 partitions, originating from 2 INSERTS
    assert_eq!(partitions.len(), 2);
    assert_eq!(
        partitions[0].object_storage_id,
        Arc::from(
            "6f3bed033bef03a66a34beead3ba5cd89eb382b9ba45bb6edfd3541e9ea65242.parquet"
                .to_string()
        )
    );
    assert_eq!(partitions[0].row_count, 3);
    assert_eq!(partitions[0].columns.len(), 3);
    assert_eq!(
        partitions[1].object_storage_id,
        Arc::from(
            "a03b99f5a111782cc00bb80adbab53dbba67b745ea21b0cbd0f80258093f12a3.parquet"
                .to_string()
        )
    );
    assert_eq!(partitions[1].row_count, 3);
    assert_eq!(partitions[1].columns.len(), 2);

    context.reload_schema().await;

    let plan = context
        .plan_query("CREATE TABLE table_rechunked AS SELECT * FROM test_table")
        .await
        .unwrap();
    context.collect(plan).await.unwrap();

    let partitions = context
        .partition_catalog
        .load_table_partitions(4 as TableVersionId)
        .await;

    // Ensure we have re-chunked the 2 partitions into 1
    assert_eq!(partitions.len(), 1);
    assert_eq!(
        partitions[0].object_storage_id,
        Arc::from(
            "80091935282490b5a715080555c1e8c58bb8ce69e07cf7533ec83aa29167cee3.parquet"
                .to_string()
        )
    );
    assert_eq!(partitions[0].row_count, 6);
    assert_eq!(partitions[0].columns.len(), 5);

    // Ensure table contents
    context.reload_schema().await;
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

    context.reload_schema().await;

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

    context.reload_schema().await;

    let results = list_columns_query(&context).await;

    let expected = vec![
        "+--------------+--------------+------------------+-----------------------------+",
        "| table_schema | table_name   | column_name      | data_type                   |",
        "+--------------+--------------+------------------+-----------------------------+",
        "| public       | test_table_1 | some_bool_value  | Boolean                     |",
        "| public       | test_table_1 | some_int_value   | Int64                       |",
        "| public       | test_table_1 | some_other_value | Decimal(38, 10)             |",
        "| public       | test_table_1 | some_time        | Timestamp(Nanosecond, None) |",
        "| public       | test_table_1 | some_value       | Float32                     |",
        "| public       | test_table_2 | some_bool_value  | Boolean                     |",
        "| public       | test_table_2 | some_int_value   | Int64                       |",
        "| public       | test_table_2 | some_other_value | Decimal(38, 10)             |",
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
    context.reload_schema().await;

    let results = list_tables_query(&context).await;

    let expected = vec![
        "+--------------------+--------------+",
        "| table_schema       | table_name   |",
        "+--------------------+--------------+",
        "| information_schema | columns      |",
        "| information_schema | tables       |",
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
    context.reload_schema().await;

    context
        .collect(
            context
                .plan_query("ALTER TABLE test_table_3 RENAME TO new_schema.test_table_3")
                .await
                .unwrap(),
        )
        .await
        .unwrap();
    context.reload_schema().await;

    let results = list_tables_query(&context).await;

    let expected = vec![
        "+--------------------+--------------+",
        "| table_schema       | table_name   |",
        "+--------------------+--------------+",
        "| information_schema | columns      |",
        "| information_schema | tables       |",
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
    context.reload_schema().await;

    let results = list_columns_query(&context).await;

    let expected = vec![
        "+--------------+--------------+------------------+-----------------------------+",
        "| table_schema | table_name   | column_name      | data_type                   |",
        "+--------------+--------------+------------------+-----------------------------+",
        "| public       | test_table_2 | some_bool_value  | Boolean                     |",
        "| public       | test_table_2 | some_int_value   | Int64                       |",
        "| public       | test_table_2 | some_other_value | Decimal(38, 10)             |",
        "| public       | test_table_2 | some_time        | Timestamp(Nanosecond, None) |",
        "| public       | test_table_2 | some_value       | Float32                     |",
        "+--------------+--------------+------------------+-----------------------------+",
    ];

    assert_batches_eq!(expected, &results);

    // Drop the second table

    let plan = context.plan_query("DROP TABLE test_table_2").await.unwrap();
    context.collect(plan).await.unwrap();

    context.reload_schema().await;

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

    context.reload_schema().await;

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
    context.reload_schema().await;

    context
        .collect(
            context
                .plan_query("ALTER TABLE test_table_2 RENAME TO new_schema.test_table_2")
                .await
                .unwrap(),
        )
        .await
        .unwrap();
    context.reload_schema().await;

    let results = list_tables_query(&context).await;

    let expected = vec![
        "+--------------------+--------------+",
        "| table_schema       | table_name   |",
        "+--------------------+--------------+",
        "| information_schema | columns      |",
        "| information_schema | tables       |",
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
    context.reload_schema().await;

    let results = list_tables_query(&context).await;

    let expected = vec![
        "+--------------------+--------------+",
        "| table_schema       | table_name   |",
        "+--------------------+--------------+",
        "| information_schema | columns      |",
        "| information_schema | tables       |",
        "| new_schema         | test_table_2 |",
        "+--------------------+--------------+",
    ];
    assert_batches_eq!(expected, &results);

    // DROP the new_schema
    context
        .collect(context.plan_query("DROP SCHEMA new_schema").await.unwrap())
        .await
        .unwrap();
    context.reload_schema().await;

    let results = list_tables_query(&context).await;

    let expected = vec![
        "+--------------------+------------+",
        "| table_schema       | table_name |",
        "+--------------------+------------+",
        "| information_schema | columns    |",
        "| information_schema | tables     |",
        "+--------------------+------------+",
    ];
    assert_batches_eq!(expected, &results);

    // Recreate the public schema and add a table to it
    context
        .collect(context.plan_query("CREATE SCHEMA public").await.unwrap())
        .await
        .unwrap();
    context.reload_schema().await;

    context
        .collect(
            context
                .plan_query("CREATE TABLE test_table_1 (key INTEGER)")
                .await
                .unwrap(),
        )
        .await
        .unwrap();
    context.reload_schema().await;

    let results = list_tables_query(&context).await;

    let expected = vec![
        "+--------------------+--------------+",
        "| table_schema       | table_name   |",
        "+--------------------+--------------+",
        "| information_schema | columns      |",
        "| information_schema | tables       |",
        "| public             | test_table_1 |",
        "+--------------------+--------------+",
    ];
    assert_batches_eq!(expected, &results);
}

#[tokio::test]
async fn test_create_and_reload_function() {
    let context = make_context_with_pg().await;

    let plan = context
        .plan_query(
            r#"CREATE FUNCTION sintau AS '
            {
                "entrypoint": "sintau",
                "language": "wasm",
                "input_types": ["f32"],
                "return_type": "f32",
                "data": "AGFzbQEAAAABDQJgAX0BfWADfX9/AX0DBQQAAAABBQQBAUREBxgDBnNpbnRhdQAABGV4cDIAAQRsb2cyAAIKjgEEKQECfUMAAAA/IgIgACAAjpMiACACk4siAZMgAZZBAEEYEAMgAiAAk5gLGQAgACAAjiIAk0EYQSwQA7wgAKhBF3RqvgslAQF/IAC8IgFBF3ZB/wBrsiABQQl0s0MAAIBPlUEsQcQAEAOSCyIBAX0DQCADIACUIAEqAgCSIQMgAUEEaiIBIAJrDQALIAMLC0oBAEEAC0Q/x2FC2eATQUuqKsJzsqY9QAHJQH6V0DZv+V88kPJTPSJndz6sZjE/HQCAP/clMD0D/T++F6bRPkzcNL/Tgrg//IiKNwBqBG5hbWUBHwQABnNpbnRhdQEEZXhwMgIEbG9nMgMIZXZhbHBvbHkCNwQAAwABeAECeDECBGhhbGYBAQABeAICAAF4AQJ4aQMEAAF4AQVzdGFydAIDZW5kAwZyZXN1bHQDCQEDAQAEbG9vcA=="
            }';"#,
        )
        .await
        .unwrap();
    context.collect(plan).await.unwrap();

    // Reload to make sure we picked up the stored function from the metadata store
    context.reload_schema().await;

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
}
