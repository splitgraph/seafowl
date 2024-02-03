use crate::statements::*;

#[rstest]
#[tokio::test]
async fn test_create_table(
    #[values(ObjectStoreType::InMemory, ObjectStoreType::Gcs)]
    object_store_type: ObjectStoreType,
) {
    let (context, _) = make_context_with_pg(object_store_type).await;

    context
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

    // Check table columns
    let results = list_columns_query(&context).await;

    let expected = [
        "+--------------+------------+------------------+------------------------------+",
        "| table_schema | table_name | column_name      | data_type                    |",
        "+--------------+------------+------------------+------------------------------+",
        "| public       | test_table | some_time        | Timestamp(Microsecond, None) |",
        "| public       | test_table | some_value       | Float32                      |",
        "| public       | test_table | some_other_value | Decimal128(38, 10)           |",
        "| public       | test_table | some_bool_value  | Boolean                      |",
        "| public       | test_table | some_int_value   | Int64                        |",
        "+--------------+------------+------------------+------------------------------+",
    ];

    assert_batches_eq!(expected, &results);
}

#[tokio::test]
async fn test_create_table_as() {
    let (context, _) = make_context_with_pg(ObjectStoreType::InMemory).await;
    create_table_and_insert(&context, "test_table").await;

    context
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

    let plan = context.plan_query("SELECT * FROM test_ctas").await.unwrap();
    let results = context.collect(plan).await.unwrap();

    let expected = [
        "+------------+----------------+-------------+",
        "| some_value | some_int_value | some_minute |",
        "+------------+----------------+-------------+",
        "| 49.0       | 3333           | 3.0         |",
        "| 48.0       | 2222           | 2.0         |",
        "| 47.0       | 1111           | 1.0         |",
        "+------------+----------------+-------------+",
    ];
    assert_batches_eq!(expected, &results);
}

#[tokio::test]
async fn test_create_table_as_from_ns_column() {
    let (context, _) = make_context_with_pg(ObjectStoreType::InMemory).await;

    // Create an external table containing a timestamp column in nanoseconds
    context
        .plan_query(
            "CREATE EXTERNAL TABLE table_with_ns_column \
            STORED AS PARQUET LOCATION 'tests/data/table_with_ns_column.parquet'",
        )
        .await
        .unwrap();

    // Create a table and check nanosecond is coerced into microsecond
    context
        .plan_query("CREATE TABLE table_with_us_column AS (SELECT * FROM staging.table_with_ns_column)")
        .await
        .unwrap();

    let results = list_columns_query(&context).await;

    let expected = ["+--------------+----------------------+----------------+------------------------------+",
        "| table_schema | table_name           | column_name    | data_type                    |",
        "+--------------+----------------------+----------------+------------------------------+",
        "| staging      | table_with_ns_column | some_int_value | Int64                        |",
        "| staging      | table_with_ns_column | some_time      | Timestamp(Nanosecond, None)  |",
        "| staging      | table_with_ns_column | some_value     | Float32                      |",
        "| public       | table_with_us_column | some_int_value | Int64                        |",
        "| public       | table_with_us_column | some_time      | Timestamp(Microsecond, None) |",
        "| public       | table_with_us_column | some_value     | Float32                      |",
        "+--------------+----------------------+----------------+------------------------------+"];

    assert_batches_eq!(expected, &results);

    // Check table is queryable
    let plan = context
        .plan_query("SELECT * FROM table_with_us_column")
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();

    let expected = [
        "+----------------+---------------------+------------+",
        "| some_int_value | some_time           | some_value |",
        "+----------------+---------------------+------------+",
        "| 1111           | 2022-01-01T20:01:01 | 42.0       |",
        "| 2222           | 2022-01-01T20:02:02 | 43.0       |",
        "| 3333           | 2022-01-01T20:03:03 | 44.0       |",
        "+----------------+---------------------+------------+",
    ];
    assert_batches_eq!(expected, &results);
}

#[rstest]
#[tokio::test]
async fn test_create_table_move_and_drop(
    #[values(
        ObjectStoreType::InMemory,
        ObjectStoreType::Local,
        ObjectStoreType::S3(None),
        ObjectStoreType::S3(Some("/path/to/folder"))
    )]
    object_store_type: ObjectStoreType,
) {
    // Create two tables, insert some data into them

    let (context, _) = make_context_with_pg(object_store_type).await;

    for table_name in ["test_table_1", "test_table_2"] {
        create_table_and_insert(&context, table_name).await;
    }

    let results = list_columns_query(&context).await;

    let expected = vec![
        "+--------------+--------------+------------------+------------------------------+",
        "| table_schema | table_name   | column_name      | data_type                    |",
        "+--------------+--------------+------------------+------------------------------+",
        "| public       | test_table_1 | some_time        | Timestamp(Microsecond, None) |",
        "| public       | test_table_1 | some_value       | Float32                      |",
        "| public       | test_table_1 | some_other_value | Decimal128(38, 10)           |",
        "| public       | test_table_1 | some_bool_value  | Boolean                      |",
        "| public       | test_table_1 | some_int_value   | Int64                        |",
        "| public       | test_table_2 | some_time        | Timestamp(Microsecond, None) |",
        "| public       | test_table_2 | some_value       | Float32                      |",
        "| public       | test_table_2 | some_other_value | Decimal128(38, 10)           |",
        "| public       | test_table_2 | some_bool_value  | Boolean                      |",
        "| public       | test_table_2 | some_int_value   | Int64                        |",
        "+--------------+--------------+------------------+------------------------------+",
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
        .plan_query("ALTER TABLE test_table_1 RENAME TO test_table_3")
        .await
        .unwrap();

    let results = list_tables_query(&context).await;

    let expected = [
        "+--------------------+--------------+",
        "| table_schema       | table_name   |",
        "+--------------------+--------------+",
        "| information_schema | columns      |",
        "| information_schema | df_settings  |",
        "| information_schema | tables       |",
        "| information_schema | views        |",
        "| public             | test_table_2 |",
        "| public             | test_table_3 |",
        "+--------------------+--------------+",
    ];
    assert_batches_eq!(expected, &results);

    // Move the table into a non-existent schema
    assert!(context
        .plan_query("ALTER TABLE test_table_3 RENAME TO \"new_./-~:schema\".test_table_3")
        .await
        .unwrap_err()
        .to_string()
        .contains("Schema \"new_./-~:schema\" doesn't exist"));

    // Create a schema and move the table to it
    context
        .plan_query("CREATE SCHEMA \"new_./-~:schema\"")
        .await
        .unwrap();

    context
        .plan_query("ALTER TABLE test_table_3 RENAME TO \"new_./-~:schema\".test_table_3")
        .await
        .unwrap();

    let results = list_tables_query(&context).await;

    let expected = [
        "+--------------------+--------------+",
        "| table_schema       | table_name   |",
        "+--------------------+--------------+",
        "| information_schema | columns      |",
        "| information_schema | df_settings  |",
        "| information_schema | tables       |",
        "| information_schema | views        |",
        "| new_./-~:schema    | test_table_3 |",
        "| public             | test_table_2 |",
        "+--------------------+--------------+",
    ];
    assert_batches_eq!(expected, &results);

    // Check that the renamed table is queryable
    let plan = context
        .plan_query("SELECT some_value FROM \"new_./-~:schema\".test_table_3")
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();

    let expected = [
        "+------------+",
        "| some_value |",
        "+------------+",
        "| 42.0       |",
        "| 43.0       |",
        "| 44.0       |",
        "+------------+",
    ];

    assert_batches_eq!(expected, &results);

    // Drop test_table_3
    context
        .plan_query("DROP TABLE \"new_./-~:schema\".test_table_3")
        .await
        .unwrap();

    let results = list_columns_query(&context).await;

    let expected = ["+--------------+--------------+------------------+------------------------------+",
        "| table_schema | table_name   | column_name      | data_type                    |",
        "+--------------+--------------+------------------+------------------------------+",
        "| public       | test_table_2 | some_time        | Timestamp(Microsecond, None) |",
        "| public       | test_table_2 | some_value       | Float32                      |",
        "| public       | test_table_2 | some_other_value | Decimal128(38, 10)           |",
        "| public       | test_table_2 | some_bool_value  | Boolean                      |",
        "| public       | test_table_2 | some_int_value   | Int64                        |",
        "+--------------+--------------+------------------+------------------------------+"];

    assert_batches_eq!(expected, &results);

    // Drop the second table

    context.plan_query("DROP TABLE test_table_2").await.unwrap();

    let results = list_columns_query(&context).await;
    assert!(results.is_empty())
}

#[rstest]
#[tokio::test]
async fn test_create_table_drop_schema(
    #[values(
        ObjectStoreType::InMemory,
        ObjectStoreType::Local,
        ObjectStoreType::S3(None),
        ObjectStoreType::S3(Some("/path/to/folder"))
    )]
    object_store_type: ObjectStoreType,
) -> Result<()> {
    let (context, _temp_dir) = make_context_with_pg(object_store_type).await;

    // Create a non-default schema
    context.plan_query("CREATE SCHEMA new_schema").await?;

    // Create a couple of tables in the default and the new schema
    for table_name in ["table_1", "table_2", "table_3"] {
        create_table_and_insert(&context, table_name).await;
        create_table_and_insert(&context, &format!("new_schema.{table_name}")).await;
    }

    let results = list_tables_query(&context).await;
    let expected = [
        "+--------------------+-------------+",
        "| table_schema       | table_name  |",
        "+--------------------+-------------+",
        "| information_schema | columns     |",
        "| information_schema | df_settings |",
        "| information_schema | tables      |",
        "| information_schema | views       |",
        "| new_schema         | table_1     |",
        "| new_schema         | table_2     |",
        "| new_schema         | table_3     |",
        "| public             | table_1     |",
        "| public             | table_2     |",
        "| public             | table_3     |",
        "+--------------------+-------------+",
    ];
    assert_batches_eq!(expected, &results);

    // Collect UUIDs of the tables in the public schema
    let table_1_uuid = context.get_table_uuid("table_1").await?;
    let table_2_uuid = context.get_table_uuid("table_2").await?;
    let table_3_uuid = context.get_table_uuid("table_3").await?;

    // DROP the public schema for the fun of it
    context
        .collect(context.plan_query("DROP SCHEMA public").await?)
        .await?;

    let results = list_tables_query(&context).await;

    let expected = [
        "+--------------------+-------------+",
        "| table_schema       | table_name  |",
        "+--------------------+-------------+",
        "| information_schema | columns     |",
        "| information_schema | df_settings |",
        "| information_schema | tables      |",
        "| information_schema | views       |",
        "| new_schema         | table_1     |",
        "| new_schema         | table_2     |",
        "| new_schema         | table_3     |",
        "+--------------------+-------------+",
    ];
    assert_batches_eq!(expected, &results);

    // Ensure the objects are dropped as well
    for table_uuid in [table_1_uuid, table_2_uuid, table_3_uuid] {
        testutils::assert_uploaded_objects(
            context
                .internal_object_store
                .get_log_store(&table_uuid.to_string())
                .object_store(),
            vec![],
        )
        .await;
    }

    // Assert that the objects in the new schema are left intact
    for table_name in [
        "new_schema.table_1",
        "new_schema.table_2",
        "new_schema.table_3",
    ] {
        let mut table = context.try_get_delta_table(table_name).await?;
        table.load().await?;
        let table_uuid = context.get_table_uuid(table_name).await?;

        testutils::assert_uploaded_objects(
            context
                .internal_object_store
                .get_log_store(&table_uuid.to_string())
                .object_store(),
            vec![
                String::from("_delta_log/00000000000000000000.json"),
                String::from("_delta_log/00000000000000000001.json"),
                table.snapshot()?.file_actions()?[0].clone().path,
            ],
        )
        .await;
    }

    // DROP the new_schema
    context
        .collect(context.plan_query("DROP SCHEMA new_schema").await?)
        .await?;

    let results = list_tables_query(&context).await;

    let expected = [
        "+--------------------+-------------+",
        "| table_schema       | table_name  |",
        "+--------------------+-------------+",
        "| information_schema | columns     |",
        "| information_schema | df_settings |",
        "| information_schema | tables      |",
        "| information_schema | views       |",
        "+--------------------+-------------+",
    ];
    assert_batches_eq!(expected, &results);

    // Recreate the public schema and add a table to it
    context.plan_query("CREATE SCHEMA public").await?;
    context
        .plan_query("CREATE TABLE table_1 (\"key\" INT)")
        .await?;

    let results = list_tables_query(&context).await;

    let expected = [
        "+--------------------+-------------+",
        "| table_schema       | table_name  |",
        "+--------------------+-------------+",
        "| information_schema | columns     |",
        "| information_schema | df_settings |",
        "| information_schema | tables      |",
        "| information_schema | views       |",
        "| public             | table_1     |",
        "+--------------------+-------------+",
    ];
    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn test_create_table_schema_already_exists() {
    let (context, _) = make_context_with_pg(ObjectStoreType::InMemory).await;

    context
        .collect(
            context
                .plan_query("CREATE TABLE some_table(\"key\" INT)")
                .await
                .unwrap(),
        )
        .await
        .unwrap();
    let err = context
        .plan_query("CREATE TABLE some_table(\"key\" INT)")
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
    let (context, _) = make_context_with_pg(ObjectStoreType::InMemory).await;

    context
        .plan_query("CREATE TABLE some_table(\"key\" INT)")
        .await
        .unwrap();

    let expected_err = "Error during planning: The staging schema can only be referenced via CREATE EXTERNAL TABLE";

    let err = context
        .plan_query("CREATE TABLE staging.some_table(\"key\" INT)")
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

// Test creating external table in the native object store type (`in`)
// that is stored in the external object store (`from`).
#[rstest]
#[case::in_mem_from_mock_http(None, "", ObjectStoreType::InMemory)]
#[case::in_mem_from_minio_http(
    Some("http://localhost:9000/seafowl-test-bucket/table_with_ns_column.parquet"),
    "",
    ObjectStoreType::InMemory
)]
// Tests the case of inheriting the credentials from the native object store, since none are
// provided via the `OPTIONS` clause.
#[case::in_minio_s3_from_minio_s3(
    Some("s3://seafowl-test-bucket/table_with_ns_column.parquet"),
    "",
    ObjectStoreType::S3(None)
)]
#[case::in_gcs_from_gcs(
    Some("gs://test-data/table_with_ns_column.parquet"),
    "",
    ObjectStoreType::Gcs
)]
// Tests the case of explicitly specifying the `OPTIONS` clause to construct a dynamic object store.
// so we can use anything other than the native object store.
#[case::in_mem_from_minio_s3_with_options(
    Some("s3://seafowl-test-bucket/table_with_ns_column.parquet"),
    " OPTIONS ('access_key_id' 'minioadmin', 'secret_access_key' 'minioadmin', 'endpoint' 'http://127.0.0.1:9000') ",
    ObjectStoreType::InMemory,
)]
#[case::in_gcs_from_minio_s3_with_options(
    Some("s3://seafowl-test-bucket/table_with_ns_column.parquet"),
    " OPTIONS ('access_key_id' 'minioadmin', 'secret_access_key' 'minioadmin', 'endpoint' 'http://127.0.0.1:9000') ",
    ObjectStoreType::Gcs,
)]
#[case::in_minio_s3_from_gcs_with_options(
    Some("gs://test-data/table_with_ns_column.parquet"),
    &format!(" OPTIONS ('google_application_credentials' '{FAKE_GCS_CREDS_PATH}') "),
    ObjectStoreType::S3(None),
)]
#[tokio::test]
async fn test_create_external_table(
    #[case] location: Option<&str>,
    #[case] options: &str,
    #[case] object_store_type: ObjectStoreType,
) {
    let url = match location {
        None => {
            let (mock_server, _) = testutils::make_mock_parquet_server(true, true).await;
            // Add a query string that's ignored by the mock (make sure DataFusion doesn't eat the whole URL)
            format!(
                "{}/some/file.parquet?query_string=ignore",
                &mock_server.uri()
            )
        }
        Some(url) => url.to_string(),
    };

    let (context, _) = make_context_with_pg(object_store_type).await;

    // Try creating a table in a non-staging schema
    let err = context
        .plan_query(
            format!(
                "CREATE EXTERNAL TABLE public.file
        STORED AS PARQUET
        LOCATION '{url}'"
            )
            .as_str(),
        )
        .await
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("Can only create external tables in the staging schema"));

    // Create a table normally
    context
        .plan_query(
            format!(
                "CREATE EXTERNAL TABLE file
        STORED AS PARQUET {options}
        LOCATION '{url}'"
            )
            .as_str(),
        )
        .await
        .unwrap();

    // Test we see the table in the information_schema
    let results = list_tables_query(&context).await;

    let expected = [
        "+--------------------+-------------+",
        "| table_schema       | table_name  |",
        "+--------------------+-------------+",
        "| information_schema | columns     |",
        "| information_schema | df_settings |",
        "| information_schema | tables      |",
        "| information_schema | views       |",
        "| staging            | file        |",
        "+--------------------+-------------+",
    ];
    assert_batches_eq!(expected, &results);

    // Test standard query
    let plan = context
        .plan_query("SELECT * FROM staging.file")
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();
    let expected = if location.is_none() {
        vec![
            "+-------+",
            "| col_1 |",
            "+-------+",
            "| 1     |",
            "| 2     |",
            "| 3     |",
            "+-------+",
        ]
    } else {
        vec![
            "+----------------+---------------------+------------+",
            "| some_int_value | some_time           | some_value |",
            "+----------------+---------------------+------------+",
            "| 1111           | 2022-01-01T20:01:01 | 42.0       |",
            "| 2222           | 2022-01-01T20:02:02 | 43.0       |",
            "| 3333           | 2022-01-01T20:03:03 | 44.0       |",
            "+----------------+---------------------+------------+",
        ]
    };

    assert_batches_eq!(expected, &results);

    // Test dropping the external table works
    context
        .collect(context.plan_query("DROP TABLE staging.file").await.unwrap())
        .await
        .unwrap();

    let results = list_tables_query(&context).await;

    let expected = [
        "+--------------------+-------------+",
        "| table_schema       | table_name  |",
        "+--------------------+-------------+",
        "| information_schema | columns     |",
        "| information_schema | df_settings |",
        "| information_schema | tables      |",
        "| information_schema | views       |",
        "+--------------------+-------------+",
    ];
    assert_batches_eq!(expected, &results);
}
