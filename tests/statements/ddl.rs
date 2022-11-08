#[tokio::test]
async fn test_create_table() {
    let (context, _) = make_context_with_pg().await;

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
async fn test_create_table_as() {
    let (context, _) = make_context_with_pg().await;
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

    let (context, _) = make_context_with_pg().await;

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
        "| information_schema | df_settings  |",
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
    assert!(results.is_empty())
}

#[tokio::test]
async fn test_create_table_drop_schema() {
    let (context, _) = make_context_with_pg().await;

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
        "| information_schema | df_settings  |",
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
        "| information_schema | df_settings  |",
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
        "| information_schema | df_settings  |",
        "| information_schema | tables       |",
        "| information_schema | views        |",
        "| public             | test_table_1 |",
        "+--------------------+--------------+",
    ];
    assert_batches_eq!(expected, &results);
}

#[tokio::test]
async fn test_create_table_schema_already_exists() {
    let (context, _) = make_context_with_pg().await;

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
    let (context, _) = make_context_with_pg().await;
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
async fn test_create_external_table_http() {
    /*
    Test CREATE EXTERNAL TABLE works with an HTTP mock server.

    This also works with https + actual S3 (tested manually)

    SELECT * FROM datafusion.public.supply_chains LIMIT 1 results in:

    bytes_scanned{filename=seafowl-public.s3.eu-west-1.amazonaws.com/tutorial/trase-supply-chains.parquet}=232699
    */

    let (mock_server, _) = http_testutils::make_mock_parquet_server(true, true).await;
    // Add a query string that's ignored by the mock (make sure DataFusion doesn't eat the whole URL)
    let url = format!(
        "{}/some/file.parquet?query_string=ignore",
        &mock_server.uri()
    );

    let (context, _) = make_context_with_pg().await;

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
