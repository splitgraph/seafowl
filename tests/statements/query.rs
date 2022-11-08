use crate::statements::*;
use test_case::test_case;

#[tokio::test]
async fn test_information_schema() {
    let (context, _) = make_context_with_pg().await;

    let plan = context
        .plan_query(
            "SELECT * FROM information_schema.tables ORDER BY table_catalog, table_name",
        )
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();

    let expected = vec![
        "+---------------+--------------------+----------------+------------+",
        "| table_catalog | table_schema       | table_name     | table_type |",
        "+---------------+--------------------+----------------+------------+",
        "| default       | information_schema | columns        | VIEW       |",
        "| default       | information_schema | df_settings    | VIEW       |",
        "| default       | system             | table_versions | VIEW       |",
        "| default       | information_schema | tables         | VIEW       |",
        "| default       | information_schema | views          | VIEW       |",
        "+---------------+--------------------+----------------+------------+",
    ];

    assert_batches_eq!(expected, &results);

    let plan = context
        .plan_query(
            format!(
                "SELECT table_schema, table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = '{}'
        ORDER BY table_name, ordinal_position",
                SYSTEM_SCHEMA,
            )
            .as_str(),
        )
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();

    let expected = vec![
        "+--------------+----------------+------------------+-------------------------+",
        "| table_schema | table_name     | column_name      | data_type               |",
        "+--------------+----------------+------------------+-------------------------+",
        "| system       | table_versions | table_schema     | Utf8                    |",
        "| system       | table_versions | table_name       | Utf8                    |",
        "| system       | table_versions | table_version_id | Int64                   |",
        "| system       | table_versions | creation_time    | Timestamp(Second, None) |",
        "+--------------+----------------+------------------+-------------------------+",
    ];
    assert_batches_eq!(expected, &results);
}

#[tokio::test]
async fn test_create_table_and_insert() {
    let (context, _) = make_context_with_pg().await;

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
        "|                 | 1111           |                  | 2022-01-01T20:01:01 | 42         |",
        "|                 | 2222           |                  | 2022-01-01T20:02:02 | 43         |",
        "|                 | 3333           |                  | 2022-01-01T20:03:03 | 44         |",
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
        "| 2022-01-01T20:03:03       |",
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
async fn test_table_time_travel() {
    let (context, _) = make_context_with_pg().await;
    let (version_results, version_timestamps) = create_table_and_some_partitions(
        &context,
        "test_table",
        Some(Duration::from_secs(1)),
    )
    .await;

    let timestamp_to_rfc3339 =
        |timestamp: Timestamp| -> String { Utc.timestamp(timestamp, 0).to_rfc3339() };

    //
    // Verify that the new table versions are shown in the corresponding system table
    //

    let plan = context
        .plan_query("SELECT table_schema, table_name, table_version_id FROM system.table_versions")
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();

    let expected = vec![
        "+--------------+------------+------------------+",
        "| table_schema | table_name | table_version_id |",
        "+--------------+------------+------------------+",
        "| public       | test_table | 1                |",
        "| public       | test_table | 2                |",
        "| public       | test_table | 3                |",
        "| public       | test_table | 4                |",
        "| public       | test_table | 5                |",
        "+--------------+------------+------------------+",
    ];
    assert_batches_eq!(expected, &results);

    //
    // Test that filtering the system table works, given that we provide all rows to DF and expect
    // it to do it.
    //
    let plan = context
        .plan_query(
            format!(
                "
            SELECT table_version_id FROM system.table_versions \
            WHERE table_version_id < 5 AND creation_time > to_timestamp('{}')
        ",
                timestamp_to_rfc3339(version_timestamps[&2])
            )
            .as_str(),
        )
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();

    let expected = vec![
        "+------------------+",
        "| table_version_id |",
        "+------------------+",
        "| 3                |",
        "| 4                |",
        "+------------------+",
    ];
    assert_batches_eq!(expected, &results);

    //
    // Now use the recorded timestamps to query specific earlier table versions and compare them to
    // the recorded results for that version.
    //

    async fn query_table_version(
        context: &DefaultSeafowlContext,
        version_id: TableVersionId,
        version_results: &HashMap<TableVersionId, Vec<RecordBatch>>,
        version_timestamps: &HashMap<TableVersionId, Timestamp>,
        timestamp_converter: fn(Timestamp) -> String,
    ) {
        let plan = context
            .plan_query(
                format!(
                    "SELECT * FROM test_table('{}')",
                    timestamp_converter(version_timestamps[&version_id])
                )
                .as_str(),
            )
            .await
            .unwrap();
        let results = context.collect(plan).await.unwrap();

        assert_eq!(version_results[&version_id], results);
    }

    for version_id in [2, 3, 4, 5] {
        query_table_version(
            &context,
            version_id as TableVersionId,
            &version_results,
            &version_timestamps,
            timestamp_to_rfc3339,
        )
        .await;
    }

    //
    // Try to query a non-existent version (timestamp older than the oldest version)
    //

    let err = context
        .plan_query("SELECT * FROM test_table('2012-12-21 20:12:21 +00:00')")
        .await
        .unwrap_err();

    assert!(err
        .to_string()
        .contains("No recorded table versions for the provided timestamp"));

    //
    // Use multiple different version specifiers in the same complex query (including the latest
    // version both explicitly and in the default notation).
    // Ensures row differences between different versions are consistent:
    // 5 - ((5 - 4) + (4 - 3) + (3 - 2)) = 2
    //

    let plan = context
        .plan_query(
            format!(
                r#"
                WITH diff_3_2 AS (
                    SELECT * FROM test_table('{}')
                    EXCEPT
                    SELECT * FROM test_table('{}')
                ), diff_4_3 AS (
                    SELECT * FROM test_table('{}')
                    EXCEPT
                    SELECT * FROM test_table('{}')
                ), diff_5_4 AS (
                    SELECT * FROM test_table('{}')
                    EXCEPT
                    SELECT * FROM test_table('{}')
                )
                SELECT * FROM test_table
                EXCEPT (
                    SELECT * FROM diff_5_4
                    UNION
                    SELECT * FROM diff_4_3
                    UNION
                    SELECT * FROM diff_3_2
                )
                ORDER BY some_int_value
            "#,
                timestamp_to_rfc3339(version_timestamps[&3]),
                timestamp_to_rfc3339(version_timestamps[&2]),
                timestamp_to_rfc3339(version_timestamps[&4]),
                timestamp_to_rfc3339(version_timestamps[&3]),
                timestamp_to_rfc3339(version_timestamps[&5]),
                timestamp_to_rfc3339(version_timestamps[&4]),
            )
            .as_str(),
        )
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();
    assert_eq!(version_results[&2], results);

    // Ensure the context table map contains the versioned + the latest table entries
    assert_eq!(
        sorted(
            context
                .inner()
                .state
                .read()
                .catalog_list
                .catalog(DEFAULT_DB)
                .unwrap()
                .schema(DEFAULT_SCHEMA)
                .unwrap()
                .table_names()
        )
        .collect::<Vec<String>>(),
        vec![
            "test_table".to_string(),
            "test_table:2".to_string(),
            "test_table:3".to_string(),
            "test_table:4".to_string(),
        ],
    );

    //
    // Verify that information schema is not polluted with versioned tables/columns
    //

    let results = list_tables_query(&context).await;

    let expected = vec![
        "+--------------------+-------------+",
        "| table_schema       | table_name  |",
        "+--------------------+-------------+",
        "| information_schema | columns     |",
        "| information_schema | df_settings |",
        "| information_schema | tables      |",
        "| information_schema | views       |",
        "| public             | test_table  |",
        "+--------------------+-------------+",
    ];
    assert_batches_eq!(expected, &results);

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

#[test_case(
    true;
    "schema introspected")
]
#[test_case(
    false;
    "schema declared")
]
#[tokio::test]
async fn test_remote_table_querying(introspect_schema: bool) {
    let (context, repo) = make_context_with_pg().await;

    // Create a table in our metadata store, and insert some dummy data
    repo.executor
        .execute(
            format!(
                "CREATE TABLE {}.\"source table\" (a INT, b FLOAT, c VARCHAR, \"date field\" DATE, e TIMESTAMP)",
                repo.schema_name
            )
            .as_str(),
        )
        .await
        .unwrap();
    repo.executor
        .execute(
            format!(
                "INSERT INTO {}.\"source table\" VALUES \
            (1, 1.1, 'one', '2022-11-01', '2022-11-01 22:11:01'),\
            (2, 2.22, 'two', '2022-11-02', '2022-11-02 22:11:02'),\
            (3, 3.333, 'three', '2022-11-03', '2022-11-03 22:11:03'),\
            (4, 4.4444, 'four', '2022-11-04', '2022-11-04 22:11:04')",
                repo.schema_name
            )
            .as_str(),
        )
        .await
        .unwrap();

    let table_column_schema = if introspect_schema {
        ""
    } else {
        "(a INT, b FLOAT, c VARCHAR, \"date field\" DATE, e TIMESTAMP)"
    };

    // Create a remote table (pointed at our metadata store table)
    let plan = context
        .plan_query(
            format!(
                "CREATE EXTERNAL TABLE remote_table {}
                STORED AS TABLE '{}.\"source table\"'
                LOCATION '{}'",
                table_column_schema,
                repo.schema_name,
                env::var("DATABASE_URL").unwrap()
            )
            .as_str(),
        )
        .await
        .unwrap();
    context.collect(plan).await.unwrap();

    // Query remote table
    let plan = context
        .plan_query("SELECT * FROM staging.remote_table")
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();

    let expected = if introspect_schema {
        // Connector-X coerces the TIMESTAMP field to Date64
        vec![
            "+---+--------+-------+------------+------------+",
            "| a | b      | c     | date field | e          |",
            "+---+--------+-------+------------+------------+",
            "| 1 | 1.1    | one   | 2022-11-01 | 2022-11-01 |",
            "| 2 | 2.22   | two   | 2022-11-02 | 2022-11-02 |",
            "| 3 | 3.333  | three | 2022-11-03 | 2022-11-03 |",
            "| 4 | 4.4444 | four  | 2022-11-04 | 2022-11-04 |",
            "+---+--------+-------+------------+------------+",
        ]
    } else {
        vec![
            "+---+--------+-------+------------+---------------------+",
            "| a | b      | c     | date field | e                   |",
            "+---+--------+-------+------------+---------------------+",
            "| 1 | 1.1    | one   | 2022-11-01 | 2022-11-01T22:11:01 |",
            "| 2 | 2.22   | two   | 2022-11-02 | 2022-11-02T22:11:02 |",
            "| 3 | 3.333  | three | 2022-11-03 | 2022-11-03T22:11:03 |",
            "| 4 | 4.4444 | four  | 2022-11-04 | 2022-11-04T22:11:04 |",
            "+---+--------+-------+------------+---------------------+",
        ]
    };
    assert_batches_eq!(expected, &results);

    // Test that projection and filtering work
    let plan = context
        .plan_query("SELECT \"date field\", b, c FROM staging.remote_table WHERE a > 3 OR c = 'two'")
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();

    let expected = vec![
        "+------------+--------+------+",
        "| date field | b      | c    |",
        "+------------+--------+------+",
        "| 2022-11-02 | 2.22   | two  |",
        "| 2022-11-04 | 4.4444 | four |",
        "+------------+--------+------+",
    ];
    assert_batches_eq!(expected, &results);

    // Verify column types in information schema
    let results = list_columns_query(&context).await;

    let expected = if introspect_schema {
        vec![
            "+--------------+--------------+-------------+-----------+",
            "| table_schema | table_name   | column_name | data_type |",
            "+--------------+--------------+-------------+-----------+",
            "| staging      | remote_table | a           | Int64     |",
            "| staging      | remote_table | b           | Float64   |",
            "| staging      | remote_table | c           | Utf8      |",
            "| staging      | remote_table | date field  | Date32    |",
            "| staging      | remote_table | e           | Date64    |",
            "+--------------+--------------+-------------+-----------+",
        ]
    } else {
        vec![
            "+--------------+--------------+-------------+-----------------------------+",
            "| table_schema | table_name   | column_name | data_type                   |",
            "+--------------+--------------+-------------+-----------------------------+",
            "| staging      | remote_table | a           | Int32                       |",
            "| staging      | remote_table | b           | Float32                     |",
            "| staging      | remote_table | c           | Utf8                        |",
            "| staging      | remote_table | date field  | Date32                      |",
            "| staging      | remote_table | e           | Timestamp(Nanosecond, None) |",
            "+--------------+--------------+-------------+-----------------------------+",
        ]
    };
    assert_batches_eq!(expected, &results);
}
