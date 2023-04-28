use crate::statements::*;
use tokio::fs::{copy, read_dir};

/// Make a SeafowlContext that's connected to a legacy SQLite catalog copy
async fn make_context_with_local_sqlite(data_dir: String) -> DefaultSeafowlContext {
    assert_ne!(data_dir.as_str(), "tests/data/seafowl-legacy-data/");

    // Copy the legacy catalog into the provided data directory
    let mut legacy_data = read_dir("tests/data/seafowl-legacy-data/").await.unwrap();
    while let Some(dir_entry) = legacy_data.next_entry().await.unwrap() {
        let file = dir_entry.file_name();
        copy(
            dir_entry.path(),
            format!("{}/{}", data_dir.clone(), file.to_str().unwrap()),
        )
        .await
        .unwrap();
    }

    let config_text = format!(
        r#"
[object_store]
type = "local"
data_dir = "{data_dir}"

[catalog]
type = "sqlite"
dsn = "{data_dir}/seafowl.sqlite""#
    );

    let config = load_config_from_string(&config_text, true, None).unwrap();
    build_context(&config).await.unwrap()
}

#[tokio::test]
async fn test_legacy_tables() {
    let data_dir = TempDir::new().unwrap();

    let context =
        make_context_with_local_sqlite(data_dir.path().display().to_string()).await;

    //
    // For start test that migration actually works
    //
    let plan = context
        .plan_query(
            r"
        CREATE TABLE test_migration AS
        (SELECT
            to_timestamp_micros(some_time) AS some_time,
            some_value,
            some_other_value,
            some_bool_value,
            some_int_value
         FROM test_table ORDER BY some_value, some_int_value
        )",
        )
        .await
        .unwrap();
    context.collect(plan).await.unwrap();

    let plan = context
        .plan_query("SELECT * FROM test_migration")
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();

    let expected = vec![
        "+---------------------+------------+------------------+-----------------+----------------+",
        "| some_time           | some_value | some_other_value | some_bool_value | some_int_value |",
        "+---------------------+------------+------------------+-----------------+----------------+",
        "|                     | 40.0       |                  |                 |                |",
        "|                     | 41.0       |                  |                 |                |",
        "| 2022-01-01T20:01:01 | 42.0       |                  |                 | 1111           |",
        "|                     | 42.0       |                  |                 |                |",
        "| 2022-01-01T20:02:02 | 43.0       |                  |                 | 2222           |",
        "| 2022-01-01T20:03:03 | 44.0       |                  |                 | 3333           |",
        "|                     | 45.0       |                  |                 |                |",
        "|                     | 46.0       |                  |                 |                |",
        "|                     | 46.0       |                  |                 |                |",
        "|                     | 47.0       |                  |                 |                |",
        "|                     | 47.0       |                  |                 |                |",
        "|                     | 48.0       |                  |                 |                |",
        "+---------------------+------------+------------------+-----------------+----------------+",
    ];

    assert_batches_eq!(expected, &results);

    //
    // Create the accompanying new tables with same content (though different column/row order) to
    // be used in comparisons
    //
    let (_, version_timestamps) = create_table_and_some_partitions(
        &context,
        "test_new_table",
        Some(Duration::from_secs(1)),
    )
    .await;

    let timestamp_to_rfc3339 = |timestamp: Timestamp| -> String {
        Utc.timestamp_opt(timestamp, 0).unwrap().to_rfc3339()
    };

    //
    // Verify that the legacy table versions are shown in the corresponding system table
    //

    let plan = context
        .plan_query("SELECT * FROM system.table_versions WHERE version = -1")
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();

    let expected = vec![
        "+--------------+------------+------------------+---------+---------------------+",
        "| table_schema | table_name | table_version_id | version | creation_time       |",
        "+--------------+------------+------------------+---------+---------------------+",
        "| public       | test_table | 1                | -1      | 2023-03-07T08:44:49 |",
        "| public       | test_table | 2                | -1      | 2023-03-07T08:44:49 |",
        "| public       | test_table | 3                | -1      | 2023-03-07T08:44:51 |",
        "| public       | test_table | 4                | -1      | 2023-03-07T08:44:53 |",
        "| public       | test_table | 5                | -1      | 2023-03-07T08:44:55 |",
        "+--------------+------------+------------------+---------+---------------------+",
    ];
    assert_batches_eq!(expected, &results);

    //
    // Test that filtering the system table works, given that we provide all rows to DF and expect
    // it to do it.
    //
    let plan = context
        .plan_query("
            SELECT table_version_id FROM system.table_versions \
            WHERE table_version_id < 5 AND creation_time > to_timestamp('2023-03-07T08:44:49+00:00')
        ")
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
    // Verify that the legacy table partitions for all versions are shown in the corresponding system table
    //

    let plan = context
        .plan_query("SELECT table_schema, table_name, table_version_id, table_partition_id, row_count FROM system.table_partitions")
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();

    let expected = vec![
        "+--------------+------------+------------------+--------------------+-----------+",
        "| table_schema | table_name | table_version_id | table_partition_id | row_count |",
        "+--------------+------------+------------------+--------------------+-----------+",
        "| public       | test_table | 1                |                    |           |",
        "| public       | test_table | 2                | 1                  | 3         |",
        "| public       | test_table | 3                | 1                  | 3         |",
        "| public       | test_table | 3                | 2                  | 3         |",
        "| public       | test_table | 4                | 1                  | 3         |",
        "| public       | test_table | 4                | 2                  | 3         |",
        "| public       | test_table | 4                | 3                  | 3         |",
        "| public       | test_table | 5                | 1                  | 3         |",
        "| public       | test_table | 5                | 2                  | 3         |",
        "| public       | test_table | 5                | 3                  | 3         |",
        "| public       | test_table | 5                | 4                  | 3         |",
        "+--------------+------------+------------------+--------------------+-----------+",
    ];
    assert_batches_eq!(expected, &results);

    //
    // Test some projections and aggregations
    //

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
        "| 3333                           | 0                                          | 48.0                       |",
        "+--------------------------------+--------------------------------------------+----------------------------+",
    ];

    assert_batches_eq!(expected, &results);

    //
    // Now use the recorded timestamps to query specific legacy and new table versions and compare
    // them to each other.
    //

    async fn compare_legacy_and_new_table_version(
        context: &DefaultSeafowlContext,
        version_id: TableVersionId,
        version_timestamps: &HashMap<DeltaDataTypeVersion, Timestamp>,
        timestamp_converter: fn(Timestamp) -> String,
    ) {
        let legacy_version_timestamps = [
            "2023-03-07T08:44:49+00:00",
            "2023-03-07T08:44:49+00:00",
            "2023-03-07T08:44:51+00:00",
            "2023-03-07T08:44:53+00:00",
            "2023-03-07T08:44:55+00:00",
        ];
        let plan = context
            .plan_query(
                format!(
                    "SELECT some_time, some_value, some_other_value, some_bool_value, some_int_value FROM test_table('{}') ORDER BY some_value",
                    legacy_version_timestamps[version_id as usize]
                )
                .as_str(),
            )
            .await
            .unwrap();
        let results = context.collect(plan).await.unwrap();

        let formatted = arrow::util::pretty::pretty_format_batches(results.as_slice())
            .unwrap()
            .to_string();

        let plan = context
            .plan_query(
                format!(
                    "SELECT * FROM test_new_table('{}') ORDER BY some_value",
                    timestamp_converter(version_timestamps[&version_id])
                )
                .as_str(),
            )
            .await
            .unwrap();
        let new_results = context.collect(plan).await.unwrap();

        let new_formatted =
            arrow::util::pretty::pretty_format_batches(new_results.as_slice())
                .unwrap()
                .to_string();

        assert_eq!(formatted, new_formatted);
    }

    for version_id in [1, 2, 3, 4] {
        compare_legacy_and_new_table_version(
            &context,
            version_id as TableVersionId,
            &version_timestamps,
            timestamp_to_rfc3339,
        )
        .await;
    }

    //
    // Try to query a non-existent legacy version (timestamp older than the oldest version)
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
            r#"
            WITH diff_3_2 AS (
                SELECT * FROM test_table('2023-03-07T08:44:51+00:00')
                EXCEPT
                SELECT * FROM test_table('2023-03-07T08:44:49+00:00')
            ), diff_4_3 AS (
                SELECT * FROM test_table('2023-03-07T08:44:53+00:00')
                EXCEPT
                SELECT * FROM test_table('2023-03-07T08:44:51+00:00')
            ), diff_5_4 AS (
                SELECT * FROM test_table('2023-03-07T08:44:55+00:00')
                EXCEPT
                SELECT * FROM test_table('2023-03-07T08:44:53+00:00')
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
        )
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();

    let expected = vec![
        "+-----------------+----------------+------------------+---------------------+------------+",
        "| some_bool_value | some_int_value | some_other_value | some_time           | some_value |",
        "+-----------------+----------------+------------------+---------------------+------------+",
        "|                 | 1111           |                  | 2022-01-01T20:01:01 | 42.0       |",
        "|                 | 2222           |                  | 2022-01-01T20:02:02 | 43.0       |",
        "|                 | 3333           |                  | 2022-01-01T20:03:03 | 44.0       |",
        "+-----------------+----------------+------------------+---------------------+------------+",
    ];

    assert_batches_eq!(expected, &results);

    // Ensure the context table map contains the versioned + the latest table entries
    assert_eq!(
        sorted(
            context
                .inner()
                .state()
                .catalog_list()
                .catalog(DEFAULT_DB)
                .unwrap()
                .schema(DEFAULT_SCHEMA)
                .unwrap()
                .table_names()
                .into_iter()
                .filter(|name| !name.contains("new"))
        )
        .collect::<Vec<String>>(),
        vec![
            "test_migration".to_string(),
            "test_table".to_string(),
            "test_table:2".to_string(),
            "test_table:3".to_string(),
            "test_table:4".to_string(),
            "test_table:5".to_string(),
        ],
    );

    //
    // Verify that information schema is not polluted with versioned tables/columns
    //

    let results = list_tables_query(&context).await;

    let expected = vec![
        "+--------------------+----------------+",
        "| table_schema       | table_name     |",
        "+--------------------+----------------+",
        "| information_schema | columns        |",
        "| information_schema | df_settings    |",
        "| information_schema | tables         |",
        "| information_schema | views          |",
        "| public             | test_migration |",
        "| public             | test_new_table |",
        "| public             | test_table     |",
        "+--------------------+----------------+",
    ];
    assert_batches_eq!(expected, &results);

    let results = list_columns_query(&context).await;

    let expected = vec![
        "+--------------+----------------+------------------+------------------------------+",
        "| table_schema | table_name     | column_name      | data_type                    |",
        "+--------------+----------------+------------------+------------------------------+",
        "| public       | test_migration | some_time        | Timestamp(Microsecond, None) |",
        "| public       | test_migration | some_value       | Float32                      |",
        "| public       | test_migration | some_other_value | Decimal128(38, 10)           |",
        "| public       | test_migration | some_bool_value  | Boolean                      |",
        "| public       | test_migration | some_int_value   | Int64                        |",
        "| public       | test_new_table | some_time        | Timestamp(Microsecond, None) |",
        "| public       | test_new_table | some_value       | Float32                      |",
        "| public       | test_new_table | some_other_value | Decimal128(38, 10)           |",
        "| public       | test_new_table | some_bool_value  | Boolean                      |",
        "| public       | test_new_table | some_int_value   | Int64                        |",
        "| public       | test_table     | some_bool_value  | Boolean                      |",
        "| public       | test_table     | some_int_value   | Int64                        |",
        "| public       | test_table     | some_other_value | Decimal128(38, 10)           |",
        "| public       | test_table     | some_time        | Timestamp(Nanosecond, None)  |",
        "| public       | test_table     | some_value       | Float32                      |",
        "+--------------+----------------+------------------+------------------------------+",
    ];
    assert_batches_eq!(expected, &results);
}

#[tokio::test]
async fn test_vacuum_legacy_tables() {
    let data_dir = TempDir::new().unwrap();

    let context = Arc::new(
        make_context_with_local_sqlite(data_dir.path().display().to_string()).await,
    );

    let plan = context
        .plan_query("SELECT * FROM system.table_versions")
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();

    let expected = vec![
        "+--------------+------------+------------------+---------+---------------------+",
        "| table_schema | table_name | table_version_id | version | creation_time       |",
        "+--------------+------------+------------------+---------+---------------------+",
        "| public       | test_table | 1                | -1      | 2023-03-07T08:44:49 |",
        "| public       | test_table | 2                | -1      | 2023-03-07T08:44:49 |",
        "| public       | test_table | 3                | -1      | 2023-03-07T08:44:51 |",
        "| public       | test_table | 4                | -1      | 2023-03-07T08:44:53 |",
        "| public       | test_table | 5                | -1      | 2023-03-07T08:44:55 |",
        "+--------------+------------+------------------+---------+---------------------+",
    ];

    assert_batches_eq!(expected, &results);

    context
        .collect(context.plan_query("VACUUM TABLE test_table").await.unwrap())
        .await
        .unwrap();

    let plan = context
        .plan_query("SELECT * FROM system.table_versions")
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();

    let expected = vec![
        "+--------------+------------+------------------+---------+---------------------+",
        "| table_schema | table_name | table_version_id | version | creation_time       |",
        "+--------------+------------+------------------+---------+---------------------+",
        "| public       | test_table | 5                | -1      | 2023-03-07T08:44:55 |",
        "+--------------+------------+------------------+---------+---------------------+",
    ];

    assert_batches_eq!(expected, &results);

    // Make sure vacuuming partitions and all tables changes nothing
    context
        .collect(context.plan_query("VACUUM PARTITIONS").await.unwrap())
        .await
        .unwrap();
    context
        .collect(context.plan_query("VACUUM TABLE test_table").await.unwrap())
        .await
        .unwrap();

    // Check the test_table is still queryable
    let plan = context
        .plan_query("SELECT some_value FROM test_table ORDER BY some_value LIMIT 4")
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();

    let expected = vec![
        "+------------+",
        "| some_value |",
        "+------------+",
        "| 40.0       |",
        "| 41.0       |",
        "| 42.0       |",
        "| 42.0       |",
        "+------------+",
    ];

    assert_batches_eq!(expected, &results);

    // DROP TABLE
    context
        .collect(context.plan_query("DROP TABLE test_table").await.unwrap())
        .await
        .unwrap();

    let plan = context
        .plan_query("SELECT * FROM system.table_versions")
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();

    let expected = vec![
        "+--------------+------------+------------------+---------+---------------+",
        "| table_schema | table_name | table_version_id | version | creation_time |",
        "+--------------+------------+------------------+---------+---------------+",
        "+--------------+------------+------------------+---------+---------------+",
    ];

    assert_batches_eq!(expected, &results);

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

    // Check we have orphan partitions
    // NB: we deduplicate object storage IDs here to avoid running the DELETE call
    // twice, but we can have two different partition IDs with same object storage ID
    // See https://github.com/splitgraph/seafowl/issues/5
    let orphans = vec![
        "ea192fa7ae3b4abca9ded70e480c188e2c260ece02a810e5f1e2be41b0d6c0f6.parquet",
        "534e5cc396e5b24725993145821b864cbfb07c2d8d7116f3d60d28bc02900861.parquet",
        "9ae6f4222893474551037d0e44ff223ca5ea8e703d141b14835025923a66ab50.parquet",
        "7fbfeeeade71978b4ae82cd3d97b8c1bd9ae7ab9a7a78ee541b66209cfd7722d.parquet",
    ];

    assert_orphan_partitions(context.clone(), orphans.clone()).await;
    let object_metas = get_object_metas().await;
    assert_eq!(object_metas.len(), 7);
    for object_meta in object_metas {
        // We're skipping the 3 seafowl.sqlite* files
        let path = object_meta.location.to_string();
        if !path.starts_with("seafowl.sqlite") {
            assert!(orphans.contains(&path.as_str()));
        }
    }

    // Run vacuum on partitions
    context
        .collect(context.plan_query("VACUUM PARTITIONS").await.unwrap())
        .await
        .unwrap();

    // Ensure no orphan partitions are left
    assert_orphan_partitions(context.clone(), vec![]).await;
    let object_metas = get_object_metas().await;
    assert_eq!(object_metas.len(), 3);
}
