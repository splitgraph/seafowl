use crate::statements::*;

#[tokio::test]
async fn test_read_time_travel() {
    let (context, _temp_dir) = make_context_with_pg(ObjectStoreType::Local).await;
    let (version_results, version_timestamps) = create_table_and_some_partitions(
        &context,
        "test_table",
        Some(Duration::from_secs(1)),
    )
    .await;

    //
    // Verify that the new table versions are shown in the corresponding system table
    //

    let plan = context
        .plan_query("SELECT table_schema, table_name, version FROM system.table_versions")
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();

    let expected = [
        "+--------------+------------+---------+",
        "| table_schema | table_name | version |",
        "+--------------+------------+---------+",
        "| public       | test_table | 0       |",
        "| public       | test_table | 1       |",
        "| public       | test_table | 2       |",
        "| public       | test_table | 3       |",
        "| public       | test_table | 4       |",
        "+--------------+------------+---------+",
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
            SELECT version FROM system.table_versions \
            WHERE version < 4 AND creation_time > to_timestamp('{}')
        ",
                timestamp_to_rfc3339(version_timestamps[&1])
            )
            .as_str(),
        )
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();

    let expected = [
        "+---------+",
        "| version |",
        "+---------+",
        "| 2       |",
        "| 3       |",
        "+---------+",
    ];
    assert_batches_eq!(expected, &results);

    //
    // Now use the recorded timestamps to query specific earlier table versions and compare them to
    // the recorded results for that version.
    //

    async fn query_table_version(
        context: &DefaultSeafowlContext,
        version_id: i64,
        version_results: &HashMap<i64, Vec<RecordBatch>>,
        version_timestamps: &HashMap<i64, Timestamp>,
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

    for version_id in [1, 2, 3, 4] {
        query_table_version(
            &context,
            version_id,
            &version_results,
            &version_timestamps,
            timestamp_to_rfc3339,
        )
        .await;
    }

    //
    // Use multiple different version specifiers in the same complex query (including the latest
    // version both explicitly and in the default notation).
    // Ensures row differences between different versions are consistent:
    // 4 - ((4 - 3) + (3 - 2) + (2 - 1)) = 1
    //

    let plan = context
        .plan_query(
            format!(
                r#"
                WITH diff_2_1 AS (
                    SELECT * FROM test_table('{}')
                    EXCEPT
                    SELECT * FROM test_table('{}')
                ), diff_3_2 AS (
                    SELECT * FROM test_table('{}')
                    EXCEPT
                    SELECT * FROM test_table('{}')
                ), diff_4_3 AS (
                    SELECT * FROM test_table('{}')
                    EXCEPT
                    SELECT * FROM test_table('{}')
                )
                SELECT * FROM test_table
                EXCEPT (
                    SELECT * FROM diff_4_3
                    UNION
                    SELECT * FROM diff_3_2
                    UNION
                    SELECT * FROM diff_2_1
                )
                ORDER BY some_int_value
            "#,
                timestamp_to_rfc3339(version_timestamps[&2]),
                timestamp_to_rfc3339(version_timestamps[&1]),
                timestamp_to_rfc3339(version_timestamps[&3]),
                timestamp_to_rfc3339(version_timestamps[&2]),
                timestamp_to_rfc3339(version_timestamps[&4]),
                timestamp_to_rfc3339(version_timestamps[&3]),
            )
            .as_str(),
        )
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();
    assert_eq!(version_results[&1], results);

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
        )
        .collect::<Vec<String>>(),
        vec![
            "test_table".to_string(),
            format!(
                "test_table:{}",
                timestamp_to_rfc3339(version_timestamps[&1]).to_ascii_lowercase()
            ),
            format!(
                "test_table:{}",
                timestamp_to_rfc3339(version_timestamps[&2]).to_ascii_lowercase()
            ),
            format!(
                "test_table:{}",
                timestamp_to_rfc3339(version_timestamps[&3]).to_ascii_lowercase()
            ),
            format!(
                "test_table:{}",
                timestamp_to_rfc3339(version_timestamps[&4]).to_ascii_lowercase()
            ),
        ],
    );

    //
    // Verify that information schema is not polluted with versioned tables/columns
    //

    let results = list_tables_query(&context).await;

    let expected = [
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
async fn test_write_time_travel() {
    let (context, _temp_dir) = make_context_with_pg(ObjectStoreType::Local).await;
    let (_version_results, version_timestamps) = create_table_and_some_partitions(
        &context,
        "test_table",
        Some(Duration::from_secs(1)),
    )
    .await;

    let timestamp_to_rfc3339 = |timestamp: Timestamp| -> String {
        Utc.timestamp_opt(timestamp, 0).unwrap().to_rfc3339()
    };

    //
    // Now create a new table from an earlier version of the input table, using the recorded
    // timestamps and the time travel syntax
    //

    context
        .plan_query(
            format!(
                r#"
                CREATE TABLE diff_table AS (
                    SELECT * FROM test_table('{}')
                    EXCEPT
                    SELECT * FROM test_table('{}')
                    ORDER BY some_other_value, some_value
                )
            "#,
                timestamp_to_rfc3339(version_timestamps[&4]),
                timestamp_to_rfc3339(version_timestamps[&2]),
            )
            .as_str(),
        )
        .await
        .unwrap();

    // Test that results are as epxected
    let plan = context
        .plan_query("SELECT some_value, some_other_value FROM diff_table")
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();

    let expected = [
        "+------------+------------------+",
        "| some_value | some_other_value |",
        "+------------+------------------+",
        "| 46.0       | 3.0000000000     |",
        "| 47.0       | 3.0000000000     |",
        "| 48.0       | 3.0000000000     |",
        "| 40.0       | 4.0000000000     |",
        "| 41.0       | 4.0000000000     |",
        "| 42.0       | 4.0000000000     |",
        "+------------+------------------+",
    ];
    assert_batches_eq!(expected, &results);

    //
    // Finally try to INSERT data from the first version of the table
    //

    context
        .plan_query(
            format!(
                r#"
                INSERT INTO diff_table SELECT * FROM test_table('{}')
            "#,
                timestamp_to_rfc3339(version_timestamps[&1]),
            )
            .as_str(),
        )
        .await
        .unwrap();

    let plan = context
        .plan_query("SELECT some_value, some_other_value FROM diff_table")
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();

    let expected = [
        "+------------+------------------+",
        "| some_value | some_other_value |",
        "+------------+------------------+",
        "| 46.0       | 3.0000000000     |",
        "| 47.0       | 3.0000000000     |",
        "| 48.0       | 3.0000000000     |",
        "| 40.0       | 4.0000000000     |",
        "| 41.0       | 4.0000000000     |",
        "| 42.0       | 4.0000000000     |",
        "| 42.0       | 1.0000000000     |",
        "| 43.0       | 1.0000000000     |",
        "| 44.0       | 1.0000000000     |",
        "+------------+------------------+",
    ];
    assert_batches_eq!(expected, &results);
}
