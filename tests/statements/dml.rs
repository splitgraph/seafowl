use crate::statements::*;

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
        "|                 | 1111           |                  | 2022-01-01T20:01:01 | 42         |",
        "|                 | 2222           |                  | 2022-01-01T20:02:02 | 43         |",
        "|                 | 3333           |                  | 2022-01-01T20:03:03 | 44         |",
        "| false           |                | 2.1500000000     |                     | 41         |",
        "| true            |                | 9.1200000000     |                     | 45         |",
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
async fn test_delete_statement() {
    let context = make_context_with_pg().await;

    create_table_and_some_partitions(&context, "test_table", None).await;

    // Check DELETE's query plan to make sure 46 (int) gets cast to a float value
    // by the optimizer
    // (NB: EXPLAIN isn't supported for user-defined nodes)
    let plan = context
        .create_logical_plan("DELETE FROM test_table WHERE some_value > 46")
        .await
        .unwrap();
    assert_eq!(
        format!("{}", plan.display()),
        "Delete: test_table WHERE some_value > Float32(46)"
    );

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
async fn test_delete_with_string_filter_exact_match() {
    let context = make_context_with_pg().await;

    context
        .collect(
            context
                .plan_query("CREATE TABLE test_table(partition TEXT, value INTEGER)")
                .await
                .unwrap(),
        )
        .await
        .unwrap();

    context
        .collect(
            context
                .plan_query("INSERT INTO test_table VALUES('one', 1)")
                .await
                .unwrap(),
        )
        .await
        .unwrap();
    context
        .collect(
            context
                .plan_query("INSERT INTO test_table VALUES('two', 2)")
                .await
                .unwrap(),
        )
        .await
        .unwrap();
    context
        .collect(
            context
                .plan_query("INSERT INTO test_table VALUES('three', 3)")
                .await
                .unwrap(),
        )
        .await
        .unwrap();

    // Filter that exactly matches partition 2
    context
        .collect(
            context
                .plan_query("DELETE FROM test_table WHERE partition = 'two'")
                .await
                .unwrap(),
        )
        .await
        .unwrap();

    let partitions = context
        .partition_catalog
        .load_table_partitions(5 as TableVersionId)
        .await
        .unwrap();

    // For some reason, the initial pruning in a DELETE doesn't discard the two
    // partitions that definitely don't match (partition != 'two'), so we end up
    // with a new partition (if we delete where value = 2, this does result in the other
    // two partitions being kept as-is, so could have something to do with strings)
    let results =
        scan_partition(&context, None, partitions[0].clone(), "test_table").await;
    let expected = vec![
        "+-----------+-------+",
        "| partition | value |",
        "+-----------+-------+",
        "| one       | 1     |",
        "| three     | 3     |",
        "+-----------+-------+",
    ];
    assert_batches_eq!(expected, &results);

    let plan = context
        .plan_query("SELECT * FROM test_table ORDER BY value ASC")
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();
    assert_batches_eq!(expected, &results);
}

#[tokio::test]
async fn test_update_statement() {
    let context = make_context_with_pg().await;

    create_table_and_some_partitions(&context, "test_table", None).await;

    // Check the UPDATE query plan to make sure IN (41, 42, 43) (int) get cast to a float value
    let query = "UPDATE test_table
    SET some_time = '2022-01-01 21:21:21Z', some_int_value = 5555, some_value = some_value - 10
    WHERE some_value IN (41, 42, 43)";

    let plan = context.create_logical_plan(query).await.unwrap();
    assert_eq!(
        format!("{}", plan.display()),
        "Update: test_table, SET: some_time = Utf8(\"2022-01-01 21:21:21Z\"), some_int_value = Int64(5555), some_value = some_value - CAST(Int64(10) AS Float32) WHERE some_value IN ([CAST(Int64(41) AS Float32), CAST(Int64(42) AS Float32), CAST(Int64(43) AS Float32)])"
    );

    //
    // Execute UPDATE with a selection, affecting partitions 1 and 4, and creating table_version 6
    //
    let plan = context.plan_query(query).await.unwrap();
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
        "|                 | 5555           |                  | 2022-01-01T21:21:21 | 32         |",
        "|                 | 5555           |                  | 2022-01-01T21:21:21 | 33         |",
        "|                 | 3333           |                  | 2022-01-01T20:03:03 | 44         |",
        "+-----------------+----------------+------------------+---------------------+------------+",
    ];
    assert_batches_eq!(expected, &results);

    let results =
        scan_partition(&context, None, partitions[3].clone(), "test_table").await;
    let expected = vec![
        "+-----------------+----------------+------------------+---------------------+------------+",
        "| some_bool_value | some_int_value | some_other_value | some_time           | some_value |",
        "+-----------------+----------------+------------------+---------------------+------------+",
        "|                 | 5555           |                  | 2022-01-01T21:21:21 | 32         |",
        "|                 | 5555           |                  | 2022-01-01T21:21:21 | 31         |",
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
        "| true            | 5555           | 5.5550000000     | 2022-01-01T21:21:21 | 42         |",
        "| true            | 5555           | 5.5550000000     | 2022-01-01T21:21:21 | 42         |",
        "| false           | 3333           | 3.3330000000     | 2022-01-01T20:03:03 | 42         |",
        "| true            | 5555           | 5.5550000000     | 2022-01-01T21:21:21 | 42         |",
        "| true            | 5555           | 5.5550000000     | 2022-01-01T21:21:21 | 42         |",
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
