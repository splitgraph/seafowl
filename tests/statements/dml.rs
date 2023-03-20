use crate::statements::*;

#[tokio::test]
async fn test_insert_two_different_schemas() {
    let context = make_context_with_pg(ObjectStoreType::InMemory).await;
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
        "+---------------------+------------+------------------+-----------------+----------------+",
        "| some_time           | some_value | some_other_value | some_bool_value | some_int_value |",
        "+---------------------+------------+------------------+-----------------+----------------+",
        "| 2022-01-01T20:01:01 | 42.0       |                  |                 | 1111           |",
        "| 2022-01-01T20:02:02 | 43.0       |                  |                 | 2222           |",
        "| 2022-01-01T20:03:03 | 44.0       |                  |                 | 3333           |",
        "|                     | 41.0       | 2.1500000000     | false           |                |",
        "|                     | 45.0       | 9.1200000000     | true            |                |",
        "|                     |            | 44.3400000000    | false           |                |",
        "+---------------------+------------+------------------+-----------------+----------------+",
    ];
    assert_batches_eq!(expected, &results);
}

#[tokio::test]
async fn test_delete_statement() {
    let context = make_context_with_pg(ObjectStoreType::InMemory).await;

    create_table_and_some_partitions(&context, "test_table", None).await;

    //
    // Check DELETE's query plan to make sure 46 (int) gets cast to a float value by the optimizer
    //
    let plan = context
        .create_logical_plan("DELETE FROM test_table WHERE some_value > 46")
        .await
        .unwrap();
    assert_eq!(
        format!("{}", plan.display_indent()),
        r#"Dml: op=[Delete] table=[test_table]
  Filter: some_value > Float32(46)
    TableScan: test_table projection=[some_time, some_value, some_other_value, some_bool_value, some_int_value], partial_filters=[some_value > Float32(46)]"#
    );

    //
    // Execute DELETE affecting partitions two of the partitions and creating new table_version
    //
    let plan = context
        .plan_query("DELETE FROM test_table WHERE some_value > 46")
        .await
        .unwrap();
    context.collect(plan).await.unwrap();

    // Verify results
    let plan = context
        .plan_query("SELECT some_value FROM test_table ORDER BY some_value")
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
        "| 43.0       |",
        "| 44.0       |",
        "| 45.0       |",
        "| 46.0       |",
        "| 46.0       |",
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

    let plan = context
        .plan_query("SELECT some_value FROM test_table ORDER BY some_value")
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();
    assert_batches_eq!(expected, &results);

    //
    // Add another partition for a new table_version
    //
    let plan = context
        .plan_query("INSERT INTO test_table (some_value) VALUES (48), (49), (50)")
        .await
        .unwrap();
    context.collect(plan).await.unwrap();

    //
    // Execute DELETE not affecting only partition with id 4, while trimming/combining the rest
    //
    let plan = context
        .plan_query("DELETE FROM test_table WHERE some_value IN (43, 45, 49)")
        .await
        .unwrap();
    context.collect(plan).await.unwrap();

    let plan = context
        .plan_query("SELECT some_value FROM test_table ORDER BY some_value")
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
        "| 44.0       |",
        "| 46.0       |",
        "| 46.0       |",
        "| 48.0       |",
        "| 50.0       |",
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

    let plan = context
        .plan_query("SELECT some_value FROM test_table ORDER BY some_value")
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();

    let expected = vec![
        "+------------+",
        "| some_value |",
        "+------------+",
        "| 41.0       |",
        "| 42.0       |",
        "| 42.0       |",
        "| 44.0       |",
        "| 46.0       |",
        "| 46.0       |",
        "+------------+",
    ];
    assert_batches_eq!(expected, &results);

    //
    // Execute blank DELETE, without qualifiers
    //
    let plan = context.plan_query("DELETE FROM test_table").await.unwrap();
    context.collect(plan).await.unwrap();

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
    let context = make_context_with_pg(ObjectStoreType::InMemory).await;

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

    let plan = context
        .plan_query("SELECT * FROM test_table ORDER BY value ASC")
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();
    let expected = vec![
        "+-----------+-------+",
        "| partition | value |",
        "+-----------+-------+",
        "| one       | 1     |",
        "| three     | 3     |",
        "+-----------+-------+",
    ];
    assert_batches_eq!(expected, &results);
}

#[tokio::test]
async fn test_update_statement() {
    let context = make_context_with_pg(ObjectStoreType::InMemory).await;

    create_table_and_some_partitions(&context, "test_table", None).await;

    //
    // Execute UPDATE with a selection, affecting only some files
    //
    let query = "UPDATE test_table
    SET some_time = '2022-01-01 21:21:21Z', some_int_value = 5555, some_value = some_value - 10
    WHERE some_value IN (41, 42, 43)";

    let plan = context.create_logical_plan(query).await.unwrap();

    // Check the UPDATE query plan to make sure IN (41, 42, 43) (int) get cast to a float value
    assert_eq!(
        format!("{}", plan.display_indent()),
        r#"Dml: op=[Update] table=[test_table]
  Projection: Utf8("2022-01-01 21:21:21Z") AS some_time, test_table.some_value - Float32(10) AS some_value, test_table.some_other_value AS some_other_value, test_table.some_bool_value AS some_bool_value, Int64(5555) AS some_int_value
    Filter: some_value = Float32(43) OR some_value = Float32(42) OR some_value = Float32(41)
      TableScan: test_table"#
    );

    // Now check the results
    let plan = context.plan_query(query).await.unwrap();
    context.collect(plan).await.unwrap();

    let plan = context
        .plan_query("SELECT * FROM test_table ORDER BY some_value")
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();
    let expected = vec![
        "+---------------------+------------+------------------+-----------------+----------------+",
        "| some_time           | some_value | some_other_value | some_bool_value | some_int_value |",
        "+---------------------+------------+------------------+-----------------+----------------+",
        "| 2022-01-01T21:21:21 | 31.0       |                  |                 | 5555           |",
        "| 2022-01-01T21:21:21 | 32.0       |                  |                 | 5555           |",
        "| 2022-01-01T21:21:21 | 32.0       |                  |                 | 5555           |",
        "| 2022-01-01T21:21:21 | 33.0       |                  |                 | 5555           |",
        "|                     | 40.0       |                  |                 |                |",
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
    // Execute UPDATE that doesn't change anything
    //
    let plan = context
        .plan_query("UPDATE test_table SET some_bool_value = TRUE WHERE some_value = 200")
        .await
        .unwrap();
    context.collect(plan).await.unwrap();

    let plan = context
        .plan_query("SELECT * FROM test_table ORDER BY some_value")
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();
    assert_batches_eq!(expected, &results);

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
        .contains("Cannot cast string 'nope' to value of Decimal128(38, 10) type"));

    //
    // Execute complex UPDATE (redundant assignment and a case assignment) without a selection
    //
    let plan = context
        .plan_query(
            "UPDATE test_table SET some_bool_value = FALSE, some_bool_value = (some_int_value = 5555), some_value = 42, \
            some_other_value = CASE WHEN some_int_value = 5555 THEN 5.555 WHEN some_int_value = 3333 THEN 3.333 ELSE 0 END"
        )
        .await
        .unwrap();
    context.collect(plan).await.unwrap();

    // Verify results
    let plan = context
        .plan_query("SELECT * FROM test_table")
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();

    let expected = vec![
        "+---------------------+------------+------------------+-----------------+----------------+",
        "| some_time           | some_value | some_other_value | some_bool_value | some_int_value |",
        "+---------------------+------------+------------------+-----------------+----------------+",
        "|                     | 42.0       | 0.0000000000     |                 |                |",
        "|                     | 42.0       | 0.0000000000     |                 |                |",
        "|                     | 42.0       | 0.0000000000     |                 |                |",
        "|                     | 42.0       | 0.0000000000     |                 |                |",
        "|                     | 42.0       | 0.0000000000     |                 |                |",
        "|                     | 42.0       | 0.0000000000     |                 |                |",
        "| 2022-01-01T21:21:21 | 42.0       | 5.5550000000     | true            | 5555           |",
        "| 2022-01-01T21:21:21 | 42.0       | 5.5550000000     | true            | 5555           |",
        "| 2022-01-01T20:03:03 | 42.0       | 3.3330000000     | false           | 3333           |",
        "| 2022-01-01T21:21:21 | 42.0       | 5.5550000000     | true            | 5555           |",
        "| 2022-01-01T21:21:21 | 42.0       | 5.5550000000     | true            | 5555           |",
        "|                     | 42.0       | 0.0000000000     |                 |                |",
        "+---------------------+------------+------------------+-----------------+----------------+",
    ];
    assert_batches_eq!(expected, &results);
}

#[tokio::test]
async fn test_update_statement_errors() {
    let context = make_context_with_pg(ObjectStoreType::InMemory).await;

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
