use crate::statements::*;

#[rstest]
#[tokio::test]
async fn test_insert_two_different_schemas(
    #[values(ObjectStoreType::InMemory, ObjectStoreType::Local, ObjectStoreType::S3)]
    object_store_type: ObjectStoreType,
) {
    let (context, _) = make_context_with_pg(object_store_type).await;
    create_table_and_insert(&context, "test_table").await;

    context
        .plan_query(
            "INSERT INTO test_table (some_value, some_bool_value, some_other_value) VALUES
                (41, FALSE, 2.15),
                (45, TRUE, 9.12),
                (NULL, FALSE, 44.34)",
        )
        .await
        .unwrap();

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

#[rstest]
#[tokio::test]
async fn test_delete_statement(
    #[values(ObjectStoreType::InMemory, ObjectStoreType::Local, ObjectStoreType::S3)]
    object_store_type: ObjectStoreType,
) {
    let (context, _) = make_context_with_pg(object_store_type).await;

    create_table_and_some_partitions(&context, "test_table", None).await;

    //
    // Ensure we have 4 partitions, and record their file names
    //
    let mut table = context.try_get_delta_table("test_table").await.unwrap();
    table.load().await.unwrap();
    let mut all_partitions = table.get_files().clone();
    assert_eq!(all_partitions.len(), 4);
    let partition_1 = all_partitions.first().unwrap().clone();
    let partition_4 = all_partitions.last().unwrap().clone();

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
    // Execute DELETE affecting partitions two and three, and creating new table_version
    //
    context
        .plan_query("DELETE FROM test_table WHERE some_value > 46")
        .await
        .unwrap();

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

    // Ensure partitions 2 and 3 have been fused into a new partition, and record it.
    table.load().await.unwrap();
    match table.get_files().as_slice() {
        [f_1, f_2, f_3]
            if f_1 == &partition_1
                && f_2 == &partition_4
                && !all_partitions.contains(f_3) =>
        {
            all_partitions.push(f_3.clone())
        }
        _ => panic!("Expected exactly 2 inherited and 1 new partition"),
    };
    let partition_5 = all_partitions.last().unwrap().clone();

    //
    // Add another partition for a new table_version and record the new partition
    //
    context
        .plan_query("INSERT INTO test_table (some_value) VALUES (48), (49), (50)")
        .await
        .unwrap();

    // Expect too see a new (6th) partition
    table.load().await.unwrap();
    match table.get_files().as_slice() {
        [f_1, f_2, f_3, f_4]
            if f_1 == &partition_1
                && f_2 == &partition_4
                && f_3 == &partition_5
                && !all_partitions.contains(f_4) =>
        {
            all_partitions.push(f_4.clone())
        }
        _ => panic!("Expected exactly 3 inherited and 1 new partition"),
    };
    let _partition_6 = all_partitions.last().unwrap().clone();

    //
    // Execute DELETE not affecting only partition with id 4, while trimming/combining the rest
    //
    context
        .plan_query("DELETE FROM test_table WHERE some_value IN (43, 45, 49)")
        .await
        .unwrap();

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

    table.load().await.unwrap();
    match table.get_files().as_slice() {
        [f_1, f_2] if f_1 == &partition_4 && !all_partitions.contains(f_2) => {
            all_partitions.push(f_2.clone())
        }
        _ => panic!("Expected exactly 1 inherited and 1 new partition"),
    };
    let partition_7 = all_partitions.last().unwrap().clone();

    //
    // Execute a no-op DELETE, leaving the new table version the same as the prior one
    //

    context
        .plan_query("DELETE FROM test_table WHERE some_value < 35")
        .await
        .unwrap();

    let plan = context
        .plan_query("SELECT some_value FROM test_table ORDER BY some_value")
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();
    assert_batches_eq!(expected, &results);

    // Both partitions are inherited from the previous version
    table.load().await.unwrap();
    match table.get_files().as_slice() {
        [f_1, f_2] if f_1 == &partition_4 && f_2 == &partition_7 => {}
        _ => panic!("Expected same partitions as before"),
    };

    //
    // Execute DELETE with multiple qualifiers
    //
    context
        .plan_query("DELETE FROM test_table WHERE some_value < 41 OR some_value > 46")
        .await
        .unwrap();

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

    // Still only one partition file, but different from before
    table.load().await.unwrap();
    match table.get_files()[..] {
        [ref f_1] if !all_partitions.contains(f_1) => all_partitions.push(f_1.clone()),
        _ => panic!("Expected exactly 1 new partition different from the previous one"),
    };

    //
    // Execute blank DELETE, without qualifiers
    //
    context.plan_query("DELETE FROM test_table").await.unwrap();

    // Verify results
    let plan = context
        .plan_query("SELECT some_value FROM test_table")
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();

    assert!(results.is_empty());

    table.load().await.unwrap();
    assert!(table.get_files().is_empty())
}

#[tokio::test]
async fn test_delete_with_string_filter_exact_match() {
    let (context, _) = make_context_with_pg(ObjectStoreType::InMemory).await;

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

#[rstest]
#[tokio::test]
async fn test_update_statement(
    #[values(ObjectStoreType::InMemory, ObjectStoreType::Local, ObjectStoreType::S3)]
    object_store_type: ObjectStoreType,
) {
    let (context, _) = make_context_with_pg(object_store_type).await;

    create_table_and_some_partitions(&context, "test_table", None).await;

    //
    // Ensure we have 4 partitions, and record their file names
    //
    let mut table = context.try_get_delta_table("test_table").await.unwrap();
    table.load().await.unwrap();
    let mut all_partitions = table.get_files().clone();
    assert_eq!(all_partitions.len(), 4);
    let partition_2 = all_partitions[1].clone();
    let partition_3 = all_partitions[2].clone();

    //
    // Execute UPDATE with a selection, affecting only partitions 1 and 4
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

    // Now execute and check the results
    context.plan_query(query).await.unwrap();

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

    // Ensure partitions 1 and 4 have been fused into a new partition, and record it.
    table.load().await.unwrap();
    match table.get_files().as_slice() {
        [f_1, f_2, f_3]
            if f_1 == &partition_2
                && f_2 == &partition_3
                && !all_partitions.contains(f_3) =>
        {
            all_partitions.push(f_3.clone())
        }
        _ => panic!("Expected exactly 2 inherited and 1 new partition"),
    };
    let partition_5 = all_partitions.last().unwrap().clone();

    //
    // Execute UPDATE that doesn't change anything
    //
    context
        .plan_query("UPDATE test_table SET some_bool_value = TRUE WHERE some_value = 200")
        .await
        .unwrap();

    let plan = context
        .plan_query("SELECT * FROM test_table ORDER BY some_value")
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();
    assert_batches_eq!(expected, &results);

    // Ensure partitions from before are still there
    table.load().await.unwrap();
    match table.get_files().as_slice() {
        [f_1, f_2, f_3]
            if f_1 == &partition_2 && f_2 == &partition_3 && f_3 == &partition_5 => {}
        _ => panic!("Expected 3 inherited partitions"),
    };

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
    context
        .plan_query(
            "UPDATE test_table SET some_bool_value = FALSE, some_bool_value = (some_int_value = 5555), some_value = 42, \
            some_other_value = CASE WHEN some_int_value = 5555 THEN 5.555 WHEN some_int_value = 3333 THEN 3.333 ELSE 0 END"
        )
        .await
        .unwrap();

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

    table.load().await.unwrap();
    match table.get_files().as_slice() {
        [f_1] if !all_partitions.contains(f_1) => {}
        _ => panic!("Expected only 1 new partition"),
    };
}

#[tokio::test]
async fn test_update_statement_errors() {
    let (context, _) = make_context_with_pg(ObjectStoreType::InMemory).await;

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

    assert_eq!(
        err.to_string(),
        "Schema error: No field named nonexistent. Valid fields are some_time, some_value, some_other_value, some_bool_value, some_int_value."
    );

    let err = context
        .plan_query("UPDATE test_table SET some_value = 42 WHERE nonexistent = 32")
        .await
        .unwrap_err();

    assert_eq!(
        err.to_string(),
        "Schema error: No field named nonexistent. Valid fields are some_time, some_value, some_other_value, some_bool_value, some_int_value."
    );

    let err = context
        .plan_query("UPDATE test_table SET some_int_value = 'nope'")
        .await
        .unwrap_err();

    assert!(err
        .to_string()
        .contains("Cannot cast string 'nope' to value of Int64 type"));
}
