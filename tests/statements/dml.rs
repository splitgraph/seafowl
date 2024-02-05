use crate::statements::*;

#[rstest]
#[tokio::test]
async fn test_insert_two_different_schemas(
    #[values(
        ObjectStoreType::InMemory,
        ObjectStoreType::Local,
        ObjectStoreType::S3(None),
        ObjectStoreType::S3(Some("/path/to/folder"))
    )]
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
        .plan_query("SELECT * FROM test_table ORDER BY some_other_value")
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();

    let expected = [
        "+---------------------+------------+------------------+-----------------+----------------+",
        "| some_time           | some_value | some_other_value | some_bool_value | some_int_value |",
        "+---------------------+------------+------------------+-----------------+----------------+",
        "| 2022-01-01T20:01:01 | 42.0       | 1.0000000000     |                 | 1111           |",
        "| 2022-01-01T20:02:02 | 43.0       | 1.0000000000     |                 | 2222           |",
        "| 2022-01-01T20:03:03 | 44.0       | 1.0000000000     |                 | 3333           |",
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
    #[values(
        ObjectStoreType::InMemory,
        ObjectStoreType::Local,
        ObjectStoreType::S3(None),
        ObjectStoreType::S3(Some("/path/to/folder"))
    )]
    object_store_type: ObjectStoreType,
) -> Result<()> {
    let (context, _) = make_context_with_pg(object_store_type).await;

    create_table_and_some_partitions(&context, "test_table", None).await;

    //
    // Ensure we have 4 partitions, and record their file names
    //
    let mut table = context.try_get_delta_table("test_table").await?;
    table.load().await?;
    let mut all_partitions = table.snapshot()?.file_actions()?.clone();
    assert_eq!(all_partitions.len(), 4);
    let partition_1 = all_partitions.last().unwrap().clone();
    let partition_4 = all_partitions.first().unwrap().clone();

    //
    // Check DELETE's query plan to make sure 46 (int) gets cast to a float value by the optimizer
    //
    let plan = context
        .create_logical_plan("DELETE FROM test_table WHERE some_value > 46")
        .await?;
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
        .await?;

    // Verify results
    let plan = context
        .plan_query("SELECT some_value FROM test_table ORDER BY some_value")
        .await?;
    let results = context.collect(plan).await?;

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
    table.load().await?;
    let partition_5 = match table.snapshot()?.file_actions()?.as_slice() {
        [f_1, f_2, f_3]
            if f_3 == &partition_1
                && f_2 == &partition_4
                && !all_partitions.contains(f_1) =>
        {
            f_1.clone()
        }
        _ => panic!("Expected exactly 2 inherited and 1 new partition"),
    };
    all_partitions.push(partition_5.clone());

    //
    // Add another partition for a new table_version and record the new partition
    //
    context
        .plan_query("INSERT INTO test_table (some_value) VALUES (48), (49), (50)")
        .await?;

    // Expect too see a new (6th) partition
    table.load().await?;
    let partition_6 = match table.snapshot()?.file_actions()?.as_slice() {
        [f_1, f_2, f_3, f_4]
            if f_4 == &partition_1
                && f_3 == &partition_4
                && f_2 == &partition_5
                && !all_partitions.contains(f_1) =>
        {
            f_1.clone()
        }
        _ => panic!("Expected exactly 3 inherited and 1 new partition"),
    };
    all_partitions.push(partition_6.clone());

    //
    // Execute DELETE not affecting only partition with id 4, while trimming/combining the rest
    //
    context
        .plan_query("DELETE FROM test_table WHERE some_value IN (43, 45, 49)")
        .await?;

    let plan = context
        .plan_query("SELECT some_value FROM test_table ORDER BY some_value")
        .await?;
    let results = context.collect(plan).await?;

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

    table.load().await?;
    let partition_7 = match table.snapshot()?.file_actions()?.as_slice() {
        [f_1, f_2] if f_2 == &partition_4 && !all_partitions.contains(f_1) => f_1.clone(),
        _ => panic!("Expected exactly 1 inherited and 1 new partition"),
    };
    all_partitions.push(partition_7.clone());

    //
    // Execute a no-op DELETE, leaving the new table version the same as the prior one
    //

    context
        .plan_query("DELETE FROM test_table WHERE some_value < 35")
        .await?;

    let plan = context
        .plan_query("SELECT some_value FROM test_table ORDER BY some_value")
        .await?;
    let results = context.collect(plan).await?;
    assert_batches_eq!(expected, &results);

    // Both partitions are inherited from the previous version
    table.load().await.unwrap();
    match table.snapshot()?.file_actions()?.as_slice() {
        [f_1, f_2] if f_2 == &partition_4 && f_1 == &partition_7 => {}
        _ => panic!("Expected same partitions as before"),
    };

    //
    // Execute DELETE with multiple qualifiers
    //
    context
        .plan_query("DELETE FROM test_table WHERE some_value < 41 OR some_value > 46")
        .await?;

    let plan = context
        .plan_query("SELECT some_value FROM test_table ORDER BY some_value")
        .await?;
    let results = context.collect(plan).await?;

    let expected = [
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
    match table.snapshot()?.file_actions()?[..] {
        [ref f_1] if !all_partitions.contains(f_1) => all_partitions.push(f_1.clone()),
        _ => panic!("Expected exactly 1 new partition different from the previous one"),
    };

    //
    // Execute blank DELETE, without qualifiers
    //
    context.plan_query("DELETE FROM test_table").await?;

    // Verify results
    let plan = context
        .plan_query("SELECT some_value FROM test_table")
        .await?;
    let results = context.collect(plan).await?;

    assert!(results.is_empty());

    table.load().await.unwrap();
    assert!(table.snapshot()?.file_actions()?.is_empty());

    Ok(())
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
    let expected = [
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
    #[values(
        ObjectStoreType::InMemory,
        ObjectStoreType::Local,
        ObjectStoreType::S3(None),
        ObjectStoreType::S3(Some("/path/to/folder"))
    )]
    object_store_type: ObjectStoreType,
) -> Result<()> {
    let (context, _) = make_context_with_pg(object_store_type).await;

    create_table_and_some_partitions(&context, "test_table", None).await;

    //
    // Ensure we have 4 partitions, and record their file names
    //
    let mut table = context.try_get_delta_table("test_table").await?;
    table.load().await?;
    let mut all_partitions = table.snapshot()?.file_actions()?.clone();
    assert_eq!(all_partitions.len(), 4);
    let partition_2 = all_partitions[2].clone();
    let partition_3 = all_partitions[1].clone();

    //
    // Execute UPDATE with a selection, affecting only partitions 1 and 4
    //
    let query = "UPDATE test_table
    SET some_time = '2022-01-01 21:21:21Z', some_int_value = 5555, some_value = some_value - 10
    WHERE some_value IN (41, 42, 43)";

    let plan = context.create_logical_plan(query).await?;

    // Check the UPDATE query plan to make sure IN (41, 42, 43) (int) get cast to a float value
    assert_eq!(
        format!("{}", plan.display_indent()),
        r#"Dml: op=[Update] table=[test_table]
  Projection: TimestampMicrosecond(1641072081000000, None) AS some_time, test_table.some_value - Float32(10) AS some_value, test_table.some_other_value AS some_other_value, test_table.some_bool_value AS some_bool_value, Int64(5555) AS some_int_value
    Filter: test_table.some_value = Float32(41) OR test_table.some_value = Float32(42) OR test_table.some_value = Float32(43)
      TableScan: test_table"#
    );

    // Now execute and check the results
    context.plan_query(query).await?;

    let plan = context
        .plan_query("SELECT * FROM test_table ORDER BY some_value, some_other_value")
        .await?;
    let results = context.collect(plan).await?;
    let expected = vec![
        "+---------------------+------------+------------------+-----------------+----------------+",
        "| some_time           | some_value | some_other_value | some_bool_value | some_int_value |",
        "+---------------------+------------+------------------+-----------------+----------------+",
        "| 2022-01-01T21:21:21 | 31.0       | 4.0000000000     |                 | 5555           |",
        "| 2022-01-01T21:21:21 | 32.0       | 1.0000000000     |                 | 5555           |",
        "| 2022-01-01T21:21:21 | 32.0       | 4.0000000000     |                 | 5555           |",
        "| 2022-01-01T21:21:21 | 33.0       | 1.0000000000     |                 | 5555           |",
        "|                     | 40.0       | 4.0000000000     |                 |                |",
        "| 2022-01-01T20:03:03 | 44.0       | 1.0000000000     |                 | 3333           |",
        "|                     | 45.0       | 2.0000000000     |                 |                |",
        "|                     | 46.0       | 2.0000000000     |                 |                |",
        "|                     | 46.0       | 3.0000000000     |                 |                |",
        "|                     | 47.0       | 2.0000000000     |                 |                |",
        "|                     | 47.0       | 3.0000000000     |                 |                |",
        "|                     | 48.0       | 3.0000000000     |                 |                |",
        "+---------------------+------------+------------------+-----------------+----------------+",
    ];
    assert_batches_eq!(expected, &results);

    // Ensure partitions 1 and 4 have been fused into a new partition, and record it.
    table.load().await?;
    let partition_5 = match table.snapshot()?.file_actions()?.as_slice() {
        [f_1, f_2, f_3]
            if f_3 == &partition_2
                && f_2 == &partition_3
                && !all_partitions.contains(f_1) =>
        {
            f_1.clone()
        }
        _ => panic!("Expected exactly 2 inherited and 1 new partition"),
    };
    all_partitions.push(partition_5.clone());

    //
    // Execute UPDATE that doesn't change anything
    //
    context
        .plan_query("UPDATE test_table SET some_bool_value = TRUE WHERE some_value = 200")
        .await?;

    let plan = context
        .plan_query("SELECT * FROM test_table ORDER BY some_value, some_other_value")
        .await?;
    let results = context.collect(plan).await?;
    assert_batches_eq!(expected, &results);

    // Ensure partitions from before are still there
    table.load().await?;
    match table.snapshot()?.file_actions()?.as_slice() {
        [f_1, f_2, f_3]
            if f_3 == &partition_2 && f_2 == &partition_3 && f_1 == &partition_5 => {}
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
        .await?;

    // Verify results
    let plan = context.plan_query("SELECT * FROM test_table").await?;
    let results = context.collect(plan).await?;

    let expected = vec![
        "+---------------------+------------+------------------+-----------------+----------------+",
        "| some_time           | some_value | some_other_value | some_bool_value | some_int_value |",
        "+---------------------+------------+------------------+-----------------+----------------+",
        "| 2022-01-01T21:21:21 | 42.0       | 5.5550000000     | true            | 5555           |",
        "| 2022-01-01T21:21:21 | 42.0       | 5.5550000000     | true            | 5555           |",
        "|                     | 42.0       | 0.0000000000     |                 |                |",
        "| 2022-01-01T21:21:21 | 42.0       | 5.5550000000     | true            | 5555           |",
        "| 2022-01-01T21:21:21 | 42.0       | 5.5550000000     | true            | 5555           |",
        "| 2022-01-01T20:03:03 | 42.0       | 3.3330000000     | false           | 3333           |",
        "|                     | 42.0       | 0.0000000000     |                 |                |",
        "|                     | 42.0       | 0.0000000000     |                 |                |",
        "|                     | 42.0       | 0.0000000000     |                 |                |",
        "|                     | 42.0       | 0.0000000000     |                 |                |",
        "|                     | 42.0       | 0.0000000000     |                 |                |",
        "|                     | 42.0       | 0.0000000000     |                 |                |",
        "+---------------------+------------+------------------+-----------------+----------------+",
    ];
    assert_batches_eq!(expected, &results);

    table.load().await?;
    match table.snapshot()?.file_actions()?.as_slice() {
        [f_1] if !all_partitions.contains(f_1) => {}
        _ => panic!("Expected only 1 new partition"),
    };

    Ok(())
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
        "Schema error: No field named nonexistent. Valid fields are test_table.some_time, test_table.some_value, test_table.some_other_value, test_table.some_bool_value, test_table.some_int_value."
    );

    let err = context
        .plan_query("UPDATE test_table SET some_value = 42 WHERE nonexistent = 32")
        .await
        .unwrap_err();

    assert_eq!(
        err.to_string(),
        "Schema error: No field named nonexistent. Valid fields are test_table.some_time, test_table.some_value, test_table.some_other_value, test_table.some_bool_value, test_table.some_int_value."
    );

    let err = context
        .plan_query("UPDATE test_table SET some_int_value = 'nope'")
        .await
        .unwrap_err();

    assert!(err
        .to_string()
        .contains("Cannot cast string 'nope' to value of Int64 type"));
}

#[tokio::test]
async fn test_copy_to_statement() -> Result<()> {
    let (context, _) = make_context_with_pg(ObjectStoreType::InMemory).await;
    create_table_and_insert(&context, "test_table").await;

    let temp_dir = TempDir::new().unwrap();
    let location = format!("{}/copy.parquet", temp_dir.path().to_string_lossy());

    // Execute the COPY TO statement
    context
        .plan_query(format!("COPY test_table TO '{location}'").as_str())
        .await?;

    // Check results
    context
        .plan_query(
            format!(
                "CREATE EXTERNAL TABLE copied_table \
            STORED AS PARQUET \
            LOCATION '{location}'"
            )
            .as_str(),
        )
        .await?;

    let results_original = context
        .collect(context.plan_query("SELECT * FROM test_table").await?)
        .await?;
    let results_copied = context
        .collect(
            context
                .plan_query("SELECT * FROM staging.copied_table")
                .await?,
        )
        .await?;

    assert_eq!(results_original, results_copied);

    Ok(())
}
