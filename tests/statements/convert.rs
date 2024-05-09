use crate::statements::*;

#[tokio::test]
async fn test_convert_from_flat_parquet_table() -> Result<()> {
    let (context, maybe_test_dir) = make_context_with_pg(ObjectStoreType::Local).await;

    // Prepare a flat Parquet table
    let table_uuid = Uuid::new_v4();
    let temp_dir = maybe_test_dir.expect("temporary data dir exists");
    let table_path = temp_dir.path().join(table_uuid.to_string());
    // Create the directory as otherwise the COPY will fail
    create_dir(table_path.clone()).await?;

    // COPY some values multiple times to test converting flat table with more than one parquet file
    context
        .plan_query(&format!(
            "COPY (VALUES (1, 'one'), (2, 'two')) TO '{}/file_1.parquet'",
            table_path.display()
        ))
        .await?;
    context
        .plan_query(&format!(
            "COPY (VALUES (3, 'three'), (4, 'four')) TO '{}/file_2.parquet'",
            table_path.display()
        ))
        .await?;
    context
        .plan_query(&format!(
            "COPY (VALUES (5, 'five'), (6, 'six')) TO '{}/file_3.parquet'",
            table_path.display()
        ))
        .await?;

    // Now test the actual conversion
    context
        .plan_query(&format!("CONVERT '{table_uuid}' TO DELTA table_converted"))
        .await?;

    // Finally test the contents of the converted table
    let plan = context
        .plan_query("SELECT * FROM table_converted ORDER BY column1")
        .await?;
    let results = context.collect(plan).await.unwrap();

    let expected = [
        "+---------+---------+",
        "| column1 | column2 |",
        "+---------+---------+",
        "| 1       | one     |",
        "| 2       | two     |",
        "| 3       | three   |",
        "| 4       | four    |",
        "| 5       | five    |",
        "| 6       | six     |",
        "+---------+---------+",
    ];
    assert_batches_eq!(expected, &results);

    // Also check the final directory state
    testutils::assert_uploaded_objects(
        context
            .internal_object_store
            .get_log_store(&table_uuid.to_string())
            .object_store(),
        vec![
            String::from("_delta_log/00000000000000000000.json"),
            String::from("file_1.parquet"),
            String::from("file_2.parquet"),
            String::from("file_3.parquet"),
        ],
    )
    .await;

    // Ensure partition/column stats are collected in add logs:
    // https://github.com/delta-io/delta-rs/pull/2491
    let mut table = context.try_get_delta_table("table_converted").await?;
    table.load().await?;
    assert_eq!(
        table.snapshot()?.min_values(&Column::from_name("column1")),
        Some(Arc::new(Int64Array::from(vec![3, 5, 1])) as _)
    );
    assert_eq!(
        table.snapshot()?.max_values(&Column::from_name("column1")),
        Some(Arc::new(Int64Array::from(vec![4, 6, 2])) as _)
    );

    assert_eq!(
        table.snapshot()?.min_values(&Column::from_name("column2")),
        Some(Arc::new(StringArray::from(vec!["four", "five", "one"])) as _)
    );
    assert_eq!(
        table.snapshot()?.max_values(&Column::from_name("column2")),
        Some(Arc::new(StringArray::from(vec!["three", "six", "two"])) as _)
    );

    Ok(())
}
