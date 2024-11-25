use crate::statements::*;

async fn prepare_data(
    context: &SeafowlContext,
    maybe_test_dir: &Option<TempDir>,
) -> Result<Uuid> {
    // Prepare a flat Parquet table
    let table_uuid = Uuid::new_v4();
    let temp_dir = maybe_test_dir.as_ref().expect("temporary data dir exists");
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
    Ok(table_uuid)
}

#[tokio::test]
async fn test_convert_from_flat_parquet_table() -> Result<()> {
    let (context, maybe_test_dir) = make_context_with_pg(ObjectStoreType::Local).await;
    let table_uuid = prepare_data(&context, &maybe_test_dir).await?;

    // Now test the actual conversion
    context
        .plan_query(&format!("CONVERT '{table_uuid}' TO DELTA table_converted"))
        .await?;

    // Run command again to test idempotency
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
            .get_internal_object_store()
            .unwrap()
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

    // Convoluted way of sort-stable stats asserting
    let state = table.snapshot()?;
    let mut min_values = state
        .min_values(&Column::from_name("column1"))
        .expect("min values exist")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Failed to downcast to Int64Array")
        .values()
        .to_vec();
    min_values.sort();
    assert_eq!(min_values, vec![1, 3, 5]);

    let mut max_values = state
        .max_values(&Column::from_name("column1"))
        .expect("max values exist")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Failed to downcast to Int64Array")
        .values()
        .to_vec();
    max_values.sort();
    assert_eq!(max_values, vec![2, 4, 6]);

    let min_values = state
        .min_values(&Column::from_name("column2"))
        .expect("min values exist");
    let min_values = min_values
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Failed to downcast to StringArray")
        .iter()
        .flatten()
        .sorted()
        .collect::<Vec<&str>>();
    assert_eq!(min_values, vec!["five", "four", "one"]);

    let max_values = state
        .max_values(&Column::from_name("column2"))
        .expect("max values exist");
    let max_values = max_values
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Failed to downcast to StringArray")
        .iter()
        .flatten()
        .sorted()
        .collect::<Vec<&str>>();
    assert_eq!(max_values, vec!["six", "three", "two"]);

    assert_eq!(
        table.statistics(),
        Some(Statistics {
            num_rows: Exact(6),
            total_byte_size: Inexact(1708),
            column_statistics: vec![
                ColumnStatistics {
                    null_count: Exact(0),
                    max_value: Exact(ScalarValue::Int64(Some(6))),
                    min_value: Exact(ScalarValue::Int64(Some(1))),
                    distinct_count: Absent
                },
                ColumnStatistics {
                    null_count: Exact(0),
                    max_value: Absent,
                    min_value: Absent,
                    distinct_count: Absent
                }
            ]
        }),
    );

    Ok(())
}

#[tokio::test]
async fn test_convert_twice_doesnt_error() -> Result<()> {
    let (context, maybe_test_dir) = make_context_with_pg(ObjectStoreType::Local).await;
    let table_uuid = prepare_data(&context, &maybe_test_dir).await?;

    // Convert twice and make sure the second conversion doesn't fail
    context
        .plan_query(&format!("CONVERT '{table_uuid}' TO DELTA table_converted"))
        .await?;

    context
        .plan_query(&format!("CONVERT '{table_uuid}' TO DELTA table_converted"))
        .await?;

    // Test the contents of the converted table
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
    Ok(())
}
