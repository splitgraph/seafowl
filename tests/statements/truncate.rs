use crate::statements::*;

#[tokio::test]
async fn test_truncate_table() -> Result<()> {
    let (context, _) = make_context_with_pg(ObjectStoreType::Local).await;

    // Create table_1 and check that it contains data
    create_table_and_insert(&context, "table_1").await;
    let plan = context.plan_query("SELECT * FROM table_1").await.unwrap();
    let results = context.collect(plan).await.unwrap();
    let expected = [
        "+---------------------+------------+------------------+-----------------+----------------+",
        "| some_time           | some_value | some_other_value | some_bool_value | some_int_value |",
        "+---------------------+------------+------------------+-----------------+----------------+",
        "| 2022-01-01T20:01:01 | 42.0       | 1.0000000000     |                 | 1111           |",
        "| 2022-01-01T20:02:02 | 43.0       | 1.0000000000     |                 | 2222           |",
        "| 2022-01-01T20:03:03 | 44.0       | 1.0000000000     |                 | 3333           |",
        "+---------------------+------------+------------------+-----------------+----------------+",
    ];
    assert_batches_eq!(expected, &results);

    // Execute TRUNCATE command
    context.plan_query("TRUNCATE TABLE table_1").await.unwrap();

    // Check that table_1 no longer contains data
    let plan = context.plan_query("SELECT * FROM table_1").await.unwrap();
    let results = context.collect(plan).await.unwrap();
    assert!(results.is_empty());

    Ok(())
}
