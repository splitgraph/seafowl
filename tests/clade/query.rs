use crate::clade::*;

#[tokio::test]
async fn test_basic_select() -> Result<(), Box<dyn std::error::Error>> {
    let (context, clade) = start_clade_server().await;
    tokio::task::spawn(clade);

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    let plan = context
        .plan_query("SELECT * FROM some_schema.some_table ORDER BY value")
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();

    let expected = [
        "+-------+------+-------+-----+",
        "| value | year | month | day |",
        "+-------+------+-------+-----+",
        "| 1     | 2020 | 1     | 1   |",
        "| 2     | 2020 | 2     | 3   |",
        "| 3     | 2020 | 2     | 5   |",
        "| 4     | 2021 | 4     | 5   |",
        "| 5     | 2021 | 12    | 4   |",
        "| 6     | 2021 | 12    | 20  |",
        "| 7     | 2021 | 12    | 20  |",
        "+-------+------+-------+-----+",
    ];
    assert_batches_eq!(expected, &results);

    Ok(())
}
