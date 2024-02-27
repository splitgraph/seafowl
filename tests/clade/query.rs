use crate::clade::*;

#[rstest]
#[tokio::test]
async fn test_basic_select(
    #[values("local.file", "s3.minio")] table: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let context = start_clade_server().await;

    // Before proceeding with the test swallow up a single initial
    // ConnectError("tcp connect error", Os { code: 61, kind: ConnectionRefused, message: "Connection refused" })
    // TODO: why does this happen?
    let _r = context.metastore.schemas.list(DEFAULT_DB).await;

    let plan = context
        .plan_query(&format!("SELECT * FROM {table} ORDER BY value"))
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
