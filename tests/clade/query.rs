use crate::clade::*;

#[rstest]
#[should_panic(expected = "External(NotATable(\"no log files\"))")]
#[case("local.file", false)]
#[case("local.file", true)]
#[case("s3.minio", true)]
#[case("gcs.fake", true)]
#[tokio::test]
async fn test_basic_select(#[case] table: &str, #[case] object_store: bool) -> () {
    let context = start_clade_server(object_store).await;

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
}
