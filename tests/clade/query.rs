use crate::clade::*;

#[rstest]
#[should_panic(
    expected = "Plan(\"Object store not configured and no object store for table \\\"file\\\" passed in\")"
)]
#[case("local.file", false)]
#[case("local.file", true)]
#[case("s3.delta", true)]
#[case("gcs.fake", true)]
#[tokio::test]
async fn test_basic_select(#[case] table: &str, #[case] object_store: bool) -> () {
    let context = start_clade_server(object_store).await;

    // Before proceeding with the test swallow up a single initial
    // ConnectError("tcp connect error", Os { code: 61, kind: ConnectionRefused, message: "Connection refused" })
    // TODO: why does this happen?
    let _r = context.metastore.schemas.list(DEFAULT_DB).await;

    let plan = context
        .plan_query(&format!("SELECT * FROM {table} ORDER BY key"))
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();

    let expected = [
        "+-----+-------+",
        "| key | value |",
        "+-----+-------+",
        "| 1   | one   |",
        "| 2   | two   |",
        "| 3   | three |",
        "| 4   | four  |",
        "+-----+-------+",
    ];
    assert_batches_eq!(expected, &results);
}
