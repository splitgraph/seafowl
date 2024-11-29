use crate::flight::*;

#[rstest]
// Errors out because the path for the table `file` doesn't exist
#[should_panic(expected = "External error: Not a Delta table: no log files")]
#[case("local.file", TestServerType::Memory, true)]
// Errors out because the metastore entry for `file` doesn't have an object store
#[should_panic(
    expected = "Error during planning: Object store not configured and no object store for table \\\"file\\\" passed in"
)]
#[case("local.file", TestServerType::InlineOnly, true)]
// path for the table `file` contains a real Delta tables and the metastore is pre-configured
#[case("local.file", TestServerType::LocalWithData, true)]
// Errors out because even though we're querying the `file_with_store` table that does have
// an object store reference in the inline metastore, the inline metastore also contains
// the `file` table which is broken.
#[should_panic(
    expected = "Error during planning: Object store not configured and no object store for table \\\"file\\\" passed in"
)]
#[case("local.file_with_store", TestServerType::InlineOnly, true)]
// Testing with properly sent inline metastore
#[case("local.file_with_store", TestServerType::InlineOnly, false)]
#[case("s3.minio", TestServerType::InlineOnly, false)]
#[case("s3.minio_prefix", TestServerType::InlineOnly, false)]
#[case("s3.iceberg", TestServerType::InlineOnly, false)]
#[case("gcs.fake", TestServerType::InlineOnly, false)]
#[tokio::test]
async fn test_inline_query(
    #[case] table: &str,
    #[case] test_server_type: TestServerType,
    #[case] include_file_without_store: bool,
) -> () {
    let (_context, mut client) = flight_server(test_server_type).await;

    let batches = get_flight_batches_inlined(
        &mut client,
        format!("SELECT * FROM {table} ORDER BY key"),
        schemas(include_file_without_store),
    )
    .await
    .unwrap();

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
    assert_batches_eq!(expected, &batches);
}
