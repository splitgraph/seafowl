use clade::schema::{TableFormat, TableObject};

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
#[case("s3.delta", TestServerType::InlineOnly, false)]
#[case("s3.delta_public", TestServerType::InlineOnly, false)]
#[case("s3.iceberg", TestServerType::InlineOnly, false)]
#[case("s3.iceberg_public", TestServerType::InlineOnly, false)]
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

#[tokio::test]
async fn test_inline_iceberg_write() {
    let (_context, mut client) = flight_server(TestServerType::InlineOnly).await;

    // Verify the v1 dataset is as expected
    let batches = get_flight_batches_inlined(
        &mut client,
        "SELECT * FROM s3.iceberg_hdfs_v1 ORDER BY key".to_string(),
        schemas(false),
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

    // WHEN data is inserted into the Iceberg table at v1
    get_flight_batches_inlined(
        &mut client,
        "INSERT INTO s3.iceberg_hdfs_v1 (key, value) VALUES (5, 'five'), (6, 'six')"
            .to_string(),
        schemas(false),
    )
    .await
    .unwrap();

    // THEN the v1 dataset is not affected
    let batches = get_flight_batches_inlined(
        &mut client,
        "SELECT * FROM s3.iceberg_hdfs_v1 ORDER BY key".to_string(),
        schemas(false),
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

    // THEN the v2 dataset contains the v1 data and the inserted data
    let mut s = schemas(false);
    s.schemas[1].tables.push(TableObject {
        name: "iceberg_hdfs_v2".to_string(),
        path: "test-data/iceberg/default.db/iceberg_table_2/metadata/v2.metadata.json"
            .to_string(),
        store: Some("minio".to_string()),
        format: TableFormat::Iceberg.into(),
    });
    let batches = get_flight_batches_inlined(
        &mut client,
        "SELECT * FROM s3.iceberg_hdfs_v2 ORDER BY key".to_string(),
        s,
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
        "| 5   | five  |",
        "| 6   | six   |",
        "+-----+-------+",
    ];
    assert_batches_eq!(expected, &batches);
}
