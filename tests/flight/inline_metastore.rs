use crate::flight::*;

#[rstest]
#[should_panic(expected = "External error: Not a Delta table: no log files")]
#[case("local.file", false)]
#[case("local.file_with_store", false)]
#[case("local.file", true)]
#[case("s3.minio", true)]
#[case("gcs.fake", true)]
#[tokio::test]
async fn test_inline_query(#[case] table: &str, #[case] local_store: bool) -> () {
    let (_context, mut client) = flight_server(local_store).await;

    let batches = get_flight_batches_inlined(
        &mut client,
        format!("SELECT * FROM {table} ORDER BY value"),
        schemas(),
    )
    .await
    .unwrap();

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
    assert_batches_eq!(expected, &batches);
}
