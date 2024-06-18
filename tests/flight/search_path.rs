use crate::flight::*;

#[tokio::test]
async fn test_default_schema_override(
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let (context, mut client) = flight_server(false).await;

    context.plan_query("CREATE SCHEMA some_schema").await?;
    create_table_and_insert(context.as_ref(), "some_schema.flight_table").await;

    // Trying to run the query without the search_path set will error out
    let err = get_flight_batches(&mut client, "SELECT * FROM flight_table".to_string())
        .await
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("table 'default.public.flight_table' not found"));

    // Now set the search_path header and re-run the query
    client
        .metadata_mut()
        .insert("search-path", MetadataValue::from_static("some_schema"));

    let results =
        get_flight_batches(&mut client, "SELECT * FROM flight_table".to_string()).await?;

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

    // Finally, client needs to remove the header explicitly to avoid default schema override
    client.metadata_mut().remove("search-path");
    let err = get_flight_batches(&mut client, "SELECT * FROM flight_table".to_string())
        .await
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("table 'default.public.flight_table' not found"));

    Ok(())
}
