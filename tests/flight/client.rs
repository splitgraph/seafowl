use crate::flight::*;

#[tokio::test]
async fn test_basic_queries() -> Result<()> {
    let (context, addr, flight) = start_flight_server().await;
    create_table_and_insert(context.as_ref(), "flight_table").await;
    tokio::task::spawn(flight);

    let mut client = create_flight_client(addr).await;

    // Test the handshake works
    let _ = client.handshake("test").await.expect("error handshaking");

    // Try out the actual query
    let cmd = CommandStatementQuery {
        query: "SELECT * FROM flight_table".to_string(),
        transaction_id: None,
    };
    let request = FlightDescriptor::new_cmd(cmd.as_any().encode_to_vec());
    let response = client.get_flight_info(request).await?;

    // Get the returned ticket
    let ticket = response.endpoint[0]
        // Extract the ticket
        .ticket
        .clone()
        .expect("expected ticket");

    // Retrieve the corresponding Flight stream and collect into batches
    let flight_stream = client.do_get(ticket).await.expect("error fetching data");
    let results: Vec<RecordBatch> = flight_stream.try_collect().await?;

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

    Ok(())
}

#[tokio::test]
async fn test_interleaving_queries() -> Result<()> {
    let (context, addr, flight) = start_flight_server().await;
    create_table_and_insert(context.as_ref(), "flight_table").await;

    tokio::task::spawn(flight);

    let mut client = create_flight_client(addr).await;

    // Fire of the first query
    let cmd = CommandStatementQuery {
        query: "SELECT MAX(some_int_value) FROM flight_table".to_string(),
        transaction_id: None,
    };
    let request = FlightDescriptor::new_cmd(cmd.as_any().encode_to_vec());
    let response = client.get_flight_info(request).await?;

    // Get the corresponding ticket
    let ticket_1 = response.endpoint[0]
        // Extract the ticket
        .ticket
        .clone()
        .expect("expected ticket");

    // Fire of the second query
    let cmd = CommandStatementQuery {
        query: "SELECT MIN(some_int_value) FROM flight_table".to_string(),
        transaction_id: None,
    };
    let request = FlightDescriptor::new_cmd(cmd.as_any().encode_to_vec());
    let response = client.get_flight_info(request).await?;

    // Get the corresponding ticket
    let ticket_2 = response.endpoint[0]
        // Extract the ticket
        .ticket
        .clone()
        .expect("expected ticket");

    // Retrieve the results for the second ticket
    let flight_stream = client
        .do_get(ticket_2.clone())
        .await
        .expect("error fetching data");
    let results: Vec<RecordBatch> = flight_stream.try_collect().await?;

    let expected = [
        "+----------------------------------+",
        "| MIN(flight_table.some_int_value) |",
        "+----------------------------------+",
        "| 1111                             |",
        "+----------------------------------+",
    ];

    assert_batches_eq!(expected, &results);

    // Try to retrieve ticket 2 results again
    let err = client.do_get(ticket_2).await.unwrap_err();
    assert!(err
        .to_string()
        .contains("Execution error: No results found for query id"));

    // Now retrieve the results for the first ticket
    let flight_stream = client.do_get(ticket_1).await.expect("error fetching data");
    let results: Vec<RecordBatch> = flight_stream.try_collect().await?;

    let expected = [
        "+----------------------------------+",
        "| MAX(flight_table.some_int_value) |",
        "+----------------------------------+",
        "| 3333                             |",
        "+----------------------------------+",
    ];

    assert_batches_eq!(expected, &results);

    Ok(())
}
