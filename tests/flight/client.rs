use crate::flight::*;
use crate::statements::create_table_and_some_partitions;

#[tokio::test]
async fn test_basic_queries() -> Result<()> {
    let (context, addr, flight) = start_flight_server().await;
    create_table_and_some_partitions(context.as_ref(), "flight_table", None).await;
    tokio::task::spawn(flight);

    // Create the channel and the client
    let port = addr.port();
    let channel = Channel::from_shared(format!("http://localhost:{port}"))
        .expect("Endpoint created")
        .connect()
        .await
        .expect("Channel connected");

    let mut client = FlightClient::new(channel);

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
        "|                     | 45.0       | 2.0000000000     |                 |                |",
        "|                     | 46.0       | 2.0000000000     |                 |                |",
        "|                     | 47.0       | 2.0000000000     |                 |                |",
        "|                     | 46.0       | 3.0000000000     |                 |                |",
        "|                     | 47.0       | 3.0000000000     |                 |                |",
        "|                     | 48.0       | 3.0000000000     |                 |                |",
        "|                     | 42.0       | 4.0000000000     |                 |                |",
        "|                     | 41.0       | 4.0000000000     |                 |                |",
        "|                     | 40.0       | 4.0000000000     |                 |                |",
        "+---------------------+------------+------------------+-----------------+----------------+",
    ];

    assert_batches_eq!(expected, &results);

    Ok(())
}
