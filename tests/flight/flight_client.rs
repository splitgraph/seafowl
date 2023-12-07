use crate::flight::*;

#[tokio::test]
async fn test_basic_queries() -> Result<()> {
    let (_, addr, flight) = start_flight_server().await;
    tokio::task::spawn(flight);

    let port = addr.port();
    let channel = Channel::from_shared(format!("http://localhost:{port}"))
        .expect("Endpoint not created")
        .connect()
        .await
        .expect("Error connecting");

    let mut client = FlightClient::new(channel);

    let _ = client.handshake("test").await.expect("error handshaking");

    // Try out the actual query
    let cmd = CommandStatementQuery {
        query: "SELECT 1".to_string(),
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

    // Retrieve the corresponding Flight stream with do_get
    let flight_stream = client.do_get(ticket).await.expect("error fetching data");

    let _batches: Vec<RecordBatch> = flight_stream.try_collect().await?;

    Ok(())
}
