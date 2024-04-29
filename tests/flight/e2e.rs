use crate::flight::*;

#[rstest]
#[tokio::test]
async fn test_interleaving_queries(
    #[future(awt)] test_seafowl: TestSeafowl,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let mut client = test_seafowl.flight_client();

    // Verify readiness first
    let resp = Client::new()
        .get(
            format!("{}/readyz", test_seafowl.http_base())
                .try_into()
                .unwrap(),
        )
        .await
        .expect("Can query health endpoint");
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(response_text(resp).await, "ready");

    // Create test table
    let _ = get_flight_batches(
        &mut client,
        "CREATE TABLE flight_table(c1 INT, c2 TEXT) AS VALUES (1, 'one'), (2, 'two')"
            .to_string(),
    )
    .await;

    // Fire of the first query
    let cmd = CommandStatementQuery {
        query: "SELECT MAX(c1) FROM flight_table".to_string(),
        transaction_id: None,
    };
    let request = FlightDescriptor::new_cmd(cmd.as_any().encode_to_vec());
    let response = client.get_flight_info(request).await?;

    // Get the corresponding ticket
    let ticket_1 = response.endpoint[0]
        .ticket
        .clone()
        .expect("expected ticket");

    // Fire of the second query
    let cmd = CommandStatementQuery {
        query: "SELECT MIN(c1) FROM flight_table".to_string(),
        transaction_id: None,
    };
    let request = FlightDescriptor::new_cmd(cmd.as_any().encode_to_vec());
    let response = client.get_flight_info(request).await?;

    // Get the corresponding ticket
    let ticket_2 = response.endpoint[0]
        .ticket
        .clone()
        .expect("expected ticket");

    // Execute a couple of queries that error out
    // One during planning (GetFlightInfo) ...
    let err = get_flight_batches(&mut client, "SELECT * FROM nonexistent".to_string())
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("status: Internal, message: \"Error during planning: table 'default.public.nonexistent' not found\"")
    );
    // ...and another one after handing off the stream to the client, so we don't really capture the status in the metrics
    let err = get_flight_batches(&mut client, "SELECT 'notanint'::INT".to_string())
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("status: Internal, message: \"Arrow error: Cast error: Cannot cast string 'notanint' to value of Int32 type\"")
    );

    // Now retrieve the results for the second ticket
    let flight_stream = client.do_get(ticket_2.clone()).await?;
    let results: Vec<RecordBatch> = flight_stream.try_collect().await?;

    let expected = [
        "+----------------------+",
        "| MIN(flight_table.c1) |",
        "+----------------------+",
        "| 1                    |",
        "+----------------------+",
    ];

    assert_batches_eq!(expected, &results);

    // Try to retrieve ticket 2 results again
    let err = client.do_get(ticket_2).await.unwrap_err();
    assert!(err
        .to_string()
        .contains("status: NotFound, message: \"No results found for query id"));

    // Now retrieve the results for the first ticket
    let flight_stream = client.do_get(ticket_1).await?;
    let results: Vec<RecordBatch> = flight_stream.try_collect().await?;

    let expected = [
        "+----------------------+",
        "| MAX(flight_table.c1) |",
        "+----------------------+",
        "| 2                    |",
        "+----------------------+",
    ];

    assert_batches_eq!(expected, &results);

    // Finally test gRPC-related metrics
    assert_eq!(
        get_metrics(GRPC_REQUESTS, test_seafowl.metrics_port()).await,
        vec![
            "# HELP grpc_requests Counter tracking gRPC request statistics",
            "# TYPE grpc_requests counter",
            "grpc_requests{path=\"/arrow.flight.protocol.FlightService/DoGet\",status=\"0\"} 4",
            "grpc_requests{path=\"/arrow.flight.protocol.FlightService/DoGet\",status=\"5\"} 1",
            "grpc_requests{path=\"/arrow.flight.protocol.FlightService/GetFlightInfo\",status=\"0\"} 4",
            "grpc_requests{path=\"/arrow.flight.protocol.FlightService/GetFlightInfo\",status=\"13\"} 1",
        ]
    );

    Ok(())
}
