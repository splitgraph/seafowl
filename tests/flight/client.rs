use crate::flight::*;

async fn get_flight_batches(
    client: &mut FlightClient,
    query: String,
) -> Result<Vec<RecordBatch>> {
    let cmd = CommandStatementQuery {
        query,
        transaction_id: None,
    };
    let request = FlightDescriptor::new_cmd(cmd.as_any().encode_to_vec());
    let response = client.get_flight_info(request).await?;

    // Get the returned ticket
    let ticket = response.endpoint[0]
        .ticket
        .clone()
        .expect("expected ticket");

    // Retrieve the corresponding Flight stream and collect into batches
    let flight_stream = client.do_get(ticket).await?;

    let batches = flight_stream.try_collect().await?;

    Ok(batches)
}

#[tokio::test]
async fn test_basic_queries() -> Result<()> {
    let (context, addr, flight) = start_flight_server().await;
    create_table_and_insert(context.as_ref(), "flight_table").await;
    tokio::task::spawn(flight);

    let mut client = create_flight_client(addr).await;

    // Test the handshake works
    let _ = client.handshake("test").await.expect("error handshaking");

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

#[tokio::test]
async fn test_ddl_types_roundtrip() -> Result<()> {
    let (_context, addr, flight) = start_flight_server().await;
    tokio::task::spawn(flight);

    let mut client = create_flight_client(addr).await;

    let all_types_query = r#"
SELECT
  1::TINYINT AS tinyint_val,
  1000::SMALLINT AS smallint_val,
  1000000::INT AS integer_val,
  1000000000::BIGINT AS bigint_val,
  'c'::CHAR AS char_val,
  'varchar'::VARCHAR AS varchar_val,
  'text'::TEXT AS text_val,
  'string'::STRING AS string_val,
  12.345::DECIMAL(5, 2) AS decimal_val,
  12.345::FLOAT AS float_val,
  12.345::REAL AS real_val,
  12.3456789101112131415::DOUBLE AS double_val,
  'true'::BOOLEAN AS bool_val,
  '2022-01-01'::DATE AS date_val,
  '2022-01-01T12:03:11.123456Z'::TIMESTAMP AS timestamp_val,
  [1,2,3,4,5] AS int_array_val,
  ['one','two'] AS text_array_val
"#;

    // Create a table from the above query
    let results = get_flight_batches(
        &mut client,
        format!("CREATE TABLE flight_types AS ({all_types_query})"),
    )
    .await?;

    // There should be no results from the table creation
    assert!(results.is_empty());

    // Now check the transmitted batches
    let results =
        get_flight_batches(&mut client, "SELECT * FROM flight_types".to_string()).await?;

    let expected = [
        "+-------------+--------------+-------------+------------+----------+-------------+----------+------------+-------------+-----------+----------+--------------------+----------+------------+----------------------------+-----------------+----------------+",
        "| tinyint_val | smallint_val | integer_val | bigint_val | char_val | varchar_val | text_val | string_val | decimal_val | float_val | real_val | double_val         | bool_val | date_val   | timestamp_val              | int_array_val   | text_array_val |",
        "+-------------+--------------+-------------+------------+----------+-------------+----------+------------+-------------+-----------+----------+--------------------+----------+------------+----------------------------+-----------------+----------------+",
        "| 1           | 1000         | 1000000     | 1000000000 | c        | varchar     | text     | string     | 12.35       | 12.345    | 12.345   | 12.345678910111213 | true     | 2022-01-01 | 2022-01-01T12:03:11.123456 | [1, 2, 3, 4, 5] | [one, two]     |",
        "+-------------+--------------+-------------+------------+----------+-------------+----------+------------+-------------+-----------+----------+--------------------+----------+------------+----------------------------+-----------------+----------------+",
    ];

    assert_batches_eq!(expected, &results);

    Ok(())
}
