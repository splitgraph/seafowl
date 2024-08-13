use crate::flight::*;

#[tokio::test]
async fn test_basic_queries() -> Result<()> {
    let (context, mut client) = flight_server(TestServerType::Memory).await;
    create_table_and_insert(context.as_ref(), "flight_table").await;

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
async fn test_ddl_types_roundtrip() -> Result<()> {
    let (_context, mut client) = flight_server(TestServerType::Memory).await;

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
