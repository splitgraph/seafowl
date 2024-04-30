use crate::flight::*;
use arrow::array::{Int32Array, StringArray};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_schema::{DataType, Field, Schema};
use clade::flight::do_put_result::AckType;
use clade::flight::{do_put_command::CommandType, DoPutCommand, DoPutResult};
use futures::StreamExt;

#[tokio::test]
async fn test_basic_upload() -> Result<()> {
    let (_context, mut client) = flight_server().await;

    let schema = Arc::new(Schema::new(vec![
        Field::new("col_1", DataType::Int32, true),
        Field::new("col_2", DataType::Utf8, true),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("one"), Some("two")])),
        ],
    )
    .unwrap();

    let cmd = DoPutCommand {
        r#type: CommandType::Upsert.into(),
        table: "do_put_test".to_string(),
        origin: Some("test-origin".to_string()),
        pk_column: vec![],
        sequence_number: Some(123),
    };

    let descriptor = FlightDescriptor::new_cmd(cmd.as_any().encode_to_vec());
    let flight_stream_builder =
        FlightDataEncoderBuilder::new().with_flight_descriptor(Some(descriptor));
    let flight_data = flight_stream_builder.build(futures::stream::iter(vec![Ok(batch)]));
    let response = client.do_put(flight_data).await?.next().await.unwrap()?;

    // Changes are still in memory
    let ack_type = AckType::try_from(
        DoPutResult::decode(response.app_metadata)
            .expect("DoPutResult")
            .r#type,
    )
    .expect("AckType");
    assert_eq!(ack_type, AckType::Memory);

    let err = get_flight_batches(&mut client, "SELECT * FROM do_put_test".to_string())
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("Tonic error: status: Internal, message: \"Error during planning: table 'default.public.do_put_test' not found\""));

    // Try to append some more data and pass the flush threshold
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![3, 4])),
            Arc::new(StringArray::from(vec![Some("three"), Some("four")])),
        ],
    )
    .unwrap();

    let cmd = DoPutCommand {
        r#type: CommandType::Upsert.into(),
        table: "do_put_test".to_string(),
        origin: Some("test-origin".to_string()),
        pk_column: vec![],
        sequence_number: Some(456),
    };

    let descriptor = FlightDescriptor::new_cmd(cmd.as_any().encode_to_vec());
    let flight_stream_builder =
        FlightDataEncoderBuilder::new().with_flight_descriptor(Some(descriptor));
    let flight_data = flight_stream_builder.build(futures::stream::iter(vec![Ok(batch)]));
    let response = client.do_put(flight_data).await?.next().await.unwrap()?;

    // Changes are now flushed to storage
    let ack_type = AckType::try_from(
        DoPutResult::decode(response.app_metadata)
            .expect("DoPutResult")
            .r#type,
    )
    .expect("AckType");
    assert_eq!(ack_type, AckType::Durable);

    let results =
        get_flight_batches(&mut client, "SELECT * FROM do_put_test".to_string()).await?;
    let expected = [
        "+-------+-------+",
        "| col_1 | col_2 |",
        "+-------+-------+",
        "| 1     | one   |",
        "| 2     | two   |",
        "| 3     | three |",
        "| 4     | four  |",
        "+-------+-------+",
    ];

    assert_batches_eq!(expected, &results);

    Ok(())
}
