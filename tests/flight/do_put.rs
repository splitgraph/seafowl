use crate::flight::*;
use arrow::array::{BooleanArray, Int32Array, StringArray};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_schema::{DataType, Field, Schema};
use clade::flight::{DataPutCommand, DataPutResult};
use clade::schema::StorageLocation;
use futures::StreamExt;
use seafowl::frontend::flight::SEAFOWL_PUT_DATA_UD_FLAG;

#[tokio::test]
async fn test_basic_upload() -> Result<()> {
    let (_context, mut client) = flight_server().await;

    let schema = Arc::new(Schema::new(vec![
        Field::new("col_1", DataType::Int32, true),
        Field::new("col_2", DataType::Utf8, true),
        Field::new(SEAFOWL_PUT_DATA_UD_FLAG, DataType::Boolean, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("one"), Some("two")])),
            Arc::new(BooleanArray::from(vec![false, false])),
        ],
    )
    .unwrap();

    let cmd = DataPutCommand {
        path: "/path/to/table".to_string(),
        store: Some(StorageLocation {
            location: "memory://".to_string(),
            options: Default::default(),
        }),
        pk_column: vec!["col_1".to_string()],
        origin: "test-origin".to_string(),
        sequence_number: 123,
    };

    let descriptor = FlightDescriptor::new_cmd(cmd.as_any().encode_to_vec());
    let flight_stream_builder =
        FlightDataEncoderBuilder::new().with_flight_descriptor(Some(descriptor));
    let flight_data = flight_stream_builder.build(futures::stream::iter(vec![Ok(batch)]));
    let response = client.do_put(flight_data).await?.next().await.unwrap()?;

    // Changes are still in memory
    let put_result = DataPutResult::decode(response.app_metadata).expect("DataPutResult");
    assert_eq!(
        put_result,
        DataPutResult {
            accepted: true,
            memory_sequence_number: Some(123),
            durable_sequence_number: None,
        }
    );

    Ok(())
}
