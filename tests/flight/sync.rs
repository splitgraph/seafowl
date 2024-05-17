use crate::flight::*;
use arrow::array::{
    BooleanArray, Float64Array, Int32Array, StringArray, TimestampMicrosecondArray,
};
use arrow_flight::encode::{FlightDataEncoder, FlightDataEncoderBuilder};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use clade::flight::{DataSyncCommand, DataSyncResult};
use futures::StreamExt;
use seafowl::frontend::flight::SEAFOWL_SYNC_DATA_UD_FLAG;
use std::time::Duration;
use uuid::Uuid;

fn put_cmd_to_flight_data(cmd: DataSyncCommand, batch: RecordBatch) -> FlightDataEncoder {
    let descriptor = FlightDescriptor::new_cmd(cmd.as_any().encode_to_vec());
    let flight_stream_builder =
        FlightDataEncoderBuilder::new().with_flight_descriptor(Some(descriptor));
    flight_stream_builder.build(futures::stream::iter(vec![Ok(batch)]))
}

#[tokio::test]
async fn test_basic_upload() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let (ctx, mut client) = flight_server().await;

    // Put #1 that creates the table, and dictates the full schema for following puts
    let schema = Arc::new(Schema::new(vec![
        Field::new("col_1", DataType::Int32, true),
        Field::new("col_2", DataType::Utf8, true),
        Field::new("col_3", DataType::Float64, true),
        Field::new(
            "col_4",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(SEAFOWL_SYNC_DATA_UD_FLAG, DataType::Boolean, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("one"), Some("two")])),
            Arc::new(Float64Array::from(vec![1.0, 2.0])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                978307261000000,
                1012611722000000,
                // 1046660583000000,
                // 1081051444000000,
                // 1115262305000000,
                // 1149566766000000,
                // 1183784827000000,
                // 1218175688000000,
                // 1252480149000000,
            ])),
            Arc::new(BooleanArray::from(vec![true, true])),
        ],
    )?;

    let table_uuid = Uuid::new_v4();
    let mut cmd = DataSyncCommand {
        path: table_uuid.to_string(),
        store: None,
        pk_column: vec!["col_1".to_string()],
        origin: "test-origin".to_string(),
        sequence_number: 12,
    };

    let flight_data = put_cmd_to_flight_data(cmd.clone(), batch);
    let response = client.do_put(flight_data).await?.next().await.unwrap()?;

    // Changes are still in memory
    let put_result =
        DataSyncResult::decode(response.app_metadata).expect("DataSyncResult");
    assert_eq!(
        put_result,
        DataSyncResult {
            accepted: true,
            memory_sequence_number: Some(12),
            durable_sequence_number: None,
        }
    );

    // The sync mechanism doesn't register the table, so for the sake of testing do it here
    ctx.metastore
        .tables
        .create(
            &ctx.default_catalog,
            &ctx.default_schema,
            "replicated_table",
            schema.as_ref(),
            table_uuid,
        )
        .await
        .unwrap();

    // Ensure table is empty
    let plan = ctx.plan_query("SELECT * FROM replicated_table").await?;
    let results = ctx.collect(plan.clone()).await?;
    assert!(results.is_empty());

    // Wait for the replication lag to exceed the configured max duration
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Now go for put #2; this will flush both it and the first put as well
    let schema = Arc::new(Schema::new(vec![
        Field::new("col_1", DataType::Int32, true),
        Field::new("col_3", DataType::Float64, true),
        Field::new(SEAFOWL_SYNC_DATA_UD_FLAG, DataType::Boolean, true),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![3, 4])),
            Arc::new(Float64Array::from(vec![3.0, 4.0])),
            Arc::new(BooleanArray::from(vec![true, true])),
        ],
    )?;
    cmd.sequence_number = 34;

    let flight_data = put_cmd_to_flight_data(cmd.clone(), batch);
    let response = client.do_put(flight_data).await?.next().await.unwrap()?;

    let put_result =
        DataSyncResult::decode(response.app_metadata).expect("DataSyncResult");
    assert_eq!(
        put_result,
        DataSyncResult {
            accepted: true,
            memory_sequence_number: Some(34),
            durable_sequence_number: None,
        }
    );

    // Changes have now been flushed to storage
    let plan = ctx.plan_query("SELECT * FROM replicated_table").await?;
    let results = ctx.collect(plan.clone()).await?;

    let expected = [
        "+-------+-------+-------+---------------------+",
        "| col_1 | col_2 | col_3 | col_4               |",
        "+-------+-------+-------+---------------------+",
        "| 1     | one   | 1.0   | 2001-01-01T00:01:01 |",
        "| 2     | two   | 2.0   | 2002-02-02T01:02:02 |",
        "| 3     |       | 3.0   |                     |",
        "| 4     |       | 4.0   |                     |",
        "+-------+-------+-------+---------------------+",
    ];

    assert_batches_eq!(expected, &results);

    Ok(())
}
