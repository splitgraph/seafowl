use crate::flight::*;

pub(crate) fn sync_cmd_to_flight_data(
    cmd: DataSyncCommand,
    batch: RecordBatch,
) -> FlightDataEncoder {
    let descriptor = FlightDescriptor::new_cmd(cmd.as_any().encode_to_vec());
    let flight_stream_builder =
        FlightDataEncoderBuilder::new().with_flight_descriptor(Some(descriptor));
    flight_stream_builder.build(futures::stream::iter(vec![Ok(batch)]))
}

async fn do_put_sync(
    cmd: DataSyncCommand,
    batch: RecordBatch,
    client: &mut FlightClient,
) -> Result<DataSyncResult> {
    let flight_data = sync_cmd_to_flight_data(cmd.clone(), batch);
    let response = client.do_put(flight_data).await?.next().await.unwrap()?;

    Ok(DataSyncResult::decode(response.app_metadata).expect("DataSyncResult"))
}

#[tokio::test]
async fn test_sync_happy_path() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let (ctx, mut client) = flight_server().await;

    //
    // Sync #1 that creates the table, and dictates the full schema for following syncs
    //

    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Int32, false),
        Field::new("c2", DataType::Utf8, false),
        Field::new("c3", DataType::Float64, true),
        Field::new("c4", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        Field::new("c5", DataType::Boolean, true),
        Field::new(SEAFOWL_SYNC_DATA_UD_FLAG, DataType::Boolean, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("one"), Some("two")])),
            Arc::new(Float64Array::from(vec![1.0, 2.0])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                978310861000000,
                1012615322000000,
            ])),
            Arc::new(BooleanArray::from(vec![false, false])),
            Arc::new(BooleanArray::from(vec![true, true])), // all-append batch
        ],
    )?;

    let table_uuid = Uuid::new_v4();
    let mut cmd = DataSyncCommand {
        path: table_uuid.to_string(),
        store: None,
        pk_columns: vec!["c1".to_string(), "c2".to_string()],
        origin: 42,
        sequence_number: 1234,
        last: false,
    };

    // Changes are still in memory
    let sync_result = do_put_sync(cmd.clone(), batch, &mut client).await?;
    assert_eq!(
        sync_result,
        DataSyncResult {
            accepted: true,
            memory_sequence_number: None, // sequence not in memory because of  `last: false`
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
    tokio::time::sleep(Duration::from_secs(1)).await;

    //
    // Now go for sync #2; this will flush both it and the first sync as well
    //

    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Int32, false),
        Field::new("c3", DataType::Float64, true),
        Field::new("c5", DataType::Boolean, true),
        Field::new("c2", DataType::Utf8, false),
        Field::new(SEAFOWL_SYNC_DATA_UD_FLAG, DataType::Boolean, true),
    ]));

    // Update row 1 such that we omit the timestamp column (c4) and so it should inherit the old
    // value from the previous sync, and explicitly set the boolean column to None
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![3, 4, 1])),
            Arc::new(Float64Array::from(vec![3.0, 4.0, 1.1])),
            Arc::new(BooleanArray::from(vec![Some(false), Some(false), None])),
            Arc::new(StringArray::from(vec![
                Some("three"),
                Some("four"),
                Some("one"),
            ])),
            Arc::new(BooleanArray::from(vec![true, true, true])),
        ],
    )?;
    cmd.last = true;

    let sync_result = do_put_sync(cmd.clone(), batch, &mut client).await?;
    assert_eq!(
        sync_result,
        DataSyncResult {
            accepted: true,
            memory_sequence_number: Some(1234),
            durable_sequence_number: Some(1234),
        }
    );

    // Changes have now been flushed to storage
    let plan = ctx.plan_query("SELECT * FROM replicated_table").await?;
    let results = ctx.collect(plan.clone()).await?;

    let expected = [
        "+----+-------+-----+---------------------+-------+",
        "| c1 | c2    | c3  | c4                  | c5    |",
        "+----+-------+-----+---------------------+-------+",
        "| 1  | one   | 1.1 | 2001-01-01T01:01:01 |       |",
        "| 2  | two   | 2.0 | 2002-02-02T02:02:02 | false |",
        "| 3  | three | 3.0 |                     | false |",
        "| 4  | four  | 4.0 |                     | false |",
        "+----+-------+-----+---------------------+-------+",
    ];

    assert_batches_sorted_eq!(expected, &results);

    //
    // Sync #3; this will be held in memory
    //

    let schema = Arc::new(Schema::new(vec![
        Field::new("c4", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        Field::new("c2", DataType::Utf8, false),
        Field::new("c1", DataType::Int32, false),
        Field::new(SEAFOWL_SYNC_DATA_UD_FLAG, DataType::Boolean, true),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(TimestampMicrosecondArray::from(vec![
                1115269505000000,
                1149573966000000,
            ])),
            Arc::new(StringArray::from(vec![Some("five"), Some("six")])),
            Arc::new(Int32Array::from(vec![5, 6])),
            Arc::new(BooleanArray::from(vec![true, true])),
        ],
    )?;
    cmd.sequence_number = 5600;
    cmd.last = true;

    let sync_result = do_put_sync(cmd.clone(), batch, &mut client).await?;
    assert_eq!(
        sync_result,
        DataSyncResult {
            accepted: true,
            memory_sequence_number: Some(5600),
            durable_sequence_number: Some(1234),
        }
    );

    //
    // Sync #4
    //

    let schema = Arc::new(Schema::new(vec![
        Field::new("c2", DataType::Utf8, false),
        Field::new("c1", DataType::Int32, false),
        Field::new("c3", DataType::Float64, true),
        Field::new(SEAFOWL_SYNC_DATA_UD_FLAG, DataType::Boolean, true),
    ]));

    // Update a row from the first sequence
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![
                Some("seven"),
                Some("four"),
                Some("eight"),
            ])),
            Arc::new(Int32Array::from(vec![7, 4, 8])),
            Arc::new(Float64Array::from(vec![7.0, 4.4, 8.0])),
            Arc::new(BooleanArray::from(vec![true, true, true])),
        ],
    )?;
    cmd.sequence_number = 78910;
    cmd.last = false;

    let sync_result = do_put_sync(cmd.clone(), batch, &mut client).await?;
    assert_eq!(
        sync_result,
        DataSyncResult {
            accepted: true,
            memory_sequence_number: Some(5600), // again 78910 not in memory since `last = false`
            durable_sequence_number: Some(1234),
        }
    );

    // Check empty payload reflects the current state
    let sync_result =
        do_put_sync(cmd.clone(), RecordBatch::new_empty(schema), &mut client).await?;
    assert_eq!(
        sync_result,
        DataSyncResult {
            accepted: true,
            memory_sequence_number: Some(5600),
            durable_sequence_number: Some(1234),
        }
    );

    //
    // Sync #5 to flush the previous 2 syncs due to max size threshold
    //

    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Int32, false),
        Field::new("c4", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        Field::new("c5", DataType::Boolean, true),
        Field::new("c2", DataType::Utf8, false),
        Field::new(SEAFOWL_SYNC_DATA_UD_FLAG, DataType::Boolean, true),
    ]));

    // Update a row from the first sequence, delete a row from the first sync in this sequence
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![4, 9, 10, 6])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                None,
                Some(1252487349000000),
                Some(1286705410000000),
                None,
            ])),
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(false),
                Some(false),
                None,
            ])),
            Arc::new(StringArray::from(vec![
                Some("four"),
                Some("nine"),
                Some("ten"),
                Some("six"),
            ])),
            Arc::new(BooleanArray::from(vec![true, true, true, false])),
        ],
    )?;
    cmd.last = true;

    let sync_result = do_put_sync(cmd.clone(), batch, &mut client).await?;
    assert_eq!(
        sync_result,
        DataSyncResult {
            accepted: true,
            memory_sequence_number: Some(78910),
            durable_sequence_number: Some(78910),
        }
    );

    // Finally check that all changes have been flushed accordingly
    let plan = ctx.plan_query("SELECT * FROM replicated_table").await?;
    let results = ctx.collect(plan.clone()).await?;

    let expected = [
        "+----+-------+-----+---------------------+-------+",
        "| c1 | c2    | c3  | c4                  | c5    |",
        "+----+-------+-----+---------------------+-------+",
        "| 1  | one   | 1.1 | 2001-01-01T01:01:01 |       |",
        "| 10 | ten   |     | 2010-10-10T10:10:10 | false |",
        "| 2  | two   | 2.0 | 2002-02-02T02:02:02 | false |",
        "| 3  | three | 3.0 |                     | false |",
        "| 4  | four  | 4.4 |                     | true  |",
        "| 5  | five  |     | 2005-05-05T05:05:05 |       |",
        "| 7  | seven | 7.0 |                     |       |",
        "| 8  | eight | 8.0 |                     |       |",
        "| 9  | nine  |     | 2009-09-09T09:09:09 | false |",
        "+----+-------+-----+---------------------+-------+",
    ];

    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}
