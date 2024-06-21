use crate::flight::*;
use clade::sync::{ColumnDescriptor, ColumnRole};

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
    let (ctx, mut client) = flight_server(false).await;

    //
    // Sync #1 that creates the table, and dictates the full schema for following syncs
    //

    let f1 = Field::new("old_c1", DataType::Int32, true);
    let cd1 = ColumnDescriptor {
        role: ColumnRole::OldPk as i32,
        name: "c1".to_string(),
    };

    let f2 = Field::new("old_c2", DataType::Utf8, true);
    let cd2 = ColumnDescriptor {
        role: ColumnRole::OldPk as i32,
        name: "c2".to_string(),
    };

    let f3 = Field::new("new_c1", DataType::Int32, true);
    let cd3 = ColumnDescriptor {
        role: ColumnRole::NewPk as i32,
        name: "c1".to_string(),
    };

    let f4 = Field::new("new_c2", DataType::Utf8, true);
    let cd4 = ColumnDescriptor {
        role: ColumnRole::NewPk as i32,
        name: "c2".to_string(),
    };

    let f5 = Field::new("value_c3", DataType::Float64, true);
    let cd5 = ColumnDescriptor {
        role: ColumnRole::Value as i32,
        name: "c3".to_string(),
    };

    let f6 = Field::new("value_c4", DataType::Utf8, true);
    let cd6 = ColumnDescriptor {
        role: ColumnRole::Value as i32,
        name: "c4".to_string(),
    };

    let f7 = Field::new("changed_c4", DataType::Boolean, false);
    let cd7 = ColumnDescriptor {
        role: ColumnRole::Changed as i32,
        name: "c4".to_string(),
    };

    let schema = Arc::new(Schema::new(vec![
        f1.clone(),
        f2.clone(),
        f3.clone(),
        f4.clone(),
        f5.clone(),
        f6.clone(),
        f7.clone(),
    ]));

    let column_descriptors = vec![
        cd1.clone(),
        cd2.clone(),
        cd3.clone(),
        cd4.clone(),
        cd5.clone(),
        cd6.clone(),
        cd7.clone(),
    ];

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![None, None])),
            Arc::new(StringArray::from(vec![None::<&str>, None::<&str>])),
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("one"), Some("two")])),
            Arc::new(Float64Array::from(vec![1.0, 2.0])),
            Arc::new(StringArray::from(vec![None, Some("two #1 in sync #1")])),
            Arc::new(BooleanArray::from(vec![false, true])),
        ],
    )?;

    let table_uuid = Uuid::new_v4();
    let mut cmd = DataSyncCommand {
        path: table_uuid.to_string(),
        store: None,
        column_descriptors,
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

    // Ensure table doesn't exist yet
    let err = ctx
        .plan_query("SELECT * FROM replicated_table")
        .await
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "External error: Not a Delta table: no log files".to_string()
    );

    //
    // Now go for sync #2; this will flush both it and the first sync as well
    //

    let schema = Arc::new(Schema::new(vec![
        f2.clone(),
        f4.clone(),
        f3.clone(),
        f1.clone(),
        f6.clone(),
    ]));
    cmd.column_descriptors = vec![
        cd2.clone(),
        cd4.clone(),
        cd3.clone(),
        cd1.clone(),
        cd6.clone(),
    ];
    cmd.last = true;

    // Update row 1 such that we omit the float column and so it should inherit the old value from
    // the previous sync.
    // Also, explicitly set the last column to None, but only in the last row (i.e. test that
    // multiple updates to the same row use the last value) and change on of the PKs.
    // Also mix up the columns from the order in the first sync.
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![
                Some("one"),
                None,
                Some("one"),
                None,
                Some("1"),
            ])),
            Arc::new(StringArray::from(vec![
                Some("one"),
                Some("three"),
                Some("1"),
                Some("four"),
                Some("1"),
            ])),
            Arc::new(Int32Array::from(vec![1, 3, 1, 4, 1])),
            Arc::new(Int32Array::from(vec![
                Some(1),
                None,
                Some(1),
                None,
                Some(1),
            ])),
            Arc::new(StringArray::from(vec![
                Some("one #1 in sync #2"),
                None,
                None,
                Some("four"),
                Some("one #2 in sync #2"),
            ])),
        ],
    )?;

    let sync_result = do_put_sync(cmd.clone(), batch, &mut client).await?;
    assert_eq!(
        sync_result,
        DataSyncResult {
            accepted: true,
            memory_sequence_number: Some(1234),
            durable_sequence_number: None,
        }
    );

    // Wait for the replication lag to exceed the configured max duration and for the flush task to
    // pick it up
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Changes should have now been flushed to storage by the flush task
    let plan = ctx.plan_query("SELECT * FROM replicated_table").await?;
    let results = ctx.collect(plan.clone()).await?;

    let expected = [
        "+----+-------+-----+-------------------+",
        "| c1 | c2    | c3  | c4                |",
        "+----+-------+-----+-------------------+",
        "| 1  | 1     | 1.0 | one #2 in sync #2 |",
        "| 2  | two   | 2.0 | two #1 in sync #1 |",
        "| 3  | three |     |                   |",
        "| 4  | four  |     | four              |",
        "+----+-------+-----+-------------------+",
    ];

    assert_batches_sorted_eq!(expected, &results);

    //
    // Check empty payload shows the first flush
    //

    cmd.column_descriptors = vec![];
    let sync_result =
        do_put_sync(cmd.clone(), RecordBatch::new_empty(schema), &mut client).await?;
    assert_eq!(
        sync_result,
        DataSyncResult {
            accepted: true,
            memory_sequence_number: Some(1234),
            durable_sequence_number: Some(1234),
        }
    );

    //
    // Sync #3; this will be held in memory
    //

    cmd.sequence_number = 5600;
    cmd.last = true;
    cmd.column_descriptors = vec![
        cd5.clone(),
        cd4.clone(),
        cd3.clone(),
        cd2.clone(),
        cd1.clone(),
    ];
    let schema = Arc::new(Schema::new(vec![
        f5.clone(),
        f4.clone(),
        f3.clone(),
        f2.clone(),
        f1.clone(),
    ]));

    // Have one row (5, 'five') be an append, followed by delete (2, 'two') followed by append (6, 'six')
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Float64Array::from(vec![Some(5.0), Some(2.0), None])),
            Arc::new(StringArray::from(vec![Some("five"), None, Some("six")])),
            Arc::new(Int32Array::from(vec![Some(5), None, Some(6)])),
            Arc::new(StringArray::from(vec![None, Some("two"), None])),
            Arc::new(Int32Array::from(vec![None, Some(2), None])),
        ],
    )?;

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

    cmd.sequence_number = 78910;
    cmd.last = false;
    cmd.column_descriptors = vec![
        cd1.clone(),
        cd2.clone(),
        cd3.clone(),
        cd4.clone(),
        cd6.clone(),
        cd7.clone(),
    ];

    let schema = Arc::new(Schema::new(vec![
        f1.clone(),
        f2.clone(),
        f3.clone(),
        f4.clone(),
        f6.clone(),
        f7.clone(),
    ]));

    // - update a row from the previous sequence
    // - update a row from the first sequence and change the PK to a row that was deleted
    // - append some rows
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![None, Some(6), None, Some(4)])),
            Arc::new(StringArray::from(vec![
                None,
                Some("six"),
                None,
                Some("four"),
            ])),
            Arc::new(Int32Array::from(vec![7, 6, 8, 2])),
            Arc::new(StringArray::from(vec![
                Some("seven"),
                Some("six"),
                Some("eight"),
                Some("two"),
            ])),
            Arc::new(StringArray::from(vec![
                Some("seven"),
                Some("six in sync #4"),
                None,
                None,
            ])),
            Arc::new(BooleanArray::from(vec![false, true, true, false])),
        ],
    )?;

    let sync_result = do_put_sync(cmd.clone(), batch, &mut client).await?;
    assert_eq!(
        sync_result,
        DataSyncResult {
            accepted: true,
            memory_sequence_number: Some(5600), // again 78910 not in memory since `last = false`
            durable_sequence_number: Some(1234),
        }
    );

    //
    // Check empty payload reflects the current state
    //

    cmd.column_descriptors = vec![];
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

    cmd.last = true;
    cmd.column_descriptors = vec![
        cd2.clone(),
        cd1.clone(),
        cd5.clone(),
        cd4.clone(),
        cd3.clone(),
    ];
    let schema = Arc::new(Schema::new(vec![
        f2.clone(),
        f1.clone(),
        f5.clone(),
        f4.clone(),
        f3.clone(),
    ]));

    // Update a row from the first sequence.
    // Also delete a row from the previous sync of this sequence with a pre-ceding change that just
    // updates it.
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![
                Some("three"),
                None,
                Some("eight"),
                None,
                Some("eight"),
            ])),
            Arc::new(Int32Array::from(vec![
                Some(3),
                None,
                Some(8),
                None,
                Some(8),
            ])),
            Arc::new(Float64Array::from(vec![
                Some(3.3),
                None,
                Some(8.8),
                Some(10.0),
                None,
            ])),
            Arc::new(StringArray::from(vec![
                Some("3"),
                Some("nine"),
                Some("eight"),
                Some("ten"),
                None,
            ])),
            Arc::new(Int32Array::from(vec![
                Some(3),
                Some(9),
                Some(8),
                Some(10),
                None,
            ])),
        ],
    )?;

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
        "+----+-------+------+-------------------+",
        "| c1 | c2    | c3   | c4                |",
        "+----+-------+------+-------------------+",
        "| 1  | 1     | 1.0  | one #2 in sync #2 |",
        "| 10 | ten   | 10.0 |                   |",
        "| 2  | two   |      | four              |",
        "| 3  | 3     | 3.3  |                   |",
        "| 5  | five  | 5.0  |                   |",
        "| 6  | six   |      | six in sync #4    |",
        "| 7  | seven |      |                   |",
        "| 9  | nine  |      |                   |",
        "+----+-------+------+-------------------+",
    ];

    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}
