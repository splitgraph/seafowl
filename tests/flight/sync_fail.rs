use crate::flight::*;
use clade::sync::{ColumnDescriptor, ColumnRole};

async fn assert_sync_error(
    cmd: DataSyncCommand,
    batch: RecordBatch,
    client: &mut FlightClient,
    message: &str,
) {
    let flight_data = sync::sync_cmd_to_flight_data(cmd, batch);
    if let Err(FlightError::Tonic(err)) = client.do_put(flight_data).await {
        assert!(err.to_string().contains(message), "Unexpected error: {err}");
    } else {
        panic!("Sync FlightError expected");
    }
}

#[tokio::test]
async fn test_sync_errors() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let (_ctx, mut client) = flight_server(TestServerType::Memory).await;

    let schema = Arc::new(Schema::new(vec![Field::new(
        "old_c1",
        DataType::Int32,
        true,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![1, 2]))],
    )?;

    let table_uuid = Uuid::new_v4();
    let mut cmd = DataSyncCommand {
        path: table_uuid.to_string(),
        store: None,
        column_descriptors: vec![],
        origin: "1".to_string(),
        sequence_number: Some(42),
    };

    // No column descriptors provided
    assert_sync_error(
        cmd.clone(),
        batch.clone(),
        &mut client,
        r#"status: InvalidArgument, message: "Invalid sync schema: Column descriptors do not match the schema"#
    ).await;

    // Provided column descriptors without new PKs
    cmd.column_descriptors = vec![ColumnDescriptor {
        role: ColumnRole::OldPk as i32,
        name: "c1".to_string(),
    }];
    assert_sync_error(
        cmd.clone(),
        batch.clone(),
        &mut client,
        r#"status: InvalidArgument, message: "Invalid sync schema: Change requested but batches do not contain old/new PK columns"#
    ).await;

    let schema = Arc::new(Schema::new(vec![
        Field::new("old_c1", DataType::Int32, true),
        Field::new("new_c1", DataType::Int32, true),
        Field::new("changed_c2", DataType::Boolean, true),
    ]));
    cmd.column_descriptors = vec![
        ColumnDescriptor {
            role: ColumnRole::OldPk as i32,
            name: "c1".to_string(),
        },
        ColumnDescriptor {
            role: ColumnRole::NewPk as i32,
            name: "c1".to_string(),
        },
        ColumnDescriptor {
            role: ColumnRole::Changed as i32,
            name: "c2".to_string(),
        },
    ];
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(Int32Array::from(vec![2, 1])),
            Arc::new(BooleanArray::from(vec![true, false])),
        ],
    )?;

    // Missing upsert/delete column
    assert_sync_error(
        cmd.clone(),
        batch.clone(),
        &mut client,
        r#"status: InvalidArgument, message: "Invalid sync schema: Field for column with `Changed` role can not be nullable: changed_c2""#
    ).await;

    Ok(())
}
