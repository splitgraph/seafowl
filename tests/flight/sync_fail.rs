use crate::flight::*;

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
    let (_ctx, mut client) = flight_server().await;

    let schema = Arc::new(Schema::new(vec![Field::new("c1", DataType::Int32, true)]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![1, 2]))],
    )?;

    let table_uuid = Uuid::new_v4();
    let mut cmd = DataSyncCommand {
        path: table_uuid.to_string(),
        store: None,
        pk_columns: vec![],
        origin: 1,
        sequence_number: 42,
        last: true,
    };

    // No PKs provided
    assert_sync_error(
        cmd.clone(),
        batch.clone(),
        &mut client,
        "status: Unimplemented, message: \"Changes to tables without primary keys are not supported\""
    ).await;

    // Non-existent PK column
    cmd.pk_columns = vec!["c1".to_string(), "c2".to_string()];
    assert_sync_error(
        cmd.clone(),
        batch.clone(),
        &mut client,
        r#"status: InvalidArgument, message: "Some PKs in [\"c1\", \"c2\"] not present in the schema"#
    ).await;

    // Missing upsert/delete column
    cmd.pk_columns = vec!["c1".to_string()];
    assert_sync_error(
        cmd.clone(),
        batch.clone(),
        &mut client,
        r#"status: InvalidArgument, message: "Change requested but batches do not contain upsert/delete flag as last column `__seafowl_ud`""#
    ).await;

    Ok(())
}
