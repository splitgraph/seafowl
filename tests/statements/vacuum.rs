use crate::statements::*;

#[tokio::test]
async fn test_vacuum_table() -> Result<()> {
    let (context, _) = make_context_with_pg(ObjectStoreType::Local).await;

    // Create table_1 and make tombstone by replacing the first file
    create_table_and_insert(&context, "table_1").await;
    context
        .plan_query("DELETE FROM table_1 WHERE some_value = 42")
        .await
        .unwrap();

    // Creates table_2 but append a new file instead of replacing the first one
    create_table_and_insert(&context, "table_2").await;
    context
        .plan_query("INSERT INTO table_2 (some_int_value) VALUES (4444), (5555), (6666)")
        .await
        .unwrap();

    // Check current table versions
    let plan = context
        .plan_query("SELECT table_name, version FROM system.table_versions")
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();

    let expected = [
        "+------------+---------+",
        "| table_name | version |",
        "+------------+---------+",
        "| table_1    | 0       |",
        "| table_1    | 1       |",
        "| table_1    | 2       |",
        "| table_2    | 0       |",
        "| table_2    | 1       |",
        "| table_2    | 2       |",
        "+------------+---------+",
    ];
    assert_batches_eq!(expected, &results);

    // Fetch data relevant for the test
    let table_1_uuid = context.get_table_uuid("table_1").await?;
    let table_2_uuid = context.get_table_uuid("table_2").await?;
    let mut table_1 = context.try_get_delta_table("table_1").await?;
    let mut table_2 = context.try_get_delta_table("table_2").await?;

    table_1.load_version(1).await?;
    let table_1_v1_file = table_1.get_files()[0].clone();
    table_1.load_version(2).await?;
    let table_1_v2_file = table_1.get_files()[0].clone();

    table_2.load().await?;
    let table_2_v1_file = table_2.get_files()[0].clone();
    table_2.load_version(2).await?;
    let table_2_v2_file = table_2.get_files()[1].clone();

    // Check initial directory state
    testutils::assert_uploaded_objects(
        context
            .internal_object_store
            .get_log_store(table_1_uuid)
            .object_store(),
        vec![
            Path::from("_delta_log/00000000000000000000.json"),
            Path::from("_delta_log/00000000000000000001.json"),
            Path::from("_delta_log/00000000000000000002.json"),
            table_1_v1_file,
            table_1_v2_file.clone(),
        ],
    )
    .await;

    // Run vacuum on table_1 to remove tombstoned file
    context.plan_query("VACUUM TABLE table_1").await.unwrap();

    // Check table versions again; table_1 now only has latest version
    let plan = context
        .plan_query("SELECT table_name, version FROM system.table_versions")
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();

    let expected = [
        "+------------+---------+",
        "| table_name | version |",
        "+------------+---------+",
        "| table_1    | 2       |",
        "| table_2    | 0       |",
        "| table_2    | 1       |",
        "| table_2    | 2       |",
        "+------------+---------+",
    ];
    assert_batches_eq!(expected, &results);

    // Check table_1_v1_file is gone as it's been tombstoned's by the `DELETE` command. Note that
    // two additional delta log entries have been made, corresponding to `VACUUM START` and
    // `VACUUM END` operations.
    testutils::assert_uploaded_objects(
        context
            .internal_object_store
            .get_log_store(table_1_uuid)
            .object_store(),
        vec![
            Path::from("_delta_log/00000000000000000000.json"),
            Path::from("_delta_log/00000000000000000001.json"),
            Path::from("_delta_log/00000000000000000002.json"),
            Path::from("_delta_log/00000000000000000003.json"),
            Path::from("_delta_log/00000000000000000004.json"),
            table_1_v2_file,
        ],
    )
    .await;

    // Likewise, trying to time-travel to table_1 v1 will fail
    table_1.load_version(1).await?;
    let err = table_1
        .scan(&context.inner.state(), Some(&vec![4_usize]), &[], None)
        .await
        .unwrap_err();
    assert!(err.to_string().contains(".parquet not found"));

    // Run vacuum on table_2 as well
    context
        .collect(context.plan_query("VACUUM TABLE table_2").await.unwrap())
        .await
        .unwrap();

    // Check both table now have only the latest version
    let plan = context
        .plan_query("SELECT table_name, version FROM system.table_versions")
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();

    let expected = [
        "+------------+---------+",
        "| table_name | version |",
        "+------------+---------+",
        "| table_1    | 2       |",
        "| table_2    | 2       |",
        "+------------+---------+",
    ];
    assert_batches_eq!(expected, &results);

    // However, no changes have been made to the actual table_2 storage since `table_2_v1_file` is
    // referenced by the latest table version.
    testutils::assert_uploaded_objects(
        context
            .internal_object_store
            .get_log_store(table_2_uuid)
            .object_store(),
        vec![
            Path::from("_delta_log/00000000000000000000.json"),
            Path::from("_delta_log/00000000000000000001.json"),
            Path::from("_delta_log/00000000000000000002.json"),
            table_2_v1_file,
            table_2_v2_file,
        ],
    )
    .await;

    // This does mean that the output of `system.table_versions` above is misleading since we can
    // still retrieve data from v1 of table_2 via time-travel.
    table_2.load_version(1).await?;
    let plan = table_2
        .scan(&context.inner.state(), Some(&vec![4_usize]), &[], None)
        .await?;
    let results = context.collect(plan).await.unwrap();

    let expected = [
        "+----------------+",
        "| some_int_value |",
        "+----------------+",
        "| 1111           |",
        "| 2222           |",
        "| 3333           |",
        "+----------------+",
    ];
    assert_batches_eq!(expected, &results);

    Ok(())
}
