use crate::statements::*;

#[ignore = "not yet implemented"]
#[tokio::test]
async fn test_vacuum_command() {
    let context = Arc::new(make_context_with_pg(ObjectStoreType::InMemory).await);

    let get_object_metas = || async {
        context
            .internal_object_store
            .inner
            .list(None)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
    };

    //
    // Create two tables with multiple versions
    //

    // Creates table_1 with table_versions 1 (empty) and 2
    create_table_and_insert(&context, "table_1").await;

    // Make table_1 with table_version 3
    let plan = context
        .plan_query("INSERT INTO table_1 (some_value) VALUES (42)")
        .await
        .unwrap();
    context.collect(plan).await.unwrap();

    // Creates table_2 with table_versions 4 (empty) and 5
    create_table_and_insert(&context, "table_2").await;

    // Make table_2 with table_version 6
    let plan = context
        .plan_query("INSERT INTO table_2 (some_value) VALUES (42), (43), (44)")
        .await
        .unwrap();
    context.collect(plan).await.unwrap();

    // Run vacuum on table_1 to remove previous versions
    context
        .collect(context.plan_query("VACUUM TABLE table_1").await.unwrap())
        .await
        .unwrap();

    // TODO: make more explicit the check for deleted table versions
    for &table_version_id in &[1, 2, 4] {
        assert_eq!(
            get_partition_count(context.clone(), table_version_id).await,
            0
        );
    }
    for &table_version_id in &[3, 5, 6] {
        assert!(get_partition_count(context.clone(), table_version_id).await > 0);
    }

    // Run vacuum on all tables; table_2 will now also lose all but the latest version
    context
        .collect(context.plan_query("VACUUM TABLES").await.unwrap())
        .await
        .unwrap();

    // Check table versions cleared up from partition counts
    for &table_version_id in &[1, 2, 4, 5] {
        assert_eq!(
            get_partition_count(context.clone(), table_version_id).await,
            0
        );
    }
    for &table_version_id in &[3, 6] {
        assert!(get_partition_count(context.clone(), table_version_id).await > 0);
    }

    // Drop tables to leave orphan partitions around
    context
        .collect(context.plan_query("DROP TABLE table_1").await.unwrap())
        .await
        .unwrap();
    context
        .collect(context.plan_query("DROP TABLE table_2").await.unwrap())
        .await
        .unwrap();

    // Check we have orphan partitions
    // NB: we deduplicate object storage IDs here to avoid running the DELETE call
    // twice, but we can have two different partition IDs with same object storage ID
    // See https://github.com/splitgraph/seafowl/issues/5
    let orphans = vec![
        FILENAME_1,
        "d7eaa930f0eba532970d950b74e6c4ff192a30c1d5eb24dcd82e27399ff439ac.parquet",
        "eab1c5d838c2e025ec9048faf15c1585f6c794a3928b1cc8690eeb3a118980b7.parquet",
    ];

    assert_orphan_partitions(context.clone(), orphans.clone()).await;
    let object_metas = get_object_metas().await;
    assert_eq!(object_metas.len(), 3);
    for (ind, &orphan) in orphans
        .into_iter()
        .unique()
        .sorted()
        .collect::<Vec<&str>>()
        .iter()
        .enumerate()
    {
        assert_eq!(object_metas[ind].location, Path::from(orphan));
    }

    // Run vacuum on partitions
    context
        .collect(context.plan_query("VACUUM PARTITIONS").await.unwrap())
        .await
        .unwrap();

    // Ensure no orphan partitions are left
    assert_orphan_partitions(context.clone(), vec![]).await;
    let object_metas = get_object_metas().await;
    assert_eq!(object_metas.len(), 0);
}
