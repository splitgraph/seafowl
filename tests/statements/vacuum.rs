use crate::statements::*;

#[tokio::test]
async fn test_vacuum_command() {
    let context = Arc::new(make_context_with_pg().await);

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
        "5ee15b994299145e8ccf231018bfb2a22193d8b5d2688d5fcf16f1c7c2364e2f.parquet",
        "7d955c5d73f6faa786eb9a45fb773e14b382bde1a5389edb4162cedee39793b0.parquet",
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

#[tokio::test]
async fn test_vacuum_with_reused_file() {
    let context = Arc::new(make_context_with_pg().await);

    // Creates table_1 (empty v1, v2) and table_2 (empty v3, v4)
    // V2 and V4 point to a single identical partition
    create_table_and_insert(&context, "table_1").await;
    create_table_and_insert(&context, "table_2").await;

    assert_eq!(get_partition_count(context.clone(), 4).await, 1);

    // Delete everything from table_2 (creates a new table version V5 without any partitions)
    context
        .collect(context.plan_query("DELETE FROM table_2").await.unwrap())
        .await
        .unwrap();

    // Vacuum (deleting table_2's old version V4)
    context
        .collect(context.plan_query("VACUUM TABLES").await.unwrap())
        .await
        .unwrap();

    // V4 is now deleted (we currently report that as 0 partitions)
    assert_eq!(get_partition_count(context.clone(), 4).await, 0);

    // But v2 is still pointing to the same partition file, so it's not considered orphaned
    assert_orphan_partitions(context.clone(), vec![]).await;

    // Make sure vacuuming partitions does nothing
    context
        .collect(context.plan_query("VACUUM PARTITIONS").await.unwrap())
        .await
        .unwrap();

    // Check the table_1 is still queryable
    let plan = context
        .plan_query("SELECT some_value FROM table_1 ORDER BY some_value")
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();

    let expected = vec![
        "+------------+",
        "| some_value |",
        "+------------+",
        "| 42         |",
        "| 43         |",
        "| 44         |",
        "+------------+",
    ];

    assert_batches_eq!(expected, &results);
}
