use std::sync::Arc;

use datafusion::assert_batches_eq;
use datafusion::prelude::{SessionConfig, SessionContext};
use object_store::memory::InMemory;
use seafowl::{
    catalog::{PostgresCatalog, TableCatalog},
    context::SeafowlContext,
    repository::testutils::make_repository,
};

// TODO use envvars or something
const DEV_DB_DSN: &str = "postgresql://sgr:password@localhost:7432/seafowl";

/// Make a SeafowlContext that's connected to a real PostgreSQL database
/// (but uses an in-memory object store)
async fn make_context_with_pg() -> SeafowlContext {
    let session_config = SessionConfig::new()
        .with_information_schema(true)
        .with_default_catalog_and_schema("default", "public");

    let context = SessionContext::with_config(session_config);
    let object_store = Arc::new(InMemory::new());
    context
        .runtime_env()
        .register_object_store("seafowl", "", object_store);

    let repository = Arc::new(make_repository(DEV_DB_DSN).await);
    let catalog = Arc::new(PostgresCatalog { repository });

    // Create a default database and collection
    let database_id = catalog.create_database("default").await;
    catalog.create_collection(database_id, "public").await;

    let result = SeafowlContext {
        inner: context,
        table_catalog: catalog.clone(),
        region_catalog: catalog.clone(),
        database: "default".to_string(),
        database_id,
    };

    // Register our database with DataFusion
    result.reload_schema().await;
    result
}

#[tokio::test]
async fn test_information_schema() {
    let context = make_context_with_pg().await;

    let plan = context
        .plan_query("SELECT * FROM information_schema.tables ORDER BY table_catalog, table_name")
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();

    let expected = vec![
        "+---------------+--------------------+------------+------------+",
        "| table_catalog | table_schema       | table_name | table_type |",
        "+---------------+--------------------+------------+------------+",
        "| datafusion    | information_schema | columns    | VIEW       |",
        "| datafusion    | information_schema | tables     | VIEW       |",
        "| default       | information_schema | columns    | VIEW       |",
        "| default       | information_schema | tables     | VIEW       |",
        "+---------------+--------------------+------------+------------+",
    ];

    assert_batches_eq!(expected, &results);
}

#[tokio::test]
async fn test_create_table() {
    let context = make_context_with_pg().await;

    let plan = context
        .plan_query(
            "CREATE TABLE test_table (
            some_time TIMESTAMP,
            some_value REAL,
            some_other_value NUMERIC,
            some_bool_value BOOLEAN,
            some_int_value BIGINT)",
        )
        .await
        .unwrap();
    context.collect(plan).await.unwrap();

    // reregister / reload the catalog
    context.reload_schema().await;

    // Check table columns
    let plan = context
        .plan_query(
            "SELECT table_schema, table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_catalog = 'default'
        ORDER BY table_name, column_name",
        )
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();

    let expected = vec![
        "+--------------+------------+------------------+-----------------------------+",
        "| table_schema | table_name | column_name      | data_type                   |",
        "+--------------+------------+------------------+-----------------------------+",
        "| public       | test_table | some_bool_value  | Boolean                     |",
        "| public       | test_table | some_int_value   | Int64                       |",
        "| public       | test_table | some_other_value | Decimal(38, 10)             |",
        "| public       | test_table | some_time        | Timestamp(Nanosecond, None) |",
        "| public       | test_table | some_value       | Float32                     |",
        "+--------------+------------+------------------+-----------------------------+",
    ];

    assert_batches_eq!(expected, &results);
}

#[tokio::test]
async fn test_create_table_and_insert() {
    let context = make_context_with_pg().await;

    // Need to reload the schema to pick up the new table version
    context.reload_schema().await;

    // Check table columns: make sure scanning through our file pads the rest with NULLs
    let plan = context
        .plan_query("SELECT * FROM test_table")
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();

    // TODO why are we not seeing some_time (timestamp values here?)
    let expected = vec![
        "+-----------------+----------------+------------------+-----------+------------+",
        "| some_bool_value | some_int_value | some_other_value | some_time | some_value |",
        "+-----------------+----------------+------------------+-----------+------------+",
        "|                 | 1111           |                  |           | 42         |",
        "|                 | 2222           |                  |           | 43         |",
        "|                 | 3333           |                  |           | 44         |",
        "+-----------------+----------------+------------------+-----------+------------+",
    ];

    assert_batches_eq!(expected, &results);

    // Test some projections and aggregations
    let plan = context
        .plan_query("SELECT MAX(some_time) FROM test_table")
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();

    let expected = vec![
        "+---------------------------+",
        "| MAX(test_table.some_time) |",
        "+---------------------------+",
        "|                           |",
        "+---------------------------+",
    ];

    assert_batches_eq!(expected, &results);

    let plan = context
        .plan_query("SELECT MAX(some_int_value), COUNT(DISTINCT some_bool_value), MAX(some_value) FROM test_table")
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();

    let expected = vec![
        "+--------------------------------+--------------------------------------------+----------------------------+",
        "| MAX(test_table.some_int_value) | COUNT(DISTINCT test_table.some_bool_value) | MAX(test_table.some_value) |",
        "+--------------------------------+--------------------------------------------+----------------------------+",
        "| 3333                           | 0                                          | 44                         |",
        "+--------------------------------+--------------------------------------------+----------------------------+",
    ];

    assert_batches_eq!(expected, &results);
}

#[tokio::test]
async fn test_insert_two_different_schemas() {
    let context = make_context_with_pg().await;

    let plan = context
        .plan_query(
            "CREATE TABLE test_table (
            some_time TIMESTAMP,
            some_value REAL,
            some_other_value NUMERIC,
            some_bool_value BOOLEAN,
            some_int_value BIGINT)",
        )
        .await
        .unwrap();
    context.collect(plan).await.unwrap();

    context.reload_schema().await;

    let plan = context
        .plan_query(
            "INSERT INTO test_table (some_int_value, some_time, some_value) VALUES
                (1111, '20:01:01', 42),
                (2222, '20:02:02', 43),
                (3333, '20:03:03', 44)",
        )
        .await
        .unwrap();
    context.collect(plan).await.unwrap();

    context.reload_schema().await;

    let plan = context
        .plan_query(
            "INSERT INTO test_table (some_value, some_bool_value, some_other_value) VALUES
                (41, FALSE, 2.15),
                (45, TRUE, 9.12),
                (NULL, FALSE, 44.34)",
        )
        .await
        .unwrap();
    context.collect(plan).await.unwrap();

    context.reload_schema().await;

    let plan = context
        .plan_query("SELECT * FROM test_table")
        .await
        .unwrap();
    let results = context.collect(plan).await.unwrap();

    let expected = vec![
        "+-----------------+----------------+------------------+-----------+------------+",
        "| some_bool_value | some_int_value | some_other_value | some_time | some_value |",
        "+-----------------+----------------+------------------+-----------+------------+",
        "|                 | 1111           |                  |           | 42         |",
        "|                 | 2222           |                  |           | 43         |",
        "|                 | 3333           |                  |           | 44         |",
        "| false           |                | 2.1500000000     |           | 41         |",
        "| true            |                | 9.1199999999     |           | 45         |",
        "| false           |                | 44.3400000000    |           |            |",
        "+-----------------+----------------+------------------+-----------+------------+",
    ];
    assert_batches_eq!(expected, &results);
}
