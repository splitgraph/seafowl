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
    let session_config = SessionConfig::new().with_information_schema(true);

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

    // Register our database with DataFusion
    // TODO: this loads all collection/table names into memory, so creating tables within the same
    // session won't reflect the changes without recreating the context.
    context.register_catalog(
        "default",
        Arc::new(catalog.load_database(database_id).await),
    );

    SeafowlContext {
        inner: context,
        table_catalog: catalog.clone(),
        region_catalog: catalog.clone(),
        database: "default".to_string(),
        database_id,
    }
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
