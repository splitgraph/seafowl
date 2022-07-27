use std::sync::Arc;

use crate::{
    catalog::{DefaultCatalog, RegionCatalog, TableCatalog},
    context::{DefaultSeafowlContext, SeafowlContext},
    repository::PostgresRepository,
};
use datafusion::{
    catalog::{
        catalog::{CatalogProvider, MemoryCatalogProvider},
        schema::MemorySchemaProvider,
    },
    prelude::{SessionConfig, SessionContext},
};
use object_store::{local::LocalFileSystem, memory::InMemory, ObjectStore};

use super::schema;

async fn build_catalog(
    config: &schema::SeafowlConfig,
) -> (Arc<dyn TableCatalog>, Arc<dyn RegionCatalog>) {
    match &config.catalog {
        schema::Catalog::Postgres(schema::Postgres { dsn, schema }) => {
            // Initialize the repository
            let repository =
                PostgresRepository::try_new(dsn.to_string(), schema.to_string())
                    .await
                    .expect("Error setting up the database");

            let catalog = Arc::new(DefaultCatalog {
                repository: Arc::new(repository),
            });

            (catalog.clone(), catalog)
        }
    }
}

fn build_object_store(cfg: &schema::SeafowlConfig) -> Arc<dyn ObjectStore> {
    match &cfg.object_store {
        schema::ObjectStore::Local(schema::Local { data_dir }) => Arc::new(
            LocalFileSystem::new_with_prefix(data_dir)
                .expect("Error creating object store"),
        ),
        schema::ObjectStore::InMemory(_) => Arc::new(InMemory::new()),
        schema::ObjectStore::S3(_) => todo!(),
    }
}

pub async fn build_context(cfg: &schema::SeafowlConfig) -> DefaultSeafowlContext {
    let session_config = SessionConfig::new()
        .with_information_schema(true)
        .with_default_catalog_and_schema("default", "public");
    let context = SessionContext::with_config(session_config);

    let object_store = build_object_store(cfg);
    context
        .runtime_env()
        .register_object_store("seafowl", "", object_store);

    let (tables, regions) = build_catalog(cfg).await;

    // Create default DB/collection
    let default_db = match tables.get_database_id_by_name("default").await {
        Some(id) => id,
        None => tables.create_database("default").await,
    };

    match tables.get_collection_id_by_name("default", "public").await {
        Some(id) => id,
        None => tables.create_collection(default_db, "public").await,
    };

    // Register the datafusion catalog (in-memory)
    let default_catalog = MemoryCatalogProvider::new();

    default_catalog
        .register_schema("public", Arc::new(MemorySchemaProvider::new()))
        .expect("memory catalog provider can register schema");
    context.register_catalog("datafusion", Arc::new(default_catalog));

    // Convergence doesn't support connecting to different DB names. We are supposed
    // to do one context per query (as we need to load the schema before executing every
    // query) and per database (since the context is supposed to be limited to the database
    // the user is connected to), but in this case we can just use the same context everywhere, but reload
    // it before we run the query.
    let context = DefaultSeafowlContext {
        inner: context,
        table_catalog: tables,
        region_catalog: regions,
        database: "default".to_string(),
        database_id: default_db,
    };

    // Register our database with DataFusion
    context.reload_schema().await;
    context
}

#[cfg(test)]
mod tests {
    use std::env;

    use super::*;

    #[tokio::test]
    async fn test_config_to_context() {
        let dsn = env::var("DATABASE_URL").unwrap();

        let config = schema::SeafowlConfig {
            object_store: schema::ObjectStore::InMemory(schema::InMemory {}),
            catalog: schema::Catalog::Postgres(schema::Postgres {
                dsn,
                schema: "public".to_string(),
            }),
            frontend: schema::Frontend {
                postgres: Some(schema::PostgresFrontend {
                    bind_host: "127.0.0.1".to_string(),
                    bind_port: 6432,
                }),
                http: Some(schema::HttpFrontend {
                    bind_host: "127.0.0.1".to_string(),
                    bind_port: 80,
                }),
            },
        };

        let context = build_context(&config).await;

        // Run a query against the context to test it works
        let results = context
            .collect(context.plan_query("SHOW TABLES").await.unwrap())
            .await
            .unwrap();
        assert!(!results.is_empty());
    }
}
