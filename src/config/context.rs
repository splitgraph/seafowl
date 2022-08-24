use std::sync::Arc;

use crate::{
    catalog,
    catalog::{DefaultCatalog, FunctionCatalog, PartitionCatalog, TableCatalog},
    context::{DefaultSeafowlContext, INTERNAL_OBJECT_STORE_SCHEME},
    repository::{interface::Repository, sqlite::SqliteRepository},
};
use datafusion::{
    catalog::{
        catalog::{CatalogProvider, MemoryCatalogProvider},
        schema::MemorySchemaProvider,
    },
    prelude::{SessionConfig, SessionContext},
};
use object_store::{local::LocalFileSystem, memory::InMemory, ObjectStore};

#[cfg(feature = "catalog-postgres")]
use crate::repository::postgres::PostgresRepository;

use crate::http_object_store::add_http_object_store;
#[cfg(feature = "object-store-s3")]
use object_store::aws::new_s3;

use super::schema::{self, S3};

async fn build_catalog(
    config: &schema::SeafowlConfig,
) -> (
    Arc<dyn TableCatalog>,
    Arc<dyn PartitionCatalog>,
    Arc<dyn FunctionCatalog>,
) {
    // Initialize the repository
    let repository: Arc<dyn Repository> = match &config.catalog {
        #[cfg(feature = "catalog-postgres")]
        schema::Catalog::Postgres(schema::Postgres { dsn, schema }) => Arc::new(
            PostgresRepository::try_new(dsn.to_string(), schema.to_string())
                .await
                .expect("Error setting up the database"),
        ),
        schema::Catalog::Sqlite(schema::Sqlite { dsn }) => Arc::new(
            SqliteRepository::try_new(dsn.to_string())
                .await
                .expect("Error setting up the database"),
        ),
    };

    let catalog = Arc::new(DefaultCatalog { repository });

    (catalog.clone(), catalog.clone(), catalog)
}

fn build_object_store(cfg: &schema::SeafowlConfig) -> Arc<dyn ObjectStore> {
    match &cfg.object_store {
        schema::ObjectStore::Local(schema::Local { data_dir }) => Arc::new(
            LocalFileSystem::new_with_prefix(data_dir)
                .expect("Error creating object store"),
        ),
        schema::ObjectStore::InMemory(_) => Arc::new(InMemory::new()),
        #[cfg(feature = "object-store-s3")]
        schema::ObjectStore::S3(S3 {
            access_key_id,
            secret_access_key,
            endpoint,
            bucket,
        }) => {
            // Use endpoint instead of partition
            let store = new_s3(
                Some(access_key_id),
                Some(secret_access_key),
                "",
                bucket,
                Some(endpoint.clone()),
                None as Option<&str>,
                std::num::NonZeroUsize::new(16).unwrap(),
                true,
            )
            .expect("Error creating object store");
            Arc::new(store)
        }
    }
}

pub async fn build_context(
    cfg: &schema::SeafowlConfig,
) -> Result<DefaultSeafowlContext, catalog::Error> {
    let session_config = SessionConfig::new()
        .with_information_schema(true)
        .with_default_catalog_and_schema("default", "public");
    let context = SessionContext::with_config(session_config);

    let object_store = build_object_store(cfg);
    context.runtime_env().register_object_store(
        INTERNAL_OBJECT_STORE_SCHEME,
        "",
        object_store,
    );

    // Register the HTTP object store for external tables
    add_http_object_store(&context);

    let (tables, partitions, functions) = build_catalog(cfg).await;

    // Create default DB/collection
    let default_db = match tables.get_database_id_by_name("default").await? {
        Some(id) => id,
        None => tables.create_database("default").await.unwrap(),
    };

    match tables
        .get_collection_id_by_name("default", "public")
        .await?
    {
        Some(id) => id,
        None => tables.create_collection(default_db, "public").await?,
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
    // the user is connected to), but in this case we can just use the same context everywhere
    // (it will reload its schema before running the query)

    Ok(DefaultSeafowlContext {
        inner: context,
        table_catalog: tables,
        partition_catalog: partitions,
        function_catalog: functions,
        database: "default".to_string(),
        database_id: default_db,
        max_partition_size: cfg.misc.max_partition_size,
    })
}

#[cfg(test)]
mod tests {
    use crate::context::SeafowlContext;

    use super::*;

    #[tokio::test]
    async fn test_config_to_context() {
        let config = schema::SeafowlConfig {
            object_store: schema::ObjectStore::InMemory(schema::InMemory {}),
            catalog: schema::Catalog::Sqlite(schema::Sqlite {
                dsn: "sqlite::memory:".to_string(),
            }),
            frontend: schema::Frontend {
                #[cfg(feature = "frontend-postgres")]
                postgres: Some(schema::PostgresFrontend {
                    bind_host: "127.0.0.1".to_string(),
                    bind_port: 6432,
                }),
                http: Some(schema::HttpFrontend {
                    bind_host: "127.0.0.1".to_string(),
                    bind_port: 80,
                    read_access: schema::AccessSettings::Any,
                    write_access: schema::AccessSettings::Any,
                    upload_data_max_length: 256 * 1024 * 1024,
                }),
            },
            misc: schema::Misc {
                max_partition_size: 1024 * 1024,
            },
        };

        let context = build_context(&config).await.unwrap();

        // Run a query against the context to test it works
        let results = context
            .collect(context.plan_query("SHOW TABLES").await.unwrap())
            .await
            .unwrap();
        assert!(!results.is_empty());
    }
}
