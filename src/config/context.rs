use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::{
    catalog::{
        DefaultCatalog, FunctionCatalog, PartitionCatalog, TableCatalog, DEFAULT_DB,
        DEFAULT_SCHEMA,
    },
    context::{DefaultSeafowlContext, INTERNAL_OBJECT_STORE_SCHEME},
    repository::{interface::Repository, sqlite::SqliteRepository},
};
use datafusion::datasource::datasource::TableProviderFactory;
use datafusion::{
    error::DataFusionError,
    execution::runtime_env::{RuntimeConfig, RuntimeEnv},
    prelude::{SessionConfig, SessionContext},
};
use deltalake::delta_datafusion::DeltaTableFactory;
use object_store::{local::LocalFileSystem, memory::InMemory, ObjectStore};

#[cfg(feature = "catalog-postgres")]
use crate::repository::postgres::PostgresRepository;

use crate::config::schema::ObjectCacheProperties;
use crate::object_store::cache::CachingObjectStore;
use crate::object_store::http::add_http_object_store;
use crate::object_store::wrapped::InternalObjectStore;
#[cfg(feature = "remote-tables")]
use datafusion_remote_tables::factory::RemoteTableFactory;
#[cfg(feature = "object-store-s3")]
use object_store::aws::AmazonS3Builder;
use parking_lot::lock_api::RwLock;
use tempfile::TempDir;

use super::schema::{self, MEBIBYTES, MEMORY_FRACTION, S3};

async fn build_catalog(
    config: &schema::SeafowlConfig,
    object_store: Arc<InternalObjectStore>,
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
        schema::Catalog::Sqlite(schema::Sqlite {
            dsn,
            journal_mode,
            read_only: false,
        }) => Arc::new(
            SqliteRepository::try_new(dsn.to_string(), *journal_mode)
                .await
                .expect("Error setting up the database"),
        ),
        schema::Catalog::Sqlite(schema::Sqlite {
            dsn,
            journal_mode,
            read_only: true,
        }) => Arc::new(
            SqliteRepository::try_new_read_only(dsn.to_string(), *journal_mode)
                .await
                .expect("Error setting up the database"),
        ),
    };

    let catalog = Arc::new(DefaultCatalog::new(repository, object_store));

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
            region,
            access_key_id,
            secret_access_key,
            endpoint,
            bucket,
            cache_properties,
        }) => {
            let mut builder = AmazonS3Builder::new()
                .with_access_key_id(access_key_id)
                .with_secret_access_key(secret_access_key)
                .with_region(region.clone().unwrap_or("".to_string()))
                .with_bucket_name(bucket)
                .with_allow_http(true);

            if let Some(endpoint) = endpoint {
                builder = builder.with_endpoint(endpoint);
            }

            let store = builder.build().expect("Error creating object store");

            if let Some(ObjectCacheProperties {
                capacity,
                min_fetch_size,
                ttl_s,
            }) = cache_properties
            {
                let tmp_dir = TempDir::new().unwrap();
                let path = tmp_dir.into_path();

                return Arc::new(CachingObjectStore::new(
                    Arc::new(store),
                    &path,
                    *capacity,
                    *min_fetch_size,
                    Duration::from_secs(*ttl_s),
                ));
            }

            Arc::new(store)
        }
    }
}

#[allow(unused_mut)]
pub async fn build_context(
    cfg: &schema::SeafowlConfig,
) -> Result<DefaultSeafowlContext, DataFusionError> {
    let mut runtime_config = RuntimeConfig::new();
    if let Some(max_memory) = cfg.runtime.max_memory {
        runtime_config = runtime_config
            .with_memory_limit((max_memory * MEBIBYTES) as usize, MEMORY_FRACTION);
    }

    if let Some(temp_dir) = &cfg.runtime.temp_dir {
        runtime_config = runtime_config.with_temp_file_path(temp_dir);
    }

    let session_config = SessionConfig::new()
        .with_information_schema(true)
        .with_default_catalog_and_schema(DEFAULT_DB, DEFAULT_SCHEMA);

    // Make sure we have at least 2 target partitions even on single-core environments
    // (issues with PartitionMode::CollectLeft hash joins if a single target partition)
    let target_partitions = session_config.target_partitions().max(2);
    let session_config = session_config.with_target_partitions(target_partitions);

    // Construct and register additional table factories (e.g. for generating remote tables) besides
    // the default ones for PARQUET, CSV, etc.
    let mut table_factories: HashMap<String, Arc<dyn TableProviderFactory>> =
        HashMap::new();
    table_factories.insert("DELTATABLE".to_string(), Arc::new(DeltaTableFactory {}));
    #[cfg(feature = "remote-tables")]
    {
        table_factories.insert("TABLE".to_string(), Arc::new(RemoteTableFactory {}));
    }

    let mut runtime_env = RuntimeEnv::new(runtime_config)?;
    runtime_env.register_table_factories(table_factories);

    let context = SessionContext::with_config_rt(session_config, Arc::new(runtime_env));

    let object_store = build_object_store(cfg);
    context.runtime_env().register_object_store(
        INTERNAL_OBJECT_STORE_SCHEME,
        "",
        object_store.clone(),
    );

    let internal_object_store = Arc::new(InternalObjectStore::new(
        object_store.clone(),
        cfg.object_store.clone(),
    ));

    // Register the HTTP object store for external tables
    add_http_object_store(&context, &cfg.misc.ssl_cert_file);

    let (tables, partitions, functions) =
        build_catalog(cfg, internal_object_store.clone()).await;

    // Create default DB/collection
    let default_db = match tables.get_database_id_by_name(DEFAULT_DB).await? {
        Some(id) => id,
        None => tables.create_database(DEFAULT_DB).await.unwrap(),
    };

    match tables
        .get_collection_id_by_name(DEFAULT_DB, DEFAULT_SCHEMA)
        .await?
    {
        Some(id) => id,
        None => tables.create_collection(default_db, DEFAULT_SCHEMA).await?,
    };

    let all_database_ids = tables.load_database_ids().await?;

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
        internal_object_store,
        database: DEFAULT_DB.to_string(),
        database_id: default_db,
        all_database_ids: Arc::from(RwLock::new(all_database_ids)),
        max_partition_size: cfg.misc.max_partition_size,
    })
}

#[cfg(test)]
mod tests {
    use crate::context::SeafowlContext;
    use sqlx::sqlite::SqliteJournalMode;

    use super::*;

    #[tokio::test]
    async fn test_config_to_context() {
        let config = schema::SeafowlConfig {
            object_store: schema::ObjectStore::InMemory(schema::InMemory {}),
            catalog: schema::Catalog::Sqlite(schema::Sqlite {
                dsn: "sqlite::memory:".to_string(),
                journal_mode: SqliteJournalMode::Wal,
                read_only: false,
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
            runtime: schema::Runtime {
                max_memory: Some(512 * 1024 * 1024),
                ..Default::default()
            },
            misc: schema::Misc {
                max_partition_size: 1024 * 1024,
                gc_interval: 0,
                ssl_cert_file: None,
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
