use std::sync::Arc;

use crate::{
    catalog::{DEFAULT_DB, DEFAULT_SCHEMA},
    context::SeafowlContext,
    repository::{interface::Repository, sqlite::SqliteRepository},
};
use datafusion::execution::context::SessionState;
use datafusion::{
    common::Result,
    execution::runtime_env::{RuntimeConfig, RuntimeEnv},
    prelude::{SessionConfig, SessionContext},
};
use deltalake::delta_datafusion::DeltaTableFactory;
use object_store::{local::LocalFileSystem, memory::InMemory, ObjectStore};

#[cfg(feature = "catalog-postgres")]
use crate::repository::postgres::PostgresRepository;

use crate::catalog::{external::ExternalStore, metastore::Metastore, CatalogError};
use crate::object_store::http::add_http_object_store;
use crate::object_store::wrapped::InternalObjectStore;
#[cfg(feature = "remote-tables")]
use datafusion_remote_tables::factory::RemoteTableFactory;
#[cfg(feature = "object-store-s3")]
use object_store::aws::AmazonS3Builder;
use object_store::gcp::GoogleCloudStorageBuilder;

use super::schema::{self, GCS, MEBIBYTES, MEMORY_FRACTION, S3};

async fn build_metastore(
    config: &schema::SeafowlConfig,
    object_store: Arc<InternalObjectStore>,
) -> Metastore {
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
        schema::Catalog::Clade(schema::Clade { dsn }) => {
            let external = Arc::new(
                ExternalStore::new(dsn.clone())
                    .await
                    .expect("Error setting up remote store"),
            );
            return Metastore::new_from_external(external, object_store);
        }
    };

    Metastore::new_from_repository(repository, object_store)
}

pub fn build_object_store(
    object_store_cfg: &schema::ObjectStore,
) -> Result<Arc<dyn ObjectStore>> {
    Ok(match &object_store_cfg {
        schema::ObjectStore::Local(schema::Local { data_dir }) => {
            Arc::new(LocalFileSystem::new_with_prefix(data_dir)?)
        }
        schema::ObjectStore::InMemory(_) => Arc::new(InMemory::new()),
        #[cfg(feature = "object-store-s3")]
        schema::ObjectStore::S3(S3 {
            region,
            access_key_id,
            secret_access_key,
            endpoint,
            bucket,
            cache_properties,
            ..
        }) => {
            let mut builder = AmazonS3Builder::new()
                .with_access_key_id(access_key_id)
                .with_secret_access_key(secret_access_key)
                .with_region(region.clone().unwrap_or_default())
                .with_bucket_name(bucket)
                .with_allow_http(true);

            if let Some(endpoint) = endpoint {
                builder = builder.with_endpoint(endpoint);
            }

            let store = builder.build()?;

            if let Some(props) = cache_properties {
                props.wrap_store(Arc::new(store))
            } else {
                Arc::new(store)
            }
        }
        #[cfg(feature = "object-store-gcs")]
        schema::ObjectStore::GCS(GCS {
            bucket,
            google_application_credentials,
            cache_properties,
            ..
        }) => {
            let gcs_builder: GoogleCloudStorageBuilder =
                GoogleCloudStorageBuilder::new().with_bucket_name(bucket);

            let gcs_builder = if let Some(path) = google_application_credentials {
                gcs_builder.with_service_account_path(path)
            } else {
                gcs_builder
            };

            let store = gcs_builder.build()?;

            if let Some(props) = cache_properties {
                props.wrap_store(Arc::new(store))
            } else {
                Arc::new(store)
            }
        }
    })
}

// Construct the session state and register additional table factories besides the default ones for
// PARQUET, CSV, etc.
pub fn build_state_with_table_factories(
    config: SessionConfig,
    runtime: Arc<RuntimeEnv>,
) -> SessionState {
    let mut state = SessionState::new_with_config_rt(config, runtime);

    state
        .table_factories_mut()
        .insert("DELTATABLE".to_string(), Arc::new(DeltaTableFactory {}));
    #[cfg(feature = "remote-tables")]
    {
        state
            .table_factories_mut()
            .insert("TABLE".to_string(), Arc::new(RemoteTableFactory {}));
    }
    state
}

pub async fn build_context(cfg: &schema::SeafowlConfig) -> Result<SeafowlContext> {
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

    let runtime_env = RuntimeEnv::new(runtime_config)?;
    let state = build_state_with_table_factories(session_config, Arc::new(runtime_env));
    let context = SessionContext::new_with_state(state);

    let object_store = build_object_store(&cfg.object_store)?;
    let internal_object_store = Arc::new(InternalObjectStore::new(
        object_store.clone(),
        cfg.object_store.clone(),
    ));

    // Register the HTTP object store for external tables
    add_http_object_store(&context, &cfg.misc.ssl_cert_file);

    let metastore = build_metastore(cfg, internal_object_store.clone()).await;

    // Create default DB/collection
    if let Err(CatalogError::CatalogDoesNotExist { .. }) =
        metastore.catalogs.get(DEFAULT_DB).await
    {
        metastore.catalogs.create(DEFAULT_DB).await.unwrap();
    }

    if let Err(CatalogError::SchemaDoesNotExist { .. }) =
        metastore.schemas.get(DEFAULT_DB, DEFAULT_SCHEMA).await
    {
        metastore.schemas.create(DEFAULT_DB, DEFAULT_SCHEMA).await?;
    }

    // Convergence doesn't support connecting to different DB names. We are supposed
    // to do one context per query (as we need to load the schema before executing every
    // query) and per database (since the context is supposed to be limited to the database
    // the user is connected to), but in this case we can just use the same context everywhere
    // (it will reload its schema before running the query)

    Ok(SeafowlContext {
        inner: context,
        metastore: Arc::new(metastore),
        internal_object_store,
        default_catalog: DEFAULT_DB.to_string(),
        default_schema: DEFAULT_SCHEMA.to_string(),
        max_partition_size: cfg.misc.max_partition_size,
    })
}

#[cfg(test)]
mod tests {
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
                #[cfg(feature = "frontend-arrow-flight")]
                flight: None,
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
                    cache_control: "max-age=43200, public".to_string(),
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
