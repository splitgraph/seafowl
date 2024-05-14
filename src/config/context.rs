use std::net::SocketAddr;
use std::sync::Arc;

use crate::{
    catalog::{DEFAULT_DB, DEFAULT_SCHEMA},
    context::SeafowlContext,
    memory_pool::MemoryPoolMetrics,
    object_store::factory::ObjectStoreFactory,
    repository::{interface::Repository, sqlite::SqliteRepository},
};
use datafusion::execution::{
    context::SessionState,
    memory_pool::{GreedyMemoryPool, MemoryPool, UnboundedMemoryPool},
};
use datafusion::{
    common::Result,
    execution::runtime_env::{RuntimeConfig, RuntimeEnv},
    prelude::{SessionConfig, SessionContext},
};
use deltalake::delta_datafusion::DeltaTableFactory;
use deltalake::storage::factories;
use metrics::describe_counter;
use metrics_exporter_prometheus::PrometheusBuilder;

#[cfg(feature = "catalog-postgres")]
use crate::repository::postgres::PostgresRepository;

use crate::catalog::{external::ExternalStore, metastore::Metastore, CatalogError};

use crate::object_store::http::add_http_object_store;

#[cfg(feature = "remote-tables")]
use datafusion_remote_tables::factory::RemoteTableFactory;

use super::schema::{self, MEBIBYTES, MEMORY_FRACTION};

pub const HTTP_REQUESTS: &str = "http_requests";
pub const GRPC_REQUESTS: &str = "grpc_requests";

async fn build_metastore(
    config: &schema::SeafowlConfig,
    object_stores: Arc<ObjectStoreFactory>,
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

            return Metastore::new_from_external(external, object_stores);
        }
    };

    Metastore::new_from_repository(repository, object_stores)
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

pub fn setup_metrics(metrics: &schema::Metrics) {
    let addr: SocketAddr = format!("{}:{}", metrics.host, metrics.port)
        .parse()
        .expect("Error parsing the Prometheus metrics export address");
    let builder = PrometheusBuilder::new().with_http_listener(addr);
    builder
        .install()
        .expect("failed to install recorder/exporter");

    describe_counter!(HTTP_REQUESTS, "Counter tracking HTTP request statistics");
    describe_counter!(GRPC_REQUESTS, "Counter tracking gRPC request statistics");
}

pub async fn build_context(cfg: schema::SeafowlConfig) -> Result<SeafowlContext> {
    let mut runtime_config = RuntimeConfig::new();

    let memory_pool: Arc<dyn MemoryPool> =
        if let Some(max_memory) = cfg.runtime.max_memory {
            Arc::new(GreedyMemoryPool::new(
                ((max_memory * MEBIBYTES) as f64 * MEMORY_FRACTION) as usize,
            ))
        } else {
            Arc::new(UnboundedMemoryPool::default())
        };

    runtime_config =
        runtime_config.with_memory_pool(Arc::new(MemoryPoolMetrics::new(memory_pool)));

    if let Some(temp_dir) = &cfg.runtime.temp_dir {
        runtime_config = runtime_config.with_temp_file_path(temp_dir);
    }

    let session_config = SessionConfig::from_env()?
        .with_information_schema(true)
        .with_default_catalog_and_schema(DEFAULT_DB, DEFAULT_SCHEMA);

    let runtime_env = RuntimeEnv::new(runtime_config)?;
    let state = build_state_with_table_factories(session_config, Arc::new(runtime_env));
    let context = SessionContext::new_with_state(state);

    let object_stores = Arc::new(ObjectStoreFactory::new_from_config(&cfg)?);

    // Register the HTTP object store for external tables
    add_http_object_store(&context, &cfg.misc.ssl_cert_file);

    let metastore = build_metastore(&cfg, object_stores.clone()).await;

    // Register the object store factory for all relevant schemes/urls
    object_stores.register(factories());

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
        config: cfg,
        inner: context,
        metastore: Arc::new(metastore),
        internal_object_store: object_stores.get_internal_store(),
        default_catalog: DEFAULT_DB.to_string(),
        default_schema: DEFAULT_SCHEMA.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use sqlx::sqlite::SqliteJournalMode;

    use super::*;

    #[tokio::test]
    async fn test_config_to_context() {
        let config = schema::SeafowlConfig {
            object_store: Some(schema::ObjectStore::InMemory(schema::InMemory {})),
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
                metrics: None,
                object_store_cache: None,
                put_data: Default::default(),
            },
        };

        let context = build_context(config).await.unwrap();

        // Run a query against the context to test it works
        let results = context
            .collect(context.plan_query("SHOW TABLES").await.unwrap())
            .await
            .unwrap();
        assert!(!results.is_empty());
    }
}
