use std::{path::PathBuf, sync::Arc};

use clap::Parser;

use datafusion::prelude::{SessionConfig, SessionContext};
use object_store::{local::LocalFileSystem, memory::InMemory, ObjectStore};
use seafowl::{
    catalog::{PostgresCatalog, RegionCatalog, TableCatalog},
    config,
    config::{load_config, SeafowlConfig},
    context::SeafowlContext,
    frontend::postgres::run_pg_server,
    repository::PostgresRepository,
};

extern crate pretty_env_logger;
#[macro_use]
extern crate log;

async fn build_catalog(
    config: &SeafowlConfig,
) -> (Arc<dyn TableCatalog>, Arc<dyn RegionCatalog>) {
    match &config.catalog {
        config::Catalog::Postgres(config::Postgres { dsn }) => {
            // Initialize the repository
            let repository =
                PostgresRepository::try_new(dsn.to_string(), "public".to_string())
                    .await
                    .expect("Error setting up the database");

            let catalog = Arc::new(PostgresCatalog {
                repository: Arc::new(repository),
            });

            (catalog.clone(), catalog)
        }
    }
}

fn build_object_store(cfg: &SeafowlConfig) -> Arc<dyn ObjectStore> {
    match &cfg.object_store {
        config::ObjectStore::Local(config::Local { data_dir }) => Arc::new(
            LocalFileSystem::new_with_prefix(data_dir)
                .expect("Error creating object store"),
        ),
        config::ObjectStore::InMemory(_) => Arc::new(InMemory::new()),
        config::ObjectStore::S3(_) => todo!(),
    }
}

async fn build_context(cfg: &SeafowlConfig) -> SeafowlContext {
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

    // Convergence doesn't support connecting to different DB names. We are supposed
    // to do one context per query (as we need to load the schema before executing every
    // query) and per database (since the context is supposed to be limited to the database
    // the user is connected to), but in this case we can just use the same context everywhere, but reload
    // it before we run the query.
    let context = SeafowlContext {
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

#[derive(Debug, Parser)]
struct Args {
    #[clap(short, long)]
    config_path: PathBuf,
}

#[tokio::main]
async fn main() {
    pretty_env_logger::init_timed();

    info!("Starting Seafowl");
    let args = Args::parse();
    let config = load_config(&args.config_path).expect("Error loading config");
    info!("Config: {:?}", config);

    let context = Arc::new(build_context(&config).await);

    let server = run_pg_server(context);
    info!("Starting the PG server on 127.0.0.1:8432");
    server.await;
}

#[cfg(test)]
mod tests {
    use std::env;

    use super::*;

    #[tokio::test]
    async fn test_config_to_context() {
        let dsn = env::var("DATABASE_URL").unwrap();

        let config = SeafowlConfig {
            object_store: config::ObjectStore::InMemory(config::InMemory {}),
            catalog: config::Catalog::Postgres(config::Postgres { dsn }),
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
