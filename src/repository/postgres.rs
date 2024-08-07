use std::{fmt::Debug, time::Duration};

use arrow_schema::Schema;
use async_trait::async_trait;
use futures::TryStreamExt;
use sqlx::{
    migrate::{MigrateDatabase, Migrator},
    postgres::PgPoolOptions,
    Executor, PgPool, Postgres, QueryBuilder, Row,
};
use uuid::Uuid;

use crate::{implement_repository, wasm_udf::data_types::CreateFunctionDetails};

use super::{
    default::RepositoryQueries,
    interface::{
        AllDatabaseColumnsResult, AllDatabaseFunctionsResult, CollectionId,
        CollectionRecord, DatabaseId, DatabaseRecord, DroppedTableDeletionStatus,
        DroppedTablesResult, Error, FunctionId, Repository, Result, TableId, TableRecord,
        TableVersionId, TableVersionsResult,
    },
};

#[derive(Debug)]
pub struct PostgresRepository {
    pub executor: PgPool,
    pub schema_name: String,
}

impl PostgresRepository {
    pub const MIGRATOR: Migrator = sqlx::migrate!("migrations/postgres");
    pub const QUERIES: RepositoryQueries = RepositoryQueries {
        latest_table_versions: r#"
        WITH desired_table_versions AS (
            SELECT DISTINCT ON (table_id) table_id, id
            FROM table_version
            ORDER BY table_id, creation_time DESC, id DESC
        )"#,
        cast_timestamp: "CAST(EXTRACT(EPOCH FROM timestamp_column) AS INT8)",
    };

    pub async fn try_new(
        dsn: String,
        schema_name: String,
    ) -> std::result::Result<Self, sqlx::Error> {
        if !Postgres::database_exists(&dsn).await? {
            let _ = Postgres::create_database(&dsn).await;
        }

        let repo = PostgresRepository::connect(dsn, schema_name.clone()).await?;

        repo.executor
            .execute(format!("CREATE SCHEMA IF NOT EXISTS {schema_name};").as_str())
            .await?;

        // Setup the schema
        repo.setup().await;
        Ok(repo)
    }

    pub async fn connect(
        dsn: String,
        schema_name: String,
    ) -> std::result::Result<Self, sqlx::Error> {
        let schema_name_2 = schema_name.clone();

        let pool = PgPoolOptions::new()
            .min_connections(1)
            .max_connections(16)
            .idle_timeout(Duration::from_millis(30000))
            .test_before_acquire(true)
            .after_connect(move |c, _m| {
                let schema_name = schema_name.to_owned();
                Box::pin(async move {
                    let query = format!("SET search_path TO {schema_name},public;");
                    c.execute(sqlx::query(&query)).await?;
                    Ok(())
                })
            })
            .connect(&dsn)
            .await?;

        Ok(Self {
            executor: pool,
            schema_name: schema_name_2,
        })
    }

    pub fn interpret_error(error: sqlx::Error) -> Error {
        if let sqlx::Error::Database(ref d) = error {
            // Reference: https://www.postgresql.org/docs/current/errcodes-appendix.html
            if let Some(code) = d.code() {
                if code == "23505" {
                    return Error::UniqueConstraintViolation(error);
                } else if code == "23503" {
                    return Error::FKConstraintViolation(error);
                }
            }
        }
        Error::SqlxError(error)
    }
}

implement_repository!(PostgresRepository);

pub mod testutils {
    use rand::Rng;

    use super::PostgresRepository;

    pub fn get_random_schema() -> String {
        // Generate a random schema (taken from IOx)
        let mut rng = rand::thread_rng();
        (&mut rng)
            .sample_iter(rand::distributions::Alphanumeric)
            .filter(|c| c.is_ascii_alphabetic())
            .take(20)
            .map(char::from)
            .collect::<String>()
    }

    pub async fn make_repository(dsn: &str) -> PostgresRepository {
        let schema_name = get_random_schema();

        PostgresRepository::try_new(dsn.to_string(), schema_name)
            .await
            .expect("Error setting up the database")
    }
}

#[cfg(test)]
mod tests {
    use std::{env, sync::Arc};

    use super::super::interface::tests::run_generic_repository_tests;
    use super::testutils::make_repository;

    #[tokio::test]
    async fn test_postgres_repository() {
        let dsn = env::var("DATABASE_URL").unwrap();
        let repository = Arc::new(make_repository(&dsn).await);

        run_generic_repository_tests(repository).await;
    }
}
