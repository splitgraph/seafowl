use std::{fmt::Debug, iter::zip, str::FromStr};

use async_trait::async_trait;
use futures::TryStreamExt;
use sqlx::sqlite::SqliteJournalMode;
use sqlx::{
    migrate::Migrator,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    Pool, QueryBuilder, Row, Sqlite,
};
use uuid::Uuid;

use crate::{
    data_types::{
        CollectionId, DatabaseId, FunctionId, PhysicalPartitionId, TableId,
        TableVersionId,
    },
    provider::{PartitionColumn, SeafowlPartition},
    repository::interface::AllTablePartitionColumnsResult,
    schema::Schema,
    wasm_udf::data_types::CreateFunctionDetails,
};

use crate::implement_repository;

use super::{
    default::RepositoryQueries,
    interface::{
        AllDatabaseColumnsResult, AllDatabaseFunctionsResult, Error, Repository, Result,
        TablePartitionsResult, TableVersionsResult,
    },
};

#[derive(Debug)]
pub struct SqliteRepository {
    pub executor: Pool<Sqlite>,
}

impl SqliteRepository {
    pub const MIGRATOR: Migrator = sqlx::migrate!("migrations/sqlite");
    pub const QUERIES: RepositoryQueries = RepositoryQueries {
        // SQLite has an automagic argmax/argmin built in without
        // having to write out a window function
        //   "If there is exactly one min() or max() aggregate in the query,
        //    then all bare columns in the result set take values from an input row
        //    which also contains the minimum or maximum."
        //
        // https://www.sqlite.org/lang_select.html#bareagg
        // TODO max(id) or max(creation_time)? the id should be a tiebreaker for creation_time
        latest_table_versions: r#"
        WITH desired_table_versions AS (
            SELECT MAX(id), table_id, id
            FROM table_version
            GROUP BY table_id
        )"#,
        all_table_versions: r#"SELECT
                database.name AS database_name,
                collection.name AS collection_name,
                "table".name AS table_name,
                table_version.id AS table_version_id,
                "table".legacy AS table_legacy,
                CAST(table_version.creation_time AS INTEGER(8)) AS creation_time
            FROM table_version
            INNER JOIN "table" ON "table".id = table_version.table_id
            INNER JOIN collection ON collection.id = "table".collection_id
            INNER JOIN database ON database.id = collection.database_id"#,
    };

    pub async fn try_new(
        dsn: String,
        journal_mode: SqliteJournalMode,
    ) -> std::result::Result<Self, sqlx::Error> {
        let options = SqliteConnectOptions::from_str(&dsn)?
            .create_if_missing(true)
            .journal_mode(journal_mode);

        let pool = SqlitePoolOptions::new().connect_with(options).await?;
        let repo = Self { executor: pool };
        repo.setup().await;
        Ok(repo)
    }

    ///
    /// Create a new `SqliteRepository` in read-only mode: assumes the
    /// database already exists and doesn't run migrations. Intended to be used
    /// in environments such as a pre-baked Docker image / LiteFS replica.
    pub async fn try_new_read_only(
        dsn: String,
        journal_mode: SqliteJournalMode,
    ) -> std::result::Result<Self, sqlx::Error> {
        let options = SqliteConnectOptions::from_str(&dsn)?
            .read_only(true)
            .journal_mode(journal_mode);

        let pool = SqlitePoolOptions::new().connect_with(options).await?;
        let repo = Self { executor: pool };
        Ok(repo)
    }

    pub fn interpret_error(error: sqlx::Error) -> Error {
        if let sqlx::Error::Database(ref d) = error {
            // Reference: https://www.sqlite.org/rescode.html
            let message = d.message();

            // For some reason, sqlx doesn't return the proper errcode for FK violations,
            // even though it's calling sqlite3_extended_errcode which is meant to return full codes.
            // Unique constraint violations do return the correct code though.
            if message.contains("FOREIGN KEY constraint failed") {
                return Error::FKConstraintViolation(error);
            }
            if message.contains("UNIQUE constraint failed") {
                return Error::UniqueConstraintViolation(error);
            }
        }
        Error::SqlxError(error)
    }
}

implement_repository!(SqliteRepository);

#[cfg(test)]
mod tests {
    use crate::repository::interface::Repository;
    use sqlx::sqlite::SqliteJournalMode;
    use std::sync::Arc;
    use tempfile::NamedTempFile;

    use super::super::interface::tests::run_generic_repository_tests;
    use super::SqliteRepository;

    #[tokio::test]
    async fn test_sqlite_repository() {
        let repository = Arc::new(
            SqliteRepository::try_new(
                "sqlite::memory:".to_string(),
                SqliteJournalMode::Wal,
            )
            .await
            .unwrap(),
        );

        run_generic_repository_tests(repository).await;
    }

    #[tokio::test]
    async fn test_sqlite_repository_read_only() {
        // Make a temporary SQLite file in the RW mode, then try
        // reading from it in RO mode

        let temp_file = NamedTempFile::new().unwrap();

        let rw_repository = SqliteRepository::try_new(
            temp_file.path().to_string_lossy().to_string(),
            SqliteJournalMode::Wal,
        )
        .await
        .unwrap();

        let db_id = rw_repository.create_database("testdb").await.unwrap();

        let ro_repository = SqliteRepository::try_new_read_only(
            temp_file.path().to_string_lossy().to_string(),
            SqliteJournalMode::Wal,
        )
        .await
        .unwrap();

        assert_eq!(
            ro_repository
                .get_database_id_by_name("testdb")
                .await
                .unwrap(),
            db_id
        );
    }
}
