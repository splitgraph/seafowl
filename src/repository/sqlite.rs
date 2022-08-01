use std::{fmt::Debug, iter::zip, str::FromStr};

use async_trait::async_trait;
use futures::TryStreamExt;
use sqlx::{
    migrate::Migrator,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    Error, Pool, QueryBuilder, Row, Sqlite,
};

use crate::{
    data_types::{
        CollectionId, DatabaseId, FunctionId, PhysicalRegionId, TableId, TableVersionId,
    },
    provider::{RegionColumn, SeafowlRegion},
    repository::interface::AllTableRegionsResult,
    schema::Schema,
    wasm_udf::data_types::CreateFunctionDetails,
};

use crate::implement_repository;

use super::{
    default::RepositoryQueries,
    interface::{AllDatabaseColumnsResult, AllDatabaseFunctionsResult, Repository},
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
        all_columns_in_database: r#"
        WITH latest_table_version AS (
            SELECT MAX(id), table_id, id
            FROM table_version
            GROUP BY table_id
        )
        SELECT
            collection.name AS collection_name,
            "table".name AS table_name,
            "table".id AS table_id,
            latest_table_version.id AS table_version_id,
            table_column.name AS column_name,
            table_column.type AS column_type
        FROM collection
        INNER JOIN "table" ON collection.id = "table".collection_id
        INNER JOIN latest_table_version ON "table".id = latest_table_version.table_id
        INNER JOIN table_column ON table_column.table_version_id = latest_table_version.id
        WHERE collection.database_id = $1
        ORDER BY collection_name, table_name
        "#,
    };

    pub async fn try_new(dsn: String) -> Result<Self, Error> {
        let options = SqliteConnectOptions::from_str(&dsn)?.create_if_missing(true);

        let pool = SqlitePoolOptions::new().connect_with(options).await?;
        let repo = Self { executor: pool };
        repo.setup().await;
        Ok(repo)
    }
}

implement_repository!(SqliteRepository);

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::super::interface::tests::run_generic_repository_tests;
    use super::SqliteRepository;

    #[tokio::test]
    async fn test_sqlite_repository() {
        let repository = Arc::new(
            SqliteRepository::try_new("sqlite::memory:".to_string())
                .await
                .unwrap(),
        );

        run_generic_repository_tests(repository).await;
    }
}
