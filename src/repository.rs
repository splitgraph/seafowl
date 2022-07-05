use async_trait::async_trait;
use futures::TryStreamExt;
use sqlx::{Error, PgPool, Row};

use crate::data_types::DatabaseId;

#[derive(sqlx::FromRow)]
pub struct AllDatabaseColumnsResult {
    pub collection_name: String,
    pub table_name: String,
    pub column_name: String,
    pub column_type: String,
    pub table_region_id: i64,
    pub object_storage_id: String,
    pub row_count: i32,
    pub min_value: Option<Vec<u8>>,
    pub max_value: Option<Vec<u8>>,
}

#[async_trait]
pub trait Repository: Send + Sync {
    async fn get_collections_in_database(
        &mut self,
        database_id: DatabaseId,
    ) -> Result<Vec<String>, Error>;
    async fn get_all_columns_in_database(
        &mut self,
        database_id: DatabaseId,
    ) -> Result<Vec<AllDatabaseColumnsResult>, Error>;
}

pub struct PostgresRepository {
    executor: PgPool,
}

#[async_trait]
impl Repository for PostgresRepository {
    async fn get_collections_in_database(
        &mut self,
        database_id: DatabaseId,
    ) -> Result<Vec<String>, Error> {
        let names = sqlx::query("SELECT name FROM collection WHERE database_id = $1")
            .bind(database_id)
            .fetch(&self.executor)
            .map_ok(|row| row.get("name"))
            .try_collect()
            .await?;
        Ok(names)
    }
    async fn get_all_columns_in_database(
        &mut self,
        database_id: DatabaseId,
    ) -> Result<Vec<AllDatabaseColumnsResult>, Error> {
        let columns = sqlx::query_as!(
            AllDatabaseColumnsResult,
            r#"
        WITH latest_table_version AS (
            SELECT DISTINCT ON (table_id) table_id, id
            FROM table_version
            ORDER BY table_id, creation_time DESC, id DESC
        )
        SELECT
            collection.name AS collection_name,
            "table".name AS table_name,
            table_column.name AS column_name,
            table_column.type AS column_type,
            physical_region.id AS table_region_id,
            physical_region.object_storage_id,
            physical_region.row_count,
            physical_region_column.min_value,
            physical_region_column.max_value
        FROM collection
        INNER JOIN "table" ON collection.id = "table".collection_id
        INNER JOIN latest_table_version ON "table".id = latest_table_version.table_id
        INNER JOIN table_column ON table_column.table_version_id = latest_table_version.id
        INNER JOIN table_region ON table_region.table_version_id = latest_table_version.id
        INNER JOIN physical_region ON physical_region.id = table_region.physical_region_id
        LEFT JOIN physical_region_column
            ON physical_region_column.physical_region_id = physical_region.id
            AND physical_region_column.name = table_column.name
        WHERE collection.database_id = $1
        ORDER BY collection_name, table_name, table_region_id
        "#,
            database_id
        )
        .fetch_all(&self.executor)
        .await?;
        Ok(columns)
    }
}
