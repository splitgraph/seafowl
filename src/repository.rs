use async_trait::async_trait;
use futures::TryStreamExt;
use sqlx::{Error, PgPool, Postgres, QueryBuilder, Row};

use crate::{
    data_types::{CollectionId, DatabaseId, Table, TableId},
    schema::Schema,
};

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
        &self,
        database_id: DatabaseId,
    ) -> Result<Vec<String>, Error>;
    async fn get_all_columns_in_database(
        &self,
        database_id: DatabaseId,
    ) -> Result<Vec<AllDatabaseColumnsResult>, Error>;

    async fn get_collection_id_by_name(
        &self,
        database_name: &str,
        collection_name: &str,
    ) -> Result<CollectionId, Error>;

    async fn create_database(&self, database_name: &str) -> Result<DatabaseId, Error>;

    async fn create_collection(
        &self,
        database_id: DatabaseId,
        collection_name: &str,
    ) -> Result<CollectionId, Error>;

    async fn create_table(
        &self,
        collection_id: CollectionId,
        table_name: &str,
        schema: Schema,
    ) -> Result<TableId, Error>;
}

pub struct PostgresRepository {
    executor: PgPool,
}

#[async_trait]
impl Repository for PostgresRepository {
    async fn get_collections_in_database(
        &self,
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
        &self,
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

    async fn create_database(&self, database_name: &str) -> Result<DatabaseId, Error> {
        let id = sqlx::query!(
            r#"
        INSERT INTO database (name) VALUES ($1) RETURNING (id)
        "#,
            database_name
        )
        .fetch_one(&self.executor)
        .await?
        .id;

        Ok(id)
    }

    async fn get_collection_id_by_name(
        &self,
        database_name: &str,
        collection_name: &str,
    ) -> Result<CollectionId, Error> {
        let id = sqlx::query!(
            r#"
        SELECT collection.id
        FROM collection JOIN database ON collection.database_id = database.id
        WHERE database.name = $1 AND collection.name = $2
        "#,
            database_name,
            collection_name
        )
        .fetch_one(&self.executor)
        .await?
        .id;

        Ok(id)
    }

    async fn create_collection(
        &self,
        database_id: DatabaseId,
        collection_name: &str,
    ) -> Result<CollectionId, Error> {
        let id = sqlx::query!(
            r#"
        INSERT INTO "collection" (database_id, name) VALUES ($1, $2) RETURNING (id)
        "#,
            database_id,
            collection_name
        )
        .fetch_one(&self.executor)
        .await?
        .id;

        Ok(id)
    }

    async fn create_table(
        &self,
        collection_id: CollectionId,
        table_name: &str,
        schema: Schema,
    ) -> Result<TableId, Error> {
        // Create new (empty) table
        let new_table_id: i64 = sqlx::query!(
            r#"
        INSERT INTO "table" (collection_id, name) VALUES ($1, $2) RETURNING (id)
        "#,
            collection_id,
            table_name
        )
        .fetch_one(&self.executor)
        .await?
        .id;

        // Create initial table version
        let new_version_id: i64 = sqlx::query!(
            r#"
        INSERT INTO table_version (table_id) VALUES ($1) RETURNING (id)
        "#,
            new_table_id
        )
        .fetch_one(&self.executor)
        .await?
        .id;

        // Create columns
        // TODO this breaks if we have more than (bind limit) columns
        let mut builder: QueryBuilder<Postgres> =
            QueryBuilder::new("INSERT INTO table_column(table_version_id, name, type) ");
        builder.push_values(schema.to_column_names_types(), |mut b, col| {
            b.push_bind(new_version_id)
                .push_bind(col.0)
                .push_bind(col.1);
        });

        let query = builder.build();
        query.execute(&self.executor).await?;

        Ok(new_table_id)
    }

    // Create a table with regions
    // Append a region to a table
    // Replace / delete a region (in a copy?)
}
