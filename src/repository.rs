use std::time::Duration;

use async_trait::async_trait;
use futures::TryStreamExt;
use sqlx::{
    migrate::Migrator, postgres::PgPoolOptions, Error, Executor, PgPool, Postgres, QueryBuilder,
    Row,
};

use crate::{
    data_types::{CollectionId, DatabaseId, TableId, TableVersionId},
    schema::Schema,
};

static MIGRATOR: Migrator = sqlx::migrate!();

#[derive(sqlx::FromRow, Debug, PartialEq, Eq)]
pub struct AllDatabaseColumnsResult {
    pub collection_name: String,
    pub table_name: String,
    pub table_version_id: TableVersionId,
    pub column_name: String,
    pub column_type: String,
}

#[derive(sqlx::FromRow, Debug, PartialEq, Eq)]
pub struct AllTableRegionsResult {
    pub table_region_id: i64,
    pub object_storage_id: String,
    pub column_name: String,
    pub column_type: String,
    pub row_count: i32,
    pub min_value: Option<Vec<u8>>,
    pub max_value: Option<Vec<u8>>,
}

#[async_trait]
pub trait Repository: Send + Sync {
    async fn setup(&self);

    async fn get_collections_in_database(
        &self,
        database_id: DatabaseId,
    ) -> Result<Vec<String>, Error>;

    async fn get_all_columns_in_database(
        &self,
        database_id: DatabaseId,
    ) -> Result<Vec<AllDatabaseColumnsResult>, Error>;

    async fn get_all_regions_in_table(
        &self,
        table_version_id: TableVersionId,
    ) -> Result<Vec<AllTableRegionsResult>, Error>;

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
    schema_name: String,
}

impl PostgresRepository {
    pub async fn connect(dsn: String, schema_name: String) -> Result<Self, Error> {
        let schema_name_2 = schema_name.clone();

        let pool = PgPoolOptions::new()
            .min_connections(1)
            .max_connections(16)
            .idle_timeout(Duration::from_millis(30000))
            .test_before_acquire(true)
            .after_connect(move |c| {
                let schema_name = schema_name.to_owned();
                Box::pin(async move {
                    let query = format!("SET search_path TO {},public;", schema_name);
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
}

#[async_trait]
impl Repository for PostgresRepository {
    async fn setup(&self) {
        let query = format!("CREATE SCHEMA IF NOT EXISTS {};", &self.schema_name);
        self.executor
            .execute(sqlx::query(&query))
            .await
            .expect("error creating schema");

        MIGRATOR
            .run(&self.executor)
            .await
            .expect("error running migrations");
    }
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
            latest_table_version.id AS table_version_id,
            table_column.name AS column_name,
            table_column.type AS column_type
        FROM collection
        INNER JOIN "table" ON collection.id = "table".collection_id
        INNER JOIN latest_table_version ON "table".id = latest_table_version.table_id
        INNER JOIN table_column ON table_column.table_version_id = latest_table_version.id
        LEFT JOIN table_region ON table_region.table_version_id = latest_table_version.id
        WHERE collection.database_id = $1
        ORDER BY collection_name, table_name
        "#,
            database_id
        )
        .fetch_all(&self.executor)
        .await?;
        Ok(columns)
    }

    async fn get_all_regions_in_table(
        &self,
        table_version_id: TableVersionId,
    ) -> Result<Vec<AllTableRegionsResult>, Error> {
        let regions = sqlx::query_as!(
            AllTableRegionsResult,
            r#"SELECT
            physical_region.id AS table_region_id,
            physical_region.object_storage_id,
            physical_region.row_count,
            physical_region_column.name AS column_name,
            physical_region_column.type AS column_type,
            physical_region_column.min_value,
            physical_region_column.max_value
        FROM table_region
        INNER JOIN physical_region ON physical_region.id = table_region.physical_region_id
        LEFT JOIN physical_region_column ON physical_region_column.physical_region_id = physical_region.id
        WHERE table_region.table_version_id = $1
        ORDER BY table_region_id
        "#,
            table_version_id
        )
        .fetch_all(&self.executor)
        .await?;
        Ok(regions)
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{
        DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    };
    use rand::Rng;
    use sqlx::migrate::MigrateDatabase;

    use super::*;

    // TODO use envvars or something
    const DEV_DB_DSN: &str = "postgresql://sgr:password@localhost:7432/seafowl";

    async fn create_db(dsn: &str) {
        if !Postgres::database_exists(dsn).await.unwrap() {
            let _ = Postgres::create_database(dsn).await;
        }
    }

    async fn make_repository() -> PostgresRepository {
        // Generate a random schema (taken from IOx)

        let schema_name = {
            let mut rng = rand::thread_rng();
            (&mut rng)
                .sample_iter(rand::distributions::Alphanumeric)
                .filter(|c| c.is_ascii_alphabetic())
                .take(20)
                .map(char::from)
                .collect::<String>()
        };

        // let dsn = std::env::var("DATABASE_URL").unwrap();
        let dsn = DEV_DB_DSN;
        create_db(dsn).await;

        let repo = PostgresRepository::connect(dsn.to_string(), schema_name.clone())
            .await
            .expect("failed to connect to the db");

        repo.executor
            .execute(format!("CREATE SCHEMA {};", schema_name).as_str())
            .await
            .expect("failed to create test schema");

        // Setup the schema
        repo.setup().await;
        repo
    }

    #[tokio::test]
    async fn test_make_repository() {
        let repository = make_repository().await;
        assert_eq!(
            repository
                .get_collections_in_database(0)
                .await
                .expect("error getting collections"),
            Vec::<String>::new()
        );
    }

    #[tokio::test]
    async fn test_create_database_collection_table() {
        let repository = make_repository().await;

        let database_id = repository
            .create_database("testdb")
            .await
            .expect("Error creating database");
        let collection_id = repository
            .create_collection(database_id, "testcol")
            .await
            .expect("Error creating collection");

        let arrow_schema = ArrowSchema::new(vec![
            ArrowField::new("date", ArrowDataType::Date64, false),
            ArrowField::new("value", ArrowDataType::Float64, false),
        ]);
        let schema = Schema {
            arrow_schema: Arc::new(arrow_schema),
        };

        let table_id = repository
            .create_table(collection_id, "testtable", schema)
            .await
            .expect("Error creating table");
        dbg!(table_id);

        // Test loading all columns

        let all_columns = repository
            .get_all_columns_in_database(database_id)
            .await
            .expect("Error getting all columns");
        assert_eq!(all_columns, [AllDatabaseColumnsResult { collection_name: "testcol".to_string(), table_name: "testtable".to_string(), table_version_id: 1, column_name: "date".to_string(), column_type: "{\"name\":\"date\",\"nullable\":false,\"type\":{\"name\":\"date\",\"unit\":\"MILLISECOND\"},\"children\":[]}".to_string() }, AllDatabaseColumnsResult { collection_name: "testcol".to_string(), table_name: "testtable".to_string(), table_version_id: 1, column_name: "value".to_string(), column_type: "{\"name\":\"value\",\"nullable\":false,\"type\":{\"name\":\"floatingpoint\",\"precision\":\"DOUBLE\"},\"children\":[]}".to_string() }]);
    }
}
