use std::{fmt::Debug, iter::zip, time::Duration};

use async_trait::async_trait;
use futures::TryStreamExt;
use sqlx::{
    migrate::{MigrateDatabase, Migrator},
    postgres::PgPoolOptions,
    Error, Executor, PgPool, Postgres, QueryBuilder, Row,
};

use crate::{
    data_types::{CollectionId, DatabaseId, PhysicalRegionId, TableId, TableVersionId},
    provider::{RegionColumn, SeafowlRegion},
    repository::interface::AllTableRegionsResult,
    schema::Schema,
};

use super::interface::{AllDatabaseColumnsResult, Repository};

static MIGRATOR: Migrator = sqlx::migrate!();

#[derive(Debug)]
pub struct PostgresRepository {
    pub executor: PgPool,
    pub schema_name: String,
}

impl PostgresRepository {
    pub async fn try_new(dsn: String, schema_name: String) -> Result<Self, Error> {
        if !Postgres::database_exists(&dsn).await? {
            let _ = Postgres::create_database(&dsn).await;
        }

        let repo = PostgresRepository::connect(dsn, schema_name.clone()).await?;

        repo.executor
            .execute(format!("CREATE SCHEMA IF NOT EXISTS {};", schema_name).as_str())
            .await?;

        // Setup the schema
        repo.setup().await;
        Ok(repo)
    }

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

    async fn get_database_id_by_name(
        &self,
        database_name: &str,
    ) -> Result<DatabaseId, Error> {
        let id = sqlx::query!(
            r#"
        SELECT id FROM database WHERE database.name = $1
        "#,
            database_name,
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
    ) -> Result<(TableId, TableVersionId), Error> {
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

        Ok((new_table_id, new_version_id))
    }

    async fn create_regions(
        &self,
        regions: Vec<SeafowlRegion>,
    ) -> Result<Vec<PhysicalRegionId>, Error> {
        // Create regions

        let mut builder: QueryBuilder<Postgres> = QueryBuilder::new(
            "INSERT INTO physical_region(row_count, object_storage_id) ",
        );
        builder.push_values(&regions, |mut b, r| {
            b.push_bind(r.row_count)
                .push_bind(r.object_storage_id.as_ref());
        });
        builder.push("RETURNING id");

        let query = builder.build();
        let region_ids: Vec<PhysicalRegionId> = query
            .fetch_all(&self.executor)
            .await?
            .iter()
            .map(|r| r.get("id"))
            .collect();

        // Create region columns

        // Make an vector of (region_id, column)
        let columns: Vec<(PhysicalRegionId, &RegionColumn)> = zip(&region_ids, &regions)
            .flat_map(|(region_id, region)| {
                region.columns.iter().map(|c| (region_id.to_owned(), c))
            })
            .collect();

        let mut builder: QueryBuilder<Postgres> =
        QueryBuilder::new("INSERT INTO physical_region_column(physical_region_id, name, type, min_value, max_value) ");
        builder.push_values(columns, |mut b, (rid, c)| {
            b.push_bind(rid)
                .push_bind(c.name.as_ref())
                .push_bind(c.r#type.as_ref())
                .push_bind(c.min_value.as_ref())
                .push_bind(c.max_value.as_ref());
        });

        let query = builder.build();
        query.execute(&self.executor).await?;

        Ok(region_ids)
    }

    async fn append_regions_to_table(
        &self,
        region_ids: Vec<PhysicalRegionId>,
        table_version_id: TableVersionId,
    ) -> Result<(), Error> {
        let mut builder: QueryBuilder<Postgres> = QueryBuilder::new(
            "INSERT INTO table_region(table_version_id, physical_region_id) ",
        );
        builder.push_values(region_ids, |mut b, rid| {
            b.push_bind(table_version_id).push_bind(rid);
        });

        let query = builder.build();
        query.execute(&self.executor).await?;

        Ok(())
    }

    async fn create_new_table_version(
        &self,
        from_version: TableVersionId,
    ) -> Result<TableVersionId, Error> {
        let new_version = sqlx::query!(
            "INSERT INTO table_version (table_id)
            SELECT table_id FROM table_version WHERE id = $1
            RETURNING (id)",
            from_version
        )
        .fetch_one(&self.executor)
        .await?
        .id;

        sqlx::query!(
            "INSERT INTO table_column (table_version_id, name, type)
            SELECT $2, name, type FROM table_column WHERE table_version_id = $1;",
            from_version,
            new_version
        )
        .execute(&self.executor)
        .await?;

        sqlx::query!(
            "INSERT INTO table_region (table_version_id, physical_region_id)
            SELECT $2, physical_region_id FROM table_region WHERE table_version_id = $1;",
            from_version,
            new_version
        )
        .execute(&self.executor)
        .await?;

        Ok(new_version)
    }

    // Drop table/collection/database
    // Currently we actually delete these, though we could mark them as deleted
    // to allow for undeletion

    // In these methods, return the ID back so that we get an error if the
    // table/collection/schema didn't actually exist
    async fn drop_table(&self, table_id: TableId) -> Result<(), Error> {
        sqlx::query!("DELETE FROM \"table\" WHERE id = $1 RETURNING id", table_id)
            .fetch_one(&self.executor)
            .await?;
        Ok(())
    }

    async fn drop_collection(&self, collection_id: CollectionId) -> Result<(), Error> {
        sqlx::query!(
            "DELETE FROM collection WHERE id = $1 RETURNING id",
            collection_id
        )
        .fetch_one(&self.executor)
        .await?;
        Ok(())
    }

    async fn drop_database(&self, database_id: DatabaseId) -> Result<(), Error> {
        sqlx::query!(
            "DELETE FROM database WHERE id = $1 RETURNING id",
            database_id
        )
        .fetch_one(&self.executor)
        .await?;
        Ok(())
    }

    // Replace / delete a region (in a copy?)
}

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
