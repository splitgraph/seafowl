use std::{fmt::Debug, iter::zip, time::Duration};

use async_trait::async_trait;
use futures::TryStreamExt;
use sqlx::{
    migrate::Migrator, postgres::PgPoolOptions, Error, Executor, PgPool, Postgres, QueryBuilder,
    Row,
};

use crate::{
    data_types::{CollectionId, DatabaseId, PhysicalRegionId, TableId, TableVersionId},
    provider::{RegionColumn, SeafowlRegion},
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
pub trait Repository: Send + Sync + Debug {
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
    ) -> Result<(TableId, TableVersionId), Error>;

    async fn create_regions(
        &self,
        region: Vec<SeafowlRegion>,
    ) -> Result<Vec<PhysicalRegionId>, Error>;

    async fn append_regions_to_table(
        &self,
        region_ids: Vec<PhysicalRegionId>,
        table_version_id: TableVersionId,
    ) -> Result<(), Error>;

    async fn create_new_table_version(
        &self,
        from_version: TableVersionId,
    ) -> Result<TableVersionId, Error>;
}

#[derive(Debug)]
pub struct PostgresRepository {
    pub executor: PgPool,
    pub schema_name: String,
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

        let mut builder: QueryBuilder<Postgres> =
            QueryBuilder::new("INSERT INTO physical_region(row_count, object_storage_id) ");
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
        let mut builder: QueryBuilder<Postgres> =
            QueryBuilder::new("INSERT INTO table_region(table_version_id, physical_region_id) ");
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

    // Replace / delete a region (in a copy?)
}

pub mod testutils {
    use crate::repository::Repository;
    use rand::Rng;
    use sqlx::Executor;
    use sqlx::{migrate::MigrateDatabase, Postgres};

    use crate::repository::PostgresRepository;

    async fn create_db(dsn: &str) {
        if !Postgres::database_exists(dsn).await.unwrap() {
            let _ = Postgres::create_database(dsn).await;
        }
    }

    pub async fn make_repository(dsn: &str) -> PostgresRepository {
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
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{
        DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    };

    use crate::repository::testutils::make_repository;

    use super::*;

    // TODO use envvars or something
    const DEV_DB_DSN: &str = "postgresql://sgr:password@localhost:7432/seafowl";

    async fn make_database_with_single_table(
        repository: &PostgresRepository,
    ) -> (DatabaseId, CollectionId, TableId, TableVersionId) {
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

        let (table_id, table_version_id) = repository
            .create_table(collection_id, "testtable", schema)
            .await
            .expect("Error creating table");

        (database_id, collection_id, table_id, table_version_id)
    }

    #[tokio::test]
    async fn test_make_repository() {
        let repository = make_repository(DEV_DB_DSN).await;
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
        let repository = make_repository(DEV_DB_DSN).await;

        let (database_id, _, _, _) = make_database_with_single_table(&repository).await;

        // Test loading all columns

        let all_columns = repository
            .get_all_columns_in_database(database_id)
            .await
            .expect("Error getting all columns");
        assert_eq!(
            all_columns,
            [
                AllDatabaseColumnsResult {
                    collection_name: "testcol".to_string(),
                    table_name: "testtable".to_string(),
                    table_version_id: 1,
                    column_name: "date".to_string(),
                    column_type: "{\"name\":\"date\",\"unit\":\"MILLISECOND\"}".to_string()
                },
                AllDatabaseColumnsResult {
                    collection_name: "testcol".to_string(),
                    table_name: "testtable".to_string(),
                    table_version_id: 1,
                    column_name: "value".to_string(),
                    column_type: "{\"name\":\"floatingpoint\",\"precision\":\"DOUBLE\"}"
                        .to_string()
                }
            ]
        );
    }

    #[tokio::test]
    async fn test_create_append_region() {
        let repository = make_repository(DEV_DB_DSN).await;

        let (_, _, _, table_version_id) = make_database_with_single_table(&repository).await;

        let region = SeafowlRegion {
            object_storage_id: Arc::from(
                "d52a8584a60b598ad0ffa11d185c3ca800b7ddb47ea448d0072b6bf7a5a209e1.parquet"
                    .to_string(),
            ),
            row_count: 2,
            columns: Arc::new(vec![
                RegionColumn {
                    name: Arc::from("timestamp".to_string()),
                    r#type: Arc::from("{\"name\":\"utf8\"}".to_string()),
                    min_value: Arc::new(None),
                    max_value: Arc::new(None),
                },
                RegionColumn {
                    name: Arc::from("integer".to_string()),
                    r#type: Arc::from(
                        "{\"name\":\"int\",\"bitWidth\":64,\"isSigned\":true}".to_string(),
                    ),
                    min_value: Arc::new(Some([49, 50].to_vec())),
                    max_value: Arc::new(Some([52, 50].to_vec())),
                },
                RegionColumn {
                    name: Arc::from("varchar".to_string()),
                    r#type: Arc::from("{\"name\":\"utf8\"}".to_string()),
                    min_value: Arc::new(None),
                    max_value: Arc::new(None),
                },
            ]),
        };

        // Create a region; since we're in a separated schema, it gets ID=1
        let region_ids = repository.create_regions(vec![region]).await.unwrap();
        assert_eq!(region_ids, vec![1]);

        // Test loading all table regions when the region is not yet attached
        let all_regions = repository
            .get_all_regions_in_table(table_version_id)
            .await
            .unwrap();
        assert_eq!(all_regions, Vec::<AllTableRegionsResult>::new());

        // Attach the region to the table
        repository
            .append_regions_to_table(region_ids, table_version_id)
            .await
            .unwrap();

        // Load again
        let all_regions = repository
            .get_all_regions_in_table(table_version_id)
            .await
            .unwrap();
        assert_eq!(
            all_regions,
            vec![
                AllTableRegionsResult {
                    table_region_id: 1,
                    object_storage_id:
                        "d52a8584a60b598ad0ffa11d185c3ca800b7ddb47ea448d0072b6bf7a5a209e1.parquet"
                            .to_string(),
                    column_name: "timestamp".to_string(),
                    column_type: "{\"name\":\"utf8\"}".to_string(),
                    row_count: 2,
                    min_value: None,
                    max_value: None
                },
                AllTableRegionsResult {
                    table_region_id: 1,
                    object_storage_id:
                        "d52a8584a60b598ad0ffa11d185c3ca800b7ddb47ea448d0072b6bf7a5a209e1.parquet"
                            .to_string(),
                    column_name: "integer".to_string(),
                    column_type: "{\"name\":\"int\",\"bitWidth\":64,\"isSigned\":true}".to_string(),
                    row_count: 2,
                    min_value: Some([49, 50].to_vec()),
                    max_value: Some([52, 50].to_vec())
                },
                AllTableRegionsResult {
                    table_region_id: 1,
                    object_storage_id:
                        "d52a8584a60b598ad0ffa11d185c3ca800b7ddb47ea448d0072b6bf7a5a209e1.parquet"
                            .to_string(),
                    column_name: "varchar".to_string(),
                    column_type: "{\"name\":\"utf8\"}".to_string(),
                    row_count: 2,
                    min_value: None,
                    max_value: None
                }
            ]
        );
    }
}
