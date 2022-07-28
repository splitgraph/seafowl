/// Default implementation for a Repository that factors out common
/// query patterns / SQL queries between Postgres and SQLite.
///
/// Usage:
///
/// The struct has to have certain fields, since this macro relies on them:
///
/// ```ignore
/// pub struct MyRepository {
///     pub executor: sqlx::Pool<sqlx::SqlxDatabaseType>
/// }
///
/// impl MyRepository {
///     pub const MIGRATOR: sqlx::Migrator = sqlx::migrate!("my/migrations");
///     pub const QUERIES: RepositoryQueries = RepositoryQueries {
///         all_columns_in_database: "SELECT ...",
///     }
/// }
///
/// implement_repository!(SqliteRepository)
/// ```
///
/// Gigajank alert: why are we doing this? The code between PG and SQLite is extremely similar.
/// But, I couldn't find a better way to factor it out in order to reduce duplication.
/// Here's what I tried:
///
///   - Use a generic `Pool<Any>`. This causes a weird borrow checker error when using a
///     `QueryBuilder` (https://github.com/launchbadge/sqlx/issues/1978)
///   - Make the implementation generic over any DB (that implements sqlx::Database). In that
///     case, we need to add a bunch of `where` clauses to the implementation giving constraints
///     on the argument, the query and the result types (see https://stackoverflow.com/a/70573732).
///     And, when we do that, we hit the borrow checker error again from #1.
///   - Add macros with default implementations for everything in the Repository trait and use them
///     instead of putting the whole implementation in a macro. This conflicts with the #[async_trait]
///     macro and breaks it (see https://stackoverflow.com/q/68573578). Another solution in that SO
///     question is generating the implementation functions with a macro and calling them
///     from the trait, which could work but still means we have to write out all functions in the
///     PG implementation, SQLite implementation and the macros for both variants of the implementation
///     functions (since we can't build a function that's generic over any DB)
///
/// In any case, this means we have to remove compile-time query checking (even if we duplicate the code
/// completely), see https://github.com/launchbadge/sqlx/issues/121 and
/// https://github.com/launchbadge/sqlx/issues/916.

/// Queries that are different between SQLite and PG
pub struct RepositoryQueries {
    pub all_columns_in_database: &'static str,
}

#[macro_export]
macro_rules! implement_repository {
    ($repo: ident) => {
#[async_trait]
impl Repository for $repo {
    async fn setup(&self) {
        $repo::MIGRATOR
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

        let columns = sqlx::query_as($repo::QUERIES.all_columns_in_database)
        .bind(database_id)
        .fetch_all(&self.executor)
        .await?;
        Ok(columns)
    }

    async fn get_all_regions_in_table(
        &self,
        table_version_id: TableVersionId,
    ) -> Result<Vec<AllTableRegionsResult>, Error> {
        let regions = sqlx::query_as(
            r#"SELECT
            physical_region.id AS table_region_id,
            physical_region.object_storage_id,
            physical_region.row_count AS row_count,
            physical_region_column.name AS column_name,
            physical_region_column.type AS column_type,
            physical_region_column.min_value,
            physical_region_column.max_value
        FROM table_region
        INNER JOIN physical_region ON physical_region.id = table_region.physical_region_id
        -- TODO left join?
        INNER JOIN physical_region_column ON physical_region_column.physical_region_id = physical_region.id
        WHERE table_region.table_version_id = $1
        ORDER BY table_region_id, physical_region_column.id
        "#,
        ).bind(table_version_id)
        .fetch_all(&self.executor)
        .await?;
        Ok(regions)
    }

    async fn create_database(&self, database_name: &str) -> Result<DatabaseId, Error> {
        let id = sqlx::query(r#"INSERT INTO database (name) VALUES ($1) RETURNING (id)"#)
            .bind(database_name)
            .fetch_one(&self.executor)
            .await?
            .try_get("id")?;

        Ok(id)
    }

    async fn get_collection_id_by_name(
        &self,
        database_name: &str,
        collection_name: &str,
    ) -> Result<CollectionId, Error> {
        let id = sqlx::query(
            r#"
        SELECT collection.id
        FROM collection JOIN database ON collection.database_id = database.id
        WHERE database.name = $1 AND collection.name = $2
        "#,
        )
        .bind(database_name)
        .bind(collection_name)
        .fetch_one(&self.executor)
        .await?
        .try_get("id")?;

        Ok(id)
    }

    async fn get_database_id_by_name(
        &self,
        database_name: &str,
    ) -> Result<DatabaseId, Error> {
        let id = sqlx::query(r#"SELECT id FROM database WHERE database.name = $1"#)
            .bind(database_name)
            .fetch_one(&self.executor)
            .await?
            .try_get("id")?;

        Ok(id)
    }

    async fn create_collection(
        &self,
        database_id: DatabaseId,
        collection_name: &str,
    ) -> Result<CollectionId, Error> {
        let id = sqlx::query(
            r#"INSERT INTO "collection" (database_id, name) VALUES ($1, $2) RETURNING (id)"#,
        ).bind(database_id).bind(collection_name)
        .fetch_one(&self.executor)
        .await?
        .try_get("id")?;

        Ok(id)
    }

    async fn create_table(
        &self,
        collection_id: CollectionId,
        table_name: &str,
        schema: Schema,
    ) -> Result<(TableId, TableVersionId), Error> {
        // Create new (empty) table
        let new_table_id: i64 = sqlx::query(
            r#"INSERT INTO "table" (collection_id, name) VALUES ($1, $2) RETURNING (id)"#,
        )
        .bind(collection_id)
        .bind(table_name)
        .fetch_one(&self.executor)
        .await?
        .try_get("id")?;

        // Create initial table version
        let new_version_id: i64 = sqlx::query(
            r#"INSERT INTO table_version (table_id) VALUES ($1) RETURNING (id)"#,
        )
        .bind(new_table_id)
        .fetch_one(&self.executor)
        .await?
        .try_get("id")?;

        // Create columns
        // TODO this breaks if we have more than (bind limit) columns
        let mut builder: QueryBuilder<_> =
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

        let mut builder: QueryBuilder<_> = QueryBuilder::new(
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
            .flat_map(|r| r.try_get("id"))
            .collect();

        // Create region columns

        // Make an vector of (region_id, column)
        let columns: Vec<(PhysicalRegionId, &RegionColumn)> = zip(&region_ids, &regions)
            .flat_map(|(region_id, region)| {
                region.columns.iter().map(|c| (region_id.to_owned(), c))
            })
            .collect();

        let mut builder: QueryBuilder<_> =
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
        let mut builder: QueryBuilder<_> = QueryBuilder::new(
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
        let new_version = sqlx::query(
            "INSERT INTO table_version (table_id)
            SELECT table_id FROM table_version WHERE id = $1
            RETURNING (id)",
        )
        .bind(from_version)
        .fetch_one(&self.executor)
        .await?
        .try_get("id")?;

        sqlx::query(
            "INSERT INTO table_column (table_version_id, name, type)
            SELECT $2, name, type FROM table_column WHERE table_version_id = $1;",
        )
        .bind(from_version)
        .bind(new_version)
        .execute(&self.executor)
        .await?;

        sqlx::query(
            "INSERT INTO table_region (table_version_id, physical_region_id)
            SELECT $2, physical_region_id FROM table_region WHERE table_version_id = $1;",
        )
        .bind(from_version)
        .bind(new_version)
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
        sqlx::query("DELETE FROM \"table\" WHERE id = $1 RETURNING id")
            .bind(table_id)
            .fetch_one(&self.executor)
            .await?;
        Ok(())
    }

    async fn drop_collection(&self, collection_id: CollectionId) -> Result<(), Error> {
        sqlx::query("DELETE FROM collection WHERE id = $1 RETURNING id")
            .bind(collection_id)
            .fetch_one(&self.executor)
            .await?;
        Ok(())
    }

    async fn drop_database(&self, database_id: DatabaseId) -> Result<(), Error> {
        sqlx::query("DELETE FROM database WHERE id = $1 RETURNING id")
            .bind(database_id)
            .fetch_one(&self.executor)
            .await?;
        Ok(())
    }
}

};
}
