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
///     pub fn interpret_error(error: sqlx::Error) -> Error {
///         // Interpret the database-specific error code and turn some sqlx errors
///         // into the Error enum values like UniqueConstraintViolation/FKConstraintViolation
///         // ...
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
    pub latest_table_versions: &'static str,
    pub cast_timestamp: &'static str,
}

#[macro_export]
macro_rules! implement_repository {
    ($repo: ident) => {
#[async_trait]
impl Repository for $repo {
    async fn setup(&self) {
        // TODO remove the entire block a couple of releases past 0.5.7
        {
            // Migrate the old field JSON format to the canonical one
            #[derive(sqlx::FromRow)]
            struct ColumnSchema {
                id: i64,
                r#type: String,
            }

            // Fetch all rows from the table
            let maybe_rows: Result<Vec<ColumnSchema>, sqlx::Error> = sqlx::query_as::<_, ColumnSchema>("SELECT id, type FROM table_column")
                .fetch_all(&self.executor)
                .await;
            if let Ok(rows) = maybe_rows {
                for row in rows {
                    // Load the `type` value using `arrow_integration_test::field_from_json`
                    if let Ok(value) = serde_json::from_str::<serde_json::Value>(row.r#type.as_str()) {
                        if let Ok(field) = arrow_integration_test::field_from_json(&value) {
                            // Convert the `Field` to a `serde_json` representation
                            if let Ok(field_json) = serde_json::to_string(&field) {
                                // Update the table with the new `type` value
                                let _ = sqlx::query("UPDATE table_column SET type = $1 WHERE id = $2")
                                    .bind(field_json)
                                    .bind(row.id)
                                    .execute(&self.executor)
                                    .await;
                            }
                        }
                    }
                }
            }
        }

        $repo::MIGRATOR
            .run(&self.executor)
            .await
            .expect("error running migrations");
    }

    async fn list_collections(
        &self,
        database_name: &str,
    ) -> Result<Vec<AllDatabaseColumnsResult>, Error> {
        let mut builder: QueryBuilder<_> = QueryBuilder::new($repo::QUERIES.latest_table_versions);

        builder.push(r#"
        SELECT
            database.name AS database_name,
            collection.name AS collection_name,
            "table".name AS table_name,
            "table".id AS table_id,
            "table".uuid AS table_uuid,
            desired_table_versions.id AS table_version_id,
            table_column.name AS column_name,
            table_column.type AS column_type
        FROM database
        INNER JOIN collection ON database.id = collection.database_id
        LEFT JOIN "table" ON collection.id = "table".collection_id
        LEFT JOIN desired_table_versions ON "table".id = desired_table_versions.table_id
        LEFT JOIN table_column ON table_column.table_version_id = desired_table_versions.id
        WHERE database.name = "#);
        builder.push_bind(database_name);

        builder.push(r#"
        ORDER BY collection_name, table_name, table_version_id, column_name
        "#);

        let query = builder.build_query_as();
        let columns = query
            .fetch(&self.executor)
            .try_collect()
            .await
            .map_err($repo::interpret_error)?;

        Ok(columns)
    }

    async fn create_database(&self, database_name: &str) -> Result<DatabaseId, Error> {
        let id = sqlx::query(r#"INSERT INTO database (name) VALUES ($1) RETURNING (id)"#)
            .bind(database_name)
            .fetch_one(&self.executor)
            .await.map_err($repo::interpret_error)?
            .try_get("id").map_err($repo::interpret_error)?;

        Ok(id)
    }

    async fn get_database(
        &self,
        name: &str,
    ) -> Result<DatabaseRecord, Error> {
        let database = sqlx::query_as(r#"SELECT id, name FROM database WHERE database.name = $1"#)
            .bind(name)
            .fetch_one(&self.executor)
            .await.map_err($repo::interpret_error)?;

        Ok(database)
    }

    async fn get_collection(
        &self,
        database_name: &str,
        collection_name: &str,
    ) -> Result<CollectionRecord, Error> {
        let collection = sqlx::query_as(
            r#"
        SELECT collection.id, database.id AS database_id, collection.name
        FROM collection JOIN database ON collection.database_id = database.id
        WHERE database.name = $1 AND collection.name = $2
        "#,
        )
        .bind(database_name)
        .bind(collection_name)
        .fetch_one(&self.executor)
        .await.map_err($repo::interpret_error)?;

        Ok(collection)
    }

    async fn get_table(
        &self,
        database_name: &str,
        collection_name: &str,
        table_name: &str,
    ) -> Result<TableRecord, Error> {
        let table = sqlx::query_as(
            r#"
        SELECT "table".id, collection.id as collection_id, "table".name
        FROM "table"
        JOIN collection ON "table".collection_id = collection.id
        JOIN database ON collection.database_id = database.id
        WHERE database.name = $1 AND collection.name = $2 AND "table".name = $3
        "#,
        )
        .bind(database_name)
        .bind(collection_name)
        .bind(table_name)
        .fetch_one(&self.executor)
        .await.map_err($repo::interpret_error)?;

        Ok(table)
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
        .await.map_err($repo::interpret_error)?
        .try_get("id").map_err($repo::interpret_error)?;

        Ok(id)
    }

    async fn create_table(
        &self,
        collection_id: CollectionId,
        table_name: &str,
        schema: &Schema,
        uuid: Uuid,
    ) -> Result<(TableId, TableVersionId), Error> {
        // Create new (empty) table
        let new_table_id: i64 = sqlx::query(
            r#"INSERT INTO "table" (collection_id, name, uuid) VALUES ($1, $2, $3) RETURNING (id)"#,
        )
        .bind(collection_id)
        .bind(table_name)
        .bind(uuid)
        .fetch_one(&self.executor)
        .await.map_err($repo::interpret_error)?
        .try_get("id").map_err($repo::interpret_error)?;

        // Create initial table version
        let new_version_id: i64 = sqlx::query(
            r#"INSERT INTO table_version (table_id) VALUES ($1) RETURNING (id)"#,
        )
        .bind(new_table_id)
        .fetch_one(&self.executor)
        .await.map_err($repo::interpret_error)?
        .try_get("id").map_err($repo::interpret_error)?;

        // Create columns
        // TODO this breaks if we have more than (bind limit) columns
        if !schema.fields().is_empty() {
            let mut builder: QueryBuilder<_> =
                QueryBuilder::new("INSERT INTO table_column(table_version_id, name, type) ");

            let fields: Vec<(String, String)> = schema.fields()
                .iter()
                .map(|f| Ok((f.name().clone(), serde_json::to_string(&f)?)))
                .collect::<Result<_>>()?;

            builder.push_values(fields, |mut b, col| {
                b.push_bind(new_version_id)
                    .push_bind(col.0)
                    .push_bind(col.1);
            });

            let query = builder.build();
            query.execute(&self.executor).await.map_err($repo::interpret_error)?;
        }

        Ok((new_table_id, new_version_id))
    }

    async fn delete_old_versions(
        &self,
        table_id: TableId,
    ) -> Result<u64, Error> {
        let delete_result = sqlx::query(
            "DELETE FROM table_version WHERE table_id = $1 AND id NOT IN \
            (SELECT DISTINCT first_value(id) OVER (PARTITION BY table_id ORDER BY creation_time DESC, id DESC) FROM table_version)"
        )
            .bind(table_id)
            .execute(&self.executor)
            .await
            .map_err($repo::interpret_error)?;

        Ok(delete_result.rows_affected())
    }

    async fn create_new_version(
        &self,
        uuid: Uuid,
        version: i64,
    ) -> Result<TableVersionId, Error> {
        // For now we only support linear history
        let last_version_id: TableVersionId = sqlx::query(r#"SELECT max(table_version.id) AS id
                FROM table_version
                JOIN "table" ON table_version.table_id = "table".id
                WHERE "table".uuid = $1"#)
            .bind(uuid)
            .fetch_one(&self.executor)
            .await.map_err($repo::interpret_error)?
            .try_get("id").map_err($repo::interpret_error)?;

        let new_version_id = sqlx::query(
            "INSERT INTO table_version (table_id, version)
            SELECT table_id, $1 FROM table_version WHERE id = $2
            RETURNING (id)",
        )
        .bind(version)
        .bind(last_version_id)
        .fetch_one(&self.executor)
        .await.map_err($repo::interpret_error)?
        .try_get("id").map_err($repo::interpret_error)?;

        sqlx::query(
            "INSERT INTO table_column (table_version_id, name, type)
            SELECT $2, name, type FROM table_column WHERE table_version_id = $1;",
        )
        .bind(last_version_id)
        .bind(new_version_id)
        .execute(&self.executor)
        .await.map_err($repo::interpret_error)?;

        Ok(new_version_id)
    }

    async fn get_all_versions(
        &self,
        database_name: &str,
        table_names: Option<Vec<String>>,
    ) -> Result<Vec<TableVersionsResult>, Error> {
        let query = format!(r#"SELECT
                database.name AS database_name,
                collection.name AS collection_name,
                "table".name AS table_name,
                table_version.id AS table_version_id,
                table_version.version AS version,
                {} AS creation_time
            FROM table_version
            INNER JOIN "table" ON "table".id = table_version.table_id
            INNER JOIN collection ON collection.id = "table".collection_id
            INNER JOIN database ON database.id = collection.database_id"#,
            $repo::QUERIES.cast_timestamp.replace("timestamp_column", "table_version.creation_time")
        );

        // We have to manually construct the query since SQLite doesn't have the proper Encode trait
        let mut builder: QueryBuilder<_> = QueryBuilder::new(&query);

        builder.push(" WHERE database.name = ");
        builder.push_bind(database_name);

        if let Some(table_names) = table_names {
            if !table_names.is_empty() {
                builder.push(" AND \"table\".name IN (");
                let mut separated = builder.separated(", ");
                for table_name in table_names.into_iter() {
                    separated.push_bind(table_name);
                }
                separated.push_unseparated(")");
            }
        }

        let query = builder.build_query_as();
        let table_versions = query
            .fetch(&self.executor)
            .try_collect()
            .await
            .map_err($repo::interpret_error)?;

        Ok(table_versions)
    }

    async fn rename_table(
        &self,
        table_id: TableId,
        new_table_name: &str,
        new_collection_id: Option<CollectionId>,
    ) -> Result<(), Error> {
        // Do RETURNING(id) here and ask for the ID back with fetch_one() to force a
        // row not found error if the table doesn't exist
        let query = if let Some(new_collection_id) = new_collection_id {
            sqlx::query("UPDATE \"table\" SET name = $1, collection_id = $2 WHERE id = $3 RETURNING id").bind(new_table_name).bind(new_collection_id).bind(table_id)
        } else {
            sqlx::query("UPDATE \"table\" SET name = $1 WHERE id = $2 RETURNING id").bind(new_table_name).bind(table_id)
        };
        query.fetch_one(&self.executor).await.map_err($repo::interpret_error)?;
        Ok(())
    }

    async fn create_function(
        &self,
        database_id: DatabaseId,
        function_name: &str,
        or_replace: bool,
        details: &CreateFunctionDetails,
    ) -> Result<FunctionId, Error> {
        let input_types = serde_json::to_string(&details.input_types).expect("Couldn't serialize input types!");

        let query = format!(
            r#"
        INSERT INTO "function" (database_id, name, entrypoint, language, input_types, return_type, data, volatility)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8){} RETURNING (id);
        "#,
            if or_replace {
                " ON CONFLICT (database_id, name) DO UPDATE SET entrypoint = EXCLUDED.entrypoint, \
                language = EXCLUDED.language, \
                input_types = EXCLUDED.input_types, \
                return_type = EXCLUDED.return_type, \
                data = EXCLUDED.data, \
                volatility = EXCLUDED.volatility"
            } else {
                ""
            }
        );

        let new_function_id: i64 = sqlx::query(query.as_str())
            .bind(database_id)
            .bind(function_name)
            .bind(details.entrypoint.clone())
            .bind(details.language.to_string())
            .bind(input_types)
            .bind(details.return_type.to_string())
            .bind(details.data.clone())
            .bind(details.volatility.to_string())
            .fetch_one(&self.executor)
            .await.map_err($repo::interpret_error)?
            .try_get("id").map_err($repo::interpret_error)?;

        Ok(new_function_id)
    }

    async fn get_all_functions_in_database(
        &self,
        database_id: DatabaseId,
    ) -> Result<Vec<AllDatabaseFunctionsResult>, Error> {
        let functions = sqlx::query_as(
            r#"
        SELECT
            name,
            id,
            entrypoint,
            language,
            input_types,
            return_type,
            data,
            volatility
        FROM function

        WHERE database_id = $1;
        "#)
        .bind(database_id)
        .fetch_all(&self.executor)
        .await.map_err($repo::interpret_error)?;

        Ok(functions)
    }


    async fn drop_function(
        &self,
        database_id: DatabaseId,
        func_names: &[String],
    ) -> Result<(), Error> {
        let query = format!(
            r#"
            DELETE FROM "function"
            WHERE database_id = $1
            AND name IN ({})
            RETURNING id;
            "#,
            func_names.iter().map(|_| "$2").collect::<Vec<_>>().join(", ")
        );

        let mut query_builder = sqlx::query(&query).bind(database_id);
        for func_name in func_names {
            query_builder = query_builder.bind(func_name);
        }
        query_builder
            .fetch_one(&self.executor)
            .await
            .map_err($repo::interpret_error)?;

        Ok(())
    }

    // Drop table/collection/database

    // In these methods, return the ID back so that we get an error if the
    // table/collection/schema didn't actually exist
    async fn delete_table(&self, table_id: TableId) -> Result<(), Error> {
        sqlx::query("DELETE FROM \"table\" WHERE id = $1 RETURNING id")
            .bind(table_id)
            .fetch_one(&self.executor)
            .await.map_err($repo::interpret_error)?;
        Ok(())
    }

    async fn delete_collection(&self, collection_id: CollectionId) -> Result<(), Error> {
        sqlx::query("DELETE FROM collection WHERE id = $1 RETURNING id")
            .bind(collection_id)
            .fetch_one(&self.executor)
            .await.map_err($repo::interpret_error)?;
        Ok(())
    }

    async fn delete_database(&self, database_id: DatabaseId) -> Result<(), Error> {
        sqlx::query("DELETE FROM database WHERE id = $1 RETURNING id")
            .bind(database_id)
            .fetch_one(&self.executor)
            .await.map_err($repo::interpret_error)?;
        Ok(())
    }

    async fn get_dropped_tables(
        &self,
        database_name: Option<String>,
    ) -> Result<Vec<DroppedTablesResult>> {
        let query = format!(r#"SELECT
                database_name,
                collection_name,
                table_name,
                uuid,
                deletion_status,
                {} AS drop_time
            FROM dropped_table"#,
            $repo::QUERIES.cast_timestamp.replace("timestamp_column", "drop_time")
        );

        let mut builder: QueryBuilder<_> = QueryBuilder::new(&query);

        if let Some(database) = database_name {
            builder.push(" WHERE database_name = ");
            builder.push_bind(database);
        }

        let dropped_tables = builder.build_query_as()
            .fetch_all(&self.executor)
            .await.map_err($repo::interpret_error)?;

        Ok(dropped_tables)
    }

    async fn update_dropped_table(&self, uuid: Uuid, deletion_status: DroppedTableDeletionStatus) -> Result<(), Error> {
        sqlx::query("UPDATE dropped_table SET deletion_status = $1 WHERE uuid = $2 RETURNING uuid")
            .bind(deletion_status)
            .bind(uuid)
            .fetch_one(&self.executor)
            .await.map_err($repo::interpret_error)?;
        Ok(())
    }

    async fn delete_dropped_table(&self, uuid: Uuid) -> Result<(), Error> {
        sqlx::query("DELETE FROM dropped_table WHERE uuid = $1 RETURNING uuid")
            .bind(uuid)
            .fetch_one(&self.executor)
            .await.map_err($repo::interpret_error)?;
        Ok(())
    }
}

};
}
