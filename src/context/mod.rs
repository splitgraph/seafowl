pub mod delta;
pub mod logical;
pub mod physical;

use crate::catalog::{DEFAULT_SCHEMA, STAGING_SCHEMA};
use crate::config::context::build_state_with_table_factories;
use crate::object_store::wrapped::InternalObjectStore;
use crate::wasm_udf::data_types::{get_volatility, CreateFunctionDetails};
use crate::wasm_udf::wasm::create_udf_from_wasm;
use crate::{
    catalog::{FunctionCatalog, TableCatalog},
    data_types::DatabaseId,
};

use base64::{engine::general_purpose::STANDARD, Engine};
pub use datafusion::error::{DataFusionError as Error, Result};
use datafusion::{error::DataFusionError, prelude::SessionContext, sql::TableReference};
use datafusion_common::OwnedTableReference;
use deltalake::DeltaTable;
use object_store::path::Path;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

// The core Seafowl object, responsible for parsing, logical and physical planning, as well as
// interacting with the catalog and object store.
pub struct SeafowlContext {
    pub inner: SessionContext,
    pub table_catalog: Arc<dyn TableCatalog>,
    pub function_catalog: Arc<dyn FunctionCatalog>,
    pub internal_object_store: Arc<InternalObjectStore>,
    pub database: String,
    pub database_id: DatabaseId,
    pub all_database_ids: Arc<RwLock<HashMap<String, DatabaseId>>>,
    pub max_partition_size: u32,
}

impl SeafowlContext {
    // Create a new `SeafowlContext` with a new inner context scoped to a different default DB
    pub async fn scope_to_database(&self, name: String) -> Result<Arc<SeafowlContext>> {
        let maybe_database_id = self.all_database_ids.read().get(name.as_str()).cloned();
        let database_id = match maybe_database_id {
            Some(db_id) => db_id,
            None => {
                // Perhaps the db was created on another node; try to reload from catalog
                let new_db_ids = self.table_catalog.load_database_ids().await?;
                new_db_ids
                    .get(name.as_str())
                    .cloned()
                    .map(|db_id| {
                        self.all_database_ids.write().insert(name.clone(), db_id);
                        db_id
                    })
                    .ok_or_else(|| {
                        DataFusionError::Plan(format!(
                            "Unknown database {name}; try creating one with CREATE DATABASE first"
                        ))
                    })?
            }
        };

        // Swap the default database in the new internal context's session config
        let session_config = self
            .inner()
            .copied_config()
            .with_default_catalog_and_schema(name.clone(), DEFAULT_SCHEMA);

        let state =
            build_state_with_table_factories(session_config, self.inner().runtime_env());

        Ok(Arc::from(SeafowlContext {
            inner: SessionContext::new_with_state(state),
            table_catalog: self.table_catalog.clone(),
            function_catalog: self.function_catalog.clone(),
            internal_object_store: self.internal_object_store.clone(),
            database: name,
            database_id,
            all_database_ids: self.all_database_ids.clone(),
            max_partition_size: self.max_partition_size,
        }))
    }

    pub fn inner(&self) -> &SessionContext {
        &self.inner
    }

    /// Reload the context to apply / pick up new schema changes
    async fn reload_schema(&self) -> Result<()> {
        // DataFusion's catalog provider interface is not async, which means that we aren't really
        // supposed to perform IO when loading the list of schemas. On the other hand, as of DF 16
        // the schema provider allows for async fetching of tables. However, this isn't that helpful,
        // since for a query with multiple tables we'd have multiple separate DB hits to load them,
        // whereas below we load everything we need up front. (Furthermore, table existence and name
        // listing are still sync meaning we'd need the pre-load for them as well.)
        // We hence load all schemas and tables into memory before every query (otherwise writes
        // applied by a different Seafowl instance won't be visible by us).

        // This does incur a latency cost to every query.

        self.inner.register_catalog(
            &self.database,
            Arc::new(self.table_catalog.load_database(self.database_id).await?),
        );

        // Register all functions in the database
        self.function_catalog
            .get_all_functions_in_database(self.database_id)
            .await?
            .iter()
            .try_for_each(|f| self.register_function(&f.name, &f.details))
    }

    // Check that the TableReference doesn't have a database/schema in it.
    // We create all external tables in the staging schema (backed by DataFusion's
    // in-memory schema provider) instead.
    fn resolve_staging_ref(
        &self,
        name: &OwnedTableReference,
    ) -> Result<OwnedTableReference> {
        // NB: Since Datafusion 16.0.0 for external tables the parsed ObjectName is coerced into the
        // `OwnedTableReference::Bare` enum variant, since qualified names are not supported for them
        // (see `external_table_to_plan` in datafusion-sql).
        //
        // This means that any potential catalog/schema references get condensed into the name, so
        // we have to unravel that name here again, and then resolve it properly.
        let reference = TableReference::from(name.to_string());
        let resolved_reference = reference.resolve(&self.database, STAGING_SCHEMA);

        if resolved_reference.catalog != self.database
            || resolved_reference.schema != STAGING_SCHEMA
        {
            return Err(DataFusionError::Plan(format!(
                "Can only create external tables in the staging schema.
                        Omit the schema/database altogether or use {}.{}.{}",
                &self.database, STAGING_SCHEMA, resolved_reference.table
            )));
        }

        Ok(TableReference::from(resolved_reference).to_owned_reference())
    }

    /// Resolve a table reference into a Delta table
    pub async fn try_get_delta_table<'a>(
        &self,
        table_name: impl Into<TableReference<'a>>,
    ) -> Result<DeltaTable> {
        let table_log_store = self
            .inner
            .table_provider(table_name)
            .await?
            .as_any()
            .downcast_ref::<DeltaTable>()
            .ok_or_else(|| {
                DataFusionError::Execution("Table {table_name} not found".to_string())
            })?
            .log_store();

        // We can't just keep hold of the downcasted ref from above because of
        // `temporary value dropped while borrowed`
        Ok(DeltaTable::new(table_log_store, Default::default()))
    }

    // Parse the uuid from the Delta table uri if available
    pub async fn get_table_uuid<'a>(
        &self,
        name: impl Into<TableReference<'a>>,
    ) -> Result<Uuid> {
        match self
            .inner
            .table_provider(name)
            .await?
            .as_any()
            .downcast_ref::<DeltaTable>()
        {
            None => {
                // TODO: try to load from DB if missing?
                Err(DataFusionError::Execution(
                    "Couldn't fetch table uuid".to_string(),
                ))
            }
            Some(delta_table) => {
                let table_uri = Path::from(delta_table.table_uri());
                let uuid = table_uri.parts().last().ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "Failed parsing the uuid suffix from uri {table_uri} for table {delta_table}"
                    ))
                })?;
                Ok(Uuid::try_parse(uuid.as_ref()).map_err(|err| {
                    DataFusionError::Execution(format!(
                        "Failed parsing uuid from {uuid:?}: {err}"
                    ))
                })?)
            }
        }
    }

    fn register_function(
        &self,
        name: &str,
        details: &CreateFunctionDetails,
    ) -> Result<()> {
        let function_code = STANDARD
            .decode(&details.data)
            .map_err(|e| Error::Execution(format!("Error decoding the UDF: {e:?}")))?;

        let function = create_udf_from_wasm(
            &details.language,
            name,
            &function_code,
            &details.entrypoint,
            &details.input_types,
            &details.return_type,
            get_volatility(&details.volatility),
        )?;
        self.inner.register_udf(function);

        Ok(())
    }
}

#[cfg(test)]
pub mod test_utils {
    use std::sync::Arc;

    use crate::config::context::build_context;
    use crate::config::schema;
    use crate::config::schema::{Catalog, SeafowlConfig, Sqlite};
    use sqlx::sqlite::SqliteJournalMode;

    use super::*;

    /// Build a real (not mocked) in-memory context that uses SQLite
    pub async fn in_memory_context() -> SeafowlContext {
        let config = SeafowlConfig {
            object_store: schema::ObjectStore::InMemory(schema::InMemory {}),
            catalog: Catalog::Sqlite(Sqlite {
                dsn: "sqlite://:memory:".to_string(),
                journal_mode: SqliteJournalMode::Wal,
                read_only: false,
            }),
            frontend: Default::default(),
            runtime: Default::default(),
            misc: Default::default(),
        };
        build_context(&config).await.unwrap()
    }

    pub async fn in_memory_context_with_test_db() -> Arc<SeafowlContext> {
        let context = in_memory_context().await;

        // Create new non-default database; we're doing this in catalog only to simulate it taking
        // place on another node
        context
            .table_catalog
            .create_database("testdb")
            .await
            .unwrap();

        let context = context
            .scope_to_database("testdb".to_string())
            .await
            .expect("'testdb' should exist");

        // Create new non-default collection
        context.plan_query("CREATE SCHEMA testcol").await.unwrap();

        // Create table
        context
            .plan_query("CREATE TABLE testcol.some_table (date DATE, value DOUBLE)")
            .await
            .unwrap();

        context
    }
}

#[cfg(test)]
mod tests {
    use datafusion::assert_batches_eq;
    use rstest::rstest;

    use super::test_utils::in_memory_context;
    use super::*;

    #[rstest]
    #[case::regular_type_names("float", "float")]
    #[case::legacy_type_names("f32", "f32")]
    #[case::uppercase_type_names("FLOAT", "REAL")]
    #[tokio::test]
    async fn test_register_udf(
        #[case] input_type: &str,
        #[case] return_type: &str,
    ) -> Result<()> {
        let ctx = in_memory_context().await;

        // Source: https://gist.github.com/going-digital/02e46c44d89237c07bc99cd440ebfa43
        let create_function_stmt = r#"CREATE FUNCTION sintau AS '
        {
            "entrypoint": "sintau",
            "language": "wasm",
            "input_types": ["int"],
            "return_type": "int",
            "data": "AGFzbQEAAAABDQJgAX0BfWADfX9/AX0DBQQAAAABBQQBAUREBxgDBnNpbnRhdQAABGV4cDIAAQRsb2cyAAIKjgEEKQECfUMAAAA/IgIgACAAjpMiACACk4siAZMgAZZBAEEYEAMgAiAAk5gLGQAgACAAjiIAk0EYQSwQA7wgAKhBF3RqvgslAQF/IAC8IgFBF3ZB/wBrsiABQQl0s0MAAIBPlUEsQcQAEAOSCyIBAX0DQCADIACUIAEqAgCSIQMgAUEEaiIBIAJrDQALIAMLC0oBAEEAC0Q/x2FC2eATQUuqKsJzsqY9QAHJQH6V0DZv+V88kPJTPSJndz6sZjE/HQCAP/clMD0D/T++F6bRPkzcNL/Tgrg//IiKNwBqBG5hbWUBHwQABnNpbnRhdQEEZXhwMgIEbG9nMgMIZXZhbHBvbHkCNwQAAwABeAECeDECBGhhbGYBAQABeAICAAF4AQJ4aQMEAAF4AQVzdGFydAIDZW5kAwZyZXN1bHQDCQEDAQAEbG9vcA=="
        }';"#;

        ctx.plan_query(create_function_stmt).await?;

        // Run the same query again to make sure we raise an error if the function already exists
        let err = ctx.plan_query(create_function_stmt).await.unwrap_err();

        assert_eq!(
            err.to_string(),
            "Error during planning: Function \"sintau\" already exists"
        );

        // Now replace the function using proper input/return types
        let replace_function_stmt = format!(
            r#"CREATE OR REPLACE FUNCTION sintau AS '
        {{
            "entrypoint": "sintau",
            "language": "wasm",
            "input_types": ["{input_type}"],
            "return_type": "{return_type}",
            "data": "AGFzbQEAAAABDQJgAX0BfWADfX9/AX0DBQQAAAABBQQBAUREBxgDBnNpbnRhdQAABGV4cDIAAQRsb2cyAAIKjgEEKQECfUMAAAA/IgIgACAAjpMiACACk4siAZMgAZZBAEEYEAMgAiAAk5gLGQAgACAAjiIAk0EYQSwQA7wgAKhBF3RqvgslAQF/IAC8IgFBF3ZB/wBrsiABQQl0s0MAAIBPlUEsQcQAEAOSCyIBAX0DQCADIACUIAEqAgCSIQMgAUEEaiIBIAJrDQALIAMLC0oBAEEAC0Q/x2FC2eATQUuqKsJzsqY9QAHJQH6V0DZv+V88kPJTPSJndz6sZjE/HQCAP/clMD0D/T++F6bRPkzcNL/Tgrg//IiKNwBqBG5hbWUBHwQABnNpbnRhdQEEZXhwMgIEbG9nMgMIZXZhbHBvbHkCNwQAAwABeAECeDECBGhhbGYBAQABeAICAAF4AQJ4aQMEAAF4AQVzdGFydAIDZW5kAwZyZXN1bHQDCQEDAQAEbG9vcA=="
        }}';"#
        );

        ctx.plan_query(replace_function_stmt.as_str()).await?;

        let results = ctx
            .collect(
                ctx.plan_query(
                    "
        SELECT v, ROUND(sintau(CAST(v AS REAL)) * 100) AS sintau
        FROM (VALUES (0.1), (0.2), (0.3), (0.4), (0.5)) d (v)",
                )
                .await?,
            )
            .await?;

        let expected = [
            "+-----+--------+",
            "| v   | sintau |",
            "+-----+--------+",
            "| 0.1 | 59.0   |",
            "| 0.2 | 95.0   |",
            "| 0.3 | 95.0   |",
            "| 0.4 | 59.0   |",
            "| 0.5 | 0.0    |",
            "+-----+--------+",
        ];

        assert_batches_eq!(expected, &results);

        Ok(())
    }

    #[tokio::test]
    async fn test_register_invalid_udf() -> Result<()> {
        let ctx = in_memory_context().await;

        // Source: https://gist.github.com/going-digital/02e46c44d89237c07bc99cd440ebfa43
        let plan = ctx
            .plan_query(
                r#"CREATE FUNCTION invalidfn AS '
            {
                "entrypoint": "invalidfn",
                "language": "wasmMessagePack",
                "input_types": ["float"],
                "return_type": "float",
                "data": ""
            }';"#,
            )
            .await;
        assert!(plan.is_err());
        assert!(plan.err().unwrap().to_string().starts_with(
            "Internal error: Error initializing WASM + MessagePack UDF \"invalidfn\": Internal(\"Error loading WASM module: failed to parse WebAssembly module"));
        Ok(())
    }

    #[tokio::test]
    async fn test_drop_function() -> Result<()> {
        let ctx = in_memory_context().await;

        let err = ctx
            .plan_query(r#"DROP FUNCTION nonexistentfunction"#)
            .await
            .unwrap_err();

        assert_eq!(
            err.to_string(),
            "Error during planning: Function \"nonexistentfunction\" not found"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_drop_function_if_exists() -> Result<()> {
        let ctx = in_memory_context().await;

        let plan = ctx
            .plan_query(r#"DROP FUNCTION IF EXISTS nonexistentfunction"#)
            .await;
        assert!(plan.is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn test_create_and_drop_two_functions() -> Result<()> {
        let ctx = in_memory_context().await;

        let create_function_stmt = r#"CREATE FUNCTION sintau AS '
        {
            "entrypoint": "sintau",
            "language": "wasm",
            "input_types": ["int"],
            "return_type": "int",
            "data": "AGFzbQEAAAABDQJgAX0BfWADfX9/AX0DBQQAAAABBQQBAUREBxgDBnNpbnRhdQAABGV4cDIAAQRsb2cyAAIKjgEEKQECfUMAAAA/IgIgACAAjpMiACACk4siAZMgAZZBAEEYEAMgAiAAk5gLGQAgACAAjiIAk0EYQSwQA7wgAKhBF3RqvgslAQF/IAC8IgFBF3ZB/wBrsiABQQl0s0MAAIBPlUEsQcQAEAOSCyIBAX0DQCADIACUIAEqAgCSIQMgAUEEaiIBIAJrDQALIAMLC0oBAEEAC0Q/x2FC2eATQUuqKsJzsqY9QAHJQH6V0DZv+V88kPJTPSJndz6sZjE/HQCAP/clMD0D/T++F6bRPkzcNL/Tgrg//IiKNwBqBG5hbWUBHwQABnNpbnRhdQEEZXhwMgIEbG9nMgMIZXZhbHBvbHkCNwQAAwABeAECeDECBGhhbGYBAQABeAICAAF4AQJ4aQMEAAF4AQVzdGFydAIDZW5kAwZyZXN1bHQDCQEDAQAEbG9vcA=="
        }';"#;

        let create_function_stmt2 = r#"CREATE FUNCTION sintau2 AS '
        {
            "entrypoint": "sintau",
            "language": "wasm",
            "input_types": ["int"],
            "return_type": "int",
            "data": "AGFzbQEAAAABDQJgAX0BfWADfX9/AX0DBQQAAAABBQQBAUREBxgDBnNpbnRhdQAABGV4cDIAAQRsb2cyAAIKjgEEKQECfUMAAAA/IgIgACAAjpMiACACk4siAZMgAZZBAEEYEAMgAiAAk5gLGQAgACAAjiIAk0EYQSwQA7wgAKhBF3RqvgslAQF/IAC8IgFBF3ZB/wBrsiABQQl0s0MAAIBPlUEsQcQAEAOSCyIBAX0DQCADIACUIAEqAgCSIQMgAUEEaiIBIAJrDQALIAMLC0oBAEEAC0Q/x2FC2eATQUuqKsJzsqY9QAHJQH6V0DZv+V88kPJTPSJndz6sZjE/HQCAP/clMD0D/T++F6bRPkzcNL/Tgrg//IiKNwBqBG5hbWUBHwQABnNpbnRhdQEEZXhwMgIEbG9nMgMIZXZhbHBvbHkCNwQAAwABeAECeDECBGhhbGYBAQABeAICAAF4AQJ4aQMEAAF4AQVzdGFydAIDZW5kAwZyZXN1bHQDCQEDAQAEbG9vcA=="
        }';"#;

        // Create two functions in two separate passes
        ctx.plan_query(create_function_stmt).await?;
        ctx.plan_query(create_function_stmt2).await?;

        // Test dropping both functions in one pass
        let plan = ctx.plan_query(r#"DROP FUNCTION sintau, sintau2"#).await;
        assert!(plan.is_ok());
        Ok(())
    }
}
