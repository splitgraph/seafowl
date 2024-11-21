use crate::catalog::external::ExternalStore;
use crate::catalog::repository::RepositoryStore;
use crate::catalog::{
    CatalogError, CatalogResult, CatalogStore, CreateFunctionError, FunctionStore,
    SchemaStore, TableStore,
};

use crate::object_store::factory::ObjectStoreFactory;
use crate::provider::{SeafowlDatabase, SeafowlFunction, SeafowlSchema};
use crate::repository::interface::{AllDatabaseFunctionsResult, Repository};
use crate::system_tables::SystemSchemaProvider;
use crate::wasm_udf::data_types::{
    CreateFunctionDataType, CreateFunctionDetails, CreateFunctionLanguage,
    CreateFunctionVolatility,
};
use clade::schema::{SchemaObject, TableObject};
use dashmap::DashMap;
use datafusion::catalog_common::memory::MemorySchemaProvider;
use datafusion::datasource::TableProvider;

use crate::catalog::memory::MemoryStore;
use deltalake::DeltaTable;
use futures::{stream, StreamExt, TryStreamExt};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use url::Url;

use super::empty::EmptyStore;

// Root URL for a storage location alongside client connection options
type LocationOptionsAndCredentials =
    (String, HashMap<String, String>, HashMap<String, String>);

// This is the main entrypoint to all individual catalogs for various objects types.
// The intention is to make it extensible and de-coupled from the underlying metastore
// persistence mechanism (such as the presently used `Repository`).
#[derive(Clone)]
pub struct Metastore {
    pub catalogs: Arc<dyn CatalogStore>,
    pub schemas: Arc<dyn SchemaStore>,
    pub tables: Arc<dyn TableStore>,
    pub functions: Arc<dyn FunctionStore>,
    staging_schema: Arc<MemorySchemaProvider>,
    pub object_stores: Arc<ObjectStoreFactory>,
}

impl Metastore {
    pub fn new_from_repository(
        repository: Arc<dyn Repository>,
        object_stores: Arc<ObjectStoreFactory>,
    ) -> Self {
        let repository_store = Arc::new(RepositoryStore { repository });

        let staging_schema = Arc::new(MemorySchemaProvider::new());
        Self {
            catalogs: repository_store.clone(),
            schemas: repository_store.clone(),
            tables: repository_store.clone(),
            functions: repository_store,
            staging_schema,
            object_stores,
        }
    }

    pub fn new_from_external(
        external_store: Arc<ExternalStore>,
        object_stores: Arc<ObjectStoreFactory>,
    ) -> Self {
        let staging_schema = Arc::new(MemorySchemaProvider::new());
        Self {
            catalogs: external_store.clone(),
            schemas: external_store.clone(),
            tables: external_store.clone(),
            functions: external_store,
            staging_schema,
            object_stores,
        }
    }

    pub fn new_from_memory(
        memory_store: Arc<MemoryStore>,
        object_stores: Arc<ObjectStoreFactory>,
    ) -> Self {
        let staging_schema = Arc::new(MemorySchemaProvider::new());
        Self {
            catalogs: memory_store.clone(),
            schemas: memory_store.clone(),
            tables: memory_store.clone(),
            functions: memory_store,
            staging_schema,
            object_stores,
        }
    }

    pub fn new_empty(object_stores: Arc<ObjectStoreFactory>) -> Self {
        let staging_schema = Arc::new(MemorySchemaProvider::new());
        let empty_store = Arc::new(EmptyStore {});
        Self {
            catalogs: empty_store.clone(),
            schemas: empty_store.clone(),
            tables: empty_store.clone(),
            functions: empty_store,
            staging_schema,
            object_stores,
        }
    }

    pub async fn build_catalog(
        &self,
        catalog_name: &str,
    ) -> CatalogResult<SeafowlDatabase> {
        let catalog_schemas = self.schemas.list(catalog_name).await?;

        // Collect all provided object store locations and options
        let store_options = catalog_schemas
            .stores
            .into_iter()
            .map(|store| (store.name, (store.url, store.options, store.credentials)))
            .collect();

        // Turn the list of all collections, tables and their columns into a nested map.
        let schemas = stream::iter(catalog_schemas.schemas)
            .then(|schema| self.build_schema(schema, &store_options))
            .try_collect()
            .await?;

        let name: Arc<str> = Arc::from(catalog_name);

        Ok(SeafowlDatabase {
            name: name.clone(),
            schemas,
            staging_schema: self.staging_schema.clone(),
            system_schema: Arc::new(SystemSchemaProvider::new(name, self.tables.clone())),
        })
    }

    async fn build_schema(
        &self,
        schema: SchemaObject,
        store_options_and_credentials: &HashMap<String, LocationOptionsAndCredentials>,
    ) -> CatalogResult<(Arc<str>, Arc<SeafowlSchema>)> {
        let schema_name = schema.name;

        let tables: DashMap<_, _> = stream::iter(schema.tables)
            .then(|table| self.build_table(table, store_options_and_credentials))
            .try_collect()
            .await?;

        Ok((
            Arc::from(schema_name.clone()),
            Arc::new(SeafowlSchema {
                name: Arc::from(schema_name),
                tables,
            }),
        ))
    }

    async fn build_table(
        &self,
        table: TableObject,
        store_options_and_credentials: &HashMap<String, LocationOptionsAndCredentials>,
    ) -> CatalogResult<(Arc<str>, Arc<dyn TableProvider>)> {
        // Build a delta table but don't load it yet; we'll do that only for tables that are
        // actually referenced in a statement, via the async `table` method of the schema provider.
        // TODO: this means that any `information_schema.columns` query will serially load all
        // delta tables present in the database. The real fix for this is to make DF use `TableSource`
        // for the information schema, and then implement `TableSource` for `DeltaTable` in delta-rs.

        let table_log_store = match table.store {
            // Use the provided customized location
            Some(name) => {
                let (location, this_store_options, this_store_credentials) =
                    store_options_and_credentials
                        .get(&name)
                        .ok_or(CatalogError::Generic {
                            reason: format!("Object store with name {name} not found"),
                        })?
                        .clone();

                self.object_stores
                    .get_log_store_for_table(
                        Url::parse(&location)?,
                        this_store_options,
                        this_store_credentials,
                        table.path,
                    )
                    .await?
            }
            // Use the configured, default, object store
            None => self
                .object_stores
                .get_default_log_store(&table.path)
                .ok_or(CatalogError::NoTableStoreInInlineMetastore {
                    name: table.name.clone(),
                })?,
        };

        let delta_table = DeltaTable::new(table_log_store, Default::default());
        Ok((Arc::from(table.name), Arc::new(delta_table) as _))
    }

    pub async fn build_functions(
        &self,
        catalog_name: &str,
    ) -> CatalogResult<Vec<SeafowlFunction>> {
        let functions = self.functions.list(catalog_name).await?;

        functions
            .iter()
            .map(|item| {
                Self::parse_create_function_details(item)
                    .map(|details| SeafowlFunction {
                        function_id: item.id,
                        name: item.name.to_owned(),
                        details,
                    })
                    .map_err(|e| CatalogError::FunctionDeserializationError {
                        reason: e.message,
                    })
            })
            .collect::<CatalogResult<Vec<SeafowlFunction>>>()
    }

    fn parse_create_function_details(
        item: &AllDatabaseFunctionsResult,
    ) -> Result<CreateFunctionDetails, CreateFunctionError> {
        let AllDatabaseFunctionsResult {
            id: _,
            name: _,
            entrypoint,
            language,
            input_types,
            return_type,
            data,
            volatility,
        } = item;

        Ok(CreateFunctionDetails {
            entrypoint: entrypoint.to_string(),
            language: CreateFunctionLanguage::from_str(language.as_str())?,
            input_types: serde_json::from_str::<Vec<CreateFunctionDataType>>(
                input_types,
            )?,
            return_type: CreateFunctionDataType::from_str(
                &return_type.as_str().to_ascii_uppercase(),
            )?,
            data: data.to_string(),
            volatility: CreateFunctionVolatility::from_str(volatility.as_str())?,
        })
    }
}
