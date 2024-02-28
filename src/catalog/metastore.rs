use crate::catalog::external::ExternalStore;
use crate::catalog::repository::RepositoryStore;
use crate::catalog::{
    CatalogError, CatalogResult, CatalogStore, CreateFunctionError, FunctionStore,
    SchemaStore, TableStore,
};
use crate::object_store::wrapped::InternalObjectStore;
use crate::provider::{SeafowlDatabase, SeafowlFunction, SeafowlSchema};
use crate::repository::interface::{AllDatabaseFunctionsResult, Repository};
use crate::system_tables::SystemSchemaProvider;
use crate::wasm_udf::data_types::{
    CreateFunctionDataType, CreateFunctionDetails, CreateFunctionLanguage,
    CreateFunctionVolatility,
};
use clade::schema::{SchemaObject, TableObject};
use datafusion::catalog::schema::MemorySchemaProvider;
use datafusion::datasource::TableProvider;
use deltalake::logstore::default_logstore;
use deltalake::storage::{ObjectStoreFactory, ObjectStoreRef, StorageOptions};
use deltalake::{DeltaResult, DeltaTable, DeltaTableError};
use object_store::path::Path;
use object_store::prefix::PrefixStore;
use object_store::{parse_url_opts, ObjectStore};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use url::Url;

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
    default_store: Arc<InternalObjectStore>,
}

impl Metastore {
    pub fn new_from_repository(
        repository: Arc<dyn Repository>,
        default_store: Arc<InternalObjectStore>,
    ) -> Self {
        let repository_store = Arc::new(RepositoryStore { repository });

        let staging_schema = Arc::new(MemorySchemaProvider::new());
        Self {
            catalogs: repository_store.clone(),
            schemas: repository_store.clone(),
            tables: repository_store.clone(),
            functions: repository_store,
            staging_schema,
            default_store,
        }
    }

    pub fn new_from_external(
        external_store: Arc<ExternalStore>,
        default_store: Arc<InternalObjectStore>,
    ) -> Self {
        let staging_schema = Arc::new(MemorySchemaProvider::new());
        Self {
            catalogs: external_store.clone(),
            schemas: external_store.clone(),
            tables: external_store.clone(),
            functions: external_store,
            staging_schema,
            default_store,
        }
    }

    pub async fn build_catalog(
        &self,
        catalog_name: &str,
    ) -> CatalogResult<SeafowlDatabase> {
        let catalog_schemas = self.schemas.list(catalog_name).await?;

        // Collect all provided object store locations into corresponding clients
        // TODO: cache this using location (+ options?) as key, potentially with
        // a TTL, and re-use when building table log stores below.
        let stores = catalog_schemas
            .stores
            .into_iter()
            .map(|store| {
                let url = Url::parse(&store.location)?;
                Ok((
                    store.location.clone(),
                    parse_url_opts(&url, store.options)?.0.into(),
                ))
            })
            .collect::<CatalogResult<_>>()?;

        // Turn the list of all collections, tables and their columns into a nested map.
        let schemas = catalog_schemas
            .schemas
            .into_iter()
            .map(|schema| self.build_schema(schema, &stores))
            .collect::<CatalogResult<HashMap<_, _>>>()?;

        let name: Arc<str> = Arc::from(catalog_name);

        Ok(SeafowlDatabase {
            name: name.clone(),
            schemas,
            staging_schema: self.staging_schema.clone(),
            system_schema: Arc::new(SystemSchemaProvider::new(name, self.tables.clone())),
        })
    }

    fn build_schema(
        &self,
        schema: SchemaObject,
        stores: &HashMap<String, Arc<dyn ObjectStore>>,
    ) -> CatalogResult<(Arc<str>, Arc<SeafowlSchema>)> {
        let schema_name = schema.name;

        let tables = schema
            .tables
            .into_iter()
            .map(|table| self.build_table(table, stores))
            .collect::<CatalogResult<HashMap<_, _>>>()?;

        Ok((
            Arc::from(schema_name.clone()),
            Arc::new(SeafowlSchema {
                name: Arc::from(schema_name),
                tables: RwLock::new(tables),
            }),
        ))
    }

    fn build_table(
        &self,
        table: TableObject,
        stores: &HashMap<String, Arc<dyn ObjectStore>>,
    ) -> CatalogResult<(Arc<str>, Arc<dyn TableProvider>)> {
        // Build a delta table but don't load it yet; we'll do that only for tables that are
        // actually referenced in a statement, via the async `table` method of the schema provider.
        // TODO: this means that any `information_schema.columns` query will serially load all
        // delta tables present in the database. The real fix for this is to make DF use `TableSource`
        // for the information schema, and then implement `TableSource` for `DeltaTable` in delta-rs.

        let table_log_store = match table.location {
            // Use the provided customized location
            Some(location) => {
                let store = stores.get(&location).ok_or(CatalogError::Generic {
                    reason: format!("Object store for location {location} not found"),
                })?;
                let prefixed_store: PrefixStore<Arc<dyn ObjectStore>> =
                    PrefixStore::new(store.clone(), &*table.path);

                let url = Url::parse(&format!("{location}/{}", table.path))?;

                default_logstore(Arc::from(prefixed_store), &url, &Default::default())
            }
            // Use the configured, default, object store
            None => self.default_store.get_log_store(&table.path),
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

impl ObjectStoreFactory for Metastore {
    fn parse_url_opts(
        &self,
        url: &Url,
        _options: &StorageOptions,
    ) -> DeltaResult<(ObjectStoreRef, Path)> {
        Err(DeltaTableError::InvalidTableLocation(url.clone().into()))
    }
}
