use std::str::FromStr;
use std::{collections::HashMap, fmt::Debug, sync::Arc};

use arrow_schema::Schema;
use async_trait::async_trait;
use datafusion::catalog::schema::MemorySchemaProvider;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use deltalake::DeltaTable;
use itertools::Itertools;
#[cfg(test)]
use mockall::automock;
use parking_lot::RwLock;
use uuid::Uuid;

use crate::object_store::wrapped::InternalObjectStore;
use crate::provider::SeafowlFunction;
use crate::repository::interface::{
    DatabaseRecord, DroppedTableDeletionStatus, DroppedTablesResult, TableRecord,
};
use crate::system_tables::SystemSchemaProvider;
use crate::wasm_udf::data_types::{
    CreateFunctionDataType, CreateFunctionDetails, CreateFunctionLanguage,
    CreateFunctionVolatility,
};
use crate::{
    provider::{SeafowlDatabase, SeafowlSchema},
    repository::interface::{
        AllDatabaseColumnsResult, AllDatabaseFunctionsResult, CollectionId,
        CollectionRecord, DatabaseId, Error as RepositoryError, FunctionId, Repository,
        TableId, TableVersionId, TableVersionsResult,
    },
};

pub const DEFAULT_DB: &str = "default";
pub const DEFAULT_SCHEMA: &str = "public";
pub const STAGING_SCHEMA: &str = "staging";

#[derive(Debug)]
pub enum Error {
    CatalogDoesNotExist { name: String },
    SchemaDoesNotExist { name: String },
    TableDoesNotExist { name: String },
    TableUuidDoesNotExist { uuid: Uuid },
    TableAlreadyExists { name: String },
    CatalogAlreadyExists { name: String },
    SchemaAlreadyExists { name: String },
    FunctionAlreadyExists { name: String },
    FunctionDeserializationError { reason: String },
    FunctionNotFound { names: String },
    // Creating a table in / dropping the staging schema
    UsedStagingSchema,
    SqlxError(sqlx::Error),
}

// TODO janky, we want to:
//  - use the ? operator to avoid a lot of map_err
//  - but there are 2 distinct error types, so we have to be able to convert them into a single type
//  - don't want to impl From<serde_json::Error> for Error  (since serde parse errors
//    might not just be for FunctionDeserializationError)
//
//  Currently, we have a struct that we automatically convert both errors into (storing their messages)
//  and then use one map_err to make the final Error::FunctionDeserializationError.
//
//  - could use Box<dyn Error>?
//  - should maybe avoid just passing the to_string() of the error reason, but this is for internal
//    use right now anyway (we made a mistake serializing the function into the DB, it's our fault)

struct CreateFunctionError {
    message: String,
}

impl From<strum::ParseError> for CreateFunctionError {
    fn from(val: strum::ParseError) -> Self {
        Self {
            message: val.to_string(),
        }
    }
}

impl From<serde_json::Error> for CreateFunctionError {
    fn from(val: serde_json::Error) -> Self {
        Self {
            message: val.to_string(),
        }
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Implement a global converter into a DataFusionError from the catalog error type.
/// These might be raised from different parts of query execution and in different contexts,
/// but we want roughly the same message in each case anyway, so we can take advantage of
/// the ? operator and automatic error conversion.
impl From<Error> for DataFusionError {
    fn from(val: Error) -> Self {
        match val {
            Error::CatalogDoesNotExist { name } => {
                DataFusionError::Plan(format!("Database {name:?} doesn't exist"))
            }
            Error::SchemaDoesNotExist { name } => {
                DataFusionError::Plan(format!("Schema {name:?} doesn't exist"))
            }
            Error::TableDoesNotExist { name } => {
                DataFusionError::Plan(format!("Table {name:?} doesn't exist"))
            }
            Error::TableUuidDoesNotExist { uuid } => {
                DataFusionError::Plan(format!("Table with UUID {uuid} doesn't exist"))
            }
            Error::FunctionDeserializationError { reason } => DataFusionError::Internal(
                format!("Error deserializing function: {reason:?}"),
            ),

            // Errors that are the user's fault.

            // Even though these are "execution" errors, we raise them from the plan stage,
            // where we manipulate data in the catalog because that's the only chance we get at
            // being async, so we follow DataFusion's convention and return these as Plan errors.
            Error::TableAlreadyExists { name } => {
                DataFusionError::Plan(format!("Table {name:?} already exists"))
            }
            Error::CatalogAlreadyExists { name } => {
                DataFusionError::Plan(format!("Database {name:?} already exists"))
            }
            Error::SchemaAlreadyExists { name } => {
                DataFusionError::Plan(format!("Schema {name:?} already exists"))
            }
            Error::FunctionAlreadyExists { name } => {
                DataFusionError::Plan(format!("Function {name:?} already exists"))
            }
            Error::FunctionNotFound { names } => {
                DataFusionError::Plan(format!("Function {names:?} not found"))
            }
            Error::UsedStagingSchema => DataFusionError::Plan(
                "The staging schema can only be referenced via CREATE EXTERNAL TABLE"
                    .to_string(),
            ),
            // Miscellaneous sqlx error. We want to log it but it's not worth showing to the user.
            Error::SqlxError(e) => {
                DataFusionError::Plan(format!("Internal SQL error: {:?}", e.to_string()))
            }
        }
    }
}

#[cfg_attr(test, automock)]
#[async_trait]
pub trait TableCatalog: Sync + Send {
    async fn load_database(&self, name: &str) -> Result<SeafowlDatabase>;
    async fn get_catalog(&self, name: &str) -> Result<DatabaseRecord, Error>;

    async fn list_catalogs(&self) -> Result<Vec<DatabaseRecord>, Error>;

    async fn get_schema(
        &self,
        catalog_name: &str,
        schema_name: &str,
    ) -> Result<CollectionRecord, Error>;

    async fn get_table(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<TableRecord, Error>;

    async fn create_catalog(&self, catalog_name: &str) -> Result<DatabaseId>;

    async fn create_schema(
        &self,
        catalog_name: &str,
        schema_name: &str,
    ) -> Result<CollectionId>;

    async fn create_table(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        schema: &Schema,
        uuid: Uuid,
    ) -> Result<(TableId, TableVersionId)>;

    async fn delete_old_table_versions(
        &self,
        catalog_name: &str,
        collection_name: &str,
        table_name: &str,
    ) -> Result<u64, Error>;

    async fn create_new_table_version(
        &self,
        uuid: Uuid,
        version: i64,
    ) -> Result<TableVersionId>;

    async fn get_all_table_versions(
        &self,
        catalog_name: &str,
        table_names: Option<Vec<String>>,
    ) -> Result<Vec<TableVersionsResult>>;

    async fn move_table(
        &self,
        old_catalog_name: &str,
        old_schema_name: &str,
        old_table_name: &str,
        new_catalog_name: &str,
        new_schema_name: &str,
        new_table_name: &str,
    ) -> Result<()>;

    async fn drop_table(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<()>;

    async fn delete_schema(&self, catalog_name: &str, schema_name: &str) -> Result<()>;

    async fn delete_catalog(&self, name: &str) -> Result<()>;

    async fn get_dropped_tables(
        &self,
        catalog_name: Option<String>,
    ) -> Result<Vec<DroppedTablesResult>>;

    async fn update_dropped_table(
        &self,
        uuid: Uuid,
        deletion_status: DroppedTableDeletionStatus,
    ) -> Result<(), Error>;

    async fn delete_dropped_table(&self, uuid: Uuid) -> Result<()>;
}

#[cfg_attr(test, automock)]
#[async_trait]
pub trait FunctionCatalog: Sync + Send {
    async fn create_function(
        &self,
        catalog_name: &str,
        function_name: &str,
        or_replace: bool,
        details: &CreateFunctionDetails,
    ) -> Result<FunctionId>;

    async fn get_all_functions_in_database(
        &self,
        catalog_name: &str,
    ) -> Result<Vec<SeafowlFunction>>;

    async fn drop_function(
        &self,
        catalog_name: &str,
        if_exists: bool,
        func_names: &[String],
    ) -> Result<()>;
}

#[derive(Clone)]
pub struct DefaultCatalog {
    repository: Arc<dyn Repository>,

    // DataFusion's in-memory schema provider for staging external tables
    staging_schema: Arc<MemorySchemaProvider>,
    object_store: Arc<InternalObjectStore>,
}

impl DefaultCatalog {
    pub fn new(
        repository: Arc<dyn Repository>,
        object_store: Arc<InternalObjectStore>,
    ) -> Self {
        let staging_schema = Arc::new(MemorySchemaProvider::new());
        Self {
            repository,
            staging_schema,
            object_store,
        }
    }

    fn to_sqlx_error(error: RepositoryError) -> Error {
        Error::SqlxError(match error {
            RepositoryError::UniqueConstraintViolation(e) => e,
            RepositoryError::FKConstraintViolation(e) => e,
            RepositoryError::SqlxError(e) => e,
        })
    }

    fn build_table(
        &self,
        table_name: &str,
        table_uuid: Uuid,
    ) -> (Arc<str>, Arc<dyn TableProvider>) {
        // Build a delta table but don't load it yet; we'll do that only for tables that are
        // actually referenced in a statement, via the async `table` method of the schema provider.
        // TODO: this means that any `information_schema.columns` query will serially load all
        // delta tables present in the database. The real fix for this is to make DF use `TableSource`
        // for the information schema, and then implement `TableSource` for `DeltaTable` in delta-rs.
        let table_log_store = self.object_store.get_log_store(table_uuid);

        let table = DeltaTable::new(table_log_store, Default::default());
        (Arc::from(table_name.to_string()), Arc::new(table) as _)
    }

    fn build_schema<'a, I>(
        &self,
        collection_name: &str,
        collection_columns: I,
    ) -> (Arc<str>, Arc<SeafowlSchema>)
    where
        I: Iterator<Item = &'a AllDatabaseColumnsResult>,
    {
        let tables = collection_columns
            .filter_map(|col| {
                if let Some(table_name) = &col.table_name
                    && let Some(table_uuid) = col.table_uuid
                {
                    Some(self.build_table(table_name, table_uuid))
                } else {
                    None
                }
            })
            .collect::<HashMap<_, _>>();

        (
            Arc::from(collection_name.to_string()),
            Arc::new(SeafowlSchema {
                name: Arc::from(collection_name.to_string()),
                tables: RwLock::new(tables),
            }),
        )
    }
}

#[async_trait]
impl TableCatalog for DefaultCatalog {
    async fn load_database(&self, name: &str) -> Result<SeafowlDatabase> {
        let all_columns = self
            .repository
            .get_all_columns_in_database(name)
            .await
            .map_err(Self::to_sqlx_error)?;

        // NB we can't distinguish between a database without tables and a database
        // that doesn't exist at all due to our query.

        // Turn the list of all collections, tables and their columns into a nested map.

        let schemas: HashMap<Arc<str>, Arc<SeafowlSchema>> = all_columns
            .iter()
            .group_by(|col| &col.collection_name)
            .into_iter()
            .map(|(cn, cc)| self.build_schema(cn, cc))
            .collect();

        // TODO load the database name too
        let name: Arc<str> = Arc::from(DEFAULT_DB);

        Ok(SeafowlDatabase {
            name: name.clone(),
            schemas,
            staging_schema: self.staging_schema.clone(),
            system_schema: Arc::new(SystemSchemaProvider::new(
                name,
                Arc::new(self.clone()),
            )),
        })
    }

    async fn create_table(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        schema: &Schema,
        uuid: Uuid,
    ) -> Result<(TableId, TableVersionId)> {
        let collection = self.get_schema(catalog_name, schema_name).await?;

        self.repository
            .create_table(collection.id, table_name, schema, uuid)
            .await
            .map_err(|e| match e {
                RepositoryError::UniqueConstraintViolation(_) => {
                    Error::TableAlreadyExists {
                        name: table_name.to_string(),
                    }
                }
                RepositoryError::FKConstraintViolation(_) => Error::SchemaDoesNotExist {
                    name: schema_name.to_string(),
                },
                RepositoryError::SqlxError(e) => Error::SqlxError(e),
            })
    }

    async fn delete_old_table_versions(
        &self,
        catalog_name: &str,
        collection_name: &str,
        table_name: &str,
    ) -> Result<u64, Error> {
        let table = self
            .get_table(catalog_name, collection_name, table_name)
            .await?;

        self.repository
            .delete_old_table_versions(table.id)
            .await
            .map_err(Self::to_sqlx_error)
    }

    async fn get_catalog(&self, name: &str) -> Result<DatabaseRecord> {
        match self.repository.get_database(name).await {
            Ok(database) => Ok(database),
            Err(RepositoryError::SqlxError(sqlx::error::Error::RowNotFound)) => {
                Err(Error::CatalogDoesNotExist {
                    name: name.to_string(),
                })
            }
            Err(e) => Err(Self::to_sqlx_error(e)),
        }
    }

    async fn list_catalogs(&self) -> Result<Vec<DatabaseRecord>, Error> {
        match self.repository.list_databases().await {
            Ok(databases) => Ok(databases),
            Err(e) => Err(Self::to_sqlx_error(e)),
        }
    }

    async fn get_schema(
        &self,
        catalog_name: &str,
        schema_name: &str,
    ) -> Result<CollectionRecord> {
        if schema_name == STAGING_SCHEMA {
            return Err(Error::UsedStagingSchema);
        }

        match self
            .repository
            .get_collection(catalog_name, schema_name)
            .await
        {
            Ok(schema) => Ok(schema),
            Err(RepositoryError::SqlxError(sqlx::error::Error::RowNotFound)) => {
                Err(Error::SchemaDoesNotExist {
                    name: schema_name.to_string(),
                })
            }
            Err(e) => Err(Self::to_sqlx_error(e)),
        }
    }

    async fn get_table(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<TableRecord> {
        match self
            .repository
            .get_table(catalog_name, schema_name, table_name)
            .await
        {
            Ok(table) => Ok(table),
            Err(RepositoryError::SqlxError(sqlx::error::Error::RowNotFound)) => {
                Err(Error::TableDoesNotExist {
                    name: table_name.to_string(),
                })
            }
            Err(e) => Err(Self::to_sqlx_error(e)),
        }
    }

    async fn create_catalog(&self, catalog_name: &str) -> Result<DatabaseId> {
        self.repository
            .create_database(catalog_name)
            .await
            .map_err(|e| match e {
                RepositoryError::UniqueConstraintViolation(_) => {
                    Error::CatalogAlreadyExists {
                        name: catalog_name.to_string(),
                    }
                }
                _ => Self::to_sqlx_error(e),
            })
    }

    async fn create_schema(
        &self,
        catalog_name: &str,
        schema_name: &str,
    ) -> Result<CollectionId> {
        if schema_name == STAGING_SCHEMA {
            return Err(Error::UsedStagingSchema);
        }

        let database = self.get_catalog(catalog_name).await?;

        self.repository
            .create_collection(database.id, schema_name)
            .await
            .map_err(|e| match e {
                RepositoryError::UniqueConstraintViolation(_) => {
                    Error::SchemaAlreadyExists {
                        name: schema_name.to_string(),
                    }
                }
                _ => Self::to_sqlx_error(e),
            })
    }

    async fn create_new_table_version(
        &self,
        uuid: Uuid,
        version: i64,
    ) -> Result<TableVersionId> {
        self.repository
            .create_new_table_version(uuid, version)
            .await
            .map_err(|e| match e {
                RepositoryError::SqlxError(sqlx::error::Error::RowNotFound) => {
                    Error::TableUuidDoesNotExist { uuid }
                }
                _ => Self::to_sqlx_error(e),
            })
    }

    async fn get_all_table_versions(
        &self,
        catalog_name: &str,
        table_names: Option<Vec<String>>,
    ) -> Result<Vec<TableVersionsResult>> {
        self.repository
            .get_all_table_versions(catalog_name, table_names)
            .await
            .map_err(Self::to_sqlx_error)
    }

    async fn move_table(
        &self,
        old_catalog_name: &str,
        old_schema_name: &str,
        old_table_name: &str,
        _new_catalog_name: &str, // For now we don't support moving across catalogs
        new_schema_name: &str,
        new_table_name: &str,
    ) -> Result<()> {
        let table = self
            .get_table(old_catalog_name, old_schema_name, old_table_name)
            .await?;
        let new_schema_id = if new_schema_name != old_schema_name {
            let schema = self.get_schema(old_catalog_name, new_schema_name).await?;
            Some(schema.id)
        } else {
            None
        };

        self.repository
            .move_table(table.id, new_table_name, new_schema_id)
            .await
            .map_err(|e| match e {
                RepositoryError::FKConstraintViolation(_) => {
                    // We only FK on collection_id, so this will be Some
                    Error::SchemaDoesNotExist {
                        name: new_schema_name.to_string(),
                    }
                }
                RepositoryError::UniqueConstraintViolation(_) => {
                    Error::TableAlreadyExists {
                        name: new_table_name.to_string(),
                    }
                }
                RepositoryError::SqlxError(sqlx::error::Error::RowNotFound) => {
                    Error::TableDoesNotExist {
                        name: old_table_name.to_string(),
                    }
                }
                _ => Self::to_sqlx_error(e),
            })
    }

    async fn drop_table(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<()> {
        let table = self
            .get_table(catalog_name, schema_name, table_name)
            .await?;

        self.repository
            .drop_table(table.id)
            .await
            .map_err(|e| match e {
                RepositoryError::SqlxError(sqlx::error::Error::RowNotFound) => {
                    Error::TableDoesNotExist {
                        name: table_name.to_string(),
                    }
                }
                _ => Self::to_sqlx_error(e),
            })
    }

    async fn delete_schema(&self, catalog_name: &str, schema_name: &str) -> Result<()> {
        let schema = self.get_schema(catalog_name, schema_name).await?;

        self.repository
            .drop_collection(schema.id)
            .await
            .map_err(|e| match e {
                RepositoryError::SqlxError(sqlx::error::Error::RowNotFound) => {
                    Error::SchemaDoesNotExist {
                        name: schema_name.to_string(),
                    }
                }
                _ => Self::to_sqlx_error(e),
            })
    }

    async fn delete_catalog(&self, name: &str) -> Result<()> {
        let database = self.get_catalog(name).await?;

        self.repository
            .delete_database(database.id)
            .await
            .map_err(|e| match e {
                RepositoryError::SqlxError(sqlx::error::Error::RowNotFound) => {
                    Error::CatalogDoesNotExist {
                        name: name.to_string(),
                    }
                }
                _ => Self::to_sqlx_error(e),
            })
    }

    async fn get_dropped_tables(
        &self,
        catalog_name: Option<String>,
    ) -> Result<Vec<DroppedTablesResult>> {
        self.repository
            .get_dropped_tables(catalog_name)
            .await
            .map_err(Self::to_sqlx_error)
    }

    async fn update_dropped_table(
        &self,
        uuid: Uuid,
        deletion_status: DroppedTableDeletionStatus,
    ) -> Result<(), Error> {
        self.repository
            .update_dropped_table(uuid, deletion_status)
            .await
            .map_err(|e| match e {
                RepositoryError::SqlxError(sqlx::error::Error::RowNotFound) => {
                    Error::TableUuidDoesNotExist { uuid }
                }
                _ => Self::to_sqlx_error(e),
            })
    }

    async fn delete_dropped_table(&self, uuid: Uuid) -> Result<()> {
        self.repository
            .delete_dropped_table(uuid)
            .await
            .map_err(|e| match e {
                RepositoryError::SqlxError(sqlx::error::Error::RowNotFound) => {
                    Error::TableUuidDoesNotExist { uuid }
                }
                _ => Self::to_sqlx_error(e),
            })
    }
}

impl DefaultCatalog {
    fn parse_create_function_details(
        item: &AllDatabaseFunctionsResult,
    ) -> std::result::Result<CreateFunctionDetails, CreateFunctionError> {
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

#[async_trait]
impl FunctionCatalog for DefaultCatalog {
    async fn create_function(
        &self,
        catalog_name: &str,
        function_name: &str,
        or_replace: bool,
        details: &CreateFunctionDetails,
    ) -> Result<FunctionId> {
        let database = self.get_catalog(catalog_name).await?;

        self.repository
            .create_function(database.id, function_name, or_replace, details)
            .await
            .map_err(|e| match e {
                RepositoryError::FKConstraintViolation(_) => Error::CatalogDoesNotExist {
                    name: catalog_name.to_string(),
                },
                RepositoryError::UniqueConstraintViolation(_) => {
                    Error::FunctionAlreadyExists {
                        name: function_name.to_string(),
                    }
                }
                _ => Self::to_sqlx_error(e),
            })
    }

    async fn get_all_functions_in_database(
        &self,
        catalog_name: &str,
    ) -> Result<Vec<SeafowlFunction>> {
        let database = self.get_catalog(catalog_name).await?;

        let all_functions = self
            .repository
            .get_all_functions_in_database(database.id)
            .await
            .map_err(Self::to_sqlx_error)?;

        all_functions
            .iter()
            .map(|item| {
                Self::parse_create_function_details(item)
                    .map(|details| SeafowlFunction {
                        function_id: item.id,
                        name: item.name.to_owned(),
                        details,
                    })
                    .map_err(|e| Error::FunctionDeserializationError {
                        reason: e.message,
                    })
            })
            .collect::<Result<Vec<SeafowlFunction>>>()
    }

    async fn drop_function(
        &self,
        catalog_name: &str,
        if_exists: bool,
        func_names: &[String],
    ) -> Result<()> {
        let database = self.get_catalog(catalog_name).await?;

        match self.repository.drop_function(database.id, func_names).await {
            Ok(id) => Ok(id),
            Err(RepositoryError::FKConstraintViolation(_)) => {
                Err(Error::CatalogDoesNotExist {
                    name: catalog_name.to_string(),
                })
            }
            Err(RepositoryError::SqlxError(sqlx::error::Error::RowNotFound)) => {
                if if_exists {
                    Ok(())
                } else {
                    Err(Error::FunctionNotFound {
                        names: func_names.join(", "),
                    })
                }
            }
            Err(e) => Err(Self::to_sqlx_error(e)),
        }
    }
}
