use std::str::FromStr;
use std::{collections::HashMap, fmt::Debug, sync::Arc};

use async_trait::async_trait;
use datafusion::catalog::schema::MemorySchemaProvider;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use deltalake::{DeltaDataTypeVersion, DeltaTable};
use itertools::Itertools;
#[cfg(test)]
use mockall::automock;
use parking_lot::RwLock;
use uuid::Uuid;

use crate::object_store::wrapped::InternalObjectStore;
use crate::provider::SeafowlFunction;
use crate::repository::interface::{DroppedTableDeletionStatus, DroppedTablesResult};
use crate::system_tables::SystemSchemaProvider;
use crate::wasm_udf::data_types::{
    CreateFunctionDataType, CreateFunctionDetails, CreateFunctionLanguage,
    CreateFunctionVolatility,
};
use crate::{
    data_types::{CollectionId, DatabaseId, FunctionId, TableId, TableVersionId},
    provider::{SeafowlCollection, SeafowlDatabase},
    repository::interface::{
        AllDatabaseColumnsResult, AllDatabaseFunctionsResult, Error as RepositoryError,
        Repository, TableVersionsResult,
    },
    schema::Schema,
};

pub const DEFAULT_DB: &str = "default";
pub const DEFAULT_SCHEMA: &str = "public";
pub const STAGING_SCHEMA: &str = "staging";

#[derive(Debug)]
pub enum Error {
    DatabaseDoesNotExist { id: DatabaseId },
    CollectionDoesNotExist { id: CollectionId },
    TableDoesNotExist { id: TableId },
    TableUuidDoesNotExist { uuid: Uuid },
    TableAlreadyExists { name: String },
    DatabaseAlreadyExists { name: String },
    CollectionAlreadyExists { name: String },
    FunctionAlreadyExists { name: String },
    FunctionDeserializationError { reason: String },
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
            // These errors are raised by routines that already take an ID instead of
            // a database/schema/table name and so the ID is supposed to be valid. An error
            // in this case is an internal consistency issue.
            Error::DatabaseDoesNotExist { id } => {
                DataFusionError::Internal(format!("Database with ID {id} doesn't exist"))
            }
            Error::CollectionDoesNotExist { id } => {
                DataFusionError::Internal(format!("Schema with ID {id} doesn't exist"))
            }
            Error::TableDoesNotExist { id } => {
                DataFusionError::Internal(format!("Table with ID {id} doesn't exist"))
            }
            Error::TableUuidDoesNotExist { uuid } => {
                DataFusionError::Internal(format!("Table with UUID {uuid} doesn't exist"))
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
            Error::DatabaseAlreadyExists { name } => {
                DataFusionError::Plan(format!("Database {name:?} already exists"))
            }
            Error::CollectionAlreadyExists { name } => {
                DataFusionError::Plan(format!("Schema {name:?} already exists"))
            }
            Error::FunctionAlreadyExists { name } => {
                DataFusionError::Plan(format!("Function {name:?} already exists"))
            }
            Error::UsedStagingSchema => DataFusionError::Plan(
                "The staging schema can only be referenced via CREATE EXTERNAL TABLE"
                    .to_string(),
            ),

            // Miscellaneous sqlx error. We want to log it but it's not worth showing to the user.
            Error::SqlxError(e) => DataFusionError::Internal(format!(
                "Internal SQL error: {:?}",
                e.to_string()
            )),
        }
    }
}

#[cfg_attr(test, automock)]
#[async_trait]
pub trait TableCatalog: Sync + Send {
    async fn load_database(&self, id: DatabaseId) -> Result<SeafowlDatabase>;
    async fn load_database_ids(&self) -> Result<HashMap<String, DatabaseId>>;
    async fn get_database_id_by_name(
        &self,
        database_name: &str,
    ) -> Result<Option<DatabaseId>>;
    async fn get_collection_id_by_name(
        &self,
        database_name: &str,
        collection_name: &str,
    ) -> Result<Option<CollectionId>>;
    async fn get_table_id_by_name(
        &self,
        database_name: &str,
        collection_name: &str,
        table_name: &str,
    ) -> Result<Option<TableId>>;

    async fn create_database(&self, database_name: &str) -> Result<DatabaseId>;

    async fn create_collection(
        &self,
        database_id: DatabaseId,
        collection_name: &str,
    ) -> Result<CollectionId>;

    async fn create_table(
        &self,
        collection_id: CollectionId,
        table_name: &str,
        schema: &Schema,
        uuid: Uuid,
    ) -> Result<(TableId, TableVersionId)>;

    async fn delete_old_table_versions(&self, table_id: TableId) -> Result<u64, Error>;

    async fn create_new_table_version(
        &self,
        uuid: Uuid,
        version: DeltaDataTypeVersion,
    ) -> Result<TableVersionId>;

    async fn get_all_table_versions(
        &self,
        database_name: &str,
        table_names: Option<Vec<String>>,
    ) -> Result<Vec<TableVersionsResult>>;

    async fn move_table(
        &self,
        table_id: TableId,
        new_table_name: &str,
        new_collection_id: Option<CollectionId>,
    ) -> Result<()>;

    async fn drop_table(&self, table_id: TableId) -> Result<()>;

    async fn drop_collection(&self, collection_id: CollectionId) -> Result<()>;

    async fn drop_database(&self, database_id: DatabaseId) -> Result<()>;

    async fn get_dropped_tables(
        &self,
        database_name: &str,
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
        database_id: DatabaseId,
        function_name: &str,
        details: &CreateFunctionDetails,
    ) -> Result<FunctionId>;

    async fn get_all_functions_in_database(
        &self,
        database_id: DatabaseId,
    ) -> Result<Vec<SeafowlFunction>>;
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
        let table_object_store = self.object_store.for_delta_table(table_uuid);

        let table = DeltaTable::new(table_object_store, Default::default());
        (Arc::from(table_name.to_string()), Arc::new(table) as _)
    }

    fn build_collection<'a, I>(
        &self,
        collection_name: &str,
        collection_columns: I,
    ) -> (Arc<str>, Arc<SeafowlCollection>)
    where
        I: Iterator<Item = &'a AllDatabaseColumnsResult>,
    {
        let tables = collection_columns
            .filter(|c| !c.table_legacy)
            .group_by(|col| (&col.table_name, &col.table_uuid))
            .into_iter()
            .map(|((table_name, table_uuid), _)| {
                self.build_table(table_name, *table_uuid)
            })
            .collect::<HashMap<_, _>>();

        (
            Arc::from(collection_name.to_string()),
            Arc::new(SeafowlCollection {
                name: Arc::from(collection_name.to_string()),
                tables: RwLock::new(tables),
            }),
        )
    }
}

#[async_trait]
impl TableCatalog for DefaultCatalog {
    async fn load_database(&self, database_id: DatabaseId) -> Result<SeafowlDatabase> {
        let all_columns = self
            .repository
            .get_all_columns_in_database(database_id, None)
            .await
            .map_err(Self::to_sqlx_error)?;

        // NB we can't distinguish between a database without tables and a database
        // that doesn't exist at all due to our query.

        // Turn the list of all collections, tables and their columns into a nested map.

        let collections: HashMap<Arc<str>, Arc<SeafowlCollection>> = all_columns
            .iter()
            .group_by(|col| &col.collection_name)
            .into_iter()
            .map(|(cn, cc)| self.build_collection(cn, cc))
            .collect();

        // TODO load the database name too
        let name: Arc<str> = Arc::from(DEFAULT_DB);

        Ok(SeafowlDatabase {
            name: name.clone(),
            collections,
            staging_schema: self.staging_schema.clone(),
            system_schema: Arc::new(SystemSchemaProvider::new(
                name,
                Arc::new(self.clone()),
            )),
        })
    }

    async fn load_database_ids(&self) -> Result<HashMap<String, DatabaseId>> {
        let all_db_ids = self
            .repository
            .get_all_database_ids()
            .await
            .map_err(Self::to_sqlx_error)?;

        Ok(HashMap::from_iter(all_db_ids))
    }

    async fn create_table(
        &self,
        collection_id: CollectionId,
        table_name: &str,
        schema: &Schema,
        uuid: Uuid,
    ) -> Result<(TableId, TableVersionId)> {
        self.repository
            .create_table(collection_id, table_name, schema, uuid)
            .await
            .map_err(|e| match e {
                RepositoryError::UniqueConstraintViolation(_) => {
                    Error::TableAlreadyExists {
                        name: table_name.to_string(),
                    }
                }
                RepositoryError::FKConstraintViolation(_) => {
                    Error::CollectionDoesNotExist { id: collection_id }
                }
                RepositoryError::SqlxError(e) => Error::SqlxError(e),
            })
    }

    async fn delete_old_table_versions(&self, table_id: TableId) -> Result<u64, Error> {
        self.repository
            .delete_old_table_versions(table_id)
            .await
            .map_err(Self::to_sqlx_error)
    }

    async fn get_collection_id_by_name(
        &self,
        database_name: &str,
        collection_name: &str,
    ) -> Result<Option<CollectionId>> {
        if collection_name == STAGING_SCHEMA {
            return Err(Error::UsedStagingSchema);
        }

        match self
            .repository
            .get_collection_id_by_name(database_name, collection_name)
            .await
        {
            Ok(id) => Ok(Some(id)),
            Err(RepositoryError::SqlxError(sqlx::error::Error::RowNotFound)) => Ok(None),
            Err(e) => Err(Self::to_sqlx_error(e)),
        }
    }

    async fn get_database_id_by_name(
        &self,
        database_name: &str,
    ) -> Result<Option<DatabaseId>> {
        match self.repository.get_database_id_by_name(database_name).await {
            Ok(id) => Ok(Some(id)),
            Err(RepositoryError::SqlxError(sqlx::error::Error::RowNotFound)) => Ok(None),
            Err(e) => Err(Self::to_sqlx_error(e)),
        }
    }

    async fn get_table_id_by_name(
        &self,
        database_name: &str,
        collection_name: &str,
        table_name: &str,
    ) -> Result<Option<TableId>> {
        if collection_name == STAGING_SCHEMA {
            return Err(Error::UsedStagingSchema);
        }

        match self
            .repository
            .get_table_id_by_name(database_name, collection_name, table_name)
            .await
        {
            Ok(id) => Ok(Some(id)),
            Err(RepositoryError::SqlxError(sqlx::error::Error::RowNotFound)) => Ok(None),
            Err(e) => Err(Self::to_sqlx_error(e)),
        }
    }

    async fn create_database(&self, database_name: &str) -> Result<DatabaseId> {
        self.repository
            .create_database(database_name)
            .await
            .map_err(|e| match e {
                RepositoryError::UniqueConstraintViolation(_) => {
                    Error::DatabaseAlreadyExists {
                        name: database_name.to_string(),
                    }
                }
                _ => Self::to_sqlx_error(e),
            })
    }

    async fn create_collection(
        &self,
        database_id: DatabaseId,
        collection_name: &str,
    ) -> Result<CollectionId> {
        if collection_name == STAGING_SCHEMA {
            return Err(Error::UsedStagingSchema);
        }

        self.repository
            .create_collection(database_id, collection_name)
            .await
            .map_err(|e| match e {
                RepositoryError::UniqueConstraintViolation(_) => {
                    Error::CollectionAlreadyExists {
                        name: collection_name.to_string(),
                    }
                }
                _ => Self::to_sqlx_error(e),
            })
    }

    async fn create_new_table_version(
        &self,
        uuid: Uuid,
        version: DeltaDataTypeVersion,
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
        database_name: &str,
        table_names: Option<Vec<String>>,
    ) -> Result<Vec<TableVersionsResult>> {
        self.repository
            .get_all_table_versions(database_name, table_names)
            .await
            .map_err(Self::to_sqlx_error)
    }

    async fn move_table(
        &self,
        table_id: TableId,
        new_table_name: &str,
        new_collection_id: Option<CollectionId>,
    ) -> Result<()> {
        self.repository
            .move_table(table_id, new_table_name, new_collection_id)
            .await
            .map_err(|e| match e {
                RepositoryError::FKConstraintViolation(_) => {
                    // We only FK on collection_id, so this will be Some
                    Error::CollectionDoesNotExist {
                        id: new_collection_id.unwrap(),
                    }
                }
                RepositoryError::UniqueConstraintViolation(_) => {
                    Error::TableAlreadyExists {
                        name: new_table_name.to_string(),
                    }
                }
                RepositoryError::SqlxError(sqlx::error::Error::RowNotFound) => {
                    Error::TableDoesNotExist { id: table_id }
                }
                _ => Self::to_sqlx_error(e),
            })
    }

    async fn drop_table(&self, table_id: TableId) -> Result<()> {
        self.repository
            .drop_table(table_id)
            .await
            .map_err(|e| match e {
                RepositoryError::SqlxError(sqlx::error::Error::RowNotFound) => {
                    Error::TableDoesNotExist { id: table_id }
                }
                _ => Self::to_sqlx_error(e),
            })
    }

    async fn drop_collection(&self, collection_id: CollectionId) -> Result<()> {
        self.repository
            .drop_collection(collection_id)
            .await
            .map_err(|e| match e {
                RepositoryError::SqlxError(sqlx::error::Error::RowNotFound) => {
                    Error::CollectionDoesNotExist { id: collection_id }
                }
                _ => Self::to_sqlx_error(e),
            })
    }

    async fn drop_database(&self, database_id: DatabaseId) -> Result<()> {
        self.repository
            .drop_database(database_id)
            .await
            .map_err(|e| match e {
                RepositoryError::SqlxError(sqlx::error::Error::RowNotFound) => {
                    Error::DatabaseDoesNotExist { id: database_id }
                }
                _ => Self::to_sqlx_error(e),
            })
    }

    async fn get_dropped_tables(
        &self,
        database_name: &str,
    ) -> Result<Vec<DroppedTablesResult>> {
        self.repository
            .get_dropped_tables(database_name)
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
        database_id: DatabaseId,
        function_name: &str,
        details: &CreateFunctionDetails,
    ) -> Result<FunctionId> {
        self.repository
            .create_function(database_id, function_name, details)
            .await
            .map_err(|e| match e {
                RepositoryError::FKConstraintViolation(_) => {
                    Error::DatabaseDoesNotExist { id: database_id }
                }
                RepositoryError::UniqueConstraintViolation(_) => {
                    // TODO overwrite function defns instead?
                    Error::FunctionAlreadyExists {
                        name: function_name.to_string(),
                    }
                }
                _ => Self::to_sqlx_error(e),
            })
    }

    async fn get_all_functions_in_database(
        &self,
        database_id: DatabaseId,
    ) -> Result<Vec<SeafowlFunction>> {
        let all_functions = self
            .repository
            .get_all_functions_in_database(database_id)
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
}
