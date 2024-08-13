use crate::repository::interface::{
    AllDatabaseFunctionsResult, CollectionRecord, DatabaseRecord,
    DroppedTableDeletionStatus, DroppedTablesResult, TableId, TableRecord,
    TableVersionId, TableVersionsResult,
};
use crate::wasm_udf::data_types::CreateFunctionDetails;
use arrow_schema::Schema;
use async_trait::async_trait;
use clade::schema::ListSchemaResponse;
use datafusion_common::DataFusionError;
use tonic::Status;
use uuid::Uuid;

mod empty;
pub mod external;
pub mod memory;
pub mod metastore;
mod repository;

pub const DEFAULT_DB: &str = "default";
pub const DEFAULT_SCHEMA: &str = "public";
pub const STAGING_SCHEMA: &str = "staging";

#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    #[error("{reason}")]
    Generic { reason: String },

    // Catalog errors
    #[error("Catalog {name:?} doesn't exist")]
    CatalogDoesNotExist { name: String },

    #[error("Catalog {name:?} already exists")]
    CatalogAlreadyExists { name: String },

    // Schema errors
    #[error("Schema {name:?} doesn't exist")]
    SchemaDoesNotExist { name: String },

    #[error("Schema {name:?} already exists")]
    SchemaAlreadyExists { name: String },

    // Table errors
    #[error("Table {name:?} doesn't exist")]
    TableDoesNotExist { name: String },

    #[error("Table with UUID {uuid} doesn't exist")]
    TableUuidDoesNotExist { uuid: Uuid },

    #[error("Table {name:?} already exists")]
    TableAlreadyExists { name: String },

    // Function errors
    #[error("Function {name:?} already exists")]
    FunctionAlreadyExists { name: String },

    #[error("Error deserializing function: {reason}")]
    FunctionDeserializationError { reason: String },

    #[error("Function {names:?} not found")]
    FunctionNotFound { names: String },

    // Creating a table in / dropping the staging schema
    #[error("The staging schema can only be referenced via CREATE EXTERNAL TABLE")]
    UsedStagingSchema,

    #[error("Catalog method not implemented: {reason}")]
    NotImplemented { reason: String },

    // Metastore implementation errors
    #[error(transparent)]
    ObjectStoreError(#[from] object_store::Error),

    #[error("Internal SQL error: {0:?}")]
    SqlxError(sqlx::Error),

    #[error("Failed parsing JSON: {0}")]
    SerdeJsonError(#[from] serde_json::Error),

    #[error(transparent)]
    TonicStatus(#[from] Status),

    #[error("Failed parsing URL: {0}")]
    UrlParseError(#[from] url::ParseError),

    #[error(
        "Object store not configured and no object store for table {name:?} passed in"
    )]
    NoTableStoreInInlineMetastore { name: String },

    #[error("No inline metastore passed in")]
    NoInlineMetastore,
}

/// Implement a global converter into a DataFusionError from the catalog error type.
/// These might be raised from different parts of query execution and in different contexts,
/// but we want roughly the same message in each case anyway, so we can take advantage of
/// the ? operator and automatic error conversion.
impl From<CatalogError> for DataFusionError {
    fn from(val: CatalogError) -> Self {
        match val {
            CatalogError::NotImplemented { reason } => {
                DataFusionError::NotImplemented(reason)
            }
            _ => DataFusionError::Plan(val.to_string()),
        }
    }
}

fn not_impl<T>() -> CatalogResult<T> {
    Err(CatalogError::NotImplemented {
        reason: "Metastore method not supported".to_string(),
    })
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

pub(super) struct CreateFunctionError {
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

pub type CatalogResult<T> = Result<T, CatalogError>;

#[async_trait]
pub trait CatalogStore: Sync + Send {
    async fn create(&self, _name: &str) -> CatalogResult<()> {
        not_impl()
    }

    async fn get(&self, _name: &str) -> CatalogResult<DatabaseRecord> {
        not_impl()
    }

    async fn delete(&self, _name: &str) -> CatalogResult<()> {
        not_impl()
    }
}

#[async_trait]
pub trait SchemaStore: Sync + Send {
    async fn create(&self, _catalog_name: &str, _schema_name: &str) -> CatalogResult<()> {
        not_impl()
    }

    async fn list(&self, _catalog_name: &str) -> CatalogResult<ListSchemaResponse> {
        not_impl()
    }

    async fn get(
        &self,
        _catalog_name: &str,
        _schema_name: &str,
    ) -> CatalogResult<CollectionRecord> {
        not_impl()
    }

    async fn delete(&self, _catalog_name: &str, _schema_name: &str) -> CatalogResult<()> {
        not_impl()
    }
}

#[async_trait]
pub trait TableStore: Sync + Send {
    async fn create(
        &self,
        _catalog_name: &str,
        _schema_name: &str,
        _table_name: &str,
        _schema: &Schema,
        _uuid: Uuid,
    ) -> CatalogResult<(TableId, TableVersionId)> {
        not_impl()
    }

    async fn get(
        &self,
        _catalog_name: &str,
        _schema_name: &str,
        _table_name: &str,
    ) -> CatalogResult<TableRecord> {
        not_impl()
    }

    async fn create_new_version(
        &self,
        _uuid: Uuid,
        _version: i64,
    ) -> CatalogResult<TableVersionId> {
        not_impl()
    }

    async fn delete_old_versions(
        &self,
        _catalog_name: &str,
        _schema_name: &str,
        _table_name: &str,
    ) -> CatalogResult<u64> {
        not_impl()
    }

    async fn get_all_versions(
        &self,
        _catalog_name: &str,
        _table_names: Option<Vec<String>>,
    ) -> CatalogResult<Vec<TableVersionsResult>> {
        not_impl()
    }

    async fn update(
        &self,
        _old_catalog_name: &str,
        _old_schema_name: &str,
        _old_table_name: &str,
        _new_catalog_name: &str,
        _new_schema_name: &str,
        _new_table_name: &str,
    ) -> CatalogResult<()> {
        not_impl()
    }

    async fn delete(
        &self,
        _catalog_name: &str,
        _schema_name: &str,
        _table_name: &str,
    ) -> CatalogResult<()> {
        not_impl()
    }

    async fn get_dropped_tables(
        &self,
        _catalog_name: Option<String>,
    ) -> CatalogResult<Vec<DroppedTablesResult>> {
        not_impl()
    }

    async fn update_dropped_table(
        &self,
        _uuid: Uuid,
        _deletion_status: DroppedTableDeletionStatus,
    ) -> CatalogResult<()> {
        not_impl()
    }

    async fn delete_dropped_table(&self, _uuid: Uuid) -> CatalogResult<()> {
        not_impl()
    }
}

#[async_trait]
pub trait FunctionStore: Sync + Send {
    async fn create(
        &self,
        _catalog_name: &str,
        _function_name: &str,
        _or_replace: bool,
        _details: &CreateFunctionDetails,
    ) -> CatalogResult<()> {
        not_impl()
    }

    async fn list(
        &self,
        _catalog_name: &str,
    ) -> CatalogResult<Vec<AllDatabaseFunctionsResult>> {
        not_impl()
    }

    async fn delete(
        &self,
        _catalog_name: &str,
        _if_exists: bool,
        _func_names: &[String],
    ) -> CatalogResult<()> {
        not_impl()
    }
}
