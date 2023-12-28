use crate::repository::interface::{
    AllDatabaseFunctionsResult, CollectionRecord, DatabaseRecord,
    DroppedTableDeletionStatus, DroppedTablesResult, TableId, TableRecord,
    TableVersionId, TableVersionsResult,
};
use crate::wasm_udf::data_types::CreateFunctionDetails;
use arrow_schema::Schema;
use async_trait::async_trait;
use floc::schema::ListSchemaResponse;
use uuid::Uuid;

pub(crate) mod metastore;
mod repository;

pub const DEFAULT_DB: &str = "default";
pub const DEFAULT_SCHEMA: &str = "public";
pub const STAGING_SCHEMA: &str = "staging";

#[derive(Debug)]
pub enum CatalogError {
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
    async fn create(&self, name: &str) -> CatalogResult<()>;

    async fn get(&self, name: &str) -> CatalogResult<DatabaseRecord>;

    async fn delete(&self, name: &str) -> CatalogResult<()>;
}

#[async_trait]
pub trait SchemaStore: Sync + Send {
    async fn create(&self, catalog_name: &str, schema_name: &str) -> CatalogResult<()>;

    async fn list(&self, catalog_name: &str) -> CatalogResult<ListSchemaResponse>;

    async fn get(
        &self,
        catalog_name: &str,
        schema_name: &str,
    ) -> CatalogResult<CollectionRecord>;

    async fn delete(&self, catalog_name: &str, schema_name: &str) -> CatalogResult<()>;
}

#[async_trait]
pub trait TableStore: Sync + Send {
    async fn create(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        schema: &Schema,
        uuid: Uuid,
    ) -> CatalogResult<(TableId, TableVersionId)>;

    async fn get(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> CatalogResult<TableRecord>;

    async fn create_new_version(
        &self,
        uuid: Uuid,
        version: i64,
    ) -> CatalogResult<TableVersionId>;

    async fn delete_old_versions(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> CatalogResult<u64>;

    async fn get_all_versions(
        &self,
        catalog_name: &str,
        table_names: Option<Vec<String>>,
    ) -> CatalogResult<Vec<TableVersionsResult>>;

    async fn update(
        &self,
        old_catalog_name: &str,
        old_schema_name: &str,
        old_table_name: &str,
        new_catalog_name: &str,
        new_schema_name: &str,
        new_table_name: &str,
    ) -> CatalogResult<()>;

    async fn delete(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> CatalogResult<()>;

    async fn get_dropped_tables(
        &self,
        catalog_name: Option<String>,
    ) -> CatalogResult<Vec<DroppedTablesResult>>;

    async fn update_dropped_table(
        &self,
        uuid: Uuid,
        deletion_status: DroppedTableDeletionStatus,
    ) -> CatalogResult<()>;

    async fn delete_dropped_table(&self, uuid: Uuid) -> CatalogResult<()>;
}

#[async_trait]
pub trait FunctionStore: Sync + Send {
    async fn create(
        &self,
        catalog_name: &str,
        function_name: &str,
        or_replace: bool,
        details: &CreateFunctionDetails,
    ) -> CatalogResult<()>;

    async fn list(
        &self,
        catalog_name: &str,
    ) -> CatalogResult<Vec<AllDatabaseFunctionsResult>>;

    async fn delete(
        &self,
        catalog_name: &str,
        if_exists: bool,
        func_names: &[String],
    ) -> CatalogResult<()>;
}
