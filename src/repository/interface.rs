use std::fmt::Debug;
use std::str::FromStr;

use async_trait::async_trait;
use deltalake::DeltaDataTypeVersion;
use strum::ParseError;
use strum_macros::{Display, EnumString};
use uuid::Uuid;

use crate::wasm_udf::data_types::CreateFunctionDetails;
use crate::{
    data_types::{
        CollectionId, DatabaseId, FunctionId, TableId, TableVersionId, Timestamp,
    },
    schema::Schema,
};

#[derive(sqlx::FromRow, Debug, PartialEq, Eq)]
pub struct AllDatabaseColumnsResult {
    pub database_name: String,
    pub collection_name: String,
    pub table_name: String,
    pub table_id: TableId,
    pub table_uuid: Uuid,
    pub table_legacy: bool,
    pub table_version_id: TableVersionId,
    pub column_name: String,
    pub column_type: String,
}

#[derive(sqlx::FromRow, Debug, PartialEq, Eq)]
pub struct TableVersionsResult {
    pub database_name: String,
    pub collection_name: String,
    pub table_name: String,
    pub table_version_id: TableVersionId,
    pub version: DeltaDataTypeVersion,
    pub table_legacy: bool,
    pub creation_time: Timestamp,
}

#[derive(sqlx::FromRow, Debug, PartialEq, Eq)]
pub struct TablePartitionsResult {
    pub database_name: String,
    pub collection_name: String,
    pub table_name: String,
    pub table_legacy: bool,
    pub table_version_id: TableVersionId,
    pub table_partition_id: Option<i64>,
    pub object_storage_id: Option<String>,
    pub row_count: Option<i32>,
}

#[derive(sqlx::FromRow, Clone, Debug, PartialEq, Eq)]
pub struct DroppedTablesResult {
    pub database_name: String,
    pub collection_name: String,
    pub table_name: String,
    pub uuid: Uuid,
    #[sqlx(try_from = "String")]
    pub deletion_status: DroppedTableDeletionStatus,
    pub drop_time: Timestamp,
}

#[derive(sqlx::Type, Debug, PartialEq, Eq, Clone, Copy, Display, EnumString)]
#[strum(serialize_all = "UPPERCASE")]
pub enum DroppedTableDeletionStatus {
    Pending,
    Retry,
    Failed,
}

// Not compatible with SQL type `VARCHAR` without this
impl TryFrom<String> for DroppedTableDeletionStatus {
    type Error = ParseError;
    fn try_from(value: String) -> Result<Self, ParseError> {
        DroppedTableDeletionStatus::from_str(value.as_str())
    }
}

#[derive(sqlx::FromRow, Debug, PartialEq, Eq)]
pub struct AllTablePartitionColumnsResult {
    pub table_partition_id: i64,
    pub object_storage_id: String,
    pub column_name: String,
    pub column_type: String,
    pub row_count: i32,
    pub min_value: Option<Vec<u8>>,
    pub max_value: Option<Vec<u8>>,
    pub null_count: Option<i32>,
}

#[derive(sqlx::FromRow, Debug, PartialEq, Eq)]
pub struct AllDatabaseFunctionsResult {
    pub name: String,
    pub id: FunctionId,
    pub entrypoint: String,
    pub language: String,
    pub input_types: String,
    pub return_type: String,
    pub data: String,
    pub volatility: String,
}

/// Wrapper for conversion of database-specific error codes into actual errors
#[derive(Debug)]
pub enum Error {
    UniqueConstraintViolation(sqlx::Error),
    FKConstraintViolation(sqlx::Error),

    // All other errors
    SqlxError(sqlx::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

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
        table_version_ids: Option<Vec<TableVersionId>>,
    ) -> Result<Vec<AllDatabaseColumnsResult>, Error>;

    async fn get_all_table_partition_columns(
        &self,
        table_version_id: TableVersionId,
    ) -> Result<Vec<AllTablePartitionColumnsResult>, Error>;

    async fn get_collection_id_by_name(
        &self,
        database_name: &str,
        collection_name: &str,
    ) -> Result<CollectionId, Error>;

    async fn get_database_id_by_name(
        &self,
        database_name: &str,
    ) -> Result<DatabaseId, Error>;

    async fn get_table_id_by_name(
        &self,
        database_name: &str,
        collection_name: &str,
        table_name: &str,
    ) -> Result<TableId, Error>;

    async fn get_all_database_ids(&self) -> Result<Vec<(String, DatabaseId)>, Error>;

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
        schema: &Schema,
        uuid: Uuid,
    ) -> Result<(TableId, TableVersionId), Error>;

    async fn delete_old_table_versions(&self, table_id: TableId) -> Result<u64, Error>;

    async fn get_orphan_partition_store_ids(&self) -> Result<Vec<String>, Error>;

    async fn delete_partitions(
        &self,
        object_storage_ids: Vec<String>,
    ) -> Result<u64, Error>;

    async fn create_new_table_version(
        &self,
        uuid: Uuid,
        version: DeltaDataTypeVersion,
    ) -> Result<TableVersionId, Error>;

    async fn get_all_table_versions(
        &self,
        database_name: &str,
        table_names: Option<Vec<String>>,
    ) -> Result<Vec<TableVersionsResult>>;

    async fn get_all_table_partitions(
        &self,
        database_name: &str,
    ) -> Result<Vec<TablePartitionsResult>>;

    async fn move_table(
        &self,
        table_id: TableId,
        new_table_name: &str,
        new_collection_id: Option<CollectionId>,
    ) -> Result<(), Error>;

    async fn create_function(
        &self,
        database_id: DatabaseId,
        function_name: &str,
        details: &CreateFunctionDetails,
    ) -> Result<FunctionId, Error>;

    async fn get_all_functions_in_database(
        &self,
        database_id: DatabaseId,
    ) -> Result<Vec<AllDatabaseFunctionsResult>, Error>;

    async fn drop_table(&self, table_id: TableId) -> Result<(), Error>;

    async fn drop_collection(&self, collection_id: CollectionId) -> Result<(), Error>;

    async fn drop_database(&self, database_id: DatabaseId) -> Result<(), Error>;

    async fn insert_dropped_tables(
        &self,
        maybe_table_id: Option<TableId>,
        maybe_collection_id: Option<CollectionId>,
        maybe_database_id: Option<DatabaseId>,
    ) -> Result<(), Error>;

    async fn get_dropped_tables(
        &self,
        database_name: &str,
    ) -> Result<Vec<DroppedTablesResult>>;

    async fn update_dropped_table(
        &self,
        uuid: Uuid,
        deletion_status: DroppedTableDeletionStatus,
    ) -> Result<(), Error>;

    async fn delete_dropped_table(&self, uuid: Uuid) -> Result<(), Error>;
}

#[cfg(test)]
pub mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{
        DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    };

    use crate::wasm_udf::data_types::{
        CreateFunctionDataType, CreateFunctionLanguage, CreateFunctionVolatility,
    };

    use super::*;

    async fn make_database_with_single_table(
        repository: Arc<dyn Repository>,
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
            .create_table(collection_id, "testtable", &schema, Uuid::default())
            .await
            .expect("Error creating table");

        (database_id, collection_id, table_id, table_version_id)
    }

    pub async fn run_generic_repository_tests(repository: Arc<dyn Repository>) {
        test_get_collections_empty(repository.clone()).await;
        let (database_id, table_id, table_version_id) =
            test_create_database_collection_table(repository.clone()).await;
        test_create_functions(repository.clone(), database_id).await;
        test_rename_table(
            repository.clone(),
            database_id,
            table_id,
            table_version_id + 1,
        )
        .await;
        test_error_propagation(repository, table_id).await;
    }

    async fn test_get_collections_empty(repository: Arc<dyn Repository>) {
        assert_eq!(
            repository
                .get_collections_in_database(0)
                .await
                .expect("error getting collections"),
            Vec::<String>::new()
        );
    }

    fn expected(
        version: TableVersionId,
        database_name: String,
        collection_name: String,
        table_name: String,
    ) -> Vec<AllDatabaseColumnsResult> {
        vec![
            AllDatabaseColumnsResult {
                database_name: database_name.clone(),
                collection_name: collection_name.clone(),
                table_name: table_name.clone(),
                table_id: 1,
                table_uuid: Default::default(),
                table_legacy: false,
                table_version_id: version,
                column_name: "date".to_string(),
                column_type: "{\"children\":[],\"name\":\"date\",\"nullable\":false,\"type\":{\"name\":\"date\",\"unit\":\"MILLISECOND\"}}".to_string(),
            },
            AllDatabaseColumnsResult {
                database_name,
                collection_name,
                table_name,
                table_id: 1,
                table_uuid: Default::default(),
                table_legacy: false,
                table_version_id: version,
                column_name: "value".to_string(),
                column_type: "{\"children\":[],\"name\":\"value\",\"nullable\":false,\"type\":{\"name\":\"floatingpoint\",\"precision\":\"DOUBLE\"}}"
                    .to_string(),
            },
        ]
    }

    async fn test_create_database_collection_table(
        repository: Arc<dyn Repository>,
    ) -> (DatabaseId, TableId, TableVersionId) {
        let (database_id, _, table_id, table_version_id) =
            make_database_with_single_table(repository.clone()).await;

        let all_database_ids = repository
            .get_all_database_ids()
            .await
            .expect("Error getting all database ids");

        assert_eq!(all_database_ids, vec![("testdb".to_string(), database_id)]);

        // Test loading all columns

        let all_columns = repository
            .get_all_columns_in_database(database_id, None)
            .await
            .expect("Error getting all columns");

        assert_eq!(
            all_columns,
            expected(
                1,
                "testdb".to_string(),
                "testcol".to_string(),
                "testtable".to_string()
            )
        );

        // Duplicate the table
        let new_version_id = repository
            .create_new_table_version(Uuid::default(), 1)
            .await
            .unwrap();

        // Test all columns again: we should have the schema for the latest table version
        let all_columns = repository
            .get_all_columns_in_database(database_id, None)
            .await
            .expect("Error getting all columns");

        assert_eq!(
            all_columns,
            expected(
                new_version_id,
                "testdb".to_string(),
                "testcol".to_string(),
                "testtable".to_string()
            )
        );

        // Try to get the original version again explicitly
        let all_columns = repository
            .get_all_columns_in_database(database_id, Some(vec![1 as TableVersionId]))
            .await
            .expect("Error getting all columns");

        assert_eq!(
            all_columns,
            expected(
                1,
                "testdb".to_string(),
                "testcol".to_string(),
                "testtable".to_string()
            )
        );

        // Check the existing table versions
        let all_table_versions: Vec<TableVersionId> = repository
            .get_all_table_versions("testdb", Some(vec!["testtable".to_string()]))
            .await
            .expect("Error getting all columns")
            .iter()
            .map(|tv| tv.table_version_id)
            .collect();

        assert_eq!(all_table_versions, vec![1, new_version_id]);

        (database_id, table_id, table_version_id)
    }

    async fn test_create_functions(
        repository: Arc<dyn Repository>,
        database_id: DatabaseId,
    ) {
        // Persist some functions
        let function_id = repository
            .create_function(
                database_id,
                "testfun",
                &CreateFunctionDetails {
                    entrypoint: "entrypoint".to_string(),
                    language: CreateFunctionLanguage::Wasm,
                    input_types: vec![
                        CreateFunctionDataType::FLOAT,
                        CreateFunctionDataType::BIGINT,
                    ],
                    return_type: CreateFunctionDataType::INT,
                    data: "data".to_string(),
                    volatility: CreateFunctionVolatility::Volatile,
                },
            )
            .await
            .unwrap();

        // Load functions
        let all_functions = repository
            .get_all_functions_in_database(database_id)
            .await
            .unwrap();

        let expected_functions = vec![AllDatabaseFunctionsResult {
            name: "testfun".to_string(),
            id: function_id,
            entrypoint: "entrypoint".to_string(),
            language: "Wasm".to_string(),
            input_types: r#"["float","bigint"]"#.to_string(),
            return_type: "INT".to_string(),
            data: "data".to_string(),
            volatility: "Volatile".to_string(),
        }];
        assert_eq!(all_functions, expected_functions);
    }

    async fn test_rename_table(
        repository: Arc<dyn Repository>,
        database_id: DatabaseId,
        table_id: TableId,
        table_version_id: TableVersionId,
    ) {
        // Rename the table to something else
        repository
            .move_table(table_id, "testtable2", None)
            .await
            .unwrap();

        let all_columns = repository
            .get_all_columns_in_database(database_id, None)
            .await
            .expect("Error getting all columns");

        assert_eq!(
            all_columns,
            expected(
                table_version_id,
                "testdb".to_string(),
                "testcol".to_string(),
                "testtable2".to_string()
            )
        );

        // Create a new schema and move the table to it
        let collection_id = repository
            .create_collection(database_id, "testcol2")
            .await
            .unwrap();
        repository
            .move_table(table_id, "testtable2", Some(collection_id))
            .await
            .unwrap();

        let all_columns = repository
            .get_all_columns_in_database(database_id, None)
            .await
            .expect("Error getting all columns");

        assert_eq!(
            all_columns,
            expected(
                table_version_id,
                "testdb".to_string(),
                "testcol2".to_string(),
                "testtable2".to_string()
            )
        );
    }

    async fn test_error_propagation(repository: Arc<dyn Repository>, table_id: TableId) {
        // Nonexistent table ID
        assert!(matches!(
            repository
                .move_table(-1, "doesntmatter", None)
                .await
                .unwrap_err(),
            Error::SqlxError(sqlx::Error::RowNotFound)
        ));

        // Existing table ID, moved to a nonexistent collection (FK violation)
        assert!(matches!(
            repository
                .move_table(table_id, "doesntmatter", Some(-1))
                .await
                .unwrap_err(),
            Error::FKConstraintViolation(_)
        ));

        // Make a new table in the existing collection with the same name
        let schema = Schema {
            arrow_schema: Arc::new(ArrowSchema::empty()),
        };

        let collection_id_1 = repository
            .get_collection_id_by_name("testdb", "testcol")
            .await
            .unwrap();
        let collection_id_2 = repository
            .get_collection_id_by_name("testdb", "testcol2")
            .await
            .unwrap();

        assert!(matches!(
            repository
                .create_table(collection_id_2, "testtable2", &schema, Uuid::default())
                .await
                .unwrap_err(),
            Error::UniqueConstraintViolation(_)
        ));

        // Make a new table in the previous collection, try renaming
        let (new_table_id, _) = repository
            .create_table(collection_id_1, "testtable2", &schema, Uuid::default())
            .await
            .unwrap();

        assert!(matches!(
            repository
                .move_table(new_table_id, "testtable2", Some(collection_id_2))
                .await
                .unwrap_err(),
            Error::UniqueConstraintViolation(_)
        ));
    }
}
