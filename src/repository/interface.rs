use std::fmt::Debug;

use async_trait::async_trait;

use crate::wasm_udf::data_types::CreateFunctionDetails;
use crate::{
    data_types::{
        CollectionId, DatabaseId, FunctionId, PhysicalPartitionId, TableId,
        TableVersionId, Timestamp,
    },
    provider::SeafowlPartition,
    schema::Schema,
};

#[derive(sqlx::FromRow, Debug, PartialEq, Eq)]
pub struct AllDatabaseColumnsResult {
    pub collection_name: String,
    pub table_name: String,
    pub table_id: TableId,
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
    pub creation_time: Timestamp,
}

#[derive(sqlx::FromRow, Debug, PartialEq, Eq)]
pub struct AllTablePartitionsResult {
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

    async fn get_all_partitions_in_table(
        &self,
        table_version_id: TableVersionId,
    ) -> Result<Vec<AllTablePartitionsResult>, Error>;

    async fn get_collection_id_by_name(
        &self,
        database_name: &str,
        collection_name: &str,
    ) -> Result<CollectionId, Error>;

    async fn get_database_id_by_name(
        &self,
        database_name: &str,
    ) -> Result<DatabaseId, Error>;

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
    ) -> Result<(TableId, TableVersionId), Error>;

    async fn delete_old_table_versions(
        &self,
        table_id: Option<TableId>,
    ) -> Result<u64, Error>;

    async fn create_partitions(
        &self,
        partition: Vec<SeafowlPartition>,
    ) -> Result<Vec<PhysicalPartitionId>, Error>;

    async fn append_partitions_to_table(
        &self,
        partition_ids: Vec<PhysicalPartitionId>,
        table_version_id: TableVersionId,
    ) -> Result<(), Error>;

    async fn get_orphan_partition_store_ids(&self) -> Result<Vec<String>, Error>;

    async fn delete_partitions(
        &self,
        object_storage_ids: Vec<String>,
    ) -> Result<u64, Error>;

    async fn create_new_table_version(
        &self,
        from_version: TableVersionId,
        inherit_partitions: bool,
    ) -> Result<TableVersionId, Error>;

    async fn get_all_table_versions(
        &self,
        table_names: Vec<String>,
    ) -> Result<Vec<TableVersionsResult>>;

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
}

#[cfg(test)]
pub mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{
        DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    };

    use crate::provider::PartitionColumn;
    use crate::wasm_udf::data_types::{
        CreateFunctionDataType, CreateFunctionLanguage, CreateFunctionVolatility,
    };

    use super::*;

    const EXPECTED_FILE_NAME: &str =
        "bdd6eef7340866d1ad99ed34ce0fa43c0d06bbed4dbcb027e9a51de48638b3ed.parquet";

    fn get_test_partition() -> SeafowlPartition {
        SeafowlPartition {
            partition_id: Some(1),
            object_storage_id: Arc::from(EXPECTED_FILE_NAME.to_string()),
            row_count: 2,
            columns: Arc::new(vec![
                PartitionColumn {
                    name: Arc::from("timestamp".to_string()),
                    r#type: Arc::from("{\"name\":\"utf8\"}".to_string()),
                    min_value: Arc::new(None),
                    max_value: Arc::new(None),
                    null_count: Some(1),
                },
                PartitionColumn {
                    name: Arc::from("integer".to_string()),
                    r#type: Arc::from(
                        "{\"name\":\"int\",\"bitWidth\":64,\"isSigned\":true}"
                            .to_string(),
                    ),
                    min_value: Arc::new(Some([49, 50].to_vec())),
                    max_value: Arc::new(Some([52, 50].to_vec())),
                    null_count: Some(0),
                },
                PartitionColumn {
                    name: Arc::from("varchar".to_string()),
                    r#type: Arc::from("{\"name\":\"utf8\"}".to_string()),
                    min_value: Arc::new(None),
                    max_value: Arc::new(None),
                    null_count: None,
                },
            ]),
        }
    }

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
            .create_table(collection_id, "testtable", &schema)
            .await
            .expect("Error creating table");

        (database_id, collection_id, table_id, table_version_id)
    }

    pub async fn run_generic_repository_tests(repository: Arc<dyn Repository>) {
        test_get_collections_empty(repository.clone()).await;
        let (database_id, table_id, table_version_id) =
            test_create_database_collection_table(repository.clone()).await;
        let new_version_id =
            test_create_append_partition(repository.clone(), table_version_id).await;
        test_create_functions(repository.clone(), database_id).await;
        test_rename_table(repository.clone(), database_id, table_id, new_version_id)
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
        collection_name: String,
        table_name: String,
    ) -> Vec<AllDatabaseColumnsResult> {
        vec![
            AllDatabaseColumnsResult {
                collection_name: collection_name.clone(),
                table_name: table_name.clone(),
                table_id: 1,
                table_version_id: version,
                column_name: "date".to_string(),
                column_type: "{\"children\":[],\"name\":\"date\",\"nullable\":false,\"type\":{\"name\":\"date\",\"unit\":\"MILLISECOND\"}}".to_string(),
            },
            AllDatabaseColumnsResult {
                collection_name,
                table_name,
                table_id: 1,
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

        // Test loading all columns

        let all_columns = repository
            .get_all_columns_in_database(database_id, None)
            .await
            .expect("Error getting all columns");

        assert_eq!(
            all_columns,
            expected(1, "testcol".to_string(), "testtable".to_string())
        );

        // Duplicate the table
        let new_version_id = repository
            .create_new_table_version(table_version_id, true)
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
            expected(1, "testcol".to_string(), "testtable".to_string())
        );

        // Check the existing table versions
        let all_table_versions: Vec<TableVersionId> = repository
            .get_all_table_versions(vec!["testtable".to_string()])
            .await
            .expect("Error getting all columns")
            .iter()
            .map(|tv| tv.table_version_id)
            .collect();

        assert_eq!(all_table_versions, vec![1, new_version_id]);

        (database_id, table_id, table_version_id)
    }

    async fn test_create_append_partition(
        repository: Arc<dyn Repository>,
        table_version_id: TableVersionId,
    ) -> TableVersionId {
        let partition = get_test_partition();

        // Create a partition
        let partition_ids = repository.create_partitions(vec![partition]).await.unwrap();
        assert_eq!(partition_ids.len(), 1);

        let partition_id = partition_ids.first().unwrap();

        // Test loading all table partitions when the partition is not yet attached
        let all_partitions = repository
            .get_all_partitions_in_table(table_version_id)
            .await
            .unwrap();
        assert_eq!(all_partitions, Vec::<AllTablePartitionsResult>::new());

        // Attach the partition to the table
        repository
            .append_partitions_to_table(partition_ids.clone(), table_version_id)
            .await
            .unwrap();

        // Load again
        let all_partitions = repository
            .get_all_partitions_in_table(table_version_id)
            .await
            .unwrap();

        let expected_partitions = vec![
            AllTablePartitionsResult {
                table_partition_id: *partition_id,
                object_storage_id: EXPECTED_FILE_NAME.to_string(),
                column_name: "timestamp".to_string(),
                column_type: "{\"name\":\"utf8\"}".to_string(),
                row_count: 2,
                min_value: None,
                max_value: None,
                null_count: Some(1),
            },
            AllTablePartitionsResult {
                table_partition_id: *partition_id,
                object_storage_id: EXPECTED_FILE_NAME.to_string(),
                column_name: "integer".to_string(),
                column_type: "{\"name\":\"int\",\"bitWidth\":64,\"isSigned\":true}"
                    .to_string(),
                row_count: 2,
                min_value: Some([49, 50].to_vec()),
                max_value: Some([52, 50].to_vec()),
                null_count: Some(0),
            },
            AllTablePartitionsResult {
                table_partition_id: *partition_id,
                object_storage_id: EXPECTED_FILE_NAME.to_string(),
                column_name: "varchar".to_string(),
                column_type: "{\"name\":\"utf8\"}".to_string(),
                row_count: 2,
                min_value: None,
                max_value: None,
                null_count: None,
            },
        ];
        assert_eq!(all_partitions, expected_partitions);

        // Duplicate the table, check it has the same partitions
        let new_version_id = repository
            .create_new_table_version(table_version_id, true)
            .await
            .unwrap();

        let all_partitions = repository
            .get_all_partitions_in_table(new_version_id)
            .await
            .unwrap();

        assert_eq!(all_partitions, expected_partitions);

        new_version_id
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
                .create_table(collection_id_2, "testtable2", &schema)
                .await
                .unwrap_err(),
            Error::UniqueConstraintViolation(_)
        ));

        // Make a new table in the previous collection, try renaming
        let (new_table_id, _) = repository
            .create_table(collection_id_1, "testtable2", &schema)
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
