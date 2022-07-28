use std::fmt::Debug;

use async_trait::async_trait;

use sqlx::Error;

use crate::{
    data_types::{CollectionId, DatabaseId, PhysicalRegionId, TableId, TableVersionId},
    provider::SeafowlRegion,
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
pub struct AllTableRegionsResult {
    pub table_region_id: i64,
    pub object_storage_id: String,
    pub column_name: String,
    pub column_type: String,
    pub row_count: i32,
    pub min_value: Option<Vec<u8>>,
    pub max_value: Option<Vec<u8>>,
}

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
    ) -> Result<Vec<AllDatabaseColumnsResult>, Error>;

    async fn get_all_regions_in_table(
        &self,
        table_version_id: TableVersionId,
    ) -> Result<Vec<AllTableRegionsResult>, Error>;

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
        schema: Schema,
    ) -> Result<(TableId, TableVersionId), Error>;

    async fn create_regions(
        &self,
        region: Vec<SeafowlRegion>,
    ) -> Result<Vec<PhysicalRegionId>, Error>;

    async fn append_regions_to_table(
        &self,
        region_ids: Vec<PhysicalRegionId>,
        table_version_id: TableVersionId,
    ) -> Result<(), Error>;

    async fn create_new_table_version(
        &self,
        from_version: TableVersionId,
    ) -> Result<TableVersionId, Error>;

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

    use crate::provider::RegionColumn;

    use super::*;

    const EXPECTED_FILE_NAME: &str =
        "bdd6eef7340866d1ad99ed34ce0fa43c0d06bbed4dbcb027e9a51de48638b3ed.parquet";

    fn get_test_region() -> SeafowlRegion {
        SeafowlRegion {
            object_storage_id: Arc::from(EXPECTED_FILE_NAME.to_string()),
            row_count: 2,
            columns: Arc::new(vec![
                RegionColumn {
                    name: Arc::from("timestamp".to_string()),
                    r#type: Arc::from("{\"name\":\"utf8\"}".to_string()),
                    min_value: Arc::new(None),
                    max_value: Arc::new(None),
                },
                RegionColumn {
                    name: Arc::from("integer".to_string()),
                    r#type: Arc::from(
                        "{\"name\":\"int\",\"bitWidth\":64,\"isSigned\":true}"
                            .to_string(),
                    ),
                    min_value: Arc::new(Some([49, 50].to_vec())),
                    max_value: Arc::new(Some([52, 50].to_vec())),
                },
                RegionColumn {
                    name: Arc::from("varchar".to_string()),
                    r#type: Arc::from("{\"name\":\"utf8\"}".to_string()),
                    min_value: Arc::new(None),
                    max_value: Arc::new(None),
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
            .create_table(collection_id, "testtable", schema)
            .await
            .expect("Error creating table");

        (database_id, collection_id, table_id, table_version_id)
    }

    pub async fn run_generic_repository_tests(repository: Arc<dyn Repository>) {
        test_get_collections_empty(repository.clone()).await;
        let table_version_id =
            test_create_database_collection_table(repository.clone()).await;
        test_create_append_region(repository, table_version_id).await;
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

    async fn test_create_database_collection_table(
        repository: Arc<dyn Repository>,
    ) -> TableVersionId {
        let (database_id, _, _, table_version_id) =
            make_database_with_single_table(repository.clone()).await;

        // Test loading all columns

        let all_columns = repository
            .get_all_columns_in_database(database_id)
            .await
            .expect("Error getting all columns");

        fn expected(version: TableVersionId) -> Vec<AllDatabaseColumnsResult> {
            vec![
                AllDatabaseColumnsResult {
                    collection_name: "testcol".to_string(),
                    table_name: "testtable".to_string(),
                    table_id: 1,
                    table_version_id: version,
                    column_name: "date".to_string(),
                    column_type: "{\"name\":\"date\",\"unit\":\"MILLISECOND\"}"
                        .to_string(),
                },
                AllDatabaseColumnsResult {
                    collection_name: "testcol".to_string(),
                    table_name: "testtable".to_string(),
                    table_id: 1,
                    table_version_id: version,
                    column_name: "value".to_string(),
                    column_type: "{\"name\":\"floatingpoint\",\"precision\":\"DOUBLE\"}"
                        .to_string(),
                },
            ]
        }

        assert_eq!(all_columns, expected(1));

        // Duplicate the table
        let new_version_id = repository
            .create_new_table_version(table_version_id)
            .await
            .unwrap();

        // Test all columns again: we should have the schema for the latest table version
        let all_columns = repository
            .get_all_columns_in_database(database_id)
            .await
            .expect("Error getting all columns");

        assert_eq!(all_columns, expected(new_version_id));

        table_version_id
    }

    async fn test_create_append_region(
        repository: Arc<dyn Repository>,
        table_version_id: TableVersionId,
    ) {
        let region = get_test_region();

        // Create a region
        let region_ids = repository.create_regions(vec![region]).await.unwrap();
        assert_eq!(region_ids.len(), 1);

        let region_id = region_ids.first().unwrap();

        // Test loading all table regions when the region is not yet attached
        let all_regions = repository
            .get_all_regions_in_table(table_version_id)
            .await
            .unwrap();
        assert_eq!(all_regions, Vec::<AllTableRegionsResult>::new());

        // Attach the region to the table
        repository
            .append_regions_to_table(region_ids.clone(), table_version_id)
            .await
            .unwrap();

        // Load again
        let all_regions = repository
            .get_all_regions_in_table(table_version_id)
            .await
            .unwrap();

        let expected_regions = vec![
            AllTableRegionsResult {
                table_region_id: *region_id,
                object_storage_id: EXPECTED_FILE_NAME.to_string(),
                column_name: "timestamp".to_string(),
                column_type: "{\"name\":\"utf8\"}".to_string(),
                row_count: 2,
                min_value: None,
                max_value: None,
            },
            AllTableRegionsResult {
                table_region_id: *region_id,
                object_storage_id: EXPECTED_FILE_NAME.to_string(),
                column_name: "integer".to_string(),
                column_type: "{\"name\":\"int\",\"bitWidth\":64,\"isSigned\":true}"
                    .to_string(),
                row_count: 2,
                min_value: Some([49, 50].to_vec()),
                max_value: Some([52, 50].to_vec()),
            },
            AllTableRegionsResult {
                table_region_id: *region_id,
                object_storage_id: EXPECTED_FILE_NAME.to_string(),
                column_name: "varchar".to_string(),
                column_type: "{\"name\":\"utf8\"}".to_string(),
                row_count: 2,
                min_value: None,
                max_value: None,
            },
        ];
        assert_eq!(all_regions, expected_regions);

        // Duplicate the table, check it has the same regions
        let new_version_id = repository
            .create_new_table_version(table_version_id)
            .await
            .unwrap();

        let all_regions = repository
            .get_all_regions_in_table(new_version_id)
            .await
            .unwrap();

        assert_eq!(all_regions, expected_regions);
    }
}
