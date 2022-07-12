use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;

use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;

use crate::{
    catalog::Catalog,
    data_types::{CollectionId, DatabaseId, PhysicalRegionId, TableId, TableVersionId},
    provider::{SeafowlCollection, SeafowlDatabase, SeafowlRegion, SeafowlTable},
    schema::Schema,
};

#[derive(Clone, Debug)]
pub struct MockCatalog {
    pub singleton_table_name: String,
    pub singleton_table_schema: ArrowSchemaRef,
}

#[async_trait]
impl Catalog for MockCatalog {
    async fn load_database(&self, _id: DatabaseId) -> SeafowlDatabase {
        let singleton_table = SeafowlTable {
            name: Arc::from(self.singleton_table_name.clone()),
            schema: Arc::new(Schema {
                arrow_schema: self.singleton_table_schema.clone(),
            }),
            table_version_id: 0,
            catalog: Arc::new(self.clone()),
        };
        let tables = HashMap::from([(
            Arc::from(self.singleton_table_name.clone()),
            Arc::from(singleton_table),
        )]);
        let collections = HashMap::from([(
            Arc::from("testcol"),
            Arc::from(SeafowlCollection {
                name: Arc::from("testcol"),
                tables,
            }),
        )]);

        SeafowlDatabase {
            name: Arc::from("testdb"),
            collections,
        }
    }
    async fn create_table(
        &self,
        _collection_id: CollectionId,
        _table_name: &str,
        _schema: Schema,
    ) -> (TableId, TableVersionId) {
        (1, 1)
    }
    async fn load_table_regions(&self, _table_version_id: TableVersionId) -> Vec<SeafowlRegion> {
        vec![SeafowlRegion {
            object_storage_id: Arc::from("some-file.parquet"),
            row_count: 3,
            columns: Arc::new(vec![]),
        }]
    }
    async fn get_collection_id_by_name(
        &self,
        database_name: &str,
        collection_name: &str,
    ) -> Option<CollectionId> {
        if database_name != "testdb" && collection_name != "testcol" {
            panic!("unexpected database / collection name")
        }
        Some(1)
    }

    async fn create_database(&self, _database_name: &str) -> DatabaseId {
        1
    }

    async fn create_collection(
        &self,
        _database_id: DatabaseId,
        _collection_name: &str,
    ) -> CollectionId {
        1
    }

    async fn create_regions(&self, regions: Vec<SeafowlRegion>) -> Vec<PhysicalRegionId> {
        (0..regions.len() as i64).collect()
    }

    async fn append_regions_to_table(
        &self,
        _region_ids: Vec<PhysicalRegionId>,
        _table_version_id: TableVersionId,
    ) {
    }
}
