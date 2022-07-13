use std::{collections::HashMap, fmt::Debug, sync::Arc};

use async_trait::async_trait;
use itertools::Itertools;
#[cfg(test)]
use mockall::automock;

use crate::{
    data_types::{CollectionId, DatabaseId, PhysicalRegionId, TableId, TableVersionId},
    provider::{RegionColumn, SeafowlCollection, SeafowlDatabase, SeafowlRegion, SeafowlTable},
    repository::{AllDatabaseColumnsResult, AllTableRegionsResult, PostgresRepository, Repository},
    schema::Schema,
};

// TODO: this trait is basically a wrapper around Repository, apart from the custom logic
// for converting rows of database / region results into SeafowlDatabase/Region structs;
// merge the two? Will a different database than PG still use the AllDatabaseColumnsResult /
// AllTableRegionsResult structs?

#[cfg_attr(test, automock)]
#[async_trait]
pub trait TableCatalog: Sync + Send + Debug {
    async fn load_database(&self, id: DatabaseId) -> SeafowlDatabase;
    async fn get_collection_id_by_name(
        &self,
        database_name: &str,
        collection_name: &str,
    ) -> Option<CollectionId>;

    async fn create_database(&self, database_name: &str) -> DatabaseId;

    async fn create_collection(
        &self,
        database_id: DatabaseId,
        collection_name: &str,
    ) -> CollectionId;

    async fn create_table(
        &self,
        collection_id: CollectionId,
        table_name: &str,
        schema: Schema,
    ) -> (TableId, TableVersionId);

    async fn create_new_table_version(&self, from_version: TableVersionId) -> TableVersionId;
}

#[cfg_attr(test, automock)]
#[async_trait]
pub trait RegionCatalog: Sync + Send + Debug {
    // TODO: figure out content addressability (currently we'll create new region meta records
    // even if the same region already exists)
    async fn create_regions(&self, regions: Vec<SeafowlRegion>) -> Vec<PhysicalRegionId>;

    async fn load_table_regions(&self, table_version_id: TableVersionId) -> Vec<SeafowlRegion>;

    async fn append_regions_to_table(
        &self,
        region_ids: Vec<PhysicalRegionId>,
        table_version_id: TableVersionId,
    );
}

#[derive(Clone, Debug)]
pub struct PostgresCatalog {
    repository: Arc<PostgresRepository>,
}

impl PostgresCatalog {
    fn build_region<'a, I>(&self, region_columns: I) -> SeafowlRegion
    where
        I: Iterator<Item = &'a AllTableRegionsResult>,
    {
        let mut iter = region_columns.peekable();

        SeafowlRegion {
            object_storage_id: Arc::from(iter.peek().unwrap().object_storage_id.clone()),
            row_count: iter.peek().unwrap().row_count,
            columns: Arc::new(
                iter.map(|region| RegionColumn {
                    name: Arc::from(region.column_name.clone()),
                    r#type: Arc::from(region.column_type.clone()),
                    min_value: Arc::new(region.min_value.clone()),
                    max_value: Arc::new(region.max_value.clone()),
                })
                .collect(),
            ),
        }
    }

    fn build_table<'a, I>(
        &self,
        table_name: &str,
        table_columns: I,
    ) -> (Arc<str>, Arc<SeafowlTable>)
    where
        I: Iterator<Item = &'a AllDatabaseColumnsResult>,
    {
        // We have an iterator of all columns and all regions in this table.
        // We want to, first, deduplicate all columns (since we repeat them for every region)
        // in order to build the table's schema.
        // We also want to make a Region object from all regions in this table.
        // Since we're going to be consuming this iterator twice (first for unique, then for the group_by),
        // collect all columns into a vector.
        let table_columns_vec = table_columns.collect_vec();

        // Recover the table version ID (this is going to be the same for all columns).
        // TODO: if the table has no columns, the result set will be empty, so we use a fake version ID.
        let table_version_id = table_columns_vec
            .get(0)
            .map_or_else(|| 0, |v| v.table_version_id);

        let table = SeafowlTable {
            name: Arc::from(table_name.to_string()),
            table_version_id,
            schema: Arc::new(Schema::from_column_names_types(
                table_columns_vec
                    .iter()
                    .map(|col| (&col.column_name, &col.column_type)),
            )),

            catalog: Arc::new(self.clone()),
        };

        (Arc::from(table_name.to_string()), Arc::new(table))
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
            .group_by(|col| &col.table_name)
            .into_iter()
            .map(|(tn, tc)| self.build_table(tn, tc))
            .collect::<HashMap<_, _>>();

        (
            Arc::from(collection_name.to_string()),
            Arc::new(SeafowlCollection {
                name: Arc::from(collection_name.to_string()),
                tables,
            }),
        )
    }
}

#[async_trait]
impl TableCatalog for PostgresCatalog {
    async fn load_database(&self, database_id: DatabaseId) -> SeafowlDatabase {
        let all_columns = self
            .repository
            .get_all_columns_in_database(database_id)
            .await
            .expect("TODO db load error");

        // This is a slight mess. We get a severely denormalized table of all collections, tables, regions,
        // columns and column min-max values in the database, sorted by collection name, table name and region ID.
        // Through some group_by, build a nested map of collections -> tables -> columns and regions with min/max values.

        let collections: HashMap<Arc<str>, Arc<SeafowlCollection>> = all_columns
            .iter()
            .group_by(|col| &col.collection_name)
            .into_iter()
            .map(|(cn, cc)| self.build_collection(cn, cc))
            .collect();

        SeafowlDatabase {
            // TODO load the database name too
            name: Arc::from("database"),
            collections,
        }
    }

    async fn create_table(
        &self,
        collection_id: CollectionId,
        table_name: &str,
        schema: Schema,
    ) -> (TableId, TableVersionId) {
        self.repository
            .create_table(collection_id, table_name, schema)
            .await
            .expect("TODO table create error")
    }

    async fn get_collection_id_by_name(
        &self,
        database_name: &str,
        collection_name: &str,
    ) -> Option<CollectionId> {
        match self
            .repository
            .get_collection_id_by_name(database_name, collection_name)
            .await
        {
            Ok(id) => Some(id),
            Err(sqlx::error::Error::RowNotFound) => None,
            Err(e) => panic!("TODO SQL error: {:?}", e),
        }
    }

    async fn create_database(&self, database_name: &str) -> DatabaseId {
        self.repository
            .create_database(database_name)
            .await
            .expect("TODO create database error")
    }

    async fn create_collection(
        &self,
        database_id: DatabaseId,
        collection_name: &str,
    ) -> CollectionId {
        self.repository
            .create_collection(database_id, collection_name)
            .await
            .expect("TODO create collection error")
    }

    async fn create_new_table_version(&self, from_version: TableVersionId) -> TableVersionId {
        self.repository
            .create_new_table_version(from_version)
            .await
            .expect("TODO create version error")
    }
}

#[async_trait]
impl RegionCatalog for PostgresCatalog {
    async fn create_regions(&self, regions: Vec<SeafowlRegion>) -> Vec<PhysicalRegionId> {
        self.repository
            .create_regions(regions)
            .await
            .expect("TODO create region error")
    }

    async fn load_table_regions(&self, table_version_id: TableVersionId) -> Vec<SeafowlRegion> {
        let all_regions = self
            .repository
            .get_all_regions_in_table(table_version_id)
            .await
            .expect("TODO db load error");
        all_regions
            .iter()
            .group_by(|col| col.table_region_id)
            .into_iter()
            .map(|(_, cs)| self.build_region(cs))
            .collect()
    }

    async fn append_regions_to_table(
        &self,
        region_ids: Vec<PhysicalRegionId>,
        table_version_id: TableVersionId,
    ) {
        self.repository
            .append_regions_to_table(region_ids, table_version_id)
            .await
            .expect("TODO attach region error")
    }
}
