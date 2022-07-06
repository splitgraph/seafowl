use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use itertools::Itertools;
use object_store::DynObjectStore;

use crate::{
    data_types::DatabaseId,
    provider::{RegionColumn, SeafowlCollection, SeafowlDatabase, SeafowlRegion, SeafowlTable},
    repository::{AllDatabaseColumnsResult, PostgresRepository, Repository},
    schema::Schema,
};

#[async_trait]
pub trait Catalog {
    async fn load_database(mut self, id: DatabaseId) -> SeafowlDatabase;
}

pub struct PostgresCatalog {
    repository: PostgresRepository,
    object_storage: Arc<DynObjectStore>,
}

impl PostgresCatalog {
    fn build_region<'a, I>(&self, region_columns: I) -> SeafowlRegion
    where
        I: Iterator<Item = &'a &'a AllDatabaseColumnsResult>,
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

        let table = SeafowlTable {
            name: Arc::from(table_name.to_string()),
            schema: Arc::new(Schema::from_column_names_types(
                table_columns_vec
                    .iter()
                    .map(|col| (&col.column_name, &col.column_type))
                    .dedup(),
            )),
            regions: Arc::new(
                table_columns_vec
                    .iter()
                    .group_by(|col| col.table_region_id)
                    .into_iter()
                    .map(|(_, r)| self.build_region(r))
                    .collect(),
            ),
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
impl Catalog for PostgresCatalog {
    async fn load_database(mut self, database_id: DatabaseId) -> SeafowlDatabase {
        let all_columns = self
            .repository
            .get_all_columns_in_database(database_id)
            .await
            .unwrap();

        // This is a slight mess. We get a severely denormalized table of all collections, tables, regions,
        // columns and column min-max values in the database, sorted by collection name, table name and region ID.
        // Through some

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
}
