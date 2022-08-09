use std::str::FromStr;
use std::{collections::HashMap, fmt::Debug, sync::Arc};

use async_trait::async_trait;
use itertools::Itertools;
#[cfg(test)]
use mockall::automock;

use crate::data_types::FunctionId;
use crate::provider::SeafowlFunction;
use crate::wasm_udf::data_types::{
    CreateFunctionDetails, CreateFunctionLanguage, CreateFunctionVolatility,
    CreateFunctionWASMType,
};
use crate::{
    data_types::{
        CollectionId, DatabaseId, PhysicalPartitionId, TableId, TableVersionId,
    },
    provider::{
        PartitionColumn, SeafowlCollection, SeafowlDatabase, SeafowlPartition,
        SeafowlTable,
    },
    repository::interface::{
        AllDatabaseColumnsResult, AllDatabaseFunctionsResult, AllTablePartitionsResult,
        Repository,
    },
    schema::Schema,
};

// TODO: this trait is basically a wrapper around Repository, apart from the custom logic
// for converting rows of database / partition results into SeafowlDatabase/Partition structs;
// merge the two? Will a different database than PG still use the AllDatabaseColumnsResult /
// AllTablePartitionsResult structs?

#[cfg_attr(test, automock)]
#[async_trait]
pub trait TableCatalog: Sync + Send + Debug {
    async fn load_database(&self, id: DatabaseId) -> SeafowlDatabase;
    async fn get_database_id_by_name(&self, database_name: &str) -> Option<DatabaseId>;
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

    async fn create_new_table_version(
        &self,
        from_version: TableVersionId,
    ) -> TableVersionId;

    async fn drop_table(&self, table_id: TableId);

    async fn drop_collection(&self, collection_id: CollectionId);

    async fn drop_database(&self, database_id: DatabaseId);
}

#[cfg_attr(test, automock)]
#[async_trait]
pub trait PartitionCatalog: Sync + Send + Debug {
    // TODO: figure out content addressability (currently we'll create new partition meta records
    // even if the same partition already exists)
    async fn create_partitions(
        &self,
        partitions: Vec<SeafowlPartition>,
    ) -> Vec<PhysicalPartitionId>;

    async fn load_table_partitions(
        &self,
        table_version_id: TableVersionId,
    ) -> Vec<SeafowlPartition>;

    async fn append_partitions_to_table(
        &self,
        partition_ids: Vec<PhysicalPartitionId>,
        table_version_id: TableVersionId,
    );
}

#[cfg_attr(test, automock)]
#[async_trait]
pub trait FunctionCatalog: Sync + Send + Debug {
    async fn create_function(
        &self,
        database_id: DatabaseId,
        function_name: &str,
        details: &CreateFunctionDetails,
    ) -> FunctionId;

    async fn get_all_functions_in_database(
        &self,
        database_id: DatabaseId,
    ) -> Vec<SeafowlFunction>;
}

#[derive(Clone, Debug)]
pub struct DefaultCatalog {
    pub repository: Arc<dyn Repository>,
}

impl DefaultCatalog {
    fn build_partition<'a, I>(&self, partition_columns: I) -> SeafowlPartition
    where
        I: Iterator<Item = &'a AllTablePartitionsResult>,
    {
        let mut iter = partition_columns.peekable();

        SeafowlPartition {
            object_storage_id: Arc::from(iter.peek().unwrap().object_storage_id.clone()),
            row_count: iter.peek().unwrap().row_count,
            columns: Arc::new(
                iter.map(|partition| PartitionColumn {
                    name: Arc::from(partition.column_name.clone()),
                    r#type: Arc::from(partition.column_type.clone()),
                    min_value: Arc::new(partition.min_value.clone()),
                    max_value: Arc::new(partition.max_value.clone()),
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
        // We have an iterator of all columns and all partitions in this table.
        // We want to, first, deduplicate all columns (since we repeat them for every partition)
        // in order to build the table's schema.
        // We also want to make a Partition object from all partitions in this table.
        // Since we're going to be consuming this iterator twice (first for unique, then for the group_by),
        // collect all columns into a vector.
        let table_columns_vec = table_columns.collect_vec();

        // Recover the table ID and version ID (this is going to be the same for all columns).
        // TODO: if the table has no columns, the result set will be empty, so we use a fake version ID.
        let (table_id, table_version_id) = table_columns_vec
            .get(0)
            .map_or_else(|| (0, 0), |v| (v.table_id, v.table_version_id));

        let table = SeafowlTable {
            name: Arc::from(table_name.to_string()),
            table_id,
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
impl TableCatalog for DefaultCatalog {
    async fn load_database(&self, database_id: DatabaseId) -> SeafowlDatabase {
        let all_columns = self
            .repository
            .get_all_columns_in_database(database_id)
            .await
            .expect("TODO db load error");

        // Turn the list of all collections, tables and their columns into a nested map.

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

    async fn get_database_id_by_name(&self, database_name: &str) -> Option<DatabaseId> {
        match self.repository.get_database_id_by_name(database_name).await {
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

    async fn create_new_table_version(
        &self,
        from_version: TableVersionId,
    ) -> TableVersionId {
        self.repository
            .create_new_table_version(from_version)
            .await
            .expect("TODO create version error")
    }

    async fn drop_table(&self, table_id: TableId) {
        self.repository
            .drop_table(table_id)
            .await
            .expect("TODO drop table error")
    }

    async fn drop_collection(&self, collection_id: CollectionId) {
        self.repository
            .drop_collection(collection_id)
            .await
            .expect("TODO drop collection error")
    }

    async fn drop_database(&self, database_id: DatabaseId) {
        self.repository
            .drop_database(database_id)
            .await
            .expect("TODO drop database error")
    }
}

#[async_trait]
impl PartitionCatalog for DefaultCatalog {
    async fn create_partitions(
        &self,
        partitions: Vec<SeafowlPartition>,
    ) -> Vec<PhysicalPartitionId> {
        self.repository
            .create_partitions(partitions)
            .await
            .expect("TODO create partition error")
    }

    async fn load_table_partitions(
        &self,
        table_version_id: TableVersionId,
    ) -> Vec<SeafowlPartition> {
        let all_partitions = self
            .repository
            .get_all_partitions_in_table(table_version_id)
            .await
            .expect("TODO db load error");
        all_partitions
            .iter()
            .group_by(|col| col.table_partition_id)
            .into_iter()
            .map(|(_, cs)| self.build_partition(cs))
            .collect()
    }

    async fn append_partitions_to_table(
        &self,
        partition_ids: Vec<PhysicalPartitionId>,
        table_version_id: TableVersionId,
    ) {
        self.repository
            .append_partitions_to_table(partition_ids, table_version_id)
            .await
            .expect("TODO attach partition error")
    }
}

#[async_trait]
impl FunctionCatalog for DefaultCatalog {
    async fn create_function(
        &self,
        database_id: DatabaseId,
        function_name: &str,
        details: &CreateFunctionDetails,
    ) -> FunctionId {
        self.repository
            .create_function(database_id, function_name, details)
            .await
            .expect("TODO create function error")
    }

    async fn get_all_functions_in_database(
        &self,
        database_id: DatabaseId,
    ) -> Vec<SeafowlFunction> {
        let all_functions = self
            .repository
            .get_all_functions_in_database(database_id)
            .await
            .expect("TODO get all functions in database error");

        let functions = all_functions
            .iter()
            .map(|item| {
                let AllDatabaseFunctionsResult {
                    id,
                    name,
                    entrypoint,
                    language,
                    input_types,
                    return_type,
                    data,
                    volatility,
                } = item;

                let details = CreateFunctionDetails {
                    entrypoint: entrypoint.clone(),
                    language: CreateFunctionLanguage::from_str(language.as_str())
                        .unwrap(),
                    input_types: serde_json::from_str::<Vec<CreateFunctionWASMType>>(
                        input_types,
                    )
                    .expect("Couldn't deserialize input types!"),
                    return_type: CreateFunctionWASMType::from_str(return_type.as_str())
                        .unwrap(),
                    data: data.clone(),
                    volatility: CreateFunctionVolatility::from_str(volatility.as_str())
                        .unwrap(),
                };

                SeafowlFunction {
                    function_id: *id,
                    name: name.clone(),
                    details,
                }
            })
            .collect::<Vec<SeafowlFunction>>();

        functions
    }
}
