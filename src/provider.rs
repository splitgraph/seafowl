use std::{any::Any, collections::HashMap, sync::Arc};

use async_trait::async_trait;

use datafusion::{
    arrow::datatypes::SchemaRef as ArrowSchemaRef,
    catalog::{catalog::CatalogProvider, schema::SchemaProvider},
    common::{DataFusionError, Result},
    datasource::{
        file_format::{parquet::ParquetFormat, FileFormat},
        listing::PartitionedFile,
        object_store::ObjectStoreUrl,
        TableProvider,
    },
    execution::context::{SessionState, TaskContext},
    logical_expr::TableType,
    logical_plan::Expr,
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        file_format::FileScanConfig, ExecutionPlan, Partitioning, SendableRecordBatchStream,
        Statistics,
    },
};

use futures::future;

use object_store::path::Path;

use crate::{catalog::Catalog, data_types::TableVersionId, schema::Schema};

pub struct SeafowlDatabase {
    pub name: Arc<str>,
    pub collections: HashMap<Arc<str>, Arc<SeafowlCollection>>,
}

impl CatalogProvider for SeafowlDatabase {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.collections.keys().map(|s| s.to_string()).collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.collections.get(name).map(|c| Arc::clone(c) as _)
    }
}

pub struct SeafowlCollection {
    pub name: Arc<str>,
    pub tables: HashMap<Arc<str>, Arc<SeafowlTable>>,
}

impl SchemaProvider for SeafowlCollection {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.tables.keys().map(|s| s.to_string()).collect()
    }

    fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        self.tables.get(name).map(|c| Arc::clone(c) as _)
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
}

#[derive(Debug)]
pub struct SeafowlRegion {
    pub object_storage_id: Arc<str>,
    pub row_count: i32,
    pub columns: Arc<Vec<RegionColumn>>,
}

#[derive(Debug)]
pub struct RegionColumn {
    pub name: Arc<str>,
    pub r#type: Arc<str>,
    pub min_value: Arc<Option<Vec<u8>>>,
    pub max_value: Arc<Option<Vec<u8>>>,
}

pub struct SeafowlTable {
    pub name: Arc<str>,
    pub schema: Arc<Schema>,
    pub table_version_id: TableVersionId,

    // We have to keep a reference to the original catalog here. This is
    // because we need it to load the regions for a given table at query plan
    // time (instead of loading the regions for _all_ tables in the database
    // when we build the DataFusion CatalogProvider). But we can't actually
    // load the regions somewhere in the SchemaProvider because none of the functions
    // there are async.
    pub catalog: Arc<dyn Catalog>,
}

#[async_trait]
impl TableProvider for SeafowlTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.schema.arrow_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        ctx: &SessionState,
        projection: &Option<Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> std::result::Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let regions = self.catalog.load_table_regions(self.table_version_id).await;

        // This code is partially taken from ListingTable but adapted to use an arbitrary
        // list of Parquet URLs rather than all files in a given directory.

        // Get our object store (with a hardcoded schema)
        let object_store_url = ObjectStoreUrl::parse("seafowl://").unwrap();
        let store = ctx.runtime_env.object_store(object_store_url.clone())?;

        // Build a list of lists of PartitionedFile groups (one file = one partition for the scan)
        // TODO: use filters and apply them to regions here (grab the code from list_files_for_scan)
        let partitioned_file_lists: Vec<Vec<PartitionedFile>> =
            future::try_join_all(regions.iter().map(|r| async {
                let path = Path::parse(&r.object_storage_id)?;
                let meta = store.head(&path).await?;
                Ok(vec![PartitionedFile {
                    object_meta: meta,
                    partition_values: vec![],
                    range: None,
                }]) as Result<_>
            }))
            .await
            .expect("general error with partitioned file lists");

        let config = FileScanConfig {
            object_store_url,
            file_schema: self.schema(),
            file_groups: partitioned_file_lists,
            statistics: Statistics::default(),
            projection: projection.clone(),
            limit,
            table_partition_cols: vec![],
        };

        let format = ParquetFormat::default();
        let plan = format.create_physical_plan(config, filters).await?;

        Ok(Arc::new(SeafowlBaseTableScanNode {
            name: self.name.to_owned(),
            schema: self.schema.to_owned(),
            regions: Arc::new(regions),
            inner: plan,
        }))
    }
}

#[derive(Debug)]
struct SeafowlBaseTableScanNode {
    pub name: Arc<str>,
    pub schema: Arc<Schema>,
    pub regions: Arc<Vec<SeafowlRegion>>,
    pub inner: Arc<dyn ExecutionPlan>,
}

impl ExecutionPlan for SeafowlBaseTableScanNode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.schema.arrow_schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.regions.len())
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.inner.execute(partition, context)
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use async_trait::async_trait;
    use bytes::{BufMut, Bytes, BytesMut};
    use datafusion::{
        arrow::datatypes::SchemaRef as ArrowSchemaRef,
        arrow::{
            array::{ArrayRef, Int64Array},
            record_batch::RecordBatch,
        },
        catalog::catalog::CatalogProvider,
        datasource::TableProvider,
        parquet::{arrow::ArrowWriter, file::properties::WriterProperties},
        physical_plan::collect,
        prelude::{SessionConfig, SessionContext},
    };
    use object_store::{memory::InMemory, path::Path, ObjectStore};

    use crate::{
        catalog::Catalog,
        data_types::{CollectionId, DatabaseId, TableId, TableVersionId},
        schema::Schema,
    };

    use super::{SeafowlCollection, SeafowlDatabase, SeafowlRegion, SeafowlTable};

    #[derive(Clone)]
    struct MockCatalog {
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
        ) -> TableId {
            1
        }
        async fn load_table_regions(
            &self,
            _table_version_id: TableVersionId,
        ) -> Vec<SeafowlRegion> {
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
    }

    #[tokio::test]
    async fn test_scan_plan() {
        // Make a batch
        let c1: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));
        let batch1 = RecordBatch::try_from_iter(vec![("c1", c1.clone())]).unwrap();

        // Write a Parquet file to the object store
        let buf = BytesMut::new();
        let mut writer = buf.writer();

        let props = WriterProperties::builder().build();
        let mut arrow_writer = ArrowWriter::try_new(&mut writer, batch1.schema(), Some(props))
            .expect("creating writer");

        arrow_writer.write(&batch1).expect("Writing batch");
        arrow_writer.close().unwrap();

        // Write the file to an in-memory store
        let object_store = InMemory::new();
        let location = Path::from("some-file.parquet");
        object_store
            .put(&location, Bytes::from(writer.into_inner()))
            .await
            .expect("Error putting data");

        let session_config = SessionConfig::new().with_information_schema(true);
        let context = SessionContext::with_config(session_config);
        context
            .runtime_env()
            .register_object_store("seafowl", "", Arc::new(object_store));

        let catalog = MockCatalog {
            singleton_table_name: "table".to_string(),
            singleton_table_schema: batch1.schema(),
        };
        let db = catalog.load_database(1).await;
        let table = db.schema("testcol").unwrap().table("table").unwrap();

        let state = context.state.read().clone();
        let plan = table
            .scan(&state, &None, &[], None)
            .await
            .expect("error creating plan");
        let task_ctx = context.task_ctx();
        let result = collect(plan, task_ctx).await.expect("error running");
        dbg!(result);
    }
}
