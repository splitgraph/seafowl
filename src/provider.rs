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

use object_store::{path::Path};

use url::Url;

use crate::schema::Schema;

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
    pub regions: Arc<Vec<SeafowlRegion>>,
}

pub struct UrlWrapper {
    pub url: Url,
}

impl AsRef<Url> for UrlWrapper {
    fn as_ref(&self) -> &Url {
        &self.url
    }
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
        // This is partially taken from ListingTable.
        let region_path = self.regions.get(0).unwrap().object_storage_id.to_string();
        let url = Url::parse(&region_path).expect("URL parsing error");
        let store = ctx.runtime_env.object_store(UrlWrapper { url })?;

        // TODO: we can load the regions dynamically here, since we're async

        // TODO: use filters and apply them to regions here (grab the code from list_files_for_scan)
        let partitioned_file_lists: Vec<Vec<PartitionedFile>> =
            future::try_join_all(self.regions.iter().map(|r| async {
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
            object_store_url: ObjectStoreUrl::parse("seafowl-object://").expect("TODO parse error"),
            file_schema: self.schema(),
            file_groups: partitioned_file_lists,
            statistics: Statistics::default(),
            projection: projection.clone(),
            limit: limit,
            table_partition_cols: vec![],
        };

        let format = ParquetFormat::default();
        let plan = format
            .create_physical_plan(config, filters)
            .await
            .expect("TODO plan gen error");

        Ok(Arc::new(SeafowlBaseTableScanNode {
            name: self.name.to_owned(),
            schema: self.schema.to_owned(),
            regions: self.regions.to_owned(),
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
