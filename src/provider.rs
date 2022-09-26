use arrow::array::{ArrayRef, UInt64Array};
use arrow::compute::{cast_with_options, CastOptions};
use arrow::datatypes::{DataType, SchemaRef};
use std::ops::Deref;
use std::{any::Any, collections::HashMap, sync::Arc};

use async_trait::async_trait;

use datafusion::common::Column;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_optimizer::pruning::{PruningPredicate, PruningStatistics};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::DisplayFormatType;
use datafusion::scalar::ScalarValue;
use datafusion::{
    arrow::datatypes::SchemaRef as ArrowSchemaRef,
    catalog::{
        catalog::CatalogProvider,
        schema::{MemorySchemaProvider, SchemaProvider},
    },
    common::{DataFusionError, Result},
    datasource::{
        file_format::{parquet::ParquetFormat, FileFormat},
        listing::PartitionedFile,
        TableProvider,
    },
    execution::context::{SessionState, TaskContext},
    logical_expr::TableType,
    logical_plan::Expr,
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        file_format::FileScanConfig, ExecutionPlan, Partitioning,
        SendableRecordBatchStream, Statistics,
    },
};
use datafusion_proto::protobuf;

use futures::future;
use log::warn;
use object_store::ObjectStore;
use prost::Message;

use object_store::path::Path;

use crate::data_types::PhysicalPartitionId;
use crate::{
    catalog::PartitionCatalog,
    data_types::{TableId, TableVersionId},
    schema::Schema,
};
use crate::{catalog::STAGING_SCHEMA, wasm_udf::data_types::CreateFunctionDetails};
use crate::{context::internal_object_store_url, data_types::FunctionId};

pub struct SeafowlDatabase {
    pub name: Arc<str>,
    pub collections: HashMap<Arc<str>, Arc<SeafowlCollection>>,
    pub staging_schema: Arc<MemorySchemaProvider>,
}

impl CatalogProvider for SeafowlDatabase {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.collections
            .keys()
            .map(|s| s.to_string())
            .chain([STAGING_SCHEMA.to_string()])
            .collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        if name == STAGING_SCHEMA {
            Some(self.staging_schema.clone())
        } else {
            self.collections.get(name).map(|c| Arc::clone(c) as _)
        }
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SeafowlPartition {
    pub partition_id: Option<PhysicalPartitionId>,
    pub object_storage_id: Arc<str>,
    pub row_count: i32,
    pub columns: Arc<Vec<PartitionColumn>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionColumn {
    pub name: Arc<str>,
    pub r#type: Arc<str>,
    pub min_value: Arc<Option<Vec<u8>>>,
    pub max_value: Arc<Option<Vec<u8>>>,
    pub null_count: Option<i32>,
}

#[derive(Clone)]
pub struct SeafowlTable {
    pub name: Arc<str>,
    pub schema: Arc<Schema>,
    pub table_id: TableId,
    pub table_version_id: TableVersionId,

    // We have to keep a reference to the original catalog here. This is
    // because we need it to load the partitions for a given table at query plan
    // time (instead of loading the partitions for _all_ tables in the database
    // when we build the DataFusion CatalogProvider). But we can't actually
    // load the partitions somewhere in the SchemaProvider because none of the functions
    // there are async.
    pub catalog: Arc<dyn PartitionCatalog>,
}

impl std::fmt::Debug for SeafowlTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SeafowlTable")
            .field("name", &self.name)
            .field("schema", &self.schema)
            .field("table_id", &self.table_id)
            .field("table_version_id", &self.table_version_id)
            .finish()
    }
}

impl SeafowlTable {
    // This code is partially taken from ListingTable but adapted to use an arbitrary
    // list of Parquet URLs rather than all files in a given directory.
    pub async fn partition_scan_plan(
        &self,
        projection: &Option<Vec<usize>>,
        partitions: Vec<SeafowlPartition>,
        filters: &[Expr],
        limit: Option<usize>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Build a list of lists of PartitionedFile groups (one file = one partition for the scan)
        let partitioned_file_lists: Vec<Vec<PartitionedFile>> =
            future::try_join_all(partitions.iter().map(|p| async {
                let path = Path::parse(&p.object_storage_id)?;
                let meta = object_store.head(&path).await?;
                Ok(vec![PartitionedFile {
                    object_meta: meta,
                    partition_values: vec![],
                    range: None,
                    extensions: None,
                }]) as Result<_>
            }))
            .await?;

        let config = FileScanConfig {
            object_store_url: internal_object_store_url(),
            file_schema: self.schema(),
            file_groups: partitioned_file_lists,
            statistics: Statistics::default(),
            projection: projection.clone(),
            limit,
            table_partition_cols: vec![],
        };

        let format = ParquetFormat::default();
        // TODO: filters here probably does nothing, since we handle pruning explicitly ourselves via
        // `partitions` param
        format.create_physical_plan(config, filters).await
    }

    // Wrap a base scan over the supplied partitions with a filter plan
    pub async fn partition_filter_plan(
        &self,
        partitions: Vec<SeafowlPartition>,
        filter: Arc<dyn PhysicalExpr>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let base_scan = self
            .partition_scan_plan(&None, partitions, &[], None, object_store)
            .await?;

        Ok(Arc::new(FilterExec::try_new(filter, base_scan)?))
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
        let mut partitions = self
            .catalog
            .load_table_partitions(self.table_version_id)
            .await?;

        // Try to prune away redundant partitions
        if !filters.is_empty() {
            match SeafowlPruningStatistics::from_partitions(
                partitions.clone(),
                self.schema(),
            ) {
                Ok(pruning_stats) => partitions = pruning_stats.prune(filters).await,
                Err(error) => {
                    warn!(
                        "Failed constructing pruning statistics for table {} (version: {}): {}",
                        self.name, self.table_version_id, error
                    )
                }
            };
        }

        // Get our object store (with a hardcoded scheme)
        let object_store_url = internal_object_store_url();
        let store = ctx.runtime_env.object_store(object_store_url.clone())?;

        Ok(Arc::new(SeafowlBaseTableScanNode {
            partitions: Arc::new(partitions.clone()),
            inner: self
                .partition_scan_plan(projection, partitions, filters, limit, store)
                .await?,
        }))
    }

    fn supports_filter_pushdown(
        &self,
        _filter: &Expr,
    ) -> Result<TableProviderFilterPushDown> {
        Ok(TableProviderFilterPushDown::Inexact)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeafowlPruningStatistics {
    pub partition_count: usize,
    pub schema: SchemaRef,
    pub partitions: Arc<Vec<SeafowlPartition>>,
    pub min_values: HashMap<String, Vec<ScalarValue>>,
    pub max_values: HashMap<String, Vec<ScalarValue>>,
    pub null_counts: HashMap<String, Vec<Option<u64>>>,
}

impl SeafowlPruningStatistics {
    // Generate maps of min/max/null stats per column, parsing the serialized values stored in partitions
    pub fn from_partitions(
        partitions: Vec<SeafowlPartition>,
        schema: SchemaRef,
    ) -> Result<Self> {
        let partition_count = partitions.len();
        let mut min_values = HashMap::new();
        let mut max_values = HashMap::new();
        let mut null_counts = HashMap::new();

        for field in schema.fields() {
            let null_value = Self::parse_bytes_value(&Arc::new(None), field.data_type())?;

            min_values.insert(
                field.name().clone(),
                vec![null_value.clone(); partition_count],
            );
            max_values.insert(field.name().clone(), vec![null_value; partition_count]);
            null_counts.insert(field.name().clone(), vec![None; partition_count]);
        }

        for (ind, partition) in partitions.iter().enumerate() {
            for column in partition.columns.as_slice() {
                let data_type = schema
                    .field_with_name(column.name.deref())
                    .unwrap()
                    .data_type();

                min_values.get_mut(column.name.as_ref()).unwrap()[ind] =
                    Self::parse_bytes_value(&column.min_value, data_type)?;
                max_values.get_mut(column.name.as_ref()).unwrap()[ind] =
                    Self::parse_bytes_value(&column.max_value, data_type)?;
                null_counts.get_mut(column.name.as_ref()).unwrap()[ind] =
                    column.null_count.map(|nc| nc as u64);
            }
        }

        Ok(Self {
            partition_count,
            schema,
            partitions: Arc::new(partitions),
            min_values,
            max_values,
            null_counts,
        })
    }

    /// Try to deserialize min/max statistics stored as raw bytes
    ///
    /// ```
    /// use std::sync::Arc;
    /// use arrow::datatypes::DataType;
    /// use datafusion::scalar::ScalarValue;
    /// use seafowl::provider::SeafowlPruningStatistics;
    /// use seafowl::context::scalar_value_to_bytes;
    ///
    /// fn parse(value: &Arc<Option<Vec<u8>>>, dt: &DataType) -> ScalarValue {
    ///     SeafowlPruningStatistics::parse_bytes_value(&value, &dt).unwrap()
    /// }
    ///
    /// // Parse missing value into a corresponding None
    /// let val = Arc::new(None);
    /// assert_eq!(parse(&val, &DataType::Int16), ScalarValue::Int16(None));
    /// assert_eq!(parse(&val, &DataType::Boolean), ScalarValue::Boolean(None));
    ///
    /// // Parse some actual value
    /// let val = Arc::from(scalar_value_to_bytes(&ScalarValue::Int32(Some(42))));
    /// assert_eq!(parse(&val, &DataType::Int32), ScalarValue::Int32(Some(42)));
    ///
    /// let val = Arc::from(scalar_value_to_bytes(&ScalarValue::Float32(Some(42.0))));
    /// assert_eq!(parse(&val, &DataType::Float32), ScalarValue::Float32(Some(42.0)));
    ///
    /// let val = Arc::from(scalar_value_to_bytes(&ScalarValue::Utf8(Some("42".to_string()))));
    /// assert_eq!(parse(&val, &DataType::Utf8), ScalarValue::Utf8(Some("42".to_string())));
    /// ```
    pub fn parse_bytes_value(
        bytes_value: &Arc<Option<Vec<u8>>>,
        data_type: &DataType,
    ) -> Result<ScalarValue> {
        match bytes_value.as_ref() {
            Some(bytes) => match protobuf::ScalarValue::decode(bytes.as_slice()) {
                Ok(proto) => {
                    match <&protobuf::ScalarValue as TryInto<ScalarValue>>::try_into(
                        &proto,
                    ) {
                        Ok(value) => Ok(value),
                        Err(error) => Err(DataFusionError::Internal(format!(
                            "Failed to deserialize min/max value: {}",
                            error
                        ))),
                    }
                }
                Err(error) => Err(DataFusionError::Internal(format!(
                    "Failed to decode min/max value: {}",
                    error
                ))),
            },
            None => {
                // Try to cast the None value to the corresponding ScalarValue matching `data_type`,
                // e.g. `ScalarValue::Int16(None)`, `ScalarValue::Boolean(None)`, etc.
                let value = ScalarValue::Null;
                let cast_options = CastOptions { safe: false };
                let cast_arr =
                    cast_with_options(&value.to_array(), data_type, &cast_options)?;
                Ok(ScalarValue::try_from_array(&cast_arr, 0)?)
            }
        }
    }

    // Prune away partitions that are refuted by the provided filter expressions
    pub async fn prune(&self, filters: &[Expr]) -> Vec<SeafowlPartition> {
        let mut partition_mask = vec![true; self.partition_count];

        if !filters.is_empty() {
            for expr in filters {
                match PruningPredicate::try_new(expr.clone(), self.schema.clone()) {
                    Ok(predicate) => match predicate.prune(self) {
                        Ok(expr_mask) => {
                            partition_mask = partition_mask
                                .iter()
                                .zip(expr_mask)
                                .map(|(&a, b)| a && b)
                                .collect()
                        }
                        Err(error) => {
                            warn!(
                                "Failed pruning the partitions for expr {:?}: {}",
                                expr, error
                            );
                        }
                    },
                    Err(error) => {
                        warn!(
                            "Failed constructing a pruning predicate for expr {:?}: {}",
                            expr, error
                        );
                    }
                }
            }
        }

        self.partitions
            .iter()
            .zip(partition_mask.iter())
            .filter_map(|(p, &keep)| if keep { Some(p.clone()) } else { None })
            .collect()
    }

    // Try to convert the vector of min/max scalar values for a column into a generic array reference
    fn get_values_array(&self, values: Option<&Vec<ScalarValue>>) -> Option<ArrayRef> {
        values.and_then(|stats| {
            ScalarValue::iter_to_array(stats.clone().into_iter())
                .map_err(|e| {
                    warn!(
                        "Failed to convert vector of min/max values {:?} to array: {}",
                        values, e
                    );
                    e
                })
                .ok()
        })
    }
}

impl PruningStatistics for SeafowlPruningStatistics {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        self.get_values_array(self.min_values.get(column.name.as_str()))
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        self.get_values_array(self.max_values.get(column.name.as_str()))
    }

    fn num_containers(&self) -> usize {
        self.partition_count
    }

    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        self.null_counts
            .get(column.name.as_str())
            .map(|stats| Arc::new(UInt64Array::from(stats.clone())) as ArrayRef)
    }
}

#[derive(Debug)]
struct SeafowlBaseTableScanNode {
    pub partitions: Arc<Vec<SeafowlPartition>>,
    pub inner: Arc<dyn ExecutionPlan>,
}

impl ExecutionPlan for SeafowlBaseTableScanNode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.inner.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.partitions.len())
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

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        self.inner.fmt_as(t, f)
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

#[derive(Debug)]
pub struct SeafowlFunction {
    pub function_id: FunctionId,
    pub name: String,
    pub details: CreateFunctionDetails,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use bytes::{BufMut, Bytes, BytesMut};
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::{col, lit, or, Expr};
    use datafusion::{
        arrow::{
            array::{ArrayRef, Int64Array},
            record_batch::RecordBatch,
        },
        assert_batches_eq,
        datasource::TableProvider,
        parquet::{arrow::ArrowWriter, file::properties::WriterProperties},
        physical_plan::collect,
        prelude::{SessionConfig, SessionContext},
    };
    use mockall::predicate;
    use object_store::{memory::InMemory, path::Path, ObjectStore};
    use test_case::test_case;

    use crate::data_types::PhysicalPartitionId;
    use crate::provider::{PartitionColumn, SeafowlPruningStatistics};
    use crate::{
        catalog::MockPartitionCatalog,
        context::{scalar_value_to_bytes, INTERNAL_OBJECT_STORE_SCHEME},
        provider::{SeafowlPartition, SeafowlTable},
        schema,
    };

    /// Helper function to make a SeafowlTable pointing to a small batch of data
    async fn make_table_with_batch() -> (SessionContext, SeafowlTable) {
        // Make a batch
        let c1: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));
        let c2: ArrayRef = Arc::new(StringArray::from(vec!["one", "two", "none"]));
        let batch =
            RecordBatch::try_from_iter(vec![("c1", c1.clone()), ("c2", c2.clone())])
                .unwrap();

        // Write a Parquet file to the object store
        let buf = BytesMut::new();
        let mut writer = buf.writer();

        let props = WriterProperties::builder().build();
        let mut arrow_writer =
            ArrowWriter::try_new(&mut writer, batch.schema(), Some(props))
                .expect("creating writer");

        arrow_writer.write(&batch).expect("Writing batch");
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
        context.runtime_env().register_object_store(
            INTERNAL_OBJECT_STORE_SCHEME,
            "",
            Arc::new(object_store),
        );

        let mut catalog = MockPartitionCatalog::new();

        catalog
            .expect_load_table_partitions()
            .with(predicate::eq(1))
            .returning(|_| {
                Ok(vec![SeafowlPartition {
                    partition_id: Some(1),
                    object_storage_id: Arc::from("some-file.parquet"),
                    row_count: 3,
                    columns: Arc::new(vec![]),
                }])
            });

        let table = SeafowlTable {
            name: Arc::from("table"),
            schema: Arc::new(schema::Schema {
                arrow_schema: batch.schema(),
            }),
            table_id: 1,
            table_version_id: 1,
            catalog: Arc::new(catalog),
        };

        (context, table)
    }

    #[tokio::test]
    async fn test_scan_plan() {
        let (context, table) = make_table_with_batch().await;
        let state = context.state.read().clone();

        let plan = table
            .scan(&state, &None, &[], None)
            .await
            .expect("error creating plan");
        let results = collect(plan, context.task_ctx())
            .await
            .expect("error running");
        let expected = vec![
            "+----+------+",
            "| c1 | c2   |",
            "+----+------+",
            "| 1  | one  |",
            "| 2  | two  |",
            "|    | none |",
            "+----+------+",
        ];

        assert_batches_eq!(expected, &results);
    }

    #[tokio::test]
    async fn test_scan_plan_projection() {
        let (context, table) = make_table_with_batch().await;
        let state = context.state.read().clone();

        let plan = table
            .scan(&state, &Some(vec![1]), &[], None)
            .await
            .expect("error creating plan");
        let results = collect(plan, context.task_ctx())
            .await
            .expect("error running");

        let expected = vec![
            "+------+", "| c2   |", "+------+", "| one  |", "| two  |", "| none |",
            "+------+",
        ];

        assert_batches_eq!(expected, &results);
    }

    #[test_case(
        vec![(Some(10), Some(20), Some(0)), (Some(20), None, Some(0)), (Some(30), Some(40), None)],
        vec![col("some_int").gt_eq(lit(25))],
        vec![1, 2];
        "Partition with missing max")
    ]
    #[test_case(
        vec![(Some(10), Some(20), None), (Some(20), Some(30), Some(0)), (Some(30), Some(40), Some(1))],
        vec![col("some_int").gt(lit(15)), col("some_int").lt(lit(25))],
        vec![0, 1];
        "Multiple expressions")
    ]
    #[test_case(
        vec![(Some(10), Some(20), None), (Some(20), Some(30), Some(0)), (Some(30), Some(40), Some(1))],
        vec![or(col("some_int").eq(lit(15)), col("some_int").eq(lit(25))), col("some_int").gt(lit(20))],
        vec![1];
        "Disjunction + AND")
    ]
    #[test_case(
        vec![(Some(10), Some(20), Some(0)), (Some(20), None, Some(0))],
        vec![col("some_int").is_null()],
        vec![];
        "Null check zero nulls")
    ]
    #[test_case(
        vec![(Some(10), Some(20), Some(0)), (Some(20), None, Some(1))],
        vec![col("some_int").is_null()],
        vec![1];
        "Null check one null")
    ]
    #[test_case(
        vec![(Some(10), Some(20), Some(0)), (Some(20), None, None)],
        vec![col("some_int").is_null()],
        vec![1];
        "Null check unknown nulls")
    ]
    #[tokio::test]
    async fn test_partition_pruning(
        part_stats: Vec<(Option<i32>, Option<i32>, Option<i32>)>,
        filters: Vec<Expr>,
        expected: Vec<usize>,
    ) {
        // Dummy schema for the tests
        let schema = Arc::new(Schema::new(vec![Field::new(
            "some_int",
            DataType::Int32,
            true,
        )]));

        // Create some fake partitions
        let mut partitions = vec![];
        for (ind, (min, max, null_count)) in part_stats.iter().enumerate() {
            let min_value = Arc::from(
                min.and_then(|v| scalar_value_to_bytes(&ScalarValue::Int32(Some(v)))),
            );
            let max_value = Arc::from(
                max.and_then(|v| scalar_value_to_bytes(&ScalarValue::Int32(Some(v)))),
            );

            partitions.push(SeafowlPartition {
                partition_id: Some(ind as PhysicalPartitionId),
                object_storage_id: Arc::from(format!("par{}.parquet", ind)),
                row_count: 3,
                columns: Arc::new(vec![PartitionColumn {
                    name: Arc::from("some_int".to_string()),
                    r#type: Arc::from(
                        "{\"name\":\"int\",\"bitWidth\":32,\"isSigned\":true}"
                            .to_string(),
                    ),
                    min_value,
                    max_value,
                    null_count: *null_count,
                }]),
            })
        }

        // Create the main partition pruning handler
        let pruning_stats =
            SeafowlPruningStatistics::from_partitions(partitions.clone(), schema)
                .unwrap();

        // Prune the partitions
        let pruned = pruning_stats.prune(filters.as_slice()).await;

        // Ensure pruning worked correctly
        assert_eq!(pruned.len(), expected.len());
        for (ind, &i) in expected.iter().enumerate() {
            assert_eq!(pruned[ind], partitions[i])
        }
    }
}
