//! Mechanism for creating virtual Seafowl system tables, inspired by influxdb_iox system tables
//! and datafusion's information_schema.

use crate::catalog::TableStore;
use crate::repository::interface::DroppedTablesResult;
use arrow::array::{Int64Builder, StringBuilder, StructBuilder, TimestampSecondBuilder};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::schema::SchemaProvider;
use datafusion::common::DataFusionError;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_expr::{Expr, TableType};
use std::any::Any;
use std::sync::Arc;

pub const SYSTEM_SCHEMA: &str = "system";
const TABLE_VERSIONS: &str = "table_versions";
const DROPPED_TABLES: &str = "dropped_tables";

pub struct SystemSchemaProvider {
    database: Arc<str>,
    table_catalog: Arc<dyn TableStore>,
}

impl SystemSchemaProvider {
    pub fn new(database: Arc<str>, table_catalog: Arc<dyn TableStore>) -> Self {
        Self {
            database,
            table_catalog,
        }
    }
}

#[async_trait]
impl SchemaProvider for SystemSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn table_names(&self) -> Vec<String> {
        vec![TABLE_VERSIONS.to_string(), DROPPED_TABLES.to_string()]
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        Ok(match name {
            // Lazy instantiate the tables, but defer loading the rows until the actual scan is invoked.
            TABLE_VERSIONS => {
                let table = TableVersionsTable::new(
                    self.database.clone(),
                    self.table_catalog.clone(),
                );
                Some(Arc::new(SystemTableProvider {
                    table: Arc::new(table),
                }))
            }
            DROPPED_TABLES => {
                let table = DroppedTablesTable::new(
                    self.database.clone(),
                    self.table_catalog.clone(),
                );
                Some(Arc::new(SystemTableProvider {
                    table: Arc::new(table),
                }))
            }
            _ => None,
        })
    }

    fn table_exist(&self, name: &str) -> bool {
        matches!(
            name.to_ascii_lowercase().as_str(),
            TABLE_VERSIONS | DROPPED_TABLES
        )
    }
}

// Base trait for Seafowl system tables, as a way to imitate OOP
#[async_trait]
trait SeafowlSystemTable: Send + Sync {
    /// The schema for this system table
    fn schema(&self) -> SchemaRef;

    /// Get the rows of the system table
    async fn load_record_batch(&self) -> Result<RecordBatch>;
}

/// Adapter that makes any `SeafowlSystemTable` a DataFusion `TableProvider`
struct SystemTableProvider<T: SeafowlSystemTable> {
    table: Arc<T>,
}

#[async_trait]
impl<T> TableProvider for SystemTableProvider<T>
where
    T: SeafowlSystemTable + 'static,
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.table.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    // TODO: Investigate streaming from sqlx instead of loading all the results in memory
    async fn scan(
        &self,
        _ctx: &SessionState,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(MemoryExec::try_new(
            &[vec![self.table.load_record_batch().await?]],
            self.table.schema(),
            projection.cloned(),
        )?))
    }
}

// Table listing all available version for the given database
struct TableVersionsTable {
    database: Arc<str>,
    schema: SchemaRef,
    table_catalog: Arc<dyn TableStore>,
}

impl TableVersionsTable {
    fn new(database: Arc<str>, table_catalog: Arc<dyn TableStore>) -> Self {
        Self {
            // This is dictated by the output of `get_all_versions`, except that we omit the
            // database_name field, since we scope down to the database at hand.
            database,
            schema: Arc::new(Schema::new(vec![
                Field::new("table_schema", DataType::Utf8, false),
                Field::new("table_name", DataType::Utf8, false),
                Field::new("table_version_id", DataType::Int64, false),
                Field::new("version", DataType::Int64, false),
                Field::new(
                    "creation_time",
                    DataType::Timestamp(TimeUnit::Second, None),
                    false,
                ),
            ])),
            table_catalog,
        }
    }
}

#[async_trait]
impl SeafowlSystemTable for TableVersionsTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    async fn load_record_batch(&self) -> Result<RecordBatch> {
        let table_versions = self
            .table_catalog
            .get_all_versions(&self.database, None)
            .await?;

        let mut builder = StructBuilder::from_fields(
            self.schema.fields().clone(),
            table_versions.len(),
        );

        // Construct the table columns from the returned rows
        for table_version in &table_versions {
            builder
                .field_builder::<StringBuilder>(0)
                .unwrap()
                .append_value(table_version.collection_name.clone());
            builder
                .field_builder::<StringBuilder>(1)
                .unwrap()
                .append_value(table_version.table_name.clone());
            builder
                .field_builder::<Int64Builder>(2)
                .unwrap()
                .append_value(table_version.table_version_id);
            builder
                .field_builder::<Int64Builder>(3)
                .unwrap()
                .append_value(table_version.version);
            builder
                .field_builder::<TimestampSecondBuilder>(4)
                .unwrap()
                .append_value(table_version.creation_time);

            builder.append(true);
        }

        let struct_array = builder.finish();

        RecordBatch::try_new(self.schema.clone(), struct_array.columns().to_vec())
            .map_err(DataFusionError::from)
    }
}

// Table listing all dropped tables that are pending lazy deletion on subsequent `VACUUM`s
struct DroppedTablesTable {
    database: Arc<str>,
    schema: SchemaRef,
    table_catalog: Arc<dyn TableStore>,
}

impl DroppedTablesTable {
    fn new(database: Arc<str>, table_catalog: Arc<dyn TableStore>) -> Self {
        Self {
            // This is dictated by the output of `get_dropped_tables`, except that we omit the
            // database_name field, since we scope down to the database at hand.
            database,
            schema: Arc::new(Schema::new(vec![
                Field::new("table_schema", DataType::Utf8, false),
                Field::new("table_name", DataType::Utf8, false),
                Field::new("uuid", DataType::Utf8, false),
                Field::new("deletion_status", DataType::Utf8, false),
                Field::new(
                    "drop_time",
                    DataType::Timestamp(TimeUnit::Second, None),
                    false,
                ),
            ])),
            table_catalog,
        }
    }
}

#[async_trait]
impl SeafowlSystemTable for DroppedTablesTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    async fn load_record_batch(&self) -> Result<RecordBatch> {
        let dropped_tables = self
            .table_catalog
            .get_dropped_tables(Some(self.database.to_string()))
            .await?
            .into_iter()
            .collect::<Vec<DroppedTablesResult>>();

        let mut builder = StructBuilder::from_fields(
            self.schema.fields().clone(),
            dropped_tables.len(),
        );

        // Construct the table columns from the returned rows
        for dropped_table in &dropped_tables {
            builder
                .field_builder::<StringBuilder>(0)
                .unwrap()
                .append_value(dropped_table.collection_name.clone());
            builder
                .field_builder::<StringBuilder>(1)
                .unwrap()
                .append_value(dropped_table.table_name.clone());
            builder
                .field_builder::<StringBuilder>(2)
                .unwrap()
                .append_value(dropped_table.uuid.to_string().clone());
            builder
                .field_builder::<StringBuilder>(3)
                .unwrap()
                .append_value(dropped_table.deletion_status.to_string().clone());
            builder
                .field_builder::<TimestampSecondBuilder>(4)
                .unwrap()
                .append_value(dropped_table.drop_time);

            builder.append(true);
        }

        let struct_array = builder.finish();

        RecordBatch::try_new(self.schema.clone(), struct_array.columns().to_vec())
            .map_err(DataFusionError::from)
    }
}
