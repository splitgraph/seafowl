use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::{catalog::CatalogProvider, schema::SchemaProvider},
    common::{DataFusionError, Result},
    datasource::TableProvider,
    execution::context::{SessionState, TaskContext},
    logical_expr::TableType,
    logical_plan::Expr,
    physical_expr::PhysicalSortExpr,
    physical_plan::{ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics},
};

use crate::data_types::{Collection, Database};

struct SeafowlCatalog {
    inner: Database,
}

impl CatalogProvider for SeafowlCatalog {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        todo!("List all collection names")
    }

    fn schema(&self, _name: &str) -> Option<Arc<dyn SchemaProvider>> {
        Some(Arc::new(SeafowlSchema { inner: todo!() }));
        todo!("Get a Collection object (store ID, name?)")
    }
}

struct SeafowlSchema {
    inner: Collection,
}

impl SchemaProvider for SeafowlSchema {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        todo!()
    }

    fn table(&self, _name: &str) -> Option<Arc<dyn TableProvider>> {
        todo!()
    }

    fn table_exist(&self, _name: &str) -> bool {
        todo!()
    }
}

struct SeafowlTable {
    // TODO: Metadata for the latest table version here
// (partitions, min/max, schema)
}

#[async_trait]
impl TableProvider for SeafowlTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        todo!()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _ctx: &SessionState,
        _projection: &Option<Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> std::result::Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        // Filter partitions by the predicate
        // Create the node to scan through them
        // No UNION node here?
        Ok(Arc::new(SeafowlBaseTableScanNode {}))
    }
}

#[derive(Debug)]
struct SeafowlBaseTableScanNode {
    // TODO: list of partitions to scan through
}

impl ExecutionPlan for SeafowlBaseTableScanNode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        todo!()
    }

    fn output_partitioning(&self) -> Partitioning {
        todo!()
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
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        todo!()
        // Hit the object store up for a certain partition, scan through it
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}
