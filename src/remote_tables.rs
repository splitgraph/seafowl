use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use connectorx::prelude::{get_arrow, CXQuery, SourceConn};
use datafusion::common::DataFusionError;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_expr::{Expr, TableType};
use std::any::Any;
use std::ops::Deref;
use std::sync::Arc;

// Implementation of a remote table, capable of querying Postgres, MySQL, SQLite, etc...
pub struct RemoteTable {
    name: Arc<str>,
    schema: SchemaRef,
    source_conn: Arc<SourceConn>,
}

impl RemoteTable {
    pub fn new(name: String, conn: &str, mut schema: SchemaRef) -> Result<Self> {
        let source_conn = SourceConn::try_from(conn).map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed initialising the remote table {:?}",
                e
            ))
        })?;

        if schema.fields().is_empty() {
            let one_row = &[CXQuery::from(
                format!("SELECT * FROM {} LIMIT 1", name).as_str(),
            )];

            // Introspect the schema
            schema = get_arrow(&source_conn, None, one_row)
                .map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed introspecting the schema {:?}",
                        e
                    ))
                })?
                .arrow_schema();
        }

        Ok(Self {
            name: Arc::from(name),
            schema,
            source_conn: Arc::new(source_conn),
        })
    }
}

#[async_trait]
impl TableProvider for RemoteTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        _ctx: &SessionState,
        projection: &Option<Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // TODO: partition query by some column to utilize concurrent fetching
        // TODO: try to push down the filters: re-construct the WHERE clause for the remote table
        // source type at hand if possible, and append to query
        // TODO: apply limit
        // TODO: apply projection to skip fetching redundant columns
        let queries = &[CXQuery::from(
            format!("SELECT * FROM {}", self.name).as_str(),
        )];

        // TODO: prettify the errors a bit
        let destination =
            get_arrow(self.source_conn.deref(), None, queries).map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed running the remote query {:?}",
                    e
                ))
            })?;
        let data = destination.arrow().map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed fetching the remote table data {:?}",
                e
            ))
        })?;

        Ok(Arc::new(MemoryExec::try_new(
            &[data],
            self.schema(),
            projection.clone(),
        )?))
    }
}
