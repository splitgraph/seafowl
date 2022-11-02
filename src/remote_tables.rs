use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use connectorx::prelude::{get_arrow, ArrowDestination, CXQuery, SourceConn};
use datafusion::common::DataFusionError;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_expr::{Expr, TableType};
use std::any::Any;
use std::sync::Arc;
use tokio::task;

// Implementation of a remote table, capable of querying Postgres, MySQL, SQLite, etc...
pub struct RemoteTable {
    name: Arc<str>,
    schema: SchemaRef,
    conn: String,
}

impl RemoteTable {
    pub async fn new(name: String, conn: String, schema: SchemaRef) -> Result<Self> {
        let mut remote_table = Self {
            name: Arc::from(name.clone()),
            schema: schema.clone(),
            conn,
        };

        if schema.fields().is_empty() {
            let one_row = vec![CXQuery::from(
                format!("SELECT * FROM {} LIMIT 1", name).as_str(),
            )];

            // Introspect the schema
            remote_table.schema = remote_table.run_queries(one_row).await?.arrow_schema();
        }

        Ok(remote_table)
    }

    async fn run_queries(
        &self,
        queries: Vec<CXQuery<String>>,
    ) -> Result<ArrowDestination> {
        // TODO: prettify the errors a bit
        let source_conn = SourceConn::try_from(self.conn.as_str()).map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed initialising the remote table connection {:?}",
                e
            ))
        })?;

        task::spawn_blocking(move || {
            get_arrow(&source_conn, None, queries.as_slice()).map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed running the remote query {:?}",
                    e
                ))
            })
        })
        .await
        .map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed executing the remote query {:?}",
                e
            ))
        })?
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
        let queries = vec![CXQuery::from(
            format!("SELECT * FROM {}", self.name).as_str(),
        )];

        let data = self.run_queries(queries).await?.arrow().map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed fetching the remote table data {:?}",
                e
            ))
        })?;

        // TODO: perform type coercion/casting if schema wasn't introspected
        Ok(Arc::new(MemoryExec::try_new(
            &[data],
            self.schema(),
            projection.clone(),
        )?))
    }
}
