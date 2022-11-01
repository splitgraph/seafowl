use crate::schema::Schema;
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
use std::sync::Arc;

// Implementation of a remote table, capable of querying Postgres, MySQL, SQLite, etc...
struct RemoteTable {
    name: Arc<str>,
    schema: Arc<Schema>,
    source_conn: SourceConn,
}

impl RemoteTable {
    #[allow(dead_code)]
    fn new(name: String, conn: &str, maybe_schema: Option<Schema>) -> Result<Self> {
        let source_conn = SourceConn::try_from(conn).map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed initialising the remote table {:?}",
                e
            ))
        })?;

        let schema = match maybe_schema {
            Some(schema) => schema,
            None => {
                // TODO: Introspect schema
                Schema::from_column_names_types(vec![].into_iter())
            }
        };

        Ok(Self {
            name: Arc::from(name),
            schema: Arc::new(schema),
            source_conn,
        })
    }
}

#[async_trait]
impl TableProvider for RemoteTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.arrow_schema.clone()
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
        let destination = get_arrow(&self.source_conn, None, queries).map_err(|e| {
            DataFusionError::Execution(format!("Failed running the remote query {:?}", e))
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
