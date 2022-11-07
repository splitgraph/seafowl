use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use connectorx::prelude::{get_arrow, ArrowDestination, CXQuery, SourceConn};
use datafusion::common::DataFusionError;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use datafusion::physical_expr::expressions::{cast, col};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_expr::{Expr, TableType};
use log::debug;
use std::any::Any;
use std::ops::Deref;
use std::sync::Arc;
use tokio::task;

// Implementation of a remote table, capable of querying Postgres, MySQL, SQLite, etc...
pub struct RemoteTable {
    // We manually escape the field names during scans, but expect the user to escape the table name
    // appropriately in the remote table definition
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

        // Scope down the schema and query column specifiers if a projection is specified
        let mut schema = self.schema.deref().clone();
        let mut columns = "*".to_string();

        if let Some(indices) = projection {
            schema = schema.project(indices)?;
            columns = schema
                .fields()
                .iter()
                .map(|f| format!("\"{}\"", f.name()))
                .collect::<Vec<String>>()
                .join(", ")
        }

        // Construct and run the remote query
        let queries = vec![CXQuery::from(
            format!("SELECT {} FROM {}", columns, self.name).as_str(),
        )];
        let arrow_data = self.run_queries(queries).await?;
        let src_schema = arrow_data.arrow_schema().deref().clone();
        let data = arrow_data.arrow().map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed extracting the fetched data {:?}",
                e
            ))
        })?;

        let mut plan: Arc<dyn ExecutionPlan> = Arc::new(MemoryExec::try_new(
            &[data],
            Arc::new(src_schema.clone()),
            projection.clone(),
        )?);

        if src_schema != schema {
            // Try to cast each referenced column to the data type originally specified by the user
            let cast_exprs: Result<Vec<(Arc<dyn PhysicalExpr>, String)>> = schema
                .fields
                .iter()
                .zip(src_schema.fields().iter())
                .map(|(f, src_f)| {
                    let col_expr = col(f.name(), &schema)?;

                    if src_f != f {
                        Ok((
                            cast(col_expr, &src_schema, f.data_type().clone())?,
                            f.name().to_string(),
                        ))
                    } else {
                        Ok((col_expr, f.name().to_string()))
                    }
                })
                .collect();

            debug!(
                "Using cast for projecting remote table {} fields: {:?}",
                self.name, cast_exprs
            );
            plan = Arc::new(ProjectionExec::try_new(cast_exprs?, plan)?)
        }

        Ok(plan)
    }
}
