use crate::remote_tables::pushdown_visitor::{
    FilterPushdown, FilterPushdownVisitor, PostgresFilterPushdown, SQLiteFilterPushdown,
};
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use connectorx::prelude::{get_arrow, ArrowDestination, CXQuery, SourceConn, SourceType};
use datafusion::common::DataFusionError;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use datafusion::physical_expr::expressions::{cast, col};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_expr::expr_visitor::ExprVisitable;
use datafusion_expr::{Expr, TableProviderFilterPushDown, TableType};
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
    source_conn: SourceConn,
}

impl RemoteTable {
    pub async fn new(name: String, conn: String, schema: SchemaRef) -> Result<Self> {
        let mut source_conn = SourceConn::try_from(conn.as_str()).map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed initialising the remote table connection {:?}",
                e
            ))
        })?;
        if conn.contains("/ddn") {
            // Splitgraph DDN disallows COPY statements that are used in the default binary protocol
            source_conn.set_protocol("cursor");
        }

        let mut remote_table = Self {
            name: Arc::from(name.clone()),
            schema: schema.clone(),
            source_conn,
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
        let source_conn = self.source_conn.clone();

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

    fn filter_expr_to_sql<T: FilterPushdownVisitor>(
        &self,
        filter: &Expr,
        source_pushdown: T,
    ) -> Option<String> {
        // Construct the initial visitor state
        let visitor = FilterPushdown {
            source: source_pushdown,
            pushdown_supported: true,
            sql_exprs: vec![],
        };

        // Perform the walk through the expr AST trying to construct the equivalent SQL for the
        // particular source type at hand.
        match filter.accept(visitor) {
            Ok(FilterPushdown {
                pushdown_supported,
                sql_exprs,
                ..
            }) => {
                if pushdown_supported && sql_exprs.len() != 1 {
                    return Some(
                        sql_exprs
                            .first()
                            .expect("Exactly 1 SQL expression expected")
                            .clone(),
                    );
                }
            }
            Err(e) => debug!(
                "Unsupported pushdown of filter expression {}, error: {}",
                filter,
                e.to_string()
            ),
        }

        None
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
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // TODO: partition query by some column to utilize concurrent fetching

        // Scope down the schema and query column specifiers if a projection is specified
        let mut schema = self.schema.deref().clone();
        let mut columns = "*".to_string();

        // TODO: Below we escape the double quotes in column names with PG-specific double double
        // quotes; this will need to be customized according to the specific source type used once
        // we start supporting more types.
        if let Some(indices) = projection {
            schema = schema.project(indices)?;
            columns = schema
                .fields()
                .iter()
                .map(|f| format!("\"{}\"", f.name().replace('\"', "\"\"")))
                .collect::<Vec<String>>()
                .join(", ")
        }

        // Apply LIMIT if any
        let limit_clause = limit.map_or("".to_string(), |size| format!(" LIMIT {size}"));

        // Construct and run the remote query
        let queries = vec![CXQuery::from(
            format!("SELECT {} FROM {}{}", columns, self.name, limit_clause).as_str(),
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
            None,
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

    fn supports_filter_pushdown(
        &self,
        filter: &Expr,
    ) -> Result<TableProviderFilterPushDown> {
        // TODO: add caching of filter to expr mapping (here and in scan)?
        let maybe_filter = match self.source_conn.ty {
            SourceType::Postgres => {
                let postgres_pushdown = PostgresFilterPushdown {};
                self.filter_expr_to_sql(filter, postgres_pushdown)
            }
            SourceType::SQLite => {
                let sqlite_pushdown = SQLiteFilterPushdown {};
                self.filter_expr_to_sql(filter, sqlite_pushdown)
            }
            _ => return Ok(TableProviderFilterPushDown::Unsupported),
        };

        if maybe_filter.is_none() {
            return Ok(TableProviderFilterPushDown::Unsupported);
        }
        Ok(TableProviderFilterPushDown::Exact)
    }
}
