use crate::filter_pushdown::{
    filter_expr_to_sql, quote_identifier_backticks, quote_identifier_double_quotes,
    MySQLFilterPushdown, PostgresFilterPushdown, SQLiteFilterPushdown,
};
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use connectorx::prelude::{get_arrow, ArrowDestination, CXQuery, SourceConn, SourceType};
use datafusion::catalog::Session;
use datafusion::common::DataFusionError;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::physical_expr::expressions::{cast, col};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_expr::utils::conjunction;
use datafusion_expr::{Expr, TableProviderFilterPushDown, TableType};
use std::any::Any;
use std::ops::Deref;
use std::sync::Arc;
use tokio::task;
use tracing::debug;

// Implementation of a remote table, capable of querying Postgres, MySQL, SQLite, etc...
#[derive(Debug)]
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
                "Failed initialising the remote table connection {e:?}"
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
                format!("SELECT * FROM {name} LIMIT 1").as_str(),
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
                    "Failed running the remote query {e:?}"
                ))
            })
        })
        .await
        .map_err(|e| {
            DataFusionError::Execution(format!("Failed executing the remote query {e:?}"))
        })?
    }

    // Convert the DataFusion expression representing a filter to an equivalent SQL string for the
    // remote data source if the entire filter can be pushed down.
    fn filter_expr_to_sql(&self, filter: &Expr) -> Option<String> {
        let result = match self.source_conn.ty {
            SourceType::Postgres => filter_expr_to_sql(filter, PostgresFilterPushdown {}),
            SourceType::SQLite => filter_expr_to_sql(filter, SQLiteFilterPushdown {}),
            SourceType::MySQL => filter_expr_to_sql(filter, MySQLFilterPushdown {}),
            _ => {
                debug!(
                    "Filter not shippable due to unsupported source type {:?}",
                    self.source_conn.ty
                );
                return None;
            }
        };

        result
            .map_err(|err| debug!("Failed constructing SQL for filter {filter}: {err}"))
            .ok()
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
        _ctx: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // TODO: partition query by some column to utilize concurrent fetching

        // Scope down the schema and query column specifiers if a projection is specified
        let mut schema = self.schema.deref().clone();
        let mut columns = "*".to_string();

        if let Some(indices) = projection {
            schema = schema.project(indices)?;
            columns = schema
                .fields()
                .iter()
                .map(|f| match self.source_conn.ty {
                    SourceType::MySQL => quote_identifier_backticks(f.name()),
                    _ => quote_identifier_double_quotes(f.name()),
                })
                .collect::<Vec<String>>()
                .join(", ")
        }

        // Apply LIMIT if any
        let limit_clause = limit.map_or("".to_string(), |size| format!(" LIMIT {size}"));

        // Try to construct the WHERE clause: all passed filters should be eligible for pushdown as
        // they've past the checks in `supports_filter_pushdown`
        let where_clause = if filters.is_empty() {
            "".to_string()
        } else {
            // NB: Given that all supplied filters have passed the shipabilty check individually,
            // there should be no harm in merging them together and converting that to equivalent SQL
            let merged_filter = conjunction(filters.to_vec()).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "Failed merging received filters into one {filters:?}"
                ))
            })?;
            let filters_sql =
                self.filter_expr_to_sql(&merged_filter).ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "Failed converting filter to SQL {merged_filter}"
                    ))
                })?;
            format!(" WHERE {filters_sql}")
        };

        // Construct and run the remote query
        let queries = vec![CXQuery::from(
            format!(
                "SELECT {} FROM {}{}{}",
                columns, self.name, where_clause, limit_clause
            )
            .as_str(),
        )];

        let arrow_data = self.run_queries(queries).await?;
        let src_schema = arrow_data.arrow_schema().deref().clone();
        let data = arrow_data.arrow().map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed extracting the fetched data {e:?}"
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

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|f| {
                if self.filter_expr_to_sql(f).is_none() {
                    TableProviderFilterPushDown::Unsupported
                } else {
                    // NB: We can keep this Exact since DF will optimize the plan by preserving the un-shippable
                    // filter nodes for itself, and pass only shippable ones to the scan function.
                    // On the other hand when all filter expressions are (exactly) shippable any limit clause
                    // will also get pushed down, providing additional optimization.
                    TableProviderFilterPushDown::Exact
                }
            })
            .collect())
    }
}
