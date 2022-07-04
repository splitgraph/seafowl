// DataFusion bindings

use futures::TryStreamExt;
use sqlparser::ast::Statement;
use std::sync::Arc;

pub use datafusion::error::{DataFusionError as Error, Result};
use datafusion::{
    arrow::record_batch::RecordBatch,
    execution::context::TaskContext,
    logical_plan::LogicalPlan,
    physical_plan::{
        coalesce_partitions::CoalescePartitionsExec, EmptyRecordBatchStream, ExecutionPlan,
        SendableRecordBatchStream,
    },
    prelude::SessionContext,
    sql::{parser::DFParser, planner::SqlToRel},
};

struct SeafowlContext {
    inner: SessionContext,
}

impl SeafowlContext {
    pub fn inner(&self) -> &SessionContext {
        &self.inner
    }

    pub async fn plan_query(&self, sql: &str) -> Result<Arc<dyn ExecutionPlan>> {
        let mut statements = DFParser::parse_sql(sql)?;

        if statements.len() != 1 {
            return Err(Error::NotImplemented(
                "The context currently only supports a single SQL statement".to_string(),
            ));
        }

        let state = self.inner.state.read().clone();
        let query_planner = SqlToRel::new(&state);

        let logical_plan = match statements.pop_front().unwrap() {
            datafusion::sql::parser::Statement::Statement(s) => match *s {
                // Delegate SELECT / EXPLAIN to the basic DataFusion logical planner
                // (though note EXPLAIN [our custom query] will mean we have to implement EXPLAIN ourselves)
                Statement::Explain { .. }
                | Statement::Query { .. }
                | Statement::ShowVariable { .. }
                | Statement::ShowColumns { .. } => query_planner.sql_statement_to_plan(*s),

                // These are handled by SqlToRel but we want to create these differently
                Statement::CreateTable { .. } => {
                    todo!()
                }
                Statement::CreateView { .. } => {
                    todo!()
                }
                Statement::CreateSchema { .. } => {
                    todo!()
                }
                Statement::CreateDatabase { .. } => {
                    todo!()
                }
                Statement::Drop { .. } => {
                    todo!()
                }

                // This DML is defined by us
                Statement::Insert { .. } => {
                    todo!()
                }
                _ => Err(Error::NotImplemented(format!(
                    "Unsupported SQL statement: {:?}",
                    sql
                ))),
            },
            datafusion::sql::parser::Statement::DescribeTable(s) => {
                query_planner.describe_table_to_plan(s)
            }
            // Stub out the standard DataFusion CREATE EXTERNAL TABLE statements since we don't support them
            datafusion::sql::parser::Statement::CreateExternalTable(_) => {
                return Err(Error::NotImplemented(format!(
                    "Unsupported SQL statement: {:?}",
                    sql
                )))
            }
        }?;
        self.inner.create_physical_plan(&logical_plan).await
    }

    pub async fn create_physical_plan(&self, plan: &LogicalPlan) -> Result<Arc<dyn ExecutionPlan>> {
        let physical_plan = self.inner.create_physical_plan(plan).await?;
        Ok(physical_plan)
    }

    // Copied from DataFusion's physical_plan
    pub async fn collect(&self, physical_plan: Arc<dyn ExecutionPlan>) -> Result<Vec<RecordBatch>> {
        let stream = self.execute_stream(physical_plan).await?;
        stream.err_into().try_collect().await
    }

    pub async fn execute_stream(
        &self,
        physical_plan: Arc<dyn ExecutionPlan>,
    ) -> Result<SendableRecordBatchStream> {
        match physical_plan.output_partitioning().partition_count() {
            0 => Ok(Box::pin(EmptyRecordBatchStream::new(
                physical_plan.schema(),
            ))),
            1 => self.execute_stream_partitioned(physical_plan, 0).await,
            _ => {
                self.execute_stream_partitioned(
                    Arc::new(CoalescePartitionsExec::new(physical_plan)),
                    0,
                )
                .await
            }
        }
    }

    pub async fn execute_stream_partitioned(
        &self,
        physical_plan: Arc<dyn ExecutionPlan>,
        partition: usize,
    ) -> Result<SendableRecordBatchStream> {
        let task_context = Arc::new(TaskContext::from(self.inner()));
        physical_plan.execute(partition, task_context)
    }
}
