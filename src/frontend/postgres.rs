use std::sync::Arc;

use async_trait::async_trait;

use convergence::{
    engine::{Engine, Portal},
    protocol::{ErrorResponse, FieldDescription, SqlState},
    protocol_ext::DataRowBatch,
    server::{self, BindOptions},
};
use convergence_arrow::table::{record_batch_to_rows, schema_to_field_desc};
use datafusion::{error::DataFusionError, physical_plan::ExecutionPlan};

use crate::{config::PostgresFrontend, context::SeafowlContext};
use sqlparser::ast::Statement;

pub struct SeafowlPortal {
    plan: Arc<dyn ExecutionPlan>,
    context: Arc<SeafowlContext>,
}

fn df_err_to_sql(err: DataFusionError) -> ErrorResponse {
    ErrorResponse::error(SqlState::DATA_EXCEPTION, err.to_string())
}

#[async_trait]
impl Portal for SeafowlPortal {
    async fn fetch(&mut self, batch: &mut DataRowBatch) -> Result<(), ErrorResponse> {
        for arrow_batch in self
            .context
            .collect(self.plan.clone())
            .await
            .map_err(df_err_to_sql)?
        {
            record_batch_to_rows(&arrow_batch, batch)?;
        }
        // Reload the schema after every query
        self.context.reload_schema().await;
        Ok(())
    }
}

struct SeafowlConvergenceEngine {
    context: Arc<SeafowlContext>,
}

#[async_trait]
impl Engine for SeafowlConvergenceEngine {
    type PortalType = SeafowlPortal;

    async fn prepare(
        &mut self,
        statement: &Statement,
    ) -> Result<Vec<FieldDescription>, ErrorResponse> {
        let plan = self
            .context
            .create_logical_plan(&statement.to_string())
            .await
            .map_err(df_err_to_sql)?;

        schema_to_field_desc(&plan.schema().as_ref().into())
    }

    async fn create_portal(
        &mut self,
        statement: &Statement,
    ) -> Result<Self::PortalType, ErrorResponse> {
        let plan = self
            .context
            .plan_query(&statement.to_string())
            .await
            .map_err(df_err_to_sql)?;
        Ok(SeafowlPortal {
            plan,
            context: self.context.clone(),
        })
    }
}

pub async fn run_pg_server(context: Arc<SeafowlContext>, config: &PostgresFrontend) {
    server::run(
        BindOptions::new()
            .with_addr(&config.bind_host)
            .with_port(config.bind_port),
        Arc::new(move || {
            let context = context.clone();
            Box::pin(async move {
                SeafowlConvergenceEngine {
                    context: context.clone(),
                }
            })
        }),
    )
    .await
    .unwrap();
}
