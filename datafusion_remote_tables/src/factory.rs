use crate::provider::RemoteTable;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProviderFactory};
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion_expr::CreateExternalTable;
use std::ops::Deref;
use std::sync::Arc;

/// Factory for creating remote tables
#[derive(Debug)]
pub struct RemoteTableFactory {}

#[async_trait]
impl TableProviderFactory for RemoteTableFactory {
    async fn create(
        &self,
        _ctx: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> Result<Arc<dyn TableProvider>> {
        let table = RemoteTable::new(
            cmd.options
                .get("format.name")
                .ok_or(DataFusionError::Execution(
                    "Missing 'name' option".to_string(),
                ))?
                .clone(),
            cmd.location.clone(),
            SchemaRef::from(cmd.schema.deref().clone()),
        )
        .await?;

        Ok(Arc::new(table))
    }
}
