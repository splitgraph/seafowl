/// Empty metastore that raises errors if the API caller didn't pass an inline metastore
/// over the Flight gRPC interface.
use crate::catalog::{
    CatalogResult, CatalogStore, FunctionStore, SchemaStore, TableStore,
};
use crate::repository::interface::AllDatabaseFunctionsResult;
use async_trait::async_trait;
use clade::schema::ListSchemaResponse;

use super::CatalogError;

#[derive(Debug, Clone)]
pub struct EmptyStore {}

#[async_trait]
impl CatalogStore for EmptyStore {}

#[async_trait]
impl SchemaStore for EmptyStore {
    async fn list(&self, _catalog_name: &str) -> CatalogResult<ListSchemaResponse> {
        Err(CatalogError::NoInlineMetastore)
    }
}

#[async_trait]
impl TableStore for EmptyStore {}

#[tonic::async_trait]
impl FunctionStore for EmptyStore {
    async fn list(
        &self,
        _catalog_name: &str,
    ) -> CatalogResult<Vec<AllDatabaseFunctionsResult>> {
        Ok(vec![])
    }
}
