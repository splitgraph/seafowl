use crate::catalog::{
    CatalogResult, CatalogStore, FunctionStore, SchemaStore, TableStore,
};
use crate::repository::interface::AllDatabaseFunctionsResult;
use clade::schema::ListSchemaResponse;

#[derive(Debug, Clone)]
pub struct MemoryStore {
    pub schemas: ListSchemaResponse,
}

#[tonic::async_trait]
impl CatalogStore for MemoryStore {}

#[tonic::async_trait]
impl SchemaStore for MemoryStore {
    async fn list(&self, _catalog_name: &str) -> CatalogResult<ListSchemaResponse> {
        Ok(self.schemas.clone())
    }
}

#[tonic::async_trait]
impl TableStore for MemoryStore {}

#[tonic::async_trait]
impl FunctionStore for MemoryStore {
    async fn list(
        &self,
        _catalog_name: &str,
    ) -> CatalogResult<Vec<AllDatabaseFunctionsResult>> {
        Ok(vec![])
    }
}
