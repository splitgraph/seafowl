use crate::catalog::{
    CatalogResult, CatalogStore, FunctionStore, SchemaStore, TableStore,
};
use crate::repository::interface::AllDatabaseFunctionsResult;
use clade::schema::schema_store_service_client::SchemaStoreServiceClient;
use clade::schema::{ListSchemaRequest, ListSchemaResponse};
use tonic::transport::{channel::Channel, Endpoint, Error};
use tonic::Request;

// An external store, facilitated via a remote clade server implementation
#[derive(Clone)]
pub struct ExternalStore {
    client: SchemaStoreServiceClient<Channel>,
}

impl ExternalStore {
    // Create a new external store implementing the clade interface
    pub async fn new(dsn: String) -> Result<Self, Error> {
        let endpoint = Endpoint::new(dsn)?.connect_lazy();
        let client = SchemaStoreServiceClient::new(endpoint);
        Ok(Self { client })
    }

    // Tonic client implementations always end up needing mut references, and apparently the way
    // to go is cloning a client instance instead of introducing synchronization primitives:
    // https://github.com/hyperium/tonic/issues/33#issuecomment-538154015
    fn client(&self) -> SchemaStoreServiceClient<Channel> {
        self.client.clone()
    }
}

#[tonic::async_trait]
impl CatalogStore for ExternalStore {}

#[tonic::async_trait]
impl SchemaStore for ExternalStore {
    async fn list(&self, catalog_name: &str) -> CatalogResult<ListSchemaResponse> {
        let req = Request::new(ListSchemaRequest {
            catalog_name: catalog_name.to_string(),
        });

        let response = self.client().list_schemas(req).await?;
        Ok(response.into_inner())
    }
}

#[tonic::async_trait]
impl TableStore for ExternalStore {}

#[tonic::async_trait]
impl FunctionStore for ExternalStore {
    async fn list(
        &self,
        _catalog_name: &str,
    ) -> CatalogResult<Vec<AllDatabaseFunctionsResult>> {
        Ok(vec![])
    }
}
