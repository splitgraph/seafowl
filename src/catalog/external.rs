use crate::catalog::{
    not_impl, CatalogResult, CatalogStore, FunctionStore, SchemaStore, TableStore,
};
use crate::repository::interface::{
    AllDatabaseFunctionsResult, CollectionRecord, DatabaseRecord,
    DroppedTableDeletionStatus, DroppedTablesResult, TableId, TableRecord,
    TableVersionId, TableVersionsResult,
};
use crate::wasm_udf::data_types::CreateFunctionDetails;
use arrow_schema::Schema;
use clade::catalog::CatalogReference;
use clade::schema::schema_store_service_client::SchemaStoreServiceClient;
use clade::schema::ListSchemaResponse;
use tonic::transport::{channel::Channel, Error};
use tonic::Request;
use uuid::Uuid;

// An external store, facilitated via a remote clade server implementation
#[derive(Clone)]
pub struct ExternalStore {
    client: SchemaStoreServiceClient<Channel>,
}

impl ExternalStore {
    // Create a new external store implementing the clade interface
    pub async fn new(dsn: String) -> Result<Self, Error> {
        println!("Client dsn is {dsn}");
        let client = SchemaStoreServiceClient::connect(dsn).await?;
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
impl CatalogStore for ExternalStore {
    async fn create(&self, _name: &str) -> CatalogResult<()> {
        not_impl()
    }

    async fn get(&self, _name: &str) -> CatalogResult<DatabaseRecord> {
        not_impl()
    }

    async fn delete(&self, _name: &str) -> CatalogResult<()> {
        not_impl()
    }
}

#[tonic::async_trait]
impl SchemaStore for ExternalStore {
    async fn create(&self, _catalog_name: &str, _schema_name: &str) -> CatalogResult<()> {
        not_impl()
    }

    async fn list(&self, catalog_name: &str) -> CatalogResult<ListSchemaResponse> {
        let req = Request::new(CatalogReference {
            catalog_name: catalog_name.to_string(),
        });

        let response = self.client().list(req).await?;
        Ok(response.into_inner())
    }

    async fn get(
        &self,
        _catalog_name: &str,
        _schema_name: &str,
    ) -> CatalogResult<CollectionRecord> {
        not_impl()
    }

    async fn delete(&self, _catalog_name: &str, _schema_name: &str) -> CatalogResult<()> {
        not_impl()
    }
}

#[tonic::async_trait]
impl TableStore for ExternalStore {
    async fn create(
        &self,
        _catalog_name: &str,
        _schema_name: &str,
        _table_name: &str,
        _schema: &Schema,
        _uuid: Uuid,
    ) -> CatalogResult<(TableId, TableVersionId)> {
        not_impl()
    }

    async fn get(
        &self,
        _catalog_name: &str,
        _schema_name: &str,
        _table_name: &str,
    ) -> CatalogResult<TableRecord> {
        not_impl()
    }

    async fn create_new_version(
        &self,
        _uuid: Uuid,
        _version: i64,
    ) -> CatalogResult<TableVersionId> {
        not_impl()
    }

    async fn delete_old_versions(
        &self,
        _catalog_name: &str,
        _schema_name: &str,
        _table_name: &str,
    ) -> CatalogResult<u64> {
        not_impl()
    }

    async fn get_all_versions(
        &self,
        _catalog_name: &str,
        _table_names: Option<Vec<String>>,
    ) -> CatalogResult<Vec<TableVersionsResult>> {
        not_impl()
    }

    async fn update(
        &self,
        _old_catalog_name: &str,
        _old_schema_name: &str,
        _old_table_name: &str,
        _new_catalog_name: &str,
        _new_schema_name: &str,
        _new_table_name: &str,
    ) -> CatalogResult<()> {
        not_impl()
    }

    async fn delete(
        &self,
        _catalog_name: &str,
        _schema_name: &str,
        _table_name: &str,
    ) -> CatalogResult<()> {
        not_impl()
    }

    async fn get_dropped_tables(
        &self,
        _catalog_name: Option<String>,
    ) -> CatalogResult<Vec<DroppedTablesResult>> {
        not_impl()
    }

    async fn update_dropped_table(
        &self,
        _uuid: Uuid,
        _deletion_status: DroppedTableDeletionStatus,
    ) -> CatalogResult<()> {
        not_impl()
    }

    async fn delete_dropped_table(&self, _uuid: Uuid) -> CatalogResult<()> {
        not_impl()
    }
}

#[tonic::async_trait]
impl FunctionStore for ExternalStore {
    async fn create(
        &self,
        _catalog_name: &str,
        _function_name: &str,
        _or_replace: bool,
        _details: &CreateFunctionDetails,
    ) -> CatalogResult<()> {
        not_impl()
    }

    async fn list(
        &self,
        _catalog_name: &str,
    ) -> CatalogResult<Vec<AllDatabaseFunctionsResult>> {
        Ok(vec![])
    }

    async fn delete(
        &self,
        _catalog_name: &str,
        _if_exists: bool,
        _func_names: &[String],
    ) -> CatalogResult<()> {
        not_impl()
    }
}
