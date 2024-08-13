use crate::fixtures::schemas;
use clade::schema::{
    schema_store_service_server::{SchemaStoreService, SchemaStoreServiceServer},
    ListSchemaRequest, ListSchemaResponse, FILE_DESCRIPTOR_SET,
};
use datafusion_common::assert_batches_eq;
use rstest::rstest;
use seafowl::catalog::DEFAULT_DB;
use seafowl::config::context::build_context;
use seafowl::config::schema::load_config_from_string;
use seafowl::context::SeafowlContext;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

mod query;

struct TestCladeMetastore {
    catalog: String,
    schemas: ListSchemaResponse,
}

#[tonic::async_trait]
impl SchemaStoreService for TestCladeMetastore {
    async fn list_schemas(
        &self,
        request: Request<ListSchemaRequest>,
    ) -> Result<Response<ListSchemaResponse>, Status> {
        let catalog = request.into_inner().catalog_name;
        if self.catalog == catalog {
            Ok(Response::new(self.schemas.clone()))
        } else {
            Err(Status::not_found(format!(
                "Catalog {catalog} does not exist",
            )))
        }
    }
}

async fn start_clade_server(object_store: bool) -> Arc<SeafowlContext> {
    // let OS choose a free port
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let object_store_section = if object_store {
        r#"[object_store]
type = "local"
data_dir = "tests/data""#
    } else {
        ""
    };

    let config_text = format!(
        r#"
{object_store_section}

[catalog]
type = "clade"
dsn = "http://{addr}""#,
    );

    let config = load_config_from_string(&config_text, false, None).unwrap();
    let context = Arc::from(build_context(config).await.unwrap());

    let clade = run_clade_server(addr);
    tokio::task::spawn(clade);

    context
}

async fn run_clade_server(addr: SocketAddr) {
    // Setup a test metastore with some fake tables in test object stores.
    let metastore = TestCladeMetastore {
        catalog: DEFAULT_DB.to_string(),
        schemas: schemas(true),
    };

    let svc = SchemaStoreServiceServer::new(metastore);

    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
        .build()
        .unwrap();

    Server::builder()
        .add_service(svc)
        .add_service(reflection)
        .serve(addr)
        .await
        .unwrap();
}
