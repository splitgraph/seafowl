use clade::schema::{
    schema_store_service_server::{SchemaStoreService, SchemaStoreServiceServer},
    ListSchemaRequest, ListSchemaResponse, SchemaObject, StorageLocation, TableObject,
    FILE_DESCRIPTOR_SET,
};
use datafusion_common::assert_batches_eq;
use object_store::aws::AmazonS3ConfigKey;
use rstest::rstest;
use seafowl::catalog::DEFAULT_DB;
use seafowl::config::context::build_context;
use seafowl::config::schema::load_config_from_string;
use seafowl::context::SeafowlContext;
use std::collections::HashMap;
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

async fn start_clade_server() -> Arc<SeafowlContext> {
    // let OS choose a a free port
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let config_text = format!(
        r#"
[object_store]
type = "local"
data_dir = "tests/data"

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
    let metastore = TestCladeMetastore {
        catalog: DEFAULT_DB.to_string(),
        schemas: ListSchemaResponse {
            schemas: vec![
                SchemaObject {
                    name: "local".to_string(),
                    stores: vec![],
                    tables: vec![TableObject {
                        name: "file".to_string(),
                        path: "delta-0.8.0-partitioned".to_string(),
                        location: None,
                    }],
                },
                SchemaObject {
                    name: "s3".to_string(),
                    stores: vec![StorageLocation {
                        location: "s3://seafowl-test-bucket".to_string(),
                        options: HashMap::from([
                            (
                                AmazonS3ConfigKey::Endpoint.as_ref().to_string(),
                                "http://127.0.0.1:9000".to_string(),
                            ),
                            (
                                AmazonS3ConfigKey::AccessKeyId.as_ref().to_string(),
                                "minioadmin".to_string(),
                            ),
                            (
                                AmazonS3ConfigKey::SecretAccessKey.as_ref().to_string(),
                                "minioadmin".to_string(),
                            ),
                            (
                                // This has been removed from the config enum, but it can
                                // still be picked up via `AmazonS3ConfigKey::from_str`
                                "aws_allow_http".to_string(),
                                "true".to_string(),
                            ),
                        ]),
                    }],
                    tables: vec![TableObject {
                        name: "minio".to_string(),
                        path: "test-data/delta-0.8.0-partitioned".to_string(),
                        location: Some("s3://seafowl-test-bucket".to_string()),
                    }],
                },
            ],
        },
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
