use clade::schema::{
    ListSchemaResponse, SchemaObject, StorageLocation, TableFormat, TableObject,
};
use object_store::aws::AmazonS3ConfigKey;
use object_store::gcp::GoogleConfigKey;
use object_store::ClientConfigKey;
use serde_json::json;
use std::collections::HashMap;
use std::path::Path;

pub const FAKE_GCS_CREDS_PATH: &str = "/tmp/fake-gcs-server.json";

pub fn fake_gcs_creds() -> String {
    let creds_json = json!({"gcs_base_url": "http://localhost:4443", "disable_oauth": true, "client_email": "", "private_key": "", "private_key_id": ""});
    // gcs_base_url should match docker-compose.yml:fake-gcs-server
    let google_application_credentials_path = Path::new(FAKE_GCS_CREDS_PATH);
    std::fs::write(
        google_application_credentials_path,
        serde_json::to_vec(&creds_json).expect("Unable to serialize creds JSON"),
    )
    .expect("Unable to write application credentials JSON file");

    google_application_credentials_path.display().to_string()
}

// Return a list of schemas with actual tables and object store configs that are used in testing
pub fn schemas(include_file_without_store: bool) -> ListSchemaResponse {
    let mut local_schema_tables = vec![TableObject {
        name: "file_with_store".to_string(),
        path: "delta-0.8.0-partitioned".to_string(),
        store: Some("local_fs".to_string()),
        format: TableFormat::Delta.into(),
    }];

    if include_file_without_store {
        local_schema_tables.push(TableObject {
            name: "file".to_string(),
            path: "delta-0.8.0-partitioned".to_string(),
            store: None,
            format: TableFormat::Delta.into(),
        })
    }

    ListSchemaResponse {
        schemas: vec![
            SchemaObject {
                name: "local".to_string(),
                tables: local_schema_tables,
            },
            SchemaObject {
                name: "s3".to_string(),
                tables: vec![
                    TableObject {
                        name: "minio".to_string(),
                        path: "test-data/delta-0.8.0-partitioned".to_string(),
                        store: Some("minio".to_string()),
                        format: TableFormat::Delta.into(),
                    },
                    TableObject {
                        name: "minio_prefix".to_string(),
                        path: "delta-0.8.0-partitioned".to_string(),
                        store: Some("minio-prefix".to_string()),
                        format: TableFormat::Delta.into(),
                    },
                ],
            },
            SchemaObject {
                name: "gcs".to_string(),
                tables: vec![TableObject {
                    name: "fake".to_string(),
                    path: "delta-0.8.0-partitioned".to_string(),
                    store: Some("fake-gcs".to_string()),
                    format: TableFormat::Delta.into(),
                }],
            },
        ],
        stores: vec![
            StorageLocation {
                name: "minio".to_string(),
                location: "s3://seafowl-test-bucket".to_string(),
                options: minio_options(),
            },
            StorageLocation {
                name: "minio-prefix".to_string(),
                location: "s3://seafowl-test-bucket/test-data".to_string(),
                options: minio_options(),
            },
            StorageLocation {
                name: "fake-gcs".to_string(),
                location: "gs://test-data".to_string(),
                options: HashMap::from([(
                    GoogleConfigKey::ServiceAccount.as_ref().to_string(),
                    fake_gcs_creds(),
                )]),
            },
            StorageLocation {
                name: "local_fs".to_string(),
                location: format!(
                    "file://{}/tests/data/",
                    std::env::var("CARGO_MANIFEST_DIR").unwrap()
                ),
                options: HashMap::new(),
            },
        ],
    }
}

pub fn minio_options() -> HashMap<String, String> {
    HashMap::from([
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
            AmazonS3ConfigKey::Client(ClientConfigKey::AllowHttp)
                .as_ref()
                .to_string(),
            "true".to_string(),
        ),
    ])
}
