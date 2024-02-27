use serde_json::json;
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
