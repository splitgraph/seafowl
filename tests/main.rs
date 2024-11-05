// Single main.rs for all integration tests
// https://endler.dev/2020/rust-compile-times/#combine-all-integration-tests-in-a-single-binary

use arrow_flight::FlightClient;
use assert_cmd::prelude::*;
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_credential_types::Credentials;
use aws_sdk_sts::Client;
use rstest::fixture;
use seafowl::config::schema::{load_config_from_string, SeafowlConfig};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::process::{Child, Command};
use std::time::Duration;
use tokio::net::TcpListener;
use tonic::transport::Channel;
use tracing::warn;

mod clade;
mod cli;
mod fixtures;
mod flight;
mod http;
mod statements;

struct TestSeafowl {
    config: SeafowlConfig,
    channel: Channel,
    proc: Child,
}

impl TestSeafowl {
    pub fn flight_client(&self) -> FlightClient {
        FlightClient::new(self.channel.clone())
    }

    pub fn http_base(&self) -> String {
        let http = self
            .config
            .frontend
            .http
            .clone()
            .expect("HTTP frontend configured");
        format!("http://{}:{}", http.bind_host, http.bind_port)
    }

    pub fn metrics_port(&self) -> u16 {
        self.config
            .misc
            .metrics
            .clone()
            .expect("Metrics configured")
            .port
    }
}

// Actual Seafowl target running in a separate process
#[fixture]
#[allow(clippy::zombie_processes)]
pub async fn test_seafowl() -> TestSeafowl {
    // Pick free ports for the frontends
    let http_addr = get_addr().await;
    let flight_addr = get_addr().await;
    let postgres_addr = get_addr().await;
    let metrics_addr = get_addr().await;

    let env_vars = HashMap::<String, String>::from([
        ("SEAFOWL__CATALOG__TYPE".to_string(), "sqlite".to_string()),
        ("SEAFOWL__CATALOG__DSN".to_string(), ":memory:".to_string()),
        (
            "SEAFOWL__OBJECT_STORE__TYPE".to_string(),
            "memory".to_string(),
        ),
        (
            "SEAFOWL__FRONTEND__HTTP__BIND_PORT".to_string(),
            http_addr.port().to_string(),
        ),
        (
            "SEAFOWL__FRONTEND__FLIGHT__BIND_PORT".to_string(),
            flight_addr.port().to_string(),
        ),
        (
            "SEAFOWL__FRONTEND__POSTGRES__BIND_PORT".to_string(),
            postgres_addr.port().to_string(),
        ),
        (
            "SEAFOWL__MISC__METRICS__PORT".to_string(),
            metrics_addr.port().to_string(),
        ),
    ]);

    let config =
        load_config_from_string("", true, Some(env_vars.clone())).expect("config");

    // Start the process
    // TODO: build context out of the env vars and return it as a field in `TestSeafowl`
    let mut child = Command::cargo_bin("seafowl")
        .expect("seafowl bin exists")
        .envs(env_vars)
        .spawn()
        .expect("seafowl started");

    // Try to connect to the client
    let max_retries = 5;
    let mut retries = 0;
    let mut delay = Duration::from_millis(200);
    loop {
        match Channel::from_shared(format!("http://{flight_addr}"))
            .expect("Endpoint created")
            .connect()
            .await
        {
            Ok(channel) => {
                return TestSeafowl {
                    channel,
                    config,
                    proc: child,
                }
            }
            Err(err) if retries < max_retries => {
                warn!(
                    "Connection attempt {}/{} to Seafowl failed: {}",
                    retries + 1,
                    max_retries,
                    err
                );
                tokio::time::sleep(delay).await;
                retries += 1;
                delay *= 2;
            }
            Err(err) => {
                let _ = child.kill();
                panic!(
                    "Failed to connect to the test Seafowl after {} retries: {:?}",
                    retries, err
                );
            }
        }
    }
}

// Custom Drop impl to try and explicitly kill the Seafowl process
impl Drop for TestSeafowl {
    fn drop(&mut self) {
        if let Err(err) = self.proc.kill() {
            println!(
                "Failed to terminate the test Seafowl process {}: {err}",
                self.proc.id()
            )
        }
    }
}

// Create a local address and bind to a free port
async fn get_addr() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap()
}

enum AssumeRoleTarget {
    MinioUser,
    DexOIDC,
}

// Get temporary creds for a specific role in MinIO. The hard-coded configs stem from the
// values used in the `docker-compose.yml` file.
async fn get_sts_creds(role: AssumeRoleTarget) -> (String, String, String) {
    match role {
        AssumeRoleTarget::MinioUser => {
            let root_creds = Credentials::from_keys("minioadmin", "minioadmin", None);

            let config = aws_config::SdkConfig::builder()
                .credentials_provider(SharedCredentialsProvider::new(root_creds))
                .region(aws_config::Region::new("us-east-1"))
                .endpoint_url("http://localhost:9000")
                .build();

            let creds = Client::new(&config)
                .assume_role()
                .role_arn("test-user")
                .send()
                .await
                .unwrap()
                .credentials
                .expect("MinIO STS credentials provided");

            (
                creds.access_key_id,
                creds.secret_access_key,
                creds.session_token,
            )
        }
        AssumeRoleTarget::DexOIDC => {
            let client = reqwest::Client::new();

            // Get a token from Dex
            let url = "http://localhost:5556/dex/token";
            let params = [
                ("grant_type", "password"),
                ("client_id", "example-app"),
                ("client_secret", "ZXhhbXBsZS1hcHAtc2VjcmV0"),
                ("username", "admin@example.com"),
                ("password", "password"),
                ("scope", "groups openid profile email offline_access"),
            ];
            let response = client.post(url).form(&params).send().await.unwrap();

            let status = response.status();
            let body = response.text().await.unwrap();
            assert_eq!(status, 200, "Dex token request failed: {body}");
            let res: serde_json::Value = serde_json::from_str(&body).unwrap();
            let dex_token = res.get("access_token").expect("token present");

            // Exchange Dex token for valid MinIO STS credentials using the AssumeRoleWithWebIdentity
            // action.
            let config = aws_config::SdkConfig::builder()
                .region(aws_config::Region::new("us-east-1"))
                .endpoint_url("http://localhost:9000")
                .build();

            let creds = Client::new(&config)
                .assume_role_with_web_identity()
                .web_identity_token(dex_token.as_str().unwrap())
                .send()
                .await
                .unwrap()
                .credentials
                .expect("MinIO STS credentials provided");

            (
                creds.access_key_id,
                creds.secret_access_key,
                creds.session_token,
            )
        }
    }
}
