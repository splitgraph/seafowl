// Single main.rs for all integration tests
// https://endler.dev/2020/rust-compile-times/#combine-all-integration-tests-in-a-single-binary

use arrow_flight::FlightClient;
use assert_cmd::prelude::*;
use rstest::fixture;
use seafowl::config::schema::{load_config_from_string, SeafowlConfig};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::process::{Child, Command};
use std::time::Duration;
use tokio::net::TcpListener;
use tonic::transport::Channel;

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
    let mut retries = 3;
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
            Err(_err) if retries > 0 => {
                retries -= 1;
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            _ => {
                let _ = child.kill();
                panic!("Failed to connect to the test Seafowl")
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
