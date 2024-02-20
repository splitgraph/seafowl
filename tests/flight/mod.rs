use arrow::record_batch::RecordBatch;
use arrow_flight::error::Result;
use arrow_flight::sql::{CommandStatementQuery, ProstMessageExt};
use arrow_flight::{FlightClient, FlightDescriptor};
use assert_cmd::prelude::*;
use datafusion_common::assert_batches_eq;
use futures::TryStreamExt;
use prost::Message;
use reqwest::StatusCode;
use rstest::{fixture, rstest};
use std::net::SocketAddr;
use std::process::{Child, Command};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use warp::hyper::Client;

use crate::http::{get_metrics, response_text};
use crate::statements::create_table_and_insert;

use seafowl::config::context::{build_context, GRPC_REQUESTS};
use seafowl::config::schema::load_config_from_string;
use seafowl::context::SeafowlContext;
use seafowl::frontend::flight::run_flight_server;

mod client;
mod e2e;
mod search_path;

struct TestSeafowl {
    cli: FlightClient,
    http_addr: SocketAddr,
    metrics_addr: SocketAddr,
    proc: Child,
}

// Actual Seafowl target running in a separate process
#[fixture]
async fn test_seafowl() -> TestSeafowl {
    // Pick free ports for the frontends
    let http_addr = get_addr().await;
    let flight_addr = get_addr().await;
    let postgres_addr = get_addr().await;
    let metrics_addr = get_addr().await;

    // Start the process
    // TODO: build config/context out of the env vars and return it as a field in `TestSeafowl`
    let mut child = Command::cargo_bin("seafowl")
        .expect("seafowl bin exists")
        .env("SEAFOWL__CATALOG__TYPE", "sqlite")
        .env("SEAFOWL__CATALOG__DSN", ":memory:")
        .env("SEAFOWL__OBJECT_STORE__TYPE", "memory")
        .env(
            "SEAFOWL__FRONTEND__HTTP__BIND_PORT",
            http_addr.port().to_string(),
        )
        .env(
            "SEAFOWL__FRONTEND__FLIGHT__BIND_PORT",
            flight_addr.port().to_string(),
        )
        .env(
            "SEAFOWL__FRONTEND__POSTGRES__BIND_PORT",
            postgres_addr.port().to_string(),
        )
        .env(
            "SEAFOWL__MISC__METRICS__PORT",
            metrics_addr.port().to_string(),
        )
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
                    cli: FlightClient::new(channel),
                    http_addr,
                    metrics_addr,
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

async fn make_test_context() -> Arc<SeafowlContext> {
    // let OS choose a free port
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let config_text = format!(
        r#"
[object_store]
type = "memory"

[catalog]
type = "sqlite"
dsn = ":memory:"

[frontend.flight]
bind_host = "127.0.0.1"
bind_port = {}"#,
        addr.port()
    );

    let config = load_config_from_string(&config_text, false, None).unwrap();

    Arc::from(build_context(config).await.unwrap())
}

async fn flight_server() -> (Arc<SeafowlContext>, FlightClient) {
    let context = make_test_context().await;

    let flight_cfg = context
        .config
        .frontend
        .flight
        .as_ref()
        .expect("Arrow Flight frontend configured")
        .clone();

    let flight = run_flight_server(context.clone(), flight_cfg.clone());
    tokio::task::spawn(flight);

    // Create the channel for the client
    let channel = Channel::from_shared(format!(
        "http://{}:{}",
        flight_cfg.bind_host, flight_cfg.bind_port
    ))
    .expect("Endpoint created")
    .connect_lazy();

    (context, FlightClient::new(channel))
}

async fn get_flight_batches(
    client: &mut FlightClient,
    query: String,
) -> Result<Vec<RecordBatch>> {
    let cmd = CommandStatementQuery {
        query,
        transaction_id: None,
    };
    let request = FlightDescriptor::new_cmd(cmd.as_any().encode_to_vec());
    let response = client.get_flight_info(request).await?;

    // Get the returned ticket
    let ticket = response.endpoint[0]
        .ticket
        .clone()
        .expect("expected ticket");

    // Retrieve the corresponding Flight stream and collect into batches
    let flight_stream = client.do_get(ticket).await?;

    let batches = flight_stream.try_collect().await?;

    Ok(batches)
}
