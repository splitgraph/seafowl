use arrow::array::{
    BooleanArray, Float64Array, Int32Array, StringArray, TimestampMicrosecondArray,
};
use arrow::record_batch::RecordBatch;
use arrow_flight::encode::{FlightDataEncoder, FlightDataEncoderBuilder};
use arrow_flight::error::{FlightError, Result};
use arrow_flight::sql::{CommandStatementQuery, ProstMessageExt};
use arrow_flight::{FlightClient, FlightDescriptor};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use datafusion_common::assert_batches_eq;
use futures::StreamExt;
use futures::TryStreamExt;
use prost::Message;
use reqwest::StatusCode;
use rstest::rstest;
use std::future;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use uuid::Uuid;
use warp::hyper::Client;

use clade::sync::{DataSyncCommand, DataSyncResult};

use crate::http::{get_metrics, response_text};
use crate::statements::create_table_and_insert;
use crate::{test_seafowl, TestSeafowl};

use seafowl::config::context::{build_context, GRPC_REQUESTS};
use seafowl::config::schema::load_config_from_string;
use seafowl::context::SeafowlContext;
use seafowl::frontend::flight::run_flight_server;
use seafowl::frontend::flight::SEAFOWL_SYNC_DATA_UD_FLAG;

mod client;
mod e2e;
mod search_path;
mod sync;
mod sync_fail;

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
bind_port = {}

[misc.sync_data]
max_in_memory_bytes = 2500
max_replication_lag_s = 1"#,
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

    let flight =
        run_flight_server(context.clone(), flight_cfg.clone(), future::pending());
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
