use arrow::record_batch::RecordBatch;
use arrow_flight::error::Result;
use arrow_flight::sql::{CommandStatementQuery, ProstMessageExt};
use arrow_flight::{FlightClient, FlightDescriptor};
use datafusion_common::assert_batches_eq;
use futures::TryStreamExt;
use prost::Message;
use seafowl::config::context::build_context;
use seafowl::config::schema::load_config_from_string;
use seafowl::context::SeafowlContext;
use seafowl::frontend::flight::run_flight_server;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use tokio::net::TcpListener;
use tonic::transport::Channel;

mod client;

async fn start_flight_server() -> (
    Arc<SeafowlContext>,
    SocketAddr,
    Pin<Box<dyn Future<Output = ()> + Send>>,
) {
    // let OS choose a a free port
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
    let context = Arc::from(build_context(&config).await.unwrap());

    let flight = run_flight_server(
        context.clone(),
        config
            .frontend
            .flight
            .expect("Arrow Flight frontend configured"),
    );

    (context, addr, Box::pin(flight))
}
