mod handler;
mod metrics;
mod sql;
mod sync;

use crate::config::schema::FlightFrontend;
use crate::context::SeafowlContext;
use crate::frontend::flight::handler::SeafowlFlightHandler;
use arrow_flight::flight_service_server::FlightServiceServer;
use futures::Future;
use metrics::MetricsLayer;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

use crate::frontend::flight::sync::writer::SeafowlDataSyncWriter;
use tonic::transport::Server;
use tracing::warn;

pub async fn run_flight_server(
    context: Arc<SeafowlContext>,
    config: FlightFrontend,
    shutdown: impl Future<Output = ()> + 'static,
) {
    let addr: SocketAddr = format!("{}:{}", config.bind_host, config.bind_port)
        .parse()
        .expect("Error parsing the Arrow Flight listen address");

    let flush_interval =
        Duration::from_secs(context.config.misc.sync_conf.flush_task_interval_s);
    let sync_manager = Arc::new(RwLock::new(SeafowlDataSyncWriter::new(context.clone())));
    let handler = SeafowlFlightHandler::new(context, sync_manager.clone());
    tokio::spawn(flush_task(flush_interval, sync_manager));

    let svc = FlightServiceServer::new(handler);

    let server = Server::builder();
    let mut server = server.layer(MetricsLayer {});

    server
        .add_service(svc)
        .serve_with_shutdown(addr, shutdown)
        .await
        .unwrap();
}

async fn flush_task(
    interval: Duration,
    sync_manager: Arc<RwLock<SeafowlDataSyncWriter>>,
) {
    loop {
        tokio::time::sleep(interval).await;
        let mut sync_manager = sync_manager.write().await;
        let _ = sync_manager
            .flush()
            .await
            .map_err(|e| warn!("Error flushing syncs: {e}"));
    }
}
