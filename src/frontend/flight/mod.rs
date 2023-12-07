mod handler;
mod sql;

use crate::config::schema::FlightFrontend;
use crate::context::SeafowlContext;
use crate::frontend::flight::handler::SeafowlFlightHandler;
use arrow_flight::flight_service_server::FlightServiceServer;
use std::net::SocketAddr;
use std::sync::Arc;
use tonic::transport::Server;

pub async fn run_flight_server(context: Arc<SeafowlContext>, config: FlightFrontend) {
    let addr: SocketAddr = format!("{}:{}", config.bind_host, config.bind_port)
        .parse()
        .expect("Error parsing the Arrow Flight listen address");

    let handler = SeafowlFlightHandler::new(context);
    let svc = FlightServiceServer::new(handler);

    Server::builder()
        .add_service(svc)
        .serve(addr)
        .await
        .unwrap();
}
