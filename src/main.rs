use std::{path::PathBuf, pin::Pin, sync::Arc};

use clap::Parser;

use futures::{future::join_all, Future, FutureExt};

use seafowl::{
    config::{
        context::build_context,
        schema::{load_config, SeafowlConfig},
    },
    context::SeafowlContext,
    frontend::http::run_server,
};

#[cfg(feature = "frontend-postgres")]
use seafowl::frontend::postgres::run_pg_server;

extern crate pretty_env_logger;
#[macro_use]
extern crate log;

#[derive(Debug, Parser)]
struct Args {
    #[clap(short, long)]
    config_path: PathBuf,
}

fn prepare_frontends(
    context: Arc<dyn SeafowlContext>,
    config: &SeafowlConfig,
) -> Vec<Pin<Box<dyn Future<Output = ()>>>> {
    let mut result: Vec<Pin<Box<dyn Future<Output = ()>>>> = Vec::new();

    #[cfg(feature = "frontend-postgres")]
    if let Some(pg) = &config.frontend.postgres {
        let server = run_pg_server(context.clone(), pg.to_owned());
        info!(
            "Starting the PostgreSQL frontend on {}:{}",
            pg.bind_host, pg.bind_port
        );
        result.push(server.boxed());
    };

    if let Some(http) = &config.frontend.http {
        let server = run_server(context, http.to_owned());
        info!(
            "Starting the HTTP frontend on {}:{}",
            http.bind_host, http.bind_port
        );
        result.push(server.boxed());
    };

    result
}

#[tokio::main]
async fn main() {
    pretty_env_logger::init_timed();

    info!("Starting Seafowl");
    let args = Args::parse();
    let config = load_config(&args.config_path).expect("Error loading config");

    let context = Arc::new(build_context(&config).await);

    let frontends = prepare_frontends(context, &config);

    if frontends.is_empty() {
        warn!("No frontends configured. You will not be able to connect to Seafowl.")
    }

    join_all(frontends).await;
}
