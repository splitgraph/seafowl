#![feature(let_chains)]

use seafowl::config::context::setup_metrics;
use tokio::select;

use std::convert::Infallible;
use std::process::exit;
use std::{
    env, fs, io,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio_graceful_shutdown::{SubsystemBuilder, SubsystemHandle, Toplevel};

use clap::Parser;

#[cfg(feature = "frontend-arrow-flight")]
use seafowl::frontend::flight::run_flight_server;
use seafowl::{
    cli,
    config::{
        context::build_context,
        schema::{build_default_config, load_config, DEFAULT_DATA_DIR},
    },
    frontend::http::run_server,
    utils::{gc_databases, run_one_off_command},
};

use tokio::time::{interval, Duration};
use tracing::level_filters::LevelFilter;
use tracing::{error, info, subscriber, warn};
use tracing_log::LogTracer;
use tracing_subscriber::filter::EnvFilter;

#[cfg(feature = "frontend-postgres")]
use seafowl::frontend::postgres::run_pg_server;

const DEFAULT_CONFIG_PATH: &str = "seafowl.toml";

#[derive(Debug, Parser)]
#[command(name = "seafowl", disable_version_flag = true)]
struct Args {
    #[arg(short, long, default_value=DEFAULT_CONFIG_PATH)]
    config_path: PathBuf,

    #[arg(
        short = 'V',
        long = "version",
        help = "Print version information",
    )]
    version: bool,

    #[arg(short, long, help = "Run a one-off command and exit")]
    one_off: Option<String>,

    #[arg(long, help = "Run commands interactively from a CLI")]
    cli: bool,

    #[arg(long, help = "Enable JSON logging")]
    json_logs: bool,
}

fn prepare_tracing(json_logs: bool) {
    // Redirect all `log`'s events to our subscriber, to collect the ones from our deps too
    LogTracer::init().expect("Failed to set logger");

    let env_filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();

    let sub = tracing_subscriber::fmt()
        .with_ansi(
            env::var_os("RUST_LOG_STYLE")
                .unwrap_or_default()
                .to_string_lossy()
                .to_ascii_lowercase()
                != "never",
        )
        .with_thread_names(true)
        .with_thread_ids(true)
        .with_env_filter(env_filter);

    if json_logs {
        subscriber::set_global_default(sub.json().finish())
    } else {
        subscriber::set_global_default(sub.compact().finish())
    }
    .expect("Global logging config set");
}

fn print_version_info(f: &mut impl std::io::Write) -> std::io::Result<()> {
    writeln!(
        f,
        "Seafowl {} ({} {})",
        env!("VERGEN_GIT_SEMVER"),
        env!("VERGEN_GIT_SHA"),
        env!("VERGEN_GIT_COMMIT_TIMESTAMP")
    )?;

    writeln!(
        f,
        "\nBuilt by rustc {} on {} at {}",
        env!("VERGEN_RUSTC_SEMVER"),
        env!("VERGEN_RUSTC_HOST_TRIPLE"),
        env!("VERGEN_BUILD_TIMESTAMP")
    )?;
    writeln!(
        f,
        "Target: {} {}",
        env!("VERGEN_CARGO_PROFILE"),
        env!("VERGEN_CARGO_TARGET_TRIPLE"),
    )?;
    writeln!(f, "Features: {}", env!("VERGEN_CARGO_FEATURES"))?;

    Ok(())
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    if args.version {
        print_version_info(&mut std::io::stdout()).unwrap();
        return;
    }

    if !args.cli {
        prepare_tracing(args.json_logs)
    }

    info!("Starting Seafowl {}", env!("VERGEN_BUILD_SEMVER"));

    let config_path = &args.config_path;

    // If the user overrode the config file, raise an error if it doesn't exist
    let default_path = Path::new(DEFAULT_CONFIG_PATH);

    let config = if config_path.exists() || config_path != default_path {
        info!("Loading the configuration from {}", config_path.display());
        load_config(config_path).expect("Error loading config")
    } else {
        // Generate a default config
        let (config_str, config) = build_default_config();
        info!(
            "Writing a default configuration file to {}",
            DEFAULT_CONFIG_PATH
        );
        fs::create_dir_all(DEFAULT_DATA_DIR).unwrap();
        fs::write(DEFAULT_CONFIG_PATH, config_str).unwrap();

        config
    };

    if !args.cli
        && let Some(ref metrics) = config.misc.metrics
    {
        setup_metrics(metrics);
    }

    let context = Arc::new(build_context(config).await.unwrap());

    if let Some(internal_object_store) = context.internal_object_store.as_ref() {
        // Temporary, remove once migrations drop the `dropped_table` catalog table
        gc_databases(context.as_ref(), internal_object_store.clone(), None).await;
    }

    if let Some(one_off_cmd) = args.one_off {
        run_one_off_command(context, &one_off_cmd, io::stdout()).await;
        return;
    } else if args.cli {
        return cli::SeafowlCli::new(context).command_loop().await.unwrap();
    }

    Toplevel::new(|s: SubsystemHandle<Infallible>| async move {
        let mut any_frontends: bool = false;
        #[cfg(feature = "frontend-arrow-flight")]
        if let Some(flight) = &context.config.frontend.flight {
            let context = context.clone();
            let flight = flight.clone();
            any_frontends = true;
            s.start(SubsystemBuilder::new("Arrow Flight frontend", move |h| async move {
                info!(
                    "Starting the Arrow Flight frontend on {}:{}",
                    flight.bind_host, flight.bind_port
                );
                run_flight_server(context, flight, async move {
                    h.on_shutdown_requested().await;
                    info!("Shutting down Flight frontend...");
                }).await;
                Ok::<(), Infallible>(())
            }));
        };
        #[cfg(feature = "frontend-postgres")]
        if let Some(pg) = &context.config.frontend.postgres {
            let context = context.clone();
            let pg = pg.clone();
            any_frontends = true;
            s.start(SubsystemBuilder::new("PostgreSQL frontend", move |h| async move {
                info!(
                    "Starting the PostgreSQL frontend on {}:{}",
                    pg.bind_host, pg.bind_port
                );
                warn!(
                    "The PostgreSQL frontend doesn't have authentication or encryption and should only be used in development!"
                );
                select! {
                    e = run_pg_server(context, pg) => e,
                    _ = h.on_shutdown_requested() => (),
                };
                Ok::<(), Infallible>(())
            }));
        };
        if let Some(http) = &context.config.frontend.http {
            let context = context.clone();
            let http = http.clone();
            any_frontends = true;
            s.start(
                SubsystemBuilder::new("HTTP frontend", move |h| async move {
                let server = run_server(context, http.clone(), async move {
                    h.on_shutdown_requested().await;
                    info!("Shutting down HTTP frontend...");
                });
                info!(
                    "Starting the HTTP frontend on {}:{}",
                    http.bind_host, http.bind_port
                );
                info!(
                    "HTTP access settings: read {}, write {}",
                    http.read_access, http.write_access
                );
                server.await;
                Ok::<(), Infallible>(())
            }));
        };

        if !any_frontends {
            error!(
                "No frontends configured. You will not be able to connect to Seafowl.\n
    Run Seafowl with --one-off instead to run a one-off command from the CLI."
            );
            exit(-1);
        }

        // Add a GC task for purging obsolete objects from the catalog and the store
        if context.config.misc.gc_interval > 0 {
            let mut interval = interval(Duration::from_secs(
                (context.config.misc.gc_interval * 3600) as u64,
            ));

            if let Some(internal_object_store) = context.internal_object_store.as_ref() {
                let internal_object_store = internal_object_store.clone();
                s.start::<Infallible, _, _>(SubsystemBuilder::new("Table garbage collection", move |_h| async move {
                    loop {
                        interval.tick().await;
                        gc_databases(&context, internal_object_store.clone(), None).await;
                    };
                }));
            } else {
                warn!("Default object store not configured, not starting the GC task")
            }
        };
    })
    .catch_signals()
    .handle_shutdown_requests(Duration::from_secs(5))
    .await
    .unwrap();
    info!("Exiting cleanly.");
}
