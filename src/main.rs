use clap::AppSettings::NoAutoVersion;
use std::process::exit;
use std::{
    env, fs, io,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
};

use clap::Parser;

use futures::{future::join_all, Future, FutureExt};

use pretty_env_logger::env_logger;
use seafowl::{
    cli,
    config::{
        context::build_context,
        schema::{build_default_config, load_config, SeafowlConfig, DEFAULT_DATA_DIR},
    },
    context::SeafowlContext,
    frontend::http::run_server,
    utils::{gc_databases, run_one_off_command},
};
use tokio::signal::ctrl_c;
#[cfg(unix)]
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::broadcast::{channel, Sender};
use tokio::time::{interval, Duration};

#[cfg(feature = "frontend-postgres")]
use seafowl::frontend::postgres::run_pg_server;

extern crate pretty_env_logger;
#[macro_use]
extern crate log;

const DEFAULT_CONFIG_PATH: &str = "seafowl.toml";

#[derive(Debug, Parser)]
#[clap(name = "seafowl", global_settings = &[NoAutoVersion])]
struct Args {
    #[clap(short, long, default_value=DEFAULT_CONFIG_PATH)]
    config_path: PathBuf,

    #[clap(
        short = 'V',
        long = "--version",
        help = "Print version information",
        takes_value = false
    )]
    version: bool,

    #[clap(short, long, help = "Run a one-off command and exit")]
    one_off: Option<String>,

    #[clap(
        long,
        help = "Run commands interactively from a CLI",
        takes_value = false
    )]
    cli: bool,
}

fn prepare_frontends(
    context: Arc<SeafowlContext>,
    config: &SeafowlConfig,
    shutdown: &Sender<()>,
) -> Vec<Pin<Box<dyn Future<Output = ()> + Send>>> {
    let mut result: Vec<Pin<Box<dyn Future<Output = ()> + Send>>> = Vec::new();

    #[cfg(feature = "frontend-postgres")]
    if let Some(pg) = &config.frontend.postgres {
        let server = run_pg_server(context.clone(), pg.to_owned());
        info!(
            "Starting the PostgreSQL frontend on {}:{}",
            pg.bind_host, pg.bind_port
        );
        warn!(
            "The PostgreSQL frontend doesn't have authentication or encryption and should only be used in development!"
        );

        let mut shutdown_r = shutdown.subscribe();
        result.push(Box::pin(async move {
            let handle = tokio::spawn(server);

            shutdown_r.recv().await.unwrap();
            info!("Shutting down the PostgreSQL frontend");
            handle.abort()
        }));
    };

    if let Some(http) = &config.frontend.http {
        let shutdown_r = shutdown.subscribe();
        let server = run_server(context, http.to_owned(), shutdown_r);
        info!(
            "Starting the HTTP frontend on {}:{}",
            http.bind_host, http.bind_port
        );
        info!(
            "HTTP access settings: read {}, write {}",
            http.read_access, http.write_access
        );
        result.push(server.boxed());
    };

    result
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
        let mut builder = pretty_env_logger::formatted_timed_builder();

        builder
            .parse_filters(
                env::var(env_logger::DEFAULT_FILTER_ENV)
                    .unwrap_or_else(|_| "sqlx=warn,info".to_string())
                    .as_str(),
            )
            .init();
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

    let context = Arc::new(build_context(&config).await.unwrap());

    if let Some(one_off_cmd) = args.one_off {
        run_one_off_command(context, &one_off_cmd, io::stdout()).await;
        return;
    } else if args.cli {
        return cli::SeafowlCli::new(context).command_loop().await.unwrap();
    }

    // Ref: https://tokio.rs/tokio/topics/shutdown#waiting-for-things-to-finish-shutting-down
    let (shutdown, _) = channel(1);

    let mut tasks = prepare_frontends(context.clone(), &config, &shutdown);

    if tasks.is_empty() {
        error!(
            "No frontends configured. You will not be able to connect to Seafowl.\n
Run Seafowl with --one-off instead to run a one-off command from the CLI."
        );
        exit(-1);
    }

    // Add a GC task for purging obsolete objects from the catalog and the store
    if config.misc.gc_interval > 0 {
        let mut shutdown_r = shutdown.subscribe();
        let mut interval =
            interval(Duration::from_secs((config.misc.gc_interval * 3600) as u64));
        tasks.push(
            async move {
                loop {
                    tokio::select! {
                        _ = interval.tick() => gc_databases(&context, None).await,
                        _ = shutdown_r.recv() => {
                            info!("GC task received shutdown signal, exiting");
                            break;
                        }
                    }
                }
            }
            .boxed(),
        );
    }

    // Add a task that will wait for a termination signal and tell frontends to stop
    tasks.push(
        async move {
            // Wait for a termination signal
            #[cfg(unix)]
            {
                let mut sigterm = signal(SignalKind::terminate())
                    .expect("Error subscribing to the SIGTERM signal");
                tokio::select! {
                    // Ctrl+C: SIGINT
                    _ = ctrl_c() => {},
                    // SIGTERM
                    _ = sigterm.recv() => {},
                }
            }

            #[cfg(not(unix))]
            {
                tokio::select! {
                    // Ctrl+C: termination request on Windows (?)
                    _ = ctrl_c() => {},
                }
            }

            info!("Shutting down...");
            shutdown.send(()).expect("Error during graceful shutdown");
        }
        .boxed(),
    );

    // Start everything and wait for it to exit (gracefully or ungracefully)
    join_all(tasks).await;
    info!("Exiting cleanly.");
}
