use clap::AppSettings::NoAutoVersion;
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
    config::{
        context::build_context,
        schema::{build_default_config, load_config, SeafowlConfig, DEFAULT_DATA_DIR},
    },
    context::SeafowlContext,
    frontend::http::run_server,
    utils::run_one_off_command,
};

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
        warn!(
            "The PostgreSQL frontend doesn't have authentication or encryption and should only be used in development!"
        );
        result.push(server.boxed());
    };

    if let Some(http) = &config.frontend.http {
        let server = run_server(context, http.to_owned());
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

    let mut builder = pretty_env_logger::formatted_timed_builder();

    builder
        .parse_filters(
            env::var(env_logger::DEFAULT_FILTER_ENV)
                .unwrap_or_else(|_| "sqlx=warn,info".to_string())
                .as_str(),
        )
        .init();

    info!("Starting Seafowl");

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
    };

    let frontends = prepare_frontends(context, &config);

    if frontends.is_empty() {
        warn!("No frontends configured. You will not be able to connect to Seafowl.")
    }

    join_all(frontends).await;
}
