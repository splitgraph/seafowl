use std::collections::HashMap;

use std::{
    fmt::{self, Display},
    path::{Path, PathBuf},
};

use crate::catalog::DEFAULT_SCHEMA;
use crate::object_store::cache::{
    DEFAULT_CACHE_CAPACITY, DEFAULT_CACHE_ENTRY_TTL, DEFAULT_MIN_FETCH_SIZE,
};
use config::{Config, ConfigError, Environment, File, FileFormat, Map};
use hex::encode;

use rand::distributions::{Alphanumeric, DistString};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use sqlx::sqlite::SqliteJournalMode;

use tracing::{info, warn};

pub const DEFAULT_DATA_DIR: &str = "seafowl-data";
pub const DEFAULT_SQLITE_DB: &str = "seafowl.sqlite";
pub const ENV_PREFIX: &str = "SEAFOWL";
// Shells don't support envvar names with dots, so use this instead
// (otherwise the config crate can't distinguish between a separator and
// an underscore as part of a config var name, e.g. object_store)
pub const ENV_SEPARATOR: &str = "__";

// Minimum amount of memory, in MB, required to run the app (roughly)
pub const MIN_MEMORY: u64 = 64;

// Default fraction of the advertised memory limit that DataFusion will use
// for its MemoryManager (e.g. to spill out data to disk on sort).
// This is what DataFusion wants from us, but the fraction probably won't be
// linear with the actual amount of memory DF's tracked data structures
// would use.
pub const MEMORY_FRACTION: f64 = 0.7;

pub const MEBIBYTES: u64 = 1024 * 1024;

#[derive(Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct SeafowlConfig {
    pub object_store: Option<ObjectStore>,
    pub catalog: Catalog,
    #[serde(default)]
    pub frontend: Frontend,
    #[serde(default)]
    pub runtime: Runtime,
    #[serde(default)]
    pub misc: Misc,
}

#[derive(Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ObjectStore {
    Local(Local),
    #[serde(rename = "memory")]
    InMemory(InMemory),
    #[cfg(feature = "object-store-s3")]
    S3(S3),
    #[cfg(feature = "object-store-gcs")]
    #[serde(rename = "gcs")]
    GCS(GCS),
}

/// Build a default config file and struct
pub fn build_default_config() -> (String, SeafowlConfig) {
    let dsn = Path::new(DEFAULT_DATA_DIR)
        .join(DEFAULT_SQLITE_DB)
        .to_str()
        .unwrap()
        .to_string();

    // Construct a template. We can't generate a default `SeafowlConfig` struct, since
    // we can't serialize it back with comments.
    let config_str = format!(
        r#"# Default Seafowl config
# For more information, see https://www.splitgraph.com/docs/seafowl/reference/seafowl-toml-configuration

# Store the data (Parquet files) on the local disk
[object_store]
type = "local"
data_dir = "{}"

# Store the catalog on the local disk
[catalog]
type = "sqlite"
dsn = "{}"

# Configure the HTTP frontend
[frontend.http]
bind_host = "127.0.0.1"
bind_port = 8080

# By default, make Seafowl readable by anyone...
read_access = "any"

# ...and writeable by users with a password.
# We store the password's SHA hash here. See the logs from the first
# startup of Seafowl to get the password or set this to a different hash.
write_access = "{}"
"#,
        // Use `escape_default` here since on Windows, these paths
        // use backslashes which toml treats as escapes.
        DEFAULT_DATA_DIR.escape_default(),
        dsn.escape_default(),
        random_password().escape_default()
    );

    let config = load_config_from_string(&config_str, false, None).unwrap();

    (config_str, config)
}

#[derive(Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct Local {
    pub data_dir: String,
}

#[derive(Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct InMemory {}

#[derive(Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct S3 {
    pub region: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub session_token: Option<String>,
    pub endpoint: Option<String>,
    pub bucket: String,
    pub prefix: Option<String>,
}

impl S3 {
    pub fn from_bucket_and_options(
        bucket: String,
        map: &mut HashMap<String, String>,
    ) -> Result<Self, ConfigError> {
        Ok(S3 {
            region: map.get("region").cloned(),
            access_key_id: map.remove("access_key_id"),
            secret_access_key: map.remove("secret_access_key"),
            session_token: map.remove("session_token"),
            endpoint: map.remove("endpoint"),
            bucket,
            prefix: None,
        })
    }
}

#[derive(Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct GCS {
    pub bucket: String,
    pub prefix: Option<String>,
    pub google_application_credentials: Option<String>,
}

impl GCS {
    pub fn from_bucket_and_options(
        bucket: String,
        map: &mut HashMap<String, String>,
    ) -> Self {
        GCS {
            bucket,
            prefix: None,
            google_application_credentials: map.remove("google_application_credentials"),
        }
    }
}

#[derive(Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Catalog {
    #[cfg(feature = "catalog-postgres")]
    Postgres(Postgres),
    Sqlite(Sqlite),
    Clade(Clade),
}

#[derive(Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct Postgres {
    pub dsn: String,
    #[serde(default = "default_schema")]
    pub schema: String,
}

#[derive(Deserialize)]
#[serde(remote = "sqlx::sqlite::SqliteJournalMode", rename_all = "snake_case")]
pub enum SqliteJournalModeDef {
    Delete,
    Truncate,
    Persist,
    Memory,
    Wal,
    Off,
}

fn default_journal_mode() -> SqliteJournalMode {
    SqliteJournalMode::Wal
}

#[derive(Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct Sqlite {
    pub dsn: String,
    #[serde(with = "SqliteJournalModeDef", default = "default_journal_mode")]
    pub journal_mode: SqliteJournalMode,
    #[serde(default)]
    pub read_only: bool,
}

#[derive(Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct Clade {
    pub dsn: String,
}

fn default_schema() -> String {
    DEFAULT_SCHEMA.to_string()
}

#[derive(Deserialize, Debug, PartialEq, Eq, Default, Clone)]
pub struct Frontend {
    #[cfg(feature = "frontend-arrow-flight")]
    pub flight: Option<FlightFrontend>,
    #[cfg(feature = "frontend-postgres")]
    pub postgres: Option<PostgresFrontend>,
    pub http: Option<HttpFrontend>,
}

#[derive(Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(default)]
pub struct FlightFrontend {
    pub bind_host: String,
    pub bind_port: u16,
}

impl Default for FlightFrontend {
    fn default() -> Self {
        Self {
            bind_host: "127.0.0.1".to_string(),
            bind_port: 47470,
        }
    }
}

#[derive(Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(default)]
pub struct PostgresFrontend {
    pub bind_host: String,
    pub bind_port: u16,
}

impl Default for PostgresFrontend {
    fn default() -> Self {
        Self {
            bind_host: "127.0.0.1".to_string(),
            bind_port: 6432,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum AccessSettings {
    Any,
    Off,
    Password { sha256_hash: String },
}

impl Display for AccessSettings {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.write_str(match self {
            AccessSettings::Any => "any",
            AccessSettings::Off => "off",
            AccessSettings::Password { sha256_hash: _ } => "password",
        })?;
        Ok(())
    }
}

impl<'de> Deserialize<'de> for AccessSettings {
    fn deserialize<D>(d: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(d)?;
        return match s.as_str() {
            "any" => Ok(AccessSettings::Any),
            "off" => Ok(AccessSettings::Off),
            s => Ok(AccessSettings::Password {
                sha256_hash: s.to_string(),
            }),
        };
    }
}

pub fn str_to_hex_hash(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input);
    encode(hasher.finalize())
}

pub fn random_password() -> String {
    let password = Alphanumeric.sample_string(&mut rand::thread_rng(), 32);
    let sha256_hash = str_to_hex_hash(&password);

    info!(
        "Writing to Seafowl will require a password. Randomly generated password: {:}",
        password
    );
    info!("The SHA-256 hash will be stored in the config as follows:");
    info!("[frontend.http]");
    info!("write_access = \"{:}\"", sha256_hash);

    sha256_hash
}

#[derive(Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(default)]
pub struct HttpFrontend {
    pub bind_host: String,
    pub bind_port: u16,
    pub read_access: AccessSettings,
    pub write_access: AccessSettings,
    pub upload_data_max_length: u64,
    pub cache_control: String,
}

impl Default for HttpFrontend {
    fn default() -> Self {
        Self {
            bind_host: "127.0.0.1".to_string(),
            bind_port: 8080,
            read_access: AccessSettings::Any,
            write_access: AccessSettings::Off,
            upload_data_max_length: 256,
            cache_control: "max-age=43200, public".to_string(), // defaults to 12 hours
        }
    }
}

#[derive(Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(default)]
pub struct Misc {
    pub max_partition_size: u32,
    // Perhaps make this accept a cron job format and use tokio-cron-scheduler?
    pub gc_interval: u16,
    pub ssl_cert_file: Option<String>,
    pub metrics: Option<Metrics>,
    pub object_store_cache: Option<ObjectCacheProperties>,
}

impl Default for Misc {
    fn default() -> Self {
        Self {
            max_partition_size: 1024 * 1024,
            gc_interval: 0,
            ssl_cert_file: None,
            metrics: None,
            object_store_cache: None,
        }
    }
}

#[derive(Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(default)]
pub struct Metrics {
    pub host: String,
    pub port: u16,
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 9090,
        }
    }
}

#[derive(Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(default)]
pub struct ObjectCacheProperties {
    pub capacity: u64,
    pub min_fetch_size: u64,
    pub ttl: u64,
}

impl Default for ObjectCacheProperties {
    fn default() -> Self {
        Self {
            capacity: DEFAULT_CACHE_CAPACITY,
            min_fetch_size: DEFAULT_MIN_FETCH_SIZE,
            ttl: DEFAULT_CACHE_ENTRY_TTL.as_secs(),
        }
    }
}

#[derive(Default, Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(default)]
pub struct Runtime {
    pub max_memory: Option<u64>,
    pub temp_dir: Option<PathBuf>,
}

pub fn validate_config(mut config: SeafowlConfig) -> Result<SeafowlConfig, ConfigError> {
    let in_memory_catalog = matches!(config.catalog, Catalog::Sqlite(Sqlite { ref dsn, journal_mode: _, read_only: _ }) if dsn.contains(":memory:"));
    let in_memory_object_store =
        matches!(config.object_store, Some(ObjectStore::InMemory(_)));

    if in_memory_catalog ^ in_memory_object_store {
        return Err(ConfigError::Message(
            "You are using an in-memory catalog with a non in-memory \
        object store or vice versa. This will cause consistency issues \
        if the process is restarted."
                .to_string(),
        ));
    }

    match config.object_store {
        Some(ObjectStore::S3(S3 {
            region: None,
            endpoint: None,
            ..
        })) => {
            return Err(ConfigError::Message(
                "You need to supply either the region or the endpoint of the S3 object store."
                    .to_string(),
            ))
        }
        Some(ObjectStore::GCS(GCS {
            google_application_credentials: None,
            ..
        })) => warn!(
            "You are trying to connect to a GCS bucket without providing credentials.
If Seafowl is running on GCP a token should be fetched using the GCP metadata endpoint."
        ),
        Some(ObjectStore::Local(_)) | Some(ObjectStore::InMemory(_))
            if config.misc.object_store_cache.is_some() =>
        {
            warn!(
            "The provided caching properties take no effect on local and in-memory object stores"
        )
        }
        None if !matches!(config.catalog, Catalog::Clade(Clade { dsn: _ })) => {
            return Err(ConfigError::Message(
                "Cannot omit the object_store section unless Clade catalog is configured"
                    .to_string(),
            ))
        }
        // When no object_store section present, default to using in-memory one internally
        None if matches!(config.catalog, Catalog::Clade(Clade { dsn: _ })) => {
            config.object_store = Some(ObjectStore::InMemory(InMemory {}))
        }
        _ => {}
    };

    if let Some(max_memory) = config.runtime.max_memory {
        if max_memory < MIN_MEMORY {
            return Err(ConfigError::Message(format!(
                "runtime.max_memory is too low (minimum {MIN_MEMORY} MB)"
            )));
        }
    };

    Ok(config)
}

pub fn load_config(path: &Path) -> Result<SeafowlConfig, ConfigError> {
    let config = Config::builder()
        .add_source(
            File::with_name(path.to_str().expect("Error parsing path")).required(false),
        )
        .add_source(Environment::with_prefix(ENV_PREFIX).separator(ENV_SEPARATOR));

    config.build()?.try_deserialize().and_then(validate_config)
}

// Load a config from a string (to test our structs are defined correctly)
pub fn load_config_from_string(
    config_str: &str,
    skip_validation: bool,
    env_override: Option<Map<String, String>>,
) -> Result<SeafowlConfig, ConfigError> {
    let config = Config::builder()
        .add_source(File::from_str(config_str, FileFormat::Toml).required(false))
        .add_source(
            Environment::with_prefix(ENV_PREFIX)
                .separator(ENV_SEPARATOR)
                .source(env_override),
        );

    if skip_validation {
        config.build()?.try_deserialize()
    } else {
        config.build()?.try_deserialize().and_then(validate_config)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        build_default_config, load_config_from_string, AccessSettings, Catalog, Frontend,
        HttpFrontend, InMemory, Local, ObjectStore, Postgres, Runtime, SeafowlConfig, S3,
    };
    use crate::config::schema::{Misc, ObjectCacheProperties, Sqlite};
    use crate::object_store::cache::DEFAULT_CACHE_CAPACITY;
    use sqlx::sqlite::SqliteJournalMode;
    use std::{collections::HashMap, path::PathBuf};

    use rstest::rstest;

    const TEST_CONFIG_S3: &str = r#"
[object_store]
type = "s3"
access_key_id = "AKI..."
secret_access_key = "ABC..."
endpoint = "https://s3.amazonaws.com:9000"
bucket = "seafowl"

[catalog]
type = "postgres"
dsn = "postgresql://user:pass@localhost:5432/somedb"
"#;

    const TEST_CONFIG_S3_PUBLIC: &str = r#"
[object_store]
type = "s3"
endpoint = "https://s3.amazonaws.com:9000"
bucket = "seafowl"

[catalog]
type = "postgres"
dsn = "postgresql://user:pass@localhost:5432/somedb"
"#;

    const TEST_CONFIG_S3_WITH_CACHE: &str = r#"
[object_store]
type = "s3"
access_key_id = "AKI..."
secret_access_key = "ABC..."
endpoint = "https://s3.amazonaws.com:9000"
bucket = "seafowl"

[catalog]
type = "postgres"
dsn = "postgresql://user:pass@localhost:5432/somedb"

[misc.object_store_cache]
min_fetch_size = 4096
ttl = 10
"#;

    const TEST_CONFIG_BASIC: &str = r#"
[object_store]
type = "local"
data_dir = "./seafowl-data"

[catalog]
type = "postgres"
dsn = "postgresql://user:pass@localhost:5432/somedb"

[frontend.http]
bind_host = "0.0.0.0"
bind_port = 80

[runtime]
max_memory = 512
temp_dir = "/tmp/seafowl"
"#;

    const TEST_CONFIG_ACCESS: &str = r#"
[object_store]
type = "memory"

[catalog]
type = "sqlite"
dsn = ":memory:"

[frontend.http]
bind_host = "0.0.0.0"
bind_port = 80
read_access = "any"
write_access = "4364aacb2f4609e22d758981474dd82622ad53fc14716f190a5a8a557082612c"
upload_data_max_length = 1
cache_control = "private, max-age=86400"
"#;

    const TEST_CONFIG_ERROR: &str = r#"
    [object_store]
    type = "local""#;

    // Invalid config: in-memory object store with an on-disk SQLite
    const TEST_CONFIG_INVALID: &str = r#"
    [object_store]
    type = "local"
    data_dir = "./seafowl-data"
    [catalog]
    type = "sqlite"
    dsn = ":memory:""#;

    // Invalid config: S3 object store with neither region or endpoint provided
    const TEST_CONFIG_INVALID_S3: &str = r#"
    [object_store]
    type = "s3"
    access_key_id = "AKI..."
    secret_access_key = "ABC..."
    bucket = "seafowl"
    [catalog]
    type = "postgres"
    dsn = "postgresql://user:pass@localhost:5432/somedb""#;

    // Valid config: no object store with a Clade catalog
    const TEST_CONFIG_VALID_CLADE: &str = r#"
    [catalog]
    type = "clade"
    dsn = "http://localhost:54321""#;

    #[cfg(feature = "object-store-s3")]
    #[rstest]
    #[case::basic_s3(TEST_CONFIG_S3, None)]
    #[case::basic_s3_with_cache(
        TEST_CONFIG_S3_WITH_CACHE,
        Some(ObjectCacheProperties{ capacity: DEFAULT_CACHE_CAPACITY, min_fetch_size: 4096, ttl: 10 }))
    ]
    fn test_parse_config_with_s3(
        #[case] config_str: &str,
        #[case] cache_props: Option<ObjectCacheProperties>,
    ) {
        let config = load_config_from_string(config_str, false, None).unwrap();

        assert_eq!(config.misc.object_store_cache, cache_props);
    }

    #[test]
    fn test_parse_public_config_with_s3() {
        let config = load_config_from_string(TEST_CONFIG_S3_PUBLIC, false, None).unwrap();

        assert_eq!(
            config.object_store,
            Some(ObjectStore::S3(S3 {
                region: None,
                access_key_id: None,
                secret_access_key: None,
                session_token: None,
                endpoint: Some("https://s3.amazonaws.com:9000".to_string()),
                bucket: "seafowl".to_string(),
                prefix: None,
            }))
        );
    }

    #[test]
    fn test_parse_no_object_store_clade() {
        let config =
            load_config_from_string(TEST_CONFIG_VALID_CLADE, false, None).unwrap();

        assert_eq!(
            config.object_store,
            Some(ObjectStore::InMemory(InMemory {}))
        );
    }

    #[test]
    fn test_parse_config_basic() {
        let config = load_config_from_string(TEST_CONFIG_BASIC, false, None).unwrap();

        assert_eq!(
            config,
            SeafowlConfig {
                object_store: Some(ObjectStore::Local(Local {
                    data_dir: "./seafowl-data".to_string(),
                })),
                catalog: Catalog::Postgres(Postgres {
                    dsn: "postgresql://user:pass@localhost:5432/somedb".to_string(),
                    schema: "public".to_string()
                }),
                frontend: Frontend {
                    #[cfg(feature = "frontend-arrow-flight")]
                    flight: None,
                    #[cfg(feature = "frontend-postgres")]
                    postgres: None,
                    http: Some(HttpFrontend {
                        bind_host: "0.0.0.0".to_string(),
                        bind_port: 80,
                        read_access: AccessSettings::Any,
                        write_access: AccessSettings::Off,
                        upload_data_max_length: 256,
                        cache_control: "max-age=43200, public".to_string(),
                    })
                },
                runtime: Runtime {
                    max_memory: Some(512),
                    temp_dir: Some(PathBuf::from("/tmp/seafowl")),
                },
                misc: Misc {
                    max_partition_size: 1024 * 1024,
                    gc_interval: 0,
                    ssl_cert_file: None,
                    metrics: None,
                    object_store_cache: None,
                },
            }
        )
    }

    #[test]
    fn test_parse_config_custom_access() {
        let config = load_config_from_string(TEST_CONFIG_ACCESS, false, None).unwrap();

        assert_eq!(
            config.frontend.http.unwrap(),
            HttpFrontend {
                bind_host: "0.0.0.0".to_string(),
                bind_port: 80,
                read_access: AccessSettings::Any,
                write_access: AccessSettings::Password {
                    sha256_hash:
                        "4364aacb2f4609e22d758981474dd82622ad53fc14716f190a5a8a557082612c"
                            .to_string()
                },
                upload_data_max_length: 1,
                cache_control: "private, max-age=86400".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_config_env_override() {
        let env_vars = HashMap::from([
            // Test overriding basic nested configs
            (
                "SEAFOWL__FRONTEND__HTTP__BIND_PORT".to_string(),
                "8080".to_string(),
            ),
            (
                "SEAFOWL__FRONTEND__HTTP__WRITE_ACCESS".to_string(),
                "4364aacb2f4609e22d758981474dd82622ad53fc14716f190a5a8a557082612c"
                    .to_string(),
            ),
            // Test overriding tagged unions
            (
                "SEAFOWL__OBJECT_STORE__TYPE".to_string(),
                "local".to_string(),
            ),
            (
                "SEAFOWL__OBJECT_STORE__DATA_DIR".to_string(),
                "some_other_path".to_string(),
            ),
            // Test that the residual catalog.schema doesn't break a catalog config where
            // we don't use that parameter (sqlite)
            ("SEAFOWL__CATALOG__TYPE".to_string(), "sqlite".to_string()),
            (
                "SEAFOWL__CATALOG__DSN".to_string(),
                "sqlite://file.sqlite".to_string(),
            ),
        ]);

        let config =
            load_config_from_string(TEST_CONFIG_BASIC, false, Some(env_vars)).unwrap();

        assert_eq!(
            config,
            SeafowlConfig {
                object_store: Some(ObjectStore::Local(Local {
                    data_dir: "some_other_path".to_string(),
                })),
                catalog: Catalog::Sqlite(Sqlite {
                    dsn: "sqlite://file.sqlite".to_string(),
                    journal_mode: SqliteJournalMode::Wal,
                    read_only: false,
                }),
                frontend: Frontend {
                    #[cfg(feature = "frontend-arrow-flight")]
                    flight: None,
                    #[cfg(feature = "frontend-postgres")]
                    postgres: None,
                    http: Some(HttpFrontend {
                        bind_host: "0.0.0.0".to_string(),
                        bind_port: 8080,
                        read_access: AccessSettings::Any,
                        write_access: AccessSettings::Password {
                            sha256_hash:
                                "4364aacb2f4609e22d758981474dd82622ad53fc14716f190a5a8a557082612c"
                                    .to_string()
                        },
                        upload_data_max_length: 256,
                        cache_control: "max-age=43200, public".to_string(),
                    })
                },
                runtime: Runtime {
                    max_memory: Some(512),
                    temp_dir: Some(PathBuf::from("/tmp/seafowl")),
                },
                misc: Misc {
                    max_partition_size: 1024 * 1024,
                    gc_interval: 0,
                    ssl_cert_file: None,
                    metrics: None,
                    object_store_cache: None,
                },
            }
        )
    }

    #[test]
    fn test_parse_config_erroneous() {
        let error = load_config_from_string(TEST_CONFIG_ERROR, false, None).unwrap_err();
        assert!(error.to_string().contains("missing field `data_dir`"))
    }

    #[test]
    fn test_parse_config_invalid() {
        let error =
            load_config_from_string(TEST_CONFIG_INVALID, false, None).unwrap_err();
        assert!(error
            .to_string()
            .contains("You are using an in-memory catalog with a non in-memory"))
    }

    #[test]
    fn test_parse_config_invalid_s3() {
        let error =
            load_config_from_string(TEST_CONFIG_INVALID_S3, false, None).unwrap_err();
        assert!(error.to_string().contains(
            "You need to supply either the region or the endpoint of the S3 object store"
        ))
    }

    #[test]
    fn test_default_config() {
        // Run the default config builder to make sure the config parses
        let (_config_str, config) = build_default_config();

        // Make sure we default to requiring a password for writes
        match &config.frontend.http.as_ref().unwrap().write_access {
            AccessSettings::Password { sha256_hash: _ } => (),
            _ => panic!("write_access didn't default to a password!"),
        };
    }
}
