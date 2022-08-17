use std::{
    fmt::{self, Display},
    path::Path,
};

use config::{Config, ConfigError, File, FileFormat};
use hex::encode;
use log::info;
use rand::distributions::{Alphanumeric, DistString};
use serde::Deserialize;
use sha2::{Digest, Sha256};

pub const DEFAULT_DATA_DIR: &str = "seafowl-data";
pub const DEFAULT_SQLITE_DB: &str = "seafowl.sqlite";

#[derive(Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct SeafowlConfig {
    pub object_store: ObjectStore,
    pub catalog: Catalog,
    #[serde(default)]
    pub frontend: Frontend,
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
        DEFAULT_DATA_DIR,
        dsn,
        random_password()
    );

    let config = load_config_from_string(&config_str, false).unwrap();

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
    pub access_key_id: String,
    pub secret_access_key: String,
    pub endpoint: String,
    pub bucket: String,
}

#[derive(Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Catalog {
    #[cfg(feature = "catalog-postgres")]
    Postgres(Postgres),
    Sqlite(Sqlite),
}

#[derive(Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct Postgres {
    pub dsn: String,
    #[serde(default = "default_schema")]
    pub schema: String,
}

#[derive(Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct Sqlite {
    pub dsn: String,
}

fn default_schema() -> String {
    "public".to_string()
}

#[derive(Deserialize, Debug, PartialEq, Eq, Default, Clone)]
pub struct Frontend {
    #[cfg(feature = "frontend-postgres")]
    pub postgres: Option<PostgresFrontend>,
    pub http: Option<HttpFrontend>,
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
}

impl Default for HttpFrontend {
    fn default() -> Self {
        Self {
            bind_host: "127.0.0.1".to_string(),
            bind_port: 8080,
            read_access: AccessSettings::Any,
            write_access: AccessSettings::Off,
        }
    }
}

#[derive(Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(default)]
pub struct Misc {
    pub max_partition_size: i64,
}

impl Default for Misc {
    fn default() -> Self {
        Self {
            max_partition_size: 1048576,
        }
    }
}

pub fn validate_config(config: SeafowlConfig) -> Result<SeafowlConfig, ConfigError> {
    let in_memory_catalog = matches!(config.catalog, Catalog::Sqlite(Sqlite { ref dsn }) if dsn.contains(":memory:"));

    let in_memory_object_store = matches!(config.object_store, ObjectStore::InMemory(_));

    if in_memory_catalog ^ in_memory_object_store {
        Err(ConfigError::Message(
            "You are using an in-memory catalog with a non in-memory \
        object store or vice versa. This will cause consistency issues \
        if the process is restarted."
                .to_string(),
        ))
    } else {
        Ok(config)
    }
}

pub fn load_config(path: &Path) -> Result<SeafowlConfig, ConfigError> {
    let config = Config::builder()
        .add_source(File::with_name(path.to_str().expect("Error parsing path")));

    config.build()?.try_deserialize().and_then(validate_config)
}

// Load a config from a string (to test our structs are defined correctly)
pub fn load_config_from_string(
    config_str: &str,
    skip_validation: bool,
) -> Result<SeafowlConfig, ConfigError> {
    let config =
        Config::builder().add_source(File::from_str(config_str, FileFormat::Toml));

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
        HttpFrontend, Local, ObjectStore, Postgres, SeafowlConfig, S3,
    };
    use crate::config::schema::Misc;

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

[frontend.http]
bind_host = "0.0.0.0"
bind_port = 80
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

    #[cfg(feature = "object-store-s3")]
    #[test]
    fn test_parse_config_with_s3() {
        let config = load_config_from_string(TEST_CONFIG_S3, false).unwrap();

        assert_eq!(
            config.object_store,
            ObjectStore::S3(S3 {
                access_key_id: "AKI...".to_string(),
                secret_access_key: "ABC...".to_string(),
                endpoint: "https://s3.amazonaws.com:9000".to_string(),
                bucket: "seafowl".to_string()
            })
        );
    }

    #[test]
    fn test_parse_config_basic() {
        let config = load_config_from_string(TEST_CONFIG_BASIC, false).unwrap();

        assert_eq!(
            config,
            SeafowlConfig {
                object_store: ObjectStore::Local(Local {
                    data_dir: "./seafowl-data".to_string(),
                }),
                catalog: Catalog::Postgres(Postgres {
                    dsn: "postgresql://user:pass@localhost:5432/somedb".to_string(),
                    schema: "public".to_string()
                }),
                frontend: Frontend {
                    #[cfg(feature = "frontend-postgres")]
                    postgres: None,
                    http: Some(HttpFrontend {
                        bind_host: "0.0.0.0".to_string(),
                        bind_port: 80,
                        read_access: AccessSettings::Any,
                        write_access: AccessSettings::Off,
                    })
                },
                misc: Misc {
                    max_partition_size: 1048576
                },
            }
        )
    }

    #[test]
    fn test_parse_config_custom_access() {
        let config = load_config_from_string(TEST_CONFIG_ACCESS, false).unwrap();

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
            }
        );
    }

    #[test]
    fn test_parse_config_erroneous() {
        let error = load_config_from_string(TEST_CONFIG_ERROR, false).unwrap_err();
        assert!(error.to_string().contains("missing field `data_dir`"))
    }

    #[test]
    fn test_parse_config_invalid() {
        let error = load_config_from_string(TEST_CONFIG_INVALID, false).unwrap_err();
        assert!(error
            .to_string()
            .contains("You are using an in-memory catalog with a non in-memory"))
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
