use std::path::Path;

use config::{Config, ConfigError, File, FileFormat};
use serde::Deserialize;

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

#[derive(Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(default)]
pub struct HttpFrontend {
    pub bind_host: String,
    pub bind_port: u16,
}

impl Default for HttpFrontend {
    fn default() -> Self {
        Self {
            bind_host: "127.0.0.1".to_string(),
            bind_port: 8080,
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
        load_config_from_string, Catalog, Frontend, HttpFrontend, Local, ObjectStore,
        Postgres, SeafowlConfig, S3,
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
                        bind_port: 80
                    })
                },
                misc: Misc {
                    max_partition_size: 1048576
                },
            }
        )
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
}
