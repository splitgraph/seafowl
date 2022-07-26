use std::path::Path;

use config::{Config, ConfigError, File, FileFormat};
use serde::Deserialize;

#[derive(Deserialize, Debug, PartialEq)]
pub struct SeafowlConfig {
    pub object_store: ObjectStore,
    pub catalog: Catalog,
}

#[derive(Deserialize, Debug, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ObjectStore {
    Local(Local),
    InMemory(InMemory),
    S3(S3),
}

#[derive(Deserialize, Debug, PartialEq)]
pub struct Local {
    pub data_dir: String,
}

#[derive(Deserialize, Debug, PartialEq)]
pub struct InMemory {}

#[derive(Deserialize, Debug, PartialEq)]
pub struct S3 {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub endpoint: String,
    pub bucket: String,
}

#[derive(Deserialize, Debug, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Catalog {
    Postgres(Postgres),
}

#[derive(Deserialize, Debug, PartialEq)]
pub struct Postgres {
    pub dsn: String,
}

pub fn load_config(path: &Path) -> Result<SeafowlConfig, ConfigError> {
    let config = Config::builder()
        .add_source(File::with_name(path.to_str().expect("Error parsing path")));

    config.build()?.try_deserialize()
}

// Load a config from a string (to test our structs are defined correctly)
pub fn load_config_from_string(config_str: &str) -> Result<SeafowlConfig, ConfigError> {
    let config =
        Config::builder().add_source(File::from_str(config_str, FileFormat::Toml));

    config.build()?.try_deserialize()
}

#[cfg(test)]
mod tests {
    use super::{
        load_config_from_string, Catalog, ObjectStore, Postgres, SeafowlConfig, S3,
    };

    const TEST_CONFIG: &str = r#"
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

    const TEST_CONFIG_ERROR: &str = r#"
    [object_store]
    type = "local""#;

    #[test]
    fn test_parse_config_with_s3() {
        let config = load_config_from_string(TEST_CONFIG).unwrap();

        assert_eq!(
            config,
            SeafowlConfig {
                object_store: ObjectStore::S3(S3 {
                    access_key_id: "AKI...".to_string(),
                    secret_access_key: "ABC...".to_string(),
                    endpoint: "https://s3.amazonaws.com:9000".to_string(),
                    bucket: "seafowl".to_string()
                }),
                catalog: Catalog::Postgres(Postgres {
                    dsn: "postgresql://user:pass@localhost:5432/somedb".to_string()
                })
            }
        )
    }

    #[test]
    fn test_parse_config_erroneous() {
        let error = load_config_from_string(TEST_CONFIG_ERROR).unwrap_err();
        assert!(error.to_string().contains("missing field `data_dir`"))
    }
}
