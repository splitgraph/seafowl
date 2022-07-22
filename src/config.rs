use config::{Config, File, FileFormat};
use serde::Deserialize;

#[derive(Deserialize, Debug, PartialEq)]
pub struct SeafowlConfig {
    object_store: ObjectStore,
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
    data_dir: String,
}

#[derive(Deserialize, Debug, PartialEq)]
pub struct InMemory {}

#[derive(Deserialize, Debug, PartialEq)]
pub struct S3 {
    access_key_id: String,
    secret_access_key: String,
    endpoint: String,
    bucket: String,
}

pub fn load_config() -> SeafowlConfig {
    let config = Config::builder().add_source(File::with_name("./seafowl.toml"));

    // TODO error handling
    config.build().unwrap().try_deserialize().unwrap()
}

pub fn load_config_from_string(config_str: &str) -> SeafowlConfig {
    let config =
        Config::builder().add_source(File::from_str(config_str, FileFormat::Toml));

    config.build().unwrap().try_deserialize().unwrap()
}

#[cfg(test)]
mod tests {
    use super::{load_config_from_string, ObjectStore, SeafowlConfig, S3};

    const TEST_CONFIG: &str = r#"
[object_store]
type = "s3"
access_key_id = "AKI..."
secret_access_key = "ABC..."
endpoint = "https://s3.amazonaws.com:9000"
bucket = "seafowl""#;

    #[test]
    fn test_parse_config_with_s3() {
        let config = load_config_from_string(TEST_CONFIG);

        assert_eq!(
            config,
            SeafowlConfig {
                object_store: ObjectStore::S3(S3 {
                    access_key_id: "AKI...".to_string(),
                    secret_access_key: "ABC...".to_string(),
                    endpoint: "https://s3.amazonaws.com:9000".to_string(),
                    bucket: "seafowl".to_string()
                })
            }
        )
    }
}
