pub mod aws;
pub mod google;
pub mod local;
mod memory;

use aws::S3Config;
use google::GCSConfig;
use local::LocalConfig;

use object_store::{
    memory::InMemory, parse_url_opts, path::Path, prefix::PrefixStore, DynObjectStore,
    ObjectStore, ObjectStoreScheme,
};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::warn;
use url::Url;

use serde::Deserialize;

#[derive(Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ObjectStoreConfig {
    Local(LocalConfig),
    Memory,
    #[serde(rename = "s3")]
    AmazonS3(S3Config),
    #[serde(rename = "gcs")]
    GoogleCloudStorage(GCSConfig),
}

pub struct StorageLocationInfo {
    // Actual object store for this location
    pub object_store: Arc<DynObjectStore>,

    pub url: String,
    pub options: HashMap<String, String>,
    pub credentials: HashMap<String, String>,
}

impl ObjectStoreConfig {
    pub fn build_from_json(
        url: &Url,
        json_str: &str,
    ) -> Result<ObjectStoreConfig, object_store::Error> {
        let (scheme, _) = ObjectStoreScheme::parse(url)?;

        match scheme {
            ObjectStoreScheme::Memory => Ok(ObjectStoreConfig::Memory),
            ObjectStoreScheme::Local => {
                let config: LocalConfig = serde_json::from_str(json_str).unwrap();
                Ok(ObjectStoreConfig::Local(config))
            }
            ObjectStoreScheme::AmazonS3 => {
                let config: S3Config = serde_json::from_str(json_str).unwrap();
                Ok(ObjectStoreConfig::AmazonS3(config))
            }
            ObjectStoreScheme::GoogleCloudStorage => {
                let config: GCSConfig = serde_json::from_str(json_str).unwrap();
                Ok(ObjectStoreConfig::GoogleCloudStorage(config))
            }
            _ => {
                unimplemented!();
            }
        }
    }

    pub fn to_hashmap(&self) -> HashMap<String, String> {
        match self {
            ObjectStoreConfig::Local(config) => config.to_hashmap(),
            ObjectStoreConfig::AmazonS3(config) => config.get_options(),
            ObjectStoreConfig::GoogleCloudStorage(config) => config.get_options(),
            ObjectStoreConfig::Memory => HashMap::new(), // Memory config has no associated data
        }
    }

    pub fn build_object_store(
        &self,
    ) -> Result<Arc<dyn ObjectStore>, object_store::Error> {
        match self {
            ObjectStoreConfig::Memory => memory::build_in_memory_storage(),
            ObjectStoreConfig::Local(local_config) => local_config.build_local_storage(),
            ObjectStoreConfig::AmazonS3(aws_config) => aws_config.build_amazon_s3(),
            ObjectStoreConfig::GoogleCloudStorage(google_config) => {
                google_config.build_google_cloud_storage()
            }
        }
    }

    pub fn build_storage_location_info(
        &self,
    ) -> Result<StorageLocationInfo, object_store::Error> {
        match self {
            ObjectStoreConfig::AmazonS3(aws_config) => Ok(StorageLocationInfo {
                object_store: aws_config.build_amazon_s3()?,
                url: aws_config.bucket_to_url(),
                options: aws_config.get_options(),
                credentials: aws_config.get_credentials(),
            }),
            ObjectStoreConfig::GoogleCloudStorage(google_config) => {
                Ok(StorageLocationInfo {
                    object_store: google_config.build_google_cloud_storage()?,
                    url: google_config.bucket_to_url(),
                    options: google_config.get_options(),
                    credentials: google_config.get_credentials(),
                })
            }
            _ => {
                unimplemented!();
            }
        }
    }

    pub fn get_base_url(&self) -> Option<Path> {
        match self {
            ObjectStoreConfig::AmazonS3(aws_config) => aws_config.get_base_url(),
            ObjectStoreConfig::GoogleCloudStorage(google_config) => {
                google_config.get_base_url()
            }
            _ => None,
        }
    }

    pub fn get_allow_http(&self) -> Result<bool, object_store::Error> {
        match self {
            ObjectStoreConfig::AmazonS3(aws_config) => Ok(aws_config.get_allow_http()),
            _ => Err(object_store::Error::Generic {
                store: "unsupported_object_store",
                source: "Only Amazon S3 is supported".into(),
            }),
        }
    }
}

pub async fn build_object_store_from_options_and_credentials(
    url: &Url,
    options: HashMap<String, String>,
    credentials: HashMap<String, String>,
) -> Result<Box<dyn ObjectStore>, object_store::Error> {
    let (scheme, _) = ObjectStoreScheme::parse(url).unwrap();

    match scheme {
        // `parse_url_opts` will swallow the URL path for memory/local FS stores
        ObjectStoreScheme::Memory => {
            let mut store: Box<dyn ObjectStore> = Box::new(InMemory::new());
            if !url.path().is_empty() {
                store = Box::new(PrefixStore::new(store, url.path()));
            }

            Ok(store)
        }
        ObjectStoreScheme::Local => {
            let store = LocalConfig {
                data_dir: url.path().to_string(),
                disable_hardlinks: false,
            }
            .build_local_storage()?;
            Ok(Box::new(store))
        }
        ObjectStoreScheme::AmazonS3 => {
            let mut s3_options =
                aws::map_options_and_credentials_into_amazon_s3_config_keys(
                    options,
                    credentials,
                )?;
            aws::add_amazon_s3_specific_options(url, &mut s3_options).await;
            aws::add_amazon_s3_environment_variables(&mut s3_options);

            let (mut store, _) = parse_url_opts(url, s3_options)?;
            if !url.path().is_empty() {
                store = Box::new(PrefixStore::new(store, url.path()));
            }
            Ok(store)
        }
        ObjectStoreScheme::GoogleCloudStorage => {
            let mut gcs_options =
                google::map_options_and_credentials_into_google_config_keys(
                    options,
                    credentials,
                )?;
            google::add_google_cloud_storage_environment_variables(&mut gcs_options);

            let (mut store, _) = parse_url_opts(url, gcs_options)?;
            if !url.path().is_empty() {
                store = Box::new(PrefixStore::new(store, url.path()));
            }
            Ok(store)
        }
        _ => {
            warn!("Unsupported URL scheme: {}", url);
            Err(object_store::Error::Generic {
                store: "unsupported_url_scheme",
                source: format!("Unsupported URL scheme: {}", url).into(),
            })
        }
    }
}

pub async fn build_storage_location_info_from_opts(
    url: &Url,
    options: &HashMap<String, String>,
    credentials: &HashMap<String, String>,
) -> Result<StorageLocationInfo, object_store::Error> {
    Ok(StorageLocationInfo {
        object_store: Arc::new(
            build_object_store_from_options_and_credentials(
                url,
                options.clone(),
                credentials.clone(),
            )
            .await?,
        ),
        options: options.clone(),
        credentials: credentials.clone(),
        url: url.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    use serde_json::json;

    #[test]
    fn test_build_from_json_local() {
        let url = Url::parse("file:///tmp").unwrap();
        let json_str = json!({
            "type": "local",
            "data_dir": "/tmp",
            "disable_hardlinks": false
        })
        .to_string();

        let config = ObjectStoreConfig::build_from_json(&url, &json_str).unwrap();
        if let ObjectStoreConfig::Local(local_config) = config {
            assert_eq!(local_config.data_dir, "/tmp");
            assert!(!local_config.disable_hardlinks);
        } else {
            panic!("Expected ObjectStoreConfig::Local");
        }
    }

    #[test]
    fn test_build_from_json_memory() {
        let url = Url::parse("memory:///").unwrap();
        let json_str = json!({
            "type": "memory"
        })
        .to_string();

        let config = ObjectStoreConfig::build_from_json(&url, &json_str).unwrap();
        assert_eq!(config, ObjectStoreConfig::Memory);
    }

    #[test]
    fn test_build_from_json_amazon_s3() {
        let url = Url::parse("s3://bucket").unwrap();
        let json_str = json!({
            "type": "s3",
            "bucket": "bucket",
            "region": "us-west-2",
            "access_key_id": "test_access_key",
            "secret_access_key": "test_secret_key"
        })
        .to_string();

        let config = ObjectStoreConfig::build_from_json(&url, &json_str).unwrap();
        if let ObjectStoreConfig::AmazonS3(s3_config) = config {
            assert_eq!(s3_config.bucket, "bucket");
            assert_eq!(s3_config.region.unwrap(), "us-west-2");
            assert_eq!(s3_config.access_key_id.unwrap(), "test_access_key");
            assert_eq!(s3_config.secret_access_key.unwrap(), "test_secret_key");
        } else {
            panic!("Expected ObjectStoreConfig::AmazonS3");
        }
    }

    #[test]
    fn test_build_from_json_google_cloud_storage() {
        let url = Url::parse("gs://bucket").unwrap();
        let json_str = json!({
            "type": "gcs",
            "bucket": "bucket",
            "google_application_credentials": "test_credentials"
        })
        .to_string();

        let config = ObjectStoreConfig::build_from_json(&url, &json_str).unwrap();
        if let ObjectStoreConfig::GoogleCloudStorage(gcs_config) = config {
            assert_eq!(gcs_config.bucket, "bucket");
            assert_eq!(
                gcs_config.google_application_credentials.unwrap(),
                "test_credentials"
            );
        } else {
            panic!("Expected ObjectStoreConfig::GoogleCloudStorage");
        }
    }

    #[test]
    fn test_build_from_json_invalid_scheme() {
        let url = Url::parse("ftp://bucket").unwrap();
        let json_str = json!({
            "type": "ftp"
        })
        .to_string();

        let result = ObjectStoreConfig::build_from_json(&url, &json_str);
        assert!(result.is_err());
    }

    #[rstest]
    #[tokio::test]
    async fn test_build_aws_object_store(#[values(true, false)] use_env: bool) {
        let url = Url::parse("s3://my-bucket").unwrap();
        let options = HashMap::new();
        let credentials: HashMap<String, String> = if use_env {
            HashMap::new()
        } else {
            HashMap::from([
                ("access_key_id".to_string(), "my-key".to_string()),
                ("secret_access_key".to_string(), "my-secret".to_string()),
            ])
        };

        let store = temp_env::async_with_vars(
            [
                ("AWS_ACCESS_KEY_ID", Some("env-key")),
                ("AWS_SECRET_ACCESS_KEY", Some("env-secret")),
            ],
            build_object_store_from_options_and_credentials(&url, options, credentials),
        )
        .await;

        let debug_output = format!("{store:?}");
        assert!(debug_output.contains("bucket: \"my-bucket\""));
        if use_env {
            assert!(debug_output.contains("key_id: \"env-key\""));
            assert!(debug_output.contains("secret_key: \"env-secret\""));
        } else {
            assert!(debug_output.contains("key_id: \"my-key\""));
            assert!(debug_output.contains("secret_key: \"my-secret\""));
        }
    }
}
