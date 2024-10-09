pub mod aws;
pub mod google;
pub mod local;
mod memory;

use aws::S3Config;
use google::GCSConfig;
use local::LocalConfig;

use object_store::{
    local::LocalFileSystem, memory::InMemory, parse_url_opts, path::Path,
    prefix::PrefixStore, DynObjectStore, ObjectStore, ObjectStoreScheme,
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

    // Options used to construct the object store (for gRPC consumers)
    pub options: HashMap<String, String>,
    pub url: String,
}

impl ObjectStoreConfig {
    pub fn to_hashmap(&self) -> HashMap<String, String> {
        match self {
            ObjectStoreConfig::Local(config) => config.to_hashmap(),
            ObjectStoreConfig::AmazonS3(config) => config.to_hashmap(),
            ObjectStoreConfig::GoogleCloudStorage(config) => config.to_hashmap(),
            ObjectStoreConfig::Memory => HashMap::new(), // Memory config has no associated data
        }
    }

    pub fn build_object_store(self) -> Result<Arc<dyn ObjectStore>, object_store::Error> {
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
                options: aws_config.to_hashmap(),
                url: aws_config.bucket_to_url(),
            }),
            ObjectStoreConfig::GoogleCloudStorage(google_config) => {
                Ok(StorageLocationInfo {
                    object_store: google_config.build_google_cloud_storage()?,
                    options: google_config.to_hashmap(),
                    url: google_config.bucket_to_url(),
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

pub async fn build_object_store_from_opts(
    url: &Url,
    options: HashMap<String, String>,
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
            let store = local::LocalConfig {
                data_dir: url.path().to_string(),
                disable_hardlinks: false,
            }
            .build_local_storage()?;
            Ok(Box::new(store))
        }
        ObjectStoreScheme::AmazonS3 => {
            let mut s3_options = aws::map_options_into_amazon_s3_config_keys(options)?;
            aws::add_amazon_s3_specific_options(url, &mut s3_options).await;
            aws::add_amazon_s3_environment_variables(&mut s3_options);

            let (store, _) = parse_url_opts(url, s3_options)?;
            Ok(store)
        }
        ObjectStoreScheme::GoogleCloudStorage => {
            let mut gcs_options = google::map_options_into_google_config_keys(options)?;
            google::add_google_cloud_storage_environment_variables(&mut gcs_options);

            let (store, _) = parse_url_opts(url, gcs_options)?;
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
) -> Result<StorageLocationInfo, object_store::Error> {
    Ok(StorageLocationInfo {
        object_store: Arc::new(build_object_store_from_opts(url, options.clone()).await?),
        options: options.clone(),
        url: url.to_string(),
    })
}
