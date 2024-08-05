pub mod aws;
pub mod google;
pub mod local;
mod memory;

use aws::S3Config;
use google::GCSConfig;
use local::LocalConfig;

use object_store::{
    local::LocalFileSystem, memory::InMemory, parse_url_opts, prefix::PrefixStore,
    DynObjectStore, ObjectStore, ObjectStoreScheme,
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

/// Creates an `ObjectStore` instance based on the provided configuration.
///
/// This function takes an `ObjectStoreConfig` reference and returns a `Result` containing
/// an `Arc` to a dynamically-dispatched `ObjectStore` trait object. It matches on the
/// provided configuration to determine the type of object store to create, and then calls
/// the appropriate builder function to instantiate the store.
///
/// # Arguments
///
/// * `config` - A reference to an `ObjectStoreConfig` enum variant that specifies the
///   type and configuration details of the object store to be created.
///
/// # Returns
///
/// * `Result<Arc<dyn ObjectStore>, object_store::Error>` - A result containing an `Arc`
///   pointing to the created object store on success, or an error if the creation fails.
///
/// # Supported Object Stores
///
/// * `ObjectStoreConfig::Memory` - Builds an in-memory object store.
/// * `ObjectStoreConfig::Local` - Builds a local filesystem-backed object store using
///   the provided local configuration.
/// * `ObjectStoreConfig::AmazonS3` - Builds an Amazon S3 object store using the provided
///   AWS configuration.
/// * `ObjectStoreConfig::GoogleCloudStorage` - Builds a Google Cloud Storage object store
///   using the provided Google Cloud configuration.
///
/// # Errors
///
/// This function returns an error if the specified object store cannot be created,
/// which might happen due to misconfiguration, invalid credentials, or other issues
/// specific to the object store's backend.
pub fn build_object_store(
    config: &ObjectStoreConfig,
) -> Result<Arc<dyn ObjectStore>, object_store::Error> {
    match config {
        ObjectStoreConfig::Memory => memory::build_in_memory_storage(),
        ObjectStoreConfig::Local(local_config) => {
            local::build_local_storage(local_config)
        }
        ObjectStoreConfig::AmazonS3(aws_config) => {
            aws::build_amazon_s3_from_config(aws_config)
        }
        ObjectStoreConfig::GoogleCloudStorage(google_config) => {
            google::build_google_cloud_storage_from_config(google_config)
        }
    }
}

/// Builds an object store based on the URL and options provided.
///
/// # Arguments
///
/// * `url` - The URL that determines the object store scheme and configuration.
/// * `options` - A hash map containing configuration options for the object store, which may be modified based on the URL scheme.
///
/// # Returns
///
/// Returns a `Result` that, on success, contains a boxed `ObjectStore` trait object. On failure, it returns an `object_store::Error`
/// indicating what went wrong, such as an unsupported URL scheme.
///
/// # Errors
///
/// * If the URL scheme is unsupported or there is an error parsing the URL or options, an error is returned.
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
            Ok(Box::new(LocalFileSystem::new_with_prefix(url.path())?))
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

pub struct StorageLocationInfo {
    // Actual object store for this location
    pub object_store: Arc<DynObjectStore>,

    // Options used to construct the object store (for gRPC consumers)
    pub options: HashMap<String, String>,
    pub url: String,
}
