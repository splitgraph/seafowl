mod aws;
mod google;
mod local;
mod memory;

use object_store::parse_url_opts;
use object_store::prefix::PrefixStore;
use object_store::ObjectStore;
use object_store::ObjectStoreScheme;
use object_store::{local::LocalFileSystem, memory::InMemory};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::warn;
use url::Url;

/// Configuration for object storage, holding settings in a key-value format.
pub type StorageConfig = HashMap<String, String>;

/// Builds an object store based on the specified scheme and configuration.
///
/// # Arguments
///
/// * `scheme` - The scheme to use for object storage, such as Memory, Local, AmazonS3, or GoogleCloudStorage.
/// * `config` - A hash map containing configuration settings needed to build the object store.
///
/// # Returns
///
/// Returns a `Result` that, on success, contains an Arc `ObjectStore` trait object. On failure, it returns an `object_store::Error`
/// indicating what went wrong, such as an unsupported scheme.
///
/// # Errors
///
/// * If the scheme is not supported or the configuration is invalid, an error is returned.
pub fn build_object_store_from_config(
    scheme: &ObjectStoreScheme,
    config: StorageConfig,
) -> Result<Arc<dyn ObjectStore>, object_store::Error> {
    match scheme {
        ObjectStoreScheme::Memory => memory::build_in_memory_storage(),
        ObjectStoreScheme::Local => {
            let file_config = local::LocalConfig::from_hashmap(&config)?;
            local::build_local_storage(&file_config)
        }
        ObjectStoreScheme::AmazonS3 => {
            let aws_config = aws::S3Config::from_hashmap(&config)?;
            aws::build_amazon_s3_from_config(&aws_config)
        }
        ObjectStoreScheme::GoogleCloudStorage => {
            let google_config = google::GCSConfig::from_hashmap(&config)?;
            google::build_google_cloud_storage_from_config(&google_config)
        }
        _ => {
            warn!("Unsupported scheme: {:?}", scheme);
            Err(object_store::Error::Generic {
                store: "unsupported_url_scheme",
                source: format!("Unsupported scheme: {:?}", scheme).into(),
            })
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
