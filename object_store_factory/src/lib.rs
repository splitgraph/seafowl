mod aws;
mod google;
mod local;
mod memory;

use object_store::parse_url_opts;
use object_store::ObjectStore;
use object_store::ObjectStoreScheme;
use std::collections::HashMap;
use tracing::warn;
use url::Url;

pub struct Config {
    pub settings: HashMap<String, String>,
}

pub fn build_object_store_from_config(
    scheme: ObjectStoreScheme,
    config: HashMap<String, String>,
) -> Result<Box<dyn ObjectStore>, object_store::Error> {
    match scheme {
        ObjectStoreScheme::Memory => memory::build_in_memory_storage(),
        ObjectStoreScheme::Local => {
            let file_config = local::Config::from_hashmap(&config)?;
            local::build_local_storage(&file_config)
        }
        ObjectStoreScheme::AmazonS3 => {
            let aws_config = aws::Config::from_hashmap(&config)?;
            aws::build_amazon_s3_from_config(&aws_config)
        }
        ObjectStoreScheme::GoogleCloudStorage => {
            let google_config = google::Config::from_hashmap(&config)?;
            google::build_google_cloud_storage_from_config(&google_config)
        }
        _ => {
            warn!("Unsupported scheme: {:?}", scheme);
            return Err(object_store::Error::Generic {
                store: "unsupported_url_scheme",
                source: format!("Unsupported scheme: {:?}", scheme).into(),
            });
        }
    }
}

pub async fn build_object_store_from_opts(
    url: &Url,
    mut options: HashMap<String, String>,
) -> Result<Box<dyn ObjectStore>, object_store::Error> {
    let (scheme, _) = ObjectStoreScheme::parse(&url).unwrap();

    match scheme {
        ObjectStoreScheme::AmazonS3 => {
            options = aws::add_amazon_s3_specific_options(url, options).await;
            options = aws::add_amazon_s3_environment_variables(options);
        }
        ObjectStoreScheme::GoogleCloudStorage => {
            options = google::add_google_cloud_storage_environment_variables(options);
        }
        _ => {
            warn!("Unsupported URL scheme: {}", url);
            return Err(object_store::Error::Generic {
                store: "unsupported_url_scheme",
                source: format!("Unsupported URL scheme: {}", url).into(),
            });
        }
    }

    let store = parse_url_opts(&url, &options)?.0;
    Ok(store)
}
