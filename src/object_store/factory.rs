/// Contains logic for constructing and caching object stores for various
/// purposes, including wrapping them in a bunch of layers for Delta Lake,
/// HTTP object stores, caching etc
use std::{
    collections::HashMap,
    hash::Hash,
    sync::{Arc, RwLock},
};

use deltalake::{
    logstore::{default_logstore, LogStore},
    storage::{FactoryRegistry, ObjectStoreRef, StorageOptions},
    DeltaResult, DeltaTableError, Path,
};
use object_store::{
    aws::AmazonS3Builder, gcp::GoogleCloudStorageBuilder, local::LocalFileSystem,
    memory::InMemory, parse_url_opts, prefix::PrefixStore, ObjectStore,
};
use url::Url;

use crate::config::schema::{self, ObjectCacheProperties, SeafowlConfig, GCS, S3};

use super::wrapped::InternalObjectStore;

pub fn build_object_store(
    object_store_cfg: &schema::ObjectStore,
    cache_properties: &Option<ObjectCacheProperties>,
) -> Result<Arc<dyn ObjectStore>, object_store::Error> {
    Ok(match &object_store_cfg {
        schema::ObjectStore::Local(schema::Local { data_dir }) => {
            Arc::new(LocalFileSystem::new_with_prefix(data_dir)?)
        }
        schema::ObjectStore::InMemory(_) => Arc::new(InMemory::new()),
        #[cfg(feature = "object-store-s3")]
        schema::ObjectStore::S3(S3 {
            region,
            access_key_id,
            secret_access_key,
            session_token,
            endpoint,
            bucket,
            ..
        }) => {
            let mut builder = AmazonS3Builder::new()
                .with_region(region.clone().unwrap_or_default())
                .with_bucket_name(bucket)
                .with_allow_http(true);

            if let Some(endpoint) = endpoint {
                builder = builder.with_endpoint(endpoint);
            }

            if let (Some(access_key_id), Some(secret_access_key)) =
                (&access_key_id, &secret_access_key)
            {
                builder = builder
                    .with_access_key_id(access_key_id)
                    .with_secret_access_key(secret_access_key);

                if let Some(token) = session_token {
                    builder = builder.with_token(token)
                }
            } else {
                builder = builder.with_skip_signature(true)
            }

            let store = builder.build()?;

            if let Some(props) = cache_properties {
                props.wrap_store(Arc::new(store))
            } else {
                Arc::new(store)
            }
        }
        #[cfg(feature = "object-store-gcs")]
        schema::ObjectStore::GCS(GCS {
            bucket,
            google_application_credentials,
            ..
        }) => {
            let gcs_builder: GoogleCloudStorageBuilder =
                GoogleCloudStorageBuilder::new().with_bucket_name(bucket);

            let gcs_builder = if let Some(path) = google_application_credentials {
                gcs_builder.with_service_account_path(path)
            } else {
                gcs_builder
            };

            let store = gcs_builder.build()?;

            if let Some(props) = cache_properties {
                props.wrap_store(Arc::new(store))
            } else {
                Arc::new(store)
            }
        }
    })
}

#[derive(PartialEq, Eq)]
struct StoreCacheKey {
    url: Url,
    options: HashMap<String, String>,
}

impl Hash for StoreCacheKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        Hash::hash(&self.url, state);

        let mut pairs: Vec<_> = self.options.iter().collect();
        pairs.sort_by_key(|i| i.0);

        Hash::hash(&pairs, state);
    }
}

pub struct ObjectStoreFactory {
    default_store: Arc<InternalObjectStore>,
    custom_stores: RwLock<HashMap<StoreCacheKey, Arc<dyn ObjectStore>>>,
    object_store_cache: Option<ObjectCacheProperties>,
}

impl ObjectStoreFactory {
    pub fn new_from_config(config: &SeafowlConfig) -> Result<Self, object_store::Error> {
        // Build internal object store
        let object_store_cfg = config
            .object_store
            .clone()
            .expect("guaranteed by config validation that this is Some");
        let object_store =
            build_object_store(&object_store_cfg, &config.misc.object_store_cache)?;
        let internal_object_store = Arc::new(InternalObjectStore::new(
            object_store.clone(),
            object_store_cfg,
        ));

        Ok(Self {
            default_store: internal_object_store,
            custom_stores: RwLock::new(HashMap::new()),
            object_store_cache: config.misc.object_store_cache.clone(),
        })
    }
}

impl ObjectStoreFactory {
    pub fn register(self: &Arc<Self>, factories: FactoryRegistry) {
        // NB: this never actually gets used, it serves only to fetch the known schemes
        // inside delta-rs in `resolve_uri_type`
        factories.insert(self.default_store.root_uri.clone(), self.clone());
        for url in ["s3://", "gs://"] {
            let url = Url::parse(url).unwrap();
            factories.insert(url, self.clone());
        }
    }
    pub fn get_log_store_for_table(
        &self,
        url: Url,
        options: HashMap<String, String>,
        table_path: String,
    ) -> Result<Arc<dyn LogStore>, object_store::Error> {
        let store = {
            let key = StoreCacheKey {
                url: url.clone(),
                options,
            };
            let mut guard = self.custom_stores.write().unwrap();
            match guard.get(&key) {
                Some(store) => store.clone(),
                None => {
                    let mut store = parse_url_opts(&key.url, &key.options)?.0.into();

                    if !(key.url.scheme() == "file" || key.url.scheme() == "memory")
                        && let Some(ref cache) = self.object_store_cache
                    {
                        // Wrap the non-local store with the caching layer
                        // TODO: share the same cache across all stores
                        store = cache.wrap_store(store);
                    }
                    guard.insert(key, store.clone());
                    store
                }
            }
        };

        let prefixed_store: PrefixStore<Arc<dyn ObjectStore>> =
            PrefixStore::new(store, table_path.clone());

        Ok(default_logstore(
            Arc::from(prefixed_store),
            &url.join(&table_path)
                .map_err(|e| object_store::Error::Generic {
                    store: "object_store_factory",
                    source: Box::new(e),
                })?,
            &Default::default(),
        ))
    }

    pub fn get_default_log_store(&self, path: &str) -> Arc<dyn LogStore> {
        self.default_store.get_log_store(path)
    }

    pub fn get_internal_store(&self) -> Arc<InternalObjectStore> {
        self.default_store.clone()
    }
}

// Doesn't get used as per the comment in [`ObjectStoreFactory::register`]
impl deltalake::storage::ObjectStoreFactory for ObjectStoreFactory {
    fn parse_url_opts(
        &self,
        url: &Url,
        _options: &StorageOptions,
    ) -> DeltaResult<(ObjectStoreRef, Path)> {
        Err(DeltaTableError::InvalidTableLocation(url.clone().into()))
    }
}
