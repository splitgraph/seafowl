/// Contains logic for constructing and caching object stores for various
/// purposes, including wrapping them in a bunch of layers for Delta Lake,
/// HTTP object stores, caching etc
use std::{collections::HashMap, hash::Hash, sync::Arc};

use dashmap::DashMap;
use deltalake::{
    logstore::{default_logstore, LogStore},
    storage::{FactoryRegistry, ObjectStoreRef, StorageOptions},
    DeltaResult, DeltaTableError, Path,
};
use object_store::{prefix::PrefixStore, ObjectStore};
use object_store_factory;
use url::Url;

use object_store_factory::ObjectStoreConfig;

use crate::config::schema::{ObjectCacheProperties, SeafowlConfig};

use super::{cache::CachingObjectStore, wrapped::InternalObjectStore};

pub fn build_object_store(
    object_store_cfg: &ObjectStoreConfig,
    cache_properties: &Option<ObjectCacheProperties>,
) -> Result<Arc<dyn ObjectStore>, object_store::Error> {
    let store = object_store_factory::build_object_store(object_store_cfg)?;

    match object_store_cfg {
        ObjectStoreConfig::Local(_) | ObjectStoreConfig::Memory => Ok(store),
        _ => {
            let cached_store = match cache_properties {
                Some(props) => {
                    Arc::new(CachingObjectStore::new_from_config(props, store))
                }
                None => store,
            };
            Ok(cached_store)
        }
    }
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
    custom_stores: DashMap<StoreCacheKey, Arc<dyn ObjectStore>>,
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
            custom_stores: DashMap::new(),
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
    pub async fn get_log_store_for_table(
        &self,
        url: Url,
        options: HashMap<String, String>,
        table_path: String,
    ) -> Result<Arc<dyn LogStore>, object_store::Error> {
        let store = {
            let used_options = options.clone();
            let key = StoreCacheKey {
                url: url.clone(),
                options,
            };

            match self.custom_stores.get_mut(&key) {
                Some(store) => store.clone(),
                None => {
                    let mut store = object_store_factory::build_object_store_from_opts(
                        &url,
                        used_options,
                    )
                    .await?
                    .into();

                    if !(key.url.scheme() == "file" || key.url.scheme() == "memory")
                        && let Some(ref cache) = self.object_store_cache
                    {
                        // Wrap the non-local store with the caching layer
                        // TODO: share the same cache across all stores
                        store =
                            Arc::new(CachingObjectStore::new_from_config(cache, store))
                    }
                    self.custom_stores.insert(key, store.clone());
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
