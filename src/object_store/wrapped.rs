use bytes::Bytes;
use futures::{stream::BoxStream, StreamExt};
use object_store::{
    path::Path, Error, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOpts, PutPayload, Result,
};
use std::fmt::{Debug, Display, Formatter};
use std::ops::Range;

use deltalake::logstore::{default_logstore, LogStore};
use object_store::{prefix::PrefixStore, PutOptions, PutResult};
use std::path::Path as StdPath;
use std::str::FromStr;
use std::sync::Arc;
use url::Url;

use object_store_factory::aws::S3Config;
use object_store_factory::google::GCSConfig;
use object_store_factory::local::LocalConfig;
use object_store_factory::ObjectStoreConfig;

// Wrapper around the object_store crate that holds on to the original config
// in order to provide a more efficient "upload" for the local object store
// (since it's stored on the local filesystem, we can just move the file to
// it instead).
#[derive(Debug, Clone)]
pub struct InternalObjectStore {
    pub inner: Arc<dyn ObjectStore>,
    pub root_uri: Url,
    pub config: ObjectStoreConfig,
}

impl InternalObjectStore {
    pub fn new(inner: Arc<dyn ObjectStore>, config: ObjectStoreConfig) -> Self {
        let mut root_uri = match config.clone() {
            ObjectStoreConfig::Local(local_config) => {
                let canonical_path =
                    StdPath::new(&local_config.data_dir).canonicalize().unwrap();
                Url::from_directory_path(canonical_path).unwrap()
            }
            ObjectStoreConfig::Memory => Url::from_str("memory://").unwrap(),
            ObjectStoreConfig::AmazonS3(aws_config) => {
                let mut base_url = if let Some(endpoint) = aws_config.endpoint {
                    // We're assuming here that the bucket isn't contained in the endpoint itself
                    format!("{endpoint}/{}", aws_config.bucket)
                } else {
                    format!("s3://{}", aws_config.bucket)
                };

                if let Some(prefix) = aws_config.prefix {
                    base_url = format!("{base_url}/{prefix}");
                }

                Url::from_str(&base_url).unwrap()
            }
            ObjectStoreConfig::GoogleCloudStorage(google_config) => {
                let mut base_url = format!("gs://{}", google_config.bucket);
                if let Some(prefix) = google_config.prefix {
                    base_url = format!("{base_url}/{prefix}");
                }

                Url::from_str(&base_url).unwrap()
            }
        };

        // If a configured bucket contains a path without a trailing slash add one.
        if !root_uri.path().ends_with('/') {
            root_uri.set_path(&format!("{}/", root_uri.path()));
        }

        Self {
            inner,
            root_uri,
            config,
        }
    }

    // If the configured object store uses a local file system as an object store return
    // the full path to the table dir
    pub fn local_table_dir(&self, table_prefix: &str) -> Option<String> {
        match &self.config {
            ObjectStoreConfig::Local(local_config) => {
                Some(format!("{}/{table_prefix}", local_config.data_dir))
            }
            _ => None,
        }
    }

    // Get the table prefix relative to the root of the internal object store.
    // This is either just a UUID, or potentially UUID prepended by some path.
    pub fn table_prefix(&self, table_prefix: &str) -> Path {
        match self.config.clone() {
            ObjectStoreConfig::AmazonS3(S3Config {
                prefix: Some(prefix),
                ..
            })
            | ObjectStoreConfig::GoogleCloudStorage(GCSConfig {
                prefix: Some(prefix),
                ..
            }) => Path::from(format!("{prefix}/{table_prefix}")),
            _ => Path::from(table_prefix),
        }
    }

    // Wrap our object store with a prefixed one corresponding to the full path to the actual table
    // root, and then wrap that with a default delta `LogStore`. This is done because:
    // 1. `LogStore` needs an object store with "/" pointing at delta table root
    //     (i.e. where `_delta_log` is located), hence the `PrefixStore`.
    // 2. We want to override `rename_if_not_exists` for AWS S3
    // This means we have 2 layers of indirection before we hit the "real" object store:
    // (Delta `LogStore` -> `PrefixStore` -> `InternalObjectStore` -> `inner`).
    pub fn get_log_store(&self, table_prefix: &str) -> Arc<dyn LogStore> {
        let prefix = self.table_prefix(table_prefix);
        let prefixed_store: PrefixStore<InternalObjectStore> =
            PrefixStore::new(self.clone(), prefix);

        default_logstore(
            Arc::from(prefixed_store),
            &self.root_uri.join(table_prefix).unwrap(),
            &Default::default(),
        )
    }

    /// Delete all objects under a given prefix
    pub async fn delete_in_prefix(&self, prefix: &Path) -> Result<(), Error> {
        let mut path_stream = self.inner.list(Some(prefix));
        while let Some(maybe_object) = path_stream.next().await {
            if let Ok(ObjectMeta { location, .. }) = maybe_object {
                self.inner.delete(&location).await?;
            }
        }
        Ok(())
    }
}

impl Display for InternalObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "InternalObjectStore({})", self.root_uri)
    }
}

#[async_trait::async_trait]
impl ObjectStore for InternalObjectStore {
    /// Save the provided bytes to the specified location.
    async fn put(&self, location: &Path, payload: PutPayload) -> Result<PutResult> {
        self.inner.put(location, payload).await
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        if let ObjectStoreConfig::Local(LocalConfig {
            disable_hardlinks: true,
            ..
        }) = self.config
        {
            return self
                .inner
                .put_opts(
                    location,
                    payload,
                    PutOptions {
                        mode: object_store::PutMode::Overwrite,
                        ..opts
                    },
                )
                .await;
        };
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart(&self, location: &Path) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart(location).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    /// Return the bytes that are stored at the specified location.
    async fn get(&self, location: &Path) -> Result<GetResult> {
        self.inner.get(location).await
    }

    /// Perform a get request with options
    /// Note: options.range will be ignored if GetResult::File
    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    /// Return the bytes that are stored at the specified location
    /// in the given byte range
    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        self.inner.get_range(location, range).await
    }

    /// Return the metadata for the specified location
    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        self.inner.head(location).await
    }

    /// Delete the object at the specified location.
    async fn delete(&self, location: &Path) -> Result<()> {
        self.inner.delete(location).await
    }

    /// Delete all the objects at the specified locations in bulk when applicable
    fn delete_stream<'a>(
        &'a self,
        locations: BoxStream<'a, Result<Path>>,
    ) -> BoxStream<'a, Result<Path>> {
        locations
            .map(|location| async {
                let location = location?;
                self.delete(&location).await?;
                Ok(location)
            })
            .buffered(10)
            .boxed()
    }

    /// List all the objects with the given prefix.
    ///
    /// Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a prefix of `foo/bar/x` but not of
    /// `foo/bar_baz/x`.
    ///
    /// Note: the order of returned [`ObjectMeta`] is not guaranteed
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    /// List objects with the given prefix and an implementation specific
    /// delimiter. Returns common prefixes (directories) in addition to object
    /// metadata.
    ///
    /// Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a prefix of `foo/bar/x` but not of
    /// `foo/bar_baz/x`.
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    /// Copy an object from one path to another in the same object store.
    ///
    /// If there exists an object at the destination, it will be overwritten.
    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.copy(from, to).await
    }

    /// Copy an object from one path to another, only if destination is empty.
    ///
    /// Will return an error if the destination already has an object.
    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        if let ObjectStoreConfig::Local(LocalConfig {
            disable_hardlinks: true,
            ..
        }) = self.config
        {
            return self.inner.copy(from, to).await;
        }
        self.inner.copy_if_not_exists(from, to).await
    }

    /// Move an object from one path to another in the same object store.
    ///
    /// Will return an error if the destination already has an object.
    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        if let ObjectStoreConfig::AmazonS3(_) = self.config {
            // TODO: AWS object store doesn't provide `copy_if_not_exists`, which gets called by the
            // the default implementation of this method, since it requires dynamodb lock to be
            // handled properly, so just do the unsafe thing for now.
            // There is a delta-rs wrapper (`S3StorageBackend`) which provides the ability to do
            // this with a lock too, so look into using that down the line instead.
            return self.inner.rename(from, to).await;
        }
        if let ObjectStoreConfig::Local(LocalConfig {
            disable_hardlinks: true,
            ..
        }) = self.config
        {
            return self.inner.rename(from, to).await;
        }
        self.inner.rename_if_not_exists(from, to).await
    }
}

#[cfg(test)]
mod tests {
    use crate::object_store::factory::build_object_store;
    use crate::object_store::wrapped::InternalObjectStore;
    use datafusion::common::Result;
    use rstest::rstest;

    use object_store_factory::aws::S3Config;
    use object_store_factory::ObjectStoreConfig;

    #[rstest]
    #[case::bucket_root("test-bucket", None, "6bb9913e-0341-446d-bb58-b865803ce0ff")]
    #[case::path_no_delimiter(
        "test-bucket",
        Some("some/path/no/delimiter"),
        "some/path/no/delimiter/6bb9913e-0341-446d-bb58-b865803ce0ff"
    )]
    #[case::path_with_delimiter(
        "test-bucket",
        Some("some/path/with/delimiter/"),
        "some/path/with/delimiter/6bb9913e-0341-446d-bb58-b865803ce0ff"
    )]
    #[test]
    fn test_table_location_s3(
        #[case] bucket: &str,
        #[case] prefix: Option<&str>,
        #[case] table_prefix: &str,
        #[values(None, Some("http://127.0.0.1:9000".to_string()))] endpoint: Option<
            String,
        >,
    ) -> Result<()> {
        let config = ObjectStoreConfig::AmazonS3(S3Config {
            region: None,
            access_key_id: Some("access_key_id".to_string()),
            secret_access_key: Some("secret_access_key".to_string()),
            bucket: bucket.to_string(),
            prefix: prefix.map(|p| p.to_string()),
            endpoint: endpoint.clone(),
            ..Default::default()
        });
        // In principle for this test we could use any object store since we only exercise the
        // prefix/log store uri logic
        let inner_store = build_object_store(&config, &None)?;

        let store = InternalObjectStore::new(inner_store, config);

        let uuid = "6bb9913e-0341-446d-bb58-b865803ce0ff";
        let prefix = store.table_prefix(uuid);
        let uri = store.get_log_store(uuid).root_uri();

        assert_eq!(prefix, table_prefix.into());

        let expected_uri = if let Some(endpoint) = endpoint {
            format!("{endpoint}/{bucket}/{table_prefix}")
        } else {
            format!("s3://{bucket}/{table_prefix}")
        };

        assert_eq!(uri, expected_uri);

        Ok(())
    }
}
