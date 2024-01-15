use crate::config::schema;
use crate::config::schema::{Local, GCS, S3};
use bytes::Bytes;
use futures::{stream::BoxStream, StreamExt, TryFutureExt};
use log::debug;
use object_store::{
    path::Path, Error, GetOptions, GetResult, ListResult, MultipartId, ObjectMeta,
    ObjectStore, Result,
};
use std::fmt::{Debug, Display, Formatter};
use std::ops::Range;
use tokio::io::AsyncWrite;

use tokio::fs::{copy, create_dir_all, remove_file, rename};

use deltalake::logstore::{default_logstore, LogStore};
use deltalake::storage::{factories, ObjectStoreFactory, ObjectStoreRef, StorageOptions};
use deltalake::{DeltaResult, DeltaTableError};
use object_store::{prefix::PrefixStore, PutOptions, PutResult};
use std::path::Path as StdPath;
use std::str::FromStr;
use std::sync::Arc;
use url::Url;

/// Wrapper around the object_store crate that holds on to the original config
/// in order to provide a more efficient "upload" for the local object store (since it's
/// stored on the local filesystem, we can just move the file to it instead).
#[derive(Debug, Clone)]
pub struct InternalObjectStore {
    pub inner: Arc<dyn ObjectStore>,
    pub root_uri: Url,
    pub config: schema::ObjectStore,
}

impl InternalObjectStore {
    pub fn new(inner: Arc<dyn ObjectStore>, config: schema::ObjectStore) -> Self {
        let mut root_uri = match config.clone() {
            schema::ObjectStore::Local(Local { data_dir }) => {
                let canonical_path = StdPath::new(&data_dir).canonicalize().unwrap();
                Url::from_directory_path(canonical_path).unwrap()
            }
            schema::ObjectStore::InMemory(_) => Url::from_str("memory://").unwrap(),
            schema::ObjectStore::S3(S3 {
                bucket,
                endpoint,
                prefix,
                ..
            }) => {
                let mut base_url = if let Some(endpoint) = endpoint {
                    // We're assuming here that the bucket isn't contained in the endpoint itself
                    format!("{endpoint}/{bucket}")
                } else {
                    format!("s3://{bucket}")
                };

                if let Some(prefix) = prefix {
                    base_url = format!("{base_url}/{prefix}");
                }

                Url::from_str(&base_url).unwrap()
            }
            schema::ObjectStore::GCS(GCS { bucket, prefix, .. }) => {
                let mut base_url = format!("gs://{bucket}");
                if let Some(prefix) = prefix {
                    base_url = format!("{base_url}/{prefix}");
                }

                Url::from_str(&base_url).unwrap()
            }
        };

        // If a configured bucket contains a path without a trailing slash add one.
        if !root_uri.path().ends_with('/') {
            root_uri.set_path(&format!("{}/", root_uri.path()));
        }

        let store = Self {
            inner,
            root_uri: root_uri.clone(),
            config,
        };

        factories().insert(root_uri, Arc::new(store.clone()));
        store
    }

    // Get the table prefix relative to the root of the internal object store.
    // This is either just a UUID, or potentially UUID prepended by some path.
    pub fn table_prefix(&self, table_prefix: &str) -> Path {
        match self.config.clone() {
            schema::ObjectStore::S3(S3 {
                prefix: Some(prefix),
                ..
            })
            | schema::ObjectStore::GCS(GCS {
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

    /// For local filesystem object stores, try "uploading" by just moving the file.
    /// Returns a None if the store isn't local.
    pub async fn fast_upload(
        &self,
        from: &StdPath,
        to: &Path,
    ) -> Option<Result<(), Error>> {
        let object_store_path = match &self.config {
            schema::ObjectStore::Local(Local { data_dir }) => data_dir,
            _ => return None,
        };

        let target_path =
            StdPath::new(&object_store_path).join(StdPath::new(to.to_string().as_str()));

        // Ensure all directories on the target path exist
        if let Some(parent_dir) = target_path.parent()
            && parent_dir != StdPath::new("")
        {
            create_dir_all(parent_dir).await.ok();
        }

        debug!(
            "Moving temporary partition file from {} to {}",
            from.display(),
            target_path.display()
        );

        let result = rename(&from, &target_path).await;

        Some(if let Err(e) = result {
            // Cross-device link (can't move files between filesystems)
            // Copy and remove the old file
            if e.raw_os_error() == Some(18) {
                copy(from, target_path)
                    .and_then(|_| remove_file(from))
                    .map_err(|e| Error::Generic {
                        store: "local",
                        source: Box::new(e),
                    })
                    .await
            } else {
                Err(Error::Generic {
                    store: "local",
                    source: Box::new(e),
                })
            }
        } else {
            Ok(())
        })
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
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<PutResult> {
        self.inner.put(location, bytes).await
    }

    async fn put_opts(
        &self,
        location: &Path,
        bytes: Bytes,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.inner.put_opts(location, bytes, opts).await
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        self.inner.put_multipart(location).await
    }

    async fn abort_multipart(
        &self,
        location: &Path,
        multipart_id: &MultipartId,
    ) -> Result<()> {
        self.inner.abort_multipart(location, multipart_id).await
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
        self.inner.copy_if_not_exists(from, to).await
    }

    /// Move an object from one path to another in the same object store.
    ///
    /// Will return an error if the destination already has an object.
    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        if let schema::ObjectStore::S3(_) = self.config {
            // TODO: AWS object store doesn't provide `copy_if_not_exists`, which gets called by the
            // the default implementation of this method, since it requires dynamodb lock to be
            // handled properly, so just do the unsafe thing for now.
            // There is a delta-rs wrapper (`S3StorageBackend`) which provides the ability to do
            // this with a lock too, so look into using that down the line instead.
            return self.inner.rename(from, to).await;
        }
        self.inner.rename_if_not_exists(from, to).await
    }
}

// TODO: Implement a proper handler/factory for this at some point
impl ObjectStoreFactory for InternalObjectStore {
    fn parse_url_opts(
        &self,
        url: &Url,
        _options: &StorageOptions,
    ) -> DeltaResult<(ObjectStoreRef, Path)> {
        if self.root_uri.scheme() == url.scheme() {
            Ok((Arc::new(self.clone()), Path::from("/")))
        } else {
            Err(DeltaTableError::InvalidTableLocation(url.clone().into()))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::config::context::build_object_store;
    use crate::config::schema::{ObjectStore, S3};
    use crate::object_store::wrapped::InternalObjectStore;
    use datafusion::common::Result;
    use rstest::rstest;

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
        let config = ObjectStore::S3(S3 {
            region: None,
            access_key_id: "access_key_id".to_string(),
            secret_access_key: "secret_access_key".to_string(),
            bucket: bucket.to_string(),
            prefix: prefix.map(|p| p.to_string()),
            endpoint: endpoint.clone(),
            cache_properties: None,
        });
        // In principle for this test we could use any object store since we only exercise the
        // prefix/log store uri logic
        let inner_store = build_object_store(&config)?;

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
