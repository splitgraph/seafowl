use crate::config::schema;
use crate::config::schema::Local;
use futures::TryFutureExt;
use log::debug;
use object_store::ObjectStore;

use tokio::fs::{copy, remove_file, rename};

use std::path::Path;
use std::sync::Arc;

/// Wrapper around the object_store crate that holds on to the original config
/// in order to provide a more efficient "upload" for the local object store (since it's
/// stored on the local filesystem, we can just move the file to it instead).
pub struct InternalObjectStore {
    pub inner: Arc<dyn ObjectStore>,
    pub config: schema::ObjectStore,
}

impl InternalObjectStore {
    /// For local filesystem object stores, try "uploading" by just moving the file.
    /// Returns a None if the store isn't local.
    pub async fn fast_upload(
        &self,
        from: &Path,
        to: &object_store::path::Path,
    ) -> Option<Result<(), object_store::Error>> {
        let object_store_path = match &self.config {
            schema::ObjectStore::Local(Local { data_dir }) => data_dir,
            _ => return None,
        };

        let target_path =
            Path::new(&object_store_path).join(Path::new(to.to_string().as_str()));

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
                    .map_err(|e| object_store::Error::Generic {
                        store: "local",
                        source: Box::new(e),
                    })
                    .await
            } else {
                Err(object_store::Error::Generic {
                    store: "local",
                    source: Box::new(e),
                })
            }
        } else {
            Ok(())
        })
    }
}
