use futures::TryFutureExt;
use object_store::Error;
use std::path::Path as StdPath;
use tokio::fs::{copy, create_dir_all, remove_file, rename};
use tracing::debug;

/// For local filesystem object stores, try "uploading" by just moving the file.
pub async fn fast_upload(from: &StdPath, to: String) -> object_store::Result<(), Error> {
    let target_path = StdPath::new(&to);

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

    if let Err(e) = result {
        // Cross-device link (can't move files between filesystems)
        // Function not implemented (os error 38)
        // Copy and remove the old file
        if e.raw_os_error() == Some(18) || e.raw_os_error() == Some(38) {
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
    }
}
