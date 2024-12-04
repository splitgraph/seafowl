use futures::TryFutureExt;
use iceberg::io::{
    S3_ACCESS_KEY_ID, S3_ALLOW_ANONYMOUS, S3_DISABLE_EC2_METADATA, S3_ENDPOINT,
    S3_REGION, S3_SECRET_ACCESS_KEY,
};
use object_store::aws::AmazonS3ConfigKey;
use object_store::Error;
use std::collections::HashMap;
use std::path::Path as StdPath;
use std::str::FromStr;
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
    }
}

// Go through all known keys for object store and convert them to corresponding file_io ones.
//
// For now only converts S3 keys.
// TODO: At some point this should be redundant, since there is an OpenDAL adapter for object_store,
// https://github.com/apache/iceberg-rust/issues/172
pub fn object_store_opts_to_file_io_props(
    opts: &HashMap<String, String>,
) -> HashMap<String, String> {
    let mut props = HashMap::new();

    for (key, val) in opts.iter() {
        let key = match AmazonS3ConfigKey::from_str(key) {
            Ok(AmazonS3ConfigKey::AccessKeyId) => S3_ACCESS_KEY_ID,
            Ok(AmazonS3ConfigKey::SecretAccessKey) => S3_SECRET_ACCESS_KEY,
            Ok(AmazonS3ConfigKey::SkipSignature)
                if ["true", "t", "1"].contains(&val.to_lowercase().as_str()) =>
            {
                // We need two options on the opendal client in this case
                props.insert(S3_ALLOW_ANONYMOUS.to_string(), val.clone());
                props.insert(S3_DISABLE_EC2_METADATA.to_string(), val.clone());
                continue;
            }
            Ok(AmazonS3ConfigKey::Region) => S3_REGION,
            Ok(AmazonS3ConfigKey::Endpoint) => S3_ENDPOINT,
            _ => key, // for now just propagate any non-matched keys
        };

        props.insert(key.to_string(), val.clone());
    }

    // FileIO requires the region prop even when the S3 store doesn't (e.g. MinIO)
    props
        .entry(S3_REGION.to_string())
        .or_insert("dummy-region".to_string());

    props
}
