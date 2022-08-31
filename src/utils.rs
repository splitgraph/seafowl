use std::{
    io::{self, IoSlice, Write},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow::json::LineDelimitedWriter;
use datafusion::error::Result;
use hex::encode;
use log::{debug, info, warn};
use object_store::path::Path;
use sha2::{Digest, Sha256};
use tokio::time::{interval, Duration};
use tokio::{fs::File, io::AsyncWrite};

use crate::context::{DefaultSeafowlContext, SeafowlContext};

// Run a one-off command and output its results to a writer
pub async fn run_one_off_command<W>(
    context: Arc<dyn SeafowlContext>,
    command: &str,
    mut output: W,
) where
    W: Write,
{
    // TODO when https://github.com/splitgraph/seafowl/issues/48 is implemented: run this
    // without splitting on the semicolon (which can also be a legitimate part of a query)
    for s in command.split(';') {
        if s.trim() == "" {
            continue;
        }
        async {
            let physical = context.plan_query(s).await?;
            let batches = context.collect(physical).await?;

            let mut writer = LineDelimitedWriter::new(&mut output);
            writer.write_batches(&batches)?;
            writer.finish()
        }
        .await
        .unwrap();
    }
}

pub async fn cleanup_job(context: Arc<DefaultSeafowlContext>) {
    let mut interval = interval(Duration::from_secs(3600));

    loop {
        match context
            .partition_catalog
            .get_orphan_partition_store_ids()
            .await
        {
            Ok(mut object_storage_ids) if !object_storage_ids.is_empty() => {
                info!("Found {} orphan partition(s)", object_storage_ids.len());

                let mut retain_map = vec![true; object_storage_ids.len()];
                for (ind, object_storage_id) in object_storage_ids.iter().enumerate() {
                    context
                        .internal_object_store
                        .inner
                        .delete(&Path::from(object_storage_id.clone()))
                        .await
                        .map_err(|e| {
                            warn!("Failed to delete orphan partition {} from object store: {:?}", object_storage_id, e);
                            retain_map[ind] = false;
                        })
                        .ok();
                }

                // Scope down only to partitions which we managed to delete in the object store
                let mut keep = retain_map.iter();
                object_storage_ids.retain(|_| *keep.next().unwrap());

                context
                    .partition_catalog
                    .delete_partitions(object_storage_ids)
                    .await
                    .map_or_else(
                        |e| {
                            warn!(
                                "Failed to delete orphan partitions from catalog: {:?}",
                                e
                            )
                        },
                        |row_count| info!("Deleted {} orphan partition(s)", row_count),
                    );
            }
            Err(e) => warn!("Failed to fetch orphan partitions: {:?}", e),
            _ => debug!("No orphan partitions to cleanup found"),
        }

        interval.tick().await;
    }
}

/// A Sha256 hasher that works as a Tokio async writer
struct AsyncSha256Hasher {
    pub hasher: Sha256,
}

impl AsyncSha256Hasher {
    pub fn new() -> Self {
        Self {
            hasher: Sha256::new(),
        }
    }
}

impl Write for AsyncSha256Hasher {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.hasher.update(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl AsyncWrite for AsyncSha256Hasher {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.get_mut().hasher.update(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(io::Write::write_vectored(&mut *self, bufs))
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

pub async fn hash_file(path: &std::path::Path) -> Result<String> {
    let mut hasher = AsyncSha256Hasher::new();

    let mut file = File::open(path).await?;
    tokio::io::copy(&mut file, &mut hasher).await?;
    Ok(encode(hasher.hasher.finalize()))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::NamedTempFile;
    use tokio::{fs::OpenOptions, io::AsyncWriteExt};

    use super::{hash_file, run_one_off_command};
    use crate::context::test_utils::in_memory_context;

    #[tokio::test]
    async fn test_command_splitting() {
        let mut buf = Vec::new();
        let context = in_memory_context().await;

        run_one_off_command(Arc::from(context), "SELECT 1; SELECT 1", &mut buf).await;

        assert_eq!(
            String::from_utf8(buf).unwrap(),
            "{\"Int64(1)\":1}\n{\"Int64(1)\":1}\n"
        );
    }

    #[tokio::test]
    async fn test_hash_file() {
        let file_path = NamedTempFile::new().unwrap().into_temp_path();
        let mut file = OpenOptions::new()
            .write(true)
            .open(&file_path)
            .await
            .unwrap();

        file.write_all(b"test").await.unwrap();
        file.flush().await.unwrap();

        assert_eq!(
            hash_file(&file_path).await.unwrap(),
            "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
        );
    }
}
