use std::{
    io::{self, IoSlice, Write},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow::json::LineDelimitedWriter;
use arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use hex::encode;
use log::{info, warn};
use sha2::{Digest, Sha256};
use tokio::{fs::File, io::AsyncWrite};

use crate::context::SeafowlContext;
use crate::repository::interface::DroppedTableDeletionStatus;

// Run a one-off command and output its results to a writer
pub async fn run_one_off_command<W>(
    context: Arc<SeafowlContext>,
    command: &str,
    mut output: W,
) where
    W: Write,
{
    for statement in context.parse_query(command).await.unwrap() {
        async {
            let logical = context
                .create_logical_plan_from_statement(statement)
                .await?;
            let physical = context.create_physical_plan(&logical).await?;
            let batches = context.collect(physical).await?;

            let mut writer = LineDelimitedWriter::new(&mut output);
            writer.write_batches(
                batches.iter().collect::<Vec<&RecordBatch>>().as_slice(),
            )?;
            writer.finish()
        }
        .await
        .unwrap();
    }
}

// Physically delete dropped tables for a given context
pub async fn gc_databases(context: &SeafowlContext, database_name: Option<String>) {
    let mut dropped_tables = context
        .metastore
        .tables
        .get_dropped_tables(database_name)
        .await
        .unwrap_or_else(|err| {
            warn!("Failed fetching dropped tables: {err:?}");
            vec![]
        });

    for dt in dropped_tables
        .iter_mut()
        .filter(|dt| dt.deletion_status != DroppedTableDeletionStatus::Failed)
    {
        // TODO: Use batch delete when the object store crate starts supporting it
        info!(
            "Trying to cleanup table {}.{}.{} with UUID (directory name) {}",
            dt.database_name, dt.collection_name, dt.table_name, dt.uuid,
        );

        let table_prefix = context.internal_object_store.table_prefix(dt.uuid);
        let result = context
            .internal_object_store
            .delete_in_prefix(&table_prefix)
            .await;

        if let Err(err) = result {
            warn!(
                "Encountered error while trying to delete table in directory {}: {err}",
                dt.uuid
            );
            if dt.deletion_status != DroppedTableDeletionStatus::Pending {
                dt.deletion_status = DroppedTableDeletionStatus::Retry;
            } else {
                dt.deletion_status = DroppedTableDeletionStatus::Failed;
            }

            context
                .metastore
                .tables
                .update_dropped_table(dt.uuid, dt.deletion_status)
                .await
                .unwrap_or_else(|err| {
                    warn!(
                        "Failed updating dropped table deletion status {}: {err:?}",
                        dt.uuid
                    )
                });
        } else {
            // Successfully cleaned up the table's directory in the object store
            context
                .metastore
                .tables
                .delete_dropped_table(dt.uuid)
                .await
                .unwrap_or_else(|err| {
                    warn!("Failed removing table from metadata {}: {err:?}", dt.uuid)
                });
        }
    }

    // Warn about cleanup errors that need manual intervention
    dropped_tables
        .iter()
        .filter(|dt| dt.deletion_status == DroppedTableDeletionStatus::Failed)
        .for_each(|dt| {
            warn!(
                "Failed to cleanup table {}.{}.{} with UUID (directory name) {}. \
                Please manually delete the corresponding directory in the object store \
                and then delete the entry in the `dropped_tables` catalog table.",
                dt.database_name, dt.collection_name, dt.table_name, dt.uuid,
            )
        })
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
