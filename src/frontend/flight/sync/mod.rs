use crate::frontend::flight::sync::writer::SeafowlDataSyncWriter;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::warn;

mod metrics;
pub mod schema;
mod utils;
pub(crate) mod writer;

#[derive(Debug, thiserror::Error)]
pub enum SyncError {
    #[error("Invalid sync schema: {reason}")]
    SchemaError { reason: String },
}

pub async fn flush_task(
    interval: Duration,
    write_timeout: Duration,
    sync_writer: Arc<RwLock<SeafowlDataSyncWriter>>,
) {
    loop {
        tokio::time::sleep(interval).await;

        if let Ok(mut writer) =
            tokio::time::timeout(write_timeout, sync_writer.write()).await
        {
            let _ = writer
                .flush()
                .await
                .map_err(|e| warn!("Error flushing syncs: {e}"));
        } else {
            warn!("Failed to acquire write lock for sync flush");
        }
    }
}
