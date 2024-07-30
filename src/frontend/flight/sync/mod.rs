use crate::frontend::flight::sync::writer::SeafowlDataSyncWriter;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::warn;

mod metrics;
pub mod schema;
mod utils;
pub(crate) mod writer;

pub(super) type Origin = String;
pub(super) type SequenceNumber = u64;

#[derive(Debug, thiserror::Error)]
pub enum SyncError {
    #[error("Invalid sync schema: {reason}")]
    SchemaError { reason: String },

    #[error("Invalid sync message: {reason}")]
    InvalidMessage { reason: String },

    #[error(transparent)]
    ArrowError(#[from] arrow_schema::ArrowError),

    #[error(transparent)]
    SerdeJsonError(#[from] serde_json::Error),

    #[error(transparent)]
    DataFusionError(#[from] datafusion::error::DataFusionError),

    #[error(transparent)]
    DeltaTableError(#[from] deltalake::errors::DeltaTableError),

    #[error(transparent)]
    ObjectStoreError(#[from] object_store::Error),
}

pub type SyncResult<T, E = SyncError> = Result<T, E>;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct SyncCommitInfo {
    // Internal version number
    version: u8,
    // The origin for which the last complete transaction was fully persisted
    origin: Origin,
    // The sequence number for which the last complete transaction was fully persisted
    sequence: SequenceNumber,
    // Flag denoting whether we've started flushing changes from a new (in-complete)
    // transaction
    new_tx: bool,
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
