use crate::sync::writer::SeafowlDataSyncWriter;
use deltalake::logstore::LogStore;
use iceberg::io::FileIO;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::warn;

mod metrics;
mod planner;
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

    #[error("Not implemented")]
    NotImplemented,

    #[error(transparent)]
    ArrowError(#[from] arrow_schema::ArrowError),

    #[error(transparent)]
    SerdeJsonError(#[from] serde_json::Error),

    #[error(transparent)]
    DataFusionError(#[from] datafusion::error::DataFusionError),

    #[error(transparent)]
    DeltaTableError(#[from] deltalake::errors::DeltaTableError),

    #[error(transparent)]
    IcebergError(#[from] iceberg::Error),

    #[error(transparent)]
    ObjectStoreError(#[from] object_store::Error),
}

pub type SyncResult<T, E = SyncError> = Result<T, E>;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
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

#[derive(Clone, Debug)]
pub struct IcebergSyncTarget {
    pub file_io: FileIO,
    pub url: String,
}

#[derive(Clone, Debug)]
pub enum LakehouseSyncTarget {
    Delta(Arc<dyn LogStore>),
    Iceberg(IcebergSyncTarget),
}

impl LakehouseSyncTarget {
    pub fn get_url(&self) -> String {
        match self {
            LakehouseSyncTarget::Iceberg(IcebergSyncTarget { url, .. }) => url.clone(),
            LakehouseSyncTarget::Delta(log_store) => log_store.root_uri(),
        }
    }
}

impl SyncCommitInfo {
    pub(super) fn new(
        origin: impl Into<Origin>,
        sequence: impl Into<SequenceNumber>,
    ) -> Self {
        SyncCommitInfo {
            version: 0,
            origin: origin.into(),
            sequence: sequence.into(),
            new_tx: false,
        }
    }

    pub(super) fn with_new_tx(mut self, new_tx: bool) -> Self {
        self.new_tx = new_tx;
        self
    }
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
