mod metrics;
pub mod schema;
mod utils;
pub(crate) mod writer;

#[derive(Debug, thiserror::Error)]
pub enum SyncError {
    #[error("Invalid sync schema: {reason}")]
    SchemaError { reason: String },
}
