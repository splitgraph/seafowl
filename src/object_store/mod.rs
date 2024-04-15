pub mod cache;
pub mod http;
pub mod wrapped;

#[derive(Clone, Debug, thiserror::Error)]
pub enum ObjectStoreError {
    #[error("{reason}")]
    Generic { reason: String },
}
