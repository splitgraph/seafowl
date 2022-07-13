use std::sync::Arc;

use datafusion::prelude::{SessionConfig, SessionContext};
use object_store::memory::InMemory;

pub fn make_session() -> SessionContext {
    let session_config = SessionConfig::new().with_information_schema(true);

    let context = SessionContext::with_config(session_config);
    let object_store = Arc::new(InMemory::new());
    context
        .runtime_env()
        .register_object_store("seafowl", "", object_store);
    context
}
