use metrics::{describe_gauge, gauge, Gauge};

const IN_MEMORY_SIZE: &str = "seafowl_sync_writer_in_memory_size";
const IN_MEMORY_ROWS: &str = "seafowl_sync_writer_in_memory_rows";

#[derive(Clone)]
pub struct SyncMetrics {
    pub in_memory_size: Gauge,
    pub in_memory_rows: Gauge,
}

impl Default for SyncMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl SyncMetrics {
    fn new() -> Self {
        describe_gauge!(
            IN_MEMORY_SIZE,
            "The total size of all pending syncs in bytes"
        );
        describe_gauge!(IN_MEMORY_ROWS, "The total row count of all pending sync");

        Self {
            in_memory_size: gauge!(IN_MEMORY_SIZE),
            in_memory_rows: gauge!(IN_MEMORY_ROWS),
        }
    }
}
