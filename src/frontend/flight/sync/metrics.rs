use metrics::{
    counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram,
    Gauge, Histogram,
};

const IN_MEMORY_SIZE: &str = "seafowl_sync_writer_in_memory_size";
const IN_MEMORY_ROWS: &str = "seafowl_sync_writer_in_memory_rows";
const COMPACTION_TIME: &str = "seafowl_sync_writer_compaction_time";
const COMPACTED: &str = "seafowl_sync_writer_compacted";
const FLUSHING_TIME: &str = "seafowl_sync_writer_flushing_time";
const FLUSHED: &str = "seafowl_sync_writer_flushed";

#[derive(Clone)]
pub struct SyncMetrics {
    pub in_memory_size: Gauge,
    pub in_memory_rows: Gauge,
    pub compaction_time: Histogram,
    pub flushing_time: Histogram,
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
        describe_gauge!(IN_MEMORY_ROWS, "The total row count of all pending syncs");
        describe_histogram!(COMPACTION_TIME, "The time taken to compact a sync");
        describe_counter!(
            COMPACTED,
            "The reduction in rows and size due to batch compaction"
        );
        describe_histogram!(
            FLUSHING_TIME,
            "The time taken to flush a collections of syncs"
        );
        describe_counter!(FLUSHED, "The total rows and size flushed");

        Self {
            in_memory_size: gauge!(IN_MEMORY_SIZE),
            in_memory_rows: gauge!(IN_MEMORY_ROWS),
            compaction_time: histogram!(COMPACTION_TIME),
            flushing_time: histogram!(FLUSHING_TIME),
        }
    }

    pub fn compacted_size(&self, size: u64) {
        let compacted_size = counter!(COMPACTED, "unit" => "size");
        compacted_size.increment(size);
    }

    pub fn compacted_rows(&self, rows: u64) {
        let compacted_rows = counter!(COMPACTED, "unit" => "rows");
        compacted_rows.increment(rows);
    }

    pub fn flushed_size(&self, size: u64) {
        let flushed_size = counter!(FLUSHED, "unit" => "size");
        flushed_size.increment(size);
    }

    pub fn flushed_rows(&self, rows: u64) {
        let flushed_rows = counter!(FLUSHED, "unit" => "rows");
        flushed_rows.increment(rows);
    }
}
