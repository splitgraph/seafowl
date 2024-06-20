use metrics::{
    counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram,
    Gauge, Histogram,
};

const REQUEST: &str = "seafowl_sync_writer_request";
const IN_MEMORY: &str = "seafowl_sync_writer_in_memory";
const COMPACTION_TIME: &str = "seafowl_sync_writer_compaction_time";
const COMPACTED: &str = "seafowl_sync_writer_compacted";
const FLUSHING_TIME: &str = "seafowl_sync_writer_flushing_time";
const FLUSHED: &str = "seafowl_sync_writer_flushed";
const UNIT_LABEL: &str = "unit";
const UNIT_SIZE: &str = "size";
const UNIT_ROWS: &str = "rows";

#[derive(Clone)]
pub struct SyncMetrics {
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
        describe_counter!(
            REQUEST,
            "The total byte size and row count of of all batches in the sync message"
        );
        describe_gauge!(
            IN_MEMORY,
            "The total byte size and row count of all pending syncs"
        );
        describe_histogram!(
            COMPACTION_TIME,
            "The time taken to compact a single sync message"
        );
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
            compaction_time: histogram!(COMPACTION_TIME),
            flushing_time: histogram!(FLUSHING_TIME),
        }
    }

    pub fn request_size(&self, size: u64) {
        let request_size = counter!(REQUEST, UNIT_LABEL => UNIT_SIZE);
        request_size.increment(size);
    }

    pub fn request_rows(&self, rows: u64) {
        let request_rows = counter!(REQUEST, UNIT_LABEL => UNIT_ROWS);
        request_rows.increment(rows);
    }

    pub fn in_memory_size(&self) -> Gauge {
        gauge!(IN_MEMORY, UNIT_LABEL => UNIT_SIZE)
    }

    pub fn in_memory_rows(&self) -> Gauge {
        gauge!(IN_MEMORY, UNIT_LABEL => UNIT_ROWS)
    }

    pub fn compacted_size(&self, size: u64) {
        let compacted_size = counter!(COMPACTED, UNIT_LABEL => UNIT_SIZE);
        compacted_size.increment(size);
    }

    pub fn compacted_rows(&self, rows: u64) {
        let compacted_rows = counter!(COMPACTED, UNIT_LABEL => UNIT_ROWS);
        compacted_rows.increment(rows);
    }

    pub fn flushed_size(&self, size: u64) {
        let flushed_size = counter!(FLUSHED, UNIT_LABEL => UNIT_SIZE);
        flushed_size.increment(size);
    }

    pub fn flushed_rows(&self, rows: u64) {
        let flushed_rows = counter!(FLUSHED, UNIT_LABEL => UNIT_ROWS);
        flushed_rows.increment(rows);
    }
}
