use metrics::{
    counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram,
    Counter, Gauge, Histogram,
};

const REQUEST_BYTES: &str = "seafowl_changeset_writer_request_bytes";
const REQUEST_ROWS: &str = "seafowl_changeset_writer_request_rows";
const IN_MEMORY_BYTES: &str = "seafowl_changeset_writer_in_memory_bytes";
const IN_MEMORY_ROWS: &str = "seafowl_changeset_writer_in_memory_rows";
const COMPACTION_TIME: &str = "seafowl_changeset_writer_compaction_time";
const COMPACTED_BYTES: &str = "seafowl_changeset_writer_compacted_bytes";
const COMPACTED_ROWS: &str = "seafowl_changeset_writer_compacted_rows";
const FLUSHING_TIME: &str = "seafowl_changeset_writer_flushing_time";
const FLUSHED_BYTES: &str = "seafowl_changeset_writer_flushed_bytes";
const FLUSHED_ROWS: &str = "seafowl_changeset_writer_flushed_rows";
const FLUSHED_LAST: &str =
    "seafowl_changeset_writer_last_successful_flush_timestamp_seconds";

#[derive(Clone)]
pub struct SyncMetrics {
    pub request_bytes: Counter,
    pub request_rows: Counter,
    pub in_memory_bytes: Gauge,
    pub in_memory_rows: Gauge,
    pub compaction_time: Histogram,
    pub compacted_bytes: Counter,
    pub compacted_rows: Counter,
    pub flushing_time: Histogram,
    pub flushed_bytes: Counter,
    pub flushed_rows: Counter,
    pub flushed_last: Gauge,
}

impl Default for SyncMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl SyncMetrics {
    fn new() -> Self {
        describe_counter!(
            REQUEST_BYTES,
            "The total byte size of all batches in the sync message"
        );
        describe_counter!(
            REQUEST_BYTES,
            "The total row count of of all batches in the sync message"
        );
        describe_gauge!(
            IN_MEMORY_BYTES,
            "The total byte size of all pending batches in memory"
        );
        describe_gauge!(
            IN_MEMORY_BYTES,
            "The total row count of all pending batches in memory"
        );
        describe_histogram!(
            COMPACTION_TIME,
            "The time taken to compact a single sync message"
        );
        describe_counter!(
            COMPACTED_BYTES,
            "The reduction in byte size due to batch compaction"
        );
        describe_counter!(
            COMPACTED_BYTES,
            "The reduction in row count due to batch compaction"
        );
        describe_histogram!(
            FLUSHING_TIME,
            "The time taken to flush a collections of syncs"
        );
        describe_counter!(FLUSHED_BYTES, "The total byte size flushed");
        describe_counter!(FLUSHED_ROWS, "The total row count flushed");
        describe_counter!(FLUSHED_LAST, "The timestamp of the last successful flush");

        Self {
            request_bytes: counter!(REQUEST_BYTES),
            request_rows: counter!(REQUEST_ROWS),
            in_memory_bytes: gauge!(IN_MEMORY_BYTES),
            in_memory_rows: gauge!(IN_MEMORY_ROWS),
            compaction_time: histogram!(COMPACTION_TIME),
            compacted_bytes: counter!(COMPACTED_BYTES),
            compacted_rows: counter!(COMPACTED_ROWS),
            flushing_time: histogram!(FLUSHING_TIME),
            flushed_bytes: counter!(FLUSHED_BYTES),
            flushed_rows: counter!(FLUSHED_ROWS),
            flushed_last: gauge!(FLUSHED_LAST),
        }
    }
}
