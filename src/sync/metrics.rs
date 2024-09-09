use crate::sync::{Origin, SequenceNumber};
use metrics::{
    counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram,
    Counter, Gauge, Histogram,
};

const REQUEST_BYTES: &str = "seafowl_changeset_writer_request_bytes_total";
const REQUEST_ROWS: &str = "seafowl_changeset_writer_request_rows_total";
const IN_MEMORY_BYTES: &str = "seafowl_changeset_writer_in_memory_bytes_current";
const IN_MEMORY_ROWS: &str = "seafowl_changeset_writer_in_memory_rows_current";
const IN_MEMORY_OLDEST: &str =
    "seafowl_changeset_writer_in_memory_oldest_timestamp_seconds";
const SQUASH_TIME: &str = "seafowl_changeset_writer_squash_time_seconds";
const SQUASHED_BYTES: &str = "seafowl_changeset_writer_squashed_bytes_total";
const SQUASHED_ROWS: &str = "seafowl_changeset_writer_squashed_rows_total";
const PRUNING_TIME: &str = "seafowl_changeset_writer_pruning_time_milliseconds";
const PRUNING_FILES: &str = "seafowl_changeset_writer_pruning_files_total";
const PLANNING_TIME: &str = "seafowl_changeset_writer_planning_time_milliseconds";
const FLUSH_TIME: &str = "seafowl_changeset_writer_flush_time_seconds";
const FLUSH_BYTES: &str = "seafowl_changeset_writer_flush_bytes_total";
const FLUSH_ROWS: &str = "seafowl_changeset_writer_flush_rows_total";
const FLUSH_LAST: &str =
    "seafowl_changeset_writer_last_successful_flush_timestamp_seconds_current";
const FLUSH_LAG: &str = "seafowl_changeset_writer_flush_lag_seconds";
const SEQUENCE_DURABLE: &str = "seafowl_changeset_writer_sequence_durable_bytes";
const SEQUENCE_MEMORY: &str = "seafowl_changeset_writer_sequence_memory_bytes";

#[derive(Clone)]
pub struct SyncWriterMetrics {
    pub request_bytes: Counter,
    pub request_rows: Counter,
    pub in_memory_bytes: Gauge,
    pub in_memory_rows: Gauge,
    pub in_memory_oldest: Gauge,
    pub squash_time: Histogram,
    pub squashed_bytes: Counter,
    pub squashed_rows: Counter,
    pub planning_time: Histogram,
    pub flush_time: Histogram,
    pub flush_bytes: Counter,
    pub flush_rows: Counter,
    pub flush_last: Gauge,
    pub flush_lag: Histogram,
}

impl Default for SyncWriterMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl SyncWriterMetrics {
    fn new() -> Self {
        describe_counter!(
            REQUEST_BYTES,
            "The total byte size of all batches in the sync message"
        );
        describe_counter!(
            REQUEST_ROWS,
            "The total row count of of all batches in the sync message"
        );
        describe_gauge!(
            IN_MEMORY_BYTES,
            "The total byte size of all pending batches in memory"
        );
        describe_gauge!(
            IN_MEMORY_ROWS,
            "The total row count of all pending batches in memory"
        );
        describe_gauge!(
            IN_MEMORY_OLDEST,
            "The timestamp of the oldest pending change set in memory"
        );
        describe_histogram!(
            SQUASH_TIME,
            "The time taken to squash a single sync message"
        );
        describe_counter!(
            SQUASHED_BYTES,
            "The reduction in byte size due to batch squashing"
        );
        describe_counter!(
            SQUASHED_ROWS,
            "The reduction in row count due to batch squashing"
        );
        describe_histogram!(PLANNING_TIME, "The time taken to construct the flush plan");
        describe_histogram!(FLUSH_TIME, "The time taken to flush a collections of syncs");
        describe_counter!(FLUSH_BYTES, "The total byte size flushed");
        describe_counter!(FLUSH_ROWS, "The total row count flushed");
        describe_counter!(FLUSH_LAST, "The timestamp of the last successful flush");
        describe_counter!(
            FLUSH_LAG,
            "The total time between the first queued change set and flush"
        );
        describe_gauge!(SEQUENCE_DURABLE, "The durable sequence number per origin");
        describe_gauge!(SEQUENCE_MEMORY, "The memory sequence number per origin");

        Self {
            request_bytes: counter!(REQUEST_BYTES),
            request_rows: counter!(REQUEST_ROWS),
            in_memory_bytes: gauge!(IN_MEMORY_BYTES),
            in_memory_rows: gauge!(IN_MEMORY_ROWS),
            in_memory_oldest: gauge!(IN_MEMORY_OLDEST),
            squash_time: histogram!(SQUASH_TIME),
            squashed_bytes: counter!(SQUASHED_BYTES),
            squashed_rows: counter!(SQUASHED_ROWS),
            planning_time: histogram!(PLANNING_TIME),
            flush_time: histogram!(FLUSH_TIME),
            flush_bytes: counter!(FLUSH_BYTES),
            flush_rows: counter!(FLUSH_ROWS),
            flush_last: gauge!(FLUSH_LAST),
            flush_lag: histogram!(FLUSH_LAG),
        }
    }

    pub fn sequence_durable(&self, origin: &Origin, sequence: SequenceNumber) {
        let sequence_durable = gauge!(SEQUENCE_DURABLE, "origin" => origin.to_string());
        sequence_durable.set(sequence as f64);
    }

    pub fn sequence_memory(&self, origin: &Origin, sequence: SequenceNumber) {
        let sequence_memory = gauge!(SEQUENCE_MEMORY, "origin" => origin.to_string());
        sequence_memory.set(sequence as f64);
    }
}

#[derive(Clone)]
pub struct SyncPlanMetrics {
    pub pruning_time: Histogram,
    pub pruning_files: Histogram,
}

impl Default for SyncPlanMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl SyncPlanMetrics {
    fn new() -> Self {
        describe_histogram!(
            PRUNING_TIME,
            "The time taken to prune partition files to re-write"
        );
        describe_histogram!(
            PRUNING_FILES,
            "The file count that partition pruning identified"
        );

        Self {
            pruning_time: histogram!(PRUNING_TIME),
            pruning_files: histogram!(PRUNING_FILES),
        }
    }
}
