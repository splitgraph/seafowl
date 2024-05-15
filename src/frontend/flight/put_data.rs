use arrow::array::RecordBatch;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::Result;
use deltalake::logstore::LogStore;
use std::cmp::{Ordering, Reverse};
use std::collections::hash_map::Entry;
use std::collections::{BinaryHeap, HashMap};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::context::delta::plan_to_object_store;
use clade::flight::DataPutResult;

use crate::context::SeafowlContext;

pub(super) struct SeafowlPutDataManager {
    context: Arc<SeafowlContext>,
    // A priority queue of table locations sorted by their oldest in-memory insertion order
    lags: BinaryHeap<Reverse<DataPutEntry>>,
    // A map of table URL => identifier (e.g. LSN) and pending batches to upsert/delete
    puts: HashMap<String, (u64, Vec<RecordBatch>)>,
    // Total size of all batches in memory currently.
    size: usize,
}

// A key identifying where the to put the pending data
#[derive(Debug)]
struct DataPutEntry {
    // Table log store
    log_store: Arc<dyn LogStore>,
    // Unix timestamp of the oldest in-memory entry for this table
    insertion_time: u64,
}

impl PartialEq<Self> for DataPutEntry {
    fn eq(&self, other: &Self) -> bool {
        self.log_store.root_uri() == other.log_store.root_uri()
    }
}

impl Eq for DataPutEntry {}

// We're interested in comparing only by insertion time, so as to be able
// to keep the entries sorted by time.
impl PartialOrd for DataPutEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DataPutEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.insertion_time.cmp(&other.insertion_time)
    }
}

impl SeafowlPutDataManager {
    pub fn new(context: Arc<SeafowlContext>) -> Self {
        Self {
            context,
            lags: Default::default(),
            puts: Default::default(),
            size: 0,
        }
    }

    // Extract the latest memory sequence number for a given table location.
    pub fn mem_seq_for_table(&self, url: &String) -> Option<u64> {
        self.puts.get(url).map(|(mem_seq, _)| *mem_seq)
    }

    pub async fn put_batches(
        &mut self,
        log_store: Arc<dyn LogStore>,
        sequence_number: u64,
        mut batches: Vec<RecordBatch>,
    ) -> Result<DataPutResult> {
        let size = batches
            .iter()
            .fold(0, |bytes, batch| bytes + batch.get_array_memory_size());

        let url = log_store.root_uri();
        match self.puts.entry(url) {
            Entry::Occupied(mut entry) => {
                let (mut _seq, data) = entry.get_mut();
                _seq = sequence_number;
                data.append(&mut batches);
            }
            Entry::Vacant(entry) => {
                entry.insert((sequence_number, batches));

                let insertion_time = now();
                let put_entry = DataPutEntry {
                    log_store,
                    insertion_time,
                };
                // We want a min-heap, so that oldest entry is pop-ed first
                self.lags.push(Reverse(put_entry));
            }
        }

        // Update the total size
        self.size += size;

        self.flush_batches().await?;

        Ok(DataPutResult {
            accepted: true,
            memory_sequence_number: Some(sequence_number),
            durable_sequence_number: None,
        })
    }

    async fn flush_batches(&mut self) -> Result<()> {
        // First flush any changes that are past the configured max duration
        while let Some(entry) = self.lags.peek()
            && now() - entry.0.insertion_time
                > self.context.config.misc.put_data.max_in_memory_duration_s
        {
            // Remove the entry from the priority queue
            let entry = self.lags.pop().unwrap().0;
            let url = entry.log_store.root_uri();

            // Fetch the pending data to put
            let (_mem_seq, data) = self.puts.remove(&url).unwrap();

            let schema = data.first().unwrap().schema();
            let input_plan: Arc<dyn ExecutionPlan> =
                Arc::new(MemoryExec::try_new(&[data], schema, None)?);

            // Exploit fast data upload to local FS, i.e. moving the file instead of actually copying
            let local_data_dir = if url.starts_with("file://") {
                Some(entry.log_store.root_uri())
            } else {
                None
            };

            plan_to_object_store(
                &self.context.inner.state(),
                &input_plan,
                entry.log_store.object_store(),
                local_data_dir,
                self.context.config.misc.max_partition_size,
            )
            .await?;
        }

        Ok(())
    }
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs()
}
