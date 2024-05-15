use arrow::array::RecordBatch;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::Result;
use deltalake::kernel::Action;
use deltalake::logstore::LogStore;
use deltalake::protocol::{DeltaOperation, SaveMode};
use deltalake::DeltaTable;
use serde_json::Value;
use std::cmp::{Ordering, Reverse};
use std::collections::hash_map::Entry;
use std::collections::{BinaryHeap, HashMap};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::context::delta::plan_to_object_store;
use clade::flight::DataPutResult;

use crate::context::SeafowlContext;
use crate::frontend::flight::handler::SEAFOWL_PUT_DATA_SEQUENCE_NUMBER;

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
        // TODO: do this out-of-band
        while let Some(entry) = self.lags.peek()
            && now() - entry.0.insertion_time
                > self.context.config.misc.put_data.max_in_memory_duration_s
        {
            // Remove the entry from the priority queue
            let entry = self.lags.pop().unwrap().0;
            let url = entry.log_store.root_uri();

            let mut table = DeltaTable::new(entry.log_store.clone(), Default::default());
            table.load().await?;

            // Fetch the pending data to put
            let (mem_seq, data) = self.puts.remove(&url).unwrap();

            let schema = data.first().unwrap().schema();
            let input_plan: Arc<dyn ExecutionPlan> =
                Arc::new(MemoryExec::try_new(&[data], schema, None)?);

            // Exploit fast data upload to local FS, i.e. moving the file instead of actually copying
            let local_data_dir = if url.starts_with("file://") {
                Some(entry.log_store.root_uri())
            } else {
                None
            };

            // Dump the batches to the object store
            let adds = plan_to_object_store(
                &self.context.inner.state(),
                &input_plan,
                entry.log_store.object_store(),
                local_data_dir,
                self.context.config.misc.max_partition_size,
            )
            .await?;

            let mut actions: Vec<Action> = adds.into_iter().map(Action::Add).collect();

            // Append a special `CommitInfo` action to record new durable sequence number
            // tied to the commit.
            let info = HashMap::from([(
                SEAFOWL_PUT_DATA_SEQUENCE_NUMBER.to_string(),
                Value::Number(mem_seq.into()),
            )]);
            let commit_info = Action::commit_info(info);
            actions.push(commit_info);

            let op = DeltaOperation::Write {
                mode: SaveMode::Append,
                partition_by: None,
                predicate: None,
            };
            self.context.commit(actions, &table, op).await?;
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
