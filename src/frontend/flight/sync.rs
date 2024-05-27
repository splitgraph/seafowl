use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::datasource::TableProvider;
use datafusion::physical_expr::expressions::{col, lit};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::{Result, ScalarValue};
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
use tracing::{debug, warn};

use crate::context::delta::plan_to_object_store;

use crate::context::SeafowlContext;
use crate::frontend::flight::handler::SEAFOWL_SYNC_DATA_SEQUENCE_NUMBER;
use crate::frontend::flight::SEAFOWL_SYNC_DATA_UD_FLAG;

pub(super) struct SeafowlDataSyncManager {
    context: Arc<SeafowlContext>,
    // A priority queue of table locations sorted by their oldest in-memory insertion order
    lags: BinaryHeap<Reverse<DataSyncTarget>>,
    // A map of table URL => collection of identifiers and pending batches to upsert/delete
    syncs: HashMap<String, DataSyncCollection>,
    // Total size of all batches in memory currently.
    size: usize,
}

// A key identifying where to sync the pending data
#[derive(Debug)]
struct DataSyncTarget {
    // Table log store
    log_store: Arc<dyn LogStore>,
    // Unix timestamp of the oldest in-memory entry for this table
    insertion_time: u64,
}

impl PartialEq<Self> for DataSyncTarget {
    fn eq(&self, other: &Self) -> bool {
        self.log_store.root_uri() == other.log_store.root_uri()
    }
}

impl Eq for DataSyncTarget {}

// We're interested in comparing only by insertion time, so as to be able
// to keep the entries sorted by time.
impl PartialOrd for DataSyncTarget {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DataSyncTarget {
    fn cmp(&self, other: &Self) -> Ordering {
        self.insertion_time.cmp(&other.insertion_time)
    }
}

// An entry storing all pending in-memory data to replicate to a given location,
// potentially resulting from multiple `do_put` calls.
#[derive(Debug)]
struct DataSyncCollection {
    // Total in-memory size of all the batches in all the items for this table
    size: usize,
    // Collection of batches to replicate
    syncs: Vec<DataSyncItem>,
}

// An object corresponding to a single `do_put` call.
#[derive(Debug)]
#[allow(dead_code)]
struct DataSyncItem {
    // Identifier for where the change originates from
    origin: String,
    // LSN of the entry
    sequence_number: u64,
    // Primary keys
    pk_columns: Vec<String>,
    // Record batches to replicate
    batches: Vec<RecordBatch>,
}

impl SeafowlDataSyncManager {
    pub fn new(context: Arc<SeafowlContext>) -> Self {
        Self {
            context,
            lags: Default::default(),
            syncs: Default::default(),
            size: 0,
        }
    }

    // Extract the latest memory sequence number for a given table location.
    pub fn mem_seq_for_table(&self, url: &String) -> Option<u64> {
        self.syncs
            .get(url)
            .and_then(|entry| entry.syncs.last().map(|s| s.sequence_number))
    }

    // Store the pending data in memory and flush if the required criteria are met.
    pub async fn sync_batches(
        &mut self,
        log_store: Arc<dyn LogStore>,
        sequence_number: u64,
        origin: String,
        pk_columns: Vec<String>,
        batches: Vec<RecordBatch>,
    ) -> Result<()> {
        let size = batches
            .iter()
            .fold(0, |bytes, batch| bytes + batch.get_array_memory_size());

        let url = log_store.root_uri();
        let item = DataSyncItem {
            origin,
            sequence_number,
            pk_columns,
            batches,
        };
        match self.syncs.entry(url) {
            Entry::Occupied(mut entry) => {
                let entry = entry.get_mut();
                entry.syncs.push(item);
                entry.size += size;
            }
            Entry::Vacant(entry) => {
                entry.insert(DataSyncCollection {
                    size,
                    syncs: vec![item],
                });

                let insertion_time = now();
                let sync_entry = DataSyncTarget {
                    log_store,
                    insertion_time,
                };
                // We want a min-heap, so that oldest entry is pop-ed first
                self.lags.push(Reverse(sync_entry));
            }
        }

        // Update the total size
        self.size += size;

        self.flush_batches().await
    }

    // Criteria for return the cached entry ready to be persisted to storage.
    // First flush any records that are explicitly beyond the configured max
    // lag, followed by further entries if we're still above max cache size.
    fn flush_ready(&mut self) -> Option<DataSyncTarget> {
        if let Some(entry) = self.lags.peek()
            && now() - entry.0.insertion_time
                >= self.context.config.misc.sync_data.max_replication_lag_s
        {
            // TODO: do this out-of-band
            Some(self.lags.pop().unwrap().0)
        } else if self.size >= self.context.config.misc.sync_data.max_in_memory_bytes {
            // TODO: maybe better to pop from a size-based max-heap in this case
            Some(self.lags.pop().unwrap().0)
        } else {
            None
        }
    }

    async fn flush_batches(&mut self) -> Result<()> {
        // First flush any changes that are past the configured max duration
        while let Some(target) = self.flush_ready() {
            let log_store = target.log_store.clone();
            let url = log_store.root_uri();

            let mut table = DeltaTable::new(log_store.clone(), Default::default());
            table.load().await?;
            // Use the schema from the object store as a source of truth, since it's not guaranteed
            // that any of the entries has the full column list.
            let full_schema = TableProvider::schema(&table);

            // Fetch the pending data to sync
            let entry = match self.syncs.remove(&url) {
                None => {
                    warn!("No pending syncs found for {url}, discarding");
                    continue;
                }
                Some(entry) => entry,
            };

            self.size -= entry.size;

            let latest_sequence_number = entry.syncs.last().unwrap().sequence_number;

            // Iterate through all syncs for this table and construct a full plan
            let mut input_plan: Arc<dyn ExecutionPlan> =
                Arc::new(MemoryExec::try_new(&[], full_schema.clone(), None)?);
            for sync in entry.syncs {
                input_plan =
                    self.plan_data_sync(full_schema.clone(), input_plan, sync)?;
            }

            // To exploit fast data upload to local FS, i.e. simply move the partition files
            // once written to the disk, try to infer whether the location is a local dir
            let local_data_dir = if url.starts_with("file://") {
                Some(log_store.root_uri())
            } else {
                None
            };

            // Dump the batches to the object store
            let adds = plan_to_object_store(
                &self.context.inner.state(),
                &input_plan,
                log_store.object_store(),
                local_data_dir,
                self.context.config.misc.max_partition_size,
            )
            .await?;

            let mut actions: Vec<Action> = adds.into_iter().map(Action::Add).collect();

            // Append a special `CommitInfo` action to record new durable sequence number
            // tied to the commit.
            let info = HashMap::from([(
                SEAFOWL_SYNC_DATA_SEQUENCE_NUMBER.to_string(),
                Value::Number(latest_sequence_number.into()),
            )]);
            let commit_info = Action::commit_info(info);
            actions.push(commit_info);

            let op = DeltaOperation::Write {
                mode: SaveMode::Append,
                partition_by: None,
                predicate: None,
            };
            self.context.commit(actions, &table, op).await?;
            debug!(
                "Committed data sync up to {latest_sequence_number} for location {url}"
            );
        }

        Ok(())
    }

    fn plan_data_sync(
        &self,
        full_schema: SchemaRef,
        input_plan: Arc<dyn ExecutionPlan>,
        sync: DataSyncItem,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let data = sync.batches;
        let schema = data.first().unwrap().schema();
        let mem_plan: Arc<dyn ExecutionPlan> =
            Arc::new(MemoryExec::try_new(&[data], schema.clone(), None)?);

        // TODO: Filter away deletes for now
        let filter_plan: Arc<dyn ExecutionPlan> = Arc::new(FilterExec::try_new(
            col(SEAFOWL_SYNC_DATA_UD_FLAG, schema.as_ref())?,
            mem_plan,
        )?);

        // Normalize the schema, by ordering columns according to the full table schema and
        // projecting any missing columns as NULLs.
        let projection = full_schema
            .all_fields()
            .iter()
            .map(|f| {
                let name = f.name();
                if schema.column_with_name(name).is_some() {
                    Ok((col(name, schema.as_ref())?, name.to_string()))
                } else {
                    Ok((
                        lit(ScalarValue::Null.cast_to(f.data_type())?),
                        name.to_string(),
                    ))
                }
            })
            .collect::<Result<_>>()?;
        let proj_plan = Arc::new(ProjectionExec::try_new(projection, filter_plan)?);

        Ok(Arc::new(UnionExec::new(vec![input_plan, proj_plan])))
    }
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs()
}
