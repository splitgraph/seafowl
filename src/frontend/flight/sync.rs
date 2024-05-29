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
use indexmap::IndexMap;
use itertools::Itertools;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, info, warn};

use crate::context::delta::plan_to_object_store;

use crate::context::SeafowlContext;
use crate::frontend::flight::handler::SEAFOWL_SYNC_DATA_SEQUENCE_NUMBER;
use crate::frontend::flight::SEAFOWL_SYNC_DATA_UD_FLAG;

type SequenceNumber = u64;
type OriginSequence = (String, SequenceNumber);

pub(super) struct SeafowlDataSyncManager {
    context: Arc<SeafowlContext>,
    // All sequences kept in memory
    seqs: HashMap<OriginSequence, DataSyncSequence>,
    // An indexed queue of table URL => pending batches to upsert/delete
    // sorted by insertion order
    syncs: IndexMap<String, DataSyncCollection>,
    // Total size of all batches in memory currently
    size: usize,
    // Map of known memory sequence numbers per origin
    origin_memory: HashMap<String, SequenceNumber>,
    // Map of known durable sequence numbers per origin
    origin_durable: HashMap<String, SequenceNumber>,
}

// A struct tracking relevant information about a single transaction/sequence from a single origin
// that may stretch across several sync commands.
#[derive(Debug, Clone)]
struct DataSyncSequence {
    // Flag denoting whether we've seen the last sync command in this sequence
    last: bool,
    // Set of locations that need to be flushed to for this sequence
    locs: HashSet<String>,
}

// An entry storing all pending in-memory data to replicate to a single table location,
// potentially resulting from multiple `do_put` calls across disparate sequences and origins.
#[derive(Debug)]
struct DataSyncCollection {
    // Total in-memory size of all the batches in all the items for this table
    size: usize,
    // Unix epoch of the first sync command in this collection
    insertion_time: u64,
    // Table log store
    log_store: Arc<dyn LogStore>,
    // Collection of batches to replicate
    syncs: Vec<DataSyncItem>,
}

// An object corresponding to a single `do_put` call.
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct DataSyncItem {
    // Identifier of the origin where the change stems from as well the sequence number
    origin_sequence: OriginSequence,
    // Primary keys
    // TODO: this should probably be per-collection (not changing from sequence to sequence)
    pk_columns: Vec<String>,
    // Record batches to replicate
    batches: Vec<RecordBatch>,
}

impl SeafowlDataSyncManager {
    pub fn new(context: Arc<SeafowlContext>) -> Self {
        Self {
            context,
            seqs: Default::default(),
            syncs: Default::default(),
            size: 0,
            origin_memory: Default::default(),
            origin_durable: Default::default(),
        }
    }

    // Extract the latest memory sequence number for a given table location.
    pub fn stored_sequences(
        &self,
        origin: &String,
    ) -> (Option<SequenceNumber>, Option<SequenceNumber>) {
        (
            self.origin_memory.get(origin).cloned(),
            self.origin_durable.get(origin).cloned(),
        )
    }

    // Store the pending data in memory and flush if the required criteria are met.
    pub async fn sync_batches(
        &mut self,
        log_store: Arc<dyn LogStore>,
        sequence_number: SequenceNumber,
        origin: String,
        pk_columns: Vec<String>,
        last: bool,
        batches: Vec<RecordBatch>,
    ) -> Result<(Option<SequenceNumber>, Option<SequenceNumber>)> {
        let url = log_store.root_uri();

        let orseq = (origin.clone(), sequence_number);

        // Upsert a sequence entry for this origin/LSN
        self
            .seqs
            .entry(orseq.clone())
            .and_modify(|seq| {
                if !seq.locs.contains(&url) {
                    debug!("Adding {url} as sync destination for {origin}, {sequence_number}");
                    seq.locs.insert(url.clone());
                }

                if last {
                    debug!(
                        "Received last sync for {url} from {origin}, {sequence_number}"
                    );
                    seq.last = true;
                }
            })
            .or_insert(DataSyncSequence {
                last,
                locs: HashSet::from([url.clone()]),
            });

        // Finally upsert the new sync item for this location
        let size = batches
            .iter()
            .fold(0, |bytes, batch| bytes + batch.get_array_memory_size());

        let item = DataSyncItem {
            origin_sequence: orseq,
            pk_columns,
            batches,
        };
        self.syncs
            .entry(url)
            .and_modify(|entry| {
                entry.syncs.push(item.clone());
                entry.size += size;
            })
            .or_insert(DataSyncCollection {
                size,
                insertion_time: now(),
                log_store,
                syncs: vec![item],
            });

        // Update the total size
        self.size += size;

        // Flag the sequence as volatile persisted for this origin if it is the last sync command
        if last {
            self.origin_memory.insert(origin.clone(), sequence_number);
        }

        self.flush_batches().await?;

        Ok(self.stored_sequences(&origin))
    }

    // Criteria for return the cached entry ready to be persisted to storage.
    // First flush any records that are explicitly beyond the configured max
    // lag, followed by further entries if we're still above max cache size.
    fn flush_ready(&mut self) -> Option<String> {
        if let Some((url, sync)) = self.syncs.first()
            && now() - sync.insertion_time
                >= self.context.config.misc.sync_data.max_replication_lag_s
        {
            // First flush any changes that are past the configured max duration
            // TODO: do this out-of-band
            Some(url.clone())
        } else if self.size >= self.context.config.misc.sync_data.max_in_memory_bytes {
            // Or if we're over the size limit flush the oldest entry
            Some(self.syncs.first().unwrap().0.clone())
        } else {
            None
        }
    }

    async fn flush_batches(&mut self) -> Result<()> {
        while let Some(url) = self.flush_ready() {
            // Fetch the pending data to sync
            let entry = match self.syncs.get(&url) {
                None => {
                    warn!("No pending syncs found for {url}, discarding");
                    continue;
                }
                Some(entry) => entry,
            };

            let log_store = entry.log_store.clone();

            let last_sequence_number = entry.syncs.last().unwrap().origin_sequence.1;

            let mut table = DeltaTable::new(log_store.clone(), Default::default());
            table.load().await?;

            if let Some(table_seq) = self.table_sequence(&table).await?
                && table_seq > last_sequence_number
            {
                info!(
                    "Location at {url} already durable up to {table_seq}, skipping {}",
                    last_sequence_number
                );
                self.remove_sync(url);
                continue;
            }

            // Use the schema from the object store as a source of truth, since it's not guaranteed
            // that any of the entries has the full column list.
            let full_schema = TableProvider::schema(&table);

            // Iterate through all syncs for this table and construct a full plan
            let mut input_plan: Arc<dyn ExecutionPlan> =
                Arc::new(MemoryExec::try_new(&[], full_schema.clone(), None)?);
            for sync in &entry.syncs {
                let data = sync.batches.clone();
                input_plan =
                    self.plan_data_sync(full_schema.clone(), input_plan, data)?;
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
                Value::Number(last_sequence_number.into()),
            )]);
            let commit_info = Action::commit_info(info);
            actions.push(commit_info);

            let op = DeltaOperation::Write {
                mode: SaveMode::Append,
                partition_by: None,
                predicate: None,
            };
            self.context.commit(actions, &table, op).await?;
            debug!("Committed data sync up to {last_sequence_number} for location {url}");

            // We've flushed all the presently accumulated batches for this location, and if we
            // observed the last sync for any of the sequences we should set them to durable
            for orseq in entry
                .syncs
                .iter()
                .map(|s| s.origin_sequence.clone())
                .unique()
                .collect::<Vec<_>>()
            {
                if let Some(seq) = self.seqs.get_mut(&orseq) {
                    // Remove the pending location for this origin/sequence
                    seq.locs.remove(&url);

                    if seq.locs.is_empty() && seq.last {
                        // We've seen the last sync for this sequence and all pending locations
                        // have been flushed to, so it's we're good to flag the sequence as durable
                        // Remove from sequences maps

                        self.origin_durable.insert(orseq.0.clone(), orseq.1);

                        // Finally remove from the sequence map
                        self.seqs.remove(&orseq);
                    }
                }
            }
            self.remove_sync(url);
        }

        Ok(())
    }

    // Remove the in-memory sync collection for the provided location, and update the size
    fn remove_sync(&mut self, url: String) {
        if let Some(sync) = self.syncs.shift_remove(&url) {
            self.size -= sync.size;
        }
    }

    // Inspect the table logs to find out what is the latest sequence number committed.
    // Note that this doesn't guarantee that the sequence is durable, since we may not have
    // yet received the last sync from it, or even if we have we may not have flushed all
    // the locations.
    async fn table_sequence(&self, table: &DeltaTable) -> Result<Option<SequenceNumber>> {
        let commit_infos = table.history(Some(1)).await?;
        Ok(
            match commit_infos
                .last()
                .expect("Table has non-zero commits")
                .info
                .get(SEAFOWL_SYNC_DATA_SEQUENCE_NUMBER)
            {
                Some(Value::Number(seq)) => seq.as_u64(),
                _ => None,
            },
        )
    }

    fn plan_data_sync(
        &self,
        full_schema: SchemaRef,
        input_plan: Arc<dyn ExecutionPlan>,
        data: Vec<RecordBatch>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
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
