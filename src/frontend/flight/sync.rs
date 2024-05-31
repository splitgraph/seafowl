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
use deltalake::kernel::{Action, Schema};
use deltalake::logstore::LogStore;
use deltalake::operations::create::CreateBuilder;
use deltalake::protocol::{DeltaOperation, SaveMode};
use deltalake::DeltaTable;
use indexmap::IndexMap;
use itertools::Itertools;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, info};

use crate::context::delta::plan_to_object_store;

use crate::context::SeafowlContext;
use crate::frontend::flight::handler::SEAFOWL_SYNC_DATA_SEQUENCE_NUMBER;
use crate::frontend::flight::SEAFOWL_SYNC_DATA_UD_FLAG;

type SequenceNumber = u64;

// A handler for caching, coalescing and flushing table syncs received via
// the Arrow Flight `do_put` calls.
//
// Each `DataSyncCommand` that accompanies a record batch carries information on
// the origin of change, sequence number, primary keys and whether this is the
// last message in the sequence.
//
// It uses a greedy table-based algorithm for flushing: once the criteria is met
// it will go through table's ordered by the oldest sync flushing all pending syncs
// for that table. Special care is taken about deducing what is the correct durable
// sequence to report back to the caller.
//
//           │sync #1│sync #2│sync #3│sync #4│sync #5│
//    ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ▶
//           │┌─────┐│       │       │┌─────┐│       │
//    table_1 │seq:1│                 │  3  │
//           │└─────┘│       │       │└─────┘│       │
//    ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ▶
//           │       │┌─────┐│       │       │┌─────┐│
//    table_2         │  1  │                 │  3  │
//           │       │└─────┘│       │       │└─────┘│
//    ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ▶
//           │       │       │┌─────┐│       │       │
//    table_3                 │  2  │
//           │       │       │└─────┘│       │       │
//    ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ▶
//           ▼       ▼       ▼       ▼       ▼       ▼
// In the above example, the first flush will target table_1, dumping payloads
// of sync #1 and sync #4 (from sequences 1 and 3). Since this is not the last
// sync of the first sequence it won't be reported as durably stored yet.
//
// Next, table_2 will get flushed (sync #2 and sync #5); this time sequence 1
// is entirely persisted to storage so it will be reported as the new durable
// sequence number. Note that while sequence 3 is also now completely flushed,
// it isn't durable, since there is a preceding sequence (2) that is still in memory.
//
// Finally, once table_3 is flushed `SeafowlDataSyncManager` will advance the
// durable sequence up to 3, since both it and 2 have now been completely persisted.
pub(super) struct SeafowlDataSyncManager {
    context: Arc<SeafowlContext>,
    // All sequences kept in memory, queued up by insertion order, per origin,
    seqs: HashMap<String, IndexMap<SequenceNumber, DataSyncSequence>>,
    // An indexed queue of table URL => pending syncs with actual batches to
    // upsert/delete sorted by insertion order
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
    // Identifier of the origin where the change stems from
    origin: String,
    // Sequence number of this particular change and origin
    sequence_number: SequenceNumber,
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
    pub async fn enqueue_sync(
        &mut self,
        log_store: Arc<dyn LogStore>,
        sequence_number: SequenceNumber,
        origin: String,
        pk_columns: Vec<String>,
        last: bool,
        batches: Vec<RecordBatch>,
    ) -> Result<(Option<SequenceNumber>, Option<SequenceNumber>)> {
        let url = log_store.root_uri();

        // If there's no delta table at this location yet create one first.
        if !log_store.is_delta_table_location().await? {
            debug!("Creating new Delta table at location: {url}");
            self.create_table(log_store.clone(), &batches).await?;
        }

        // Upsert a sequence entry for this origin and sequence number
        let sequence = DataSyncSequence {
            last,
            locs: HashSet::from([url.clone()]),
        };
        self
            .seqs
            .entry(origin.clone())
            .and_modify(|origin_seqs| {
                origin_seqs.entry(sequence_number).and_modify(|seq| {if !seq.locs.contains(&url) {
                    debug!("Adding {url} as sync destination for {origin}, {sequence_number}");
                    seq.locs.insert(url.clone());
                }

                    if last {
                        debug!(
                        "Received last sync for {url} from {origin}, {sequence_number}"
                    );
                        seq.last = true;
                    }}).or_insert(sequence.clone());
            })
            .or_insert(IndexMap::from([(sequence_number, sequence)]));

        // Finally upsert the new sync item for this location
        let size = batches
            .iter()
            .fold(0, |bytes, batch| bytes + batch.get_array_memory_size());

        let item = DataSyncItem {
            origin: origin.clone(),
            sequence_number,
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

        while self.flush_ready() {
            // TODO: do out-of-band
            self.flush_syncs().await?;
        }

        Ok(self.stored_sequences(&origin))
    }

    async fn create_table(
        &self,
        log_store: Arc<dyn LogStore>,
        batches: &[RecordBatch],
    ) -> Result<DeltaTable> {
        // Get the actual table schema by removing the `SEAFOWL_SYNC_DATA_UD_FLAG` column
        // from the first sync.
        let schema = batches.first().unwrap().schema();
        let idxs = (0..schema.all_fields().len() - 1).collect::<Vec<usize>>();
        let schema = schema.project(&idxs)?;

        let delta_schema = Schema::try_from(&schema)?;

        Ok(CreateBuilder::new()
            .with_log_store(log_store)
            .with_columns(delta_schema.fields().clone())
            .with_comment(format!("Synced by Seafowl {}", env!("CARGO_PKG_VERSION")))
            .await?)
    }

    // Criteria for return the cached entry ready to be persisted to storage.
    // First flush any records that are explicitly beyond the configured max
    // lag, followed by further entries if we're still above max cache size.
    fn flush_ready(&mut self) -> bool {
        if let Some((_, sync)) = self.syncs.first()
            && now() - sync.insertion_time
                >= self.context.config.misc.sync_data.max_replication_lag_s
        {
            // First flush any changes that are past the configured max duration
            true
        } else if self.size >= self.context.config.misc.sync_data.max_in_memory_bytes {
            // Or if we're over the size limit flush the oldest entry
            true
        } else {
            false
        }
    }

    // Flush the table containing the oldest sync in memory
    async fn flush_syncs(&mut self) -> Result<()> {
        if let Some((url, entry)) = self.syncs.first() {
            let url = url.clone();
            let log_store = entry.log_store.clone();

            let last_sequence_number = entry.syncs.last().unwrap().sequence_number;

            let mut table = DeltaTable::new(log_store.clone(), Default::default());
            table.load().await?;

            if let Some(table_seq) = self.table_sequence(&table).await?
                && table_seq > last_sequence_number
            {
                info!(
                    "Location at {url} already durable up to {table_seq}, skipping {}",
                    last_sequence_number
                );
                self.remove_sync(&url);
                return Ok(());
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

            // We've flushed all the presently accumulated batches for this location.
            // Modify our syncs and sequences maps to reflect this.
            let orseq = entry
                .syncs
                .iter()
                .map(|s| (s.origin.clone(), s.sequence_number))
                .unique()
                .collect::<Vec<_>>();
            self.remove_sequence_locations(url.clone(), orseq);
            self.remove_sync(&url);
            self.advance_durable();
        }

        Ok(())
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

    // Remove the pending location from a sequence for all syncs in the collection
    fn remove_sequence_locations(
        &mut self,
        url: String,
        orseq: Vec<(String, SequenceNumber)>,
    ) {
        for (origin, seq_num) in orseq {
            if let Some(origin_seqs) = self.seqs.get_mut(&origin) {
                if let Some(seq) = origin_seqs.get_mut(&seq_num) {
                    // Remove the pending location for this origin/sequence
                    seq.locs.remove(&url);
                }
            }
        }
    }

    // Remove the in-memory sync collection for the provided location, and update the size
    fn remove_sync(&mut self, url: &String) {
        if let Some(sync) = self.syncs.shift_remove(url) {
            self.size -= sync.size;
        }
    }

    // Iterate through all origins and all sequences and:
    //    - mark as durable all flushed and final sequences up to the first one that is not
    //    - remove the durable sequences from the map
    fn advance_durable(&mut self) {
        let mut origins_to_remove = HashSet::new();

        for (origin, origin_seqs) in &mut self.seqs {
            let mut remove_count = 0;
            let mut new_durable = 0;

            // Iteration is in order of insertion, so it's basically a FIFO queue
            for (seq_num, seq) in origin_seqs.into_iter() {
                if seq.locs.is_empty() && seq.last {
                    // We've seen the last sync for this sequence, all pending locations
                    // have been flushed to and there's no preceding sequence to be flushed,
                    // so we're good to flag the sequence as durable
                    self.origin_durable.insert(origin.clone(), *seq_num);

                    remove_count += 1;
                    new_durable = *seq_num;
                    debug!("Set new durable sequence {new_durable} for {origin}");
                } else {
                    // We've run into a sequence that is either not last or still has locations
                    // that need to be flushed
                    break;
                }
            }

            if remove_count == origin_seqs.len() {
                // Remove the origin since there are no more sequences remaining
                debug!("No more pending sequences for origin {origin}, removing");
                origins_to_remove.insert(origin.clone());
            } else if remove_count > 0 {
                // Remove the durable sequences for this origin
                debug!("Trimming pending sequences for {origin} up to {new_durable}");
                origin_seqs.retain(|sn, _| sn > &new_durable);
            }
        }

        for origin in origins_to_remove {
            self.seqs.remove(&origin);
        }
    }
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs()
}

#[cfg(test)]
mod tests {
    use crate::context::test_utils::in_memory_context;
    use crate::frontend::flight::sync::{SeafowlDataSyncManager, SequenceNumber};
    use crate::frontend::flight::SEAFOWL_SYNC_DATA_UD_FLAG;
    use arrow::{array::RecordBatch, util::data_gen::create_random_batch};
    use arrow_schema::{DataType, Field, Schema};
    use rand::Rng;
    use rstest::rstest;
    use std::collections::VecDeque;
    use std::sync::Arc;

    // Create a randomly sized vector of random record batches with
    // a pre-defined schema
    fn random_batches() -> Vec<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int32, true),
            Field::new("c2", DataType::Utf8, true),
            Field::new(SEAFOWL_SYNC_DATA_UD_FLAG, DataType::Boolean, false),
        ]));

        // Generate a random length between 1 and 3
        let len: usize = rand::thread_rng().gen_range(1..=3);

        (0..len)
            .map(|_| create_random_batch(schema.clone(), 10, 0.2, 0.8).unwrap())
            .collect()
    }

    fn assert_sequences(
        sync_mgr: &mut SeafowlDataSyncManager,
        origin: &String,
        memory_sequence: Option<u64>,
        durable_sequence: Option<u64>,
    ) {
        assert_eq!(
            sync_mgr.stored_sequences(origin),
            (memory_sequence, durable_sequence),
            "Unexpected memory/durable sequence; \nseqs {:?}",
            sync_mgr.seqs,
        )
    }

    const FLUSH: (&str, i64) = ("__flush", -1);

    #[rstest]
    #[case::basic(
        &[("table_1", 1), ("table_2", 2), ("table_1", 3), FLUSH, FLUSH],
        vec![Some(1), Some(3)]
    )]
    #[case::doc_example(
        &[("table_1", 1), ("table_2", 1), ("table_3", 2), ("table_1", 3), ("table_2", 3), FLUSH, FLUSH, FLUSH],
        vec![None, Some(1), Some(3)]
    )]
    #[case::long_sequence(
        &[("table_1", 1), ("table_1", 1), ("table_1", 1), ("table_1", 1), ("table_2", 1),
            ("table_2", 2), ("table_2", 2), ("table_2", 2), ("table_3", 2), ("table_3", 3),
            ("table_3", 3), ("table_1", 4), ("table_3", 4), ("table_1", 4), ("table_3", 4),
            FLUSH, FLUSH, FLUSH],
        vec![None, Some(1), Some(4)]
    )]
    #[case::long_sequence_mid_flush(
        &[("table_1", 1), ("table_1", 1), ("table_1", 1), FLUSH, ("table_1", 1), ("table_2", 1),
            ("table_2", 2), ("table_2", 2), FLUSH, ("table_2", 2), ("table_3", 2), FLUSH, ("table_3", 3),
            FLUSH, ("table_3", 3), ("table_1", 4), ("table_3", 4), ("table_1", 4), FLUSH, ("table_3", 4),
            FLUSH, FLUSH],
        // Reasoning for the observed durable sequences:
        // - seq 1 not seen last sync
        // - seq 1 seen last sync, but it is in a unflushed table (2)
        // - seq 1 done, seq 2 seen last, but it is in a unflushed table (3)
        // - seq 2 done
        // - seq 3 done, seq 4 partial
        // - seq 4 seen last sync, but it is in a unflushed table (1)
        // - seq 4 done
        vec![None, None, Some(1), Some(2), Some(3), Some(3), Some(4)]
    )]
    #[tokio::test]
    async fn test_sync_flush(
        #[case] table_sequence: &[(&str, i64)],
        #[case] durable_sequences: Vec<Option<u64>>,
    ) {
        let ctx = Arc::new(in_memory_context().await);
        let mut sync_mgr = SeafowlDataSyncManager::new(ctx.clone());

        let origin = "origin".to_string();

        let mut durable_sequences = VecDeque::from(durable_sequences);
        let mut mem_seq = None;
        let mut dur_seq = None;
        // Enqueue all syncs first, checking the memory sequence in-between
        for (sync_no, (table_name, sequence)) in table_sequence.iter().enumerate() {
            if (*table_name, *sequence) == FLUSH {
                // Flush and assert on the next expected durable sequence
                sync_mgr.flush_syncs().await.unwrap();

                dur_seq = durable_sequences.pop_front().unwrap();
                assert_sequences(&mut sync_mgr, &origin, mem_seq, dur_seq);
                continue;
            }

            let log_store = ctx.internal_object_store.get_log_store(table_name);

            // Determine whether this is the last sync of the sequence, i.e. are there no upcoming
            // syncs with the same sequence number?
            let last = !table_sequence
                .iter()
                .skip(sync_no + 1)
                .any(|&(_, next_sequence)| *sequence == next_sequence);

            sync_mgr
                .enqueue_sync(
                    log_store,
                    *sequence as SequenceNumber,
                    origin.clone(),
                    vec!["c1".to_string()],
                    last,
                    random_batches(),
                )
                .await
                .unwrap();

            // If this is the last sync in the sequence then it should be reported as in-memory
            if last {
                mem_seq = Some(*sequence as SequenceNumber);
            }

            assert_sequences(&mut sync_mgr, &origin, mem_seq, dur_seq);
        }

        // Ensure everything has been flushed from memory
        assert!(sync_mgr.seqs.is_empty());
        assert!(sync_mgr.syncs.is_empty());
        assert_eq!(sync_mgr.size, 0);
    }
}
