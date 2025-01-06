use arrow::array::RecordBatch;
use arrow_schema::SchemaBuilder;
use clade::sync::ColumnRole;
use deltalake::kernel::{Action, Schema};
use deltalake::operations::create::CreateBuilder;
use deltalake::protocol::{DeltaOperation, SaveMode};
use deltalake::DeltaTable;
use futures::stream;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::table::StaticTable;
use iceberg::TableIdent;
use iceberg_datafusion::IcebergTableProvider;
use indexmap::IndexMap;
use itertools::Itertools;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tracing::{debug, info};
use url::Url;
use uuid::Uuid;

use crate::catalog::DEFAULT_SCHEMA;
use crate::context::delta::plan_to_delta_adds;

use crate::context::iceberg::record_batches_to_iceberg;
use crate::context::SeafowlContext;
use crate::sync::metrics::SyncWriterMetrics;
use crate::sync::planner::SeafowlSyncPlanner;
use crate::sync::schema::SyncSchema;
use crate::sync::utils::{get_size_and_rows, squash_batches};
use crate::sync::{
    IcebergSyncTarget, Origin, SequenceNumber, SyncCommitInfo, SyncError, SyncResult,
};

use super::LakehouseSyncTarget;

// Denotes the last sequence number that was fully committed
pub const SYNC_COMMIT_INFO: &str = "sync_commit_info";
const MAX_ROWS_PER_SYNC: usize = 100_000;

// A handler for caching, coalescing and flushing table syncs received via
// the Arrow Flight `do_put` calls.
//
// Each `DataSyncCommand` that accompanies a record batch carries information on
// the origin of change, sequence number, primary keys and whether this is the
// last message in the sequence.
//
// It uses a greedy table-based algorithm for flushing: once the criteria is met
// it will go through tables ordered by the oldest sync, flushing all pending syncs
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
// is entirely persisted to storage so it will be recorded as the new durable
// sequence number. Note that while sequence 3 is also now completely flushed,
// it isn't durable, since there is a preceding sequence (2) that is still in memory.
//
// Finally, once table_3 is flushed `SeafowlDataSyncWriter` will advance the
// durable sequence up to 3, since both it and 2 have now been completely persisted.
pub struct SeafowlDataSyncWriter {
    context: Arc<SeafowlContext>,
    // An indexed-queue of transactions sorted by insertion order
    txs: IndexMap<Uuid, Transaction>,
    // An indexed queue of table URL => pending syncs with actual batches to
    // upsert/delete sorted by insertion order
    syncs: IndexMap<String, DataSyncCollection>,
    // Total size of all batches in memory currently
    size: usize,
    // Map of known memory sequence numbers per origin
    origin_memory: HashMap<Origin, SequenceNumber>,
    // Map of known durable sequence numbers per origin
    origin_durable: HashMap<Origin, SequenceNumber>,
    // Keep track of various metrics for observability
    metrics: SyncWriterMetrics,
}

// Besides the two conditions mentioned below, another implicit condition for marking a transaction
// durable is that all preceding txs in the queue are also durable.
#[derive(Debug, Clone)]
pub(super) struct Transaction {
    // The origin of this transaction
    origin: Origin,
    // A (potentially yet unknown) sequence number; once this is known the transaction has been
    // received in full, which is condition #1 for marking it as durable.
    sequence: Option<SequenceNumber>,
    // Locations that have pending flushes for this transaction; having flushed all locations for a
    // transaction is condition #2 for marking it as durable.
    locations: HashSet<String>,
}

// An entry storing all pending in-memory data to replicate to a single table location,
// potentially resulting from multiple `do_put` calls across disparate sequences and origins.
#[derive(Debug)]
pub(super) struct DataSyncCollection {
    // Total in-memory size of all the batches in all the items for this table
    size: usize,
    // Total in-memory rows of all the batches in all the items for this table
    rows: usize,
    // Unix epoch of the first sync command in this collection
    insertion_time: u64,
    // Table log store
    sync_target: LakehouseSyncTarget,
    // Collection of batches to replicate
    pub(super) syncs: Vec<DataSyncItem>,
}

// An object corresponding to a single `do_put` call.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct DataSyncItem {
    // Flag denoting whether the batches have been physically squashed yet or not. If true there is
    // a single record batch in the `data` field, and this is strictly the case during flushing.
    pub(super) is_squashed: bool,
    // The (internal) id of the transaction that this change belongs to; for now it corresponds to
    // a random v4 UUID generated for the first message received in this transaction.
    pub(super) tx_ids: Vec<Uuid>,
    // Old and new primary keys, changed and value columns
    pub(super) sync_schema: SyncSchema,
    // Record batch to replicate
    pub(super) data: Vec<RecordBatch>,
}

impl SeafowlDataSyncWriter {
    pub fn new(context: Arc<SeafowlContext>) -> Self {
        Self {
            context,
            txs: Default::default(),
            syncs: Default::default(),
            size: 0,
            origin_memory: Default::default(),
            origin_durable: Default::default(),
            metrics: Default::default(),
        }
    }

    // Extract the latest memory sequence number for a given table location.
    pub fn stored_sequences(
        &self,
        origin: &Origin,
    ) -> (Option<SequenceNumber>, Option<SequenceNumber>) {
        (
            self.origin_memory.get(origin).cloned(),
            self.origin_durable.get(origin).cloned(),
        )
    }

    // Store the pending data in memory and flush if the required criteria are met.
    pub fn enqueue_sync(
        &mut self,
        sync_target: LakehouseSyncTarget,
        sequence_number: Option<SequenceNumber>,
        origin: Origin,
        sync_schema: SyncSchema,
        batches: Vec<RecordBatch>,
    ) -> SyncResult<()> {
        let url = match sync_target {
            LakehouseSyncTarget::Delta(ref log_store) => log_store.root_uri(),
            LakehouseSyncTarget::Iceberg(ref iceberg_sync_target) => {
                iceberg_sync_target.url.clone()
            }
        };

        let (sync_size, sync_rows) = get_size_and_rows(&batches);

        // Upsert a sequence entry for this origin and sequence number
        let tx_id = if let Some((tx_id, tx)) = self.txs.last_mut()
            && tx.sequence.is_none()
        {
            if origin != tx.origin {
                return Err(SyncError::InvalidMessage {
                    reason: format!(
                        "Transaction from a new origin ({origin}) started without finishing the transaction from the previous one ({})",
                        tx.origin
                    )
                });
            }

            // Merge the information for the pending transaction
            if let Some(seq) = sequence_number {
                // A sequence number was provided, denoting the end of the transaction
                debug!("Received sequence number {seq} for {origin} of the pending tx ({tx_id})");
                tx.sequence = sequence_number;
            }

            if sync_rows > 0 {
                debug!("Adding {url} as sync destination for {origin} in the pending tx ({tx_id})");
                tx.locations.insert(url.clone());
            }

            *tx_id
        } else {
            // The previous message contains a definite sequence number (or there are no entries at
            // all), so this means this message belongs to a new transaction
            if sync_rows == 0
                && let Some(seq) = sequence_number
            {
                // We received an empty transaction, this isn't supported
                return Err(SyncError::InvalidMessage {
                    reason: format!(
                        "Received empty transaction for origin {origin} with sequence number {seq}",
                    )
                });
            }

            let tx_id = Uuid::new_v4();
            debug!("Adding {url} as sync destination for {origin} in a new tx {tx_id}");
            self.txs.insert(
                tx_id,
                Transaction {
                    origin: origin.clone(),
                    sequence: sequence_number,
                    locations: HashSet::from([url.clone()]),
                },
            );
            tx_id
        };

        if sync_rows > 0 {
            self.metrics.request_bytes.increment(sync_size as u64);
            self.metrics.request_rows.increment(sync_rows as u64);

            self.syncs
                .entry(url.clone())
                .and_modify(|entry| {
                    let prev_item = entry.syncs.last_mut().unwrap();
                    let (_, prev_rows) = get_size_and_rows(&prev_item.data);
                    if prev_item.sync_schema.is_compatible_with(&sync_schema)
                        && prev_rows + sync_rows <= MAX_ROWS_PER_SYNC
                    {
                        debug!("{}: Appending batch with schema {} to existing sync collection with {} row(s), schema {}", &url, sync_schema, prev_rows, prev_item.sync_schema);
                        // Just append to the last item if the sync schema matches and the row count
                        // is smaller than a predefined value
                        prev_item.is_squashed = false;
                        prev_item.tx_ids.push(tx_id);
                        prev_item.data.extend(batches.clone());
                    } else {
                        debug!("{}: Adding new sync item for batch with schema {} (incompatible schema or too many rows in old item)", &url, sync_schema);
                        entry.syncs.push(DataSyncItem {
                            is_squashed: false,
                            tx_ids: vec![tx_id],
                            sync_schema: sync_schema.clone(),
                            data: batches.clone(),
                        });
                    }
                    entry.size += sync_size;
                    entry.rows += sync_rows;
                })
                .or_insert(DataSyncCollection {
                    size: sync_size,
                    rows: sync_rows,
                    insertion_time: now(),
                    sync_target,
                    syncs: vec![DataSyncItem {
                        is_squashed: false,
                        tx_ids: vec![tx_id],
                        sync_schema: sync_schema.clone(),
                        data: batches,
                    }],
                });

            // Update the total size and metrics
            self.size += sync_size;
            self.metrics.in_memory_bytes.increment(sync_size as f64);
            self.metrics.in_memory_rows.increment(sync_rows as f64);
            self.metrics.in_memory_oldest.set(
                self.syncs
                    .first()
                    .map(|(_, v)| v.insertion_time as f64)
                    .unwrap_or(0.0),
            );
        }

        // Flag the sequence as volatile persisted for this origin if it is the last message of the
        // transaction
        if let Some(seq) = sequence_number {
            self.metrics.sequence_memory(&origin, seq);
            self.origin_memory.insert(origin, seq);
        }

        Ok(())
    }

    async fn create_table(
        &self,
        sync_target: LakehouseSyncTarget,
        sync_schema: &SyncSchema,
    ) -> SyncResult<()> {
        // Get the actual table schema by removing the OldPk and Changed column roles from the schema.
        let mut builder = SchemaBuilder::new();
        sync_schema.columns().iter().for_each(|col| {
            if matches!(col.role(), ColumnRole::NewPk | ColumnRole::Value) {
                let field = col.field().as_ref().clone().with_name(col.name());
                builder.push(field);
            }
        });
        match sync_target {
            LakehouseSyncTarget::Delta(log_store) => {
                let delta_schema = Schema::try_from(&builder.finish())?;

                CreateBuilder::new()
                    .with_log_store(log_store)
                    .with_columns(delta_schema.fields().cloned())
                    .with_comment(format!(
                        "Synced by Seafowl {}",
                        env!("CARGO_PKG_VERSION")
                    ))
                    .await?;
                Ok(())
            }
            LakehouseSyncTarget::Iceberg(IcebergSyncTarget { file_io, url, .. }) => {
                let mut table_location = Url::parse(&url).unwrap();

                // Turn metadata location into table location
                // E.g. s3://a/b/metadata/v3.metadata.json -> s3://a/b
                match table_location.path_segments_mut() {
                    Ok(mut segments) => segments.pop_if_empty().pop(),
                    Err(_) => {
                        return Err(SyncError::InvalidMessage {
                            reason: format!(
                                "Could not compute metadata directory from URL: {}",
                                url
                            ),
                        })
                    }
                };
                match table_location.path_segments_mut() {
                    Ok(mut segments) => segments.pop_if_empty().pop(),
                    Err(_) => {
                        return Err(SyncError::InvalidMessage {
                            reason: format!(
                                "Could not compute table directory from URL: {}",
                                url
                            ),
                        })
                    }
                };

                match record_batches_to_iceberg(
                    stream::empty(),
                    builder.finish().into(),
                    &file_io,
                    table_location.as_str(),
                )
                .await
                {
                    Err(e) => Err(SyncError::InvalidMessage {
                        reason: e.to_string(),
                    }),
                    Ok(_) => Ok(()),
                }
            }
        }
    }

    pub async fn flush(&mut self) -> SyncResult<()> {
        while let Some(url) = self.flush_ready()? {
            self.flush_syncs(url).await?;
        }

        self.metrics.in_memory_oldest.set(
            self.syncs
                .first()
                .map(|(_, v)| v.insertion_time as f64)
                .unwrap_or(0.0),
        );

        Ok(())
    }

    // Criteria for flushing a cached entry to object storage.
    //
    // First flush any records that are explicitly beyond the configured max
    // lag, followed by further entries if we're still above max cache size.
    fn flush_ready(&mut self) -> SyncResult<Option<String>> {
        if let Some((url, sync)) = self.syncs.first()
            && now() - sync.insertion_time
                >= self.context.config.misc.sync_conf.max_replication_lag_s
        {
            // First flush any changes that are past the configured max duration
            info!(
                "Flushing due to lag-based criteria ({}): {url}",
                sync.insertion_time
            );
            return Ok(Some(url.clone()));
        }

        if self.size >= self.context.config.misc.sync_conf.max_in_memory_bytes {
            // Or if we're over the size limit try squashing the largest entry
            if let Some((url, _)) = self.syncs.iter().max_by_key(|(_, entry)| entry.size)
            {
                let url = url.clone();
                self.physical_squashing(&url)?;

                // And if we're still above the threshold flush
                if self.size >= self.context.config.misc.sync_conf.max_in_memory_bytes {
                    info!("Flushing due to size-based criteria ({}): {url}", self.size);
                    return Ok(Some(url));
                }
            }
        }

        // TODO: Given that we have logical squashing now, this criteria can be removed, i.e. by
        // recursively squashing 100 syncs until we can apply the compacted batches to the base scan
        if let Some((url, entry)) = self.syncs.iter().find(|(_, entry)| {
            entry.syncs.len() >= self.context.config.misc.sync_conf.max_syncs_per_url
        }) {
            // Otherwise if there are pending syncs with more than a predefined number of calls
            // waiting to be flushed flush them.
            // This is a guard against hitting a stack overflow when applying syncs, since this
            // results in deeply nested plan trees that are known to be problematic for now:
            // - https://github.com/apache/datafusion/issues/9373
            // - https://github.com/apache/datafusion/issues/9375
            info!(
                "Flushing due to max-sync messages criteria ({}): {url}",
                entry.syncs.len()
            );
            return Ok(Some(url.clone()));
        }

        Ok(None)
    }

    // Flush the table with the provided url
    async fn flush_syncs(&mut self, url: String) -> SyncResult<()> {
        self.physical_squashing(&url)?;
        let entry = match self.syncs.get(&url) {
            Some(table_syncs) => table_syncs,
            None => {
                info!("No pending syncs to flush");
                return Ok(());
            }
        };

        info!(
            "Flushing {} bytes in {} rows across {} sync items for url {url}",
            entry.size,
            entry.rows,
            entry.syncs.len()
        );

        let start = Instant::now();
        let insertion_time = entry.insertion_time;
        let rows = entry.rows;
        let size = entry.size;
        let url = url.clone();
        match &entry.sync_target {
            LakehouseSyncTarget::Delta(log_store) => {
                // If there's no delta table at this location yet create one first.
                if !log_store.is_delta_table_location().await? {
                    debug!("Creating new Delta table at location: {url}");
                    self.create_table(
                        entry.sync_target.clone(),
                        &entry.syncs.first().unwrap().sync_schema,
                    )
                    .await?;
                }

                let mut table = DeltaTable::new(log_store.clone(), Default::default());
                table.load().await?;

                let last_sync_commit = self.table_sequence(&table).await?;
                let new_sync_commit = self.commit_info(&last_sync_commit, &entry.syncs);

                if entry.syncs.is_empty() {
                    // TODO: Update metrics
                    self.remove_sync(&url);
                    return Ok(());
                }

                let planner = SeafowlSyncPlanner::new(self.context.clone());
                let planning_start = Instant::now();
                let (plan, removes) =
                    planner.plan_delta_syncs(&entry.syncs, &table).await?;
                let planning_time = planning_start.elapsed().as_millis();
                info!("Flush plan generated in {planning_time} ms");
                self.metrics.planning_time.record(planning_time as f64);

                // To exploit fast data upload to local FS, i.e. simply move the partition files
                // once written to the disk, try to infer whether the location is a local dir
                let local_data_dir = if url.starts_with("file://") {
                    Some(log_store.root_uri())
                } else {
                    None
                };

                // Dump the batches to the object store
                let adds = plan_to_delta_adds(
                    &self.context.inner.state(),
                    &plan,
                    log_store.object_store(),
                    local_data_dir,
                    self.context.config.misc.max_partition_size,
                )
                .await?;

                let mut actions: Vec<Action> =
                    adds.into_iter().map(Action::Add).collect();
                info!(
                    "Removing {} out of {} files from state and adding {} new one(s)",
                    removes.len(),
                    table.get_files_count(),
                    actions.len(),
                );
                actions.extend(removes);
                debug!("Actions to commit:\n{actions:?}");

                // Append a special `CommitInfo` action to record latest durable sequence number
                // tied to the commit from this origin if any.
                if let Some(ref sync_commit) = new_sync_commit {
                    let info = HashMap::from([(
                        SYNC_COMMIT_INFO.to_string(),
                        serde_json::to_value(sync_commit)?,
                    )]);
                    let commit_info = Action::commit_info(info);
                    actions.push(commit_info);
                }

                let op = DeltaOperation::Write {
                    mode: SaveMode::Append,
                    partition_by: None,
                    predicate: None,
                };
                self.context.commit_delta_table(actions, &table, op).await?;
                debug!(
                    "Committed data sync up to {new_sync_commit:?} for location {url}"
                );
            }
            LakehouseSyncTarget::Iceberg(IcebergSyncTarget { file_io, url, .. }) => {
                if !file_io.exists(url).await.unwrap() {
                    self.create_table(
                        entry.sync_target.clone(),
                        &entry.syncs.first().unwrap().sync_schema,
                    )
                    .await?;
                }
                let iceberg_table = StaticTable::from_metadata_file(
                    url,
                    TableIdent::from_strs(vec![DEFAULT_SCHEMA, "dummy_name"]).unwrap(),
                    file_io.clone(),
                )
                .await?
                .into_table();
                let table_provider =
                    IcebergTableProvider::try_new_from_table(iceberg_table).await?;
                let table = table_provider.table();
                let schema =
                    schema_to_arrow_schema(table.metadata().current_schema()).unwrap();
                let planner = SeafowlSyncPlanner::new(self.context.clone());
                let plan = planner
                    .plan_iceberg_syncs(&entry.syncs, Arc::new(schema))
                    .await?;
                self.context
                    .plan_to_iceberg_table(file_io, url, &plan)
                    .await?;
            }
        };

        // We've flushed all the presently accumulated batches for this location.
        // Modify our syncs and sequences maps to reflect this.
        let tx_ids = entry
            .syncs
            .iter()
            .flat_map(|sync| sync.tx_ids.clone())
            .unique()
            .collect();
        self.remove_tx_locations(&url, tx_ids);
        self.remove_sync(&url);
        self.advance_durable();

        // Record flush metrics
        let flush_duration = start.elapsed().as_millis();
        self.metrics.flush_time.record(flush_duration as f64);
        self.metrics.flush_bytes.increment(size as u64);
        self.metrics.flush_rows.increment(rows as u64);
        self.metrics.flush_last.set(now() as f64);
        self.metrics
            .flush_lag
            .record((now() - insertion_time) as f64);
        self.origin_durable.iter().for_each(|(origin, seq)| {
            self.metrics.sequence_durable(origin, *seq);
        });

        Ok(())
    }

    // Perform physical squashing of change batches for a particular table
    fn physical_squashing(&mut self, url: &String) -> SyncResult<()> {
        if let Some(table_syncs) = self.syncs.get_mut(url) {
            let old_size = self.size;
            let start = Instant::now();
            // Squash the batches and measure the time it took and the reduction in rows/size
            // TODO: parallelize this
            for item in table_syncs.syncs.iter_mut().filter(|i| !i.is_squashed) {
                let (old_size, old_rows) = get_size_and_rows(&item.data);

                let start = Instant::now();
                let batch = squash_batches(&mut item.sync_schema, &item.data)?;
                let duration = start.elapsed().as_millis();

                // Get new size and row count
                let size = batch.get_array_memory_size();
                let rows = batch.num_rows();
                let size_delta = old_size as i64 - size as i64;
                let rows_delta = old_rows as i64 - rows as i64;
                debug!(
                    "Physical squashing removed {size_delta} rows in {duration} ms for batches with schema \n{}",
                    item.sync_schema,
                );

                self.metrics.squash_time.record(duration as f64);
                self.metrics
                    .squashed_bytes
                    .increment((old_size.saturating_sub(size)) as u64);
                self.metrics
                    .squashed_rows
                    .increment(old_rows.saturating_sub(rows) as u64);
                self.metrics.in_memory_bytes.decrement(size_delta as f64);
                self.metrics.in_memory_rows.increment(rows_delta as f64);

                self.size -= old_size;
                self.size += size;
                table_syncs.size -= old_size;
                table_syncs.size += size;
                table_syncs.rows -= old_rows;
                table_syncs.rows += rows;

                item.is_squashed = true;
                item.data = vec![batch];
            }

            let duration = start.elapsed().as_millis();
            info!(
                "Squashed {} bytes in {duration} ms for {url}",
                old_size - self.size,
            );
        };

        Ok(())
    }

    // Inspect the table logs to find out what is the latest origin/sequence number committed.
    // Note that the origin/sequence denote only the last _fully_ flushed, and in general there
    // may be further commits from subsequent origin/sequences, as denoted by the
    // `SYNC_COMMIT_NEW_TRANSACTION` flag, where the sequence number in particular may not be yet known
    // exactly.
    async fn table_sequence(
        &self,
        table: &DeltaTable,
    ) -> SyncResult<Option<SyncCommitInfo>> {
        let commit_infos = table.history(Some(1)).await?;
        Ok(
            match commit_infos
                .last()
                .expect("Table has non-zero commits")
                .info
                .get(SYNC_COMMIT_INFO)
            {
                Some(val) => serde_json::from_value(val.clone())?,
                _ => None,
            },
        )
    }

    // Deduce the effective origin/sequence up to which _this_ table will be fully durable
    // after the pending flush.
    fn commit_info(
        &self,
        last_sync_commit: &Option<SyncCommitInfo>,
        syncs: &[DataSyncItem],
    ) -> Option<SyncCommitInfo> {
        let new_tx = syncs
            .last()
            .map(|sync| self.txs[sync.tx_ids.last().unwrap()].sequence.is_none())
            .unwrap_or_default();

        syncs
            .iter()
            .rev()
            .find_map(|sync| {
                if let Some(Transaction {
                    origin,
                    sequence: Some(seq),
                    ..
                }) = self.txs.get(sync.tx_ids.last().unwrap())
                {
                    Some(SyncCommitInfo::new(origin, *seq).with_new_tx(new_tx))
                } else {
                    None
                }
            })
            .or(last_sync_commit.clone().map(|sync_commit| {
                // Inherit the previous full commit identifiers, but update the fact that we're now
                // starting a new transaction too.
                sync_commit.with_new_tx(new_tx)
            }))
    }

    // Remove the pending location from a sequence for all syncs in the collection
    fn remove_tx_locations(&mut self, url: &String, tx_ids: Vec<Uuid>) {
        for tx_id in tx_ids {
            // Remove the pending location for this origin/sequence
            if let Some(tx) = self.txs.get_mut(&tx_id) {
                tx.locations.remove(url);
            }
        }
    }

    // Remove the in-memory sync collection for the provided location, and update the size
    fn remove_sync(&mut self, url: &String) {
        if let Some(sync) = self.syncs.shift_remove(url) {
            self.size -= sync.size;
            self.metrics.in_memory_bytes.decrement(sync.size as f64);
            self.metrics.in_memory_rows.decrement(sync.rows as f64);
        }
    }

    // Iterate through all origin-sequences in the insertion order and:
    //    - mark as durable all flushed and final sequences up to the first one that is not
    //    - remove the durable sequences from the map
    fn advance_durable(&mut self) {
        let mut durable_txs = HashSet::new();

        // Iterate through all origins in order of insertion
        for (tx_id, tx) in &mut self.txs {
            if let Some(seq) = tx.sequence
                && tx.locations.is_empty()
            {
                // We've seen the last sync for this transaction, and there are no more locations
                // with pending flushes; it's safe to mark the sequence as durable for this origin.
                self.origin_durable.insert(tx.origin.clone(), seq);
                durable_txs.insert(*tx_id);
                debug!("Set new durable sequence {seq} for {}", tx.origin);
            } else {
                // We either haven't seen the end of the transaction or some locations still have
                // pending flushes; we can't advance the durable sequences anymore.
                break;
            }
        }

        self.txs.retain(|tx_id, _| !durable_txs.contains(tx_id));
    }
}

pub(super) fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs()
}

#[cfg(test)]
mod tests {
    use crate::context::test_utils::in_memory_context;
    use crate::sync::schema::{arrow_to_sync_schema, SyncSchema};
    use crate::sync::writer::{SeafowlDataSyncWriter, SequenceNumber};
    use crate::sync::{LakehouseSyncTarget, SyncResult};
    use arrow::array::{
        BooleanArray, Decimal128Array, Float32Array, Int32Array, StringArray,
    };
    use arrow::{array::RecordBatch, util::data_gen::create_random_batch};
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use datafusion_common::assert_batches_eq;
    use rand::Rng;
    use rstest::rstest;
    use std::collections::HashMap;
    use std::ops::Range;
    use std::sync::Arc;
    use uuid::Uuid;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("old_pk_c1", DataType::Int32, true),
            Field::new("new_pk_c1", DataType::Int32, true),
            Field::new("value_c2", DataType::Float32, true),
        ]))
    }

    // Create a randomly sized vector of random record batches with
    // a pre-defined schema
    fn random_batches(schema: SchemaRef) -> Vec<RecordBatch> {
        // Generate a random length between 1 and 3
        let len: usize = rand::thread_rng().gen_range(1..=3);

        (0..len)
            .map(|_| create_random_batch(schema.clone(), 10, 0.2, 0.8).unwrap())
            .collect()
    }

    const T1: &str = "table_1";
    const T2: &str = "table_2";
    const T3: &str = "table_3";
    static A: &str = "origin-A"; // first origin
    static B: &str = "origin-B"; // second origin
    static FLUSH: (&str, &str, Option<i64>) = ("__flush", "", None);

    #[rstest]
    #[case::basic(
        &[(T1, A, Some(100)), (T2, A, Some(200)), (T1, A, Some(300)), FLUSH, FLUSH],
        vec![vec![Some(100), Some(300)]]
    )]
    #[case::basic_2_origins(
        &[(T1, A, Some(100)), (T2, B, Some(200)), (T1, A, Some(300)), FLUSH, FLUSH],
        vec![
            vec![Some(100), Some(300)],
            vec![None, Some(200)],
        ]
    )]
    #[should_panic(
        expected = "Transaction from a new origin (origin-B) started without finishing the transaction from the previous one (origin-A)"
    )]
    #[case::incomplete_transaction(
        &[(T1, A, None), (T2, B, Some(100)), FLUSH],
        vec![],
    )]
    #[case::race_2_origins(
        &[(T1, A, None), (T2, A, Some(100)), (T1, B, Some(200)), FLUSH, FLUSH],
        vec![
            vec![None, Some(100)],
            vec![None, Some(200)],
        ]
    )]
    #[case::doc_example(
        &[(T1, A, None), (T2, A, Some(1)), (T3, A, Some(2)), (T1, A, None), (T2, A, Some(3)), FLUSH, FLUSH, FLUSH],
        vec![vec![None, Some(1), Some(3)]]
    )]
    #[case::staircase(
        &[(T1, A, None), (T2, A, None), (T3, A, Some(1)), (T1, A, None), (T2, A, None), (T3, A, Some(2)),
            FLUSH, FLUSH, FLUSH],
        vec![vec![None, None, Some(2)]]
    )]
    #[case::staircase_2_origins(
        &[(T1, A, None), (T2, A, Some(1)), (T3, B, Some(1001)), (T1, B, Some(1002)), (T2, A, None), (T3, A, Some(2)),
            FLUSH, FLUSH, FLUSH],
        vec![
            vec![None, Some(1), Some(2)],
            vec![None, None, Some(1002)],
        ]
        )]
    #[case::long_sequence(
        &[(T1, A, None), (T1, A, None), (T1, A, None), (T1, A, None), (T2, A, Some(1)),
            (T2, A, None), (T2, A, None), (T2, A, None), (T3, A, Some(2)), (T3, A, None),
            (T3, A, Some(3)), (T1, A, None), (T3, A, None), (T1, A, None), (T3, A, Some(4)),
            FLUSH, FLUSH, FLUSH],
        vec![vec![None, Some(1), Some(4)]]
    )]
    #[case::long_sequence_mid_flush(
        &[(T1, A, None), (T1, A, None), (T1, A, None), FLUSH, (T1, A, None), (T2, A, Some(1)),
            (T2, A, None), (T2, A, None), FLUSH, (T2, A, None), (T3, A, Some(2)), FLUSH, (T3, A, None),
            FLUSH, (T3, A, Some(3)), (T1, A, None), (T3, A,None), (T1, A, None), FLUSH, (T3, A, Some(4)),
            FLUSH, FLUSH],
        // Reasoning for the observed durable sequences:
        // - seq 1 not seen last sync
        // - seq 1 seen last sync, but it is in an unflushed table (t2)
        // - seq 1 done, seq 2 seen last, but it is in an unflushed table (t3)
        // - seq 2 done
        // - seq 3 done, seq 4 partial
        // - seq 4 seen last sync, but it is in an unflushed table (t1)
        // - seq 4 done
        vec![vec![None, None, Some(1), Some(2), Some(3), Some(3), Some(4)]]
    )]
    #[tokio::test]
    async fn test_sync_flush(
        #[case] table_sequence: &[(&str, &str, Option<i64>)],
        #[case] mut durable_sequences: Vec<Vec<Option<u64>>>,
    ) {
        use crate::sync::LakehouseSyncTarget;

        let ctx = Arc::new(in_memory_context().await);
        let mut sync_mgr = SeafowlDataSyncWriter::new(ctx.clone());
        let arrow_schema = test_schema();
        let sync_schema = arrow_to_sync_schema(arrow_schema.clone()).unwrap();

        let mut mem_seq = HashMap::from([(A.to_string(), None), (B.to_string(), None)]);
        let mut dur_seq = HashMap::from([(A.to_string(), None), (B.to_string(), None)]);

        // Start enqueueing syncs, flushing them and checking the memory sequence in-between
        for (table_name, origin, sequence) in table_sequence {
            if (*table_name, *origin, *sequence) == FLUSH {
                // Flush and assert on the next expected durable sequence
                let url = sync_mgr.syncs.first().unwrap().0.clone();
                sync_mgr.flush_syncs(url).await.unwrap();

                for (o, durs) in durable_sequences.iter_mut().enumerate() {
                    let origin = if o == 0 { A.to_string() } else { B.to_string() };
                    // Update expected durable sequences for these origins
                    dur_seq.insert(origin.clone(), durs.remove(0));

                    assert_eq!(
                        sync_mgr.stored_sequences(&origin),
                        (mem_seq[&origin], dur_seq[&origin]),
                        "Unexpected flush memory/durable sequence; \ntxs {:?}",
                        sync_mgr.txs,
                    );
                }
                continue;
            }

            let log_store = ctx
                .get_internal_object_store()
                .unwrap()
                .get_log_store(table_name);
            let origin: String = (*origin).to_owned();

            sync_mgr
                .enqueue_sync(
                    LakehouseSyncTarget::Delta(log_store),
                    sequence.map(|seq| seq as SequenceNumber),
                    origin.clone(),
                    sync_schema.clone(),
                    random_batches(arrow_schema.clone()),
                )
                .unwrap();

            // If this is the last sync in the sequence then it should be reported as in-memory
            if let Some(seq) = sequence {
                mem_seq.insert(origin.clone(), Some(*seq as SequenceNumber));
            }

            assert_eq!(
                sync_mgr.stored_sequences(&origin),
                (mem_seq[&origin], dur_seq[&origin]),
                "Unexpected enqueue memory/durable sequence; \ntxs {:?}",
                sync_mgr.txs,
            );
        }

        // Ensure everything has been flushed from memory
        assert!(sync_mgr.txs.is_empty());
        assert!(sync_mgr.syncs.is_empty());
        assert_eq!(sync_mgr.size, 0);
    }

    #[tokio::test]
    async fn test_empty_sync() -> SyncResult<()> {
        let ctx = Arc::new(in_memory_context().await);
        let mut sync_mgr = SeafowlDataSyncWriter::new(ctx.clone());
        let arrow_schema = test_schema();
        let sync_schema = arrow_to_sync_schema(arrow_schema.clone())?;

        // Enqueue all syncs
        let log_store = ctx.get_internal_object_store()?.get_log_store("test_table");
        let sync_target = LakehouseSyncTarget::Delta(log_store);

        // Add first non-empty sync
        sync_mgr.enqueue_sync(
            sync_target.clone(),
            None,
            A.to_string(),
            sync_schema.clone(),
            random_batches(arrow_schema.clone()),
        )?;

        // Add empty sync with an explicit sequence number denoting the end of the transaction
        sync_mgr.enqueue_sync(
            sync_target.clone(),
            Some(100),
            A.to_string(),
            SyncSchema::empty(),
            vec![],
        )?;

        // Adding a new empty sync with `Some` sequence number amounts to an empty transaction which
        // isn't supported
        let err = sync_mgr
            .enqueue_sync(
                sync_target,
                Some(200),
                A.to_string(),
                SyncSchema::empty(),
                vec![],
            )
            .unwrap_err();
        assert!(err.to_string().contains(
            "Received empty transaction for origin origin-A with sequence number 200"
        ),);

        // Ensure the tx is marked as durable after flushing
        let url = sync_mgr.syncs.first().unwrap().0.clone();
        sync_mgr.flush_syncs(url).await?;
        assert_eq!(
            sync_mgr.stored_sequences(&A.to_string()),
            (Some(100), Some(100))
        );

        Ok(())
    }

    #[rstest]
    #[case::insert_basic(
        vec![None, None],
        vec![Some(0..500), Some(500..1000)],
        1000,
        0,
        999,
    )]
    // #[case::insert_idempotent(
    //     vec![None, None, None, None],
    //     vec![Some(0..500), Some(0..500), Some(500..1000), Some(500..1000)],
    //     1000,
    //     0,
    //     999,
    // )]
    #[case::insert_update(
        vec![None, Some(0..300), None, Some(300..1000)],
        vec![Some(0..500), Some(1000..1300), Some(500..1000), Some(1300..2000)],
        1000,
        1000,
        1999,
    )]
    // #[case::insert_update_idempotent(
    //     vec![None, Some(0..300), Some(0..300), Some(300..1000), Some(300..1000)],
    //     vec![Some(0..1000), Some(1000..1300), Some(1000..1300), Some(1300..2000), Some(1300..2000)],
    //     1000,
    //     1000,
    //     1999,
    // )]
    #[case::insert_idempotent_update_idempotent(
        vec![None, Some(0..300), None, Some(300..1000), None, Some(0..300), None, Some(300..1000)],
        vec![Some(0..500), Some(1000..1300), Some(500..1000), Some(1300..2000), Some(0..500), Some(1000..1300), Some(500..1000), Some(1300..2000)],
        1000,
        1000,
        1999,
    )]
    #[tokio::test]
    async fn test_bulk_changes(
        #[case] old_pks: Vec<Option<Range<i32>>>,
        #[case] new_pks: Vec<Option<Range<i32>>>,
        #[case] count: usize,
        #[case] min: usize,
        #[case] max: usize,
        #[values(1, 2, 3, 4, 5)] flush_freq: usize,
    ) {
        let ctx = Arc::new(in_memory_context().await);
        let mut sync_mgr = SeafowlDataSyncWriter::new(ctx.clone());
        let arrow_schema = test_schema();
        let sync_schema = arrow_to_sync_schema(arrow_schema.clone()).unwrap();

        // Enqueue all syncs
        let table_uuid = Uuid::new_v4();
        let log_store = ctx
            .get_internal_object_store()
            .unwrap()
            .get_log_store(&table_uuid.to_string());
        let sync_target = LakehouseSyncTarget::Delta(log_store.clone());
        // The sync mechanism doesn't register the table, so for the sake of testing do it here
        ctx.metastore
            .tables
            .create(
                &ctx.default_catalog,
                &ctx.default_schema,
                "test_table",
                arrow_schema.as_ref(),
                table_uuid,
            )
            .await
            .unwrap();

        // INSERT 1K rows in batches of `insert_batch_rows` rows
        for (seq, (old_pks, new_pks)) in
            old_pks.into_iter().zip(new_pks.into_iter()).enumerate()
        {
            let old_pks = old_pks.map(Iterator::collect::<Vec<i32>>);
            let new_pks = new_pks.map(Iterator::collect::<Vec<i32>>);
            let rows = old_pks
                .clone()
                .map(|pks| pks.len())
                .or(new_pks.clone().map(|pks| pks.len()))
                .unwrap();

            let batch = RecordBatch::try_new(
                arrow_schema.clone(),
                vec![
                    Arc::new(
                        old_pks
                            .map(Int32Array::from)
                            .unwrap_or(Int32Array::new_null(rows)),
                    ),
                    Arc::new(
                        new_pks
                            .map(Int32Array::from)
                            .unwrap_or(Int32Array::new_null(rows)),
                    ),
                    Arc::new(Float32Array::new_null(rows)),
                ],
            )
            .unwrap();

            sync_mgr
                .enqueue_sync(
                    sync_target.clone(),
                    Some(seq as SequenceNumber),
                    A.to_string(),
                    sync_schema.clone(),
                    vec![batch],
                )
                .unwrap();

            if (seq + 1) % flush_freq == 0 {
                // Flush after every flush_freq sync
                sync_mgr.flush_syncs(log_store.root_uri()).await.unwrap();
            }
        }

        // Flush any remaining syncs
        sync_mgr.flush_syncs(log_store.root_uri()).await.unwrap();

        // Check row count, min and max directly:
        let results = ctx
            .collect(
                ctx.plan_query(
                    "SELECT COUNT(*), COUNT(DISTINCT c1), MIN(c1), MAX(c1) FROM test_table",
                )
                .await
                .unwrap(),
            )
            .await
            .unwrap();

        let dist_min_max = format!("| {count}     | {count}                          | {min:<4}               | {max:<4}               |");
        let expected = [
            "+----------+-------------------------------+--------------------+--------------------+",
            "| count(*) | count(DISTINCT test_table.c1) | min(test_table.c1) | max(test_table.c1) |",
            "+----------+-------------------------------+--------------------+--------------------+",
            dist_min_max.as_str(),
            "+----------+-------------------------------+--------------------+--------------------+",
        ];

        assert_batches_eq!(expected, &results);
    }

    type OldNewVal = (Vec<i32>, Vec<i32>, Option<Vec<&'static str>>);

    #[rstest]
    #[case(vec![(vec![1, 2], vec![1, 2], None)])]
    #[case(vec![
        (vec![1, 2, 3], vec![3, 1, 2], Some(vec!["a", "b", "a"])),
        (vec![1, 2, 3], vec![3, 1, 2], Some(vec!["b", "a", "b"])),
    ])]
    #[case(vec![
        (vec![2], vec![3], Some(vec!["b"])),
        (vec![1], vec![2], Some(vec!["a"])),
        (vec![3], vec![1], Some(vec!["b"])),
        (vec![2], vec![3], Some(vec!["a"])),
        (vec![1], vec![2], Some(vec!["b"])),
        (vec![3], vec![1], Some(vec!["a"]))])
    ]
    #[tokio::test]
    async fn test_sync_pk_cycles(#[case] pk_cycle: Vec<OldNewVal>) {
        let ctx = Arc::new(in_memory_context().await);
        let mut sync_mgr = SeafowlDataSyncWriter::new(ctx.clone());

        ctx.plan_query(
            "CREATE TABLE test_table(c1 INT, c2 TEXT) AS VALUES (1, 'a'), (2, 'b')",
        )
        .await
        .unwrap();
        let table_uuid = ctx.get_table_uuid("test_table").await.unwrap();

        // Ensure original content
        let plan = ctx.plan_query("SELECT * FROM test_table").await.unwrap();
        let results = ctx.collect(plan).await.unwrap();

        let expected = [
            "+----+----+",
            "| c1 | c2 |",
            "+----+----+",
            "| 1  | a  |",
            "| 2  | b  |",
            "+----+----+",
        ];
        assert_batches_eq!(expected, &results);

        let schema = Arc::new(Schema::new(vec![
            Field::new("old_pk_c1", DataType::Int32, true),
            Field::new("new_pk_c1", DataType::Int32, true),
            Field::new("changed_c2", DataType::Boolean, true),
            Field::new("value_c2", DataType::Utf8, true),
        ]));
        let sync_schema = arrow_to_sync_schema(schema.clone()).unwrap();

        // Enqueue all syncs
        let log_store = ctx
            .get_internal_object_store()
            .unwrap()
            .get_log_store(&table_uuid.to_string());
        let sync_target = LakehouseSyncTarget::Delta(log_store.clone());

        // Cycle through the PKs, to end up in the same place as at start
        for (old_pks, new_pks, value) in pk_cycle {
            sync_mgr
                .enqueue_sync(
                    sync_target.clone(),
                    None,
                    A.to_string(),
                    sync_schema.clone(),
                    vec![RecordBatch::try_new(
                        schema.clone(),
                        vec![
                            Arc::new(Int32Array::from(old_pks.clone())),
                            Arc::new(Int32Array::from(new_pks)),
                            Arc::new(BooleanArray::from(vec![
                                value.is_some();
                                old_pks.len()
                            ])),
                            Arc::new(
                                value
                                    .map(StringArray::from)
                                    .unwrap_or(StringArray::new_null(old_pks.len())),
                            ),
                        ],
                    )
                    .unwrap()],
                )
                .unwrap();
        }
        sync_mgr.flush_syncs(log_store.root_uri()).await.unwrap();

        // Ensure updated content is the same as original
        let plan = ctx
            .plan_query("SELECT * FROM test_table ORDER BY c1")
            .await
            .unwrap();
        let results = ctx.collect(plan).await.unwrap();

        let expected = [
            "+----+----+",
            "| c1 | c2 |",
            "+----+----+",
            "| 1  | a  |",
            "| 2  | b  |",
            "+----+----+",
        ];
        assert_batches_eq!(expected, &results);
    }

    #[tokio::test]
    async fn test_decimal_sync() -> SyncResult<()> {
        let ctx = Arc::new(in_memory_context().await);
        let mut sync_mgr = SeafowlDataSyncWriter::new(ctx.clone());

        ctx.plan_query("CREATE TABLE test_decimal(c1 INT, c2 DECIMAL(38, 6))")
            .await
            .unwrap();
        let table_uuid = ctx.get_table_uuid("test_decimal").await.unwrap();

        let precision = 38;
        let scale = 6;
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("old_pk_c1", DataType::Int32, true),
            Field::new("new_pk_c1", DataType::Int32, true),
            Field::new("value_c2", DataType::Decimal128(precision, scale), true),
        ]));
        let sync_schema = arrow_to_sync_schema(arrow_schema.clone())?;
        let data = RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(Int32Array::from(vec![None, None, None, None, None, None])),
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6])),
                Arc::new(
                    Decimal128Array::from(vec![
                        99999999999999999999999999999999999999i128, // 38 9s
                        -99999999999999999999999999999999999999i128, // 38 9s
                        9999999999999999999999999999999999999i128,  // 37 9s
                        -9999999999999999999999999999999999999i128, // 37 9s
                        999999999999999999999999999999999999i128,   // 36 9s
                        -999999999999999999999999999999999999i128,  // 36 9s
                    ])
                    .with_precision_and_scale(precision, scale)?,
                ),
            ],
        )?;

        // Enqueue all syncs
        let log_store = ctx
            .get_internal_object_store()?
            .get_log_store(&table_uuid.to_string());

        let sync_target = LakehouseSyncTarget::Delta(log_store.clone());

        // Add first non-empty sync
        sync_mgr.enqueue_sync(
            sync_target.clone(),
            None,
            A.to_string(),
            sync_schema.clone(),
            vec![data],
        )?;
        sync_mgr.flush_syncs(log_store.root_uri()).await?;

        // Ensure updated content is the same as original
        let plan = ctx
            .plan_query("SELECT * FROM test_decimal ORDER BY c1")
            .await
            .unwrap();
        let results = ctx.collect(plan).await.unwrap();

        let expected = [
            "+----+------------------------------------------+",
            "| c1 | c2                                       |",
            "+----+------------------------------------------+",
            "| 1  | 99999999999999999999999999999999.999999  |",
            "| 2  | -99999999999999999999999999999999.999999 |",
            "| 3  | 9999999999999999999999999999999.999999   |",
            "| 4  | -9999999999999999999999999999999.999999  |",
            "| 5  | 999999999999999999999999999999.999999    |",
            "| 6  | -999999999999999999999999999999.999999   |",
            "+----+------------------------------------------+",
        ];
        assert_batches_eq!(expected, &results);

        Ok(())
    }
}
