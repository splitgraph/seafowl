use arrow::array::RecordBatch;
use arrow_schema::{SchemaBuilder, SchemaRef};
use clade::sync::ColumnRole;
use datafusion::datasource::{provider_as_source, MemTable, TableProvider};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_optimizer::pruning::PruningPredicate;
use datafusion::prelude::DataFrame;
use datafusion_common::{JoinType, Result, ScalarValue, ToDFSchema};
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::{
    col, is_null, lit, when, LogicalPlan, LogicalPlanBuilder, Projection,
};
use datafusion_expr::{is_true, Expr};
use deltalake::delta_datafusion::DeltaTableProvider;
use deltalake::kernel::{Action, Add, Remove, Schema};
use deltalake::logstore::LogStore;
use deltalake::operations::create::CreateBuilder;
use deltalake::protocol::{DeltaOperation, SaveMode};
use deltalake::DeltaTable;
use indexmap::IndexMap;
use std::collections::{HashMap, HashSet};
use std::ops::Not;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::context::delta::plan_to_object_store;

use crate::context::SeafowlContext;
use crate::frontend::flight::handler::SYNC_COMMIT_INFO;
use crate::frontend::flight::sync::metrics::SyncMetrics;
use crate::frontend::flight::sync::schema::SyncSchema;
use crate::frontend::flight::sync::utils::{
    construct_qualifier, get_prune_map, merge_schemas, squash_batches,
};
use crate::frontend::flight::sync::{
    Origin, SequenceNumber, SyncCommitInfo, SyncError, SyncResult,
};

const SYNC_REF: &str = "sync_data";
pub(super) const JOIN_COLUMN: &str = "__join_col";
pub(super) const LOWER_SYNC: &str = "__lower_sync";
pub(super) const UPPER_SYNC: &str = "__upper_sync";
const FINE_GRAINED_PRUNING_ROW_CRITERIA: i64 = 3_000_000;

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
pub(crate) struct SeafowlDataSyncWriter {
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
    metrics: SyncMetrics,
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
    log_store: Arc<dyn LogStore>,
    // Collection of batches to replicate
    pub(super) syncs: Vec<DataSyncItem>,
}

// An object corresponding to a single `do_put` call.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct DataSyncItem {
    // The (internal) id of the transaction that this change belongs to; for now it corresponds to
    // a random v4 UUID generated for the first message received in this transaction.
    pub(super) tx_id: Uuid,
    // Old and new primary keys, changed and value columns
    pub(super) sync_schema: SyncSchema,
    // Record batch to replicate
    pub(super) batch: RecordBatch,
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
        log_store: Arc<dyn LogStore>,
        sequence_number: Option<SequenceNumber>,
        origin: Origin,
        sync_schema: SyncSchema,
        batches: Vec<RecordBatch>,
    ) -> SyncResult<()> {
        let url = log_store.root_uri();

        let (sync_size, sync_rows) =
            batches.iter().fold((0, 0), |(size, rows), batch| {
                (
                    size + batch.get_array_memory_size(),
                    rows + batch.num_rows(),
                )
            });

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
            // Squash the batches and measure the time it took and the reduction in rows/size
            self.metrics.request_bytes.increment(sync_size as u64);
            self.metrics.request_rows.increment(sync_rows as u64);
            let start = Instant::now();
            let batch = squash_batches(&sync_schema, batches)?;
            let duration = start.elapsed().as_secs();

            // Get new size and row count
            let size = batch.get_array_memory_size();
            let rows = batch.num_rows();

            let item = DataSyncItem {
                tx_id,
                sync_schema,
                batch,
            };
            self.syncs
                .entry(url)
                .and_modify(|entry| {
                    entry.syncs.push(item.clone());
                    entry.size += size;
                    entry.rows += rows;
                })
                .or_insert(DataSyncCollection {
                    size,
                    rows,
                    insertion_time: now(),
                    log_store,
                    syncs: vec![item],
                });

            // Update the total size and metrics
            self.size += size;
            self.metrics.in_memory_bytes.increment(size as f64);
            self.metrics.in_memory_rows.increment(rows as f64);
            self.metrics.squash_time.record(duration as f64);
            self.metrics
                .squashed_bytes
                .increment((sync_size - size) as u64);
            self.metrics
                .squashed_rows
                .increment((sync_rows - rows) as u64);
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
        log_store: Arc<dyn LogStore>,
        sync_schema: &SyncSchema,
    ) -> SyncResult<DeltaTable> {
        // Get the actual table schema by removing the OldPk and Changed column roles from the schema.
        let mut builder = SchemaBuilder::new();
        sync_schema.columns().iter().for_each(|col| {
            if matches!(col.role(), ColumnRole::NewPk | ColumnRole::Value) {
                let field = col.field().as_ref().clone().with_name(col.name());
                builder.push(field);
            }
        });

        let delta_schema = Schema::try_from(&builder.finish())?;

        Ok(CreateBuilder::new()
            .with_log_store(log_store)
            .with_columns(delta_schema.fields().cloned())
            .with_comment(format!("Synced by Seafowl {}", env!("CARGO_PKG_VERSION")))
            .await?)
    }

    pub async fn flush(&mut self) -> SyncResult<()> {
        while let Some(url) = self.flush_ready() {
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

    // Criteria for return the cached entry ready to be persisted to storage.
    // First flush any records that are explicitly beyond the configured max
    // lag, followed by further entries if we're still above max cache size.
    fn flush_ready(&mut self) -> Option<String> {
        if let Some((url, sync)) = self.syncs.first()
            && now() - sync.insertion_time
                >= self.context.config.misc.sync_conf.max_replication_lag_s
        {
            // First flush any changes that are past the configured max duration
            info!(
                "Flushing due to lag-based criteria ({}): {url}",
                sync.insertion_time
            );
            Some(url.clone())
        } else if self.size >= self.context.config.misc.sync_conf.max_in_memory_bytes {
            // Or if we're over the size limit flush the oldest entry
            let url = self.syncs.first().map(|kv| kv.0.clone()).unwrap();
            info!("Flushing due to size-based criteria ({}): {url}", self.size);
            Some(url.clone())
        } else if let Some((url, entry)) = self.syncs.iter().find(|(_, entry)| {
            entry.syncs.len() >= self.context.config.misc.sync_conf.max_syncs_per_url
        }) {
            // Otherwise if there are pending syncs with more than a predefined number of calls
            // waiting to be flushed flush them.
            // This is a guard against hitting a stack overflow when applying syncs, since this
            // results in deeply nested plan trees that are known to be problematic for now:
            // - https://github.com/apache/datafusion/issues/9373
            // - https://github.com/apache/datafusion/issues/9375
            // TODO: Make inter-sync squashing work when/even in absence of sync schema match
            info!(
                "Flushing due to max-sync messages criteria ({}): {url}",
                entry.syncs.len()
            );
            Some(url.clone())
        } else {
            None
        }
    }

    // Flush the table containing the oldest sync in memory
    async fn flush_syncs(&mut self, url: String) -> SyncResult<()> {
        let entry = match self.syncs.get(&url) {
            Some(table_syncs) => table_syncs,
            None => {
                info!("No pending syncs to flush");
                return Ok(());
            }
        };

        info!("Flushing {} syncs for url {url}", entry.syncs.len());

        let start = Instant::now();
        let insertion_time = entry.insertion_time;
        let rows = entry.rows;
        let size = entry.size;
        let url = url.clone();
        let log_store = entry.log_store.clone();

        // If there's no delta table at this location yet create one first.
        if !log_store.is_delta_table_location().await? {
            debug!("Creating new Delta table at location: {url}");
            self.create_table(
                log_store.clone(),
                &entry.syncs.first().unwrap().sync_schema,
            )
            .await?;
        }

        let mut table = DeltaTable::new(log_store.clone(), Default::default());
        table.load().await?;

        let last_sync_commit = self.table_sequence(&table).await?;
        let (syncs, new_sync_commit) = self.skip_syncs(&last_sync_commit, &entry.syncs);

        info!(
            "Location at {url} already durable up to {:?}, skipping {} messages",
            last_sync_commit,
            entry.syncs.len() - syncs.len(),
        );

        if syncs.is_empty() {
            // TODO: Update metrics
            self.remove_sync(&url);
            return Ok(());
        }

        // Use the schema from the object store as a source of truth, since it's not guaranteed
        // that any of the entries has the full column list.
        let full_schema = TableProvider::schema(&table);

        let prune_start = Instant::now();
        // Gather previous Add files that (might) need to be re-written.
        let files = self.prune_partitions(syncs, full_schema.clone(), &table)?;
        let prune_time = prune_start.elapsed().as_millis();
        info!(
            "Partition pruning found {} files in {prune_time} ms",
            files.len(),
        );
        self.metrics.pruning_time.record(prune_time as f64);
        self.metrics.pruning_files.record(files.len() as f64);
        // Create removes to prune away files that are refuted by the qualifier
        let removes = files
            .iter()
            .map(|add| {
                Action::Remove(Remove {
                    path: add.path.clone(),
                    deletion_timestamp: Some(now() as i64),
                    data_change: true,
                    extended_file_metadata: Some(true),
                    partition_values: Some(add.partition_values.clone()),
                    size: Some(add.size),
                    tags: None,
                    deletion_vector: None,
                    base_row_id: None,
                    default_row_commit_version: None,
                })
            })
            .collect::<Vec<_>>();

        // Create a special Delta table provider that will only hit the above partition files
        let base_scan = Arc::new(
            DeltaTableProvider::try_new(
                table.snapshot()?.clone(),
                log_store.clone(),
                Default::default(),
            )?
            .with_files(files),
        );

        // Convert the custom Delta table provider into a base logical plan
        let base_plan =
            LogicalPlanBuilder::scan(SYNC_REF, provider_as_source(base_scan), None)?
                .build()?;

        // Construct a state for physical planning; we omit all analyzer/optimizer rules to increase
        // the stack overflow threshold that occurs during recursive plan tree traversal in DF.
        let state = SessionStateBuilder::new_from_existing(self.context.inner.state())
            .with_analyzer_rules(vec![])
            .with_optimizer_rules(vec![])
            .build();
        let base_df = DataFrame::new(state.clone(), base_plan);
        let (sync_schema, sync_df) = self.squash_syncs(syncs)?;

        let input_df = self.apply_syncs(full_schema, base_df, sync_df, &sync_schema)?;
        let input_plan = input_df.create_physical_plan().await?;

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
        self.context.commit(actions, &table, op).await?;
        debug!("Committed data sync up to {new_sync_commit:?} for location {url}");

        // We've flushed all the presently accumulated batches for this location.
        // Modify our syncs and sequences maps to reflect this.
        let tx_ids = entry.syncs.iter().map(|sync| sync.tx_id).collect();
        self.remove_tx_locations(url.clone(), tx_ids);
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

    // Given a known `SyncCommitInfo` from a previous commit try to deduce whether some of the pending
    // sync are actually redundant (since they were already persisted) and skip them.
    // Also create a new `SyncCommitInfo` that corresponds to the last full transaction in the
    // pending changes,
    fn skip_syncs<'a>(
        &self,
        last_sync_commit: &Option<SyncCommitInfo>,
        syncs: &'a [DataSyncItem],
    ) -> (&'a [DataSyncItem], Option<SyncCommitInfo>) {
        let syncs = if let Some(sync_commit) = last_sync_commit {
            let mut start = 0;
            for (ind, sync) in syncs.iter().enumerate() {
                if let Some(Transaction {
                    origin,
                    sequence: Some(seq),
                    ..
                }) = self.txs.get(&sync.tx_id)
                    && origin == &sync_commit.origin
                    && seq == &sync_commit.sequence
                {
                    // We've found a matching sequence in the pending syncs, we can skip everything
                    // up to it
                    start = ind + 1;
                    break;
                }
            }

            &syncs[start..]
        } else {
            syncs
        };

        // Now deduce the effective origin/sequence up to which this table will be fully durable
        // after the pending flush.
        let new_tx = syncs
            .last()
            .map(|sync| self.txs[&sync.tx_id].sequence.is_none())
            .unwrap_or_default();
        let new_sync_commit = syncs
            .iter()
            .rev()
            .find_map(|sync| {
                if let Some(Transaction {
                    origin,
                    sequence: Some(seq),
                    ..
                }) = self.txs.get(&sync.tx_id)
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
            }));

        (syncs, new_sync_commit)
    }

    // Perform logical squashing of all pending sync batches into a single dataframe/plan, which can
    // then be joined against the base scan
    fn squash_syncs(
        &self,
        syncs: &[DataSyncItem],
    ) -> SyncResult<(SyncSchema, DataFrame)> {
        let first_sync = syncs.first().unwrap();
        let mut sync_schema = first_sync.sync_schema.clone();
        let first_batch = first_sync.batch.clone();
        let provider = MemTable::try_new(first_batch.schema(), vec![vec![first_batch]])?;
        let mut sync_df = DataFrame::new(
            self.context.inner.state(),
            LogicalPlanBuilder::scan(
                LOWER_SYNC,
                provider_as_source(Arc::new(provider)),
                None,
            )?
            .build()?,
        );

        // Make a plan to squash all syncs into a single change stream
        for sync in &syncs[1..] {
            (sync_schema, sync_df) = self.merge_syncs(
                &sync_schema,
                sync_df,
                &sync.sync_schema,
                sync.batch.clone(),
            )?;
        }

        Ok((sync_schema, sync_df))
    }

    // Build a plan to merge two adjacent syncs into one.
    // It assumes that both syncs were squashed before-hand (i.e. no PK-chains in either one).
    fn merge_syncs(
        &self,
        lower_schema: &SyncSchema,
        lower_df: DataFrame,
        upper_schema: &SyncSchema,
        upper_data: RecordBatch,
    ) -> SyncResult<(SyncSchema, DataFrame)> {
        let provider = MemTable::try_new(upper_data.schema(), vec![vec![upper_data]])?;
        let upper_df = DataFrame::new(
            self.context.inner.state(),
            LogicalPlanBuilder::scan(
                UPPER_SYNC,
                provider_as_source(Arc::new(provider)),
                None,
            )?
            .build()?,
        );

        let (lower_join_cols, upper_join_cols): (Vec<String>, Vec<String>) = upper_schema
            .map_columns(ColumnRole::OldPk, |sc| {
                let lower_name = lower_schema
                    .column(sc.name(), ColumnRole::NewPk)
                    .expect("PK columns identical")
                    .field()
                    .name()
                    .clone();
                let upper_name = sc.field().name().clone();
                (lower_name, upper_name)
            })
            .into_iter()
            .unzip();

        // TODO merge syncs using a union if schemas match
        let upper_df = upper_df.with_column(UPPER_SYNC, lit(true))?;
        let lower_df = lower_df.with_column(LOWER_SYNC, lit(true))?;

        let merged_sync = upper_df.join(
            lower_df,
            JoinType::Full,
            &upper_join_cols
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            &lower_join_cols
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            None,
        )?;

        // Build the merged projection and column descriptors
        let (col_desc, projection) = merge_schemas(lower_schema, upper_schema)?;

        // The `DataFrame::select`/`LogicalPlanBuilder::project` projection API leads to sub-optimal
        // performance since it does a bunch of expression normalization that we don't really need.
        // So deconstruct the present one and add a vanilla `Projection` on top of it.
        let (session_state, plan) = merged_sync.into_parts();
        let sync_plan = Projection::try_new(projection, Arc::new(plan))
            .map(LogicalPlan::Projection)?;

        Ok((
            SyncSchema::try_new(col_desc, sync_plan.schema().inner().clone())?,
            DataFrame::new(session_state, sync_plan),
        ))
    }

    fn apply_syncs(
        &self,
        full_schema: SchemaRef,
        base_df: DataFrame,
        sync_df: DataFrame,
        sync_schema: &SyncSchema,
    ) -> SyncResult<DataFrame> {
        // Skip rows where both old and new primary keys are NULL, meaning a row inserted/updated
        // and deleted within the same sync message (so it shouldn't be in the input nor output)
        let old_pk_nulls = sync_schema
            .map_columns(ColumnRole::OldPk, |c| is_null(col(c.field().name())))
            .into_iter()
            .reduce(|e1: Expr, e2| e1.and(e2))
            .unwrap();

        let new_pk_nulls = sync_schema
            .map_columns(ColumnRole::NewPk, |c| is_null(col(c.field().name())))
            .into_iter()
            .reduce(|e1: Expr, e2| e1.and(e2))
            .unwrap();

        // These differ since the physical column names are reflected in the ColumnDescriptor,
        // while logical column names are found in the arrow fields
        let (base_pk_cols, sync_pk_cols): (Vec<String>, Vec<String>) = sync_schema
            .map_columns(ColumnRole::OldPk, |c| {
                (c.name().clone(), c.field().name().clone())
            })
            .into_iter()
            .unzip();

        let input_df = base_df
            .with_column(JOIN_COLUMN, lit(true))?
            .join(
                sync_df.filter(old_pk_nulls.clone().and(new_pk_nulls.clone()).not())?, // Filter out any temp rows
                JoinType::Full,
                &base_pk_cols
                    .iter()
                    .map(|pk| pk.as_str())
                    .collect::<Vec<_>>(),
                &sync_pk_cols
                    .iter()
                    .map(|pk| pk.as_str())
                    .collect::<Vec<_>>(),
                None,
            )?
            .filter(old_pk_nulls.clone().not().and(new_pk_nulls.clone()).not())?; // Remove deletes

        // Normalize the schema, by ordering columns according to the full table schema and
        // projecting the sync data accordingly.
        let projection = full_schema
            .flattened_fields()
            .iter()
            .map(|f| {
                let name = f.name();

                let expr = if let Some(sync_col) = sync_schema
                    .column(name, ColumnRole::Value)
                    .or(sync_schema.column(name, ColumnRole::NewPk))
                {
                    // The column is present in the sync schema...
                    when(
                        old_pk_nulls.clone().and(new_pk_nulls.clone()),
                        // ...but the row doesn't exist in the sync, so inherit the old value
                        col(name),
                    )
                    .otherwise(
                        if let Some(changed_sync_col) =
                            sync_schema.column(name, ColumnRole::Changed)
                        {
                            // ... and there is a `Changed` flag denoting whether the column has changed.
                            when(
                                is_true(col(changed_sync_col.field().name())),
                                // If it's true take the new value
                                col(sync_col.field().name()),
                            )
                            .otherwise(
                                // If it's false take the old value
                                col(name),
                            )?
                        } else {
                            // ... and the sync has a new corresponding value without a `Changed` flag
                            col(sync_col.field().name())
                        },
                    )?
                } else {
                    when(
                        is_null(col(JOIN_COLUMN)),
                        // Column is not present in the sync schema, and the old row doesn't exist
                        // either, project a NULL
                        lit(ScalarValue::Null.cast_to(f.data_type())?),
                    )
                    .otherwise(
                        // Column is not present in the sync schema, but the old row does exist
                        // so project its value
                        col(name),
                    )?
                };

                Ok(expr.alias(name))
            })
            .collect::<Result<_>>()?;

        // The `DataFrame::select`/`LogicalPlanBuilder::project` projection API leads to sub-optimal
        // performance since it does a bunch of expression normalization that we don't really need.
        // So deconstruct the present one and add a vanilla `Projection` on top of it.
        let (session_state, plan) = input_df.into_parts();
        let sync_plan = Projection::try_new(projection, Arc::new(plan))
            .map(LogicalPlan::Projection)?;

        Ok(DataFrame::new(session_state, sync_plan))
    }

    // Get a list of files that need to be scanned, re-written and removed based on the PK values in
    // the sync items.
    fn prune_partitions(
        &self,
        syncs: &[DataSyncItem],
        full_schema: SchemaRef,
        table: &DeltaTable,
    ) -> SyncResult<Vec<Add>> {
        let snapshot = table.snapshot()?;
        let files = snapshot.file_actions()?;

        // First perform coarse-grained pruning, by only looking at global min-max in the syncs.
        // Generate a qualifier expression for old PKs; we definitely need to overwrite those in case
        // of PK-changing UPDATEs or DELETEs. Note that this can be `None` if it's an all-INSERT
        // syncs vec.
        let old_pk_qualifier = construct_qualifier(syncs, ColumnRole::OldPk)?;
        // Next construct the qualifier for new PKs; these are only needed for idempotence.
        let new_pk_qualifier = construct_qualifier(syncs, ColumnRole::NewPk)?;
        let qualifier = match (old_pk_qualifier, new_pk_qualifier) {
            (Some(old_pk_q), Some(new_pk_q)) => old_pk_q.or(new_pk_q),
            (Some(old_pk_q), None) => old_pk_q,
            (None, Some(new_pk_q)) => new_pk_q,
            _ => panic!("There can be no situation without either old or new PKs"),
        };

        let prune_expr = create_physical_expr(
            &qualifier,
            &full_schema.clone().to_dfschema()?,
            &ExecutionProps::new(),
        )?;
        let pruning_predicate =
            PruningPredicate::try_new(prune_expr, full_schema.clone())?;

        let mut prune_map = pruning_predicate.prune(snapshot)?;

        let partition_count = prune_map.iter().filter(|p| **p).count();
        let total_rows = files
            .iter()
            .zip(&prune_map)
            .filter_map(|(add, keep)| {
                if *keep {
                    if let Ok(Some(stats)) = add.get_json_stats() {
                        Some(stats.num_records)
                    } else {
                        warn!("Missing log stats for Add {add:?}");
                        None
                    }
                } else {
                    None
                }
            })
            .sum::<i64>();
        debug!("Coarse-grained pruning found {partition_count} partitions with {total_rows} rows in total to re-write");

        // TODO: think of a better heuristic to trigger granular pruning
        // Try granular pruning if total row count is higher than 3M
        if total_rows > FINE_GRAINED_PRUNING_ROW_CRITERIA {
            let prune_start = Instant::now();
            let new_prune_map = get_prune_map(syncs, snapshot)?;
            let new_partition_count = new_prune_map.iter().filter(|p| **p).count();
            info!(
                "Fine-grained pruning scoped out {} partitions in {} ms",
                partition_count - new_partition_count,
                prune_start.elapsed().as_millis()
            );
            prune_map = new_prune_map;
        }

        Ok(files
            .iter()
            .zip(prune_map)
            .filter_map(|(add, keep)| if keep { Some(add.clone()) } else { None })
            .collect::<Vec<Add>>())
    }

    // Remove the pending location from a sequence for all syncs in the collection
    fn remove_tx_locations(&mut self, url: String, tx_ids: Vec<Uuid>) {
        for tx_id in tx_ids {
            // Remove the pending location for this origin/sequence
            if let Some(tx) = self.txs.get_mut(&tx_id) {
                tx.locations.remove(&url);
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

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs()
}

#[cfg(test)]
mod tests {
    use crate::context::test_utils::in_memory_context;
    use crate::frontend::flight::sync::schema::SyncSchema;
    use crate::frontend::flight::sync::writer::{
        SeafowlDataSyncWriter, SequenceNumber, LOWER_SYNC,
    };
    use arrow::{array::RecordBatch, util::data_gen::create_random_batch};
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use clade::sync::{ColumnDescriptor, ColumnRole};
    use rand::Rng;
    use rstest::rstest;
    use std::collections::HashMap;

    use crate::frontend::flight::sync::{SyncCommitInfo, SyncResult};
    use arrow::array::{BooleanArray, Float32Array, Int32Array, StringArray};
    use datafusion::dataframe::DataFrame;
    use datafusion::datasource::{provider_as_source, MemTable};
    use datafusion_common::assert_batches_eq;
    use datafusion_expr::{col, LogicalPlanBuilder};
    use itertools::Itertools;
    use std::sync::Arc;
    use uuid::Uuid;

    fn sync_schema() -> (SchemaRef, SyncSchema) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("old_c1", DataType::Int32, true),
            Field::new("new_c1", DataType::Int32, true),
            Field::new("value_c2", DataType::Float32, true),
        ]));

        let column_descriptors = vec![
            ColumnDescriptor {
                role: ColumnRole::OldPk as i32,
                name: "c1".to_string(),
            },
            ColumnDescriptor {
                role: ColumnRole::NewPk as i32,
                name: "c1".to_string(),
            },
            ColumnDescriptor {
                role: ColumnRole::Value as i32,
                name: "c2".to_string(),
            },
        ];

        (
            schema.clone(),
            SyncSchema::try_new(column_descriptors, schema).unwrap(),
        )
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
        let ctx = Arc::new(in_memory_context().await);
        let mut sync_mgr = SeafowlDataSyncWriter::new(ctx.clone());
        let (arrow_schema, sync_schema) = sync_schema();

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
                    log_store,
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

    #[rstest]
    #[case::no_old_no_new(
        &[(T1, A, None), (T2, A, None)],
        None,
        0,
        None,
    )]
    #[case::no_old_some_new(
        &[(T1, A, None), (T2, A, None), (T3, A, Some(10))],
        None,
        0,
        Some(SyncCommitInfo::new(A, 10u64)),
    )]
    #[case::some_old_new(
        &[(T1, A, None), (T2, A, None)],
        Some(SyncCommitInfo::new(A, 5u64)),
        0,
        Some(SyncCommitInfo::new(A, 5u64).with_new_tx(true)),
    )]
    #[case::some_old_full_tx(
        &[(T1, A, None), (T2, A, None), (T3, A, Some(10))],
        Some(SyncCommitInfo::new(A, 5u64)),
        0,
        Some(SyncCommitInfo::new(A, 10u64)),
    )]
    #[case::some_old_full_and_new_tx(
        &[(T1, A, None), (T2, A, None), (T3, A, Some(10)), (T1, A, None)],
        Some(SyncCommitInfo::new(A, 5u64)),
        0,
        Some(SyncCommitInfo::new(A, 10u64).with_new_tx(true)),
    )]
    #[case::some_old_full_tx_skip(
        &[(T1, A, None), (T2, A, None), (T3, A, Some(10))],
        Some(SyncCommitInfo::new(A, 10u64)),
        1,
        Some(SyncCommitInfo::new(A, 10u64)),
    )]
    #[case::some_old_full_and_new_tx_skip(
        &[(T1, A, None), (T2, A, None), (T3, A, Some(10)), (T1, A, None)],
        Some(SyncCommitInfo::new(A, 10u64)),
        1,
        Some(SyncCommitInfo::new(A, 10u64).with_new_tx(true)),
    )]
    #[case::some_old_multiple_txs(
        &[(T1, A, None), (T2, A, None), (T3, A, Some(10)),
            (T1, B, None), (T2, B, None), (T3, B, Some(20)),
            (T1, A, None), (T2, A, None), (T3, A, Some(30))],
        Some(SyncCommitInfo::new(A, 5u64)),
        0,
        Some(SyncCommitInfo::new(A, 30u64)),
    )]
    #[case::some_old_multiple_txs_skip(
        &[(T1, A, None), (T2, A, None), (T3, A, Some(10)),
            (T1, B, None), (T2, B, None), (T3, B, Some(20)),
            (T1, A, None), (T2, A, None), (T3, A, Some(30))],
        Some(SyncCommitInfo::new(B, 20u64)),
        2,
        Some(SyncCommitInfo::new(A, 30u64)),
    )]
    #[tokio::test]
    async fn test_sync_skipping(
        #[case] syncs: &[(&str, &str, Option<i64>)],
        #[case] last_sync_commit: Option<SyncCommitInfo>,
        #[case] skip_ind: usize,
        #[case] expected_sync_commit: Option<SyncCommitInfo>,
    ) {
        let ctx = Arc::new(in_memory_context().await);
        let mut sync_mgr = SeafowlDataSyncWriter::new(ctx.clone());
        let (arrow_schema, sync_schema) = sync_schema();

        // Enqueue all syncs
        for (table_name, origin, sequence) in syncs {
            let log_store = ctx
                .get_internal_object_store()
                .unwrap()
                .get_log_store(table_name);

            sync_mgr
                .enqueue_sync(
                    log_store,
                    sequence.map(|seq| seq as SequenceNumber),
                    origin.to_string(),
                    sync_schema.clone(),
                    random_batches(arrow_schema.clone()),
                )
                .unwrap();
        }

        let in_syncs = &sync_mgr.syncs.first().unwrap().1.syncs;
        let (out_syncs, new_sync_commit) =
            sync_mgr.skip_syncs(&last_sync_commit, in_syncs);

        assert_eq!(expected_sync_commit, new_sync_commit);
        assert_eq!(in_syncs[skip_ind..].len(), out_syncs.len());
        assert_eq!(in_syncs[skip_ind..].to_vec(), out_syncs.to_vec());
    }

    #[tokio::test]
    async fn test_empty_sync() {
        let ctx = Arc::new(in_memory_context().await);
        let mut sync_mgr = SeafowlDataSyncWriter::new(ctx.clone());
        let (arrow_schema, sync_schema) = sync_schema();

        // Enqueue all syncs
        let log_store = ctx
            .get_internal_object_store()
            .unwrap()
            .get_log_store("test_table");

        // Add first non-empty sync
        sync_mgr
            .enqueue_sync(
                log_store.clone(),
                None,
                A.to_string(),
                sync_schema.clone(),
                random_batches(arrow_schema.clone()),
            )
            .unwrap();

        // Add empty sync with an explicit sequence number denoting the end of the transaction
        sync_mgr
            .enqueue_sync(
                log_store.clone(),
                Some(100),
                A.to_string(),
                SyncSchema::empty(),
                vec![],
            )
            .unwrap();

        // Adding a new empty sync with `Some` sequence number amounts to an empty transaction which
        // isn't supported
        let err = sync_mgr
            .enqueue_sync(
                log_store,
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
        sync_mgr.flush_syncs(url).await.unwrap();
        assert_eq!(
            sync_mgr.stored_sequences(&A.to_string()),
            (Some(100), Some(100))
        );
    }

    #[rstest]
    #[case(100, 50)]
    #[case(50, 100)]
    #[case(100, 100)]
    #[tokio::test]
    async fn test_bulk_insert_update(
        #[case] insert_batch_rows: i32,
        #[case] update_batch_rows: i32,
    ) {
        let ctx = Arc::new(in_memory_context().await);
        let mut sync_mgr = SeafowlDataSyncWriter::new(ctx.clone());
        let (arrow_schema, sync_schema) = sync_schema();

        // Enqueue all syncs
        let table_uuid = Uuid::new_v4();
        let log_store = ctx
            .get_internal_object_store()
            .unwrap()
            .get_log_store(&table_uuid.to_string());
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

        // INSERT 10K rows in batches of `insert_batch_rows` rows
        let batch_rows = insert_batch_rows as usize;
        for (seq, new_pks) in (0..1000).chunks(batch_rows).into_iter().enumerate() {
            let batch = RecordBatch::try_new(
                arrow_schema.clone(),
                vec![
                    Arc::new(Int32Array::new_null(batch_rows)),
                    Arc::new(Int32Array::from(new_pks.collect::<Vec<i32>>())),
                    Arc::new(Float32Array::new_null(batch_rows)),
                ],
            )
            .unwrap();

            sync_mgr
                .enqueue_sync(
                    log_store.clone(),
                    Some(seq as SequenceNumber),
                    A.to_string(),
                    sync_schema.clone(),
                    vec![batch],
                )
                .unwrap();

            if (seq + 1) % 5 == 0 {
                // Flush after every 5th sync
                sync_mgr.flush_syncs(log_store.root_uri()).await.unwrap();
            }
        }

        // Check row count, min and max directly:
        let results = ctx
            .collect(
                ctx.plan_query("SELECT COUNT(*), MIN(c1), MAX(c1) FROM test_table")
                    .await
                    .unwrap(),
            )
            .await
            .unwrap();
        let expected = [
            "+----------+--------------------+--------------------+",
            "| count(*) | min(test_table.c1) | max(test_table.c1) |",
            "+----------+--------------------+--------------------+",
            "| 1000     | 0                  | 999                |",
            "+----------+--------------------+--------------------+",
        ];

        assert_batches_eq!(expected, &results);

        // Now UPDATE each row by shifting the PK over by 1000 in batches of `update_batch_rows`` rows
        let start_seq = 10_000;
        let batch_rows = update_batch_rows as usize;
        for (seq, (old_pks, new_pks)) in (0..1000)
            .chunks(batch_rows)
            .into_iter()
            .zip((1000..2000).chunks(batch_rows).into_iter())
            .enumerate()
        {
            let batch = RecordBatch::try_new(
                arrow_schema.clone(),
                vec![
                    Arc::new(Int32Array::from(old_pks.collect::<Vec<i32>>())),
                    Arc::new(Int32Array::from(new_pks.collect::<Vec<i32>>())),
                    Arc::new(Float32Array::new_null(batch_rows)),
                ],
            )
            .unwrap();

            sync_mgr
                .enqueue_sync(
                    log_store.clone(),
                    Some(start_seq + seq as SequenceNumber),
                    A.to_string(),
                    sync_schema.clone(),
                    vec![batch],
                )
                .unwrap();

            if (seq + 1) % 5 == 0 {
                // Flush after every 5th sync
                sync_mgr.flush_syncs(log_store.root_uri()).await.unwrap();
            }
        }

        // Check row count, min and max directly again:
        let results = ctx
            .collect(
                ctx.plan_query("SELECT COUNT(*), MIN(c1), MAX(c1) FROM test_table")
                    .await
                    .unwrap(),
            )
            .await
            .unwrap();
        let expected = [
            "+----------+--------------------+--------------------+",
            "| count(*) | min(test_table.c1) | max(test_table.c1) |",
            "+----------+--------------------+--------------------+",
            "| 1000     | 1000               | 1999               |",
            "+----------+--------------------+--------------------+",
        ];
        assert_batches_eq!(expected, &results);
    }

    #[tokio::test]
    async fn test_sync_merging() -> SyncResult<()> {
        let ctx = Arc::new(in_memory_context().await);
        let sync_mgr = SeafowlDataSyncWriter::new(ctx.clone());

        // Construct lower and upper schema that cover all possible cases
        let lower_schema = Arc::new(Schema::new(vec![
            Field::new("old_c1", DataType::Int32, true),
            Field::new("new_c1", DataType::Int32, true),
            // value only in lower
            Field::new("value_c3", DataType::Utf8, true),
            // value in both
            Field::new("value_c5", DataType::Utf8, true),
            // lower has value and changed
            Field::new("changed_c6", DataType::Boolean, true),
            Field::new("value_c6", DataType::Utf8, true),
            // lower has value but not changed
            Field::new("value_c7", DataType::Utf8, true),
            // value and changed only in lower
            Field::new("changed_c8", DataType::Boolean, true),
            Field::new("value_c8", DataType::Utf8, true),
            // value and changed in both
            Field::new("changed_c10", DataType::Boolean, true),
            Field::new("value_c10", DataType::Utf8, true),
        ]));

        let upper_schema = Arc::new(Schema::new(vec![
            Field::new("old_c1", DataType::Int32, true),
            Field::new("new_c1", DataType::Int32, true),
            // value only in upper
            Field::new("value_c4", DataType::Utf8, true),
            // value in both
            Field::new("value_c5", DataType::Utf8, true),
            // upper has value but not changed
            Field::new("value_c6", DataType::Utf8, true),
            // upper has value and changed
            Field::new("changed_c7", DataType::Boolean, true),
            Field::new("value_c7", DataType::Utf8, true),
            // value and changed only in upper
            Field::new("changed_c9", DataType::Boolean, true),
            Field::new("value_c9", DataType::Utf8, true),
            // value and changed in both
            Field::new("changed_c10", DataType::Boolean, true),
            Field::new("value_c10", DataType::Utf8, true),
        ]));

        let lower_column_descriptors = vec![
            ColumnDescriptor {
                role: ColumnRole::OldPk as i32,
                name: "c1".to_string(),
            },
            ColumnDescriptor {
                role: ColumnRole::NewPk as i32,
                name: "c1".to_string(),
            },
            ColumnDescriptor {
                role: ColumnRole::Value as i32,
                name: "c3".to_string(),
            },
            ColumnDescriptor {
                role: ColumnRole::Value as i32,
                name: "c5".to_string(),
            },
            ColumnDescriptor {
                role: ColumnRole::Changed as i32,
                name: "c6".to_string(),
            },
            ColumnDescriptor {
                role: ColumnRole::Value as i32,
                name: "c6".to_string(),
            },
            ColumnDescriptor {
                role: ColumnRole::Value as i32,
                name: "c7".to_string(),
            },
            ColumnDescriptor {
                role: ColumnRole::Changed as i32,
                name: "c8".to_string(),
            },
            ColumnDescriptor {
                role: ColumnRole::Value as i32,
                name: "c8".to_string(),
            },
            ColumnDescriptor {
                role: ColumnRole::Changed as i32,
                name: "c10".to_string(),
            },
            ColumnDescriptor {
                role: ColumnRole::Value as i32,
                name: "c10".to_string(),
            },
        ];

        let upper_column_descriptors = vec![
            ColumnDescriptor {
                role: ColumnRole::OldPk as i32,
                name: "c1".to_string(),
            },
            ColumnDescriptor {
                role: ColumnRole::NewPk as i32,
                name: "c1".to_string(),
            },
            ColumnDescriptor {
                role: ColumnRole::Value as i32,
                name: "c4".to_string(),
            },
            ColumnDescriptor {
                role: ColumnRole::Value as i32,
                name: "c5".to_string(),
            },
            ColumnDescriptor {
                role: ColumnRole::Value as i32,
                name: "c6".to_string(),
            },
            ColumnDescriptor {
                role: ColumnRole::Changed as i32,
                name: "c7".to_string(),
            },
            ColumnDescriptor {
                role: ColumnRole::Value as i32,
                name: "c7".to_string(),
            },
            ColumnDescriptor {
                role: ColumnRole::Changed as i32,
                name: "c9".to_string(),
            },
            ColumnDescriptor {
                role: ColumnRole::Value as i32,
                name: "c9".to_string(),
            },
            ColumnDescriptor {
                role: ColumnRole::Changed as i32,
                name: "c10".to_string(),
            },
            ColumnDescriptor {
                role: ColumnRole::Value as i32,
                name: "c10".to_string(),
            },
        ];

        // UPDATE, INSERT
        let lower_rows = 7;
        let lower_batch = RecordBatch::try_new(
            lower_schema.clone(),
            vec![
                // Insert 3 rows, Update 3 rows and delete 1 row
                Arc::new(Int32Array::from(vec![
                    None,
                    None,
                    None,
                    Some(4),
                    Some(6),
                    Some(8),
                    Some(10),
                ])),
                Arc::new(Int32Array::from(vec![
                    Some(1),
                    Some(2),
                    Some(3),
                    Some(5),
                    Some(7),
                    Some(9),
                    None,
                ])),
                Arc::new(StringArray::from(vec!["lower_c3"; lower_rows])),
                Arc::new(StringArray::from(vec!["lower_c5"; lower_rows])),
                Arc::new(BooleanArray::from(vec![true; lower_rows])),
                Arc::new(StringArray::from(vec!["lower_c6"; lower_rows])),
                Arc::new(StringArray::from(vec!["lower_c7"; lower_rows])),
                Arc::new(BooleanArray::from(vec![true; lower_rows])),
                Arc::new(StringArray::from(vec!["lower_c8"; lower_rows])),
                Arc::new(BooleanArray::from(vec![true; lower_rows])),
                Arc::new(StringArray::from(vec!["lower_c10"; lower_rows])),
            ],
        )?;

        let upper_rows = 7;
        let upper_batch = RecordBatch::try_new(
            upper_schema.clone(),
            vec![
                // Insert 1 row,
                // Update 1 row inserted in lower, 1 row updated in lower and 1 other row,
                // Delete 1 row inserted in lower, 1 row updated in lower and 1 other row
                Arc::new(Int32Array::from(vec![
                    None,
                    Some(2),
                    Some(7),
                    Some(14),
                    Some(3),
                    Some(9),
                    Some(16),
                ])),
                Arc::new(Int32Array::from(vec![
                    Some(11),
                    Some(12),
                    Some(13),
                    Some(15),
                    None,
                    None,
                    None,
                ])),
                Arc::new(StringArray::from(vec!["upper_c4"; upper_rows])),
                Arc::new(StringArray::from(vec!["upper_c5"; upper_rows])),
                Arc::new(StringArray::from(vec!["upper_c6"; upper_rows])),
                Arc::new(BooleanArray::from(vec![false; upper_rows])),
                Arc::new(StringArray::from(vec!["upper_c7"; upper_rows])),
                Arc::new(BooleanArray::from(vec![false; upper_rows])),
                Arc::new(StringArray::from(vec!["upper_c9"; upper_rows])),
                Arc::new(BooleanArray::from(vec![false; upper_rows])),
                Arc::new(StringArray::from(vec!["upper_c10"; upper_rows])),
            ],
        )?;

        let provider = MemTable::try_new(lower_schema.clone(), vec![vec![lower_batch]])?;
        let lower_df = DataFrame::new(
            ctx.inner.state(),
            LogicalPlanBuilder::scan(
                LOWER_SYNC,
                provider_as_source(Arc::new(provider)),
                None,
            )?
            .build()?,
        );

        let (_, merged_df) = sync_mgr.merge_syncs(
            &SyncSchema::try_new(lower_column_descriptors, lower_schema)?,
            lower_df,
            &SyncSchema::try_new(upper_column_descriptors, upper_schema)?,
            upper_batch,
        )?;

        // Pre-sort the merged batch to keep the order stable
        let results = merged_df
            .sort(vec![
                col("old_pk_c1").sort(true, true),
                col("new_pk_c1").sort(true, true),
            ])?
            .collect()
            .await?;

        let expected = [
            "+-----------+-----------+------------+----------+----------+------------+----------+----------+------------+----------+-------------+-----------+------------+----------+------------+------------+----------+",
            "| old_pk_c1 | new_pk_c1 | changed_c3 | value_c3 | value_c5 | changed_c6 | value_c6 | value_c7 | changed_c8 | value_c8 | changed_c10 | value_c10 | changed_c4 | value_c4 | changed_c7 | changed_c9 | value_c9 |",
            "+-----------+-----------+------------+----------+----------+------------+----------+----------+------------+----------+-------------+-----------+------------+----------+------------+------------+----------+",
            "|           |           | true       | lower_c3 | upper_c5 | true       | upper_c6 | upper_c7 | false      | lower_c8 | false       | upper_c10 | true       | upper_c4 | false      | false      | upper_c9 |",
            "|           | 1         | true       | lower_c3 | lower_c5 | true       | lower_c6 | lower_c7 | true       | lower_c8 | true        | lower_c10 | false      |          | true       | false      |          |",
            "|           | 11        | false      |          | upper_c5 | true       | upper_c6 | upper_c7 | false      |          | false       | upper_c10 | true       | upper_c4 | false      | false      | upper_c9 |",
            "|           | 12        | true       | lower_c3 | upper_c5 | true       | upper_c6 | upper_c7 | false      | lower_c8 | false       | upper_c10 | true       | upper_c4 | false      | false      | upper_c9 |",
            "| 4         | 5         | true       | lower_c3 | lower_c5 | true       | lower_c6 | lower_c7 | true       | lower_c8 | true        | lower_c10 | false      |          | true       | false      |          |",
            "| 6         | 13        | true       | lower_c3 | upper_c5 | true       | upper_c6 | upper_c7 | false      | lower_c8 | false       | upper_c10 | true       | upper_c4 | false      | false      | upper_c9 |",
            "| 8         |           | true       | lower_c3 | upper_c5 | true       | upper_c6 | upper_c7 | false      | lower_c8 | false       | upper_c10 | true       | upper_c4 | false      | false      | upper_c9 |",
            "| 10        |           | true       | lower_c3 | lower_c5 | true       | lower_c6 | lower_c7 | true       | lower_c8 | true        | lower_c10 | false      |          | true       | false      |          |",
            "| 14        | 15        | false      |          | upper_c5 | true       | upper_c6 | upper_c7 | false      |          | false       | upper_c10 | true       | upper_c4 | false      | false      | upper_c9 |",
            "| 16        |           | false      |          | upper_c5 | true       | upper_c6 | upper_c7 | false      |          | false       | upper_c10 | true       | upper_c4 | false      | false      | upper_c9 |",
            "+-----------+-----------+------------+----------+----------+------------+----------+----------+------------+----------+-------------+-----------+------------+----------+------------+------------+----------+",
        ];
        assert_batches_eq!(expected, &results);

        Ok(())
    }

    #[rstest]
    #[case(vec![(vec![1, 2], vec![1, 2])])]
    #[case(vec![(vec![1, 2], vec![2, 1]), (vec![1, 2], vec![2, 1])])]
    #[case(vec![
        (vec![2], vec![3]),
        (vec![1], vec![2]),
        (vec![3], vec![1]),
        (vec![2], vec![3]),
        (vec![1], vec![2]),
        (vec![3], vec![1])])
    ]
    #[tokio::test]
    async fn test_sync_pk_cycles(
        #[case] pk_cycle: Vec<(Vec<i32>, Vec<i32>)>,
    ) -> SyncResult<()> {
        let ctx = Arc::new(in_memory_context().await);
        let mut sync_mgr = SeafowlDataSyncWriter::new(ctx.clone());

        ctx.plan_query(
            "CREATE TABLE test_table(c1 INT, c2 INT) AS VALUES (1, 1), (2, 2)",
        )
        .await?;
        let table_uuid = ctx.get_table_uuid("test_table").await?;

        // Ensure original content
        let plan = ctx.plan_query("SELECT * FROM test_table").await.unwrap();
        let results = ctx.collect(plan).await.unwrap();

        let expected = [
            "+----+----+",
            "| c1 | c2 |",
            "+----+----+",
            "| 1  | 1  |",
            "| 2  | 2  |",
            "+----+----+",
        ];
        assert_batches_eq!(expected, &results);

        let schema = Arc::new(Schema::new(vec![
            Field::new("old_c1", DataType::Int32, true),
            Field::new("new_c1", DataType::Int32, true),
            Field::new("changed_c2", DataType::Boolean, true),
            Field::new("value_c2", DataType::Int32, true),
        ]));

        let column_descriptors = vec![
            ColumnDescriptor {
                role: ColumnRole::OldPk as i32,
                name: "c1".to_string(),
            },
            ColumnDescriptor {
                role: ColumnRole::NewPk as i32,
                name: "c1".to_string(),
            },
            ColumnDescriptor {
                role: ColumnRole::Changed as i32,
                name: "c2".to_string(),
            },
            ColumnDescriptor {
                role: ColumnRole::Value as i32,
                name: "c2".to_string(),
            },
        ];

        let sync_schema = SyncSchema::try_new(column_descriptors, schema.clone())?;

        // Enqueue all syncs
        let log_store = ctx
            .get_internal_object_store()
            .unwrap()
            .get_log_store(&table_uuid.to_string());

        // Cycle through the PKs, to end up in the same place as at start
        for (old_pks, new_pks) in pk_cycle {
            sync_mgr.enqueue_sync(
                log_store.clone(),
                None,
                A.to_string(),
                sync_schema.clone(),
                vec![RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int32Array::from(old_pks.clone())),
                        Arc::new(Int32Array::from(new_pks)),
                        Arc::new(BooleanArray::from(vec![false; old_pks.len()])),
                        Arc::new(Int32Array::from(vec![None; old_pks.len()])),
                    ],
                )?],
            )?;
        }

        sync_mgr.flush_syncs(log_store.root_uri()).await?;

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
            "| 1  | 1  |",
            "| 2  | 2  |",
            "+----+----+",
        ];
        assert_batches_eq!(expected, &results);

        Ok(())
    }
}
