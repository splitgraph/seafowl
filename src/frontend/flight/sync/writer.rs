use arrow::array::RecordBatch;
use arrow_schema::{SchemaBuilder, SchemaRef};
use clade::sync::ColumnRole;
use datafusion::datasource::{provider_as_source, TableProvider};
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_optimizer::pruning::PruningPredicate;
use datafusion::prelude::DataFrame;
use datafusion_common::{JoinType, Result, ScalarValue, ToDFSchema};
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::{col, is_null, lit, when, LogicalPlanBuilder};
use datafusion_expr::{is_true, Expr};
use deltalake::kernel::{Action, Remove, Schema};
use deltalake::logstore::LogStore;
use deltalake::operations::create::CreateBuilder;
use deltalake::protocol::{DeltaOperation, SaveMode};
use deltalake::DeltaTable;
use indexmap::IndexMap;
use itertools::Itertools;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::ops::Not;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tracing::{debug, info};

use crate::context::delta::plan_to_object_store;

use crate::context::SeafowlContext;
use crate::frontend::flight::handler::SEAFOWL_SYNC_DATA_SEQUENCE_NUMBER;
use crate::frontend::flight::sync::metrics::SyncMetrics;
use crate::frontend::flight::sync::schema::SyncSchema;
use crate::frontend::flight::sync::utils::{compact_batches, construct_qualifier};

pub(super) type Origin = String;
pub(super) type SequenceNumber = u64;
const SYNC_REF: &str = "sync_data";
const SYNC_JOIN_COLUMN: &str = "__sync_join";

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
    // All sequences kept in memory, queued up by insertion order, per origin,
    seqs: HashMap<Origin, IndexMap<SequenceNumber, DataSyncSequence>>,
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
#[derive(Debug, Clone)]
pub(super) struct DataSyncItem {
    // Identifier of the origin where the change stems from
    pub(super) origin: Origin,
    // Sequence number of this particular change and origin
    pub(super) sequence_number: SequenceNumber,
    // Old and new primary keys, changed and value columns
    pub(super) sync_schema: SyncSchema,
    // Record batch to replicate
    pub(super) batch: RecordBatch,
}

impl SeafowlDataSyncWriter {
    pub fn new(context: Arc<SeafowlContext>) -> Self {
        Self {
            context,
            seqs: Default::default(),
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
        sequence_number: SequenceNumber,
        origin: Origin,
        sync_schema: SyncSchema,
        last: bool,
        batches: Vec<RecordBatch>,
    ) -> Result<()> {
        let url = log_store.root_uri();

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

        // Compactify the batches and measure the time it took and the reduction in rows/size
        let (old_size, old_rows) = batches.iter().fold((0, 0), |(size, rows), batch| {
            (
                size + batch.get_array_memory_size(),
                rows + batch.num_rows(),
            )
        });
        self.metrics.request_bytes.increment(old_size as u64);
        self.metrics.request_rows.increment(old_rows as u64);
        let start = Instant::now();
        let batch = compact_batches(&sync_schema, batches)?;
        let duration = start.elapsed().as_secs();

        // Get new size and row count
        let size = batch.get_array_memory_size();
        let rows = batch.num_rows();

        let item = DataSyncItem {
            origin: origin.clone(),
            sequence_number,
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
        self.metrics.compaction_time.record(duration as f64);
        self.metrics
            .compacted_bytes
            .increment((old_size - size) as u64);
        self.metrics
            .compacted_rows
            .increment((old_rows - rows) as u64);
        self.metrics.in_memory_oldest.set(
            self.syncs
                .first()
                .map(|(_, v)| v.insertion_time as f64)
                .unwrap_or(0.0),
        );

        // Flag the sequence as volatile persisted for this origin if it is the last sync command
        if last {
            self.metrics.sequence_memory(&origin, sequence_number);
            // TODO: (when) shsould we be removing the memory sequence number?

            self.origin_memory.insert(origin, sequence_number);
        }

        Ok(())
    }

    async fn create_table(
        &self,
        log_store: Arc<dyn LogStore>,
        sync_schema: &SyncSchema,
    ) -> Result<DeltaTable> {
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

    pub async fn flush(&mut self) -> Result<()> {
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
            Some(url.clone())
        } else if self.size >= self.context.config.misc.sync_conf.max_in_memory_bytes {
            // Or if we're over the size limit flush the oldest entry
            self.syncs.first().map(|kv| kv.0.clone())
        } else if let Some((url, _)) = self.syncs.iter().find(|(_, entry)| {
            entry.syncs.len() >= self.context.config.misc.sync_conf.max_syncs_per_url
        }) {
            // Otherwise if there are pending syncs with more than a predefined number of calls
            // waiting to be flushed flush them.
            // This is a guard against hitting a stack overflow when applying syncs, since this
            // results in deeply nested plan trees that are known to be problematic for now:
            // - https://github.com/apache/datafusion/issues/9373
            // - https://github.com/apache/datafusion/issues/9375
            // TODO: Make inter-sync compaction work even when/in absence of sync schema match
            Some(url.clone())
        } else {
            None
        }
    }

    // Flush the table containing the oldest sync in memory
    async fn flush_syncs(&mut self, url: String) -> Result<()> {
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

        let last_sequence_number = entry.syncs.last().unwrap().sequence_number;

        let mut table = DeltaTable::new(log_store.clone(), Default::default());
        table.load().await?;

        if let Some(table_seq) = self.table_sequence(&table).await?
            && table_seq > last_sequence_number
        {
            // TODO 1: partial skipping if only a subset of syncs older than the committed sequence
            // TODO 2: persist the final flag to to enable >= comparison
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

        // Generate a qualifier expression for pruning partition files and filtering the base scan
        let qualifier = construct_qualifier(&entry.syncs)?;

        // Iterate through all syncs for this table and construct a full plan by applying each
        // individual sync
        let base_plan = LogicalPlanBuilder::scan_with_filters(
            SYNC_REF,
            provider_as_source(Arc::new(table.clone())),
            None,
            vec![qualifier.clone()],
        )?
        .build()?;

        let state = self
            .context
            .inner
            .state()
            .with_analyzer_rules(vec![])
            .with_optimizer_rules(vec![]);
        let mut sync_df = DataFrame::new(state, base_plan);

        for sync in &entry.syncs {
            sync_df = self.apply_sync(
                full_schema.clone(),
                sync_df,
                &sync.sync_schema,
                sync.batch.clone(),
            )?;
        }

        let input_plan = sync_df.create_physical_plan().await?;

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

        // Prune away files that are refuted by the qualifier
        actions.extend(self.get_removes(&qualifier, full_schema.clone(), &table)?);

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

    fn apply_sync(
        &self,
        full_schema: SchemaRef,
        input_df: DataFrame,
        sync_schema: &SyncSchema,
        data: RecordBatch,
    ) -> Result<DataFrame> {
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

        // Construct the sync dataframe out of the record batch
        let sync_df = self.context.inner.read_batch(data)?;

        // These differ since the physical column names are reflected in the ColumnDescriptor,
        // while logical column names are found in the arrow fields
        let (input_pk_cols, sync_pk_cols): (Vec<String>, Vec<String>) = sync_schema
            .map_columns(ColumnRole::OldPk, |c| {
                (c.name().clone(), c.field().name().clone())
            })
            .into_iter()
            .unzip();

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
                        is_null(col(SYNC_JOIN_COLUMN)),
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

        let input_df = input_df
            .with_column(SYNC_JOIN_COLUMN, lit(true))?
            .join(
                sync_df,
                JoinType::Full,
                &input_pk_cols
                    .iter()
                    .map(|pk| pk.as_str())
                    .collect::<Vec<_>>(),
                &sync_pk_cols
                    .iter()
                    .map(|pk| pk.as_str())
                    .collect::<Vec<_>>(),
                None,
            )?
            .filter(old_pk_nulls.not().and(new_pk_nulls).not())? // Remove deletes
            .select(projection)?;

        Ok(input_df)
    }

    // Get a list of files that will be scanned and removed based on the pruning qualifier.
    fn get_removes(
        &self,
        predicate: &Expr,
        full_schema: SchemaRef,
        table: &DeltaTable,
    ) -> Result<Vec<Action>> {
        let prune_expr = create_physical_expr(
            predicate,
            &full_schema.clone().to_dfschema()?,
            &ExecutionProps::new(),
        )?;
        let pruning_predicate =
            PruningPredicate::try_new(prune_expr, full_schema.clone())?;
        let snapshot = table.snapshot()?;
        let prune_map = pruning_predicate.prune(snapshot)?;

        Ok(snapshot
            .file_actions()?
            .iter()
            .zip(prune_map)
            .filter_map(|(add, keep)| {
                if keep {
                    Some(Action::Remove(Remove {
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
                    }))
                } else {
                    None
                }
            })
            .collect::<Vec<Action>>())
    }

    // Remove the pending location from a sequence for all syncs in the collection
    fn remove_sequence_locations(
        &mut self,
        url: String,
        orseq: Vec<(Origin, SequenceNumber)>,
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
            self.metrics.in_memory_bytes.decrement(sync.size as f64);
            self.metrics.in_memory_rows.decrement(sync.rows as f64);
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
    use crate::frontend::flight::sync::schema::SyncSchema;
    use crate::frontend::flight::sync::writer::{SeafowlDataSyncWriter, SequenceNumber};
    use arrow::{array::RecordBatch, util::data_gen::create_random_batch};
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use clade::sync::{ColumnDescriptor, ColumnRole};
    use rand::Rng;
    use rstest::rstest;
    use std::collections::HashMap;

    use std::sync::Arc;

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
    static FLUSH: (&str, &str, i64) = ("__flush", "-1", -1);

    #[rstest]
    #[case::basic(
        &[(T1, A, 1), (T2, A, 2), (T1, A, 3), FLUSH, FLUSH],
        vec![vec![Some(1), Some(3)]]
    )]
    #[case::basic_2_origins(
        &[(T1, A, 1), (T2, B, 1001), (T1, A, 3), FLUSH, FLUSH],
        vec![
            vec![Some(3), Some(3)],
            vec![None, Some(1001)],
        ]
    )]
    #[case::doc_example(
        &[(T1, A, 1), (T2, A, 1), (T3, A, 2), (T1, A, 3), (T2, A, 3), FLUSH, FLUSH, FLUSH],
        vec![vec![None, Some(1), Some(3)]]
    )]
    #[case::staircase(
        &[(T1, A, 1), (T2, A, 1), (T3, A, 1), (T1, A, 2), (T2, A, 2), (T3, A, 2),
            FLUSH, FLUSH, FLUSH],
        vec![vec![None, None, Some(2)]]
    )]
    #[case::staircase_2_origins(
        &[(T1, A, 1), (T2, A, 1), (T3, B, 1001), (T1, B, 1002), (T2, A, 2), (T3, A, 2),
            FLUSH, FLUSH, FLUSH],
        vec![
            vec![None, Some(1), Some(2)],
            vec![None, None, Some(1002)],
        ]
        )]
    #[case::long_sequence(
        &[(T1, A, 1), (T1, A, 1), (T1, A, 1), (T1, A, 1), (T2, A, 1),
            (T2, A, 2), (T2, A, 2), (T2, A, 2), (T3, A, 2), (T3, A, 3),
            (T3, A, 3), (T1, A, 4), (T3, A, 4), (T1, A, 4), (T3, A, 4),
            FLUSH, FLUSH, FLUSH],
        vec![vec![None, Some(1), Some(4)]]
    )]
    #[case::long_sequence_mid_flush(
        &[(T1, A, 1), (T1, A, 1), (T1, A, 1), FLUSH, (T1, A, 1), (T2, A, 1),
            (T2, A, 2), (T2, A, 2), FLUSH, (T2, A, 2), (T3, A, 2), FLUSH, (T3, A, 3),
            FLUSH, (T3, A, 3), (T1, A, 4), (T3, A, 4), (T1, A, 4), FLUSH, (T3, A, 4),
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
        #[case] table_sequence: &[(&str, &str, i64)],
        #[case] mut durable_sequences: Vec<Vec<Option<u64>>>,
    ) {
        let ctx = Arc::new(in_memory_context().await);
        let mut sync_mgr = SeafowlDataSyncWriter::new(ctx.clone());
        let (arrow_schema, sync_schema) = sync_schema();

        let mut mem_seq = HashMap::from([(A.to_string(), None), (B.to_string(), None)]);
        let mut dur_seq = HashMap::from([(A.to_string(), None), (B.to_string(), None)]);

        // Start enqueueing syncs, flushing them and checking the memory sequence in-between
        for (sync_no, (table_name, origin, sequence)) in table_sequence.iter().enumerate()
        {
            if (*table_name, *origin, *sequence) == FLUSH {
                // Flush and assert on the next expected durable sequence
                let url = sync_mgr.syncs.first().unwrap().0.clone();
                sync_mgr.flush_syncs(url).await.unwrap();

                for (o, durs) in durable_sequences.iter_mut().enumerate() {
                    let origin = if o == 0 { A.to_string() } else { B.to_string() };
                    // Update expected durable sequences for this origins
                    dur_seq.insert(origin.clone(), durs.remove(0));

                    assert_eq!(
                        sync_mgr.stored_sequences(&origin),
                        (mem_seq[&origin], dur_seq[&origin]),
                        "Unexpected flush memory/durable sequence; \nseqs {:?}",
                        sync_mgr.seqs,
                    );
                }
                continue;
            }

            let log_store = ctx.internal_object_store.get_log_store(table_name);
            let origin: String = (*origin).to_owned();

            // Determine whether this is the last sync of the sequence, i.e. are there no upcoming
            // syncs with the same sequence number from this origin?
            let last = !table_sequence
                .iter()
                .skip(sync_no + 1)
                .any(|&(_, o, s)| *sequence == s && o == origin);

            sync_mgr
                .enqueue_sync(
                    log_store,
                    *sequence as SequenceNumber,
                    origin.clone(),
                    sync_schema.clone(),
                    last,
                    random_batches(arrow_schema.clone()),
                )
                .unwrap();

            // If this is the last sync in the sequence then it should be reported as in-memory
            if last {
                mem_seq.insert(origin.clone(), Some(*sequence as SequenceNumber));
            }

            assert_eq!(
                sync_mgr.stored_sequences(&origin),
                (mem_seq[&origin], dur_seq[&origin]),
                "Unexpected enqueue memory/durable sequence; \nseqs {:?}",
                sync_mgr.seqs,
            );
        }

        // Ensure everything has been flushed from memory
        assert!(sync_mgr.seqs.is_empty());
        assert!(sync_mgr.syncs.is_empty());
        assert_eq!(sync_mgr.size, 0);
    }
}
