use arrow::record_batch::RecordBatch;
use arrow_flight::sql::metadata::{SqlInfoData, SqlInfoDataBuilder};
use arrow_flight::sql::SqlInfo;
use arrow_schema::SchemaRef;
use clade::sync::{DataSyncCommand, DataSyncResult};
use dashmap::DashMap;
use datafusion::common::Result;
use datafusion::execution::SendableRecordBatchStream;
use datafusion_common::DataFusionError;
use deltalake::kernel::Schema;
use deltalake::operations::create::CreateBuilder;
use lazy_static::lazy_static;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};
use tonic::metadata::MetadataMap;
use tonic::Status;
use tracing::{debug, error, info};
use url::Url;

use crate::context::SeafowlContext;
use crate::frontend::flight::sync::SeafowlDataSyncManager;

pub const SEAFOWL_SYNC_DATA_UD_FLAG: &str = "__seafowl_ud";
pub const SEAFOWL_SYNC_DATA_SEQUENCE_NUMBER: &str = "sequence";
pub const SEAFOWL_SYNC_CALL_MAX_ROWS: usize = 65536;

lazy_static! {
    pub static ref SEAFOWL_SQL_DATA: SqlInfoData = {
        let mut builder = SqlInfoDataBuilder::new();
        // Server information
        builder.append(SqlInfo::FlightSqlServerName, "Seafowl Flight SQL Server");
        builder.append(SqlInfo::FlightSqlServerVersion, env!("VERGEN_GIT_SEMVER"));
        // 1.3 comes from https://github.com/apache/arrow/blob/f9324b79bf4fc1ec7e97b32e3cce16e75ef0f5e3/format/Schema.fbs#L24
        builder.append(SqlInfo::FlightSqlServerArrowVersion, "1.3");
        builder.build().unwrap()
    };
}

// This struct is responsible for fulfilling the Arrow Flight (SQL) contract
// by interacting with the context, and keeping track of the relevant state.
// Note that the `Mutex` below is needed solely because `FlightSqlService`
// has a `Sync` trait bound, so we need to employ a synchronisation mechanism.
pub(super) struct SeafowlFlightHandler {
    pub context: Arc<SeafowlContext>,
    pub results: Arc<DashMap<String, Mutex<SendableRecordBatchStream>>>,
    sync_manager: Arc<RwLock<SeafowlDataSyncManager>>,
}

impl SeafowlFlightHandler {
    pub fn new(context: Arc<SeafowlContext>) -> Self {
        Self {
            context: context.clone(),
            results: Arc::new(Default::default()),
            sync_manager: Arc::new(RwLock::new(SeafowlDataSyncManager::new(context))),
        }
    }

    // Plan and execute the query, persisting the resulting stream handle in memory
    pub async fn query_to_stream(
        &self,
        query: &str,
        query_id: String,
        metadata: &MetadataMap,
    ) -> Result<SchemaRef> {
        let ctx = if let Some(search_path) = metadata.get("search-path") {
            self.context.scope_to_schema(
                search_path
                    .to_str()
                    .map_err(|e| DataFusionError::Execution(format!(
                        "Couldn't parse search path from header value {search_path:?}: {e}"
                    )))?
                    .to_string(),
            )
        } else {
            self.context.clone()
        };

        let plan = ctx
            .plan_query(query)
            .await
            .inspect_err(|err| info!("Error planning query id {query_id}: {err}"))?;
        let batch_stream = ctx
            .execute_stream(plan)
            .await
            .inspect_err(|err| info!("Error executing query id {query_id}: {err}"))?;
        let schema = batch_stream.schema();

        self.results.insert(query_id, Mutex::new(batch_stream));

        Ok(schema)
    }

    // Get a specific stream from the map
    pub async fn fetch_stream(
        &self,
        query_id: &str,
    ) -> core::result::Result<SendableRecordBatchStream, Status> {
        let (_, batch_stream_mutex) = self.results.remove(query_id).ok_or_else(|| {
            error!("No results found for query id {query_id}");
            Status::not_found(format!("No results found for query id {query_id}"))
        })?;

        Ok(batch_stream_mutex.into_inner())
    }

    pub async fn process_sync_cmd(
        &self,
        cmd: DataSyncCommand,
        batches: Vec<RecordBatch>,
    ) -> Result<DataSyncResult> {
        let log_store = match cmd.store {
            None => self.context.internal_object_store.get_log_store(&cmd.path),
            Some(store_loc) => self
                .context
                .metastore
                .object_stores
                .get_log_store_for_table(
                    Url::parse(&store_loc.location).map_err(|e| {
                        DataFusionError::Execution(format!(
                            "Couldn't parse sync location: {e}"
                        ))
                    })?,
                    store_loc.options,
                    cmd.path,
                )?,
        };

        let url = log_store.root_uri();

        if !log_store.is_delta_table_location().await? {
            // If it's not a delta table yet create one first.
            // Get the actual table schema by removing the `SEAFOWL_SYNC_DATA_UD_FLAG` column
            // from the first sync.
            let schema = batches.first().unwrap().schema();
            let idxs = (0..schema.all_fields().len() - 1).collect::<Vec<usize>>();
            let schema = schema.project(&idxs)?;

            let delta_schema = Schema::try_from(&schema)?;

            debug!("Creating new Delta table at location: {url}",);
            CreateBuilder::new()
                .with_log_store(log_store.clone())
                .with_columns(delta_schema.fields().clone())
                .with_comment(format!("Synced by Seafowl {}", env!("CARGO_PKG_VERSION")))
                .await?;
        }

        let num_rows = batches
            .iter()
            .fold(0, |rows, batch| rows + batch.num_rows());
        if num_rows == 0 {
            // Get the current volatile and durable sequence numbers
            debug!("Received empty batches, returning current sequence numbers");
            let (mem_seq, dur_seq) =
                self.sync_manager.read().await.stored_sequences(&cmd.origin);
            return Ok(DataSyncResult {
                accepted: true,
                memory_sequence_number: mem_seq,
                durable_sequence_number: dur_seq,
            });
        }

        debug!("Processing data change with {num_rows} rows for url {url}");
        // TODO: make timeout configurable
        match tokio::time::timeout(Duration::from_secs(3), self.sync_manager.write())
            .await
        {
            Ok(mut sync_manager) => {
                let (mem_seq, dur_seq) = sync_manager
                    .sync_batches(
                        log_store,
                        cmd.sequence_number,
                        cmd.origin,
                        cmd.pk_column,
                        cmd.last,
                        batches,
                    )
                    .await?;
                Ok(DataSyncResult {
                    accepted: true,
                    memory_sequence_number: mem_seq,
                    durable_sequence_number: dur_seq,
                })
            }
            Err(_) => {
                debug!("Timeout waiting for data sync write lock for url {url}");
                Ok(DataSyncResult {
                    accepted: false,
                    memory_sequence_number: None,
                    durable_sequence_number: None,
                })
            }
        }
    }
}
