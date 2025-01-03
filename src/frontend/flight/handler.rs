use crate::catalog::memory::MemoryStore;
use crate::catalog::metastore::Metastore;
use arrow::record_batch::RecordBatch;
use arrow_flight::sql::metadata::{SqlInfoData, SqlInfoDataBuilder};
use arrow_flight::sql::{ProstMessageExt, SqlInfo, TicketStatementQuery};
use arrow_flight::{FlightDescriptor, FlightEndpoint, FlightInfo, Ticket};
use clade::schema::TableFormat;
use clade::sync::{DataSyncCommand, DataSyncResponse};
use dashmap::DashMap;
use datafusion::common::Result;
use datafusion::execution::SendableRecordBatchStream;
use datafusion_common::DataFusionError;
use iceberg::io::FileIO;
use lazy_static::lazy_static;
use object_store_factory::object_store_opts_to_file_io_props;
use prost::Message;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};
use tonic::{Request, Status};
use tracing::{debug, error, info};
use url::Url;

use crate::context::SeafowlContext;
use crate::sync::schema::SyncSchema;
use crate::sync::writer::SeafowlDataSyncWriter;
use crate::sync::{IcebergSyncTarget, LakehouseSyncTarget, SyncError, SyncResult};

lazy_static! {
    pub static ref SEAFOWL_SQL_DATA: SqlInfoData = {
        let mut builder = SqlInfoDataBuilder::new();
        // Server information
        builder.append(SqlInfo::FlightSqlServerName, "Seafowl Flight SQL Server");
        builder.append(SqlInfo::FlightSqlServerVersion, env!("VERGEN_RUSTC_SEMVER"));
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
    sync_writer: Arc<RwLock<SeafowlDataSyncWriter>>,
    // Denotes whether we're past the first sync response, thus indicating Seafowl (re)starts
    first_sync: Arc<AtomicBool>,
}

impl SeafowlFlightHandler {
    pub fn new(
        context: Arc<SeafowlContext>,
        sync_writer: Arc<RwLock<SeafowlDataSyncWriter>>,
    ) -> Self {
        Self {
            context: context.clone(),
            results: Arc::new(Default::default()),
            sync_writer,
            first_sync: Arc::new(AtomicBool::new(true)),
        }
    }

    // Plan and execute the query, persisting the resulting stream handle in memory
    pub async fn query_to_stream(
        &self,
        query: &str,
        query_id: String,
        request: Request<FlightDescriptor>,
        memory_store: Option<MemoryStore>,
    ) -> Result<FlightInfo> {
        let mut ctx = if let Some(search_path) = request.metadata().get("search-path") {
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

        if let Some(memory_store) = memory_store {
            // If metastore was inlined with the query use it for resolving tables/locations
            ctx = ctx.with_metastore(Arc::new(Metastore::new_from_memory(
                Arc::new(memory_store),
                ctx.metastore.object_stores.clone(),
            )));
        }

        let plan = ctx
            .plan_query(query)
            .await
            .inspect_err(|err| info!("Error planning query id {query_id}: {err}"))?;
        let batch_stream = ctx
            .execute_stream(plan)
            .await
            .inspect_err(|err| info!("Error executing query id {query_id}: {err}"))?;
        let schema = batch_stream.schema();

        self.results
            .insert(query_id.clone(), Mutex::new(batch_stream));

        // Issue a ticket for fetching the query stream
        let ticket = TicketStatementQuery {
            statement_handle: query_id.clone().into(),
        };

        let endpoint = FlightEndpoint::new()
            .with_ticket(Ticket::new(ticket.as_any().encode_to_vec()));

        let flight_info = FlightInfo::new()
            .try_with_schema(&schema)?
            .with_endpoint(endpoint)
            .with_descriptor(request.into_inner());

        Ok(flight_info)
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
        sync_schema: SyncSchema,
        batches: Vec<RecordBatch>,
    ) -> SyncResult<DataSyncResponse> {
        let first = self.first_sync.load(Ordering::SeqCst);
        if first {
            // We're past first response
            self.first_sync.store(false, Ordering::SeqCst);
        }

        let num_rows = batches
            .iter()
            .fold(0, |rows, batch| rows + batch.num_rows());

        // Short-circuit the "probing" request
        if num_rows == 0 && cmd.sequence_number.is_none() {
            // Get the current volatile and durable sequence numbers
            debug!("Received empty batches with no sequence number, returning current sequence numbers");
            let (mem_seq, dur_seq) =
                self.sync_writer.read().await.stored_sequences(&cmd.origin);
            return Ok(DataSyncResponse {
                accepted: true,
                memory_sequence_number: mem_seq,
                durable_sequence_number: dur_seq,
                first,
            });
        }

        let sync_target = match cmd.format() {
            TableFormat::Delta => {
                let log_store = match cmd.store {
                    None => self
                        .context
                        .get_internal_object_store()?
                        .get_log_store(&cmd.path),
                    Some(store_loc) => {
                        self.context
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
                            )
                            .await?
                    }
                };
                LakehouseSyncTarget::Delta(log_store)
            }
            TableFormat::Iceberg => {
                let (location, file_io) = match cmd.store {
                    None => {
                        return Err(SyncError::NotImplemented);
                    }
                    Some(store_loc) => {
                        let location = store_loc.location;
                        let options = store_loc.options;
                        let file_io_props = object_store_opts_to_file_io_props(&options);
                        let file_io = FileIO::from_path(&location)
                            .unwrap()
                            .with_props(file_io_props)
                            .build()?;
                        (location, file_io)
                    }
                };

                // Create the full path to table metadata by combining the object store location and
                // relative table metadata path
                let absolute_path = format!(
                    "{}/{}",
                    location.trim_end_matches("/"),
                    cmd.path.trim_start_matches("/")
                );
                LakehouseSyncTarget::Iceberg(IcebergSyncTarget {
                    file_io,
                    url: absolute_path.clone(),
                })
            }
        };

        let url = sync_target.get_url();
        let num_batches = batches.len();

        debug!("Processing data change with {num_rows} rows, {num_batches} batches, descriptor {sync_schema}, url {url} from origin {:?} at position {:?}",
	       cmd.origin,
	       cmd.sequence_number);
        match tokio::time::timeout(
            Duration::from_secs(self.context.config.misc.sync_conf.write_lock_timeout_s),
            self.sync_writer.write(),
        )
        .await
        {
            Ok(mut sync_writer) => {
                sync_writer.enqueue_sync(
                    sync_target,
                    cmd.sequence_number,
                    cmd.origin.clone(),
                    sync_schema,
                    batches,
                )?;

                sync_writer.flush().await?;

                let (mem_seq, dur_seq) = sync_writer.stored_sequences(&cmd.origin);
                Ok(DataSyncResponse {
                    accepted: true,
                    memory_sequence_number: mem_seq,
                    durable_sequence_number: dur_seq,
                    first,
                })
            }
            Err(_) => {
                debug!("Timeout waiting for data sync write lock for url {url}");
                Ok(DataSyncResponse {
                    accepted: false,
                    memory_sequence_number: None,
                    durable_sequence_number: None,
                    first,
                })
            }
        }
    }
}
