use crate::context::SeafowlContext;
use arrow::record_batch::RecordBatch;
use arrow_flight::sql::metadata::{SqlInfoData, SqlInfoDataBuilder};
use arrow_flight::sql::SqlInfo;
use arrow_schema::SchemaRef;
use clade::flight::{DataPutCommand, DataPutResult};
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use datafusion::common::Result;
use datafusion::execution::SendableRecordBatchStream;
use datafusion_common::DataFusionError;
use deltalake::kernel::Schema as DeltaSchema;
use deltalake::operations::create::CreateBuilder;
use deltalake::DeltaTable;
use lazy_static::lazy_static;
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::metadata::MetadataMap;
use tonic::Status;
use tracing::{debug, error, info};
use url::Url;

pub const SEAFOWL_PUT_DATA_UD_FLAG: &str = "__seafowl_ud";
const SEAFOWL_PUT_DATA_ORIGIN: &str = "origin";
const SEAFOWL_PUT_DATA_SEQUENCE_NUMBER: &str = "sequence";

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
    pub writes: Arc<DashMap<String, (u64, Vec<RecordBatch>)>>,
}

impl SeafowlFlightHandler {
    pub fn new(context: Arc<SeafowlContext>) -> Self {
        Self {
            context,
            results: Arc::new(Default::default()),
            writes: Arc::new(Default::default()),
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

    pub async fn process_put_cmd(
        &self,
        cmd: DataPutCommand,
        mut batches: Vec<RecordBatch>,
    ) -> Result<DataPutResult> {
        let store_loc = cmd.store.unwrap().clone();
        let log_store = self
            .context
            .metastore
            .object_stores
            .get_log_store_for_table(
                Url::parse(&store_loc.location).map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Couldn't parse put location: {e}"
                    ))
                })?,
                store_loc.options,
                cmd.path,
            )?;

        let url = log_store.root_uri();
        // Check if a table exists yet in the provided location, and if not create one
        let dur_seq = if !log_store.is_delta_table_location().await? {
            let schema = batches.first().unwrap().schema();
            let delta_schema = DeltaSchema::try_from(schema.as_ref())?;

            debug!(
                "Creating new Delta table at location: {}",
                log_store.root_uri()
            );
            CreateBuilder::new()
                .with_log_store(log_store)
                .with_columns(delta_schema.fields().clone())
                .with_comment(format!("Created by Seafowl {}", env!("CARGO_PKG_VERSION")))
                .with_metadata([
                    (
                        SEAFOWL_PUT_DATA_ORIGIN.to_string(),
                        Value::String(cmd.origin),
                    ),
                    (
                        SEAFOWL_PUT_DATA_SEQUENCE_NUMBER.to_string(),
                        Value::Number(cmd.sequence_number.into()),
                    ),
                ])
                .await?;
            None
        } else {
            let mut table = DeltaTable::new(log_store, Default::default());
            table.load().await?;

            // TODO: handle all edge cases with missing/un-parsable sequence numbers
            let commit_infos = table.history(Some(1)).await?;
            match commit_infos
                .last()
                .expect("Table has non-zero comits")
                .info
                .get(SEAFOWL_PUT_DATA_SEQUENCE_NUMBER)
            {
                Some(Value::Number(seq)) => seq.as_u64(),
                _ => None,
            }
        };

        let num_rows = batches
            .iter()
            .fold(0, |rows, batch| rows + batch.num_rows());
        if num_rows == 0 {
            debug!("Received empty batches, returning current sequence numbers");

            let dur_seq = dur_seq.unwrap_or(0);
            let mem_seq = self
                .writes
                .get(&url)
                .map(|r| r.value().0)
                .unwrap_or(dur_seq);

            let result = DataPutResult {
                accepted: false,
                memory_sequence_number: mem_seq,
                durable_sequence_number: dur_seq,
            };
            return Ok(result);
        }

        debug!("Processing data change with {num_rows} rows for url {url}");
        let _size = batches
            .iter()
            .fold(0, |bytes, batch| bytes + batch.get_array_memory_size());

        match self.writes.entry(url) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().1.append(&mut batches);
                let result = DataPutResult {
                    accepted: true,
                    memory_sequence_number: cmd.sequence_number,
                    durable_sequence_number: 0,
                };
                Ok(result)
            }
            Entry::Vacant(entry) => {
                entry.insert((cmd.sequence_number, batches));
                let result = DataPutResult {
                    accepted: true,
                    memory_sequence_number: cmd.sequence_number,
                    durable_sequence_number: 0,
                };
                Ok(result)
            }
        }
    }
}
