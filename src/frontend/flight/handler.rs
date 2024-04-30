use crate::context::delta::CreateDeltaTableDetails;
use crate::context::SeafowlContext;
use arrow::record_batch::RecordBatch;
use arrow_flight::sql::metadata::{SqlInfoData, SqlInfoDataBuilder};
use arrow_flight::sql::SqlInfo;
use arrow_schema::SchemaRef;
use clade::flight::do_put_result::AckType;
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use datafusion::common::Result;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::{DataFusionError, TableReference};
use lazy_static::lazy_static;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::metadata::MetadataMap;
use tonic::Status;
use tracing::{error, info};

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
    pub writes: Arc<DashMap<String, Vec<RecordBatch>>>,
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

    pub async fn process_batches(
        &self,
        table: String,
        mut batches: Vec<RecordBatch>,
    ) -> Result<AckType> {
        match self.writes.entry(table.clone()) {
            Entry::Occupied(mut entry) => {
                let schema = batches.first().unwrap().schema().clone();
                entry.get_mut().append(&mut batches);

                if entry
                    .get()
                    .iter()
                    .fold(0, |rows, batch| rows + batch.num_rows())
                    > 3
                {
                    let batches = entry.remove();

                    let table_ref = TableReference::bare(table.clone());
                    self.context
                        .create_delta_table(
                            table_ref,
                            CreateDeltaTableDetails::EmptyTable(schema.as_ref().clone()),
                        )
                        .await?;

                    let plan: Arc<dyn ExecutionPlan> =
                        Arc::new(MemoryExec::try_new(&[batches], schema, None).unwrap());

                    self.context.plan_to_delta_table(table, &plan).await?;
                    Ok(AckType::Durable)
                } else {
                    Ok(AckType::Memory)
                }
            }
            Entry::Vacant(entry) => {
                entry.insert(batches);
                Ok(AckType::Memory)
            }
        }
    }
}
