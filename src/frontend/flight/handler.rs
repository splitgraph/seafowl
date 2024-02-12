use crate::context::SeafowlContext;
use arrow_flight::sql::metadata::{SqlInfoData, SqlInfoDataBuilder};
use arrow_flight::sql::SqlInfo;
use arrow_schema::SchemaRef;
use dashmap::DashMap;
use datafusion::common::Result;
use datafusion::execution::SendableRecordBatchStream;
use datafusion_common::DataFusionError;
use lazy_static::lazy_static;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::metadata::MetadataMap;
use tonic::Status;

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
}

impl SeafowlFlightHandler {
    pub fn new(context: Arc<SeafowlContext>) -> Self {
        Self {
            context,
            results: Arc::new(Default::default()),
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

        let plan = ctx.plan_query(query).await?;
        let batch_stream = ctx.execute_stream(plan).await?;
        let schema = batch_stream.schema();

        self.results.insert(query_id, Mutex::new(batch_stream));

        Ok(schema)
    }

    // Get a specific stream from the map
    pub async fn fetch_stream(
        &self,
        query_id: String,
    ) -> core::result::Result<SendableRecordBatchStream, Status> {
        let (_, batch_stream_mutex) =
            self.results
                .remove(&query_id)
                .ok_or(Status::not_found(format!(
                    "No results found for query id {query_id}"
                )))?;

        Ok(batch_stream_mutex.into_inner())
    }
}
