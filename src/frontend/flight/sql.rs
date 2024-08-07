use crate::catalog::memory::MemoryStore;
use crate::frontend::flight::handler::{
    SeafowlFlightHandler, SEAFOWL_SQL_DATA, SEAFOWL_SYNC_CALL_MAX_ROWS,
};
use crate::frontend::flight::sync::schema::SyncSchema;
use crate::frontend::flight::sync::SyncError;
use arrow::record_batch::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::sql::server::{FlightSqlService, PeekableFlightDataStream};
use arrow_flight::sql::{
    Any, Command, CommandGetSqlInfo, CommandStatementQuery, SqlInfo, TicketStatementQuery,
};
use arrow_flight::{
    FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse,
    Ticket,
};
use async_trait::async_trait;
use clade::{schema::InlineMetastoreCommandStatementQuery, sync::DataSyncCommand};
use futures::Stream;
use futures::StreamExt;
use futures::TryStreamExt;
use prost::Message;
use std::pin::Pin;
use tonic::metadata::MetadataValue;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, warn};
use uuid::Uuid;

#[async_trait]
impl FlightSqlService for SeafowlFlightHandler {
    type FlightService = Self;

    // Perform authentication; for now just pass-through everything
    async fn do_handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>,
        Status,
    > {
        debug!("Handshake request: {:?}", request.metadata());
        let result = HandshakeResponse {
            protocol_version: 0,
            payload: vec![].into(),
        };
        let output = futures::stream::iter(vec![Ok(result)]);

        let mut resp: Response<Pin<Box<dyn Stream<Item = Result<_, _>> + Send>>> =
            Response::new(Box::pin(output));

        // Include a dummy auth header, so that clients that expect it don't error out (e.g. Python's
        // adbc_driver_flightsql)
        let md = MetadataValue::try_from("Bearer empty")
            .map_err(|_| Status::invalid_argument("authorization not parsable"))?;
        resp.metadata_mut().insert("authorization", md);
        Ok(resp)
    }

    // Get metadata about the Flight SQL server
    async fn get_flight_info_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        debug!(
            "Flight SQL server metadata request: {:?}",
            request.metadata()
        );
        let flight_descriptor = request.into_inner();
        let ticket = Ticket::new(query.encode_to_vec());
        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        let flight_info = FlightInfo::new()
            .try_with_schema(query.into_builder(&SEAFOWL_SQL_DATA).schema().as_ref())
            .map_err(|e| Status::from_error(Box::new(e)))?
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);

        Ok(Response::new(flight_info))
    }

    // As per the spec, we should execute the query here
    // https://arrow.apache.org/docs/format/FlightSql.html#query-execution
    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        // Use a new UUID to fingerprint a query
        let query_id = Uuid::new_v4().to_string();

        debug!(
            "Executing query with id {query_id} for request {:?}:\n {}",
            request.metadata(),
            query.query,
        );
        let info = self
            .query_to_stream(&query.query, query_id.clone(), request, None)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let resp = Response::new(info);
        debug!("Results for query id {query_id} ready for streaming");
        Ok(resp)
    }

    // Fallback method for our custom commands
    async fn get_flight_info_fallback(
        &self,
        cmd: Command,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let inline_query: InlineMetastoreCommandStatementQuery = match cmd {
            Command::Unknown(message) => Message::decode(&*message.value).map_err(|err| {
                let err = format!("Couldn't decode command: {err}");
                warn!(err);
                Status::invalid_argument(err)
            })?,
            _ => unreachable!("The fallback method should only be called with `Command::Unknown` variant"),
        };

        let query_id = Uuid::new_v4().to_string();

        debug!(
            "Executing inlined query with id {query_id} for request {:?}:\n {}",
            request.metadata(),
            inline_query.query,
        );

        let memory_store = MemoryStore {
            schemas: inline_query.schemas.expect("Schema list provided"),
        };

        let info = self
            .query_to_stream(
                &inline_query.query,
                query_id.clone(),
                request,
                Some(memory_store),
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let resp = Response::new(info);
        debug!("Results for inlined query id {query_id} ready for streaming");
        Ok(resp)
    }

    // Fetch the result batch stream, convert to flight stream and return.
    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let query_id =
            String::from_utf8_lossy(ticket.statement_handle.as_ref()).to_string();
        debug!(
            "Fetching stream for query id {query_id}, request: {:?}",
            request.metadata()
        );
        let batch_stream = self.fetch_stream(&query_id).await?;
        let schema = batch_stream.schema();

        // The Flight encoder below expects a stream where the error type on the item is a
        // `FlightError`, hence we need to map the DF batch stream here
        let mapped_stream = batch_stream.map(|batch| {
            batch.map_err(|e| FlightError::from_external_error(Box::new(e)))
        });

        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(mapped_stream)
            .map_err(Status::from);

        debug!("Returning stream for query id {query_id}");
        Ok(Response::new(Box::pin(stream)))
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}

    async fn do_put_fallback(
        &self,
        request: Request<PeekableFlightDataStream>,
        message: Any,
    ) -> Result<Response<<Self as FlightService>::DoPutStream>, Status> {
        // Extract the command
        let cmd: DataSyncCommand = Message::decode(&*message.value).map_err(|err| {
            let err = format!("Couldn't decode command: {err}");
            warn!(err);
            Status::invalid_argument(err)
        })?;

        // Extract the batches
        let batches: Vec<RecordBatch> = FlightRecordBatchStream::new_from_flight_data(
            request.into_inner().map_err(|e| e.into()),
        )
        .try_collect()
        .await?;

        let sync_schema = if !batches.is_empty() {
            // Validate row count under prescribed limit
            if batches
                .iter()
                .fold(0, |rows, batch| rows + batch.num_rows())
                > SEAFOWL_SYNC_CALL_MAX_ROWS
            {
                let err = format!("Change contains more than max allowed {SEAFOWL_SYNC_CALL_MAX_ROWS} rows");
                warn!(err);
                return Err(Status::invalid_argument(err));
            }

            SyncSchema::try_new(
                cmd.column_descriptors.clone(),
                batches.first().unwrap().schema(),
            )
            .map_err(|err| {
                warn!("{err}");
                Status::invalid_argument(err.to_string())
            })?
        } else {
            SyncSchema::empty()
        };

        let put_result = self
            .process_sync_cmd(cmd.clone(), sync_schema, batches)
            .await
            .map_err(|e| {
                let err = format!("Failed processing DoPut for {}: {e}", cmd.path);
                warn!(err);
                match e {
                    SyncError::InvalidMessage { reason } => {
                        Status::invalid_argument(reason)
                    }
                    _ => Status::internal(err),
                }
            })?;

        Ok(Response::new(Box::pin(futures::stream::iter(vec![Ok(
            arrow_flight::PutResult {
                app_metadata: put_result.encode_to_vec().into(),
            },
        )]))))
    }
}
