use crate::frontend::flight::handler::{SeafowlFlightHandler, SEAFOWL_SQL_DATA};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::{
    CommandGetSqlInfo, CommandStatementQuery, ProstMessageExt, SqlInfo,
    TicketStatementQuery,
};
use arrow_flight::{
    FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse,
    Ticket,
};
use async_trait::async_trait;
use futures::Stream;
use futures::StreamExt;
use futures::TryStreamExt;
use prost::Message;
use std::pin::Pin;
use tonic::metadata::MetadataValue;
use tonic::{Request, Response, Status, Streaming};
use uuid::Uuid;

#[async_trait]
impl FlightSqlService for SeafowlFlightHandler {
    type FlightService = Self;

    // Perform authentication; for now just pass-through everything
    async fn do_handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>,
        Status,
    > {
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
        // TODO: Should we use something else here (and keep that in the results map)?
        let query_id = Uuid::new_v4().to_string();

        let schema = self
            .query_to_stream(&query.query, query_id.clone(), request.metadata())
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let ticket = TicketStatementQuery {
            statement_handle: query_id.into(),
        };

        let endpoint = FlightEndpoint::new()
            .with_ticket(Ticket::new(ticket.as_any().encode_to_vec()));

        let flight_info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(e.to_string()))?
            .with_endpoint(endpoint)
            .with_descriptor(request.into_inner());

        let resp = Response::new(flight_info);
        Ok(resp)
    }

    // Fetch the result batch stream, convert to flight stream and return.
    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let query_id =
            String::from_utf8_lossy(ticket.statement_handle.as_ref()).to_string();
        let batch_stream = self
            .fetch_stream(query_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
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

        Ok(Response::new(Box::pin(stream)))
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}
