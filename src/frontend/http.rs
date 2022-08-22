use arrow::csv::ReaderBuilder;
use arrow::datatypes::Schema;
use arrow::error::ArrowError;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::ExecutionPlan;
use std::error::Error;
use std::io::Cursor;
use std::{convert::Infallible, net::SocketAddr, sync::Arc};
use warp::Rejection;

use arrow::json::LineDelimitedWriter;
use arrow::record_batch::RecordBatch;
use bytes::{BufMut, Bytes};
use datafusion::parquet::arrow::{ArrowReader, ParquetFileArrowReader};

use datafusion::physical_plan::memory::MemoryExec;
use datafusion::{
    datasource::DefaultTableSource,
    logical_plan::{LogicalPlan, PlanVisitor, TableScan},
};
use futures::{future, TryStreamExt};
use hex::encode;
use log::debug;
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use warp::multipart::{FormData, Part};
use warp::reply::Response;
use warp::{hyper::StatusCode, Filter, Reply};

use crate::auth::{token_to_principal, AccessPolicy, Action, UserContext};
use crate::config::schema::AccessSettings;
use crate::{
    config::schema::{str_to_hex_hash, HttpFrontend},
    context::SeafowlContext,
    data_types::TableVersionId,
    provider::SeafowlTable,
};

use super::http_utils::{handle_rejection, into_response, ApiError};

const QUERY_HEADER: &str = "X-Seafowl-Query";
const IF_NONE_MATCH: &str = "If-None-Match";
const ETAG: &str = "ETag";
const AUTHORIZATION: &str = "Authorization";
const BEARER_PREFIX: &str = "Bearer ";
// We have a very lax CORS on this, so we don't mind browsers
// caching it for as long as possible.
const CORS_MAXAGE: u32 = 86400;

#[derive(Default)]
struct ETagBuilderVisitor {
    table_versions: Vec<TableVersionId>,
}

impl PlanVisitor for ETagBuilderVisitor {
    type Error = Infallible;

    fn pre_visit(&mut self, plan: &LogicalPlan) -> Result<bool, Self::Error> {
        if let LogicalPlan::TableScan(TableScan { source, .. }) = plan {
            // TODO handle external Parquet tables too
            if let Some(default_table_source) =
                source.as_any().downcast_ref::<DefaultTableSource>()
            {
                if let Some(table) = default_table_source
                    .table_provider
                    .as_any()
                    .downcast_ref::<SeafowlTable>()
                {
                    self.table_versions.push(table.table_version_id)
                }
            }
        }
        Ok(true)
    }
}

fn plan_to_etag(plan: &LogicalPlan) -> String {
    let mut visitor = ETagBuilderVisitor::default();
    plan.accept(&mut visitor).unwrap();

    debug!("Extracted table versions: {:?}", visitor.table_versions);

    let mut hasher = Sha256::new();
    hasher.update(json!(visitor.table_versions).to_string());
    encode(hasher.finalize())
}

#[derive(Debug, Deserialize)]
struct QueryBody {
    query: String,
}

pub fn is_read_only(plan: &LogicalPlan) -> bool {
    !matches!(
        plan,
        LogicalPlan::CreateExternalTable(_)
            | LogicalPlan::CreateMemoryTable(_)
            | LogicalPlan::CreateView(_)
            | LogicalPlan::CreateCatalogSchema(_)
            | LogicalPlan::CreateCatalog(_)
            | LogicalPlan::DropTable(_)
            | LogicalPlan::Analyze(_)
            | LogicalPlan::Extension(_)
    )
}

/// Execute a physical plan and output its results to a JSON Lines format
async fn physical_plan_to_json(
    context: Arc<dyn SeafowlContext>,
    physical: Arc<dyn ExecutionPlan>,
) -> Result<Vec<u8>, DataFusionError> {
    let batches = context.collect(physical).await?;
    let mut buf = Vec::new();
    let mut writer = LineDelimitedWriter::new(&mut buf);
    writer
        .write_batches(&batches)
        .map_err(DataFusionError::ArrowError)?;
    writer.finish().map_err(DataFusionError::ArrowError)?;
    Ok(buf)
}

/// POST /q
pub async fn uncached_read_write_query(
    user_context: UserContext,
    query: String,
    context: Arc<dyn SeafowlContext>,
) -> Result<Vec<u8>, ApiError> {
    let logical = context.create_logical_plan(&query).await?;

    if !user_context.can_perform_action(if is_read_only(&logical) {
        Action::Read
    } else {
        Action::Write
    }) {
        return Err(ApiError::WriteForbidden);
    };

    let physical = context.create_physical_plan(&logical).await?;

    let buf = physical_plan_to_json(context, physical).await?;
    Ok(buf)
}

fn header_to_user_context(
    header: Option<String>,
    policy: &AccessPolicy,
) -> Result<UserContext, ApiError> {
    let token = header
        .map(|h| {
            if h.starts_with(BEARER_PREFIX) {
                Ok(h.trim_start_matches(BEARER_PREFIX).to_string())
            } else {
                Err(ApiError::InvalidAuthorizationHeader)
            }
        })
        .transpose()?;

    token_to_principal(token, policy).map(|principal| UserContext {
        principal,
        policy: policy.clone(),
    })
}

pub fn with_auth(
    policy: AccessPolicy,
) -> impl Filter<Extract = (UserContext,), Error = Rejection> + Clone {
    warp::header::optional::<String>(AUTHORIZATION).and_then(
        move |header: Option<String>| {
            future::ready(
                header_to_user_context(header, &policy).map_err(warp::reject::custom),
            )
        },
    )
}

// Disable the cached GET endpoint if we require auth for reads / they're disabled.
// (since caching + auth is yet another can of worms)
pub fn cached_read_query_authz(
    policy: AccessPolicy,
) -> impl Filter<Extract = (), Error = Rejection> + Clone {
    warp::any()
        .and_then(move || match policy.read {
            AccessSettings::Any => future::ok(()),
            _ => future::err(warp::reject::custom(ApiError::ReadOnlyEndpointDisabled)),
        })
        .untuple_one()
}

/// GET /q/[query hash]
pub async fn cached_read_query(
    query_hash: String,
    query: String,
    if_none_match: Option<String>,
    context: Arc<dyn SeafowlContext>,
) -> Result<Response, ApiError> {
    // Ignore dots at the end
    let query_hash = query_hash.split('.').next().unwrap();

    let hash_str = str_to_hex_hash(&query);

    debug!(
        "Received query: {}, URL hash {}, actual hash {}",
        query, query_hash, hash_str
    );

    // Verify the query hash matches the query
    if query_hash != hash_str {
        return Err(ApiError::HashMismatch(hash_str, query_hash.to_string()));
    };

    // Plan the query
    let plan = context.create_logical_plan(&query).await?;
    debug!("Query plan: {:?}", plan);

    // Write queries should come in as POST requests
    if !is_read_only(&plan) {
        return Err(ApiError::NotReadOnlyQuery);
    };

    // Pre-execution check: if ETags match, we don't need to re-execute the query
    let etag = plan_to_etag(&plan);
    debug!("ETag: {}, if-none-match header: {:?}", etag, if_none_match);

    if let Some(if_none_match) = if_none_match {
        if etag == if_none_match {
            return Ok(warp::reply::with_status(
                "NOT_MODIFIED",
                StatusCode::NOT_MODIFIED,
            )
            .into_response());
        }
    }

    // Guess we'll have to actually run the query
    let physical = context.create_physical_plan(&plan).await?;
    let buf = physical_plan_to_json(context, physical).await?;

    Ok(warp::reply::with_header(buf, ETAG, etag).into_response())
}

/// POST /upload/[schema]/[table]
pub async fn upload(
    schema_name: String,
    table_name: String,
    user_context: UserContext,
    form: FormData,
    context: Arc<dyn SeafowlContext>,
) -> Result<Response, ApiError> {
    if !user_context.can_perform_action(Action::Write) {
        return Err(ApiError::WriteForbidden);
    };
    let parts: Vec<Part> = form
        .try_collect()
        .await
        .map_err(ApiError::UploadBodyLoadError)?;

    let mut has_header = true;
    let mut csv_schema: Option<Schema> = None;
    let mut filename = String::new();
    for p in parts {
        if p.name() == "has_header" {
            let value_bytes =
                load_part(p).await.map_err(ApiError::UploadBodyLoadError)?;

            has_header = String::from_utf8(value_bytes)
                .map_err(|_| ApiError::UploadHasHeaderParseError)?
                .starts_with("true");
            debug!("Form part has_header is: {}", has_header);
        } else if p.name() == "schema" && p.content_type() == Some("application/json") {
            let value_bytes =
                load_part(p).await.map_err(ApiError::UploadBodyLoadError)?;

            csv_schema = Some(
                Schema::from(
                    &serde_json::from_slice::<serde_json::Value>(value_bytes.as_slice())
                        .map_err(ApiError::UploadSchemaDeserializationError)?,
                )
                .map_err(ApiError::UploadSchemaParseError)?,
            );
        } else if p.name() == "data" || p.name() == "file" {
            filename = p.filename().ok_or(ApiError::UploadMissingFile)?.to_string();

            // Load the file content from the request
            // TODO: we're actually buffering the entire file into memory here which is sub-optimal,
            // since we could be writing the contents of the stream out into a temp file on the disk
            // to have a smaller memory footprint. However, for this to be supported warp first
            // needs to support streaming here: https://github.com/seanmonstar/warp/issues/323
            let source = load_part(p).await.map_err(ApiError::UploadBodyLoadError)?;

            // Parse the schema and load the file contents into a vector of record batches
            let (schema, partition) =
                match filename.split('.').last().ok_or_else(|| {
                    ApiError::UploadMissingFilenameExtension(filename.clone())
                })? {
                    "csv" => load_csv_bytes(source, csv_schema.clone(), has_header)
                        .map_err(|e| ApiError::UploadFileLoadError(e.into()))?,
                    "parquet" => load_parquet_bytes(source)
                        .map_err(ApiError::UploadFileLoadError)?,
                    _ => return Err(ApiError::UploadUnsupportedFileFormat(filename)),
                };

            // Create an execution plan for yielding the record batches we just generated
            let execution_plan = Arc::new(MemoryExec::try_new(
                &[partition],
                Arc::new(schema.clone()),
                None,
            )?);

            // Execute the plan and persist objects as well as table/partition metadata
            context
                .plan_to_table(execution_plan, schema_name.clone(), table_name.clone())
                .await?;
        }
    }

    if filename.is_empty() {
        return Err(ApiError::UploadMissingFile);
    }

    Ok(warp::reply::with_status(Ok("done"), StatusCode::OK).into_response())
}

async fn load_part(p: Part) -> Result<Vec<u8>, warp::Error> {
    p.stream()
        .try_fold(Vec::new(), |mut vec, data| {
            vec.put(data);
            async move { Ok(vec) }
        })
        .await
}

fn load_csv_bytes(
    source: Vec<u8>,
    schema: Option<Schema>,
    has_header: bool,
) -> Result<(Schema, Vec<RecordBatch>), ArrowError> {
    // If the schema part wasn't specified we'll need to infer it
    let builder = match schema {
        Some(schema) => ReaderBuilder::new()
            .with_schema(Arc::new(schema))
            .has_header(has_header),
        None => ReaderBuilder::new()
            .infer_schema(None)
            .has_header(has_header),
    };

    let mut cursor = Cursor::new(source);
    let csv_reader = builder.build(&mut cursor)?;
    let schema = csv_reader.schema().as_ref().clone();
    let partition: Vec<RecordBatch> = csv_reader
        .into_iter()
        .collect::<Result<Vec<RecordBatch>, ArrowError>>()?;

    Ok((schema, partition))
}

/// Load a Parquet file from a byte vector and returns its schema/batches
fn load_parquet_bytes(
    source: Vec<u8>,
) -> Result<(Schema, Vec<RecordBatch>), Box<dyn Error + Send + Sync>> {
    // We have to return a boxed error here, since this could return either a
    // ParquetError or an ArrowError
    // (could also return an ApiError::UploadFileLoadError with a string?)
    let mut parquet_reader = ParquetFileArrowReader::try_new(Bytes::from(source))?;

    let schema = parquet_reader.get_schema()?;

    let partition: Vec<RecordBatch> = parquet_reader
        .get_record_reader(1024)?
        .collect::<Result<Vec<RecordBatch>, ArrowError>>()?;

    Ok((schema, partition))
}

pub fn filters(
    context: Arc<dyn SeafowlContext>,
    access_policy: AccessPolicy,
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    let cors = warp::cors()
        .allow_any_origin()
        .allow_headers(vec!["X-Seafowl-Query", "Authorization", "Content-Type"])
        .allow_methods(vec!["GET", "POST"])
        .max_age(CORS_MAXAGE);

    let log = warp::log(module_path!());

    // Cached read query
    let ctx = context.clone();
    let cached_read_query_route = warp::path!("q" / String)
        .and(warp::get())
        .and(cached_read_query_authz(access_policy.clone()))
        .and(
            // Extract the query either from the header or from the JSON body
            warp::header::<String>(QUERY_HEADER)
                .or(warp::body::json().map(|b: QueryBody| b.query))
                .unify(),
        )
        .and(warp::header::optional::<String>(IF_NONE_MATCH))
        .and(warp::any().map(move || ctx.clone()))
        .then(cached_read_query)
        .map(into_response);

    // Uncached read/write query
    let ctx = context.clone();
    let uncached_read_write_query_route = warp::path!("q")
        .and(warp::post())
        .and(with_auth(access_policy.clone()))
        .and(
            // Extract the query from the JSON body
            warp::body::json().map(|b: QueryBody| b.query),
        )
        .and(warp::any().map(move || ctx.clone()))
        .then(uncached_read_write_query)
        .map(into_response);

    // Upload endpoint
    let ctx = context.clone();
    let upload_route = warp::path!("upload" / String / String)
        .and(warp::post())
        .and(with_auth(access_policy))
        .and(warp::multipart::form())
        .and(warp::any().map(move || ctx.clone()))
        .then(upload)
        .map(into_response);

    cached_read_query_route
        .or(uncached_read_write_query_route)
        .or(upload_route)
        .with(cors)
        .with(log)
        .recover(handle_rejection)
}

pub async fn run_server(context: Arc<dyn SeafowlContext>, config: HttpFrontend) {
    let filters = filters(context, AccessPolicy::from_config(&config));

    let socket_addr: SocketAddr = format!("{}:{}", config.bind_host, config.bind_port)
        .parse()
        .expect("Error parsing the listen address");
    warp::serve(filters).run(socket_addr).await;
}

#[cfg(test)]
mod tests {

    use bytes::Bytes;

    use itertools::Itertools;

    use std::{collections::HashMap, sync::Arc};

    use warp::{Filter, Rejection, Reply};

    use warp::http::Response;
    use warp::{
        hyper::{header::IF_NONE_MATCH, StatusCode},
        test::request,
    };

    use crate::auth::AccessPolicy;

    use crate::{
        context::{test_utils::in_memory_context, SeafowlContext},
        frontend::http::{filters, ETAG, QUERY_HEADER},
    };

    use super::AUTHORIZATION;

    /// Build an in-memory context with a single table
    /// We implicitly assume here that this table is the only one in this context
    /// and has version ID 1 (otherwise the hashes won't match).
    async fn in_memory_context_with_single_table() -> Arc<dyn SeafowlContext> {
        let context = Arc::new(in_memory_context().await);

        context
            .collect(
                context
                    .plan_query("CREATE TABLE test_table(col_1 INTEGER)")
                    .await
                    .unwrap(),
            )
            .await
            .unwrap();

        context
            .collect(
                context
                    .plan_query("INSERT INTO test_table VALUES (1)")
                    .await
                    .unwrap(),
            )
            .await
            .unwrap();
        context
    }

    async fn in_memory_context_with_modified_table() -> Arc<dyn SeafowlContext> {
        let context = in_memory_context_with_single_table().await;
        context
            .collect(
                context
                    .plan_query("INSERT INTO test_table VALUES (2)")
                    .await
                    .unwrap(),
            )
            .await
            .unwrap();
        context
    }

    fn free_for_all() -> AccessPolicy {
        AccessPolicy::free_for_all()
    }

    const SELECT_QUERY: &str = "SELECT COUNT(*) AS c FROM test_table";
    const INSERT_QUERY: &str = "INSERT INTO test_table VALUES (2)";
    const CREATE_QUERY: &str = "CREATE TABLE other_test_table(col_1 INTEGER)";
    const SELECT_QUERY_HASH: &str =
        "7fbbf7dddfd330d03e5e08cc5885ad8ca823e1b56e7cbadd156daa0e21c288f6";
    const CREATE_QUERY_HASH: &str =
        "be185830b7db691f3ffd33c81a83bb4ed48e2411fc3fc500ee20b8ec7effb8a6";
    const V1_ETAG: &str =
        "038966de9f6b9a901b20b4c6ca8b2a46009feebe031babc842d43690c0bc222b";
    const V2_ETAG: &str =
        "06d033ece6645de592db973644cf7357255f24536ff7b03c3b2ace10736f7636";

    #[tokio::test]
    async fn test_get_cached_hash_mismatch() {
        let context = in_memory_context_with_single_table().await;
        let handler = filters(context, free_for_all());

        let resp = request()
            .method("GET")
            .path("/q/wrong-hash")
            .header(QUERY_HEADER, SELECT_QUERY)
            .reply(&handler)
            .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_cors() {
        let context = in_memory_context_with_single_table().await;
        let handler = filters(context, free_for_all());

        let resp = request()
            .method("OPTIONS")
            .path("/q/somehash")
            .header("Access-Control-Request-Method", "GET")
            .header("Access-Control-Request-Headers", "x-seafowl-query")
            .header("Origin", "https://example.org")
            .reply(&handler)
            .await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get("Access-Control-Allow-Origin").unwrap(),
            "https://example.org"
        );
        assert_eq!(
            resp.headers()
                .get("Access-Control-Allow-Methods")
                .unwrap()
                .to_str()
                .unwrap()
                .split(',')
                .map(|s| s.trim())
                .sorted()
                .collect::<Vec<&str>>(),
            vec!["GET", "POST"]
        );
    }

    #[tokio::test]
    async fn test_get_cached_write_query_error() {
        let context = in_memory_context_with_single_table().await;
        let handler = filters(context, free_for_all());

        let resp = request()
            .method("GET")
            .path(format!("/q/{}", CREATE_QUERY_HASH).as_str())
            .header(QUERY_HEADER, CREATE_QUERY)
            .reply(&handler)
            .await;
        assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(resp.body(), "NOT_READ_ONLY_QUERY");
    }

    #[tokio::test]
    async fn test_get_cached_no_etag() {
        let context = in_memory_context_with_single_table().await;
        let handler = filters(context, free_for_all());

        let resp = request()
            .method("GET")
            .path(format!("/q/{}", SELECT_QUERY_HASH).as_str())
            .header(QUERY_HEADER, SELECT_QUERY)
            .reply(&handler)
            .await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.body(), "{\"c\":1}\n");
        assert_eq!(resp.headers().get(ETAG).unwrap().to_str().unwrap(), V1_ETAG);
    }

    #[tokio::test]
    async fn test_get_cached_no_query() {
        let context = in_memory_context_with_single_table().await;
        let handler = filters(context, free_for_all());

        let resp = request()
            .method("GET")
            .path(format!("/q/{}", SELECT_QUERY_HASH).as_str())
            .reply(&handler)
            .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        // TODO: return a better error than this
        // https://github.com/splitgraph/seafowl/issues/22
        assert_eq!(resp.body(), "Request body deserialize error: EOF while parsing a value at line 1 column 0");
    }

    #[tokio::test]
    async fn test_get_cached_no_etag_query_in_body() {
        let context = in_memory_context_with_single_table().await;
        let handler = filters(context, free_for_all());

        let resp = request()
            .method("GET")
            .path(format!("/q/{}", SELECT_QUERY_HASH).as_str())
            .json(&HashMap::from([("query", SELECT_QUERY)]))
            .reply(&handler)
            .await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.body(), "{\"c\":1}\n");
        assert_eq!(resp.headers().get(ETAG).unwrap().to_str().unwrap(), V1_ETAG);
    }

    #[tokio::test]
    async fn test_get_cached_no_etag_extension() {
        let context = in_memory_context_with_single_table().await;
        let handler = filters(context, free_for_all());

        let resp = request()
            .method("GET")
            .path(format!("/q/{}.bin", SELECT_QUERY_HASH).as_str())
            .header(QUERY_HEADER, SELECT_QUERY)
            .reply(&handler)
            .await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.body(), "{\"c\":1}\n");
        assert_eq!(resp.headers().get(ETAG).unwrap().to_str().unwrap(), V1_ETAG);
    }

    #[tokio::test]
    async fn test_get_cached_reuse_etag() {
        // Pass the same ETag as If-None-Match, should return a 301

        let context = in_memory_context_with_single_table().await;
        let handler = filters(context, free_for_all());

        let resp = request()
            .method("GET")
            .path(format!("/q/{}", SELECT_QUERY_HASH).as_str())
            .header(QUERY_HEADER, SELECT_QUERY)
            .header(IF_NONE_MATCH, V1_ETAG)
            .reply(&handler)
            .await;
        assert_eq!(resp.status(), StatusCode::NOT_MODIFIED);
        assert_eq!(resp.body(), "NOT_MODIFIED");
    }

    #[tokio::test]
    async fn test_get_cached_etag_new_version() {
        // Pass the same ETag as If-None-Match, but the table version changed -> reruns the query

        let context = in_memory_context_with_modified_table().await;
        let handler = filters(context, free_for_all());

        let resp = request()
            .method("GET")
            .path(format!("/q/{}", SELECT_QUERY_HASH).as_str())
            .header(QUERY_HEADER, SELECT_QUERY)
            .header(ETAG, V1_ETAG)
            .reply(&handler)
            .await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.body(), "{\"c\":2}\n");
        assert_eq!(resp.headers().get(ETAG).unwrap().to_str().unwrap(), V2_ETAG);
    }

    async fn _query_uncached_endpoint<R, H>(
        handler: &H,
        query: &'_ str,
        token: Option<&str>,
    ) -> Response<Bytes>
    where
        R: Reply,
        H: Filter<Extract = R, Error = Rejection> + Clone + 'static,
    {
        let mut builder = request()
            .method("POST")
            .path("/q")
            .json(&HashMap::from([("query", query)]));

        if let Some(t) = token {
            builder = builder.header(AUTHORIZATION, format!("Bearer {}", t));
        }

        builder.reply(handler).await
    }

    async fn query_uncached_endpoint<R, H>(handler: &H, query: &'_ str) -> Response<Bytes>
    where
        R: Reply,
        H: Filter<Extract = R, Error = Rejection> + Clone + 'static,
    {
        _query_uncached_endpoint(handler, query, None).await
    }

    async fn query_uncached_endpoint_token<R, H>(
        handler: &H,
        query: &'_ str,
        token: &str,
    ) -> Response<Bytes>
    where
        R: Reply,
        H: Filter<Extract = R, Error = Rejection> + Clone + 'static,
    {
        _query_uncached_endpoint(handler, query, Some(token)).await
    }

    #[tokio::test]
    async fn test_get_uncached_read_query() {
        let context = in_memory_context_with_single_table().await;
        let handler = filters(context, free_for_all());

        let resp = query_uncached_endpoint(&handler, SELECT_QUERY).await;

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.body(), "{\"c\":1}\n");
    }

    #[tokio::test]
    async fn test_get_uncached_write_query() {
        let context = in_memory_context_with_single_table().await;
        let handler = filters(context, free_for_all());

        let resp = query_uncached_endpoint(&handler, INSERT_QUERY).await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.body(), "");

        let resp = query_uncached_endpoint(&handler, SELECT_QUERY).await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.body(), "{\"c\":2}\n");
    }

    #[tokio::test]
    async fn test_error_parse() {
        let context = in_memory_context_with_single_table().await;
        let handler = filters(context, free_for_all());

        let resp = query_uncached_endpoint(&handler, "SLEECT 1").await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            resp.body(),
            "SQL error: ParserError(\"Expected an SQL statement, found: SLEECT\")"
        );
    }

    #[tokio::test]
    async fn test_error_parse_seafowl() {
        let context = in_memory_context_with_single_table().await;
        let handler = filters(context, free_for_all());

        let resp =
            query_uncached_endpoint(&handler, "CREATE FUNCTION what_function").await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            resp.body(),
            "SQL error: ParserError(\"Expected AS, found: EOF\")"
        );
    }

    #[tokio::test]
    async fn test_error_plan_missing_table() {
        let context = in_memory_context_with_single_table().await;
        let handler = filters(context, free_for_all());

        let resp = query_uncached_endpoint(&handler, "SELECT * FROM missing_table").await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            resp.body(),
            "Error during planning: 'default.public.missing_table' not found"
        );
    }

    #[tokio::test]
    async fn test_error_execution() {
        let context = in_memory_context_with_single_table().await;
        let handler = filters(context, free_for_all());

        let resp = query_uncached_endpoint(&handler, "SELECT 1/0").await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        assert_eq!(resp.body(), "Arrow error: Divide by zero error");
    }

    #[tokio::test]
    async fn test_password_read_disables_cached_get() {
        let context = in_memory_context_with_single_table().await;
        let handler = filters(
            context,
            AccessPolicy::free_for_all().with_read_password("somepw"),
        );

        let resp = request()
            .method("GET")
            .path(format!("/q/{}", SELECT_QUERY_HASH).as_str())
            .header(QUERY_HEADER, SELECT_QUERY)
            .reply(&handler)
            .await;
        assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(resp.body(), "READ_ONLY_ENDPOINT_DISABLED");
    }

    #[tokio::test]
    async fn test_password_writes_anonymous_can_read() {
        let context = in_memory_context_with_single_table().await;
        let handler = filters(
            context,
            AccessPolicy::free_for_all().with_write_password("somepw"),
        );

        let resp = query_uncached_endpoint(&handler, "SELECT * FROM test_table").await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_password_writes_writer_can_write() {
        let context = in_memory_context_with_single_table().await;
        let handler = filters(
            context,
            AccessPolicy::free_for_all().with_write_password("somepw"),
        );

        let resp =
            query_uncached_endpoint_token(&handler, "DROP TABLE test_table", "somepw")
                .await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_password_writes_anonymous_cant_write() {
        let context = in_memory_context_with_single_table().await;
        let handler = filters(
            context,
            AccessPolicy::free_for_all().with_write_password("somepw"),
        );

        let resp = query_uncached_endpoint(&handler, "DROP TABLE test_table").await;
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
        assert_eq!(resp.body(), "WRITE_FORBIDDEN");
    }

    #[tokio::test]
    async fn test_password_read_writes_anonymous_cant_access() {
        let context = in_memory_context_with_single_table().await;
        let handler = filters(
            context,
            AccessPolicy::free_for_all()
                .with_write_password("somepw")
                .with_read_password("somereadpw"),
        );

        let resp = query_uncached_endpoint(&handler, "SELECT 1").await;
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(resp.body(), "NEED_ACCESS_TOKEN");
    }

    #[tokio::test]
    async fn test_password_read_writes_reader_cant_write() {
        let context = in_memory_context_with_single_table().await;
        let handler = filters(
            context,
            AccessPolicy::free_for_all()
                .with_write_password("somepw")
                .with_read_password("somereadpw"),
        );

        let resp = query_uncached_endpoint_token(
            &handler,
            "DROP TABLE test_table",
            "somereadpw",
        )
        .await;
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
        assert_eq!(resp.body(), "WRITE_FORBIDDEN");
    }

    #[tokio::test]
    async fn test_password_read_writes_writer_can_write() {
        let context = in_memory_context_with_single_table().await;
        let handler = filters(
            context,
            AccessPolicy::free_for_all()
                .with_write_password("somepw")
                .with_read_password("somereadpw"),
        );

        let resp =
            query_uncached_endpoint_token(&handler, "DROP TABLE test_table", "somepw")
                .await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_read_only_anonymous_cant_write() {
        let context = in_memory_context_with_single_table().await;
        let handler =
            filters(context, AccessPolicy::free_for_all().with_write_disabled());

        let resp = query_uncached_endpoint(&handler, "DROP TABLE test_table").await;
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
        assert_eq!(resp.body(), "WRITE_FORBIDDEN");
    }

    #[tokio::test]
    async fn test_read_only_useless_access_token() {
        let context = in_memory_context_with_single_table().await;
        let handler =
            filters(context, AccessPolicy::free_for_all().with_write_disabled());

        let resp =
            query_uncached_endpoint_token(&handler, "DROP TABLE test_table", "somepw")
                .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        assert_eq!(resp.body(), "USELESS_ACCESS_TOKEN");
    }

    #[tokio::test]
    async fn test_password_writes_anonymous_wrong_token() {
        let context = in_memory_context_with_single_table().await;
        let handler = filters(
            context,
            AccessPolicy::free_for_all().with_write_password("somepw"),
        );

        let resp = query_uncached_endpoint_token(&handler, "SELECT 1", "otherpw").await;
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(resp.body(), "INVALID_ACCESS_TOKEN");
    }

    #[tokio::test]
    async fn test_password_writes_anonymous_invalid_header() {
        let context = in_memory_context_with_single_table().await;
        let handler = filters(
            context,
            AccessPolicy::free_for_all().with_write_password("somepw"),
        );

        let resp = request()
            .method("POST")
            .path("/q")
            .json(&HashMap::from([("query", "SELECT 1")]))
            .header(AUTHORIZATION, "InvalidAuthzHeader")
            .reply(&handler)
            .await;

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(resp.body(), "INVALID_AUTHORIZATION_HEADER");
    }

    #[tokio::test]
    async fn test_http_type_conversion() {
        let context = Arc::new(in_memory_context().await);
        let handler = filters(context, free_for_all());

        let query = r#"
SELECT
  1::SMALLINT AS smallint_val,
  1000000::INTEGER AS integer_val,
  10000000000::BIGINT AS bigint_val,
  'c'::CHAR AS char_val,
  'varchar'::VARCHAR AS varchar_val,
  'text'::TEXT AS text_val,
  -- Unsupported 12.345::DECIMAL(5, 2) AS decimal_val,
  12.345::FLOAT AS float_val,
  12.345::REAL AS real_val,
  12.3456789101112131415::DOUBLE AS double_val,
  'true'::BOOLEAN AS bool_val,
  '2022-01-01'::DATE AS date_val,
  '2022-01-01T12:03:11.123456'::TIMESTAMP AS timestamp_val,
  [1,2,3,4,5] AS int_array_val,
  ['one','two'] AS text_array_val
"#;
        // NB: we can return arrays from these SELECT queries, but we don't support
        // CREATE TABLE queries with arrays, so we don't officially support arrays.

        let resp = request()
            .method("POST")
            .path("/q")
            .json(&HashMap::from([("query", query)]))
            .reply(&handler)
            .await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.body(),
            r#"{"smallint_val":1,"integer_val":1000000,"bigint_val":10000000000,"char_val":"c","varchar_val":"varchar","text_val":"text","float_val":12.345,"real_val":12.345,"double_val":12.345678910111213,"bool_val":true,"date_val":"2022-01-01","timestamp_val":"2022-01-01 12:03:11.123456","int_array_val":[1,2,3,4,5],"text_array_val":["one","two"]}
"#
        );
    }
}
