use arrow::csv::ReaderBuilder;
use arrow::datatypes::Schema;
use arrow::error::ArrowError;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::ExecutionPlan;
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
use serde_json::{json, Value};
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

use super::http_utils::{into_response, ApiError};

const QUERY_HEADER: &str = "X-Seafowl-Query";
const IF_NONE_MATCH: &str = "If-None-Match";
const ETAG: &str = "ETag";
const AUTHORIZATION: &str = "Authorization";
const BEARER_PREFIX: &str = "Bearer ";

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
    context.reload_schema().await;
    // TODO: handle/propagate errors
    let logical = context.create_logical_plan(&query).await?;

    if !user_context.can_perform_action(if is_read_only(&logical) {
        Action::Read
    } else {
        Action::Write
    }) {
        return Err(ApiError::Forbidden);
    };

    let physical = context.create_physical_plan(&logical).await?;

    let buf = physical_plan_to_json(context, physical).await?;
    Ok(buf)
}

pub fn with_auth(
    policy: AccessPolicy,
) -> impl Filter<Extract = (UserContext,), Error = Rejection> + Clone {
    warp::header::optional::<String>(AUTHORIZATION).and_then(
        move |header: Option<String>| {
            let token = match header {
                Some(h) => {
                    if !h.starts_with(BEARER_PREFIX) {
                        return future::err(warp::reject::reject());
                    };

                    Some(h.trim_start_matches(BEARER_PREFIX).to_string())
                }
                None => None,
            };

            // TODO propagate a 401 here
            match token_to_principal(token, &policy) {
                Ok(principal) => future::ok(UserContext {
                    principal,
                    policy: policy.clone(),
                }),
                Err(_) => future::err(warp::reject::reject()),
            }
        },
    )
}

// Disable the cached GET endpoint if we require auth for reads / they're disabled.
// (since caching + auth is yet another can of worms)
pub fn cached_read_query_authz(
    policy: AccessPolicy,
) -> impl Filter<Extract = (), Error = Rejection> + Clone {
    warp::any()
        .and_then(move || {
            match policy.read {
                AccessSettings::Any => future::ok(()),
                // TODO errors
                _ => future::err(warp::reject::reject()),
            }
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

    context.reload_schema().await;
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
    form: FormData,
    context: Arc<dyn SeafowlContext>,
) -> Response {
    let parts: Vec<Part> = form.try_collect().await.unwrap();

    let mut has_header = true;
    let mut csv_schema: Option<Schema> = None;
    let mut filename = String::new();
    for p in parts {
        if p.name() == "has_header" {
            let value_bytes = load_part(p).await.unwrap();

            has_header = String::from_utf8(value_bytes).unwrap().starts_with("true");
            debug!("Form part has_header is: {}", has_header);
        } else if p.name() == "schema" && p.content_type() == Some("application/json") {
            let value_bytes = load_part(p).await.unwrap();

            let schema_json: Value =
                serde_json::from_slice(value_bytes.as_slice()).unwrap();
            csv_schema = Some(Schema::from(&schema_json).unwrap());
        } else if p.name() == "data" || p.name() == "file" {
            filename = p.filename().unwrap().to_string();

            // Load the file content from the request
            // TODO: we're actually buffering the entire file into memory here which is sub-optimal,
            // since we could be writing the contents of the stream out into a temp file on the disk
            // to have a smaller memory footprint. However, for this to be supported warp first
            // needs to support streaming here: https://github.com/seanmonstar/warp/issues/323
            let source = load_part(p).await.unwrap();

            // Parse the schema and load the file contents into a vector of record batches
            let (schema, partition) = match filename.split('.').last().unwrap() {
                "csv" => load_csv_bytes(source, csv_schema.clone(), has_header).unwrap(),
                "parquet" => load_parquet_bytes(source).unwrap(),
                _ => {
                    return warp::reply::with_status(
                        format!("File {} not supported", filename),
                        StatusCode::BAD_REQUEST,
                    )
                    .into_response();
                }
            };

            // Create an execution plan for yielding the record batches we just generated
            let execution_plan = Arc::new(
                MemoryExec::try_new(&[partition], Arc::new(schema.clone()), None)
                    .unwrap(),
            );

            // Execute the plan and persist objects as well as table/partition metadata
            if let Err(error) = context
                .plan_to_table(execution_plan, schema_name.clone(), table_name.clone())
                .await
            {
                return warp::reply::with_status(
                    error.to_string(),
                    StatusCode::BAD_REQUEST,
                )
                .into_response();
            }
        }
    }

    if filename.is_empty() {
        return warp::reply::with_status(
            "No part containing file found in the request!",
            StatusCode::BAD_REQUEST,
        )
        .into_response();
    }

    warp::reply::with_status(Ok("done"), StatusCode::OK).into_response()
}

async fn load_part(p: Part) -> Result<Vec<u8>, warp::Rejection> {
    p.stream()
        .try_fold(Vec::new(), |mut vec, data| {
            vec.put(data);
            async move { Ok(vec) }
        })
        .await
        .map_err(|e| {
            eprintln!("Error reading part's data: {}", e);
            warp::reject::reject()
        })
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
    let csv_reader = builder.build(&mut cursor).unwrap();
    let schema = csv_reader.schema().as_ref().clone();
    let partition: Vec<RecordBatch> = csv_reader
        .into_iter()
        .collect::<Result<Vec<RecordBatch>, ArrowError>>()?;

    Ok((schema, partition))
}

fn load_parquet_bytes(source: Vec<u8>) -> Result<(Schema, Vec<RecordBatch>), ArrowError> {
    let mut parquet_reader =
        ParquetFileArrowReader::try_new(Bytes::from(source)).unwrap();

    let schema = parquet_reader.get_schema().unwrap();

    let partition: Vec<RecordBatch> = parquet_reader
        .get_record_reader(1024)
        .unwrap()
        .collect::<Result<Vec<RecordBatch>, ArrowError>>(
    )?;

    Ok((schema, partition))
}

pub fn filters(
    context: Arc<dyn SeafowlContext>,
    access_policy: AccessPolicy,
) -> impl Filter<Extract = impl Reply, Error = warp::Rejection> + Clone {
    let cors = warp::cors()
        .allow_any_origin()
        .allow_headers(vec!["X-Seafowl-Query", "Authorization", "Content-Type"])
        .allow_methods(vec!["GET", "POST"]);

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
        .and(with_auth(access_policy))
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
        .and(warp::multipart::form())
        .and(warp::any().map(move || ctx.clone()))
        .then(upload);

    cached_read_query_route
        .or(uncached_read_write_query_route)
        .or(upload_route)
        .with(cors)
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
    use arrow::array::{Int32Array, StringArray};
    use arrow::csv::WriterBuilder;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use bytes::Bytes;
    use datafusion::assert_batches_eq;
    use datafusion::from_slice::FromSlice;
    use datafusion::parquet::arrow::ArrowWriter;
    use itertools::Itertools;
    use std::io::Cursor;
    use std::{collections::HashMap, sync::Arc};
    use warp::{Filter, Rejection, Reply};

    use warp::http::Response;
    use warp::{
        hyper::{header::IF_NONE_MATCH, StatusCode},
        test::request,
    };

    use test_case::test_case;

    use crate::auth::AccessPolicy;

    use crate::{
        context::{test_utils::in_memory_context, SeafowlContext},
        frontend::http::{filters, ETAG, QUERY_HEADER},
    };

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
        context.reload_schema().await;

        context
            .collect(
                context
                    .plan_query("INSERT INTO test_table VALUES (1)")
                    .await
                    .unwrap(),
            )
            .await
            .unwrap();
        context.reload_schema().await;
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
        context.reload_schema().await;
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

    // Create a mock upload request and execute it
    async fn mock_upload_request(
        filename: &str,
        file_content: Vec<u8>,
        schema_name: &str,
        table_name: &str,
        schema_json: Option<&str>,
        has_headers: Option<&str>,
        context: Arc<dyn SeafowlContext>,
    ) -> Response<Bytes> {
        fn append_parts_to_body(
            mut body: Vec<u8>,
            boundary: &str,
            name: &str,
            content_type: &str,
            filename: Option<&str>,
            mut part: Vec<u8>,
            end: bool,
        ) -> Vec<u8> {
            body.append(
                &mut format!(
                    "--{}\r\n\
                Content-Disposition: form-data; name=\"{}\"{}\n\
                Content-Type: {}\n\n",
                    boundary,
                    name,
                    if let Some(filename) = filename {
                        format!("; filename=\"{}\"", filename)
                    } else {
                        String::new()
                    },
                    content_type
                )
                .as_bytes()
                .to_vec(),
            );

            body.append(&mut part);
            if end {
                body.append(&mut format!("--{}--", boundary).as_bytes().to_vec())
            } else {
                body.append(&mut "\n\n".as_bytes().to_vec());
            };

            body
        }

        let handler = filters(context, free_for_all());

        let boundary = "0123456789";
        let mut body: Vec<u8> = vec![];

        if let Some(has_headers) = has_headers {
            body = append_parts_to_body(
                body,
                boundary,
                "has_header",
                "application/json",
                None,
                has_headers.as_bytes().to_vec(),
                false,
            );
        }

        if let Some(schema_json) = schema_json {
            body = append_parts_to_body(
                body,
                boundary,
                "schema",
                "application/json",
                None,
                schema_json.as_bytes().to_vec(),
                false,
            );
        }

        body = append_parts_to_body(
            body,
            boundary,
            "data",
            "application/octet-stream",
            Some(filename),
            file_content,
            true,
        );

        request()
            .method("POST")
            .path(format!("/upload/{}/{}", schema_name, table_name).as_str())
            .header("Host", "localhost:3030")
            .header("User-Agent", "curl/7.64.1")
            .header("Accept", "*/*")
            .header("Content-Length", 232)
            .header(
                "Content-Type",
                format!("multipart/form-data; boundary={}", boundary),
            )
            .body(body)
            .reply(&handler)
            .await
    }

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

    async fn query_uncached_endpoint<R, H>(handler: &H, query: &'_ str) -> Response<Bytes>
    where
        R: Reply,
        H: Filter<Extract = R, Error = Rejection> + Clone + 'static,
    {
        request()
            .method("POST")
            .path("/q")
            .json(&HashMap::from([("query", query)]))
            .reply(handler)
            .await
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

    #[test_case(
        "csv",
        true,
        Some(true);
        "CSV file schema supplied w/ headers")
    ]
    #[test_case(
        "csv",
        true,
        Some(false);
        "CSV file schema supplied w/o headers")
    ]
    #[test_case(
        "csv",
        false,
        Some(true);
        "CSV file schema inferred w/ headers")
    ]
    #[test_case(
        "csv",
        false,
        Some(false);
        "CSV file schema inferred w/o headers")
    ]
    #[test_case(
        "parquet",
        false,
        None;
        "Parquet file")
    ]
    #[tokio::test]
    async fn test_upload_base(
        file_format: &str,
        include_schema: bool,
        add_headers: Option<bool>,
    ) {
        let context = in_memory_context_with_single_table().await;
        let _handler = filters(context.clone(), free_for_all());

        let table_name = format!("{}_table", file_format);

        // Prepare the schema + data (a single record batch) which we'll convert to bytes via
        // a corresponding writer
        let schema = Arc::new(Schema::new(vec![
            Field::new("number", DataType::Int32, false),
            Field::new("parity", DataType::Utf8, false),
        ]));

        // For CSV uploads we can supply the schema as another part of the multipart request, to
        // reduce the ambiguity resulting from automatic schema inference
        let schema_json = r#"{
                "fields": [
                    {
                        "name": "number",
                        "nullable": false,
                        "type": {
                            "name": "int",
                            "bitWidth": 32,
                            "isSigned": true
                        }
                    },
                    {
                        "name": "parity",
                        "nullable": false,
                        "type": {
                            "name": "utf8"
                        }
                    }
                ]
            }"#;

        let range = 0..2000;
        let mut input_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from_slice(range.clone().collect_vec())),
                Arc::new(StringArray::from(
                    range
                        .map(|number| if number % 2 == 0 { "even" } else { "odd" })
                        .collect_vec(),
                )),
            ],
        )
        .unwrap();

        // Write out the CSV/Parquet format data into mock request
        let mut form_data: Cursor<Vec<u8>> = Default::default();
        // drop the writer early to release the borrow.
        if file_format == "csv" {
            let mut writer = WriterBuilder::new()
                .has_headers(if let Some(has_headers) = add_headers {
                    has_headers
                } else {
                    true
                })
                .build(&mut form_data);
            writer.write(&input_batch).unwrap();
        } else if file_format == "parquet" {
            let mut writer = ArrowWriter::try_new(&mut form_data, schema, None).unwrap();
            writer.write(&input_batch).unwrap();
            writer.close().unwrap();
        }
        form_data.set_position(0);

        let resp = mock_upload_request(
            format!("test.{}", file_format).as_str(),
            form_data.into_inner(),
            "test_upload",
            table_name.as_str(),
            if include_schema && file_format == "csv" {
                Some(schema_json)
            } else {
                None
            },
            add_headers.map(|h| if h { "true" } else { "false" }),
            context.clone(),
        )
        .await;

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.body(), "done");

        context.reload_schema().await;

        // Verify the newly created table contents
        let plan = context
            .plan_query(format!("SELECT * FROM test_upload.{}", table_name).as_str())
            .await
            .unwrap();
        let results = context.collect(plan).await.unwrap();

        // Generate expected output from the input batch that was used in the multipart request
        if !include_schema && add_headers == Some(false) {
            // Rename the columns, as they will be inferred without a schema
            input_batch = RecordBatch::try_from_iter_with_nullable(vec![
                ("column_1", input_batch.column(0).clone(), false),
                ("column_2", input_batch.column(1).clone(), false),
            ])
            .unwrap();
        }
        let formatted = arrow::util::pretty::pretty_format_batches(&[input_batch])
            .unwrap()
            .to_string();

        let expected: Vec<&str> = formatted.trim().lines().collect();

        assert_batches_eq!(expected, &results);
    }

    #[tokio::test]
    async fn test_upload_to_existing_table() {
        let context = in_memory_context_with_single_table().await;

        // Prepare the schema that matches the existing table + some data
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col_1",
            DataType::Int32,
            true,
        )]));

        let input_batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![2, 3]))],
        )
        .unwrap();

        // Write out the new data into mock request for Parquet upload
        let mut form_data: Cursor<Vec<u8>> = Default::default();
        // drop the writer early to release the borrow.
        {
            let mut writer = ArrowWriter::try_new(&mut form_data, schema, None).unwrap();
            writer.write(&input_batch).unwrap();
            writer.close().unwrap();
        }
        form_data.set_position(0);

        // Create a mock request and execute it
        let resp = mock_upload_request(
            "test.parquet",
            form_data.into_inner(),
            "public",
            "test_table",
            None,
            None,
            context.clone(),
        )
        .await;

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.body(), "done");

        context.reload_schema().await;

        // Verify the newly created table contents
        let plan = context
            .plan_query("SELECT * FROM test_table")
            .await
            .unwrap();
        let results = context.collect(plan).await.unwrap();

        let expected = vec![
            "+-------+",
            "| col_1 |",
            "+-------+",
            "| 1     |",
            "| 2     |",
            "| 3     |",
            "+-------+",
        ];

        assert_batches_eq!(expected, &results);

        // Now try with schema that doesn't matches the existing table (re-use inout batch from before)
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col_1",
            DataType::Int32,
            false,
        )]));

        let input_batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![4, 5]))],
        )
        .unwrap();

        // Write out the new data into mock request for Parquet upload
        let mut form_data: Cursor<Vec<u8>> = Default::default();
        // drop the writer early to release the borrow.
        {
            let mut writer = ArrowWriter::try_new(&mut form_data, schema, None).unwrap();
            writer.write(&input_batch).unwrap();
            writer.close().unwrap();
        }
        form_data.set_position(0);

        let resp = mock_upload_request(
            "test.parquet",
            form_data.into_inner(),
            "public",
            "test_table",
            None,
            None,
            context,
        )
        .await;

        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        assert_eq!(resp.body(), "Execution error: The table public.test_table already exists but has a different schema than the one provided.");
    }
}
