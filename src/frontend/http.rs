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
use arrow::record_batch::{RecordBatch, RecordBatchReader};
use arrow_integration_test::{schema_from_json, schema_to_json};
use arrow_schema::SchemaRef;
use bytes::{Buf, Bytes};

use datafusion::datasource::DefaultTableSource;
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion_common::tree_node::{TreeNode, TreeNodeVisitor, VisitRecursion};
use datafusion_expr::logical_plan::{LogicalPlan, TableScan};
use deltalake::parquet::data_type::AsBytes;
use deltalake::DeltaTable;
use futures::{future, StreamExt};
use hex::encode;
use log::{debug, info, warn};
use percent_encoding::{percent_decode_str, utf8_percent_encode, NON_ALPHANUMERIC};
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::sync::broadcast::Receiver;
use warp::http::HeaderValue;
use warp::multipart::{FormData, Part};
use warp::reply::{with_header, Response};
use warp::{hyper::header, hyper::StatusCode, Filter, Reply};

use crate::auth::{token_to_principal, AccessPolicy, Action, UserContext};
use crate::config::schema::{AccessSettings, MEBIBYTES};
use crate::{
    config::schema::{str_to_hex_hash, HttpFrontend},
    context::{
        is_read_only, is_statement_read_only, DefaultSeafowlContext, SeafowlContext,
    },
};

use super::http_utils::{handle_rejection, into_response, ApiError};

const QUERY_HEADER: &str = "X-Seafowl-Query";
const BEARER_PREFIX: &str = "Bearer ";
// We have a very lax CORS on this, so we don't mind browsers
// caching it for as long as possible.
const CORS_MAXAGE: u32 = 86400;

// Vary on Origin, as warp's CORS responds with Access-Control-Allow-Origin: [origin],
// so we can't cache the response in the browser if the origin changes.
// NB: We can't vary by `Authorization` in here since Cloudflare states that it doesn't
// take the vary values into account in caching decisions:
// https://developers.cloudflare.com/cache/about/cache-control/#other
const VARY: &str = "Content-Type, Origin, X-Seafowl-Query";

#[derive(Default)]
struct ETagBuilderVisitor {
    table_versions: Vec<u8>,
}

impl TreeNodeVisitor for ETagBuilderVisitor {
    type N = LogicalPlan;

    fn pre_visit(
        &mut self,
        plan: &LogicalPlan,
    ) -> Result<VisitRecursion, DataFusionError> {
        if let LogicalPlan::TableScan(TableScan { source, .. }) = plan {
            // TODO handle external Parquet tables too
            if let Some(default_table_source) =
                source.as_any().downcast_ref::<DefaultTableSource>()
            {
                if let Some(table) = default_table_source
                    .table_provider
                    .as_any()
                    .downcast_ref::<DeltaTable>()
                {
                    self.table_versions
                        .extend(table.table_uri().as_bytes().to_vec());
                    self.table_versions
                        .extend(table.version().as_bytes().to_vec());
                }
            }
        }
        Ok(VisitRecursion::Continue)
    }
}

fn plan_to_etag(plan: &LogicalPlan) -> String {
    let mut visitor = ETagBuilderVisitor::default();
    plan.visit(&mut visitor).unwrap();

    debug!("Extracted table versions: {:?}", visitor.table_versions);

    let mut hasher = Sha256::new();
    hasher.update(json!(visitor.table_versions).to_string());
    encode(hasher.finalize())
}

// Construct a content-type header value that also includes schema information.
fn content_type_with_schema(schema: SchemaRef) -> HeaderValue {
    let schema_string = schema_to_json(schema.as_ref()).to_string();
    let output = utf8_percent_encode(&schema_string, NON_ALPHANUMERIC);

    HeaderValue::from_str(format!("application/json; arrow-schema={output}").as_str())
        .unwrap_or_else(|e| {
            // Seems silly to error out here if the query itself succeeded.
            warn!(
                "Couldn't generate content type header for output schema {output}: {e:?}"
            );
            HeaderValue::from_static("application/json")
        })
}

#[derive(Debug, Deserialize)]
struct QueryBody {
    query: String,
}

/// Execute a physical plan and output its results to a JSON Lines format
async fn physical_plan_to_json(
    context: Arc<DefaultSeafowlContext>,
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

/// POST /q or /[database_name]/q
pub async fn uncached_read_write_query(
    database_name: Option<String>,
    user_context: UserContext,
    query: String,
    mut context: Arc<DefaultSeafowlContext>,
) -> Result<Response, ApiError> {
    // If a specific DB name was used as a parameter in the route, scope the context to it,
    // effectively making it the default DB for the duration of the session.
    if let Some(name) = database_name {
        if name != context.database {
            context = context.scope_to_database(name)?;
        }
    }

    let statements = context.parse_query(&query).await?;

    // We assume that there's at least one statement throughout the rest of this function
    if statements.is_empty() {
        return Err(ApiError::EmptyMultiStatement);
    };

    let reads = statements
        .iter()
        .filter(|s| is_statement_read_only(s))
        .count();

    // Check for authorization
    if !user_context.can_perform_action(if reads == statements.len() {
        Action::Read
    } else {
        Action::Write
    }) {
        return Err(ApiError::WriteForbidden);
    };

    // If we have a read statement, make sure it's the last and only one (that's the only one
    // we'll return actual results for)
    if (reads > 1)
        || (reads == 1
            && !is_statement_read_only(
                statements
                    .last()
                    .expect("at least one statement in the list"),
            ))
    {
        return Err(ApiError::InvalidMultiStatement);
    }

    // Execute all statements up until the last one.
    let mut plan_to_output = None;

    for statement in statements {
        let logical = context
            .create_logical_plan_from_statement(statement)
            .await?;
        plan_to_output = Some(context.create_physical_plan(&logical).await?);
    }

    // Collect output for the last statement
    let plan = plan_to_output.expect("at least one statement in the list");
    let schema = plan.schema();
    let buf = physical_plan_to_json(context, plan).await?;

    // Construct the response and headers
    let mut response = buf.into_response();
    if reads > 0 {
        response
            .headers_mut()
            .insert(header::CONTENT_TYPE, content_type_with_schema(schema));
    }
    Ok(response)
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
    warp::header::optional::<String>(header::AUTHORIZATION.as_str()).and_then(
        move |header: Option<String>| {
            future::ready(
                header_to_user_context(header, &policy).map_err(warp::reject::custom),
            )
        },
    )
}

// Disable the cached GET endpoint if the reads are disabled. Otherwise extract the principal and
// check whether it is allowed to perform reads.
pub fn cached_read_query_authz(
    policy: AccessPolicy,
) -> impl Filter<Extract = (), Error = Rejection> + Clone {
    warp::any()
        .and(with_auth(policy.clone()))
        .and_then(move |user_context: UserContext| match policy.read {
            AccessSettings::Off => {
                future::err(warp::reject::custom(ApiError::ReadOnlyEndpointDisabled))
            }
            _ => {
                if user_context.can_perform_action(Action::Read) {
                    future::ok(())
                } else {
                    future::err(warp::reject::custom(ApiError::ReadForbidden))
                }
            }
        })
        .untuple_one()
}

/// GET /q/[query hash] or /[database_name]/q/[query hash]
pub async fn cached_read_query(
    database_name: Option<String>,
    query_hash: String,
    raw_query: String,
    if_none_match: Option<String>,
    mut context: Arc<DefaultSeafowlContext>,
) -> Result<Response, ApiError> {
    // Ignore dots at the end
    let query_hash = query_hash.split('.').next().unwrap();

    let decoded_query = percent_decode_str(&raw_query).decode_utf8()?;

    let hash_str = str_to_hex_hash(&decoded_query);

    debug!(
        "Received query: {}, URL hash {}, actual hash {}",
        decoded_query, query_hash, hash_str
    );

    // Verify the query hash matches the query
    if query_hash != hash_str {
        return Err(ApiError::HashMismatch(hash_str, query_hash.to_string()));
    };

    // If a specific DB name was used as a parameter in the route, scope the context to it,
    // effectively making it the default DB for the duration of the session.
    if let Some(name) = database_name {
        if name != context.database {
            context = context.scope_to_database(name)?;
        }
    }

    // Plan the query
    let plan = context.create_logical_plan(&decoded_query).await?;
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
    let schema = physical.schema().clone();
    let buf = physical_plan_to_json(context, physical).await?;

    // Construct the response and headers
    let mut response = buf.into_response();
    response
        .headers_mut()
        .insert(header::ETAG, etag.parse().unwrap());
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, content_type_with_schema(schema));
    Ok(response)
}

/// POST /upload/[schema]/[table] or /[database]/upload/[schema]/[table]
pub async fn upload(
    database_name: Option<String>,
    schema_name: String,
    table_name: String,
    user_context: UserContext,
    mut form: FormData,
    mut context: Arc<DefaultSeafowlContext>,
) -> Result<Response, ApiError> {
    if !user_context.can_perform_action(Action::Write) {
        return Err(ApiError::WriteForbidden);
    };

    if let Some(name) = database_name {
        if name != context.database {
            context = context.scope_to_database(name)?;
        }
    }

    let mut has_header = true;
    let mut csv_schema: Option<Schema> = None;
    let mut filename = String::new();
    let mut part_data: Vec<u8> = vec![];
    while let Some(maybe_part) = form.next().await {
        let part = maybe_part.map_err(ApiError::UploadBodyLoadError)?;

        if part.name() == "has_header" {
            let value_bytes = load_part(part).await?;

            has_header = String::from_utf8(value_bytes)
                .map_err(|_| ApiError::UploadHasHeaderParseError)?
                .starts_with("true");
            debug!("Form part has_header is: {}", has_header);
        } else if part.name() == "schema" {
            let value_bytes = load_part(part).await?;

            csv_schema = Some(
                schema_from_json(
                    &serde_json::from_slice::<serde_json::Value>(value_bytes.as_slice())
                        .map_err(ApiError::UploadSchemaDeserializationError)?,
                )
                .map_err(ApiError::UploadSchemaParseError)?,
            );
        } else if part.name() == "data" || part.name() == "file" {
            filename = part
                .filename()
                .ok_or(ApiError::UploadMissingFile)?
                .to_string();

            // Load the file content from the request
            // TODO: we're actually buffering the entire file into memory here which is sub-optimal.
            // Instead we should be writing the contents of the stream out into a temp file on the
            // disk to have a smaller memory footprint.
            part_data = load_part(part).await?;
        }
    }

    if filename.is_empty() {
        return Err(ApiError::UploadMissingFile);
    }

    // Parse the schema and load the file contents into a vector of record batches
    let (schema, partition) = match filename
        .split('.')
        .last()
        .ok_or_else(|| ApiError::UploadMissingFilenameExtension(filename.clone()))?
    {
        "csv" => load_csv_bytes(part_data, csv_schema.clone(), has_header)
            .map_err(|e| ApiError::UploadFileLoadError(e.into()))?,
        "parquet" => {
            load_parquet_bytes(part_data).map_err(ApiError::UploadFileLoadError)?
        }
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

    Ok(warp::reply::with_status(Ok("done"), StatusCode::OK).into_response())
}

async fn load_part(mut part: Part) -> Result<Vec<u8>, ApiError> {
    let mut bytes: Vec<u8> = vec![];
    while let Some(maybe_bytes) = part.data().await {
        bytes.extend(maybe_bytes.map_err(ApiError::UploadBodyLoadError)?.chunk())
    }
    Ok(bytes)
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
    let parquet_reader = ParquetRecordBatchReader::try_new(Bytes::from(source), 1024)?;

    let schema = parquet_reader.schema();

    let partition: Vec<RecordBatch> =
        parquet_reader.collect::<Result<Vec<RecordBatch>, ArrowError>>()?;

    Ok((schema.as_ref().clone(), partition))
}

// We need the allow to silence the compiler: it asks us to add warp::generic::Tuple to the first
// parameter of the return type, but that struct is not exportable (generic is private).
// TODO: Fix the signature and remove the allow attribute at some point.
#[allow(opaque_hidden_inferred_bound)]
pub fn filters(
    context: Arc<DefaultSeafowlContext>,
    config: HttpFrontend,
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    let access_policy = AccessPolicy::from_config(&config);

    let cors = warp::cors()
        .allow_any_origin()
        .allow_headers(vec![
            "X-Seafowl-Query",
            header::AUTHORIZATION.as_str(),
            header::CONTENT_TYPE.as_str(),
        ])
        .allow_methods(vec!["GET", "POST"])
        .max_age(CORS_MAXAGE);

    let log = warp::log(module_path!());

    // TODO: The path parameter parsing below is very unergonomic, mostly because warp doesn't
    // provide better primitives (e.g. `warp::generic::Either` is private outside of the crate).
    // The logic is to try and match the endpoint with or without the optional prefix for the DB
    // name, and then map the first parameter in both cases to the same `Option<String>` type, which
    // lets us use the `unify` filter method.

    // Cached read query
    let ctx = context.clone();
    let cached_read_query_route = warp::path::param::<String>()
        .map(Some)
        .or_else(|_| async { Ok::<(Option<String>,), Infallible>((None,)) })
        .and(warp::path!("q" / String))
        .or(warp::any()
            .map(move || None::<String>)
            .and(warp::path!("q" / String)))
        .and(warp::path::end())
        .and(warp::get())
        .unify()
        .and(cached_read_query_authz(access_policy.clone()))
        .and(
            // Extract the query either from the header or from the JSON body
            warp::header::<String>(QUERY_HEADER)
                .or(warp::body::json().map(|b: QueryBody| b.query))
                .unify(),
        )
        .and(warp::header::optional::<String>(
            header::IF_NONE_MATCH.as_str(),
        ))
        .and(warp::any().map(move || ctx.clone()))
        .then(cached_read_query)
        .map(into_response);

    // Uncached read/write query
    let ctx = context.clone();
    let uncached_read_write_query_route = warp::path::param::<String>()
        .map(Some)
        .or_else(|_| async { Ok::<(Option<String>,), Infallible>((None,)) })
        .and(warp::path!("q"))
        .or(warp::any()
            .map(move || None::<String>)
            .and(warp::path!("q")))
        .and(warp::path::end())
        .and(warp::post())
        .unify()
        .and(with_auth(access_policy.clone()))
        .and(
            // Extract the query from the JSON body
            warp::body::json().map(|b: QueryBody| b.query),
        )
        .and(warp::any().map(move || ctx.clone()))
        .then(uncached_read_write_query)
        .map(into_response);

    // Upload endpoint
    let ctx = context;
    let upload_route = warp::path::param::<String>()
        .map(Some)
        .or_else(|_| async { Ok::<(Option<String>,), Infallible>((None,)) })
        .and(warp::path!("upload" / String / String))
        .or(warp::any()
            .map(move || None::<String>)
            .and(warp::path!("upload" / String / String)))
        .and(warp::post())
        .unify()
        .and(with_auth(access_policy))
        .and(
            warp::multipart::form().max_length(config.upload_data_max_length * MEBIBYTES),
        )
        .and(warp::any().map(move || ctx.clone()))
        .then(upload)
        .map(into_response);

    cached_read_query_route
        .or(uncached_read_write_query_route)
        .or(upload_route)
        .with(cors)
        .with(log)
        .map(|r| with_header(r, header::VARY, VARY))
        .recover(handle_rejection)
}

pub async fn run_server(
    context: Arc<DefaultSeafowlContext>,
    config: HttpFrontend,
    mut shutdown: Receiver<()>,
) {
    let filters = filters(context, config.clone());

    let socket_addr: SocketAddr = format!("{}:{}", config.bind_host, config.bind_port)
        .parse()
        .expect("Error parsing the listen address");
    let (_, future) =
        warp::serve(filters).bind_with_graceful_shutdown(socket_addr, async move {
            shutdown.recv().await.unwrap();
            info!("Shutting down Warp...");
        });
    future.await
}

#[cfg(test)]
pub mod tests {
    use bytes::Bytes;

    use itertools::Itertools;

    use std::fmt::Display;
    use std::{collections::HashMap, sync::Arc};

    use rand::{rngs::mock::StepRng, Rng};
    use std::cell::RefCell;
    use uuid::Builder;
    thread_local!(static STEP_RNG: RefCell<StepRng> = RefCell::new(StepRng::new(1, 1)));

    use rstest::rstest;
    use uuid::Uuid;
    use warp::{Filter, Rejection, Reply};

    use warp::http::Response;
    use warp::hyper::header;
    use warp::{
        hyper::{header::IF_NONE_MATCH, StatusCode},
        test::request,
    };

    use crate::auth::AccessPolicy;

    use crate::catalog::DEFAULT_DB;
    use crate::config::schema::{str_to_hex_hash, HttpFrontend};
    use crate::testutils::schema_from_header;
    use crate::{
        context::{test_utils::in_memory_context, DefaultSeafowlContext, SeafowlContext},
        frontend::http::{filters, QUERY_HEADER},
    };

    fn http_config_from_access_policy(access_policy: AccessPolicy) -> HttpFrontend {
        HttpFrontend {
            read_access: access_policy.read,
            write_access: access_policy.write,
            ..HttpFrontend::default()
        }
    }

    /// Build an in-memory context with a single table
    /// We implicitly assume here that this table is the only one in this context
    /// and has version ID 1 (otherwise the hashes won't match).
    async fn in_memory_context_with_single_table(
        new_db: Option<&str>,
    ) -> Arc<DefaultSeafowlContext> {
        let mut context = Arc::new(in_memory_context().await);

        // TODO: the ergonomics of setting up multi-db tests is not the nicest, since we don't support
        // cross-db queries, so we have to switch context back and forth.
        if let Some(db_name) = new_db {
            context
                .collect(
                    context
                        .plan_query(&format!("CREATE DATABASE IF NOT EXISTS {db_name}"))
                        .await
                        .unwrap(),
                )
                .await
                .unwrap();

            context = context.scope_to_database(db_name.to_string()).unwrap();
        }

        context
            .collect(
                context
                    .plan_query("CREATE TABLE test_table(col_1 INT)")
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

        if new_db.is_some() {
            // Re-scope to the original DB
            return context.scope_to_database(DEFAULT_DB.to_string()).unwrap();
        }

        context
    }

    async fn in_memory_context_with_modified_table(
        new_db: Option<&str>,
    ) -> Arc<DefaultSeafowlContext> {
        let mut context = in_memory_context_with_single_table(new_db).await;

        if let Some(db_name) = new_db {
            context = context.scope_to_database(db_name.to_string()).unwrap();
        }

        context
            .collect(
                context
                    .plan_query("INSERT INTO test_table VALUES (2)")
                    .await
                    .unwrap(),
            )
            .await
            .unwrap();

        if new_db.is_some() {
            // Re-scope to the original DB
            return context.scope_to_database(DEFAULT_DB.to_string()).unwrap();
        }

        context
    }

    fn free_for_all() -> AccessPolicy {
        AccessPolicy::free_for_all()
    }

    fn make_uri(path: impl Display, db_prefix: Option<&str>) -> String {
        match db_prefix {
            None => path.to_string(),
            Some(db_name) => format!("/{db_name}{path}"),
        }
    }

    const SELECT_QUERY: &str = "SELECT COUNT(*) AS c FROM test_table";
    const PERCENT_ENCODED_SELECT_QUERY: &str =
        "SELECT%20COUNT(*)%20AS%20c%20FROM%20test_table";
    const BAD_PERCENT_ENCODED_SELECT_QUERY: &str =
        "%99SELECT%0A%20%20COUNT(*)%20AS%20c%0AFROM%20test_table";
    const INSERT_QUERY: &str = "INSERT INTO test_table VALUES (2)";
    const CREATE_QUERY: &str = "CREATE TABLE other_test_table(col_1 INT)";
    const SELECT_QUERY_HASH: &str =
        "7fbbf7dddfd330d03e5e08cc5885ad8ca823e1b56e7cbadd156daa0e21c288f6";
    const V1_ETAG: &str =
        "1230e7ce41e2f7c2050b75e36b6f313f5cc4dd99b255f2761f589d60a44eee00";
    const V2_ETAG: &str =
        "b17259a6a4e10c9a8b42ce23e683b919ada82b2ed1fafbbcd10ff42c63ff2443";

    pub fn deterministic_uuid() -> Uuid {
        // A crude hack to get reproducible bytes as source for table UUID generation, to enable
        // transparent etag asserts
        STEP_RNG.with(|rng| {
            let bytes: [u8; 16] = rng.borrow_mut().gen();
            Builder::from_random_bytes(bytes).into_uuid()
        })
    }

    #[rstest]
    #[tokio::test]
    async fn test_get_cached_hash_mismatch(
        #[values(None, Some("test_db"))] new_db: Option<&str>,
    ) {
        let context = in_memory_context_with_single_table(new_db).await;
        let handler = filters(context, http_config_from_access_policy(free_for_all()));

        let resp = request()
            .method("GET")
            .path(make_uri("/q/wrong-hash", new_db).as_str())
            .header(QUERY_HEADER, SELECT_QUERY)
            .reply(&handler)
            .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[rstest]
    #[tokio::test]
    async fn test_cors(#[values(None, Some("test_db"))] new_db: Option<&str>) {
        let context = in_memory_context_with_single_table(new_db).await;
        let handler = filters(context, http_config_from_access_policy(free_for_all()));

        let resp = request()
            .method("OPTIONS")
            .path(make_uri("/q/somehash", new_db).as_str())
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

    #[rstest]
    #[tokio::test]
    async fn test_get_cached_write_query_error(
        #[values(None, Some("test_db"))] new_db: Option<&str>,
    ) {
        let context = in_memory_context_with_single_table(new_db).await;
        let handler = filters(context, http_config_from_access_policy(free_for_all()));

        let resp = request()
            .method("GET")
            .path(
                make_uri(format!("/q/{}", str_to_hex_hash(CREATE_QUERY)), new_db)
                    .as_str(),
            )
            .header(QUERY_HEADER, CREATE_QUERY)
            .reply(&handler)
            .await;
        assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(resp.body(), "NOT_READ_ONLY_QUERY");
    }

    #[rstest]
    #[tokio::test]
    async fn test_get_cached_no_etag(
        #[values(None, Some("test_db"))] new_db: Option<&str>,
    ) {
        let context = in_memory_context_with_single_table(new_db).await;
        let handler = filters(context, http_config_from_access_policy(free_for_all()));

        let resp = request()
            .method("GET")
            .path(make_uri(format!("/q/{SELECT_QUERY_HASH}"), new_db).as_str())
            .header(QUERY_HEADER, SELECT_QUERY)
            .reply(&handler)
            .await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.body(), "{\"c\":1}\n");
        assert_eq!(
            resp.headers().get(header::ETAG).unwrap().to_str().unwrap(),
            V1_ETAG
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_get_cached_no_query(
        #[values(None, Some("test_db"))] new_db: Option<&str>,
    ) {
        let context = in_memory_context_with_single_table(new_db).await;
        let handler = filters(context, http_config_from_access_policy(free_for_all()));

        let resp = request()
            .method("GET")
            .path(make_uri(format!("/q/{SELECT_QUERY_HASH}"), new_db).as_str())
            .reply(&handler)
            .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        // TODO: return a better error than this
        // https://github.com/splitgraph/seafowl/issues/22
        assert_eq!(resp.body(), "Request body deserialize error: EOF while parsing a value at line 1 column 0");
    }

    #[rstest]
    #[tokio::test]
    async fn test_get_cached_no_etag_query_in_body(
        #[values(None, Some("test_db"))] new_db: Option<&str>,
    ) {
        let context = in_memory_context_with_single_table(new_db).await;
        let handler = filters(context, http_config_from_access_policy(free_for_all()));

        let resp = request()
            .method("GET")
            .path(make_uri(format!("/q/{SELECT_QUERY_HASH}"), new_db).as_str())
            .json(&HashMap::from([("query", SELECT_QUERY)]))
            .reply(&handler)
            .await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.body(), "{\"c\":1}\n");
        assert_eq!(
            resp.headers().get(header::ETAG).unwrap().to_str().unwrap(),
            V1_ETAG
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_get_cached_no_etag_extension(
        #[values(None, Some("test_db"))] new_db: Option<&str>,
    ) {
        let context = in_memory_context_with_single_table(new_db).await;
        let handler = filters(context, http_config_from_access_policy(free_for_all()));

        let resp = request()
            .method("GET")
            .path(make_uri(format!("/q/{SELECT_QUERY_HASH}.bin"), new_db).as_str())
            .header(QUERY_HEADER, SELECT_QUERY)
            .reply(&handler)
            .await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.body(), "{\"c\":1}\n");
        assert_eq!(
            resp.headers().get(header::ETAG).unwrap().to_str().unwrap(),
            V1_ETAG
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_get_cached_reuse_etag(
        #[values(None, Some("test_db"))] new_db: Option<&str>,
    ) {
        // Pass the same ETag as If-None-Match, should return a 304

        let context = in_memory_context_with_single_table(new_db).await;
        let handler = filters(context, http_config_from_access_policy(free_for_all()));

        let resp = request()
            .method("GET")
            .path(make_uri(format!("/q/{SELECT_QUERY_HASH}"), new_db).as_str())
            .header(QUERY_HEADER, SELECT_QUERY)
            .header(IF_NONE_MATCH, V1_ETAG)
            .reply(&handler)
            .await;
        assert_eq!(resp.status(), StatusCode::NOT_MODIFIED);
        assert_eq!(resp.body(), "NOT_MODIFIED");
    }

    #[rstest]
    #[tokio::test]
    async fn test_get_encoded_query_special_chars(
        #[values(None, Some("test_db"))] new_db: Option<&str>,
    ) {
        let context = in_memory_context_with_single_table(new_db).await;
        let handler = filters(context, http_config_from_access_policy(free_for_all()));

        let resp = request()
            .method("GET")
            .path(make_uri(format!("/q/{SELECT_QUERY_HASH}.bin"), new_db).as_str())
            .header(QUERY_HEADER, PERCENT_ENCODED_SELECT_QUERY)
            .reply(&handler)
            .await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.body(), "{\"c\":1}\n");
        assert_eq!(
            resp.headers().get(header::ETAG).unwrap().to_str().unwrap(),
            V1_ETAG
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_query_bad_encoding(
        #[values(None, Some("test_db"))] new_db: Option<&str>,
    ) {
        let context = in_memory_context_with_single_table(new_db).await;
        let handler = filters(context, http_config_from_access_policy(free_for_all()));

        let resp = request()
            .method("GET")
            .path(make_uri(format!("/q/{SELECT_QUERY_HASH}.bin"), new_db).as_str())
            .header(QUERY_HEADER, BAD_PERCENT_ENCODED_SELECT_QUERY)
            .reply(&handler)
            .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        assert_eq!(resp.body(), "QUERY_DECODE_ERROR");
    }

    #[rstest]
    #[tokio::test]
    async fn test_get_cached_etag_new_version(
        #[values(None, Some("test_db"))] new_db: Option<&str>,
    ) {
        // Pass the same ETag as If-None-Match, but the table version changed -> reruns the query

        let context = in_memory_context_with_modified_table(new_db).await;
        let handler = filters(context, http_config_from_access_policy(free_for_all()));

        let resp = request()
            .method("GET")
            .path(make_uri(format!("/q/{SELECT_QUERY_HASH}"), new_db).as_str())
            .header(QUERY_HEADER, SELECT_QUERY)
            .header(header::ETAG, V1_ETAG)
            .reply(&handler)
            .await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.body(), "{\"c\":2}\n");
        assert_eq!(
            resp.headers().get(header::ETAG).unwrap().to_str().unwrap(),
            V2_ETAG
        );
    }

    async fn _query_uncached_endpoint<R, H>(
        handler: &H,
        query: &'_ str,
        path_prefix: Option<&str>,
        token: Option<&str>,
    ) -> Response<Bytes>
    where
        R: Reply,
        H: Filter<Extract = R, Error = Rejection> + Clone + 'static,
    {
        let mut builder = request()
            .method("POST")
            .path(make_uri("/q", path_prefix).as_str())
            .json(&HashMap::from([("query", query)]));

        if let Some(t) = token {
            builder = builder.header(header::AUTHORIZATION, format!("Bearer {t}"));
        }

        builder.reply(handler).await
    }

    async fn query_uncached_endpoint<R, H>(
        handler: &H,
        query: &'_ str,
        path_prefix: Option<&str>,
    ) -> Response<Bytes>
    where
        R: Reply,
        H: Filter<Extract = R, Error = Rejection> + Clone + 'static,
    {
        _query_uncached_endpoint(handler, query, path_prefix, None).await
    }

    async fn query_uncached_endpoint_token<R, H>(
        handler: &H,
        query: &'_ str,
        path_prefix: Option<&str>,
        token: &str,
    ) -> Response<Bytes>
    where
        R: Reply,
        H: Filter<Extract = R, Error = Rejection> + Clone + 'static,
    {
        _query_uncached_endpoint(handler, query, path_prefix, Some(token)).await
    }

    #[tokio::test]
    async fn test_get_uncached_read_nonexistent_db() {
        let context = in_memory_context_with_single_table(None).await;
        let handler = filters(context, http_config_from_access_policy(free_for_all()));

        let resp =
            query_uncached_endpoint(&handler, SELECT_QUERY, Some("missing_db")).await;

        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            resp.body(),
            "Error during planning: Unknown database missing_db; try creating one with CREATE DATABASE first"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_get_uncached_read_query(
        #[values(None, Some("test_db"))] new_db: Option<&str>,
    ) {
        let context = in_memory_context_with_single_table(new_db).await;
        let handler = filters(context, http_config_from_access_policy(free_for_all()));

        let resp = query_uncached_endpoint(&handler, SELECT_QUERY, new_db).await;

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.body(), "{\"c\":1}\n");
    }

    #[rstest]
    #[tokio::test]
    async fn test_get_uncached_write_query(
        #[values(None, Some("test_db"))] new_db: Option<&str>,
    ) {
        let context = in_memory_context_with_single_table(new_db).await;
        let handler = filters(context, http_config_from_access_policy(free_for_all()));

        let resp = query_uncached_endpoint(&handler, INSERT_QUERY, new_db).await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.body(), "");

        let resp = query_uncached_endpoint(&handler, SELECT_QUERY, new_db).await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.body(), "{\"c\":2}\n");
    }

    #[rstest]
    #[tokio::test]
    async fn test_error_parse(#[values(None, Some("test_db"))] new_db: Option<&str>) {
        let context = in_memory_context_with_single_table(new_db).await;
        let handler = filters(context, http_config_from_access_policy(free_for_all()));

        let resp = query_uncached_endpoint(&handler, "SLEECT 1", new_db).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            resp.body(),
            "SQL error: ParserError(\"Expected an SQL statement, found: SLEECT\")"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_error_parse_seafowl(
        #[values(None, Some("test_db"))] new_db: Option<&str>,
    ) {
        let context = in_memory_context_with_single_table(new_db).await;
        let handler = filters(context, http_config_from_access_policy(free_for_all()));

        let resp =
            query_uncached_endpoint(&handler, "CREATE FUNCTION what_function", new_db)
                .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            resp.body(),
            "SQL error: ParserError(\"Expected AS, found: EOF\")"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_error_plan_missing_table(
        #[values(None, Some("test_db"))] new_db: Option<&str>,
    ) {
        let context = in_memory_context_with_single_table(new_db).await;
        let handler = filters(context, http_config_from_access_policy(free_for_all()));

        let resp =
            query_uncached_endpoint(&handler, "SELECT * FROM missing_table", new_db)
                .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        if let Some(db_name) = new_db {
            assert_eq!(
                resp.body(),
                format!("Error during planning: table '{db_name}.public.missing_table' not found").as_str()
            );
        } else {
            assert_eq!(
                resp.body(),
                "Error during planning: table 'default.public.missing_table' not found"
            );
        }
    }

    #[rstest]
    #[tokio::test]
    async fn test_error_execution(#[values(None, Some("test_db"))] new_db: Option<&str>) {
        let context = in_memory_context_with_single_table(new_db).await;
        let handler = filters(context, http_config_from_access_policy(free_for_all()));

        let resp =
            query_uncached_endpoint(&handler, "SELECT 'notanint'::int", new_db).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        assert_eq!(resp.body(), "Arrow error: Cast error: Cannot cast string 'notanint' to value of Int32 type");
    }

    #[rstest]
    #[tokio::test]
    async fn test_password_read_anonymous_cant_cached_get(
        #[values(None, Some("test_db"))] new_db: Option<&str>,
    ) {
        let context = in_memory_context_with_single_table(new_db).await;
        let handler = filters(
            context,
            http_config_from_access_policy(
                AccessPolicy::free_for_all().with_read_password("somepw"),
            ),
        );

        let resp = request()
            .method("GET")
            .path(make_uri(format!("/q/{SELECT_QUERY_HASH}"), new_db).as_str())
            .header(QUERY_HEADER, SELECT_QUERY)
            .reply(&handler)
            .await;
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
        assert_eq!(resp.body(), "READ_FORBIDDEN");
    }

    #[rstest]
    #[tokio::test]
    async fn test_password_read_reader_can_cached_get(
        #[values(None, Some("test_db"))] new_db: Option<&str>,
    ) {
        let context = in_memory_context_with_single_table(new_db).await;
        let handler = filters(
            context,
            http_config_from_access_policy(
                AccessPolicy::free_for_all().with_read_password("somepw"),
            ),
        );

        let resp = request()
            .method("GET")
            .path(make_uri(format!("/q/{SELECT_QUERY_HASH}"), new_db).as_str())
            .header(QUERY_HEADER, SELECT_QUERY)
            .header(header::AUTHORIZATION, "Bearer somepw")
            .reply(&handler)
            .await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.body(), "{\"c\":1}\n");
    }

    #[rstest]
    #[tokio::test]
    async fn test_disabled_read_disabled_cached_get(
        #[values(None, Some("test_db"))] new_db: Option<&str>,
    ) {
        let context = in_memory_context_with_single_table(new_db).await;
        let handler = filters(
            context,
            http_config_from_access_policy(
                AccessPolicy::free_for_all().with_read_disabled(),
            ),
        );

        let resp = request()
            .method("GET")
            .path(make_uri(format!("/q/{SELECT_QUERY_HASH}"), new_db).as_str())
            .header(QUERY_HEADER, SELECT_QUERY)
            .reply(&handler)
            .await;
        assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(resp.body(), "READ_ONLY_ENDPOINT_DISABLED");
    }

    #[rstest]
    #[tokio::test]
    async fn test_password_writes_anonymous_can_read(
        #[values(None, Some("test_db"))] new_db: Option<&str>,
    ) {
        let context = in_memory_context_with_single_table(new_db).await;
        let handler = filters(
            context,
            http_config_from_access_policy(
                AccessPolicy::free_for_all().with_write_password("somepw"),
            ),
        );

        let resp =
            query_uncached_endpoint(&handler, "SELECT * FROM test_table", new_db).await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[rstest]
    #[tokio::test]
    async fn test_password_writes_writer_can_write(
        #[values(None, Some("test_db"))] new_db: Option<&str>,
    ) {
        let context = in_memory_context_with_single_table(new_db).await;
        let handler = filters(
            context,
            http_config_from_access_policy(
                AccessPolicy::free_for_all().with_write_password("somepw"),
            ),
        );

        let resp = query_uncached_endpoint_token(
            &handler,
            "DROP TABLE test_table",
            new_db,
            "somepw",
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[rstest]
    #[tokio::test]
    async fn test_password_writes_anonymous_cant_write(
        #[values(None, Some("test_db"))] new_db: Option<&str>,
    ) {
        let context = in_memory_context_with_single_table(new_db).await;
        let handler = filters(
            context,
            http_config_from_access_policy(
                AccessPolicy::free_for_all().with_write_password("somepw"),
            ),
        );

        let resp =
            query_uncached_endpoint(&handler, "DROP TABLE test_table", new_db).await;
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
        assert_eq!(resp.body(), "WRITE_FORBIDDEN");
    }

    #[rstest]
    #[tokio::test]
    async fn test_password_read_writes_anonymous_cant_access(
        #[values(None, Some("test_db"))] new_db: Option<&str>,
    ) {
        let context = in_memory_context_with_single_table(new_db).await;
        let handler = filters(
            context,
            http_config_from_access_policy(
                AccessPolicy::free_for_all()
                    .with_write_password("somepw")
                    .with_read_password("somereadpw"),
            ),
        );

        let resp = query_uncached_endpoint(&handler, "SELECT 1", new_db).await;
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(resp.body(), "NEED_ACCESS_TOKEN");
    }

    #[rstest]
    #[tokio::test]
    async fn test_password_read_writes_reader_cant_write(
        #[values(None, Some("test_db"))] new_db: Option<&str>,
    ) {
        let context = in_memory_context_with_single_table(new_db).await;
        let handler = filters(
            context,
            http_config_from_access_policy(
                AccessPolicy::free_for_all()
                    .with_write_password("somepw")
                    .with_read_password("somereadpw"),
            ),
        );

        let resp = query_uncached_endpoint_token(
            &handler,
            "DROP TABLE test_table",
            new_db,
            "somereadpw",
        )
        .await;
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
        assert_eq!(resp.body(), "WRITE_FORBIDDEN");
    }

    #[rstest]
    #[tokio::test]
    async fn test_password_read_writes_writer_can_write(
        #[values(None, Some("test_db"))] new_db: Option<&str>,
    ) {
        let context = in_memory_context_with_single_table(new_db).await;
        let handler = filters(
            context,
            http_config_from_access_policy(
                AccessPolicy::free_for_all()
                    .with_write_password("somepw")
                    .with_read_password("somereadpw"),
            ),
        );

        let resp = query_uncached_endpoint_token(
            &handler,
            "DROP TABLE test_table",
            new_db,
            "somepw",
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[rstest]
    #[tokio::test]
    async fn test_read_only_anonymous_cant_write(
        #[values(None, Some("test_db"))] new_db: Option<&str>,
    ) {
        let context = in_memory_context_with_single_table(new_db).await;
        let handler = filters(
            context,
            http_config_from_access_policy(
                AccessPolicy::free_for_all().with_write_disabled(),
            ),
        );

        let resp =
            query_uncached_endpoint(&handler, "DROP TABLE test_table", new_db).await;
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
        assert_eq!(resp.body(), "WRITE_FORBIDDEN");
    }

    #[rstest]
    #[tokio::test]
    async fn test_read_only_useless_access_token(
        #[values(None, Some("test_db"))] new_db: Option<&str>,
    ) {
        let context = in_memory_context_with_single_table(new_db).await;
        let handler = filters(
            context,
            http_config_from_access_policy(
                AccessPolicy::free_for_all().with_write_disabled(),
            ),
        );

        let resp = query_uncached_endpoint_token(
            &handler,
            "DROP TABLE test_table",
            new_db,
            "somepw",
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        assert_eq!(resp.body(), "USELESS_ACCESS_TOKEN");
    }

    #[rstest]
    #[tokio::test]
    async fn test_password_writes_anonymous_wrong_token(
        #[values(None, Some("test_db"))] new_db: Option<&str>,
    ) {
        let context = in_memory_context_with_single_table(new_db).await;
        let handler = filters(
            context,
            http_config_from_access_policy(
                AccessPolicy::free_for_all().with_write_password("somepw"),
            ),
        );

        let resp =
            query_uncached_endpoint_token(&handler, "SELECT 1", new_db, "otherpw").await;
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(resp.body(), "INVALID_ACCESS_TOKEN");
    }

    #[rstest]
    #[tokio::test]
    async fn test_password_writes_anonymous_invalid_header(
        #[values(None, Some("test_db"))] new_db: Option<&str>,
    ) {
        let context = in_memory_context_with_single_table(new_db).await;
        let handler = filters(
            context,
            http_config_from_access_policy(
                AccessPolicy::free_for_all().with_write_password("somepw"),
            ),
        );

        let resp = request()
            .method("POST")
            .path(make_uri("/q", new_db).as_str())
            .json(&HashMap::from([("query", "SELECT 1")]))
            .header(header::AUTHORIZATION, "InvalidAuthzHeader")
            .reply(&handler)
            .await;

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(resp.body(), "INVALID_AUTHORIZATION_HEADER");
    }

    #[rstest]
    #[tokio::test]
    async fn test_multi_statement_no_reads(
        #[values(None, Some("test_db"))] new_db: Option<&str>,
    ) {
        let context = in_memory_context_with_single_table(new_db).await;
        let handler = filters(
            context,
            http_config_from_access_policy(
                AccessPolicy::free_for_all().with_write_password("somepw"),
            ),
        );

        let resp = query_uncached_endpoint_token(
            &handler,
            "DROP TABLE test_table;CREATE TABLE test_table(\"key\" VARCHAR);
            INSERT INTO test_table VALUES('hey')",
            new_db,
            "somepw",
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);

        let resp = query_uncached_endpoint_token(
            &handler,
            "SELECT * FROM test_table;",
            new_db,
            "somepw",
        )
        .await;
        assert_eq!(resp.body(), "{\"key\":\"hey\"}\n");
    }

    #[rstest]
    #[tokio::test]
    async fn test_multi_statement_read_at_end(
        #[values(None, Some("test_db"))] new_db: Option<&str>,
    ) {
        let context = in_memory_context_with_single_table(new_db).await;
        let handler = filters(
            context,
            http_config_from_access_policy(
                AccessPolicy::free_for_all().with_write_password("somepw"),
            ),
        );

        let resp = query_uncached_endpoint_token(
            &handler,
            "DROP TABLE test_table;CREATE TABLE test_table(\"key\" VARCHAR);
            INSERT INTO test_table VALUES('hey');SELECT * FROM test_table;",
            new_db,
            "somepw",
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.body(), "{\"key\":\"hey\"}\n");
    }

    #[rstest]
    #[tokio::test]
    async fn test_multi_statement_read_not_at_end_error(
        #[values(None, Some("test_db"))] new_db: Option<&str>,
    ) {
        let context = in_memory_context_with_single_table(new_db).await;
        let handler = filters(
            context,
            http_config_from_access_policy(
                AccessPolicy::free_for_all().with_write_password("somepw"),
            ),
        );

        let resp =
            query_uncached_endpoint_token(&handler, "SELECT * FROM test_table;DROP TABLE test_table;CREATE TABLE test_table(\"key\" VARCHAR);
            INSERT INTO test_table VALUES('hey')", new_db,"somepw")
                .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        assert!(String::from_utf8(resp.body().to_vec())
            .unwrap()
            .contains("must be at the end of a multi-statement query"));
    }

    #[rstest]
    #[tokio::test]
    async fn test_multi_statement_multiple_reads_error(
        #[values(None, Some("test_db"))] new_db: Option<&str>,
    ) {
        let context = in_memory_context_with_single_table(new_db).await;
        let handler = filters(
            context,
            http_config_from_access_policy(
                AccessPolicy::free_for_all().with_write_password("somepw"),
            ),
        );

        let resp =
            query_uncached_endpoint_token(&handler, "DROP TABLE test_table;CREATE TABLE test_table(\"key\" VARCHAR);
            INSERT INTO test_table VALUES('hey');SELECT * FROM test_table;SELECT * FROM test_table;", new_db, "somepw")
                .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        assert!(String::from_utf8(resp.body().to_vec())
            .unwrap()
            .contains("Only one read statement is allowed"));
    }

    #[rstest]
    #[case::cached_get(
        "GET",
        "/q/f7ff4745e8469a83bffdf247aef5f9ee2bbb9019bbf4a725b31ee36993d5d484"
    )]
    #[case::uncached_post("POST", "/q")]
    #[tokio::test]
    async fn test_http_type_conversion(#[case] method: &str, #[case] path: &str) {
        let context = Arc::new(in_memory_context().await);
        let handler = filters(
            context.clone(),
            http_config_from_access_policy(AccessPolicy::free_for_all()),
        );

        let query = r#"
SELECT
  1::SMALLINT AS smallint_val,
  1000000::INT AS integer_val,
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
  '2022-01-01T12:03:11.123456Z'::TIMESTAMP AS timestamp_val,
  [1,2,3,4,5] AS int_array_val,
  ['one','two'] AS text_array_val
"#;
        // NB: we can return arrays from these SELECT queries, but we don't support
        // CREATE TABLE queries with arrays, so we don't officially support arrays.

        let resp = request()
            .method(method)
            .path(path)
            .json(&HashMap::from([("query", query)]))
            .reply(&handler)
            .await;
        assert_eq!(resp.status(), StatusCode::OK);

        // Ensure schema round-trip works
        let expected_schema = context
            .plan_query(query)
            .await
            .unwrap()
            .schema()
            .as_ref()
            .clone();
        assert_eq!(schema_from_header(resp.headers()), expected_schema,);

        // Assert result
        assert_eq!(
            resp.body(),
            &Bytes::from(
                r#"{"bigint_val":10000000000,"bool_val":true,"char_val":"c","date_val":"2022-01-01","double_val":12.345678910111213,"float_val":12.345,"int_array_val":[1,2,3,4,5],"integer_val":1000000,"real_val":12.345,"smallint_val":1,"text_array_val":["one","two"],"text_val":"text","timestamp_val":"2022-01-01T12:03:11.123456","varchar_val":"varchar"}
"#
            )
        );
    }
}
