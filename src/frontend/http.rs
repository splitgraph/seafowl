use arrow::error::ArrowError;
use datafusion::error::DataFusionError;

use std::fmt::Debug;
use std::io::Write;
use std::time::Instant;
use std::{net::SocketAddr, sync::Arc};
use warp::{hyper, Rejection};

use arrow::json::writer::record_batches_to_json_rows;
use arrow::record_batch::RecordBatch;
#[cfg(feature = "frontend-arrow-flight")]
use arrow_flight::flight_service_client::FlightServiceClient;

use arrow_integration_test::{schema_from_json, schema_to_json};
use arrow_schema::SchemaRef;
use bytes::Buf;

use datafusion::datasource::DefaultTableSource;

use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::tree_node::{TreeNode, TreeNodeVisitor, VisitRecursion};
use datafusion_common::FileType;
use datafusion_expr::logical_plan::{LogicalPlan, TableScan};
use deltalake::parquet::data_type::AsBytes;
use deltalake::DeltaTable;
use futures::{future, StreamExt};
use hex::encode;
use metrics::counter;
use percent_encoding::{percent_decode_str, utf8_percent_encode, NON_ALPHANUMERIC};
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::sync::broadcast::Receiver;
use tracing::{debug, info, warn};
use warp::http::HeaderValue;
use warp::log::Info;
use warp::multipart::{FormData, Part};
use warp::reply::{with_header, Response};
use warp::{hyper::header, hyper::StatusCode, Filter, Reply};

use super::http_utils::{handle_rejection, into_response, ApiError};
use crate::auth::{token_to_principal, AccessPolicy, Action, UserContext};
use crate::catalog::DEFAULT_DB;
#[cfg(feature = "metrics")]
use crate::config::context::HTTP_REQUESTS;
use crate::config::schema::{AccessSettings, HttpFrontend, MEBIBYTES};
use crate::{
    config::schema::str_to_hex_hash,
    context::logical::{is_read_only, is_statement_read_only},
    context::SeafowlContext,
};

const QUERY_HEADER: &str = "X-Seafowl-Query";
const BEARER_PREFIX: &str = "Bearer ";
// We have a very lax CORS on this, so we don't mind browsers
// caching it for as long as possible.
const CORS_MAXAGE: u32 = 86400;
const QUERY_TIME_HEADER: &str = "X-Seafowl-Query-Time";

// Vary on Origin, as warp's CORS responds with Access-Control-Allow-Origin: [origin],
// so we can't cache the response in the browser if the origin changes.
// NB: Cloudflare doesn't take the vary values into account in caching decisions:
// https://developers.cloudflare.com/cache/about/cache-control/#other
const VARY: &str = "Authorization, Content-Type, Origin, X-Seafowl-Query";

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

/// Convert rows from a `RecordBatch` to their JSON Lines byte representation, with newlines at the end
fn batch_to_json(
    maybe_batch: Result<RecordBatch, DataFusionError>,
) -> Result<Vec<u8>, ArrowError> {
    let mut buf = Vec::new();
    for row in record_batches_to_json_rows(&[&maybe_batch?])? {
        buf.extend(
            serde_json::to_vec(&row)
                .map_err(|error| ArrowError::JsonError(error.to_string()))?,
        );
        buf.push(b'\n');
    }
    Ok(buf)
}

// Execute the plan and stream the results
async fn plan_to_response(
    context: Arc<SeafowlContext>,
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Response, DataFusionError> {
    let stream = context.execute_stream(plan).await?.map(|maybe_batch| {
        batch_to_json(maybe_batch)
            // Seems like at this point wrap/hyper don't really handle the stream error well,
            // i.e. the status code returned is 200 even when stream fails.
            // To at least make this more transparent convert the error message to payload,
            // otherwise the client just gets an opaque empty reply.
            .or_else(|e| Ok::<Vec<u8>, ArrowError>(e.to_string().into_bytes()))
    });
    let body = hyper::Body::wrap_stream(stream);
    Ok(Response::new(body))
}

/// POST /q or /[database_name]/q
pub async fn uncached_read_write_query(
    database_name: String,
    user_context: UserContext,
    query: String,
    mut context: Arc<SeafowlContext>,
) -> Result<Response, ApiError> {
    let timer = Instant::now();

    // If a specific DB name was used as a parameter in the route, scope the context to it,
    // effectively making it the default DB for the duration of the session.
    if database_name != context.default_catalog {
        context = context.scope_to_catalog(database_name);
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

    // Stream output for the last statement
    let plan = plan_to_output.expect("at least one statement in the list");
    let schema = plan.schema();
    let mut response = plan_to_response(context, plan).await?;

    if reads > 0 {
        response
            .headers_mut()
            .insert(header::CONTENT_TYPE, content_type_with_schema(schema));
    }

    let elapsed = timer.elapsed().as_millis().to_string();
    response
        .headers_mut()
        .insert(QUERY_TIME_HEADER, elapsed.parse().unwrap());

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

/// Supports either one of:
/// 1. GET /q/[query]
/// 2. GET /q/[query hash] {"query": "[query]"}
/// 3. GET -H "X-Seafowl-Query: [query]" /q/[query hash]
/// In all cases the path can have an optional prefix parameter in order do specify a non-default
/// database as target, e.g. /[database_name]/q/[query]
pub async fn cached_read_query(
    database_name: String,
    query_or_hash: String,
    maybe_raw_query: Option<String>,
    if_none_match: Option<String>,
    mut context: Arc<SeafowlContext>,
) -> Result<Response, ApiError> {
    let timer = Instant::now();

    // Ignore dots at the end
    let query_or_hash = query_or_hash.split('.').next().unwrap();

    let decoded_query = if let Some(raw_query) = maybe_raw_query {
        // If we managed to extract the query from the body or the header, the string from the path
        // parameter is supposed to be the query hash; decode the query and validate
        let query_hash = query_or_hash;
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

        decoded_query.to_string()
    } else {
        // Otherwise, the query itself is passed as the path param
        percent_decode_str(query_or_hash).decode_utf8()?.to_string()
    };

    // If a specific DB name was used as a parameter in the route, scope the context to it,
    // effectively making it the default DB for the duration of the session.
    if database_name != context.default_catalog {
        context = context.scope_to_catalog(database_name);
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
    let mut response = plan_to_response(context, physical).await?;

    let elapsed = timer.elapsed().as_millis().to_string();
    response
        .headers_mut()
        .insert(QUERY_TIME_HEADER, elapsed.parse().unwrap());
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
    database_name: String,
    schema_name: String,
    table_name: String,
    user_context: UserContext,
    mut form: FormData,
    mut context: Arc<SeafowlContext>,
) -> Result<Response, ApiError> {
    if !user_context.can_perform_action(Action::Write) {
        return Err(ApiError::WriteForbidden);
    };

    if database_name != context.default_catalog {
        context = context.scope_to_catalog(database_name.clone());
    }

    let mut has_header = true;
    let mut schema: Option<SchemaRef> = None;
    let mut filename = String::new();
    let ref_temp_file = context.inner.runtime_env().disk_manager.create_tmp_file(
        format!("Creating a target file to append to {database_name}.{schema_name}.{table_name}").as_str(),
    )?;
    while let Some(maybe_part) = form.next().await {
        let mut part = maybe_part.map_err(ApiError::UploadBodyLoadError)?;

        if part.name() == "has_header" {
            let value_bytes = load_part(part).await?;

            has_header = String::from_utf8(value_bytes)
                .map_err(|_| ApiError::UploadHasHeaderParseError)?
                .starts_with("true");
            debug!("Form part has_header is: {}", has_header);
        } else if part.name() == "schema" {
            let value_bytes = load_part(part).await?;

            schema = Some(Arc::new(
                schema_from_json(
                    &serde_json::from_slice::<serde_json::Value>(value_bytes.as_slice())
                        .map_err(ApiError::UploadSchemaDeserializationError)?,
                )
                .map_err(ApiError::UploadSchemaParseError)?,
            ));
        } else if part.name() == "data" || part.name() == "file" {
            filename = part
                .filename()
                .ok_or(ApiError::UploadMissingFile)?
                .to_string();

            // Write out the incoming bytes into the temporary file
            while let Some(maybe_bytes) = part.data().await {
                ref_temp_file.inner().write_all(
                    maybe_bytes.map_err(ApiError::UploadBodyLoadError)?.chunk(),
                )?;
            }
        }
    }

    if filename.is_empty() {
        return Err(ApiError::UploadMissingFile);
    }

    let file_type = match filename
        .split('.')
        .last()
        .ok_or_else(|| ApiError::UploadMissingFilenameExtension(filename.clone()))?
    {
        "csv" => FileType::CSV,
        "parquet" => FileType::PARQUET,
        _ => return Err(ApiError::UploadUnsupportedFileFormat(filename)),
    };

    // Execute the plan and persist objects as well as table/partition metadata
    let temp_path = ref_temp_file.path();
    let table = context
        .file_to_table(
            temp_path.display().to_string(),
            file_type,
            schema,
            has_header,
            schema_name.clone(),
            table_name.clone(),
        )
        .await?;

    Ok(warp::reply::with_status(
        Ok::<String, ApiError>(format!(
            "{filename} appended to table {table_name} version {}\n",
            table.version()
        )),
        StatusCode::OK,
    )
    .into_response())
}

async fn load_part(mut part: Part) -> Result<Vec<u8>, ApiError> {
    let mut bytes: Vec<u8> = vec![];
    while let Some(maybe_bytes) = part.data().await {
        bytes.extend(maybe_bytes.map_err(ApiError::UploadBodyLoadError)?.chunk())
    }
    Ok(bytes)
}

/// GET /healthz or /readyz
pub async fn health_endpoint(context: Arc<SeafowlContext>) -> Result<Response, ApiError> {
    #[cfg(feature = "frontend-arrow-flight")]
    if let Some(flight) = &context.config.frontend.flight {
        // TODO: run SELECT 1 or something similar?
        if let Err(err) = FlightServiceClient::connect(format!(
            "http://{}:{}",
            flight.bind_host, flight.bind_port
        ))
        .await
        {
            warn!(%err, "Arrow Flight client can't connect, health check failed");
            return Ok(warp::reply::with_status(
                "not_ready",
                StatusCode::SERVICE_UNAVAILABLE,
            )
            .into_response());
        };
    };

    Ok(warp::reply::with_status("ready", StatusCode::OK).into_response())
}

pub fn filters(
    context: Arc<SeafowlContext>,
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

    let log = warp::log::custom(|info: Info<'_>| {
        let path = info.path();

        #[cfg(feature = "metrics")]
        {
            let route = if path.contains("/upload/") {
                "/upload".to_string()
            } else if path.contains("/q") {
                "/q".to_string()
            } else {
                path.to_string()
            };

            counter!(
                HTTP_REQUESTS,
                "method" => info.method().as_str().to_string(),
                // Omit a potential db prefix or url-encoded query from the path
                "route" => route,
                "status" => info.status().as_u16().to_string(),
            )
            .increment(1);
        }

        info!(
            target: module_path!(),
            "{} \"{} {} {:?}\" {} \"{}\" \"{}\" {:?}",
            info.remote_addr().map(|addr| addr.to_string()).unwrap_or("-".to_string()),
            info.method(),
            path,
            info.version(),
            info.status().as_u16(),
            info.referer().unwrap_or("-"),
            info.user_agent().unwrap_or("-"),
            info.elapsed(),
        );
    });

    // Cached read query
    let ctx = context.clone();
    let cached_read_query_route = warp::path!(String / "q" / String)
        .or(warp::any()
            .map(move || DEFAULT_DB.to_string())
            .and(warp::path!("q" / String)))
        .and(warp::path::end())
        .and(warp::get())
        .unify()
        .and(cached_read_query_authz(access_policy.clone()))
        .and(
            // Extract the query either from the JSON body or the header
            warp::body::json()
                .map(|b: QueryBody| Some(b.query))
                .or(warp::header::optional::<String>(QUERY_HEADER))
                .unify(),
        )
        .and(warp::header::optional::<String>(
            header::IF_NONE_MATCH.as_str(),
        ))
        .and(warp::any().map(move || ctx.clone()))
        .then(cached_read_query)
        .map(move |r: Result<Response, ApiError>| {
            if let Ok(response) = r {
                with_header(
                    response,
                    header::CACHE_CONTROL,
                    config.cache_control.clone(),
                )
                .into_response()
            } else {
                into_response(r)
            }
        });

    // Uncached read/write query
    let ctx = context.clone();
    let uncached_read_write_query_route = warp::path!(String / "q")
        .or(warp::any()
            .map(move || DEFAULT_DB.to_string())
            .and(warp::path!("q")))
        .and(warp::path::end())
        .and(warp::post())
        .unify()
        .and(with_auth(access_policy.clone()))
        .and(
            // Extract the query from the JSON body
            warp::body::json().map(|b: QueryBody| b.query).or_else(|r| {
                future::err(warp::reject::custom(ApiError::QueryParsingError(r)))
            }),
        )
        .and(warp::any().map(move || ctx.clone()))
        .then(uncached_read_write_query)
        .map(into_response);

    // Upload endpoint
    let ctx = context.clone();
    let upload_route = warp::path!(String / "upload" / String / String)
        .or(warp::any()
            .map(move || DEFAULT_DB.to_string())
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

    // Health-check/readiness probe
    let ctx = context;
    let health_route = warp::path!("healthz")
        .or(warp::path!("readyz"))
        .and(warp::path::end())
        .and(warp::get())
        .unify()
        .and(warp::any().map(move || ctx.clone()))
        .then(health_endpoint)
        .map(into_response);

    cached_read_query_route
        .or(uncached_read_write_query_route)
        .or(upload_route)
        .or(health_route)
        .with(cors)
        .with(log)
        .map(|r| with_header(r, header::VARY, VARY))
        .recover(handle_rejection)
}

pub async fn run_server(
    context: Arc<SeafowlContext>,
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
    use crate::testutils::{assert_header_is_float, schema_from_header};
    use crate::{
        context::{test_utils::in_memory_context, SeafowlContext},
        frontend::http::{filters, QUERY_HEADER, QUERY_TIME_HEADER},
    };

    fn http_config_from_access_policy_and_cache_control(
        access_policy: AccessPolicy,
        cache_control: Option<&str>,
    ) -> HttpFrontend {
        if cache_control.is_none() {
            return http_config_from_access_policy(access_policy);
        }
        HttpFrontend {
            read_access: access_policy.read,
            write_access: access_policy.write,
            cache_control: cache_control.unwrap().to_string(),
            ..HttpFrontend::default()
        }
    }

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
    ) -> Arc<SeafowlContext> {
        let mut context = Arc::new(in_memory_context().await);

        if let Some(db_name) = new_db {
            context
                .plan_query(&format!("CREATE DATABASE IF NOT EXISTS {db_name}"))
                .await
                .unwrap();

            context = context.scope_to_catalog(db_name.to_string());
        }

        context
            .plan_query("CREATE TABLE test_table(col_1 INT)")
            .await
            .unwrap();

        context
            .plan_query("INSERT INTO test_table VALUES (1)")
            .await
            .unwrap();

        if new_db.is_some() {
            // Re-scope to the original DB
            return context.scope_to_catalog(DEFAULT_DB.to_string());
        }

        context
    }

    async fn in_memory_context_with_modified_table(
        new_db: Option<&str>,
    ) -> Arc<SeafowlContext> {
        let mut context = in_memory_context_with_single_table(new_db).await;

        if let Some(db_name) = new_db {
            context = context.scope_to_catalog(db_name.to_string());
        }

        context
            .plan_query("INSERT INTO test_table VALUES (2)")
            .await
            .unwrap();

        if new_db.is_some() {
            // Re-scope to the original DB
            return context.scope_to_catalog(DEFAULT_DB.to_string());
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

    async fn query_cached_endpoint<R, H>(
        handler: &H,
        path: &str,
        maybe_body: Option<HashMap<&'_ str, &'_ str>>,
        maybe_headers: Option<Vec<(&'_ str, &'_ str)>>,
    ) -> Response<Bytes>
    where
        R: Reply,
        H: Filter<Extract = R, Error = Rejection> + Clone + 'static,
    {
        let mut builder = request().method("GET").path(path);

        if let Some(body) = maybe_body {
            builder = builder.json(&body);
        }

        if let Some(headers) = maybe_headers {
            for (key, value) in headers {
                builder = builder.header(key, value);
            }
        }

        builder.reply(handler).await
    }

    async fn query_uncached_endpoint<R, H>(
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
    #[case::query_in_path(PERCENT_ENCODED_SELECT_QUERY, None, None)]
    #[case::query_in_body(
        SELECT_QUERY_HASH,
        Some(HashMap::from([("query", SELECT_QUERY)])),
        None,
    )]
    #[case::query_in_header(
        SELECT_QUERY_HASH,
        None,
        Some(vec![(QUERY_HEADER, SELECT_QUERY)]),
    )]
    #[case::encoded_query_in_header(
        SELECT_QUERY_HASH,
        None,
        Some(vec![(QUERY_HEADER, PERCENT_ENCODED_SELECT_QUERY)]),
    )]
    #[tokio::test]
    async fn test_get_cached_no_etag(
        #[case] query_param: &str,
        #[case] maybe_body: Option<HashMap<&'_ str, &'_ str>>,
        #[case] maybe_headers: Option<Vec<(&'_ str, &'_ str)>>,
        #[values(None, Some("test_db"))] new_db: Option<&str>,
        #[values("", ".bin")] extension: &str,
        #[values(None, Some("private, max-age=86400"))] cache_control: Option<&str>,
    ) {
        let context = in_memory_context_with_single_table(new_db).await;
        let handler = filters(
            context,
            http_config_from_access_policy_and_cache_control(
                free_for_all(),
                cache_control,
            ),
        );

        let resp = query_cached_endpoint(
            &handler,
            make_uri(format!("/q/{query_param}{extension}"), new_db).as_str(),
            maybe_body,
            maybe_headers,
        )
        .await;

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.body(), "{\"c\":1}\n");
        assert_eq!(
            resp.headers().get(header::ETAG).unwrap().to_str().unwrap(),
            V1_ETAG
        );
        assert_eq!(
            resp.headers()
                .get(header::CACHE_CONTROL)
                .unwrap()
                .to_str()
                .unwrap(),
            cache_control.unwrap_or("max-age=43200, public")
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_get_cached_hash_no_query(
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
        assert_eq!(
            resp.body(),
            "SQL error: ParserError(\"Expected an SQL statement, found: 7\")"
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

    #[tokio::test]
    async fn test_get_uncached_read_nonexistent_db() {
        let context = in_memory_context_with_single_table(None).await;
        let handler = filters(context, http_config_from_access_policy(free_for_all()));

        let resp =
            query_uncached_endpoint(&handler, SELECT_QUERY, Some("missing_db"), None)
                .await;

        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            resp.body(),
            "Error during planning: Catalog \"missing_db\" doesn't exist"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_get_uncached_read_query(
        #[values(None, Some("test_db"))] new_db: Option<&str>,
    ) {
        let context = in_memory_context_with_single_table(new_db).await;
        let handler = filters(context, http_config_from_access_policy(free_for_all()));

        let resp = query_uncached_endpoint(&handler, SELECT_QUERY, new_db, None).await;

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

        let resp = query_uncached_endpoint(&handler, INSERT_QUERY, new_db, None).await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.body(), "");

        let resp = query_uncached_endpoint(&handler, SELECT_QUERY, new_db, None).await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.body(), "{\"c\":2}\n");
    }

    #[rstest]
    #[tokio::test]
    async fn test_error_parse(#[values(None, Some("test_db"))] new_db: Option<&str>) {
        let context = in_memory_context_with_single_table(new_db).await;
        let handler = filters(context, http_config_from_access_policy(free_for_all()));

        let resp = query_uncached_endpoint(&handler, "SLEECT 1", new_db, None).await;
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

        let resp = query_uncached_endpoint(
            &handler,
            "CREATE FUNCTION what_function",
            new_db,
            None,
        )
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

        let resp = query_uncached_endpoint(
            &handler,
            "SELECT * FROM missing_table",
            new_db,
            None,
        )
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
            query_uncached_endpoint(&handler, "SELECT 'notanint'::int", new_db, None)
                .await;
        assert_eq!(resp.status(), StatusCode::OK);

        let error_msg = String::from_utf8_lossy(resp.body());
        assert_eq!(
            error_msg,
            "Cast error: Cannot cast string 'notanint' to value of Int32 type"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_error_json_conversion(
        #[values(None, Some("test_db"))] new_db: Option<&str>,
    ) {
        let context = in_memory_context_with_single_table(new_db).await;
        let handler = filters(context, http_config_from_access_policy(free_for_all()));

        let resp =
            query_uncached_endpoint(&handler, "SELECT 1::NUMERIC", new_db, None).await;
        assert_eq!(resp.status(), StatusCode::OK);

        let error_msg = String::from_utf8_lossy(resp.body());
        assert_eq!(
            error_msg,
            "Json error: data type Decimal128(38, 10) not supported in nested map for json writer"
        );
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
            query_uncached_endpoint(&handler, "SELECT * FROM test_table", new_db, None)
                .await;
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

        let resp = query_uncached_endpoint(
            &handler,
            "DROP TABLE test_table",
            new_db,
            Some("somepw"),
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
            query_uncached_endpoint(&handler, "DROP TABLE test_table", new_db, None)
                .await;
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

        let resp = query_uncached_endpoint(&handler, "SELECT 1", new_db, None).await;
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

        let resp = query_uncached_endpoint(
            &handler,
            "DROP TABLE test_table",
            new_db,
            Some("somereadpw"),
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

        let resp = query_uncached_endpoint(
            &handler,
            "DROP TABLE test_table",
            new_db,
            Some("somepw"),
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
            query_uncached_endpoint(&handler, "DROP TABLE test_table", new_db, None)
                .await;
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

        let resp = query_uncached_endpoint(
            &handler,
            "DROP TABLE test_table",
            new_db,
            Some("somepw"),
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
            query_uncached_endpoint(&handler, "SELECT 1", new_db, Some("otherpw")).await;
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

        let resp = query_uncached_endpoint(
            &handler,
            "DROP TABLE test_table;CREATE TABLE test_table(\"key\" VARCHAR);
            INSERT INTO test_table VALUES('hey')",
            new_db,
            Some("somepw"),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);

        let resp = query_uncached_endpoint(
            &handler,
            "SELECT * FROM test_table;",
            new_db,
            Some("somepw"),
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

        let resp = query_uncached_endpoint(
            &handler,
            "DROP TABLE test_table;CREATE TABLE test_table(\"key\" VARCHAR);
            INSERT INTO test_table VALUES('hey');SELECT * FROM test_table;",
            new_db,
            Some("somepw"),
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
            query_uncached_endpoint(&handler, "SELECT * FROM test_table;DROP TABLE test_table;CREATE TABLE test_table(\"key\" VARCHAR);
            INSERT INTO test_table VALUES('hey')", new_db, Some("somepw"))
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
            query_uncached_endpoint(&handler, "DROP TABLE test_table;CREATE TABLE test_table(\"key\" VARCHAR);
            INSERT INTO test_table VALUES('hey');SELECT * FROM test_table;SELECT * FROM test_table;", new_db, Some("somepw"))
                .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        assert!(String::from_utf8(resp.body().to_vec())
            .unwrap()
            .contains("Only one read statement is allowed"));
    }

    #[rstest]
    #[case::cached_get(
        "GET",
        "/q/20d000bdf79cec1a968b422ed8c719122b236ec3831f00114c7dfd09f9a62d83"
    )]
    #[case::uncached_post("POST", "/q")]
    #[tokio::test]
    async fn test_http_type_conversion_and_timing_header(
        #[case] method: &str,
        #[case] path: &str,
    ) {
        let context = Arc::new(in_memory_context().await);
        let handler = filters(
            context.clone(),
            http_config_from_access_policy(AccessPolicy::free_for_all()),
        );

        let query = r#"
SELECT
  1::TINYINT AS tinyint_val,
  1000::SMALLINT AS smallint_val,
  1000000::INT AS integer_val,
  1000000000::BIGINT AS bigint_val,
  'c'::CHAR AS char_val,
  'varchar'::VARCHAR AS varchar_val,
  'text'::TEXT AS text_val,
  'string'::STRING AS string_val,
  -- Unsupported in the JSON output 12.345::DECIMAL(5, 2) AS decimal_val,
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
                r#"{"bigint_val":1000000000,"bool_val":true,"char_val":"c","date_val":"2022-01-01","double_val":12.345678910111213,"float_val":12.345,"int_array_val":[1,2,3,4,5],"integer_val":1000000,"real_val":12.345,"smallint_val":1000,"string_val":"string","text_array_val":["one","two"],"text_val":"text","timestamp_val":"2022-01-01T12:03:11.123456","tinyint_val":1,"varchar_val":"varchar"}
"#
            )
        );

        // Assert the "request-to-response" time header is present
        assert!(resp.headers().contains_key(QUERY_TIME_HEADER));
        // Assert that it's a float
        let header_value = resp.headers().get(QUERY_TIME_HEADER).unwrap();
        assert_header_is_float(header_value);
    }
}
