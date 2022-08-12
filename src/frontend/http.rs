use std::{net::SocketAddr, sync::Arc};

use arrow::json::LineDelimitedWriter;
use datafusion::{
    datasource::DefaultTableSource,
    logical_plan::{LogicalPlan, PlanVisitor, TableScan},
};
use hex::encode;
use log::debug;
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use warp::{hyper::StatusCode, Filter, Reply};

use crate::{
    config::schema::HttpFrontend, context::SeafowlContext, data_types::TableVersionId,
    provider::SeafowlTable,
};

const QUERY_HEADER: &str = "X-Seafowl-Query";
const IF_NONE_MATCH: &str = "If-None-Match";
const ETAG: &str = "ETag";

#[derive(Default)]
struct ETagBuilderVisitor {
    table_versions: Vec<TableVersionId>,
}

impl PlanVisitor for ETagBuilderVisitor {
    type Error = ();

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

// POST /q
pub fn uncached_read_write_query(
    context: Arc<dyn SeafowlContext>,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("q")
        .and(warp::post())
        .and(
            // Extract the query from the JSON body
            warp::body::json().map(|b: QueryBody| b.query),
        )
        .then(move |query: String| {
            let context = context.clone();
            async move {
                context.reload_schema().await;
                // TODO: handle/propagate errors
                // TODO (when authz is implemented) check for read-only queries
                let physical = context.plan_query(&query).await.unwrap();
                let batches = context.collect(physical).await.unwrap();

                let mut buf = Vec::new();
                let mut writer = LineDelimitedWriter::new(&mut buf);
                writer.write_batches(&batches).unwrap();
                writer.finish().unwrap();

                buf.into_response()
            }
        })
}

// GET /q/[query hash]
pub fn cached_read_query(
    context: Arc<dyn SeafowlContext>,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("q" / String)
        .and(warp::get())
        .and(
            // Extract the query either from the header or from the JSON body
            warp::header::<String>(QUERY_HEADER)
                .or(warp::body::json().map(|b: QueryBody| b.query))
                .unify(),
        )
        .and(warp::header::optional::<String>(IF_NONE_MATCH))
        .then(
            move |query_hash: String, query: String, if_none_match: Option<String>| {
                let context = context.clone();

                async move {
                    // Ignore dots at the end
                    let query_hash = query_hash.split('.').next().unwrap();

                    context.reload_schema().await;
                    let mut hasher = Sha256::new();
                    hasher.update(&query);
                    let hash_str = encode(hasher.finalize());

                    debug!(
                        "Received query: {}, URL hash {}, actual hash {}",
                        query, query_hash, hash_str
                    );

                    // Verify the query hash matches the query
                    if query_hash != hash_str {
                        return warp::reply::with_status(
                            "HASH_MISMATCH",
                            StatusCode::BAD_REQUEST,
                        )
                        .into_response();
                    }

                    // Plan the query
                    // TODO handle error
                    let plan = context.create_logical_plan(&query).await.unwrap();
                    debug!("Query plan: {:?}", plan);

                    // Write queries should come in as POST requests
                    match plan {
                        LogicalPlan::CreateExternalTable(_)
                        | LogicalPlan::CreateMemoryTable(_)
                        | LogicalPlan::CreateView(_)
                        | LogicalPlan::CreateCatalogSchema(_)
                        | LogicalPlan::CreateCatalog(_)
                        | LogicalPlan::DropTable(_)
                        | LogicalPlan::Analyze(_)
                        | LogicalPlan::Extension(_) => {
                            return warp::reply::with_status(
                                "NOT_READ_ONLY_QUERY",
                                StatusCode::METHOD_NOT_ALLOWED,
                            )
                            .into_response()
                        }
                        _ => (),
                    };

                    // Pre-execution check: if ETags match, we don't need to re-execute the query
                    let etag = plan_to_etag(&plan);
                    debug!("ETag: {}, if-none-match header: {:?}", etag, if_none_match);

                    if let Some(if_none_match) = if_none_match {
                        if etag == if_none_match {
                            return warp::reply::with_status(
                                "NOT_MODIFIED",
                                StatusCode::NOT_MODIFIED,
                            )
                            .into_response();
                        }
                    }

                    // Guess we'll have to actually run the query
                    let physical = context.create_physical_plan(&plan).await.unwrap();
                    let batches = context.collect(physical).await.unwrap();

                    let mut buf = Vec::new();
                    let mut writer = LineDelimitedWriter::new(&mut buf);
                    writer.write_batches(&batches).unwrap();
                    writer.finish().unwrap();

                    warp::reply::with_header(buf, ETAG, etag).into_response()
                }
            },
        )
}

pub fn filters(
    context: Arc<dyn SeafowlContext>,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    let cors = warp::cors()
        .allow_any_origin()
        .allow_headers(vec!["X-Seafowl-Query", "Authorization", "Content-Type"])
        .allow_methods(vec!["GET", "POST"]);

    cached_read_query(context.clone())
        .or(uncached_read_write_query(context))
        .with(cors)
}

pub async fn run_server(context: Arc<dyn SeafowlContext>, config: HttpFrontend) {
    let filters = filters(context);

    let socket_addr: SocketAddr = format!("{}:{}", config.bind_host, config.bind_port)
        .parse()
        .expect("Error parsing the listen address");
    warp::serve(filters).run(socket_addr).await;
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use warp::{
        hyper::{header::IF_NONE_MATCH, StatusCode},
        test::request,
    };

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
        let handler = filters(context);

        let resp = request()
            .method("GET")
            .path("/q/wrong-hash")
            .header(QUERY_HEADER, SELECT_QUERY)
            .reply(&handler)
            .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        assert_eq!(resp.body(), "HASH_MISMATCH");
    }

    #[tokio::test]
    async fn test_get_cached_write_query_error() {
        let context = in_memory_context_with_single_table().await;
        let handler = filters(context);

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
        let handler = filters(context);

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
        let handler = filters(context);

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
        let handler = filters(context);

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
        let handler = filters(context);

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
        let handler = filters(context);

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
        let handler = filters(context);

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

    #[tokio::test]
    async fn test_get_uncached_read_query() {
        let context = in_memory_context_with_single_table().await;
        let handler = filters(context);

        let resp = request()
            .method("POST")
            .path("/q")
            .json(&HashMap::from([("query", SELECT_QUERY)]))
            .reply(&handler)
            .await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.body(), "{\"c\":1}\n");
    }

    #[tokio::test]
    async fn test_get_uncached_write_query() {
        let context = in_memory_context_with_single_table().await;
        let handler = filters(context);

        let resp = request()
            .method("POST")
            .path("/q")
            .json(&HashMap::from([("query", INSERT_QUERY)]))
            .reply(&handler)
            .await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.body(), "");

        let resp = request()
            .method("POST")
            .path("/q")
            .json(&HashMap::from([("query", SELECT_QUERY)]))
            .reply(&handler)
            .await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.body(), "{\"c\":2}\n");
    }

    #[tokio::test]
    async fn test_http_type_conversion() {
        let context = Arc::new(in_memory_context().await);
        let handler = filters(context);

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
