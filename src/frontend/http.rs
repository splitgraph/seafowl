use std::{net::SocketAddr, sync::Arc};

use arrow::json::LineDelimitedWriter;
use datafusion::{
    datasource::DefaultTableSource,
    logical_plan::{LogicalPlan, PlanVisitor, TableScan},
};
use hex::encode;
use log::debug;
use serde_json::json;
use sha2::{Digest, Sha256};
use warp::{hyper::StatusCode, Filter, Reply};

use crate::{
    config::HttpFrontend, context::SeafowlContext, data_types::TableVersionId,
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

// GET /q/[query hash]
pub fn cached_read_query(
    context: Arc<dyn SeafowlContext>,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("q" / String)
        .and(warp::header::<String>(QUERY_HEADER))
        .and(warp::header::optional::<String>(IF_NONE_MATCH))
        .then(move |query_hash, query: String, if_none_match| {
            let context = context.clone();

            async move {
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
        })
}

pub fn filters(
    context: Arc<dyn SeafowlContext>,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    cached_read_query(context)
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
    use std::sync::Arc;

    use arrow::{array::Int8Array, record_batch::RecordBatch};
    use datafusion::{
        logical_plan::{
            provider_as_source, CreateCatalog, DFSchema, LogicalPlan, LogicalPlanBuilder,
        },
        physical_plan::empty::EmptyExec,
    };
    use mockall::predicate;
    use warp::{
        hyper::{header::IF_NONE_MATCH, StatusCode},
        test::request,
    };

    use crate::{
        catalog::MockRegionCatalog,
        context::{MockSeafowlContext, SeafowlContext},
        data_types::TableVersionId,
        frontend::http::ETAG,
        provider::SeafowlTable,
        schema::Schema as SeafowlSchema,
    };

    use super::{cached_read_query, QUERY_HEADER};

    use datafusion::arrow::datatypes::{
        DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    };

    /// Build a fully mocked context that plans a scan through a table with a certain version
    /// and returns a single-value result.
    fn build_mock_context_with_table_version(
        table_version_id: TableVersionId,
    ) -> Arc<dyn SeafowlContext> {
        let arrow_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "col1",
            ArrowDataType::Int8,
            true,
        )]));
        let region_catalog = MockRegionCatalog::new();
        let region_catalog_ptr = Arc::new(region_catalog);

        let table_source = SeafowlTable {
            name: Arc::from("some_table"),
            schema: Arc::new(SeafowlSchema {
                arrow_schema: arrow_schema.clone(),
            }),
            table_id: 0,
            table_version_id,
            catalog: region_catalog_ptr,
        };
        let plan = LogicalPlanBuilder::scan(
            "some_table",
            provider_as_source(Arc::new(table_source)),
            None,
        )
        .unwrap()
        .build()
        .unwrap();

        let mut sf_context = MockSeafowlContext::new();
        sf_context.expect_reload_schema().return_const(());

        sf_context
            .expect_create_logical_plan()
            .with(predicate::eq(SELECT_QUERY))
            .returning(move |_| Ok(plan.clone()));

        let schema = arrow_schema.clone();
        sf_context
            .expect_create_physical_plan()
            .returning(move |_| Ok(Arc::new(EmptyExec::new(true, schema.clone()))));

        let schema = arrow_schema;
        sf_context.expect_collect().returning(move |_| {
            Ok(vec![RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new([Some(1)].into_iter().collect::<Int8Array>())],
            )?])
        });

        Arc::new(sf_context)
    }

    fn build_mock_context_write_query() -> Arc<dyn SeafowlContext> {
        let plan = LogicalPlan::CreateCatalog(CreateCatalog {
            catalog_name: "catalog".to_string(),
            if_not_exists: true,
            schema: Arc::new(DFSchema::empty()),
        });

        let mut sf_context = MockSeafowlContext::new();
        sf_context.expect_reload_schema().return_const(());

        sf_context
            .expect_create_logical_plan()
            .with(predicate::eq(CREATE_QUERY))
            .returning(move |_| Ok(plan.clone()));

        Arc::new(sf_context)
    }

    const SELECT_QUERY: &str = "SELECT COUNT(*) FROM testcol.some_table";
    const CREATE_QUERY: &str = "CREATE DATABASE catalog";
    const SELECT_QUERY_HASH: &str =
        "aa352ab71747f77b955ff09bf28ab9b60db0ce7c10022fcbd4961808063443b8";
    const CREATE_QUERY_HASH: &str =
        "58a04cbb43d016a84d478e8291c34535771068cae2b229a550f7748a8ccef2a0";
    const V1_ETAG: &str =
        "d0bca111f8628137adc4c16f123496dcdd1d590d06cb5d9acd68b39fe656fb97";
    const V2_ETAG: &str =
        "080a9ed428559ef602668b4c00f114f1a11c3f6b02a435f0bdc154578e4d7f22";

    #[tokio::test]
    async fn test_get_cached_hash_mismatch() {
        let context = build_mock_context_with_table_version(0);
        let handler = cached_read_query(context);

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
        let context = build_mock_context_write_query();
        let handler = cached_read_query(context);

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
        let context = build_mock_context_with_table_version(0);
        let handler = cached_read_query(context);

        let resp = request()
            .method("GET")
            .path(format!("/q/{}", SELECT_QUERY_HASH).as_str())
            .header(QUERY_HEADER, SELECT_QUERY)
            .reply(&handler)
            .await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.body(), "{\"col1\":1}\n");
        assert_eq!(resp.headers().get(ETAG).unwrap().to_str().unwrap(), V1_ETAG);
    }

    #[tokio::test]
    async fn test_get_cached_reuse_etag() {
        // Pass the same ETag as If-None-Match, should return a 301

        let context = build_mock_context_with_table_version(0);
        let handler = cached_read_query(context);

        let resp = request()
            .method("GET")
            .path(format!("/q/{}", SELECT_QUERY_HASH).as_str())
            .header(QUERY_HEADER, "SELECT COUNT(*) FROM testcol.some_table")
            .header(IF_NONE_MATCH, V1_ETAG)
            .reply(&handler)
            .await;
        assert_eq!(resp.status(), StatusCode::NOT_MODIFIED);
        assert_eq!(resp.body(), "NOT_MODIFIED");
    }

    #[tokio::test]
    async fn test_get_cached_etag_new_version() {
        // Pass the same ETag as If-None-Match, but the table version changed -> reruns the query

        let context = build_mock_context_with_table_version(1);
        let handler = cached_read_query(context);

        let resp = request()
            .method("GET")
            .path(format!("/q/{}", SELECT_QUERY_HASH).as_str())
            .header(QUERY_HEADER, "SELECT COUNT(*) FROM testcol.some_table")
            .header(ETAG, V1_ETAG)
            .reply(&handler)
            .await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.body(), "{\"col1\":1}\n");
        assert_eq!(resp.headers().get(ETAG).unwrap().to_str().unwrap(), V2_ETAG);
    }
}
