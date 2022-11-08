use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use tokio::process::Command;

use futures::Future;
use futures::FutureExt;
use seafowl::config::context::build_context;
use seafowl::config::schema::load_config_from_string;

use seafowl::frontend::http::filters;
use warp::hyper::body::to_bytes;
use warp::hyper::client::HttpConnector;
use warp::hyper::Body;
use warp::hyper::Client;
use warp::hyper::Method;
use warp::hyper::Request;
use warp::hyper::Response;
use warp::hyper::StatusCode;

use arrow::array::{Int32Array, StringArray};
use arrow::csv::WriterBuilder;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::assert_batches_eq;
use datafusion::from_slice::FromSlice;
use datafusion::parquet::arrow::ArrowWriter;
use itertools::Itertools;
use seafowl::context::SeafowlContext;
use std::net::SocketAddr;
use tempfile::Builder;
use test_case::test_case;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Sender;

/// Make an HTTP server that listens on a random free port,
/// uses an in-memory SQLite and requires a password ("write_password") for writes
/// Returns the server's address, the actual server Future and a channel to stop the server
async fn make_read_only_http_server() -> (
    SocketAddr,
    Pin<Box<dyn Future<Output = ()> + Send>>,
    Sender<()>,
    Arc<dyn SeafowlContext>,
) {
    let config_text = r#"
[object_store]
type = "memory"

[catalog]
type = "sqlite"
dsn = ":memory:"

[frontend.http]
# sha hash of "write_password"
write_access = "b786e07f52fc72d32b2163b6f63aa16344fd8d2d84df87b6c231ab33cd5aa125""#;

    let config = load_config_from_string(config_text, false, None).unwrap();
    let context = Arc::from(build_context(&config).await.unwrap());

    let filters = filters(context.clone(), config.frontend.http.unwrap());
    let (tx, rx) = oneshot::channel();
    let (addr, server) = warp::serve(filters).bind_with_graceful_shutdown(
        // Pass port :0 to pick a random free port
        "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
        async {
            rx.await.ok();
        },
    );

    dbg!(format!("Starting the server on {:?}", addr));
    (addr, server.boxed(), tx, context)
}

async fn response_text(response: Response<Body>) -> String {
    let body_bytes = to_bytes(response.into_body()).await.unwrap();
    String::from_utf8(body_bytes.to_vec()).unwrap()
}

fn query_body(query: &str) -> Body {
    Body::from(serde_json::to_string(&HashMap::from([("query", query)])).unwrap())
}

async fn post_query(
    client: &Client<HttpConnector>,
    uri: &str,
    query: &str,
    token: Option<&str>,
) -> Response<Body> {
    let mut builder = Request::builder()
        .method(Method::POST)
        .uri(uri)
        .header("content-type", "application/json");

    if let Some(t) = token {
        builder = builder.header("Authorization", format!("Bearer {}", t));
    }

    let req = builder.body(query_body(query)).unwrap();
    client.request(req).await.unwrap()
}
