use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use futures::Future;
use futures::FutureExt;
use seafowl::auth::AccessPolicy;
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

use std::net::SocketAddr;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Sender;

/// Make an HTTP server that listens on a random free port,
/// uses an in-memory SQLite and requires a password ("write_password") for writes
/// Returns the server's address, the actual server Future and a channel to stop the server
async fn make_read_only_http_server() -> (
    SocketAddr,
    Pin<Box<dyn Future<Output = ()> + Send>>,
    Sender<()>,
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

    let config = load_config_from_string(config_text, false).unwrap();
    let context = build_context(&config).await;

    let filters = filters(
        Arc::from(context),
        AccessPolicy::from_config(&config.frontend.http.unwrap()),
    );
    let (tx, rx) = oneshot::channel();
    let (addr, server) = warp::serve(filters).bind_with_graceful_shutdown(
        // Pass port :0 to pick a random free port
        "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
        async {
            rx.await.ok();
        },
    );

    dbg!(format!("Starting the server on {:?}", addr));
    (addr, server.boxed(), tx)
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

#[tokio::test]
async fn test_http_server_reader_writer() {
    // It's questionable how much value this testing adds on top of the tests in http.rs, but we do:
    //   - test the code consumes the config correctly, which we don't do in HTTP tests
    //   - hit the server that's actually listening on a port instead of calling warp routines directly.
    // Still, this test is mostly to make sure the whole thing roughly does what is intended by the config.

    let (addr, server, terminate) = make_read_only_http_server().await;

    tokio::task::spawn(server);
    let client = Client::new();
    let uri = format!("http://{}/q", addr);

    // POST SELECT 1 as a read-only user
    let resp = post_query(&client, &uri, "SELECT 1", None).await;
    dbg!(&resp);
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(response_text(resp).await, "{\"Int64(1)\":1}\n");

    // POST CREATE TABLE as a read-only user
    let resp =
        post_query(&client, &uri, "CREATE TABLE test_table (col INTEGER)", None).await;
    dbg!(&resp);
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    assert_eq!(response_text(resp).await, "WRITE_FORBIDDEN");

    // Same, wrong token
    let resp = post_query(
        &client,
        &uri,
        "CREATE TABLE test_table (col INTEGER)",
        Some("wrongpw"),
    )
    .await;
    dbg!(&resp);
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(response_text(resp).await, "INVALID_ACCESS_TOKEN");

    // Perform a write correctly
    let resp = post_query(
        &client,
        &uri,
        "CREATE TABLE test_table (col INTEGER)",
        Some("write_password"),
    )
    .await;
    dbg!(&resp);
    assert_eq!(resp.status(), StatusCode::OK);

    // TODO use single statement when https://github.com/splitgraph/seafowl/issues/48 lands
    let resp = post_query(
        &client,
        &uri,
        "INSERT INTO test_table VALUES(1)",
        Some("write_password"),
    )
    .await;
    dbg!(&resp);
    assert_eq!(resp.status(), StatusCode::OK);

    // SELECT from the table as a read-only user
    let resp = post_query(&client, &uri, "SELECT * FROM test_table", None).await;
    dbg!(&resp);
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(response_text(resp).await, "{\"col\":1}\n");

    // Stop the server
    // NB this won't run if the test fails, but at that point we're terminating the process
    // anyway. Maybe it'll become a problem if we have a bunch of tests running that all
    // start servers and don't stop them.
    terminate.send(()).unwrap();
}
