use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use tokio::process::Command;

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

    let config = load_config_from_string(config_text, false).unwrap();
    let context = Arc::from(build_context(&config).await.unwrap());

    let filters = filters(
        context.clone(),
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

#[tokio::test]
async fn test_http_server_reader_writer() {
    // It's questionable how much value this testing adds on top of the tests in http.rs, but we do:
    //   - test the code consumes the config correctly, which we don't do in HTTP tests
    //   - hit the server that's actually listening on a port instead of calling warp routines directly.
    // Still, this test is mostly to make sure the whole thing roughly does what is intended by the config.

    let (addr, server, terminate, _) = make_read_only_http_server().await;

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
    let (addr, server, terminate, context) = make_read_only_http_server().await;

    tokio::task::spawn(server);

    let table_name = format!("{}_table", file_format);

    // Prepare the schema + data (a single record batch) which we'll save to a temp file via
    // a corresponding writer
    let schema = Arc::new(Schema::new(vec![
        Field::new("number", DataType::Int32, false),
        Field::new("parity", DataType::Utf8, false),
    ]));

    // For CSV uploads we can supply the schema as another part of the multipart request, to
    // remove the ambiguity resulting from automatic schema inference
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

    // Open a temp file to write the data into
    let mut named_tempfile = Builder::new()
        .suffix(format!(".{}", file_format).as_str())
        .tempfile()
        .unwrap();

    // Write out the CSV/Parquet format data to a temp file
    // drop the writer early to release the borrow.
    if file_format == "csv" {
        let mut writer = WriterBuilder::new()
            .has_headers(if let Some(has_headers) = add_headers {
                has_headers
            } else {
                true
            })
            .build(&mut named_tempfile);
        writer.write(&input_batch).unwrap();
    } else if file_format == "parquet" {
        let mut writer = ArrowWriter::try_new(&mut named_tempfile, schema, None).unwrap();
        writer.write(&input_batch).unwrap();
        writer.close().unwrap();
    }

    // Generate curl arguments
    let mut curl_args: Vec<String> = vec![
        "-H".to_string(),
        "Authorization: Bearer write_password".to_string(),
    ];
    if include_schema {
        curl_args.append(&mut vec![
            "-F".to_string(),
            format!("schema={};type=application/json", schema_json),
        ]);
    }
    if let Some(has_headers) = add_headers {
        curl_args.append(&mut vec![
            "-F".to_string(),
            format!("has_header={}", has_headers),
        ])
    }
    curl_args.append(&mut vec![
        "-F".to_string(),
        format!("data=@{}", named_tempfile.path().to_str().unwrap()),
        format!("http://{}/upload/test_upload/{}", addr, table_name),
    ]);

    let mut child = Command::new("curl").args(curl_args).spawn().unwrap();
    let status = child.wait().await.unwrap();
    dbg!("Upload status is {}", status);

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

    terminate.send(()).unwrap();
}

#[tokio::test]
async fn test_upload_to_existing_table() {
    let (addr, server, terminate, context) = make_read_only_http_server().await;

    tokio::task::spawn(server);

    // Create a pre-existing table to upload into
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

    let mut named_tempfile = Builder::new().suffix(".parquet").tempfile().unwrap();
    // drop the writer early to release the borrow.
    {
        let mut writer = ArrowWriter::try_new(&mut named_tempfile, schema, None).unwrap();
        writer.write(&input_batch).unwrap();
        writer.close().unwrap();
    }

    let mut child = Command::new("curl")
        .args(&[
            "-H",
            "Authorization: Bearer write_password",
            "-F",
            format!("data=@{}", named_tempfile.path().to_str().unwrap()).as_str(),
            format!("http://{}/upload/public/test_table", addr).as_str(),
        ])
        .spawn()
        .unwrap();
    let status = child.wait().await.unwrap();
    dbg!("Upload status is {}", status);

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

    // Now try with schema that doesn't matches the existing table (re-use input batch from before)
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

    // Open a temp file to write the data into
    let mut named_tempfile = Builder::new().suffix(".parquet").tempfile().unwrap();
    // drop the writer early to release the borrow.
    {
        let mut writer = ArrowWriter::try_new(&mut named_tempfile, schema, None).unwrap();
        writer.write(&input_batch).unwrap();
        writer.close().unwrap();
    }

    let output = Command::new("curl")
        .args(&[
            "-H",
            "Authorization: Bearer write_password",
            "-F",
            format!("data=@{}", named_tempfile.path().to_str().unwrap()).as_str(),
            format!("http://{}/upload/public/test_table", addr).as_str(),
        ])
        .output()
        .await
        .unwrap();

    assert_eq!(
        "Execution error: The table public.test_table already exists but has a different schema than the one provided.".to_string(),
        String::from_utf8(output.stdout).unwrap()
    );

    terminate.send(()).unwrap();
}
