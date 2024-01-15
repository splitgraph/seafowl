use std::cmp::min;
use std::str::FromStr;
use std::sync::Arc;

use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field, Schema};

use arrow::record_batch::RecordBatch;
use arrow_integration_test::schema_from_json;
use datafusion::parquet::arrow::ArrowWriter;
use futures::TryStreamExt;
use itertools::Itertools;
use object_store::path::Path;
use object_store::ObjectStore;
use percent_encoding::percent_decode_str;
use warp::http::{HeaderMap, HeaderValue};
use warp::hyper::header;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

struct MockResponse {
    supports_ranges: bool,
    body: Vec<u8>,
}

impl Respond for MockResponse {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        if self.supports_ranges && request.headers.contains_key(&"Range".into()) {
            // Bunch of unwraps/expects here to simplify test code (not testing range handling
            // in depth)
            let range_header = request
                .headers
                .get(&"Range".into())
                .unwrap()
                .get(0)
                .unwrap()
                .to_string();
            let range = range_header
                .strip_prefix("bytes=")
                .expect("Range doesn't start with bytes=");
            let mut range_vals = range.split('-');
            let start = range_vals.next().unwrap().parse::<u64>().unwrap() as usize;
            let end = range_vals.next().unwrap().parse::<u64>().unwrap() as usize;

            let body = self.body[start..min(end + 1, self.body.len())].to_vec();
            let body_len = body.len();

            ResponseTemplate::new(206) // Partial Content
                .set_body_bytes(body)
                .append_header(
                    "Content-Disposition",
                    "attachment; filename=\"file.parquet\"",
                )
                .append_header("Content-Length", body_len.to_string().as_str())
                .append_header(
                    "Content-Range",
                    format!("bytes {}-{}/{}", start, end, self.body.len()).as_str(),
                )
        } else {
            ResponseTemplate::new(200)
                .set_body_bytes(self.body.clone())
                .append_header(
                    "Content-Disposition",
                    "attachment; filename=\"file.parquet\"",
                )
                .append_header("Content-Length", self.body.len().to_string().as_str())
        }
    }
}

pub async fn make_mock_parquet_server(
    supports_ranges: bool,
    supports_head: bool,
) -> (MockServer, Vec<u8>) {
    // Make a simple Parquet file
    let schema = Arc::new(Schema::new(vec![Field::new(
        "col_1",
        DataType::Int32,
        true,
    )]));

    let input_batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )
    .unwrap();

    let mut buf = Vec::new();

    {
        let mut writer = ArrowWriter::try_new(&mut buf, schema, None).unwrap();
        writer.write(&input_batch).unwrap();
        writer.close().unwrap();
    }

    // Clone the body to return it to the caller
    let body = buf.clone();

    // Make a mock server that returns this file
    let mock_server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/some/file.parquet"))
        .respond_with(MockResponse {
            supports_ranges,
            body: buf,
        })
        .mount(&mock_server)
        .await;

    if supports_head {
        Mock::given(method("HEAD"))
            .and(path("/some/file.parquet"))
            .respond_with(
                ResponseTemplate::new(200)
                    .append_header("Content-Length", body.len().to_string().as_str())
                    .append_header("Accept-Ranges", "bytes"),
            )
            .mount(&mock_server)
            .await;
    } else {
        // For presigned S3 URLs, the server doesn't support HEAD requests
        Mock::given(method("HEAD"))
            .respond_with(ResponseTemplate::new(403))
            .mount(&mock_server)
            .await;
    }

    (mock_server, body)
}

pub async fn assert_uploaded_objects(
    object_store: Arc<dyn ObjectStore>,
    expected: Vec<Path>,
) {
    let actual = object_store
        .list(None)
        .map_ok(|meta| meta.location)
        .try_collect::<Vec<Path>>()
        .await
        .map(|p| p.into_iter().sorted().collect_vec())
        .unwrap();
    assert_eq!(expected.into_iter().sorted().collect_vec(), actual);
}

pub fn schema_from_header(headers: &HeaderMap<HeaderValue>) -> Schema {
    let schema_escaped = headers
        .get(header::CONTENT_TYPE)
        .expect("content-type header")
        .to_str()
        .expect("content-type header as a str")
        .split("arrow-schema=")
        .last()
        .expect("arrow-schema last param in content-type header");
    let schema_str = percent_decode_str(schema_escaped)
        .decode_utf8()
        .expect("escaped schema decodable")
        .to_string();
    let schema_json = serde_json::from_str::<serde_json::Value>(schema_str.as_str())
        .expect("decoded schema is valid JSON");

    schema_from_json(&schema_json).expect("arrow schema reconstructable from JSON")
}

pub fn assert_header_is_float(header: &HeaderValue) -> bool {
    let float_str = header.to_str().unwrap();
    let parsed_float = f64::from_str(float_str).unwrap();
    parsed_float.is_finite()
}
