use std::cmp::min;
use std::sync::Arc;

use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field, Schema};

use arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
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

pub async fn make_mock_parquet_server(supports_ranges: bool) -> (MockServer, Vec<u8>) {
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

    Mock::given(method("HEAD"))
        .and(path("/some/file.parquet"))
        .respond_with(
            ResponseTemplate::new(200)
                .append_header("Content-Length", body.len().to_string().as_str()),
        )
        .mount(&mock_server)
        .await;

    (mock_server, body)
}
