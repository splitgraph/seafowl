use itertools::Itertools;
use warp::hyper::body::to_bytes;
use warp::hyper::Body;
use warp::hyper::Client;
use warp::hyper::Response;
use warp::hyper::StatusCode;

// Hack because integration tests do not set cfg(test)
// https://users.rust-lang.org/t/sharing-helper-function-between-unit-and-integration-tests/9941/2
#[allow(clippy::duplicate_mod)]
#[allow(dead_code)]
#[path = "../../src/testutils.rs"]
mod testutils;

pub async fn response_text(response: Response<Body>) -> String {
    let body_bytes = to_bytes(response.into_body()).await.unwrap();
    String::from_utf8(body_bytes.to_vec()).unwrap()
}

pub async fn get_metrics(metrics_type: &str, port: u16) -> Vec<String> {
    let resp = Client::new()
        .get(
            format!("http://127.0.0.1:{port}/metrics")
                .try_into()
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let mut lines = response_text(resp)
        .await
        .lines()
        .filter(|l| l.contains(metrics_type) && !l.contains("/upload"))
        .map(String::from)
        .collect_vec();
    lines.sort();
    lines
}
