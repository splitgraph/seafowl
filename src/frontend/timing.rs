use std::time::Instant;
use warp::{Filter, Rejection, Reply};

pub fn instrument() -> impl Filter<Extract = (Instant,), Error = std::convert::Infallible> + Copy {
    warp::any().map(|| Instant::now())
}

pub async fn handle_request(
    start_time: Instant,
    body: warp::hyper::body::Bytes,
) -> Result<impl Reply, Rejection> {
    let elapsed = start_time.elapsed();
    let response_with_header = warp::reply::with_header(body, "X-Runtime", format!("{:?}", elapsed));
    Ok(response_with_header)
}