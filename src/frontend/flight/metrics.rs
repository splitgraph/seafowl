use metrics::counter;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tonic::body::BoxBody;
use tonic::codegen::{http, Body, StdError};
use tonic::Code;
use tower::{Layer, Service};
use tracing::info;

use crate::config::context::GRPC_REQUESTS;

#[derive(Clone)]
pub struct MetricsLayer {}

impl<S> Layer<S> for MetricsLayer {
    type Service = MetricsService<S>;

    fn layer(&self, service: S) -> Self::Service {
        MetricsService { inner: service }
    }
}

// This service implements the Log behavior
#[derive(Clone)]
pub struct MetricsService<S> {
    inner: S,
}

impl<B, S> Service<http::Request<B>> for MetricsService<S>
where
    B: Body + Send + 'static,
    B::Error: Into<StdError> + Send + 'static,
    S: Service<http::Request<B>, Response = http::Response<BoxBody>>
        + Clone
        + Send
        + 'static,
    S::Future: Send,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        // This is necessary due to some consistency issues, for details see
        // https://github.com/tower-rs/tower/issues/547#issuecomment-767629149
        let clone = self.inner.clone();
        let path = request.uri().path().to_string();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        Box::pin(async move {
            // Execute the actual gRPC call
            let response = inner.call(request).await;

            // Parse the status code for metrics
            let code = match response {
                Ok(ref r) => {
                    // Tonic converts any `Err`s into veritable `http::Response`s,
                    // by simply dumping the `Status` info into some headers inside
                    // of `Grpc::map_response`.
                    if let Some(header_value) = r.headers().get("grpc-status") {
                        Code::from_bytes(header_value.as_bytes())
                    } else {
                        // Happy path; if the original tonic result was `Ok` this translates
                        // to no grpc-specific headers (and should thus be inferred as `Ok`)
                        Code::Ok
                    }
                }
                Err(_) => {
                    // The above means that this is some internal protocol level error or similar.
                    // TODO: investigate when this happens.
                    info!(
                        path,
                        "Error in metrics middleware while executing inner gRPC call"
                    );
                    Code::Unknown
                }
            };

            counter!(
                GRPC_REQUESTS,
                "path" => path,
                "status" => (code as i32).to_string(),
            )
            .increment(1);

            response
        })
    }
}
