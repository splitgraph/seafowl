// Warp error handling and propagation
// Courtesy of https://github.com/seanmonstar/warp/pull/909#issuecomment-1184854848
//
// Usage:
//
//   1) A handler function, instead of returning a Warp reply/rejection, returns a
//   `Result<Reply, ApiError>.`
//
//   This is because rejections are meant to say "this filter can't handle this request, but maybe
//   some other can" (see https://github.com/seanmonstar/warp/issues/388#issuecomment-576453485).
//   A rejection means Warp will fall through to another filter and ultimately hit a rejection
//   handler, with people reporting rejections take way too long to process with more routes.
//
//   In our case, the error in our handler function is final and we also would like to be able
//   to use the ? operator to bail out of the handler if an error exists, which using a Result type
//   handles for us.
//
//   2) ApiError knows how to convert itself to an HTTP response + status code (error-specific), allowing
//   us to implement Reply for ApiError.
//
//   3) We can't implement Reply for Result<Reply, Reply> (we don't control Result), so we have to
//   add a final function `into_response` that converts our Result into a Response. We won't need
//   to do this when https://github.com/seanmonstar/warp/pull/909 is merged:
//
//   ```
//   .then(my_handler_func)
//   .map(into_response)
//   ```
//
//   4) Some errors (raised in filters that e.g. extract the authz context) can't be forwarded
//   like this, so we also use the Rejections mechanism just for those, implementing Reject for
//   ApiError and making a small handler that turns the error into a real HTTP response. We need
//   to add the handler to our filter as follows:
//
//   ```
//   .or(some_route)
//   .or(some_other_route)
//   .recover(handle_rejection)
//   ```
//
//   (maybe we need a recover for every route to minimize the amount of back and forth with Warp?)
use datafusion::error::DataFusionError;

use warp::hyper::{Body, Response, StatusCode};
use warp::reject::Reject;
use warp::{Rejection, Reply};

#[derive(Debug)]
pub enum ApiError {
    DataFusionError(DataFusionError),
    HashMismatch(String, String),
    NotReadOnlyQuery,
    ReadOnlyEndpointDisabled,
    ReadForbidden,
    WriteForbidden,
    NeedAccessToken,
    UselessAccessToken,
    WrongAccessToken,
    InvalidAuthorizationHeader,
    InvalidMultiStatement,
    EmptyMultiStatement,
    UploadMissingFile,
    UploadMissingFilename,
    UploadMissingFilenameExtension(String),
    UploadSchemaDeserializationError(serde_json::Error),
    UploadSchemaParseError(arrow::error::ArrowError),
    UploadFileLoadError(Box<dyn std::error::Error + Send + Sync>),
    UploadBodyLoadError(warp::Error),
    UploadHasHeaderParseError,
    UploadUnsupportedFileFormat(String),
    QueryDecodeError,
    QueryParsingError(Rejection),
    JsonError(String),
}

// Wrap DataFusion errors so that we can automagically return an
// `ApiError(DataFusionError)` by using the `?` operator
impl From<DataFusionError> for ApiError {
    fn from(err: DataFusionError) -> Self {
        ApiError::DataFusionError(err)
    }
}

// Similarly, wrap Utf8 string decode errors.
impl From<std::str::Utf8Error> for ApiError {
    fn from(_e: std::str::Utf8Error) -> ApiError {
        ApiError::QueryDecodeError
    }
}

impl ApiError {
    fn status_code_body(self: &ApiError) -> (StatusCode, String) {
        match self {
            // TODO: figure out which DF errors to propagate, we have ones that are the server's fault
            // here too (e.g. ResourcesExhausted) and potentially some that leak internal
            // information (e.g. ObjectStore?)
            ApiError::DataFusionError(e) => (StatusCode::BAD_REQUEST, e.to_string()),
            ApiError::HashMismatch(expected, got) => (StatusCode::BAD_REQUEST, format!("Invalid hash: expected {expected:?}, got {got:?}. Resend your query with {expected:?}")),
            ApiError::NotReadOnlyQuery => (StatusCode::METHOD_NOT_ALLOWED, "NOT_READ_ONLY_QUERY".to_string()),
            ApiError::ReadOnlyEndpointDisabled => (StatusCode::METHOD_NOT_ALLOWED, "READ_ONLY_ENDPOINT_DISABLED".to_string()),
            ApiError::ReadForbidden => (StatusCode::FORBIDDEN, "READ_FORBIDDEN".to_string()),
            ApiError::WriteForbidden => (StatusCode::FORBIDDEN, "WRITE_FORBIDDEN".to_string()),
            ApiError::NeedAccessToken => (StatusCode::UNAUTHORIZED, "NEED_ACCESS_TOKEN".to_string()),
            ApiError::UselessAccessToken => (StatusCode::BAD_REQUEST, "USELESS_ACCESS_TOKEN".to_string()),
            ApiError::WrongAccessToken => (StatusCode::UNAUTHORIZED, "INVALID_ACCESS_TOKEN".to_string()),
            ApiError::InvalidAuthorizationHeader => (StatusCode::UNAUTHORIZED, "INVALID_AUTHORIZATION_HEADER".to_string()),
            ApiError::InvalidMultiStatement => (StatusCode::BAD_REQUEST, "Only one read statement is allowed and it must be at the end of a multi-statement query".to_string()),
            ApiError::EmptyMultiStatement => (StatusCode::BAD_REQUEST, "Empty query received".to_string()),
            ApiError::UploadMissingFile => (StatusCode::BAD_REQUEST, "No part containing file found in the request!".to_string()),
            ApiError::UploadMissingFilename => (StatusCode::BAD_REQUEST, "No filename found in the request!".to_string()),
            ApiError::UploadMissingFilenameExtension(filename) => (StatusCode::BAD_REQUEST, format!("File {filename} missing extension")),
            ApiError::UploadSchemaDeserializationError(e) => (StatusCode::BAD_REQUEST, format!("Error deserializing the file schema: {e:}")),
            ApiError::UploadSchemaParseError(e) => (StatusCode::BAD_REQUEST, format!("Error parsing the file schema: {e:}")),
            ApiError::UploadBodyLoadError(e) => (StatusCode::BAD_REQUEST, format!("Error loading the upload body: {e:}")),
            ApiError::UploadFileLoadError(e) => (StatusCode::BAD_REQUEST, format!("Error loading the upload file: {e:}")),
            ApiError::UploadHasHeaderParseError => (StatusCode::BAD_REQUEST, "Invalid has_header".to_string()),
            ApiError::UploadUnsupportedFileFormat(filename) => (StatusCode::BAD_REQUEST, format!("File {filename} not supported")),
            ApiError::QueryDecodeError => (StatusCode::BAD_REQUEST, "QUERY_DECODE_ERROR".to_string()),
            ApiError::QueryParsingError(r) => (StatusCode::BAD_REQUEST, format!("No query found in the request: {r:?}")),
            ApiError::JsonError(message) => (StatusCode::INTERNAL_SERVER_ERROR, message.clone()),
        }
    }

    fn response(&self) -> Response<Body> {
        let (status, body) = self.status_code_body();
        Response::builder()
            .status(status)
            .body(body.into())
            .expect("Could not construct Response")
    }
}

impl Reply for ApiError {
    fn into_response(self) -> Response<Body> {
        self.response()
    }
}

// While most errors come in without using the Rejections mechanism,
// the filters that we call via and() (e.g. authorization context) can't do
// that, since we're not returning Replies with them when the filter runs.
// Instead, we raise them as Rejections and have a special rejection handler.
impl Reject for ApiError {}

pub fn into_response<S: Reply, E: Reply>(reply_res: Result<S, E>) -> Response<Body> {
    match reply_res {
        Ok(resp) => resp.into_response(),
        Err(err) => err.into_response(),
    }
}

pub async fn handle_rejection(r: Rejection) -> Result<impl Reply, Rejection> {
    if let Some(e) = r.find::<ApiError>() {
        Ok(e.response())
    } else {
        // https://github.com/seanmonstar/warp/issues/451 claims we need to reimplement
        // the standard Warp rejections handler (e.g. for missing headers), but it seems
        // like we can just forward the rejection and it'll try and keep handling it.
        Err(r)
    }
}
