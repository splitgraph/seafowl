/// ObjectStore implementation for HTTP/HTTPs for DataFusion's CREATE EXTERNAL TABLE
use async_trait::async_trait;
use bytes::{Buf, BufMut, Bytes};
use chrono::Utc;
use futures::stream::BoxStream;
use futures::StreamExt;
use log::warn;
use object_store::path::Path;
use object_store::{GetResult, ListResult, ObjectMeta, ObjectStore};

use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::ops::Range;
use warp::hyper::body::{to_bytes, HttpBody};
use warp::hyper::client::HttpConnector;
use warp::hyper::header::CONTENT_LENGTH;
use warp::hyper::{Body, Client, Method, Request, StatusCode};

#[derive(Debug)]
struct HttpObjectStore {
    client: Client<HttpConnector>,
}

impl Display for HttpObjectStore {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

#[derive(Debug)]
enum HttpObjectStoreError {
    WritesUnsupported,
    NoContentLengthResponse,
    HttpError(warp::hyper::Error),
}

impl From<warp::hyper::Error> for HttpObjectStoreError {
    fn from(e: warp::hyper::Error) -> Self {
        Self::HttpError(e)
    }
}

impl From<HttpObjectStoreError> for object_store::Error {
    fn from(e: HttpObjectStoreError) -> Self {
        object_store::Error::Generic {
            store: "http",
            source: Box::new(e),
        }
    }
}

impl Display for HttpObjectStoreError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WritesUnsupported => writeln!(f, "Writes to HTTP are unsupported"),
            Self::NoContentLengthResponse => {
                writeln!(f, "Server did not respond with a Content-Length header")
            }
            Self::HttpError(e) => writeln!(f, "HTTP error: {:?}", e),
        }
    }
}

impl Error for HttpObjectStoreError {}
//
// impl HttpObjectStore {
//     pub fn new() -> Self {
//         Self {
//             client: Client::new(),
//         }
//     }
// }

async fn range_to_bytes<T, E>(
    body: T,
    range: &Range<usize>,
) -> Result<Bytes, warp::hyper::Error>
where
    E: Debug,
    T: HttpBody<Error = E>,
{
    let mut vec = Vec::with_capacity(range.end - range.start);
    let mut body = Box::pin(body);

    // Skip bytes until we reach the start
    let mut pos: usize = 0;

    while pos < range.end {
        let mut buf = body.data().await.unwrap().unwrap();
        let buf_len = buf.remaining();

        if pos < range.start {
            if buf_len > range.start - pos {
                // This buffer spans the range start. Skip up to the start and
                // add the rest to the buffer.
                buf.advance(range.start - pos);
                vec.put(buf);
            }
        } else if buf_len >= range.end - pos {
            // This buffer spans the range end. Add what's missing to the buffer
            // and break out of the loop.
            vec.put(buf.take(range.end - pos));
            break;
        } else {
            vec.put(buf)
        }
        pos += buf_len;
    }

    Ok(vec.into())
}

#[async_trait]
impl ObjectStore for HttpObjectStore {
    async fn put(&self, _location: &Path, _bytes: Bytes) -> object_store::Result<()> {
        Err(object_store::Error::NotSupported {
            source: Box::new(HttpObjectStoreError::WritesUnsupported),
        })
    }

    async fn get(&self, location: &Path) -> object_store::Result<GetResult> {
        let request = Request::builder().uri(location.to_string()).header(
            "User-Agent",
            format!("Seafowl/{}", env!("VERGEN_GIT_SEMVER")),
        );

        let response = self
            .client
            .request(request.body(Body::empty()).unwrap())
            .await
            .map_err(HttpObjectStoreError::HttpError)?;

        let body = response.into_body();

        Ok(GetResult::Stream(
            body.map(|c| c.map_err(|e| HttpObjectStoreError::HttpError(e).into()))
                .boxed(),
        ))
    }

    async fn get_range(
        &self,
        location: &Path,
        range: Range<usize>,
    ) -> object_store::Result<Bytes> {
        let request = Request::builder()
            .uri(location.to_string())
            .header(
                "User-Agent",
                format!("Seafowl/{}", env!("VERGEN_GIT_SEMVER")),
            )
            .header("Range", format!("bytes={}-{}", range.start, range.end));

        let response = self
            .client
            .request(request.body(Body::empty()).unwrap())
            .await
            .map_err(HttpObjectStoreError::HttpError)?;

        // If the server returned a 206: it understood our range query
        if response.status() == StatusCode::PARTIAL_CONTENT {
            Ok(to_bytes(response.into_body())
                .await
                .map_err(HttpObjectStoreError::HttpError)?)
        } else {
            // Slice the range out ourselves
            warn!(
                "The server doesn't support Range requests. Complete object downloaded."
            );
            Ok(range_to_bytes(response.into_body(), &range)
                .await
                .map_err(HttpObjectStoreError::HttpError)?)
        }
    }

    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        let request = Request::builder()
            .method(Method::HEAD)
            .uri(location.to_string())
            .header(
                "User-Agent",
                format!("Seafowl/{}", env!("VERGEN_GIT_SEMVER")),
            );

        let response = self
            .client
            .request(request.body(Body::empty()).unwrap())
            .await
            .unwrap();

        let length = response
            .headers()
            .get(CONTENT_LENGTH)
            .ok_or(HttpObjectStoreError::NoContentLengthResponse)?
            .to_str()
            .map_err(|_| HttpObjectStoreError::NoContentLengthResponse)?
            .parse::<u64>()
            .map_err(|_| HttpObjectStoreError::NoContentLengthResponse)?;

        Ok(ObjectMeta {
            location: location.clone(),
            // DF only uses this in `paths_to_batch` which constructs a fake batch for partition
            // pruning (column _df_part_file_modified), but it doesn't look like partition pruning
            // expressions can access this.
            last_modified: Utc::now(),
            size: usize::try_from(length).expect("unsupported size on this platform"),
        })
    }

    async fn delete(&self, _location: &Path) -> object_store::Result<()> {
        Err(object_store::Error::NotSupported {
            source: Box::new(HttpObjectStoreError::WritesUnsupported),
        })
    }

    async fn list(
        &self,
        _prefix: Option<&Path>,
    ) -> object_store::Result<BoxStream<'_, object_store::Result<ObjectMeta>>> {
        todo!()
        // DF: If the prefix is a file, use a head request, otherwise list
        // let is_dir = self.url.as_str().ends_with('/');
    }

    async fn list_with_delimiter(
        &self,
        _prefix: Option<&Path>,
    ) -> object_store::Result<ListResult> {
        // Not used by DF, so we punt on implementing this
        Err(object_store::Error::NotImplemented)
    }

    async fn copy(&self, _from: &Path, _to: &Path) -> object_store::Result<()> {
        Err(object_store::Error::NotSupported {
            source: Box::new(HttpObjectStoreError::WritesUnsupported),
        })
    }

    async fn copy_if_not_exists(
        &self,
        _from: &Path,
        _to: &Path,
    ) -> object_store::Result<()> {
        Err(object_store::Error::NotSupported {
            source: Box::new(HttpObjectStoreError::WritesUnsupported),
        })
    }
}
