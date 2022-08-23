/// ObjectStore implementation for HTTP/HTTPs for DataFusion's CREATE EXTERNAL TABLE
use async_trait::async_trait;
use bytes::{Buf, BufMut, Bytes};
use chrono::Utc;
use futures::stream::BoxStream;
use futures::{stream, Stream, StreamExt};
use log::warn;
use object_store::path::Path;
use object_store::{GetResult, ListResult, ObjectMeta, ObjectStore};

use datafusion::prelude::SessionContext;
use reqwest::{header, Client, RequestBuilder, StatusCode};
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::ops::Range;
use std::sync::Arc;

#[derive(Debug)]
pub struct HttpObjectStore {
    client: Client,
    scheme: String,
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
    ListingUnsupported,
    InvalidRangeError,
    HttpClientError(reqwest::Error),
}

impl From<HttpObjectStoreError> for object_store::Error {
    fn from(e: HttpObjectStoreError) -> Self {
        object_store::Error::Generic {
            store: "http",
            source: Box::new(e),
        }
    }
}

impl From<reqwest::Error> for HttpObjectStoreError {
    fn from(e: reqwest::Error) -> Self {
        Self::HttpClientError(e)
    }
}

impl Display for HttpObjectStoreError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WritesUnsupported => writeln!(f, "Writes to HTTP are unsupported"),
            Self::NoContentLengthResponse => {
                writeln!(f, "Server did not respond with a Content-Length header")
            }
            Self::ListingUnsupported => writeln!(f, "HTTP doesn't support listing"),
            Self::InvalidRangeError => {
                writeln!(f, "Invalid range passed to the HTTP store")
            }
            Self::HttpClientError(e) => writeln!(f, "HTTP error: {:?}", e),
        }
    }
}

impl Error for HttpObjectStoreError {}

impl HttpObjectStore {
    pub fn new(scheme: String) -> Self {
        Self {
            client: Client::new(),
            // DataFusion strips the URL scheme when passing it to us (e.g. http://), so we
            // have to record it in the object in order to reconstruct the actual full URL.
            scheme,
        }
    }

    fn get_uri(&self, path: &Path) -> String {
        format!("{}://{}", &self.scheme, path)
    }

    fn request_builder(&self, path: &Path) -> RequestBuilder {
        self.client.get(self.get_uri(path)).header(
            "User-Agent",
            format!("Seafowl/{}", env!("VERGEN_GIT_SEMVER")),
        )
    }
}

async fn range_to_bytes<S, E>(
    body: S,
    range: &Range<usize>,
) -> Result<Bytes, HttpObjectStoreError>
where
    E: Into<HttpObjectStoreError>,
    S: Stream<Item = Result<Bytes, E>>,
{
    let mut vec = Vec::with_capacity(range.end - range.start);
    let mut body = Box::pin(body);

    // Skip bytes until we reach the start
    let mut pos: usize = 0;

    while pos < range.end {
        let mut buf = body
            .next()
            .await
            .ok_or(HttpObjectStoreError::InvalidRangeError)?
            .map_err(|e| e.into())?;
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
        let response = self
            .request_builder(location)
            .send()
            .await
            .map_err(HttpObjectStoreError::HttpClientError)?;

        let body = response.bytes_stream();

        Ok(GetResult::Stream(
            body.map(|c| c.map_err(|e| HttpObjectStoreError::HttpClientError(e).into()))
                .boxed(),
        ))
    }

    async fn get_range(
        &self,
        location: &Path,
        range: Range<usize>,
    ) -> object_store::Result<Bytes> {
        let response = self
            .request_builder(location)
            .header("Range", format!("bytes={}-{}", range.start, range.end))
            .send()
            .await
            .map_err(HttpObjectStoreError::HttpClientError)?;

        // If the server returned a 206: it understood our range query
        if response.status() == StatusCode::PARTIAL_CONTENT {
            Ok(response
                .bytes()
                .await
                .map_err(HttpObjectStoreError::HttpClientError)?)
        } else {
            // Slice the range out ourselves
            warn!(
                "The server doesn't support Range requests. Complete object downloaded."
            );
            Ok(range_to_bytes(response.bytes_stream(), &range).await?)
        }
    }

    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        let response = self
            .request_builder(location)
            .send()
            .await
            .map_err(HttpObjectStoreError::HttpClientError)?;

        let length = response
            .headers()
            .get(header::CONTENT_LENGTH)
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
        prefix: Option<&Path>,
    ) -> object_store::Result<BoxStream<'_, object_store::Result<ObjectMeta>>> {
        // DataFusion uses the HEAD request instead of it's listing a single file
        // (path doesn't end with a slash). Since HTTP doesn't support listing anyway,
        // this makes our job easier.

        match prefix {
            None => Err(HttpObjectStoreError::ListingUnsupported.into()),
            Some(p) => {
                let p_str = p.to_string();
                if p_str.ends_with('/') {
                    Err(HttpObjectStoreError::ListingUnsupported.into())
                } else {
                    // Use the HEAD implementation instead
                    Ok(Box::pin(stream::iter(vec![self.head(p).await])))
                }
            }
        }
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

pub fn add_http_object_store(context: &SessionContext) {
    context.runtime_env().register_object_store(
        "http",
        "anyhost",
        Arc::new(HttpObjectStore::new("http".to_string())),
    );

    context.runtime_env().register_object_store(
        "https",
        "anyhost",
        Arc::new(HttpObjectStore::new("https".to_string())),
    );
}
