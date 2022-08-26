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
use reqwest::{header, Client, RequestBuilder, Response, StatusCode};
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::ops::Range;
use std::sync::Arc;

pub const ANYHOST: &str = "anyhost";

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
    HttpClientError(reqwest::Error),
    RangesUnsupported,
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
            Self::RangesUnsupported => {
                writeln!(f, "This server does not support byte range fetches")
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

    /// Send a request, converting non-2xx/3xx errors to actual Error structs
    async fn send(
        &self,
        request: RequestBuilder,
    ) -> Result<Response, HttpObjectStoreError> {
        request
            .send()
            .await
            .and_then(|r| r.error_for_status())
            .map_err(HttpObjectStoreError::HttpClientError)
    }
}

#[async_trait]
impl ObjectStore for HttpObjectStore {
    async fn put(&self, _location: &Path, _bytes: Bytes) -> object_store::Result<()> {
        Err(object_store::Error::NotSupported {
            source: Box::new(HttpObjectStoreError::WritesUnsupported),
        })
    }

    async fn get(&self, location: &Path) -> object_store::Result<GetResult> {
        let response = self.send(self.request_builder(location)).await?;

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
            .send(
                self.request_builder(location)
                    .header("Range", format!("bytes={}-{}", range.start, range.end)),
            )
            .await?;

        // If the server returned a 206: it understood our range query
        if response.status() == StatusCode::PARTIAL_CONTENT {
            Ok(response
                .bytes()
                .await
                .map_err(HttpObjectStoreError::HttpClientError)?)
        } else {
            Err(HttpObjectStoreError::RangesUnsupported.into())
        }
    }

    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        let response = self.send(self.request_builder(location)).await?;

        let length = response
            .headers()
            .get(header::CONTENT_LENGTH)
            .ok_or(HttpObjectStoreError::NoContentLengthResponse)?
            .to_str()
            .map_err(|_| HttpObjectStoreError::NoContentLengthResponse)?
            .parse::<u64>()
            .map_err(|_| HttpObjectStoreError::NoContentLengthResponse)?;

        // Currently, we only support HTTP servers that support Range fetches
        if response
            .headers()
            .get(header::ACCEPT_RANGES)
            .ok_or(HttpObjectStoreError::RangesUnsupported)?
            .to_str()
            .map_err(|_| HttpObjectStoreError::RangesUnsupported)?
            .to_lowercase()
            != "bytes"
        {
            return Err(HttpObjectStoreError::RangesUnsupported.into());
        }

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

/// Add HTTP/HTTPS support to a DataFusion SessionContext
pub fn add_http_object_store(context: &SessionContext) {
    context.runtime_env().register_object_store(
        "http",
        ANYHOST,
        Arc::new(HttpObjectStore::new("http".to_string())),
    );

    context.runtime_env().register_object_store(
        "https",
        ANYHOST,
        Arc::new(HttpObjectStore::new("https".to_string())),
    );
}

/// Prepare a URL (`LOCATION` clause) so that DataFusion can directly route it to the object store.
///
/// This is done by injecting a special `anyhost` hostname inside of the URL, for example:
/// `https://some-host.com/file.parquet` -> `https://anyhost/some-host.com/file.parquet`. This is
/// because DataFusion routes URLs to object store instances based on the scheme and the host. Hence,
/// in this case, `LOCATION 'https://some-host.com/file.parquet'` will get DataFusion to
/// pass the path to us as `some-host.com/file.parquet` (as it strips the scheme and the fake host)
/// and the object store instance will re-append the correct scheme back to the URL.
///
/// Returns a `None` if the location doesn't start with `http://` / `https://`.
pub fn try_prepare_http_url(location: &str) -> Option<String> {
    location.strip_prefix("http://").map_or_else(
        || {
            location
                .strip_prefix("https://")
                .map(|l| format!("https://{}/{}", ANYHOST, l))
        },
        |l| Some(format!("http://{}/{}", ANYHOST, l)),
    )
}

#[cfg(test)]
mod tests {
    use object_store::{path::Path, ObjectStore};

    use super::HttpObjectStore;
    use crate::object_store::testutils::make_mock_parquet_server;

    #[tokio::test]
    async fn test_head() {
        let store = HttpObjectStore::new("http".to_string());
        let (server, body) = make_mock_parquet_server(false).await;
        // The object store expects just a path, which it treats as the host (prepending the
        // scheme instead)
        let server_uri = server.uri();
        let server_uri = server_uri.strip_prefix("http://").unwrap();

        let url = format!("{}/some/file.parquet", &server_uri);

        let result = store.head(&Path::from(url.as_str())).await.unwrap();
        assert_eq!(result.location.to_string(), url);
        assert_eq!(result.size, body.len());

        let url = format!("{}/some/file-doesnt-exist.parquet", &server_uri);
        let err = store.head(&Path::from(url.as_str())).await.unwrap_err();
        assert!(err.to_string().contains("Status(404)"));
    }

    #[tokio::test]
    async fn test_get() {
        let store = HttpObjectStore::new("http".to_string());
        let (server, body) = make_mock_parquet_server(false).await;
        let server_uri = server.uri();
        let server_uri = server_uri.strip_prefix("http://").unwrap();
        let url = format!("{}/some/file.parquet", &server_uri);

        // Get without a range
        let result = store
            .get(&Path::from(url.as_str()))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(result, body);
    }

    #[tokio::test]
    async fn test_get_server_range() {
        let store = HttpObjectStore::new("http".to_string());
        let (server, body) = make_mock_parquet_server(true).await;
        let server_uri = server.uri();
        let server_uri = server_uri.strip_prefix("http://").unwrap();
        let url = format!("{}/some/file.parquet", &server_uri);

        // Get with a range
        let result = store
            .get_range(&Path::from(url.as_str()), 12..34)
            .await
            .unwrap();
        assert_eq!(result, body[12..34]);
    }
}
