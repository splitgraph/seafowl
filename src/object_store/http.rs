/// ObjectStore implementation for HTTP/HTTPs for DataFusion's CREATE EXTERNAL TABLE
use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use futures::stream::BoxStream;
use futures::{stream, StreamExt};

use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore,
};
use percent_encoding::{percent_decode_str, utf8_percent_encode, NON_ALPHANUMERIC};

use crate::object_store::cache::{
    CachingObjectStore, DEFAULT_CACHE_CAPACITY, DEFAULT_CACHE_ENTRY_TTL,
    DEFAULT_MIN_FETCH_SIZE,
};
use datafusion::prelude::SessionContext;
use lazy_static::lazy_static;
use log::warn;
use regex::Regex;
use reqwest::{header, Client, ClientBuilder, RequestBuilder, Response, StatusCode};
use std::env;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::fs::File;
use std::io::Read;
use std::ops::Range;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::io::AsyncWrite;
use url::Url;
use warp::hyper::header::{
    IF_MATCH, IF_MODIFIED_SINCE, IF_NONE_MATCH, IF_UNMODIFIED_SINCE, RANGE,
};

pub const ANYHOST: &str = "anyhost";

lazy_static! {
    static ref CONTENT_RANGE_RE: Regex =
        Regex::new(r"(^bytes)\s+(\d+)\s?-\s?(\d+)?\s?/?\s?(\d+|\*)?").unwrap();
}

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
            Self::HttpClientError(e) => writeln!(f, "HTTP error: {e:?}"),
        }
    }
}

impl Error for HttpObjectStoreError {}

fn get_client(ssl_cert_file: &Option<String>) -> std::io::Result<Client> {
    let builder: ClientBuilder = match ssl_cert_file {
        Some(cert_pem_file_path) => {
            let mut buf = Vec::new();
            let cert_result = File::open(cert_pem_file_path)
                .and_then(|mut file| file.read_to_end(&mut buf))
                .and_then(|_| {
                    reqwest::Certificate::from_pem(&buf).map_err(|e| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!(
                                "Error parsing SSL cert file {cert_pem_file_path}: {e}"
                            ),
                        )
                    })
                });
            cert_result.map(|cert| ClientBuilder::new().add_root_certificate(cert))
        }
        None => Ok(ClientBuilder::new()),
    }?;
    builder.build().map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Error creating reqwest client builder: {e}"),
        )
    })
}

impl HttpObjectStore {
    pub fn new(scheme: String, ssl_cert_file: &Option<String>) -> Self {
        Self {
            client: get_client(ssl_cert_file).expect("Couldn't get reqwest client!"),
            // DataFusion strips the URL scheme when passing it to us (e.g. http://), so we
            // have to record it in the object in order to reconstruct the actual full URL.
            scheme,
        }
    }

    /// Reverse the URL encoding done by `try_prepare_http_url`
    fn get_uri(&self, path: &Path) -> String {
        let path_str = path.to_string();
        let decoded = percent_decode_str(path_str.as_str()).decode_utf8().unwrap();
        format!("{}://{}", &self.scheme, decoded)
    }

    fn request_builder(&self, path: &Path) -> RequestBuilder {
        self.client.get(self.get_uri(path)).header(
            "User-Agent",
            format!("Seafowl/{}", env!("VERGEN_GIT_SEMVER")),
        )
    }

    // TODO: Use `GetOptionsExt::with_get_options` on the `RequestBuilder` returned by `self.request_builder`
    // when it becomes public.
    fn request_builder_with_get_options(
        &self,
        path: &Path,
        options: GetOptions,
    ) -> RequestBuilder {
        let mut request_builder = self.request_builder(path);

        if let Some(range) = options.range {
            let range = format!("bytes={}-{}", range.start, range.end.saturating_sub(1));
            request_builder = request_builder.header(RANGE, range);
        }

        if let Some(tag) = options.if_match {
            request_builder = request_builder.header(IF_MATCH, tag);
        }

        if let Some(tag) = options.if_none_match {
            request_builder = request_builder.header(IF_NONE_MATCH, tag);
        }

        const DATE_FORMAT: &str = "%a, %d %b %Y %H:%M:%S GMT";
        if let Some(date) = options.if_unmodified_since {
            request_builder = request_builder
                .header(IF_UNMODIFIED_SINCE, date.format(DATE_FORMAT).to_string());
        }

        if let Some(date) = options.if_modified_since {
            request_builder = request_builder
                .header(IF_MODIFIED_SINCE, date.format(DATE_FORMAT).to_string());
        }

        request_builder
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

    /// Emulate the results of a HEAD request by using a GET request with
    /// an empty range
    ///
    /// Workaround for AWS S3 not supporting pre-signing for both HEAD and GET:
    ///
    /// https://stackoverflow.com/questions/15717230/pre-signing-amazon-s3-urls-for-both-head-and-get-verbs
    async fn head_via_get(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        let response = self
            .send(self.request_builder(location).header("Range", "bytes=0-0"))
            .await?;

        let content_range_str = response
            .headers()
            .get(header::CONTENT_RANGE)
            .ok_or(HttpObjectStoreError::NoContentLengthResponse)?
            .to_str()
            .map_err(|_| HttpObjectStoreError::NoContentLengthResponse)?;

        let length = CONTENT_RANGE_RE
            .captures(content_range_str)
            .ok_or(HttpObjectStoreError::NoContentLengthResponse)?
            .get(4)
            .unwrap()
            .as_str() // The regex is static, so capture group 4 is guaranteed to exist
            .parse::<u64>()
            .map_err(|_| HttpObjectStoreError::NoContentLengthResponse)?;

        Ok(ObjectMeta {
            location: location.clone(),
            last_modified: Utc::now(),
            size: usize::try_from(length).expect("unsupported size on this platform"),
            e_tag: None,
        })
    }
}

#[async_trait]
impl ObjectStore for HttpObjectStore {
    async fn put(&self, _location: &Path, _bytes: Bytes) -> object_store::Result<()> {
        Err(object_store::Error::NotSupported {
            source: Box::new(HttpObjectStoreError::WritesUnsupported),
        })
    }

    async fn put_multipart(
        &self,
        _location: &Path,
    ) -> object_store::Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        Err(object_store::Error::NotSupported {
            source: Box::new(HttpObjectStoreError::WritesUnsupported),
        })
    }

    async fn abort_multipart(
        &self,
        _location: &Path,
        _multipart_id: &MultipartId,
    ) -> object_store::Result<()> {
        Err(object_store::Error::NotSupported {
            source: Box::new(HttpObjectStoreError::WritesUnsupported),
        })
    }

    async fn get(&self, location: &Path) -> object_store::Result<GetResult> {
        self.get_opts(location, GetOptions::default()).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let response = self
            .send(self.request_builder_with_get_options(location, options))
            .await?;

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
                    // The Range header is inclusive, so a range 0..5 will result in a request 0..4,
                    // which will load bytes 0, 1, 2, 3, 4.
                    .header("Range", format!("bytes={}-{}", range.start, range.end - 1)),
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
        let uri = self.get_uri(location);
        let response = self
            .send(self.client.head(&uri).header(
                "User-Agent",
                format!("Seafowl/{}", env!("VERGEN_GIT_SEMVER")),
            ))
            .await;

        if let Err(HttpObjectStoreError::HttpClientError(ref e)) = response {
            if e.status() == Some(StatusCode::FORBIDDEN) {
                warn!("HEAD request for location {} failed. Assuming an S3-like API and trying to emulate HEAD via GET", &uri);
                return self.head_via_get(location).await;
            }
        }

        let response = response?;

        let length = response
            .headers()
            .get(header::CONTENT_LENGTH)
            .ok_or(HttpObjectStoreError::NoContentLengthResponse)?
            .to_str()
            .map_err(|_| HttpObjectStoreError::NoContentLengthResponse)?
            .parse::<u64>()
            .map_err(|_| HttpObjectStoreError::NoContentLengthResponse)?;

        // Currently, we only support HTTP servers that support Range fetches
        let accept_ranges = response
            .headers()
            .get(header::ACCEPT_RANGES)
            .ok_or(HttpObjectStoreError::RangesUnsupported)?
            .to_str()
            .map_err(|_| HttpObjectStoreError::RangesUnsupported)?
            .to_lowercase();

        if accept_ranges != "bytes" {
            return Err(HttpObjectStoreError::RangesUnsupported.into());
        }

        Ok(ObjectMeta {
            location: location.clone(),
            // DF only uses this in `paths_to_batch` which constructs a fake batch for partition
            // pruning (column _df_part_file_modified), but it doesn't look like partition pruning
            // expressions can access this.
            last_modified: Utc::now(),
            size: usize::try_from(length).expect("unsupported size on this platform"),
            e_tag: None,
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
pub fn add_http_object_store(context: &SessionContext, ssl_cert_file: &Option<String>) {
    let tmp_dir = TempDir::new().unwrap();
    // NB won't delete tmp_dir any more
    let path = tmp_dir.into_path();

    let http_object_store = CachingObjectStore::new(
        Arc::new(HttpObjectStore::new("http".to_string(), ssl_cert_file)),
        &path,
        DEFAULT_MIN_FETCH_SIZE,
        DEFAULT_CACHE_CAPACITY,
        DEFAULT_CACHE_ENTRY_TTL,
    );
    let https_object_store = CachingObjectStore::new_from_sibling(
        &http_object_store,
        Arc::new(HttpObjectStore::new("https".to_string(), ssl_cert_file)),
    );

    context.runtime_env().register_object_store(
        &Url::parse(format!("http://{ANYHOST}").as_str()).unwrap(),
        Arc::new(http_object_store),
    );

    context.runtime_env().register_object_store(
        &Url::parse(format!("https://{ANYHOST}").as_str()).unwrap(),
        Arc::new(https_object_store),
    );
}

/// Prepare a URL (`LOCATION` clause) so that DataFusion can directly route it to this HTTP object store.
///
/// We do two hacks here to get this working.
///
/// 1) DataFusion routes URLs to object store instances based on the scheme and the host, but the HTTP
///    store can handle any host. DataFusion also strips the host and the scheme. To work around this,
///    we prepend a special `anyhost` hostname inside of the URL, for example:
///    `https://some-host.com/file.parquet` -> `https://anyhost/some-host.com/file.parquet`.
///    Hence, in this case, `LOCATION 'https://some-host.com/file.parquet'` will get DataFusion to
///    pass the path to us as `some-host.com/file.parquet` (as it strips the scheme and the fake host)
///    and the object store instance will re-append the correct scheme back to the URL.
/// 2) DataFusion also strips the query string, which is important in AWS signatures. To stop it from
///    doing that, we URI-encode the full location to make it look like it's part of the URL path.
///
/// This means that the location that our object store gets needs to be decoded (see HttpObjectStore::get_uri)
///
/// Returns a `None` if the location doesn't start with `http://` / `https://`.
pub fn try_prepare_http_url(location: &str) -> Option<String> {
    location.strip_prefix("http://").map_or_else(
        || {
            location.strip_prefix("https://").map(|l| {
                format!(
                    "https://{}/{}",
                    ANYHOST,
                    utf8_percent_encode(l, NON_ALPHANUMERIC)
                )
            })
        },
        |l| {
            Some(format!(
                "http://{}/{}",
                ANYHOST,
                utf8_percent_encode(l, NON_ALPHANUMERIC)
            ))
        },
    )
}

#[cfg(test)]
mod tests {
    use crate::object_store::cache::{
        CachingObjectStore, DEFAULT_CACHE_CAPACITY, DEFAULT_CACHE_ENTRY_TTL,
        DEFAULT_MIN_FETCH_SIZE,
    };
    use object_store::{path::Path, ObjectStore};
    use std::sync::Arc;
    use tempfile::TempDir;

    use super::HttpObjectStore;
    use crate::testutils::make_mock_parquet_server;

    fn make_cached_object_store() -> CachingObjectStore {
        let tmp_dir = TempDir::new().unwrap();
        // NB this won't delete tmp_dir any more, but it's up to the cache's FS
        // manager to do it.
        let path = tmp_dir.into_path();

        CachingObjectStore::new(
            Arc::new(HttpObjectStore::new("http".to_string(), &None)),
            &path,
            DEFAULT_MIN_FETCH_SIZE,
            DEFAULT_CACHE_CAPACITY,
            DEFAULT_CACHE_ENTRY_TTL,
        )
    }

    #[tokio::test]
    async fn test_head() {
        let store = make_cached_object_store();
        let (server, body) = make_mock_parquet_server(false, true).await;
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

    /// Test we correctly fall back to using a GET with a zero range to get the length of the
    /// document if HEAD isn't supported
    #[tokio::test]
    async fn test_head_get_emulation() {
        let store = make_cached_object_store();
        let (server, body) = make_mock_parquet_server(true, false).await;
        let server_uri = server.uri();
        let server_uri = server_uri.strip_prefix("http://").unwrap();

        let url = format!("{}/some/file.parquet", &server_uri);

        let result = store.head(&Path::from(url.as_str())).await.unwrap();
        assert_eq!(result.location.to_string(), url);
        assert_eq!(result.size, body.len());
    }

    #[tokio::test]
    async fn test_get() {
        let store = make_cached_object_store();
        let (server, body) = make_mock_parquet_server(false, true).await;
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
        let store = make_cached_object_store();
        let (server, body) = make_mock_parquet_server(true, true).await;
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
