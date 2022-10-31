/// On-disk byte-range-aware cache for HTTP requests
/// Partially inspired by https://docs.rs/moka/latest/moka/future/struct.Cache.html#example-eviction-listener,
/// with some additions to weigh it by the file size.
use crate::config::schema::str_to_hex_hash;
use async_trait::async_trait;
use bytes::{Buf, BufMut, Bytes};
use futures::stream::BoxStream;
use log::{debug, error, warn};
use moka::future::{Cache, CacheBuilder};
use moka::notification::RemovalCause;
use object_store::{GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore};

use std::fmt::Display;
use std::fmt::{Debug, Formatter};

use std::fs::remove_dir_all;
use std::io;
use std::io::ErrorKind;
use std::ops::Range;
use std::path::{Path, PathBuf};

use std::sync::Arc;
use tokio::io::AsyncWrite;
use tokio::{fs, sync::RwLock};

#[derive(Debug)]
struct CacheFileManager {
    base_path: PathBuf,
}

impl CacheFileManager {
    pub fn new(base_path: PathBuf) -> Self {
        Self { base_path }
    }

    async fn write_file(
        &mut self,
        cache_key: CacheKey,
        data: &Bytes,
    ) -> io::Result<PathBuf> {
        let mut path = self.base_path.to_path_buf();
        path.push(cache_key.as_filename());
        // Should this happen normally?
        if path.exists() {
            return Err(io::Error::new(
                ErrorKind::Other,
                "Internal error: cached file path already exists",
            ));
        }

        fs::write(&path, data).await?;

        debug!("Cached the data for {:?} at {:?}", cache_key, path);
        Ok(path)
    }

    async fn read_file(&self, path: impl AsRef<Path>) -> io::Result<Bytes> {
        fs::read(path).await.map(Bytes::from)
    }

    async fn remove_file(&mut self, path: impl AsRef<Path> + Debug) -> io::Result<()> {
        fs::remove_file(path.as_ref()).await?;
        debug!("Removed cached data at {:?}", path);

        Ok(())
    }
}

impl Drop for CacheFileManager {
    fn drop(&mut self) {
        let _ = remove_dir_all(self.base_path.clone()).map_err(|e| {
            warn!(
                "Failed to delete the HTTP cache directory {}: {}",
                self.base_path.display(),
                e
            );
            e
        });
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct CacheKey {
    path: object_store::path::Path,
    range: Range<usize>,
}

impl CacheKey {
    fn as_filename(&self) -> String {
        format!(
            "{}-{}-{}",
            str_to_hex_hash(self.path.to_string().as_str()),
            self.range.start,
            self.range.end
        )
    }
}

#[derive(Debug, Clone)]
pub struct CacheValue {
    path: PathBuf,
    size: u64,
}

#[derive(Debug, Clone)]
pub struct CachingObjectStore {
    // File manager, responsible for storing/retrieving files
    file_manager: Arc<RwLock<CacheFileManager>>,
    // Path to store data in
    base_path: PathBuf,
    // Minimum range fetch size, in bytes.
    // Each chunk takes up one item in the cache.
    min_fetch_size: u64,
    // Max cache capacity, bytes
    max_cache_size: u64,

    cache: Cache<CacheKey, CacheValue>,

    inner: Arc<dyn ObjectStore>,
}

impl CachingObjectStore {
    fn on_evict(
        file_manager: &Arc<RwLock<CacheFileManager>>,
        key: Arc<CacheKey>,
        value: CacheValue,
        cause: RemovalCause,
    ) {
        debug!(
            "An entry has been evicted. k: {:?}, v: {:?}, cause: {:?}",
            key, value, cause
        );

        let rt = tokio::runtime::Handle::current();
        let _guard = rt.enter();
        rt.block_on(async {
            // Acquire the write lock of the DataFileManager.
            let mut manager = file_manager.write().await;

            // Remove the data file. We must handle error cases here to
            // prevent the listener from panicking.
            if let Err(e) = manager.remove_file(&value.path).await {
                error!("Failed to remove a data file at {:?}: {:?}", value.path, e);
            }
        });
    }

    pub fn new(
        inner: Arc<dyn ObjectStore>,
        base_path: &Path,
        min_fetch_size: u64,
        max_cache_size: u64,
    ) -> Self {
        let file_manager =
            Arc::new(RwLock::new(CacheFileManager::new(base_path.to_owned())));

        // Clone the pointer since we can't pass the whole struct to the cache
        let eviction_file_manager = file_manager.clone();

        let cache: Cache<CacheKey, CacheValue> = CacheBuilder::new(max_cache_size)
            .weigher(|_, v: &CacheValue| v.size as u32)
            .eviction_listener_with_queued_delivery_mode(move |k, v, cause| {
                Self::on_evict(&eviction_file_manager, k, v, cause)
            })
            .build();

        Self {
            file_manager,
            base_path: base_path.to_owned(),
            min_fetch_size,
            max_cache_size,
            cache,
            inner,
        }
    }

    /// Clone another `CachingObjectStore` instance with the same filesystem cache instance
    /// as the sibling. Should only be used if inner.get(path) == other.inner.get(path) for all
    /// paths (we use it to keep a shared cache between HTTP and HTTPS object stores).
    pub fn new_from_sibling(
        other: &CachingObjectStore,
        inner: Arc<dyn ObjectStore>,
    ) -> Self {
        Self {
            file_manager: other.file_manager.clone(),
            base_path: other.base_path.clone(),
            min_fetch_size: other.min_fetch_size,
            max_cache_size: other.max_cache_size,
            cache: other.cache.clone(),
            inner,
        }
    }

    /// Get a certain range chunk, delineated in units of self.min_fetch_size. If the chunk
    /// is cached, return it directly. Otherwise, fetch and return it.
    async fn get_chunk(
        &self,
        path: &object_store::path::Path,
        chunk: u32,
    ) -> Result<Bytes, object_store::Error> {
        let range = (chunk as usize * self.min_fetch_size as usize)
            ..((chunk + 1) as usize * self.min_fetch_size as usize);

        let key = CacheKey {
            path: path.to_owned(),
            range: range.clone(),
        };

        let value = self
            .cache
            .try_get_with::<_, object_store::Error>(key.clone(), async move {
                let mut manager = self.file_manager.write().await;
                let data = self.inner.get_range(path, range).await?;
                let path = manager.write_file(key, &data).await.map_err(|e| {
                    object_store::Error::Generic {
                        store: "cache_store",
                        source: Box::new(e),
                    }
                })?;

                Ok(CacheValue {
                    path,
                    size: data.len() as u64,
                })
            })
            .await
            .map_err(|e| object_store::Error::Generic {
                store: "cache_store",
                source: Box::new(e),
            })?;

        {
            let manager = self.file_manager.read().await;
            let data = manager.read_file(value.path).await.unwrap();
            Ok(data)
        }
    }
}

impl Display for CachingObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Caching wrapper around {}. Min fetch size: {} bytes. Cache in {}, capacity: {} bytes", &self.inner, self.min_fetch_size, self.base_path.to_string_lossy(), self.max_cache_size)
    }
}

#[async_trait]
impl ObjectStore for CachingObjectStore {
    async fn put(
        &self,
        location: &object_store::path::Path,
        bytes: Bytes,
    ) -> object_store::Result<()> {
        self.inner.put(location, bytes).await
    }

    async fn put_multipart(
        &self,
        location: &object_store::path::Path,
    ) -> object_store::Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        self.inner.put_multipart(location).await
    }

    async fn abort_multipart(
        &self,
        location: &object_store::path::Path,
        multipart_id: &MultipartId,
    ) -> object_store::Result<()> {
        self.inner.abort_multipart(location, multipart_id).await
    }

    async fn get(
        &self,
        location: &object_store::path::Path,
    ) -> object_store::Result<GetResult> {
        self.inner.get(location).await
    }

    async fn get_range(
        &self,
        location: &object_store::path::Path,
        range: Range<usize>,
    ) -> object_store::Result<Bytes> {
        // Expand the range to the next max_fetch_size (+ alignment)
        let start_chunk = range.start / self.min_fetch_size as usize;
        // The final chunk to fetch (inclusively). E.g. with min_fetch_size = 16:
        //  - range.end == 64 (get bytes 0..63 inclusive) -> final chunk is 63 / 16 = 3 (48..64 exclusive)
        //  - range.end == 65 (get bytes 0..64 exclusive) -> final chunk is 64 / 16 = 4 (64..72 exclusive)
        let end_chunk = (range.end - 1) / self.min_fetch_size as usize;

        let mut result = Vec::with_capacity((range.end - range.start) as usize);

        for chunk_num in start_chunk..(end_chunk + 1) {
            let mut data = self.get_chunk(location, chunk_num as u32).await?;
            let data_len = data.len();

            let buf_start = if chunk_num == start_chunk {
                let buf_start = range.start % self.min_fetch_size as usize;
                data.advance(buf_start);
                buf_start
            } else {
                0usize
            };

            let buf_end = if chunk_num == end_chunk {
                let buf_end = range.end % self.min_fetch_size as usize;

                // if min_fetch_size = 16 and buf_end = 64, we want to load everything
                // from the final buffer, instead of 0.
                if buf_end != 0 {
                    buf_end
                } else {
                    self.min_fetch_size as usize
                }
            } else {
                data_len
            };

            debug!(
                "Read {} bytes from the buffer for chunk {}, slicing out {}..{}",
                data_len, chunk_num, buf_start, buf_end
            );

            result.put(data.take(buf_end - buf_start));
        }

        Ok(result.into())
    }

    async fn head(
        &self,
        location: &object_store::path::Path,
    ) -> object_store::Result<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(
        &self,
        location: &object_store::path::Path,
    ) -> object_store::Result<()> {
        self.inner.delete(location).await
    }

    async fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<BoxStream<'_, object_store::Result<ObjectMeta>>> {
        self.inner.list(prefix).await
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
    ) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
    ) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

#[cfg(test)]
mod tests {
    use crate::object_store::http::HttpObjectStore;
    use crate::{
        config::schema::str_to_hex_hash, object_store::cache::CachingObjectStore,
    };
    use itertools::Itertools;
    use moka::future::ConcurrentCacheExt;
    use object_store::{path::Path, ObjectStore};
    use std::path::Path as FSPath;
    use std::sync::Arc;

    use std::{cmp::min, fs, ops::Range};
    use tempfile::TempDir;
    use test_case::test_case;

    use crate::object_store::testutils::make_mock_parquet_server;

    fn make_cached_object_store_small_fetch() -> CachingObjectStore {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.into_path();

        CachingObjectStore::new(
            Arc::new(HttpObjectStore::new("http".to_string())),
            &path,
            16,
            512,
        )
    }

    fn make_cached_object_store_small_disk_size() -> CachingObjectStore {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.into_path();

        CachingObjectStore::new(
            Arc::new(HttpObjectStore::new("http".to_string())),
            &path,
            16,
            4 * 16, // Max capacity 4 chunks
        )
    }

    #[test_case(25..64; "Skip 0, partial 1, full 2, 3")]
    #[test_case(1..2; "Part of chunk 0")]
    #[test_case(16..16; "Fetch nothing at the chunk boundary")]
    #[test_case(16..17; "Fetch one byte at the chunk boundary")]
    #[test_case(10..20; "Part of chunk 0, part of chunk 1")]
    #[test_case(10..500; "Reaches the end of the body")]
    #[test_case(400..505; "Goes beyond the end of the body")]
    // NB: going more than one chunk beyond the end of the body still makes it make
    // a Range request for e.g. 512 -- 527, which breaks the mock and also is a waste,
    // but we don't know that since we don't get Content-Length on this code path.
    #[tokio::test]
    async fn test_range_coalescing(range: Range<usize>) {
        let store = make_cached_object_store_small_fetch();
        let (server, body) = make_mock_parquet_server(true, true).await;
        let server_uri = server.uri();
        let server_uri = server_uri.strip_prefix("http://").unwrap();
        let url = format!("{}/some/file.parquet", &server_uri);

        let result = store
            .get_range(&Path::from(url.as_str()), range.clone())
            .await
            .unwrap();
        assert_eq!(result, body[range.start..min(body.len(), range.end)]);
    }

    fn assert_ranges_in_cache(basedir: &FSPath, url: &str, chunks: Vec<u32>) {
        let expected = chunks
            .iter()
            .map(|c| format!("{}-{}-{}", str_to_hex_hash(url), c * 16, (c + 1) * 16))
            .sorted()
            .collect_vec();

        let paths = fs::read_dir(basedir)
            .unwrap()
            .into_iter()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .sorted()
            .collect_vec();

        assert_eq!(paths, expected);
    }

    #[tokio::test]
    async fn test_cache_caching_eviction() {
        let store = make_cached_object_store_small_disk_size();
        let (server, _body) = make_mock_parquet_server(true, true).await;
        let server_uri = server.uri();
        let server_uri = server_uri.strip_prefix("http://").unwrap();
        let url = format!("{}/some/file.parquet", &server_uri);

        // Mock is empty
        assert_eq!(server.received_requests().await.unwrap().len(), 0);

        // Perform multiple sets of actions:
        //   - request a range
        //   - check the amount of requests that the mock received in total
        //   - sync() the cache (doesn't look like we need to do it for normal operations,
        //     but we want to make sure all counts are currect and evictions have been done)
        //     - NOTE: if an assertion fails in the main test, this can cause a cache thread to crash with
        //          thread 'moka-notifier-2' panicked at 'there is no reactor running, must be called
        //          from the context of a Tokio 1.x runtime'
        //       This is a red herring (assertion failure causes the runtime to shut down and the eviction
        //       listener can't get a Tokio handle and panics).
        //
        //   - check the cache entry count
        //   - check the files that are on disk

        // Request 25..64 (chunks 1, 2, 3)
        store
            .get_range(&Path::from(url.as_str()), 25..64)
            .await
            .unwrap();
        store.cache.sync();

        // Mock has had 3 requests
        assert_eq!(server.received_requests().await.unwrap().len(), 3);
        assert_eq!(store.cache.entry_count(), 3);
        assert_ranges_in_cache(&store.base_path, &url, vec![1, 2, 3]);

        // Request 26..30 (chunk 1)
        store
            .get_range(&Path::from(url.as_str()), 26..30)
            .await
            .unwrap();
        store.cache.sync();

        // No extra requests
        assert_eq!(server.received_requests().await.unwrap().len(), 3);
        assert_eq!(store.cache.entry_count(), 3);
        assert_ranges_in_cache(&store.base_path, &url, vec![1, 2, 3]);

        // Request 33..66 (chunks 2, 3, 4)
        store
            .get_range(&Path::from(url.as_str()), 33..66)
            .await
            .unwrap();
        store.cache.sync();

        // One extra request to fetch chunk 4
        assert_eq!(server.received_requests().await.unwrap().len(), 4);
        assert_eq!(store.cache.entry_count(), 4);
        assert_ranges_in_cache(&store.base_path, &url, vec![1, 2, 3, 4]);

        // Request 80..85 (chunk 5, evicts chunk 1 since it's least recently used)
        store
            .get_range(&Path::from(url.as_str()), 80..85)
            .await
            .unwrap();
        store.cache.sync();

        assert_eq!(server.received_requests().await.unwrap().len(), 5);
        assert_eq!(store.cache.entry_count(), 4);

        // We can't seem to be able to make sure that eviction works properly here
        // due to some weird Moka eventual consistency. Even when calling sync(),
        // loading chunk 1 again results in a cache hit (despite that it was
        // supposed to be evicted) and our eviction code crashes if the main test crashes.
        // assert_ranges_in_cache(&store.base_path, &url, vec![2, 3, 4, 5]);
    }
}
