/// On-disk byte-range-aware cache for HTTP requests
/// Partially inspired by https://docs.rs/moka/latest/moka/future/struct.Cache.html#example-eviction-listener,
/// with some additions to weigh it by the file size.
use crate::config::schema::str_to_hex_hash;
use async_trait::async_trait;
use bytes::{Buf, BufMut, Bytes};
use futures::stream::BoxStream;
use moka::future::{Cache, CacheBuilder, FutureExt};
use moka::notification::RemovalCause;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore, PutOptions,
    PutResult,
};
use tracing::{debug, error, warn};

use std::fmt::Display;
use std::fmt::{Debug, Formatter};

use std::fs::remove_dir_all;
use std::io;
use std::io::ErrorKind;
use std::ops::Range;
use std::path::{Path, PathBuf};

use moka::policy::EvictionPolicy;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWrite;

pub const DEFAULT_MIN_FETCH_SIZE: u64 = 1024 * 1024; // 1 MiB
pub const DEFAULT_CACHE_CAPACITY: u64 = 1024 * 1024 * 1024; // 1 GiB
pub const DEFAULT_CACHE_ENTRY_TTL: Duration = Duration::from_secs(3 * 60);

#[derive(Debug)]
struct CacheFileManager {
    base_path: PathBuf,
}

impl CacheFileManager {
    pub fn new(base_path: PathBuf) -> Self {
        Self { base_path }
    }

    async fn write_file(
        &self,
        cache_key: &CacheKey,
        data: Arc<Bytes>,
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

        tokio::fs::write(&path, data.as_ref()).await?;

        debug!("Written data for {:?} to {:?}", cache_key, path);
        Ok(path)
    }

    async fn read_file(&self, path: impl AsRef<Path>) -> io::Result<Bytes> {
        tokio::fs::read(path).await.map(Bytes::from)
    }

    async fn remove_file(&self, path: impl AsRef<Path> + Debug) -> io::Result<()> {
        tokio::fs::remove_file(path.as_ref()).await?;
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

#[derive(Clone)]
pub enum CacheValue {
    File(PathBuf, usize),
    Memory(Arc<Bytes>),
}

impl CacheValue {
    fn size(&self) -> usize {
        match self {
            CacheValue::File(_, size) => *size,
            CacheValue::Memory(data) => data.len(),
        }
    }
}

// Override the debug output to avoid printing out the entire raw byte content of `CacheValue::Memory`
impl Debug for CacheValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CacheValue::File(path, size) => write!(f, "File({path:?}, size: {size})"),
            CacheValue::Memory(data) => write!(f, "Memory(size: {})", data.len()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CachingObjectStore {
    // File manager, responsible for storing/retrieving files
    file_manager: Arc<CacheFileManager>,
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
    async fn on_evict(
        file_manager: Arc<CacheFileManager>,
        key: Arc<CacheKey>,
        value: CacheValue,
        cause: RemovalCause,
    ) {
        debug!(
            "An entry has been evicted. k: {:?}, v: {:?}, cause: {:?}",
            key, value, cause
        );

        if let CacheValue::File(path, _) = value {
            // Remove the data file. We must handle error cases here to
            // prevent the listener from panicking.
            if let Err(e) = file_manager.remove_file(&path).await {
                error!("Failed to remove a data file at {path:?}: {:?}", e);
            }
        }
    }

    pub fn new(
        inner: Arc<dyn ObjectStore>,
        base_path: &Path,
        min_fetch_size: u64,
        max_cache_size: u64,
        ttl: Duration,
    ) -> Self {
        let file_manager = Arc::new(CacheFileManager::new(base_path.to_owned()));

        // Clone the pointer since we can't pass the whole struct to the cache
        let eviction_file_manager = file_manager.clone();

        // TODO: experiment with time-to-idle
        let cache: Cache<CacheKey, CacheValue> = CacheBuilder::new(max_cache_size)
            .weigher(|_, v: &CacheValue| v.size() as u32)
            .async_eviction_listener(move |k, v, cause| {
                Self::on_evict(eviction_file_manager.clone(), k, v, cause).boxed()
            })
            .eviction_policy(EvictionPolicy::lru())
            .time_to_live(ttl)
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
    ///
    /// There is some nuance in the fetching process worth elaborating on. Namely, when there is a
    /// cache miss:
    /// - All concurrent calls for the same missing chunk are coalesced, thanks to `Cache::try_get_with`,
    ///   and so there is only one outbound request for the data.
    /// - As soon as the data is fetched we:
    ///     a) spawn a task to persist the data to the disk
    ///     b) add a cache entry with the in-memory data (which is shared by the write task)
    /// - Subsequently, all waiting calls are unblocked and will get the new cache value that is
    ///   either read from disk (if the write task finished quickly enough), or from memory (if the
    ///   write task is still running).
    /// - Once the write task completes it will either replace the cache value with a file pointer,
    ///   (if it completed successfully), or invalidate the memory entry (if it didn't).
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
                // The Arc here is solely to avoid copying the data into the closure below, as the
                // writing can be done through a reference as well.
                debug!("Fetching data for {key:?}");
                let data = Arc::new(self.inner.get_range(path, range).await?);

                // Run the blocking write + cache value insert in a separate task
                let cache = self.cache.clone();
                let file_manager = self.file_manager.clone();
                let size = data.len();
                let data_to_write = data.clone();
                tokio::task::spawn(async move {
                    match file_manager.write_file(&key, data_to_write).await {
                        Ok(path) => {
                            // Write task completed successfully, replace the in-memory cache entry
                            // with the file-pointer one.
                            debug!("Upserting file pointer for {key:?} into the cache");
                            let value = CacheValue::File(path, size);
                            cache.insert(key, value).await;
                        }
                        Err(err) => {
                            // Write task failed, remove the cache entry; we could also defer that to
                            // TTL/LRU eviction, but then we risk ballooning the memory usage.
                            warn!("Failed writing value for {key:?} to a file: {err}");
                            warn!("Removing cache entry");
                            cache.invalidate(&key).await;
                        }
                    }
                });

                // While the write task runs cache the in-memory bytes and serve that to all calls
                // prior to the write task completing.
                debug!("Caching value for ({path:?}, {chunk}) in memory");
                Ok(CacheValue::Memory(data))
            })
            .await
            .map_err(|e| object_store::Error::Generic {
                store: "cache_store",
                source: Box::new(e),
            })?;

        match value {
            CacheValue::File(path, _) => {
                debug!("Cache value for ({path:?}, {chunk}) fetched from the file");
                let data = self.file_manager.read_file(path).await.unwrap();
                Ok(data)
            }
            CacheValue::Memory(data) => {
                debug!("Cache value for ({path:?}, {chunk}) fetched from memory");
                Ok(data.as_ref().clone())
            }
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
    ) -> object_store::Result<PutResult> {
        self.inner.put(location, bytes).await
    }

    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        bytes: Bytes,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, bytes, opts).await
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

    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
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

        let mut result = Vec::with_capacity(range.end - range.start);

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

    /// Delete all the objects at the specified locations in bulk when applicable
    fn delete_stream<'a>(
        &'a self,
        locations: BoxStream<'a, object_store::Result<object_store::path::Path>>,
    ) -> BoxStream<'a, object_store::Result<object_store::path::Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
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
    use object_store::{path::Path, ObjectStore};
    use std::path::Path as FSPath;
    use std::sync::Arc;

    use crate::object_store::cache::{CacheKey, CacheValue, DEFAULT_CACHE_ENTRY_TTL};
    use rstest::rstest;
    use std::collections::HashSet;
    use std::time::Duration;
    use std::{cmp::min, fs, ops::Range};
    use tempfile::TempDir;

    use crate::testutils::make_mock_parquet_server;

    fn make_cached_object_store_small_fetch() -> CachingObjectStore {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.into_path();

        CachingObjectStore::new(
            Arc::new(HttpObjectStore::new("http".to_string(), &None)),
            &path,
            16,
            512,
            DEFAULT_CACHE_ENTRY_TTL,
        )
    }

    fn make_cached_object_store_small_disk_size() -> CachingObjectStore {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.into_path();

        CachingObjectStore::new(
            Arc::new(HttpObjectStore::new("http".to_string(), &None)),
            &path,
            16,
            4 * 16, // Max capacity 4 chunks
            Duration::from_secs(1),
        )
    }

    // Util function to wait until all keys are persisted to disk or it times out
    async fn wait_all_ranges_on_disk(
        on_disk_keys: HashSet<CacheKey>,
        store: &CachingObjectStore,
    ) -> HashSet<CacheKey> {
        let mut i = 1;
        loop {
            // Get all the keys for which the values are already on disk
            let new_on_disk_keys: HashSet<CacheKey> = store
                .cache
                .into_iter()
                .filter_map(|(k, v)| {
                    if matches!(v, CacheValue::File(_, _)) {
                        Some(k.as_ref().clone())
                    } else {
                        None
                    }
                })
                .collect();

            // Ensure that this is strictly monotonic, i.e. a given cache value can
            // only go from memory to disk, and not vice-versa
            assert!(new_on_disk_keys.is_superset(&on_disk_keys));
            let on_disk_keys = new_on_disk_keys;

            // If all values are on disk exit
            if on_disk_keys.len() == store.cache.entry_count() as usize {
                return on_disk_keys;
            }

            if i >= 5 {
                panic!("Failed to ensure cache value is on disk in 5 iterations")
            }
            i += 1;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    #[rstest]
    #[case::skip_0_partial_1_full_2_3(25..64)]
    #[case::part_of_chunk_0(1..2)]
    #[case::fetch_nothing_at_the_chunk_boundary(16..16)]
    #[case::fetch_one_byte_at_the_chunk_boundary(16..17)]
    #[case::part_of_chunk_0_part_of_chunk_1(10..20)]
    #[case::reaches_the_end_of_the_body(10..484)]
    #[case::goes_beyond_the_end_of_the_body(400..490)]
    // NB: going more than one chunk beyond the end of the body still makes it make
    // a Range request for e.g. 512 -- 527, which breaks the mock and also is a waste,
    // but we don't know that since we don't get Content-Length on this code path.
    #[tokio::test]
    async fn test_range_coalescing(#[case] range: Range<usize>) {
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
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .sorted()
            .collect_vec();

        assert_eq!(paths, expected);
    }

    #[tokio::test]
    async fn test_cache_caching_eviction() {
        let store = make_cached_object_store_small_disk_size();
        let (server, body) = make_mock_parquet_server(true, true).await;
        let server_uri = server.uri();
        let server_uri = server_uri.strip_prefix("http://").unwrap();
        let url = format!("{}/some/file.parquet", &server_uri);

        // Mock is empty
        assert_eq!(server.received_requests().await.unwrap().len(), 0);

        // Perform multiple sets of actions:
        //   - request a range
        //   - check raw bytes fetched correspond to the source
        //   - check the amount of requests that the mock received in total
        //   - `run_pending_tasks()` aka sync the cache
        //   - check the cache entry count
        //   - check the files that are on disk

        // Request 25..64 (chunks 1, 2, 3)
        let bytes = store
            .get_range(&Path::from(url.as_str()), 25..64)
            .await
            .unwrap();
        assert_eq!(bytes, body[25..64]);
        store.cache.run_pending_tasks().await;

        // Mock has had 3 requests
        assert_eq!(server.received_requests().await.unwrap().len(), 3);
        assert_eq!(store.cache.entry_count(), 3);

        let on_disk_keys = wait_all_ranges_on_disk(HashSet::new(), &store).await;
        assert_ranges_in_cache(&store.base_path, &url, vec![1, 2, 3]);

        // Request 26..30 (chunk 1)
        let bytes = store
            .get_range(&Path::from(url.as_str()), 26..30)
            .await
            .unwrap();
        assert_eq!(bytes, body[26..30]);
        store.cache.run_pending_tasks().await;

        // No extra requests
        assert_eq!(server.received_requests().await.unwrap().len(), 3);
        assert_eq!(store.cache.entry_count(), 3);
        assert_ranges_in_cache(&store.base_path, &url, vec![1, 2, 3]);

        // Request 33..66 (chunks 2, 3, 4)
        let bytes = store
            .get_range(&Path::from(url.as_str()), 33..66)
            .await
            .unwrap();
        assert_eq!(bytes, body[33..66]);
        store.cache.run_pending_tasks().await;

        // One extra request to fetch chunk 4
        assert_eq!(server.received_requests().await.unwrap().len(), 4);
        assert_eq!(store.cache.entry_count(), 4);

        let mut on_disk_keys = wait_all_ranges_on_disk(on_disk_keys, &store).await;
        assert_ranges_in_cache(&store.base_path, &url, vec![1, 2, 3, 4]);

        // Request 80..85 (chunk 5)
        let bytes = store
            .get_range(&Path::from(url.as_str()), 80..85)
            .await
            .unwrap();
        assert_eq!(bytes, body[80..85]);
        store.cache.run_pending_tasks().await;

        assert_eq!(server.received_requests().await.unwrap().len(), 5);
        assert_eq!(store.cache.entry_count(), 4);

        on_disk_keys.retain(|k| k.range.start >= 32); // The first chunk got LRU-evicted
        wait_all_ranges_on_disk(on_disk_keys, &store).await;
        assert_ranges_in_cache(&store.base_path, &url, vec![2, 3, 4, 5]);

        // Ensure that our TTL parameter is honored, so that we don't get a frozen cache after it
        // gets filled up once.
        let _ = tokio::time::sleep(Duration::from_secs(1)).await;
        store.cache.run_pending_tasks().await;

        assert_eq!(store.cache.entry_count(), 0);
        assert_ranges_in_cache(&store.base_path, &url, vec![]);
    }
}
