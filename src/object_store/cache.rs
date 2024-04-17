/// On-disk byte-range-aware cache for HTTP requests
/// Partially inspired by https://docs.rs/moka/latest/moka/future/struct.Cache.html#example-eviction-listener,
/// with some additions to weigh it by the file size.
use crate::config::schema::str_to_hex_hash;
use async_trait::async_trait;
use bytes::{Buf, Bytes, BytesMut};
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

    async fn write_file(&self, cache_key: &CacheKey, data: Bytes) -> io::Result<PathBuf> {
        let mut path = self.base_path.to_path_buf();
        path.push(cache_key.as_filename());

        // TODO: when does this happen?
        if path.exists() {
            debug!("{cache_key:?} file already exists, skipping write");
            return Ok(path.clone());
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

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct CacheKey {
    path: object_store::path::Path,
    range: Range<usize>,
}

impl Debug for CacheKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{:?}", self.path, self.range)
    }
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
    Memory(Bytes),
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

    /// Get a continuous range of chunks, each delineated in units of self.min_fetch_size. The main
    /// goal is to coalesce fetching of any missing chunks in batches of maximum range to minimize
    //  the outgoing calls needed to satisfy the incoming call.
    ///
    /// The algorithm works as follows:
    /// - If a chunk is missing from the cache add it to a batch of pending chunks to fetch at once.
    /// - If the chunk is present in the cache fetch the entire pending chunk batch (if any) and
    ///     a) add a cache entry with the in-memory data (which is shared by the write task)
    ///     b) spawn a task to persist the data to the disk
    /// - All subsequent calls will get the new cache value that is either read from disk (if the
    ///     write task finished quickly enough), or from memory (if the write task is still running).
    /// - Once the write task completes it will either replace the cache value with a file pointer,
    ///   (if it completed successfully), or invalidate the memory entry (if it didn't).
    ///
    /// NB: This is a best-effort implementation, i.e. there are no synchronization primitives used
    /// so there is no guarantee that another thread won't duplicate some of the requests.
    async fn get_chunk_range(
        &self,
        location: &object_store::path::Path,
        start_chunk: usize,
        end_chunk: usize,
    ) -> object_store::Result<Bytes> {
        let mut result = BytesMut::with_capacity(
            (end_chunk.saturating_sub(start_chunk) + 1) * self.min_fetch_size as usize,
        );

        let mut chunk_batch = vec![];
        for chunk in start_chunk..=end_chunk {
            let chunk_range = (chunk * self.min_fetch_size as usize)
                ..((chunk + 1) * self.min_fetch_size as usize);

            let key = CacheKey {
                path: location.to_owned(),
                range: chunk_range.clone(),
            };

            let chunk_data = match self.cache.get(&key).await {
                // If the value is missing extend the chunk range to fetch and continue
                None => {
                    chunk_batch.push(key);
                    None
                }
                Some(value) => {
                    // Now get the cache value for the current chunk
                    match value {
                        CacheValue::Memory(data) => {
                            debug!("Cache value for {key:?} fetched from memory");
                            Some(data.clone())
                        }
                        CacheValue::File(path, _) => {
                            debug!("Cache value for {key:?} fetching from the file");
                            match self.file_manager.read_file(path).await {
                                Ok(data) => Some(data),
                                Err(err) => {
                                    warn!(
                                        "Re-downloading cache value for {key:?}: {err}"
                                    );
                                    let data = self
                                        .inner
                                        .get_range(location, chunk_range.clone())
                                        .await?;
                                    self.cache_chunk_data(key, data.clone()).await;
                                    Some(data)
                                }
                            }
                        }
                    }
                }
            };

            if (chunk_data.is_some() || chunk == end_chunk) && !chunk_batch.is_empty() {
                // We either got a value, or are at the last chunk, so first we need to resolve any
                // outstanding coalesced chunk requests thus far.
                let first = chunk_batch.first().unwrap();
                let last = chunk_batch.last().unwrap();

                let batch_range = first.range.start..last.range.end;
                debug!("{location}-{batch_range:?} fetching");
                let mut batch_data =
                    self.inner.get_range(location, batch_range.clone()).await?;
                debug!("{location}-{batch_range:?} fetched");
                result.extend_from_slice(&batch_data);

                for key in &chunk_batch {
                    // Split the next chunk from the batch
                    let data = if batch_data.len() < self.min_fetch_size as usize {
                        batch_data.clone()
                    } else {
                        batch_data.split_to(self.min_fetch_size as usize)
                    };

                    self.cache_chunk_data(key.clone(), data).await;
                }

                chunk_batch = vec![];
            }

            // Finally append the current chunk data (if not included in the batch above).
            if let Some(data) = chunk_data {
                result.extend_from_slice(&data);
            }
        }

        Ok(result.into())
    }

    async fn cache_chunk_data(&self, key: CacheKey, data: Bytes) {
        // Cache the memory value
        let entry = self
            .cache
            .entry_by_ref(&key)
            .or_insert(CacheValue::Memory(data.clone()))
            .await;

        // Finally trigger persisting to disk
        if entry.is_fresh() {
            let cache = self.cache.clone();
            let file_manager = self.file_manager.clone();
            tokio::spawn(async move {
                // Run pending tasks to avert eviction races.
                cache.run_pending_tasks().await;
                let size = data.len();
                match file_manager.write_file(&key, data).await {
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
                        warn!("Invalidating cache entry for {key:?}; failed writing to a file: {err}");
                        cache.invalidate(&key).await;
                    }
                }
            });
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
        debug!("{location}-{range:?} get_range");
        // Expand the range to the next max_fetch_size (+ alignment)
        let start_chunk = range.start / self.min_fetch_size as usize;
        // The final chunk to fetch (inclusively). E.g. with min_fetch_size = 16:
        //  - range.end == 64 (get bytes 0..63 inclusive) -> final chunk is 63 / 16 = 3 (48..64 exclusive)
        //  - range.end == 65 (get bytes 0..64 exclusive) -> final chunk is 64 / 16 = 4 (64..72 exclusive)
        let end_chunk = (range.end - 1) / self.min_fetch_size as usize;

        let mut data = self
            .get_chunk_range(location, start_chunk, end_chunk)
            .await?;

        // Finally trim away the expanded range from the chunks that are outside the requested range
        let offset = range.start - start_chunk * self.min_fetch_size as usize;
        data.advance(offset);
        data.truncate(range.end - range.start);
        debug!("{location}-{range:?} return");
        Ok(data)
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

        // Mock has had 1 request that coalesced 3 chunks
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
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
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
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
        assert_eq!(server.received_requests().await.unwrap().len(), 2);
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

        assert_eq!(server.received_requests().await.unwrap().len(), 3);
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
