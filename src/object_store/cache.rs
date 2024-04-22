/// On-disk byte-range-aware cache for object stores
/// Partially inspired by https://docs.rs/moka/latest/moka/future/struct.Cache.html#example-eviction-listener,
/// with some additions to weigh it by the file size.
use crate::config::schema::{str_to_hex_hash, ObjectCacheProperties};
use async_trait::async_trait;
use bytes::{Buf, Bytes, BytesMut};
use futures::stream::BoxStream;
use metrics::{
    counter, describe_counter, describe_histogram, gauge, histogram, Counter, Gauge,
};
use moka::future::{Cache, CacheBuilder, FutureExt};
use moka::notification::RemovalCause;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore, PutOptions,
    PutResult,
};
use tempfile::TempDir;
use tokio::time::Instant;
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
    metrics: CachingObjectStoreMetrics,
}

impl CacheFileManager {
    pub fn new(base_path: PathBuf, metrics: CachingObjectStoreMetrics) -> Self {
        Self { base_path, metrics }
    }

    async fn write_file(&self, cache_key: &CacheKey, data: Bytes) -> io::Result<PathBuf> {
        let mut path = self.base_path.to_path_buf();
        path.push(cache_key.as_filename());

        // TODO: when does this happen?
        if path.exists() {
            debug!("{cache_key:?} file already exists, skipping write");
            self.metrics.double_write_errors.increment(1);
            return Ok(path.clone());
        }

        let start = Instant::now();
        tokio::fs::write(&path, data.as_ref()).await?;

        debug!("Written data for {:?} to {:?}", cache_key, path);
        self.metrics.log_cache_disk_write(start, data.len());
        Ok(path)
    }

    async fn read_file(&self, path: impl AsRef<Path>) -> io::Result<Bytes> {
        let start = Instant::now();
        tokio::fs::read(path).await.map(|v| {
            self.metrics.log_cache_disk_read(start, v.len());
            Bytes::from(v)
        })
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

const REQUESTS: &str = "seafowl_object_store_requests_total";
const REQUEST_LATENCY: &str = "seafowl_object_store_request_latency_seconds";

const REQUESTED_BYTES: &str =
    "seafowl_object_store_cache_get_range_requested_bytes_total";
const INBOUND_REQUESTS: &str = "seafowl_object_store_cache_get_range_requests_total";
const CACHE_HIT_READS: &str = "seafowl_object_store_cache_hit_read_bytes_total";
const CACHE_WRITES: &str = "seafowl_object_store_cache_disk_written_bytes_total";
const CACHE_DISK_TIME: &str = "seafowl_object_store_cache_disk_latency_seconds";
const CACHE_WARNINGS: &str = "seafowl_object_store_cache_warnings_total";
const CACHE_MISS_BYTES: &str = "seafowl_object_store_cache_get_range_read_bytes_total";

const CACHE_USAGE: &str = "seafowl_object_store_cache_usage_bytes";
const CACHE_CAPACITY: &str = "seafowl_object_store_cache_capacity_bytes";
const CACHE_EVICTED: &str = "seafowl_object_store_cache_evicted_bytes";

#[derive(Clone)]
pub struct CachingObjectStoreMetrics {
    get_range_calls: Counter,
    get_range_bytes: Counter,
    cache_miss_bytes: Counter,
    redownload_errors: Counter,
    double_write_errors: Counter,
    deletion_errors: Counter,
    cache_disk_read: Counter,
    cache_disk_write: Counter,
    cache_memory_read: Counter,
    cache_usage: Gauge,
    cache_capacity: Gauge,
    cache_evicted: Counter,
}

impl Default for CachingObjectStoreMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl CachingObjectStoreMetrics {
    pub fn new() -> Self {
        describe_counter!(REQUESTS, "Number of calls to the actual object store");
        describe_histogram!(
            REQUEST_LATENCY,
            "Time-to-first-byte of various requests to the actual object store"
        );
        describe_counter!(
            REQUESTED_BYTES,
            "Bytes requested in get_range calls by DataFusion before caching"
        );
        describe_counter!(
            INBOUND_REQUESTS,
            "Number of get_range requests from DataFusion before caching"
        );
        describe_counter!(
            CACHE_HIT_READS,
            "Bytes read from the object store cache (hit)"
        );
        describe_counter!(
            CACHE_DISK_TIME,
            "Time spent waiting for disk cache read / write"
        );
        describe_counter!(CACHE_WRITES, "Bytes written to on-disk cache");
        describe_counter!(
            CACHE_MISS_BYTES,
            "Bytes downloaded from the upstream object store for get_range cache misses"
        );
        describe_counter!(
            CACHE_WARNINGS,
            "Number of times various cache race conditions were discovered (read-after-evict, double-write, double-delete)"
        );
        describe_counter!(CACHE_USAGE, "Approximate current occupation of the cache");
        describe_counter!(CACHE_CAPACITY, "Total cache capacity");
        describe_counter!(CACHE_EVICTED, "Bytes evicted from cache");

        Self {
            get_range_calls: counter!(INBOUND_REQUESTS),
            get_range_bytes: counter!(REQUESTED_BYTES),
            cache_miss_bytes: counter!(CACHE_MISS_BYTES),
            redownload_errors: counter!(CACHE_WARNINGS, "error" => "redownload"),
            double_write_errors: counter!(CACHE_WARNINGS, "error" => "double_write"),
            deletion_errors: counter!(CACHE_WARNINGS, "error" => "deletion"),
            cache_memory_read: counter!(CACHE_HIT_READS, "location" => "memory"),
            cache_disk_read: counter!(CACHE_HIT_READS, "location" => "disk"),
            cache_disk_write: counter!(CACHE_WRITES),
            cache_usage: gauge!(CACHE_USAGE),
            cache_capacity: gauge!(CACHE_CAPACITY),
            cache_evicted: counter!(CACHE_EVICTED),
        }
    }

    pub fn log_object_store_outbound_request(
        &self,
        start: Instant,
        operation: &'static str,
        success: bool,
    ) {
        // TODO: start using this function by wrapping all other object store requests
        let count = counter!(REQUESTS, "operation" => operation, "status" => if success { "true" } else { "false" });
        let latency = histogram!(REQUEST_LATENCY, "operation" => operation, "status" => if success { "true" } else { "false" });

        count.increment(1);
        latency.record(start.elapsed().as_secs_f64());
    }

    pub fn log_get_range_outbound_request(
        &self,
        start: Instant,
        result: &object_store::Result<Bytes>,
    ) {
        self.log_object_store_outbound_request(start, "get_range", result.is_ok());
        if let Ok(data) = result {
            self.cache_miss_bytes
                .increment(data.len().try_into().unwrap())
        }
    }

    pub fn log_get_range_request(&self, length: usize) {
        self.get_range_calls.increment(1);
        self.get_range_bytes.increment(length.try_into().unwrap());
    }

    pub fn log_cache_memory_read(&self, length: usize) {
        self.cache_memory_read.increment(length.try_into().unwrap());
    }

    pub fn log_cache_disk_read(&self, start: Instant, length: usize) {
        self.cache_disk_read.increment(length.try_into().unwrap());
        histogram!(CACHE_DISK_TIME, "operation" => "read")
            .record(start.elapsed().as_secs_f64());
    }
    pub fn log_cache_disk_write(&self, start: Instant, length: usize) {
        self.cache_disk_write.increment(length.try_into().unwrap());
        histogram!(CACHE_DISK_TIME, "operation" => "write")
            .record(start.elapsed().as_secs_f64());
    }
}

impl Debug for CachingObjectStoreMetrics {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachingObjectStoreMetrics").finish()
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
    metrics: CachingObjectStoreMetrics,
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

        if cause != RemovalCause::Replaced {
            file_manager
                .metrics
                .cache_evicted
                .increment(value.size().try_into().unwrap());
            file_manager
                .metrics
                .cache_usage
                .decrement(value.size() as f64);
        };

        if let CacheValue::File(path, _) = value {
            // Remove the data file. We must handle error cases here to
            // prevent the listener from panicking.
            if let Err(e) = file_manager.remove_file(&path).await {
                file_manager.metrics.deletion_errors.increment(1);
                error!("Failed to remove a data file at {path:?}: {:?}", e);
            }
        }
    }

    pub fn new_from_config(
        config: &ObjectCacheProperties,
        inner: Arc<dyn ObjectStore>,
    ) -> Self {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.into_path();

        Self::new(
            inner,
            &path,
            config.min_fetch_size,
            config.capacity,
            Duration::from_secs(config.ttl),
        )
    }

    pub fn new(
        inner: Arc<dyn ObjectStore>,
        base_path: &Path,
        min_fetch_size: u64,
        max_cache_size: u64,
        ttl: Duration,
    ) -> Self {
        let metrics = CachingObjectStoreMetrics::new();
        metrics.cache_capacity.set(max_cache_size as f64);
        metrics.cache_usage.set(0.0);

        let file_manager =
            Arc::new(CacheFileManager::new(base_path.to_owned(), metrics.clone()));

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
            metrics,
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
            // Each metric is an Arc and we accumulate them across
            // all object stores, so it's fine to just clone them
            metrics: other.metrics.clone(),
        }
    }

    async fn get_range_inner(
        &self,
        location: &object_store::path::Path,
        range: Range<usize>,
    ) -> object_store::Result<Bytes> {
        let start = Instant::now();
        let result = self.inner.get_range(location, range).await;
        self.metrics.log_get_range_outbound_request(start, &result);
        result
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
                            self.metrics.log_cache_memory_read(data.len());
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

                                    self.metrics.redownload_errors.increment(1);
                                    let data = self
                                        .get_range_inner(location, chunk_range.clone())
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
                    self.get_range_inner(location, batch_range.clone()).await?;

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

        // Record the cache capacity here (weighted_size reads a variable and
        // doesn't scan the cache, so this is a lightweight operation)
        // Also clone the metric pointer so that we can update it once again once we
        // wrote it to disk / invalidated for maximum precision.
        let cache_usage = self.metrics.cache_usage.clone();
        cache_usage.set(self.cache.weighted_size() as f64);

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
                };
                cache_usage.set(cache.weighted_size() as f64);
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
        self.metrics.get_range_calls.increment(1);
        self.metrics
            .get_range_bytes
            .increment((range.end - range.start).try_into().unwrap());

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
    use metrics::with_local_recorder;
    use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusRecorder};
    use object_store::{path::Path, ObjectStore};
    use std::path::Path as FSPath;
    use std::sync::Arc;

    use crate::object_store::cache::{CacheKey, CacheValue, DEFAULT_CACHE_ENTRY_TTL};
    use rstest::rstest;
    use std::collections::HashSet;
    use std::time::Duration;
    use std::{cmp::min, fs, ops::Range};
    use tempfile::TempDir;

    use super::{
        CACHE_EVICTED, CACHE_MISS_BYTES, CACHE_USAGE, CACHE_WRITES, INBOUND_REQUESTS,
        REQUESTED_BYTES,
    };
    use crate::testutils::make_mock_parquet_server;

    const CACHE_DISK_READ: &str =
        "seafowl_object_store_cache_hit_read_bytes_total{location=\"disk\"}";

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

    fn assert_metric(recorder: &PrometheusRecorder, key: &str, value: u64) {
        let rendered = recorder.handle().render();
        let metric_line = rendered
            .lines()
            .find(|l| l.starts_with(&format!("{key} ")))
            .unwrap_or_else(|| panic!("no metric {key} found"));
        assert_eq!(metric_line, format!("{key} {value}"))
    }

    #[rstest]
    #[case::skip_0_partial_1_full_2_3(25..64, 48)]
    #[case::part_of_chunk_0(1..2, 16)]
    #[case::fetch_nothing_at_the_chunk_boundary(16..16, 0)]
    #[case::fetch_one_byte_at_the_chunk_boundary(16..17, 16)]
    #[case::part_of_chunk_0_part_of_chunk_1(10..20, 32)]
    #[case::reaches_the_end_of_the_body(10..484, 496)]
    #[case::goes_beyond_the_end_of_the_body(400..490, 96)]
    // NB: going more than one chunk beyond the end of the body still makes it make
    // a Range request for e.g. 512 -- 527, which breaks the mock and also is a waste,
    // but we don't know that since we don't get Content-Length on this code path.
    #[tokio::test]
    async fn test_range_coalescing(
        #[case] range: Range<usize>,
        #[case] total_fetched: u64,
    ) {
        let recorder = PrometheusBuilder::new().build_recorder();
        let store = with_local_recorder(&recorder, make_cached_object_store_small_fetch);

        let (server, body) = make_mock_parquet_server(true, true).await;
        let server_uri = server.uri();
        let server_uri = server_uri.strip_prefix("http://").unwrap();
        let url = format!("{}/some/file.parquet", &server_uri);

        let result = store
            .get_range(&Path::from(url.as_str()), range.clone())
            .await
            .unwrap();
        assert_eq!(result, body[range.start..min(body.len(), range.end)]);

        assert_metric(&recorder, INBOUND_REQUESTS, 1);
        assert_metric(
            &recorder,
            REQUESTED_BYTES,
            (range.end - range.start).try_into().unwrap(),
        );

        // These are all going to be cache misses since we create the store
        // from scratch every time
        print!("{}", recorder.handle().render());
        assert_metric(&recorder, CACHE_MISS_BYTES, total_fetched);
        assert_metric(&recorder, CACHE_DISK_READ, 0);
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
        let recorder = PrometheusBuilder::new().build_recorder();
        let store =
            with_local_recorder(&recorder, make_cached_object_store_small_disk_size);

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

        assert_metric(&recorder, CACHE_MISS_BYTES, 48);
        assert_metric(&recorder, CACHE_DISK_READ, 0);
        assert_metric(&recorder, CACHE_EVICTED, 0);

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

        // NB: disk/memory hits round to the chunk size
        assert_metric(&recorder, CACHE_MISS_BYTES, 48);
        assert_metric(&recorder, CACHE_DISK_READ, 16);
        assert_metric(&recorder, CACHE_USAGE, 48);
        assert_metric(&recorder, CACHE_EVICTED, 0);

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

        assert_metric(&recorder, CACHE_MISS_BYTES, 64);
        assert_metric(&recorder, CACHE_DISK_READ, 48);

        assert_metric(&recorder, CACHE_USAGE, 48);
        assert_metric(&recorder, CACHE_EVICTED, 0);

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

        assert_metric(&recorder, CACHE_MISS_BYTES, 80);
        assert_metric(&recorder, CACHE_DISK_READ, 48);
        assert_metric(&recorder, CACHE_WRITES, 64);
        assert_metric(&recorder, CACHE_USAGE, 48);
        assert_metric(&recorder, CACHE_EVICTED, 16);

        on_disk_keys.retain(|k| k.range.start >= 32); // The first chunk got LRU-evicted
        wait_all_ranges_on_disk(on_disk_keys, &store).await;
        assert_ranges_in_cache(&store.base_path, &url, vec![2, 3, 4, 5]);

        // Ensure that our TTL parameter is honored, so that we don't get a frozen cache after it
        // gets filled up once.
        let _ = tokio::time::sleep(Duration::from_secs(1)).await;
        store.cache.run_pending_tasks().await;

        assert_eq!(store.cache.entry_count(), 0);
        assert_ranges_in_cache(&store.base_path, &url, vec![]);
        assert_metric(&recorder, CACHE_EVICTED, 80);
    }
}
