use crate::config::schema::str_to_hex_hash;
use bytes::{Buf, BufMut, Bytes};
use log::{debug, error};
/// On-disk byte-range-aware cache for HTTP requests
/// Partially inspired by https://docs.rs/moka/latest/moka/future/struct.Cache.html#example-eviction-listener,
/// with some additions to weigh it by the file size.
use moka::future::{Cache, CacheBuilder};
use moka::notification::RemovalCause;
use std::error::Error;
use std::fmt::Debug;
use std::future::Future;
use std::io;
use std::io::ErrorKind;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use tokio::{fs, sync::RwLock};
use url::Url;

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

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct CacheKey {
    url: Url,
    range: Range<u64>,
}

impl CacheKey {
    fn as_filename(&self) -> String {
        format!(
            "{}-{}-{}",
            str_to_hex_hash(self.url.to_string().as_str()),
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

pub struct HttpCache {
    file_manager: Arc<RwLock<CacheFileManager>>,
    base_path: PathBuf,
    min_fetch_size: u64,
    max_disk_size: u64,

    cache: Cache<CacheKey, CacheValue>,
}

pub enum CacheError {
    FetcherError(Box<dyn Error + Send + Sync>),
    IoError(io::Error),
}

impl HttpCache {
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

    pub fn new(base_path: &Path, min_fetch_size: u64, max_disk_size: u64) -> Self {
        let file_manager =
            Arc::new(RwLock::new(CacheFileManager::new(base_path.to_owned())));

        // Clone the pointer since we can't pass the whole struct to the cache
        let eviction_file_manager = file_manager.clone();

        let cache: Cache<CacheKey, CacheValue> = CacheBuilder::new(max_disk_size)
            .weigher(|_, v: &CacheValue| v.size as u32)
            .eviction_listener_with_queued_delivery_mode(move |k, v, cause| {
                Self::on_evict(&eviction_file_manager, k, v, cause)
            })
            .build();

        Self {
            file_manager,
            base_path: base_path.to_owned(),
            min_fetch_size,
            max_disk_size,
            cache,
        }
    }

    /// Get a certain range chunk, delineated in units of self.min_fetch_size. If the chunk
    /// is cached, return it directly. Otherwise, fetch and return it.
    async fn get_chunk(
        &self,
        url: &Url,
        chunk: u32,
        fetch_func: impl Fn(
            &Url,
            &Range<u64>,
        )
            -> Pin<Box<dyn Future<Output = Result<Bytes, Box<dyn Error + Send + Sync>>>>>,
    ) -> Result<Bytes, Arc<CacheError>> {
        let range = (chunk as u64 * self.min_fetch_size)
            ..((chunk + 1) as u64 * self.min_fetch_size);

        let key = CacheKey {
            url: url.clone(),
            range: range.clone(),
        };

        let value = self
            .cache
            .try_get_with::<_, CacheError>(key.clone(), async move {
                let mut manager = self.file_manager.write().await;
                let data = fetch_func(url, &range)
                    .await
                    .map_err(CacheError::FetcherError)?;
                let path = manager
                    .write_file(key, &data)
                    .await
                    .map_err(CacheError::IoError)?;

                Ok(CacheValue {
                    path,
                    size: data.len() as u64,
                })
            })
            .await?;

        {
            let manager = self.file_manager.read().await;
            let data = manager.read_file(value.path).await.unwrap();
            Ok(data)
        }
    ,}

    pub async fn get_range(
        &self,
        url: &Url,
        range: &Range<u64>,
        fetch_func: impl Fn(
            &Url,
            &Range<u64>,
        )
            -> Pin<Box<dyn Future<Output = Result<Bytes, Box<dyn Error + Send + Sync>>>>>,
    ) -> Result<Bytes, Arc<CacheError>> {
        // Expand the range to the next max_fetch_size (+ alignment)
        let start_chunk = (range.start / self.min_fetch_size) as u32;
        let end_chunk = (range.end / self.min_fetch_size) as u32;

        let mut result = Vec::with_capacity((range.end - range.start + 1) as usize);

        for chunk_num in start_chunk..(end_chunk + 1) {
            let mut data = self.get_chunk(url, chunk_num, &fetch_func).await?;

            let buf_start = if chunk_num == start_chunk {
                let buf_start = (range.start % self.min_fetch_size) as usize;
                data.advance(buf_start);
                buf_start
            } else {
                0usize
            };

            let buf_end = if chunk_num == end_chunk {
                (range.end % self.min_fetch_size) as usize
            } else {
                data.len()
            };

            result.put(data.take(buf_end - buf_start));
        }

        Ok(result.into())
    }
}
