use std::collections::HashMap;
use std::ffi::{CStr, c_char, c_int, c_void};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Instant;

use parking_lot::Mutex;
use xxhash_rust::xxh3::xxh3_64;

tonic::include_proto!("grpc_vfs");

mod env_config;
mod handle;

struct Capabilities {
    atomic_batch: bool,
    point_in_time_reads: bool,
    sector_size: i32,
    heartbeat_interval_millis: i64,
}

#[derive(Clone, Debug)]
struct CachedPage {
    pub offset: u64,
    pub file_name: String,
    pub database: String,
    pub checksum: u64,
}

struct GrpcVfs {
    runtime: tokio::runtime::Runtime,
    capabilities: Capabilities,
    grpc_client: grpsqlite_client::GrpsqliteClient<tonic::transport::Channel>,
    context: String,
    files: Arc<Mutex<Vec<handle::GrpcVfsHandle>>>,
    lease: Arc<Mutex<Option<String>>>,
    write_batch: Arc<Mutex<Vec<AtomicWrite>>>,
    write_batch_open: AtomicBool,
    current_read_timestamp: Arc<Mutex<Option<i64>>>,
    heartbeat_shutdown_tx: Arc<Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
    first_page_cache: Arc<Mutex<Vec<u8>>>,
    database: Arc<Mutex<String>>,
    env_config: env_config::EnvConfig,
    /// Cache of cache_keys (database:file:offset) -> CachedPage info
    cached_pages: Option<tinyufo::TinyUfo<String, CachedPage>>,
    /// Track checksums for cached pages: cache_key -> checksum
    cached_page_checksums: Arc<Mutex<HashMap<String, CachedPage>>>,
}

fn cache_key(database: &str, file_name: &str, offset: u64) -> String {
    format!("{}:{}:{}", database, file_name, offset)
}

fn cache_file_path(cache_dir: &str, cache_key: &str) -> PathBuf {
    Path::new(cache_dir).join(cache_key)
}

impl GrpcVfs {
    async fn create_grpc_client(
        config: &env_config::EnvConfig,
    ) -> Result<
        grpsqlite_client::GrpsqliteClient<tonic::transport::Channel>,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        let endpoint = tonic::transport::Endpoint::from_shared(config.grpc_vfs_url.clone())?
            .connect_timeout(std::time::Duration::from_secs(
                config.grpc_vfs_connect_timeout_secs,
            ))
            .keep_alive_timeout(std::time::Duration::from_secs(
                config.grpc_vfs_connect_timeout_secs,
            ))
            .keep_alive_while_idle(true)
            .http2_keep_alive_interval(std::time::Duration::from_secs(
                config.grpc_vfs_connect_timeout_secs,
            ));

        let client = grpsqlite_client::GrpsqliteClient::connect(endpoint).await?;
        Ok(client)
    }

    pub fn new() -> Self {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .enable_io()
            .build()
            .unwrap(); // SQLite is single-threaded, so we can use a single-threaded runtime

        let config = env_config::EnvConfig::new();

        let (client, capabilities, context) = runtime.block_on(async {
            let mut client = Self::create_grpc_client(&config).await.unwrap();

            let req = GetCapabilitiesRequest {
                client_token: std::env::var("GRPC_VFS_CLIENT_TOKEN")
                    .unwrap_or_else(|_| "".to_string()),
                readonly: false,
            };

            let start = Instant::now();
            let res = client.get_capabilities(req).await;
            log::debug!("get_capabilities took {:?}", start.elapsed());
            let capabilities_response = res.unwrap().into_inner();
            let capabilities = Capabilities {
                atomic_batch: capabilities_response.atomic_batch,
                point_in_time_reads: capabilities_response.point_in_time_reads,
                sector_size: capabilities_response.sector_size,
                heartbeat_interval_millis: capabilities_response.heartbeat_interval_millis,
            };

            (client, capabilities, capabilities_response.context)
        });

        // If the local cache dir is set, make sure it exists
        if let Some(local_cache_dir) = &config.local_cache_dir {
            std::fs::create_dir_all(local_cache_dir).unwrap();
        }
        let local_page_cache = {
            if config.local_cache_dir.is_some() && config.max_cache_bytes.is_some() {
                let max_cache_bytes = config.max_cache_bytes.unwrap();
                let max_pages = (max_cache_bytes / capabilities.sector_size as u64).max(1);
                Some(tinyufo::TinyUfo::new(
                    max_pages as usize,
                    max_pages as usize,
                ))
            } else {
                None
            }
        };

        Self {
            runtime,
            context,
            capabilities,
            grpc_client: client,
            files: Arc::new(Mutex::new(vec![])),
            lease: Arc::new(Mutex::new(None)),
            write_batch: Arc::new(Mutex::new(vec![])),
            write_batch_open: AtomicBool::new(false),
            current_read_timestamp: Arc::new(Mutex::new(None)),
            heartbeat_shutdown_tx: Arc::new(Mutex::new(None)),
            first_page_cache: Arc::new(Mutex::new(vec![])),
            database: Arc::new(Mutex::new(String::new())),
            env_config: config,
            cached_page_checksums: Arc::new(Mutex::new(HashMap::new())),
            cached_pages: local_page_cache,
        }
    }

    fn start_heartbeat_thread(&self) {
        // Create shutdown channel
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
        *self.heartbeat_shutdown_tx.lock() = Some(shutdown_tx);

        // Launch heartbeat thread to keep the lease alive
        let context = self.context.clone();
        let lease = self.lease.clone();
        let heartbeat_interval = if self.capabilities.heartbeat_interval_millis == 0 {
            5000
        } else {
            self.capabilities.heartbeat_interval_millis
        } as u64;

        log::debug!(
            "starting heartbeat thread with interval: {}ms",
            heartbeat_interval
        );

        let env_config = self.env_config.clone();

        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .enable_io()
                .build()
                .unwrap();

            rt.block_on(async {
                // Create a new gRPC client within this runtime using the factory (otherwise it will hang because of the shared channel)
                let grpc_client = match GrpcVfs::create_grpc_client(&env_config).await {
                    Ok(client) => client,
                    Err(e) => {
                        log::error!("Failed to connect gRPC client in heartbeat thread: {:?}", e);
                        return;
                    }
                };
                let mut interval =
                    tokio::time::interval(std::time::Duration::from_millis(heartbeat_interval));

                // Consume the immediate first tick - we want to wait one full interval before first heartbeat
                interval.tick().await;

                loop {
                    tokio::select! {
                        _ = interval.tick() => {
                            // Check if we still have a lease
                            let lease_id = {
                                let lease_guard = lease.lock();
                                lease_guard.as_ref().map(|l| l.clone())
                            };

                            if let Some(lease_id) = lease_id {
                                let req = HeartbeatLeaseRequest {
                                    context: context.clone(),
                                    lease_id,
                                };

                                log::debug!("heartbeating lease: {:?}", req);

                                // Add timeout to prevent hanging
                                let start = std::time::Instant::now();

                                match tokio::time::timeout(
                                    std::time::Duration::from_millis(heartbeat_interval),
                                    grpc_client.clone().heartbeat_lease(req)
                                ).await {
                                    Ok(Ok(_)) => {
                                        log::debug!("heartbeat successful in {:?}", start.elapsed());
                                    }
                                    Ok(Err(status)) => {
                                        if status.code() == tonic::Code::NotFound {
                                            log::error!("heartbeat failed in {:?} - lease not found: {:?}", start.elapsed(), status);
                                            log::debug!("lease has been lost, exiting heartbeat loop");
                                            break;
                                        } else {
                                            log::warn!(
                                                "heartbeat failed with transient error: {:?}",
                                                status
                                            );
                                            // Continue the loop for transient errors
                                        }
                                    }
                                    Err(_) => {
                                        log::warn!("heartbeat timed out after {}ms, continuing...", heartbeat_interval);
                                        // Continue the loop on timeout
                                    }
                                }
                            } else {
                                // No lease to heartbeat, exit the loop
                                log::debug!("no lease to heartbeat, exiting heartbeat loop");
                                break;
                            }
                        }
                        _ = &mut shutdown_rx => {
                            log::debug!("heartbeat thread received shutdown signal");
                            break;
                        }
                    }
                }
            });
        });
    }
}

impl sqlite_plugin::vfs::Vfs for GrpcVfs {
    type Handle = handle::GrpcVfsHandle;

    fn register_logger(&self, logger: sqlite_plugin::logger::SqliteLogger) {
        struct LogCompat {
            logger: Mutex<sqlite_plugin::logger::SqliteLogger>,
        }

        impl log::Log for LogCompat {
            fn enabled(&self, _metadata: &log::Metadata) -> bool {
                true
            }

            fn log(&self, record: &log::Record) {
                // println!("log: {:?}", record);
                let level = match record.level() {
                    log::Level::Error => sqlite_plugin::logger::SqliteLogLevel::Error,
                    log::Level::Warn => sqlite_plugin::logger::SqliteLogLevel::Warn,
                    _ => sqlite_plugin::logger::SqliteLogLevel::Notice,
                };
                let msg = format!("{}", record.args());
                self.logger.lock().log(level, msg.as_bytes());
            }

            fn flush(&self) {}
        }

        let log = LogCompat {
            logger: Mutex::new(logger),
        };
        if let Err(e) = log::set_boxed_logger(Box::new(log)) {
            // Logger already set, ignore the error
            eprintln!("Logger already initialized: {}", e);
        }
    }

    fn open(
        &self,
        path: Option<&str>,
        opts: sqlite_plugin::flags::OpenOpts,
    ) -> sqlite_plugin::vfs::VfsResult<Self::Handle> {
        log::debug!("open: path={}, opts={:?}", path.unwrap_or(""), opts);
        let mode = opts.mode();

        if mode.is_readonly() && !self.capabilities.point_in_time_reads {
            log::error!("read-only mode is not supported for this server");
            return Err(sqlite_plugin::vars::SQLITE_CANTOPEN);
        }

        let mut database = None;

        if opts.kind() == sqlite_plugin::flags::OpenKind::MainDb {
            let database_name = path
                .and_then(|p| Path::new(p).file_name())
                .and_then(|name| name.to_str())
                .unwrap_or("")
                .to_string();
            log::debug!("database name: {}", database_name);
            database = Some(database_name.clone());
            *self.database.lock() = database_name;
        }

        // acquire a lease if we are RW
        if !mode.is_readonly() && opts.kind() == sqlite_plugin::flags::OpenKind::MainDb {
            let lease_result = self.runtime.block_on(async {
                let req = AcquireLeaseRequest {
                    context: self.context.clone(),
                    database: self.database.lock().clone(),
                };

                log::debug!("acquiring lease for main db {}", path.unwrap_or(""));
                let start = Instant::now();
                match self.grpc_client.clone().acquire_lease(req).await {
                    Ok(response) => {
                        log::debug!("acquire_lease completed in {:?}", start.elapsed());
                        let lease_response = response.into_inner();
                        log::debug!("lease acquired: {:?}", lease_response.lease_id);
                        *self.lease.lock() = Some(lease_response.lease_id);

                        // Start the heartbeat thread to keep the lease alive
                        self.start_heartbeat_thread();

                        Ok(())
                    }
                    Err(status) => {
                        log::debug!("acquire_lease errored in {:?}", start.elapsed());
                        log::error!("failed to acquire lease: {:?}", status);
                        Err(sqlite_plugin::vars::SQLITE_CANTOPEN)
                    }
                }
            });

            if let Err(err) = lease_result {
                return Err(err);
            }
        }

        if self.env_config.preload_cache == true {
            // TODO: spawn preload cache with env_config.preload_cache_concurrency
            log::debug!("TODO kicking off preload cache");
        }

        let handle = handle::GrpcVfsHandle::new(
            path.unwrap_or("").to_string(),
            mode.is_readonly(),
            opts.kind() == sqlite_plugin::flags::OpenKind::MainDb,
            database,
        );
        self.files.lock().push(handle.clone());
        Ok(handle)
    }

    fn delete(&self, path: &str) -> sqlite_plugin::vfs::VfsResult<()> {
        log::debug!("delete: path={}", path);

        // Delete over the server
        let database = { self.database.lock().clone() };
        let result = self.runtime.block_on(async {
            let req = DeleteRequest {
                context: self.context.clone(),
                database: database.clone(),
                lease_id: self.lease.lock().clone().unwrap_or("".to_string()),
                file_name: path.to_string(),
            };

            let start = Instant::now();
            match self.grpc_client.clone().delete(req).await {
                Ok(response) => {
                    log::debug!("delete completed in {:?}", start.elapsed());
                    log::debug!("delete successful: {:?}", response.into_inner());
                    Ok(())
                }
                Err(status) => {
                    log::debug!("delete errored in {:?}", start.elapsed());
                    log::error!("delete failed: {:?}", status);
                    Err(sqlite_plugin::vars::SQLITE_IOERR_DELETE)
                }
            }
        });

        // Delete from the cache
        let mut cache_checksums = self.cached_page_checksums.lock();
        let deleted_keys = cache_checksums
            .iter()
            .filter(|(k, _)| k.starts_with(path))
            .map(|(k, _)| k.clone())
            .collect::<Vec<String>>();
        for key in deleted_keys {
            log::debug!("deleting cache file: {}", key);
            cache_checksums.remove(&key);
            self.cached_pages.as_ref().unwrap().remove(&key);
            let cached_path =
                cache_file_path(self.env_config.local_cache_dir.as_ref().unwrap(), &key);
            if let Err(e) = std::fs::remove_file(&cached_path) {
                log::error!("failed to remove cache file {:?}: {}", cached_path, e);
                return Err(sqlite_plugin::vars::SQLITE_IOERR_DELETE);
            }
        }

        match result {
            Ok(_) => {
                // Delete locally only if server delete succeeded
                let mut files = self.files.lock();
                files.retain(|f| f.file_path != path);

                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    fn access(
        &self,
        path: &str,
        flags: sqlite_plugin::flags::AccessFlags,
    ) -> sqlite_plugin::vfs::VfsResult<bool> {
        let exists = self.files.lock().iter().any(|f| f.file_path == path);
        log::debug!("access: path={path}, flags={flags:?}, exists={exists}");
        Ok(exists)
    }

    fn file_size(&self, handle: &mut Self::Handle) -> sqlite_plugin::vfs::VfsResult<usize> {
        log::debug!("file_size: path={}", handle.file_path);
        let database = { self.database.lock().clone() };
        self.runtime.block_on(async {
            let req = GetFileSizeRequest {
                context: self.context.clone(),
                database: database.clone(),
                lease_id: self.lease.lock().clone().unwrap_or("".to_string()),
                file_name: handle.file_path.clone(),
            };

            let start = Instant::now();
            match self.grpc_client.clone().get_file_size(req).await {
                Ok(response) => {
                    log::debug!("get_file_size completed in {:?}", start.elapsed());
                    let size = response.into_inner().size;
                    log::debug!("file_size is: {}", size);
                    Ok(size as usize)
                }
                Err(status) => {
                    log::debug!("get_file_size errored in {:?}", start.elapsed());
                    log::error!("get_file_size failed: {:?}", status);
                    Err(sqlite_plugin::vars::SQLITE_IOERR_FSTAT)
                }
            }
        })
    }

    fn truncate(
        &self,
        handle: &mut Self::Handle,
        size: usize,
    ) -> sqlite_plugin::vfs::VfsResult<()> {
        log::debug!("truncate: path={}, size={}", handle.file_path, size);
        // Truncate over the server
        let database = { self.database.lock().clone() };
        let result = self.runtime.block_on(async {
            let req = TruncateRequest {
                context: self.context.clone(),
                database: database.clone(),
                lease_id: self.lease.lock().clone().unwrap_or("".to_string()),
                file_name: handle.file_path.clone(),
                size: size as i64,
            };

            let start = Instant::now();
            match self.grpc_client.clone().truncate(req).await {
                Ok(response) => {
                    log::debug!("truncate completed in {:?}", start.elapsed());
                    log::debug!("truncate successful: {:?}", response.into_inner());
                    Ok(())
                }
                Err(status) => {
                    log::debug!("truncate errored in {:?}", start.elapsed());
                    log::error!("truncate failed: {:?}", status);
                    Err(sqlite_plugin::vars::SQLITE_IOERR_TRUNCATE)
                }
            }
        });

        // Clear cache entries for pages beyond the truncated size
        let mut cache_checksums = self.cached_page_checksums.lock();
        let keys_to_remove: Vec<String> = cache_checksums
            .iter()
            .filter_map(|(key, cached_page)| {
                if cached_page.database == database
                    && cached_page.file_name == handle.file_path
                    && cached_page.offset >= size as u64
                    && cached_page.offset > 0
                {
                    Some(key.clone())
                } else {
                    None
                }
            })
            .collect();

        for key in keys_to_remove {
            log::debug!("removing cached page beyond truncate size: {}", key);
            cache_checksums.remove(&key);

            // Remove from TinyUfo cache if it exists
            if let Some(ref cache) = self.cached_pages {
                cache.remove(&key);
            }

            // Remove cache file if local caching is enabled
            if let Some(cache_dir) = &self.env_config.local_cache_dir {
                let cache_file = cache_file_path(cache_dir, &key);
                if cache_file.exists() {
                    if let Err(e) = std::fs::remove_file(&cache_file) {
                        log::error!("failed to remove cache file {:?}: {}", cache_file, e);
                        return Err(sqlite_plugin::vars::SQLITE_IOERR_TRUNCATE);
                    }
                }
            }
        }

        match result {
            Ok(_) => {
                // Clear first page cache if we're RW and truncating affects the first page
                let is_rw = self.lease.lock().is_some();
                if is_rw && handle.is_main_db && size < self.capabilities.sector_size as usize {
                    log::debug!("clearing first page cache due to truncate");
                    self.first_page_cache.lock().clear();
                }

                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    fn write(
        &self,
        handle: &mut Self::Handle,
        offset: usize,
        data: &[u8],
    ) -> sqlite_plugin::vfs::VfsResult<usize> {
        log::debug!(
            "write: path={}, offset={}, data={:?}",
            handle.file_path,
            offset,
            data
        );

        let lease_id = self.lease.lock().clone();
        let is_rw = lease_id.is_some();
        let is_first_page = offset + data.len() <= self.capabilities.sector_size as usize;

        if self.write_batch_open.load(Ordering::Acquire) {
            log::debug!("adding to write batch");
            self.write_batch.lock().push(AtomicWrite {
                offset: offset as i64,
                data: data.to_vec(),
                context: self.context.clone(),
                lease_id: lease_id.unwrap_or("".to_string()),
                checksum: xxh3_64(data),
            });

            // Update first page cache as part of the batch
            if is_rw && is_first_page && handle.is_main_db {
                log::debug!("updating first page cache for batched write (full sector)");
                *self.first_page_cache.lock() = data.to_vec();
            }

            return Ok(data.len());
        }

        // Write over the server
        log::debug!("writing directly to server");
        let checksum = xxh3_64(data);
        let database = { self.database.lock().clone() };
        let result = self.runtime.block_on(async {
            let req = WriteRequest {
                context: self.context.clone(),
                lease_id: lease_id.unwrap_or("".to_string()),
                data: data.to_vec(),
                file_name: handle.file_path.clone(),
                offset: offset as i64,
                checksum: checksum,
                database: database.clone(),
            };

            let start = Instant::now();
            match self.grpc_client.clone().write(req).await {
                Ok(response) => {
                    log::debug!("write completed in {:?}", start.elapsed());
                    log::debug!("write successful: {:?}", response.into_inner());
                    Ok(())
                }
                Err(status) => {
                    log::debug!("write errored in {:?}", start.elapsed());
                    log::error!("write failed: {:?}", status);
                    Err(sqlite_plugin::vars::SQLITE_IOERR_WRITE)
                }
            }
        });

        match result {
            Ok(_) => {
                // On successful direct write, update the cache
                if is_rw && is_first_page && handle.is_main_db {
                    log::debug!("updating first page cache for direct write (full sector)");
                    *self.first_page_cache.lock() = data.to_vec();
                }

                // Write to local cache files if not first page and caching is enabled
                if !is_first_page && self.env_config.local_cache_dir.is_some() {
                    let cached_key = cache_key(&database, &handle.file_path, offset as u64);

                    let cached_page = CachedPage {
                        offset: offset as u64,
                        file_name: handle.file_path.clone(),
                        database: database.clone(),
                        checksum,
                    };

                    // Put in TinyUfo cache (weight = 1 for each page)
                    let evicted = self.cached_pages.as_ref().unwrap().put(
                        cached_key.clone(),
                        cached_page.clone(),
                        1,
                    );

                    let mut checksums = self.cached_page_checksums.lock();

                    // Clean up evicted cache files
                    for evicted_item in evicted {
                        log::debug!("evicting cache item: {}", evicted_item.key);
                        let evicted_key = cache_key(
                            &evicted_item.data.database,
                            &evicted_item.data.file_name,
                            evicted_item.data.offset,
                        );
                        checksums.remove(&evicted_key);

                        if let Some(cache_dir) = &self.env_config.local_cache_dir {
                            let evicted_cache_file = cache_file_path(cache_dir, &evicted_key);
                            if evicted_cache_file.exists() {
                                if let Err(e) = std::fs::remove_file(&evicted_cache_file) {
                                    log::warn!(
                                        "failed to remove evicted cache file {:?}: {}",
                                        evicted_cache_file,
                                        e
                                    );
                                }
                            }
                        }
                    }

                    // Update the checksum tracking
                    checksums.insert(cached_key.clone(), cached_page);

                    // Write data to cache file
                    if let Some(cache_dir) = &self.env_config.local_cache_dir {
                        let cache_file = cache_file_path(cache_dir, &cached_key);
                        if let Err(e) = std::fs::write(&cache_file, data) {
                            log::error!("failed to write cache file {:?}: {}", cache_file, e);
                            return Err(sqlite_plugin::vars::SQLITE_IOERR_WRITE);
                        }
                        log::debug!("wrote {} bytes to cache file: {}", data.len(), cached_key);
                    }
                }

                Ok(data.len())
            }
            Err(err) => Err(err),
        }
    }

    fn read(
        &self,
        handle: &mut Self::Handle,
        offset: usize,
        data: &mut [u8],
    ) -> sqlite_plugin::vfs::VfsResult<usize> {
        log::debug!(
            "read: path={}, offset={}, len={}",
            handle.file_path,
            offset,
            data.len()
        );

        let lease_id = self.lease.lock().clone();
        let is_rw = lease_id.is_some();
        let is_first_page = offset + data.len() <= self.capabilities.sector_size as usize;

        let database = { self.database.lock().clone() };

        if is_rw && is_first_page && handle.is_main_db {
            log::debug!("reading from first page cache");
            let mut cache = self.first_page_cache.lock();

            // If cache is empty, populate it from server.
            if cache.is_empty() {
                log::debug!("first page cache miss, reading from server");

                let mut read_timestamp_guard = self.current_read_timestamp.lock();
                let current_timestamp = read_timestamp_guard.unwrap_or(0);

                let result = self.runtime.block_on(async {
                    let req = ReadRequest {
                        context: self.context.clone(),
                        lease_id: lease_id.unwrap_or("".to_string()),
                        file_name: handle.file_path.clone(),
                        offset: 0, // Always read from start for first page
                        length: self.capabilities.sector_size as i64,
                        time_millis: current_timestamp,
                        checksum: 0, // Empty checksum for now
                        database: database.clone(),
                    };

                    let start = Instant::now();
                    match self.grpc_client.clone().read(req).await {
                        Ok(response) => {
                            log::debug!("read (first page cache miss) took {:?}", start.elapsed());
                            let response_data = response.into_inner();
                            log::debug!(
                                "first page read successful: {} bytes, it will \"hit\" after",
                                response_data.data.len()
                            );
                            Ok(response_data)
                        }
                        Err(status) => {
                            log::debug!("read (first page cache miss) took {:?}", start.elapsed());
                            log::error!("first page read failed: {:?}", status);
                            Err(sqlite_plugin::vars::SQLITE_IOERR_READ)
                        }
                    }
                });

                match result {
                    Ok(response_data) => {
                        // Update cache with server data
                        *cache = response_data.data;

                        // Set the read timestamp
                        if current_timestamp == 0 && response_data.time_millis != 0 {
                            *read_timestamp_guard = Some(response_data.time_millis);
                        }
                    }
                    Err(err) => return Err(err),
                }
            }

            // Read from cache, handling short reads correctly.
            let cache_len = cache.len();
            if offset >= cache_len {
                return Ok(0); // Reading past the end of the cached file.
            }

            let readable_bytes = cache_len - offset;
            let bytes_to_copy = std::cmp::min(data.len(), readable_bytes);

            data[..bytes_to_copy].copy_from_slice(&cache[offset..offset + bytes_to_copy]);

            log::debug!("first page cache hit: read {} bytes", bytes_to_copy);
            return Ok(bytes_to_copy);
        }

        // read from local cache files if not first page and blind local reads is enabled
        if !is_first_page
            && self.env_config.local_cache_dir.is_some()
            && self.env_config.local_reads
        {
            let cached_key = cache_key(&database, &handle.file_path, offset as u64);
            let cached_page = self.cached_pages.as_ref().unwrap().get(&cached_key);
            if cached_page.is_some() {
                log::debug!("local read from local cache file: {}", cached_key);
                // Read the cached page into the data buffer from the cache file
                let cache_file = cache_file_path(
                    &self.env_config.local_cache_dir.as_ref().unwrap(),
                    &cached_key,
                );
                let mut file = std::fs::File::open(cache_file).unwrap();
                let bytes_read = file.read(data).unwrap();
                return Ok(bytes_read);
            }
        }

        // Get the current read timestamp or use 0 if we don't have one
        let current_timestamp = self.current_read_timestamp.lock().unwrap_or(0);

        // Read from the server
        let result = self.runtime.block_on(async {
            log::debug!("reading from server with timestamp: {}", current_timestamp);
            // if local cache enabled, and page is cached locally, send checksum of page
            let mut checksum = 0;
            if self.env_config.local_cache_dir.is_some() {
                let cached_key = cache_key(&database, &handle.file_path, offset as u64);
                if let Some(cached_page) = self.cached_pages.as_ref().unwrap().get(&cached_key) {
                    checksum = cached_page.checksum;
                }
            }
            let req = ReadRequest {
                context: self.context.clone(),
                lease_id: lease_id.unwrap_or("".to_string()),
                file_name: handle.file_path.clone(),
                offset: offset as i64,
                length: data.len() as i64,
                time_millis: current_timestamp,
                checksum,
                database: database.clone(),
            };

            let start = Instant::now();
            match self.grpc_client.clone().read(req).await {
                Ok(response) => {
                    log::debug!("read completed in {:?}", start.elapsed());
                    let response_data = response.into_inner();
                    log::debug!("read successful: {} bytes", response_data.data.len());
                    Ok(response_data)
                }
                Err(status) => {
                    log::debug!("read errored in {:?}", start.elapsed());
                    log::error!("read failed: {:?}", status);
                    Err(sqlite_plugin::vars::SQLITE_IOERR_READ)
                }
            }
        });

        match result {
            Ok(response_data) => {
                // Set the read timestamp if we don't have one and the response has a non-zero timestamp
                if current_timestamp == 0 && response_data.time_millis != 0 {
                    *self.current_read_timestamp.lock() = Some(response_data.time_millis);
                }

                if response_data.data.len() == 0 && self.env_config.local_cache_dir.is_some() {
                    // Read the local cache file
                    let cached_key = cache_key(&database, &handle.file_path, offset as u64);
                    log::debug!(
                        "server sent no data, reading from local cache file: {}",
                        cached_key
                    );
                    let cache_file = cache_file_path(
                        &self.env_config.local_cache_dir.as_ref().unwrap(),
                        &cached_key,
                    );
                    let mut file = std::fs::File::open(cache_file).unwrap();
                    let bytes_read = file.read(data).unwrap();
                    return Ok(bytes_read);
                }

                // Insert into the local cache
                if !is_first_page
                    && self.env_config.local_cache_dir.is_some()
                    && !response_data.data.is_empty()
                {
                    let cached_key = cache_key(&database, &handle.file_path, offset as u64);
                    let checksum = xxh3_64(&response_data.data);

                    let cached_page = CachedPage {
                        offset: offset as u64,
                        file_name: handle.file_path.clone(),
                        database: database.clone(),
                        checksum,
                    };

                    // Put in TinyUfo cache (weight = 1 for each page)
                    let evicted = self.cached_pages.as_ref().unwrap().put(
                        cached_key.clone(),
                        cached_page.clone(),
                        1,
                    );

                    let mut checksums = self.cached_page_checksums.lock();

                    // Clean up evicted cache files
                    for evicted_item in evicted {
                        log::debug!("evicting cache item: {}", evicted_item.key);
                        let evicted_key = cache_key(
                            &evicted_item.data.database,
                            &evicted_item.data.file_name,
                            evicted_item.data.offset,
                        );
                        checksums.remove(&evicted_key);

                        if let Some(cache_dir) = &self.env_config.local_cache_dir {
                            let evicted_cache_file = cache_file_path(cache_dir, &evicted_key);
                            if evicted_cache_file.exists() {
                                if let Err(e) = std::fs::remove_file(&evicted_cache_file) {
                                    log::warn!(
                                        "failed to remove evicted cache file {:?}: {}",
                                        evicted_cache_file,
                                        e
                                    );
                                }
                            }
                        }
                    }

                    // Update the checksum tracking
                    checksums.insert(cached_key.clone(), cached_page);

                    // Write data to cache file
                    if let Some(cache_dir) = &self.env_config.local_cache_dir {
                        let cache_file = cache_file_path(cache_dir, &cached_key);
                        if let Err(e) = std::fs::write(&cache_file, &response_data.data) {
                            log::error!("failed to write cache file {:?}: {}", cache_file, e);
                            return Err(sqlite_plugin::vars::SQLITE_IOERR_READ);
                        }
                        log::debug!(
                            "wrote {} bytes to cache file: {}",
                            response_data.data.len(),
                            cached_key
                        );
                    }
                }

                let len = data.len().min(response_data.data.len());
                data[..len].copy_from_slice(&response_data.data[..len]);

                Ok(len)
            }
            Err(err) => Err(err),
        }
    }

    fn close(&self, handle: Self::Handle) -> sqlite_plugin::vfs::VfsResult<()> {
        log::debug!("close: path={}", handle.file_path);

        // Close rpc
        let result = self.runtime.block_on(async {
            // Delete all local cache files
            if self.env_config.local_cache_dir.is_some() {
                log::debug!("deleting local cache files");
                let mut cache_checksums = self.cached_page_checksums.lock();
                let deleted_keys = cache_checksums
                    .iter()
                    .filter(|(k, _)| k.starts_with(&handle.file_path))
                    .map(|(k, _)| k.clone())
                    .collect::<Vec<String>>();
                for key in deleted_keys {
                    log::debug!("deleting cache file: {}", key);
                    cache_checksums.remove(&key);
                    self.cached_pages.as_ref().unwrap().remove(&key);
                    let cached_path =
                        cache_file_path(self.env_config.local_cache_dir.as_ref().unwrap(), &key);
                    if let Err(e) = std::fs::remove_file(&cached_path) {
                        log::error!("failed to remove cache file {:?}: {}", cached_path, e);
                        return Err(sqlite_plugin::vars::SQLITE_IOERR_DELETE);
                    }
                }
            }

            let req = CloseRequest {
                context: self.context.clone(),
                lease_id: self.lease.lock().clone().unwrap_or("".to_string()),
                file_name: handle.file_path.clone(),
            };

            let start = Instant::now();
            match self.grpc_client.clone().close(req).await {
                Ok(response) => {
                    log::debug!("close completed in {:?}", start.elapsed());
                    log::debug!("close successful: {:?}", response.into_inner());
                    Ok(())
                }
                Err(status) => {
                    log::debug!("close errored in {:?}", start.elapsed());
                    log::error!("close failed: {:?}", status);
                    Err(sqlite_plugin::vars::SQLITE_IOERR_CLOSE)
                }
            }
        });

        match result {
            Ok(_) => {
                // Clean up locally only if server close succeeded
                let mut files = self.files.lock();
                files.retain(|f| f.file_path != handle.file_path);

                // If we have a lease and this looks like the main database file (not -wal or -wal2),
                // signal the heartbeat thread to shutdown
                if self.lease.lock().is_some() && handle.is_main_db {
                    log::debug!("signaling heartbeat thread to shutdown for main database close");
                    if let Some(shutdown_tx) = self.heartbeat_shutdown_tx.lock().take() {
                        let _ = shutdown_tx.send(()); // Ignore if receiver is already dropped
                    }
                }

                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    fn device_characteristics(&self) -> i32 {
        log::debug!("device_characteristics");
        let mut characteristics: i32 = sqlite_plugin::vfs::DEFAULT_DEVICE_CHARACTERISTICS;

        if self.capabilities.atomic_batch {
            log::debug!("enabling SQLITE_IOCAP_BATCH_ATOMIC");
            characteristics |= sqlite_plugin::vars::SQLITE_IOCAP_BATCH_ATOMIC;
        }

        // Do we bother with SQLITE_IOCAP_IMMUTABLE if we're opened in read only mode?

        characteristics
    }

    fn pragma(
        &self,
        handle: &mut Self::Handle,
        pragma: sqlite_plugin::vfs::Pragma<'_>,
    ) -> Result<Option<String>, sqlite_plugin::vfs::PragmaErr> {
        log::debug!("pragma: file={:?}, pragma={:?}", handle.file_path, pragma);

        // Call the gRPC server to handle the pragma
        let database = { self.database.lock().clone() };
        let result = self.runtime.block_on(async {
            let req = PragmaRequest {
                context: self.context.clone(),
                lease_id: self.lease.lock().clone().unwrap_or("".to_string()),
                file_name: handle.file_path.clone(),
                pragma_name: pragma.name.to_string(),
                pragma_value: pragma.arg.unwrap_or("").to_string(),
                database: database.clone(),
            };

            let start = Instant::now();
            match self.grpc_client.clone().pragma(req).await {
                Ok(response) => {
                    log::debug!("pragma completed in {:?}", start.elapsed());
                    let pragma_response = response.into_inner();
                    if pragma_response.response.is_empty() {
                        Ok(None)
                    } else {
                        Ok(Some(pragma_response.response))
                    }
                }
                Err(status) => {
                    log::debug!("pragma errored in {:?}", start.elapsed());
                    log::debug!("pragma failed: {:?}", status);
                    if status.code() == tonic::Code::NotFound {
                        Err(sqlite_plugin::vfs::PragmaErr::NotFound)
                    } else {
                        Err(sqlite_plugin::vfs::PragmaErr::Fail(
                            sqlite_plugin::vars::SQLITE_ERROR,
                            Some(format!("pragma failed: {}", status.message())),
                        ))
                    }
                }
            }
        });

        result
    }

    fn file_control(
        &self,
        handle: &mut Self::Handle,
        op: c_int,
        _p_arg: *mut c_void,
    ) -> sqlite_plugin::vfs::VfsResult<()> {
        log::debug!("file_control: file={:?}, op={:?}", handle.file_path, op);
        match op {
            sqlite_plugin::vars::SQLITE_FCNTL_BEGIN_ATOMIC_WRITE => {
                log::debug!("begin_atomic_write control given");
                // Open the write batch
                self.write_batch_open.store(true, Ordering::Release);
                Ok(())
            }
            sqlite_plugin::vars::SQLITE_FCNTL_COMMIT_ATOMIC_WRITE => {
                log::debug!("commit_atomic_write control given");
                // Close the write batch
                self.write_batch_open.store(false, Ordering::Release);

                // Send the batch over the server
                let result = self.runtime.block_on(async {
                    let batch = self.write_batch.lock().clone();
                    if batch.is_empty() {
                        log::debug!("write batch is empty, nothing to commit");
                        return Ok(());
                    }

                    let database = { self.database.lock().clone() };
                    let req = AtomicWriteBatchRequest {
                        context: self.context.clone(),
                        lease_id: self.lease.lock().clone().unwrap_or("".to_string()),
                        file_name: handle.file_path.clone(),
                        writes: batch,
                        database: database.clone(),
                    };

                    let start = Instant::now();
                    match self.grpc_client.clone().atomic_write_batch(req).await {
                        Ok(response) => {
                            log::debug!("atomic write batch completed in {:?}", start.elapsed());
                            log::debug!(
                                "atomic write batch successful: {:?}",
                                response.into_inner()
                            );
                            Ok(())
                        }
                        Err(status) => {
                            log::debug!("atomic write batch errored in {:?}", start.elapsed());
                            log::error!("atomic write batch failed: {:?}", status);
                            Err(sqlite_plugin::vars::SQLITE_IOERR_WRITE)
                        }
                    }
                });

                // Clear the batch after sending
                self.write_batch.lock().clear();

                match result {
                    Ok(_) => Ok(()),
                    Err(err) => Err(err),
                }
            }
            sqlite_plugin::vars::SQLITE_FCNTL_ROLLBACK_ATOMIC_WRITE => {
                log::debug!("rollback_atomic_write control given");
                // Close the write batch
                self.write_batch_open.store(false, Ordering::Release);
                // Clear the batch
                self.write_batch.lock().clear();
                // Clear the first page cache as it may contain rolled-back data
                self.first_page_cache.lock().clear();
                Ok(())
            }
            _ => Err(sqlite_plugin::vars::SQLITE_NOTFOUND),
        }
    }

    fn sector_size(&self) -> i32 {
        log::debug!("sector_size");
        self.capabilities.sector_size
    }

    fn unlock(
        &self,
        handle: &mut Self::Handle,
        _level: sqlite_plugin::flags::LockLevel,
    ) -> sqlite_plugin::vfs::VfsResult<()> {
        log::debug!("unlock: path={}", handle.file_path);

        // Drop the read timestamp
        *self.current_read_timestamp.lock() = None;

        Ok(())
    }
}

const VFS_NAME: &CStr = c"grpsqlite";

/// This function initializes the VFS statically.
/// Called automatically when the library is loaded.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn initialize_grpsqlite() -> i32 {
    let vfs = GrpcVfs::new();

    if let Err(err) = sqlite_plugin::vfs::register_static(
        VFS_NAME.to_owned(),
        vfs,
        sqlite_plugin::vfs::RegisterOpts { make_default: true },
    ) {
        eprintln!("Failed to initialize grpsqlite: {}", err);
        return err;
    }

    // set the log level to trace
    log::set_max_level(log::LevelFilter::Trace);
    sqlite_plugin::vars::SQLITE_OK
}

/// This function is called by `SQLite` when the extension is loaded. It registers
/// the memvfs VFS with `SQLite`.
/// # Safety
/// This function should only be called by sqlite's extension loading mechanism.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite3_grpsqlite_init(
    _db: *mut c_void,
    _pz_err_msg: *mut *mut c_char,
    p_api: *mut sqlite_plugin::sqlite3_api_routines,
) -> std::os::raw::c_int {
    let vfs = GrpcVfs::new();
    if let Err(err) = unsafe {
        sqlite_plugin::vfs::register_dynamic(
            p_api,
            VFS_NAME.to_owned(),
            vfs,
            sqlite_plugin::vfs::RegisterOpts { make_default: true },
        )
    } {
        return err;
    }

    // set the log level to trace
    log::set_max_level(log::LevelFilter::Trace);

    sqlite_plugin::vars::SQLITE_OK_LOAD_PERMANENTLY
}
