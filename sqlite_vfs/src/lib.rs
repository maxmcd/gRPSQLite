use std::ffi::{CStr, c_char, c_int, c_void};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use parking_lot::Mutex;

tonic::include_proto!("grpc_vfs");

mod handle;

struct Capabilities {
    atomic_batch: bool,
    point_in_time_reads: bool,
    sector_size: i32,
    heartbeat_interval_millis: i64,
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
}

impl GrpcVfs {
    async fn create_grpc_client() -> Result<
        grpsqlite_client::GrpsqliteClient<tonic::transport::Channel>,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        let url =
            std::env::var("GRPC_VFS_URL").unwrap_or_else(|_| "http://localhost:50051".to_string());

        let connect_timeout_secs = std::env::var("GRPC_VFS_CONNECT_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(10);

        let endpoint = tonic::transport::Endpoint::from_shared(url)?
            .connect_timeout(std::time::Duration::from_secs(connect_timeout_secs))
            .keep_alive_timeout(std::time::Duration::from_secs(connect_timeout_secs))
            .keep_alive_while_idle(true)
            .http2_keep_alive_interval(std::time::Duration::from_secs(connect_timeout_secs));

        let client = grpsqlite_client::GrpsqliteClient::connect(endpoint).await?;
        Ok(client)
    }

    pub fn new() -> Self {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .enable_io()
            .build()
            .unwrap(); // SQLite is single-threaded, so we can use a single-threaded runtime

        let (client, capabilities, context) = runtime.block_on(async {
            let mut client = Self::create_grpc_client().await.unwrap();

            let req = GetCapabilitiesRequest {
                client_token: std::env::var("GRPC_VFS_CLIENT_TOKEN")
                    .unwrap_or_else(|_| "".to_string()),
                file_name: "".to_string(), // not relevant
                readonly: false,
            };

            let res = client.get_capabilities(req).await;
            let capabilities_response = res.unwrap().into_inner();
            let capabilities = Capabilities {
                atomic_batch: capabilities_response.atomic_batch,
                point_in_time_reads: capabilities_response.point_in_time_reads,
                sector_size: capabilities_response.sector_size,
                heartbeat_interval_millis: capabilities_response.heartbeat_interval_millis,
            };

            (client, capabilities, capabilities_response.context)
        });

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

        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .enable_io()
                .build()
                .unwrap();

            rt.block_on(async {
                // Create a new gRPC client within this runtime using the factory (otherwise it will hang because of the shared channel)
                let grpc_client = match GrpcVfs::create_grpc_client().await {
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
                                let heartbeat_result = tokio::time::timeout(
                                    std::time::Duration::from_millis(heartbeat_interval),
                                    grpc_client.clone().heartbeat_lease(req)
                                ).await;

                                match heartbeat_result {
                                    Ok(Ok(_)) => {
                                        log::debug!("heartbeat successful");
                                    }
                                    Ok(Err(status)) => {
                                        if status.code() == tonic::Code::NotFound {
                                            log::error!("heartbeat failed - lease not found: {:?}", status);
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
        log::set_boxed_logger(Box::new(log)).expect("failed to setup global logger");
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

        // acquire a lease if we are RW
        if !mode.is_readonly() && opts.kind() == sqlite_plugin::flags::OpenKind::MainDb {
            let lease_result = self.runtime.block_on(async {
                let req = AcquireLeaseRequest {
                    context: self.context.clone(),
                    database: path.unwrap_or("").to_string(),
                };

                log::debug!("acquiring lease for main db {}", path.unwrap_or(""));
                match self.grpc_client.clone().acquire_lease(req).await {
                    Ok(response) => {
                        let lease_response = response.into_inner();
                        log::debug!("lease acquired: {:?}", lease_response.lease_id);
                        *self.lease.lock() = Some(lease_response.lease_id);

                        // Start the heartbeat thread to keep the lease alive
                        self.start_heartbeat_thread();

                        Ok(())
                    }
                    Err(status) => {
                        log::error!("failed to acquire lease: {:?}", status);
                        Err(sqlite_plugin::vars::SQLITE_CANTOPEN)
                    }
                }
            });

            if let Err(err) = lease_result {
                return Err(err);
            }
        }

        let handle = handle::GrpcVfsHandle::new(
            path.unwrap_or("").to_string(),
            mode.is_readonly(),
            opts.kind() == sqlite_plugin::flags::OpenKind::MainDb,
        );
        self.files.lock().push(handle.clone());
        Ok(handle)
    }

    fn delete(&self, path: &str) -> sqlite_plugin::vfs::VfsResult<()> {
        log::debug!("delete: path={}", path);

        // Delete over the server
        self.runtime.block_on(async {
            let req = DeleteRequest {
                context: self.context.clone(),
                lease_id: self.lease.lock().clone().unwrap_or("".to_string()),
                file_name: path.to_string(),
            };

            match self.grpc_client.clone().delete(req).await {
                Ok(response) => {
                    log::debug!("delete successful: {:?}", response.into_inner());
                }
                Err(status) => {
                    log::error!("delete failed: {:?}", status);
                }
            }
        });

        // Delete locally
        let mut files = self.files.lock();
        files.retain(|f| f.file_path != path);
        Ok(())
    }

    fn access(
        &self,
        path: &str,
        flags: sqlite_plugin::flags::AccessFlags,
    ) -> sqlite_plugin::vfs::VfsResult<bool> {
        log::debug!("access: path={}, flags={:?}", path, flags);
        Ok(self.files.lock().iter().any(|f| f.file_path == path))
    }

    fn file_size(&self, handle: &mut Self::Handle) -> sqlite_plugin::vfs::VfsResult<usize> {
        log::debug!("file_size: path={}", handle.file_path);
        self.runtime.block_on(async {
            let req = GetFileSizeRequest {
                context: self.context.clone(),
                lease_id: self.lease.lock().clone().unwrap_or("".to_string()),
                file_name: handle.file_path.clone(),
            };

            match self.grpc_client.clone().get_file_size(req).await {
                Ok(response) => {
                    let size = response.into_inner().size;
                    log::debug!("file_size is: {}", size);
                    Ok(size as usize)
                }
                Err(status) => {
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
        let result = self.runtime.block_on(async {
            let req = TruncateRequest {
                context: self.context.clone(),
                lease_id: self.lease.lock().clone().unwrap_or("".to_string()),
                file_name: handle.file_path.clone(),
                size: size as i64,
            };

            match self.grpc_client.clone().truncate(req).await {
                Ok(response) => {
                    log::debug!("truncate successful: {:?}", response.into_inner());
                    Ok(())
                }
                Err(status) => {
                    log::error!("truncate failed: {:?}", status);
                    Err(sqlite_plugin::vars::SQLITE_IOERR_TRUNCATE)
                }
            }
        });

        match result {
            Ok(_) => Ok(()),
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

        if self.write_batch_open.load(Ordering::Acquire) {
            log::debug!("adding to write batch");
            self.write_batch.lock().push(AtomicWrite {
                offset: offset as i64,
                data: data.to_vec(),
                context: self.context.clone(),
                lease_id: self.lease.lock().clone().unwrap_or("".to_string()),
            });
            return Ok(data.len());
        }

        // Write over the server
        log::debug!("writing directly to server");
        let result = self.runtime.block_on(async {
            let req = WriteRequest {
                context: self.context.clone(),
                lease_id: self.lease.lock().clone().unwrap_or("".to_string()),
                data: data.to_vec(),
                file_name: handle.file_path.clone(),
                offset: offset as i64,
            };

            match self.grpc_client.clone().write(req).await {
                Ok(response) => {
                    log::debug!("write successful: {:?}", response.into_inner());
                    Ok(())
                }
                Err(status) => {
                    log::error!("write failed: {:?}", status);
                    Err(sqlite_plugin::vars::SQLITE_IOERR_WRITE)
                }
            }
        });

        match result {
            Ok(_) => Ok(data.len()),
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

        // Get the current read timestamp or use 0 if we don't have one
        let current_timestamp = self.current_read_timestamp.lock().unwrap_or(0);

        // Read from the server
        let result = self.runtime.block_on(async {
            let req = ReadRequest {
                context: self.context.clone(),
                lease_id: self.lease.lock().clone().unwrap_or("".to_string()),
                file_name: handle.file_path.clone(),
                offset: offset as i64,
                length: data.len() as i64,
                time_millis: current_timestamp,
            };

            match self.grpc_client.clone().read(req).await {
                Ok(response) => {
                    let response_data = response.into_inner();
                    log::debug!("read successful: {} bytes", response_data.data.len());
                    Ok(response_data)
                }
                Err(status) => {
                    log::error!("read failed: {:?}", status);
                    Err(sqlite_plugin::vars::SQLITE_IOERR_READ)
                }
            }
        });

        match result {
            Ok(response_data) => {
                let len = data.len().min(response_data.data.len());
                data[..len].copy_from_slice(&response_data.data[..len]);

                // Set the read timestamp if we don't have one and the response has a non-zero timestamp
                if current_timestamp == 0 && response_data.time_millis != 0 {
                    *self.current_read_timestamp.lock() = Some(response_data.time_millis);
                }

                Ok(len)
            }
            Err(err) => Err(err),
        }
    }

    fn close(&self, handle: Self::Handle) -> sqlite_plugin::vfs::VfsResult<()> {
        log::debug!("close: path={}", handle.file_path);

        // Close rpc
        self.runtime.block_on(async {
            let req = CloseRequest {
                context: self.context.clone(),
                lease_id: self.lease.lock().clone().unwrap_or("".to_string()),
                file_name: handle.file_path.clone(),
            };

            match self.grpc_client.clone().close(req).await {
                Ok(response) => {
                    log::debug!("close successful: {:?}", response.into_inner());
                }
                Err(status) => {
                    log::error!("close failed: {:?}", status);
                }
            }
        });

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
        Err(sqlite_plugin::vfs::PragmaErr::NotFound)
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

                    let req = AtomicWriteBatchRequest {
                        context: self.context.clone(),
                        lease_id: self.lease.lock().clone().unwrap_or("".to_string()),
                        file_name: handle.file_path.clone(),
                        writes: batch,
                    };

                    match self.grpc_client.clone().atomic_write_batch(req).await {
                        Ok(response) => {
                            log::debug!(
                                "atomic write batch successful: {:?}",
                                response.into_inner()
                            );
                            Ok(())
                        }
                        Err(status) => {
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
