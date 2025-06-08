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
}

impl GrpcVfs {
    pub fn new() -> Self {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .enable_io()
            .build()
            .unwrap(); // SQLite is single-threaded, so we can use a single-threaded runtime

        let (client, capabilities, context) = runtime.block_on(async {
            let url = std::env::var("GRPC_VFS_URL")
                .unwrap_or_else(|_| "http://localhost:50051".to_string());

            let connect_timeout_secs = std::env::var("GRPC_VFS_CONNECT_TIMEOUT_SECS")
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(10);

            let endpoint = tonic::transport::Endpoint::from_shared(url)
                .unwrap()
                .connect_timeout(std::time::Duration::from_secs(connect_timeout_secs));

            let mut client = grpsqlite_client::GrpsqliteClient::connect(endpoint)
                .await
                .unwrap();

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
            };

            // TODO: get the lease, launch heartbeat task (or thread?)

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
        }
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

        // acquire a lease if we are RW
        if !mode.is_readonly() {
            let lease_result = self.runtime.block_on(async {
                let req = AcquireLeaseRequest {
                    context: self.context.clone(),
                    database: path.unwrap_or("").to_string(),
                };

                match self.grpc_client.clone().acquire_lease(req).await {
                    Ok(response) => {
                        let lease_response = response.into_inner();
                        log::debug!("lease acquired: {:?}", lease_response.lease_id);
                        *self.lease.lock() = Some(lease_response.lease_id);
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

        let handle = handle::GrpcVfsHandle::new(path.unwrap_or("").to_string(), mode.is_readonly());
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

        // Read from the server
        let result = self.runtime.block_on(async {
            let req = ReadRequest {
                context: self.context.clone(),
                lease_id: self.lease.lock().clone().unwrap_or("".to_string()),
                file_name: handle.file_path.clone(),
                offset: offset as i64,
                length: data.len() as i64,
                time_millis: 0, // TODO: implement proper timestamp for point-in-time reads
            };

            match self.grpc_client.clone().read(req).await {
                Ok(response) => {
                    let response_data = response.into_inner();
                    log::debug!("read successful: {} bytes", response_data.data.len());
                    Ok(response_data.data)
                }
                Err(status) => {
                    log::error!("read failed: {:?}", status);
                    Err(sqlite_plugin::vars::SQLITE_IOERR_READ)
                }
            }
        });

        match result {
            Ok(response_data) => {
                let len = data.len().min(response_data.len());
                data[..len].copy_from_slice(&response_data[..len]);
                Ok(len)
            }
            Err(err) => Err(err),
        }
    }

    fn close(&self, handle: Self::Handle) -> sqlite_plugin::vfs::VfsResult<()> {
        log::debug!("close: path={}", handle.file_path);
        let mut files = self.files.lock();
        files.retain(|f| f.file_path != handle.file_path);
        // TODO: release the lease
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

    fn lock(
        &self,
        handle: &mut Self::Handle,
        level: sqlite_plugin::flags::LockLevel,
    ) -> sqlite_plugin::vfs::VfsResult<()> {
        log::debug!("lock: path={}", handle.file_path);
        // TODO: get read timestamp if read replica
        Ok(())
    }

    fn unlock(
        &self,
        handle: &mut Self::Handle,
        level: sqlite_plugin::flags::LockLevel,
    ) -> sqlite_plugin::vfs::VfsResult<()> {
        log::debug!("unlock: path={}", handle.file_path);
        // TODO: drop read timestamp if read replica
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
