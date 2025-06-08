use std::ffi::{CStr, c_char, c_int, c_void};
use std::sync::Arc;

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
}

impl GrpcVfs {
    pub fn new() -> Self {
        let runtime = tokio::runtime::Builder::new_current_thread()
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

            (client, capabilities, capabilities_response.context)
        });

        Self {
            runtime,
            context,
            capabilities,
            grpc_client: client,
            files: Arc::new(Mutex::new(vec![])),
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
        let mode = opts.mode();
        let handle = handle::GrpcVfsHandle::new(path.unwrap_or("").to_string(), mode.is_readonly());
        self.files.lock().push(handle.clone());
        Ok(handle)
    }

    fn delete(&self, path: &str) -> sqlite_plugin::vfs::VfsResult<()> {
        todo!()
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
        todo!()
    }

    fn truncate(
        &self,
        handle: &mut Self::Handle,
        size: usize,
    ) -> sqlite_plugin::vfs::VfsResult<()> {
        todo!()
    }

    fn write(
        &self,
        handle: &mut Self::Handle,
        offset: usize,
        data: &[u8],
    ) -> sqlite_plugin::vfs::VfsResult<usize> {
        todo!()
    }

    fn read(
        &self,
        handle: &mut Self::Handle,
        offset: usize,
        data: &mut [u8],
    ) -> sqlite_plugin::vfs::VfsResult<usize> {
        todo!()
    }

    fn close(&self, handle: Self::Handle) -> sqlite_plugin::vfs::VfsResult<()> {
        let mut files = self.files.lock();
        files.retain(|f| f.file_path != handle.file_path);
        // TODO: release the lease
        Ok(())
    }

    fn device_characteristics(&self) -> i32 {
        let mut characteristics: i32 = sqlite_plugin::vfs::DEFAULT_DEVICE_CHARACTERISTICS;

        if self.capabilities.atomic_batch {
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
        // log::debug!("file_control: file={:?}, op={:?}", handle.name, op);
        match op {
            sqlite_plugin::vars::SQLITE_FCNTL_COMMIT_ATOMIC_WRITE => {
                log::debug!("commit_atomic_write control given");
                Ok(())
            }
            sqlite_plugin::vars::SQLITE_FCNTL_ROLLBACK_ATOMIC_WRITE => {
                log::debug!("rollback_atomic_write control given");
                Ok(())
            }
            sqlite_plugin::vars::SQLITE_FCNTL_BEGIN_ATOMIC_WRITE => {
                log::debug!("begin_atomic_write control given");
                Ok(())
            }
            _ => Err(sqlite_plugin::vars::SQLITE_NOTFOUND),
        }
    }

    fn sector_size(&self) -> i32 {
        self.capabilities.sector_size
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
