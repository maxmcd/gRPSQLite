use std::ffi::{CStr, c_char, c_int, c_void};

use parking_lot::Mutex;

tonic::include_proto!("grpc_vfs");

mod handle;

struct Capabilities {
    context: String,
    atomic_batch: bool,
    point_in_time_reads: bool,
    wal2: bool,
    sector_size: usize,
}

struct GrpcVfs {
    runtime: tokio::runtime::Runtime,
    capabilities: tokio::sync::OnceCell<Capabilities>,
    grpc_client:
        tokio::sync::OnceCell<grpsqlite_client::GrpsqliteClient<tonic::transport::Channel>>,
}

impl GrpcVfs {
    pub fn new() -> Self {
        Self {
            runtime: tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap(), // SQLite is single-threaded, so we can use a single-threaded runtime
            capabilities: tokio::sync::OnceCell::new(),
            grpc_client: tokio::sync::OnceCell::new(),
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
        // TODO: tokio oncecell to get capabilities
        todo!()
    }

    fn delete(&self, path: &str) -> sqlite_plugin::vfs::VfsResult<()> {
        todo!()
    }

    fn access(
        &self,
        path: &str,
        flags: sqlite_plugin::flags::AccessFlags,
    ) -> sqlite_plugin::vfs::VfsResult<bool> {
        todo!()
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
        todo!()
    }

    fn device_characteristics(&self) -> i32 {
        // TODO: tokio oncecell to get capabilities
        todo!()
    }

    fn pragma(
        &self,
        handle: &mut Self::Handle,
        pragma: sqlite_plugin::vfs::Pragma<'_>,
    ) -> Result<Option<String>, sqlite_plugin::vfs::PragmaErr> {
        // log::debug!("pragma: file={:?}, pragma={:?}", handle.name, pragma);
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
        // TODO: tokio oncecell to get capabilities
        todo!()
    }
}

impl GrpcVfs {
    fn get_grpc_client(&self) -> grpsqlite_client::GrpsqliteClient<tonic::transport::Channel> {
        let client = self.runtime.block_on(async {
            self.grpc_client
                .get_or_try_init(|| async {
                    grpsqlite_client::GrpsqliteClient::connect("http://localhost:50051").await // TODO: get from env
                })
                .await
        });
        client.unwrap().clone()
    }

    fn get_capabilities(&self, file_name: &str) -> Capabilities {
        let mut client = self.get_grpc_client();
        let req = GetCapabilitiesRequest {
            client_token: "test".to_string(), // TODO: get from env
            file_name: file_name.to_string(),
            readonly: false,
        };
        let res = self
            .runtime
            .block_on(async { client.get_capabilities(req).await });
        let caps = res.unwrap().into_inner();
        Capabilities {
            context: caps.context,
            atomic_batch: caps.atomic_batch,
            point_in_time_reads: caps.point_in_time_reads,
            wal2: caps.wal2,
            sector_size: caps.sector_size as usize,
        }
    }
}

const VFS_NAME: &CStr = c"grpcvfs";

/// This function initializes the VFS statically.
/// Called automatically when the library is loaded.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn initialize_grpcvfs() -> i32 {
    let vfs = GrpcVfs::new();

    if let Err(err) = sqlite_plugin::vfs::register_static(
        VFS_NAME.to_owned(),
        vfs,
        sqlite_plugin::vfs::RegisterOpts { make_default: true },
    ) {
        eprintln!("Failed to initialize grpcvfs: {}", err);
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
pub unsafe extern "C" fn sqlite3_grpcvfs_init(
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
