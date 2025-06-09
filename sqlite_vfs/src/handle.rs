#[derive(Clone)]
pub struct GrpcVfsHandle {
    pub file_path: String,
    read_only: bool,
    pub is_main_db: bool,
}

impl GrpcVfsHandle {
    pub fn new(file_path: String, read_only: bool, is_main_db: bool) -> Self {
        Self {
            file_path,
            read_only,
            is_main_db,
        }
    }
}

impl sqlite_plugin::vfs::VfsHandle for GrpcVfsHandle {
    fn readonly(&self) -> bool {
        return self.read_only;
    }

    fn in_memory(&self) -> bool {
        // This is really up to the server to implement, and doesn't matter since the VFS handles this anyway
        false
    }
}
