#[derive(Clone)]
pub struct GrpcVfsHandle {
    pub file_path: String,
    read_only: bool,
}

impl GrpcVfsHandle {
    pub fn new(file_path: String, read_only: bool) -> Self {
        Self {
            file_path,
            read_only,
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
