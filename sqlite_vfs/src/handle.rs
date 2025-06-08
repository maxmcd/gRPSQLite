pub struct GrpcVfsHandle {
    pub file_path: String,
}

impl sqlite_plugin::vfs::VfsHandle for GrpcVfsHandle {
    fn readonly(&self) -> bool {
        todo!()
    }

    fn in_memory(&self) -> bool {
        todo!()
    }
}
