pub struct GrpcVfsHandle {}

impl sqlite_plugin::vfs::VfsHandle for GrpcVfsHandle {
    fn readonly(&self) -> bool {
        todo!()
    }

    fn in_memory(&self) -> bool {
        todo!()
    }
}
