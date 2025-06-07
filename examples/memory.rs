use std::collections::HashMap;

use tonic::{transport::Server, Request, Response, Status};

#[derive(Default, Debug)]
pub struct MemoryVfs {
  files: HashMap<String, Vec<u8>>,
}

#[tonic::async_trait]
impl grpsqlite::grpsq_lite_server::GrpsqLite for MemoryVfs {
    async fn get_capabilities(
        &self,
        _request: Request<grpsqlite::GetCapabilitiesRequest>,
    ) -> Result<Response<grpsqlite::GetCapabilitiesResponse>, Status> {
        todo!()
    }

    async fn acquire_lease(
        &self,
        _request: Request<grpsqlite::AcquireLeaseRequest>,
    ) -> Result<Response<grpsqlite::AcquireLeaseResponse>, Status> {
        todo!()
    }

    async fn close(
        &self,
        _request: Request<grpsqlite::CloseRequest>,
    ) -> Result<Response<()>, Status> {
        todo!()
    }

    async fn heartbeat_lease(
        &self,
        _request: Request<grpsqlite::HeartbeatLeaseRequest>,
    ) -> Result<Response<()>, Status> {
        todo!()
    }

    async fn read(
        &self,
        _request: Request<grpsqlite::ReadRequest>,
    ) -> Result<Response<grpsqlite::ReadResponse>, Status> {
        todo!()
    }

    async fn write(
        &self,
        _request: Request<grpsqlite::WriteRequest>,
    ) -> Result<Response<()>, Status> {
        todo!()
    }

    async fn atomic_write_batch(
        &self,
        _request: Request<grpsqlite::AtomicWriteBatchRequest>,
    ) -> Result<Response<()>, Status> {
        todo!()
    }

    async fn get_file_size(
        &self,
        _request: Request<grpsqlite::GetFileSizeRequest>,
    ) -> Result<Response<grpsqlite::GetFileSizeResponse>, Status> {
        todo!()
    }

    async fn pragma(
        &self,
        _request: Request<grpsqlite::PragmaRequest>,
    ) -> Result<Response<grpsqlite::PragmaResponse>, Status> {
        todo!()
    }

    async fn truncate(
        &self,
        _request: Request<grpsqlite::TruncateRequest>,
    ) -> Result<Response<()>, Status> {
        todo!()
    }

    async fn delete(
        &self,
        _request: Request<grpsqlite::DeleteRequest>,
    ) -> Result<Response<()>, Status> {
        todo!()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a grpc server and run it
    let addr = "127.0.0.1:50051".parse().unwrap();
    let memory_vfs = MemoryVfs::default();
    let server = grpsqlite::grpsq_lite_server::GrpsqLiteServer::new(memory_vfs);
    println!("Server is running on {}", addr);
    Server::builder()
        .add_service(server)
        .serve(addr)
        .await?;
    Ok(())
}
