/**
 * Example server that holds everything in memory.
 *
 * Despite being all in-memory, clients can restart and still see the database, as long as the server has not been restarted.
 */

use std::{collections::HashMap, sync::{Arc, Mutex}};

use tonic::{transport::Server, Request, Response, Status};

#[derive(Default, Debug)]
pub struct MemoryVfs {
  files: Arc<Mutex<HashMap<String, Vec<u8>>>>,
}

#[tonic::async_trait]
impl grpsqlite::grpsqlite_server::Grpsqlite for MemoryVfs {
    async fn get_capabilities(
        &self,
        _request: Request<grpsqlite::GetCapabilitiesRequest>,
    ) -> Result<Response<grpsqlite::GetCapabilitiesResponse>, Status> {
        return Ok(Response::new(grpsqlite::GetCapabilitiesResponse {
            context: "memory".to_string(),
            atomic_batch: true,
            point_in_time_reads: false,
            wal2: false,
        }));
    }

    async fn acquire_lease(
        &self,
        _request: Request<grpsqlite::AcquireLeaseRequest>,
    ) -> Result<Response<grpsqlite::AcquireLeaseResponse>, Status> {
        // no-op
        Ok(Response::new(grpsqlite::AcquireLeaseResponse {
            lease_id: "-".to_string(), // we're not checking, so they don't need to be unique
        }))
    }

    async fn close(
        &self,
        _request: Request<grpsqlite::CloseRequest>,
    ) -> Result<Response<()>, Status> {
        // no-op
        Ok(Response::new(()))
    }

    async fn heartbeat_lease(
        &self,
        _request: Request<grpsqlite::HeartbeatLeaseRequest>,
    ) -> Result<Response<()>, Status> {
        // no-op
        Ok(Response::new(()))
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
        request: Request<grpsqlite::GetFileSizeRequest>,
    ) -> Result<Response<grpsqlite::GetFileSizeResponse>, Status> {
        let file_name = request.into_inner().file_name;
        let files = self.files.lock().unwrap();
        Ok(Response::new(grpsqlite::GetFileSizeResponse {
            size: files.get(&file_name).map(|f| f.len() as i64).unwrap_or(0),
        }))
    }

    async fn pragma(
        &self,
        request: Request<grpsqlite::PragmaRequest>,
    ) -> Result<Response<grpsqlite::PragmaResponse>, Status> {
        let pragma_request = request.into_inner();
        let pragma_name = &pragma_request.pragma_name;
        let pragma_value = &pragma_request.pragma_value;
        println!("pragma: {:?} {:?}", pragma_name, pragma_value);
        Ok(Response::new(grpsqlite::PragmaResponse {
            response: "".to_string(), // nothing returned
        }))
    }

    async fn truncate(
        &self,
        request: Request<grpsqlite::TruncateRequest>,
    ) -> Result<Response<()>, Status> {
        let inner = request.into_inner();
        let mut files = self.files.lock().unwrap();
        files.get_mut(&inner.file_name).map(|f| f.truncate(inner.size as usize));
        Ok(Response::new(()))
    }

    async fn delete(
        &self,
        request: Request<grpsqlite::DeleteRequest>,
    ) -> Result<Response<()>, Status> {
        let inner = request.into_inner();
        let mut files = self.files.lock().unwrap();
        files.remove(&inner.file_name);
        Ok(Response::new(()))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a grpc server and run it
    let addr = "127.0.0.1:50051".parse().unwrap();
    let memory_vfs = MemoryVfs::default();
    let server = grpsqlite::grpsqlite_server::GrpsqliteServer::new(memory_vfs);
    println!("Server is running on {}", addr);
    Server::builder()
        .add_service(server)
        .serve(addr)
        .await?;
    Ok(())
}
