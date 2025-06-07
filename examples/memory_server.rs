/**
 * Example server that holds everything in memory.
 *
 * Despite being all in-memory, clients can restart and still see the database, as long as the server has not been restarted.
 *
 * One obvious omission in this server is that we are not checking leases or context (despite giving out unique IDs).
 */
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use tonic::{Request, Response, Status, transport::Server};
use uuid::Uuid;

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
            context: Uuid::new_v4().to_string(), // not used, example only
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
            lease_id: Uuid::new_v4().to_string(), // not checked, example only
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
        request: Request<grpsqlite::ReadRequest>,
    ) -> Result<Response<grpsqlite::ReadResponse>, Status> {
        let inner = request.into_inner();
        let files = self.files.lock().unwrap();
        let file = files
            .get(&inner.file_name)
            .ok_or(Status::not_found("File not found"))?;

        let offset = inner.offset as usize;
        let length = inner.length as usize;

        // Check if offset is beyond file size
        if offset >= file.len() {
            return Err(Status::out_of_range("Read offset beyond file size"));
        }

        // Calculate the actual end position (don't read beyond file size)
        let end = std::cmp::min(offset + length, file.len());
        let data = file[offset..end].to_vec();

        Ok(Response::new(grpsqlite::ReadResponse {
            data,
            time_millis: 0, // no time
        }))
    }

    async fn write(
        &self,
        request: Request<grpsqlite::WriteRequest>,
    ) -> Result<Response<()>, Status> {
        let inner = request.into_inner();
        let mut files = self.files.lock().unwrap();
        let file = files.entry(inner.file_name).or_insert_with(Vec::new);
        let offset = inner.offset as usize;

        if offset + inner.data.len() > file.len() {
            file.resize(offset + inner.data.len(), 0);
        }
        file[offset..offset + inner.data.len()].copy_from_slice(&inner.data);
        Ok(Response::new(()))
    }

    async fn atomic_write_batch(
        &self,
        request: Request<grpsqlite::AtomicWriteBatchRequest>,
    ) -> Result<Response<()>, Status> {
        let inner = request.into_inner();
        let mut files = self.files.lock().unwrap();
        let file = files.entry(inner.file_name).or_insert_with(Vec::new);

        // Apply all writes atomically
        for write in inner.writes {
            let offset = write.offset as usize;

            if offset + write.data.len() > file.len() {
                file.resize(offset + write.data.len(), 0);
            }
            file[offset..offset + write.data.len()].copy_from_slice(&write.data);
        }

        Ok(Response::new(()))
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
        files
            .get_mut(&inner.file_name)
            .map(|f| f.truncate(inner.size as usize));
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
    Server::builder().add_service(server).serve(addr).await?;
    Ok(())
}
