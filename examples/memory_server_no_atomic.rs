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
        println!("get_capabilities");
        return Ok(Response::new(grpsqlite::GetCapabilitiesResponse {
            context: Uuid::new_v4().to_string(), // not used, example only
            atomic_batch: false,
            point_in_time_reads: false,
            sector_size: 4096,
        }));
    }

    async fn acquire_lease(
        &self,
        request: Request<grpsqlite::AcquireLeaseRequest>,
    ) -> Result<Response<grpsqlite::AcquireLeaseResponse>, Status> {
        println!("acquire_lease");
        let inner = request.into_inner();
        let mut files = self.files.lock().unwrap();

        // Ensure the file exists in our in-memory storage
        files.entry(inner.database).or_insert_with(Vec::new);

        Ok(Response::new(grpsqlite::AcquireLeaseResponse {
            lease_id: Uuid::new_v4().to_string(), // not checked, example only
        }))
    }

    async fn close(
        &self,
        request: Request<grpsqlite::CloseRequest>,
    ) -> Result<Response<()>, Status> {
        let inner = request.into_inner();
        println!("close context={}", inner.context);
        // no-op
        Ok(Response::new(()))
    }

    async fn heartbeat_lease(
        &self,
        request: Request<grpsqlite::HeartbeatLeaseRequest>,
    ) -> Result<Response<()>, Status> {
        let inner = request.into_inner();
        println!(
            "heartbeat_lease context={} id={}",
            inner.context, inner.lease_id
        );
        // no-op
        Ok(Response::new(()))
    }

    async fn read(
        &self,
        request: Request<grpsqlite::ReadRequest>,
    ) -> Result<Response<grpsqlite::ReadResponse>, Status> {
        let inner = request.into_inner();
        println!(
            "read context={} file={} offset={} length={}",
            inner.context, inner.file_name, inner.offset, inner.length
        );
        let files = self.files.lock().unwrap();
        let file = files
            .get(&inner.file_name)
            .ok_or(Status::not_found("File not found"))?;

        let offset = inner.offset as usize;
        let length = inner.length as usize;

        // Check if offset is beyond file size
        if offset >= file.len() {
            println!("read offset is beyond file size");
            return Ok(Response::new(grpsqlite::ReadResponse {
                data: vec![],
                time_millis: 0, // no time
            }));
        }

        // Read as much data as available, up to the requested length
        let end_offset = std::cmp::min(offset + length, file.len());
        let data = file[offset..end_offset].to_vec();

        println!("read data length: {}", data.len());

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
        println!(
            "write context={} file={} offset={} length={}",
            inner.context,
            inner.file_name,
            inner.offset,
            inner.data.len()
        );
        let mut files = self.files.lock().unwrap();
        let file = files.entry(inner.file_name).or_insert_with(Vec::new);
        let offset = inner.offset as usize;

        if offset + inner.data.len() > file.len() {
            file.resize(offset + inner.data.len(), 0);
        }
        println!("write data at offset {}", offset);
        file[offset..offset + inner.data.len()].copy_from_slice(&inner.data);
        Ok(Response::new(()))
    }

    async fn atomic_write_batch(
        &self,
        _request: Request<grpsqlite::AtomicWriteBatchRequest>,
    ) -> Result<Response<()>, Status> {
        Err(Status::unimplemented("atomic_write_batch is not supported"))
    }

    async fn get_file_size(
        &self,
        request: Request<grpsqlite::GetFileSizeRequest>,
    ) -> Result<Response<grpsqlite::GetFileSizeResponse>, Status> {
        let inner = request.into_inner();
        println!(
            "get_file_size context={} file={}",
            inner.context, inner.file_name
        );
        let file_name = inner.file_name;
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
        println!(
            "pragma context={} pragma={} value={}",
            pragma_request.context, pragma_request.pragma_name, pragma_request.pragma_value
        );
        Ok(Response::new(grpsqlite::PragmaResponse {
            response: "".to_string(), // nothing returned
        }))
    }

    async fn truncate(
        &self,
        request: Request<grpsqlite::TruncateRequest>,
    ) -> Result<Response<()>, Status> {
        let inner = request.into_inner();
        println!(
            "truncate context={} file={} size={}",
            inner.context, inner.file_name, inner.size
        );
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
        println!("delete context={} file={}", inner.context, inner.file_name);
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
