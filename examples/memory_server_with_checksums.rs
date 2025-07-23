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

#[derive(Debug)]
struct StoredPage {
    data: Vec<u8>,
    checksum: u64,
}

#[derive(Default, Debug)]
pub struct MemoryVfs {
    files: Arc<Mutex<HashMap<String, HashMap<u64, StoredPage>>>>,
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
            atomic_batch: true,
            point_in_time_reads: false,
            sector_size: 4096,
            heartbeat_interval_millis: 0, // use default
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
        files.entry(inner.database).or_insert_with(HashMap::new);

        Ok(Response::new(grpsqlite::AcquireLeaseResponse {
            lease_id: Uuid::new_v4().to_string(), // not checked, example only
        }))
    }

    async fn close(
        &self,
        request: Request<grpsqlite::CloseRequest>,
    ) -> Result<Response<()>, Status> {
        let inner = request.into_inner();
        println!("close context={} file={}", inner.context, inner.file_name);
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
            "read context={} file={} offset={} length={} checksum={}",
            inner.context, inner.file_name, inner.offset, inner.length, inner.checksum
        );
        let files = self.files.lock().unwrap();
        let file = files
            .get(&inner.file_name)
            .ok_or(Status::not_found("File not found"))?;

        let offset = inner.offset as u64;
        let length = inner.length as usize;

        // Look up the page at this offset
        let data = match file.get(&offset) {
            Some(page) => {
                // If client provided a checksum and it matches ours, return empty data
                if inner.checksum != 0 && inner.checksum == page.checksum {
                    println!("Checksum match! Returning empty data");
                    vec![]
                } else {
                    // Take only the requested length from the page
                    let end = std::cmp::min(length, page.data.len());
                    page.data[..end].to_vec()
                }
            }
            None => {
                // If page doesn't exist, return zeros
                vec![0u8; length]
            }
        };

        println!("read data length: {}", data.len());

        Ok(Response::new(grpsqlite::ReadResponse {
            data,
            time_millis: 0, // no time
            checksum: 0,    // no checksum
        }))
    }

    async fn write(
        &self,
        request: Request<grpsqlite::WriteRequest>,
    ) -> Result<Response<()>, Status> {
        let inner = request.into_inner();
        println!(
            "write context={} file={} offset={} length={} checksum={}",
            inner.context,
            inner.file_name,
            inner.offset,
            inner.data.len(),
            inner.checksum
        );
        let mut files = self.files.lock().unwrap();
        let file = files.entry(inner.file_name).or_insert_with(HashMap::new);

        let offset = inner.offset as u64;

        // Store the data as a single page at the given offset
        let stored_page = StoredPage {
            data: inner.data.clone(),
            checksum: inner.checksum,
        };

        println!("write data at offset {}: {:?}", offset, inner.data);
        file.insert(offset, stored_page);
        Ok(Response::new(()))
    }

    async fn atomic_write_batch(
        &self,
        request: Request<grpsqlite::AtomicWriteBatchRequest>,
    ) -> Result<Response<()>, Status> {
        let inner = request.into_inner();
        println!(
            "atomic_write_batch context={} file={} writes={}",
            inner.context,
            inner.file_name,
            inner.writes.len()
        );
        let mut files = self.files.lock().unwrap();
        let file = files.entry(inner.file_name).or_insert_with(HashMap::new);

        // Apply all writes atomically
        for write in inner.writes {
            let offset = write.offset as u64;

            log::debug!(
                "atomic_write_batch write context={} offset={} length={} checksum={}",
                inner.context,
                offset,
                write.data.len(),
                write.checksum
            );

            // Store each write as a page
            let stored_page = StoredPage {
                data: write.data,
                checksum: write.checksum,
            };
            file.insert(offset, stored_page);
        }

        Ok(Response::new(()))
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

        let size = match files.get(&file_name) {
            Some(file) => {
                // Calculate the maximum offset + data length to get file size
                file.iter()
                    .map(|(offset, page)| offset + page.data.len() as u64)
                    .max()
                    .unwrap_or(0) as i64
            }
            None => 0,
        };

        Ok(Response::new(grpsqlite::GetFileSizeResponse { size }))
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

        if pragma_request.pragma_name == "is_memory_server" {
            return Ok(Response::new(grpsqlite::PragmaResponse {
                response: "YES!!!!!!".to_string(),
            }));
        }

        Err(Status::not_found("pragma not supported"))
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

        if let Some(file) = files.get_mut(&inner.file_name) {
            let truncate_size = inner.size as u64;
            // Remove pages that start at or beyond the truncation point
            file.retain(|offset, _page| *offset < truncate_size);

            // Also truncate any pages that extend beyond the truncation point
            for (offset, page) in file.iter_mut() {
                if *offset < truncate_size && *offset + page.data.len() as u64 > truncate_size {
                    let new_len = (truncate_size - *offset) as usize;
                    page.data.truncate(new_len);
                }
            }
        }

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
