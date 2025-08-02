/**
 * Example server that holds everything in memory.
 *
 * Despite being all in-memory, clients can restart and still see the database, as long as the server has not been restarted.
 *
 * One obvious omission in this server is that we are not checking leases or context (despite giving out unique IDs).
 */
use std::sync::Arc;
use tokio::sync::Mutex;

use slatedb::object_store::{ObjectStore, memory::InMemory};
use slatedb::{Db, SlateDBError, WriteBatch};

use tonic::{Request, Response, Status, transport::Server};
use uuid::Uuid;

const PAGE_SIZE: usize = 4096;

pub struct MemoryVfs {
    db: Arc<Mutex<Db>>,
}

impl MemoryVfs {
    async fn new() -> Result<Self, SlateDBError> {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let kv_store = Db::open("/tmp/test_kv_store", object_store).await?;
        Ok(Self {
            db: Arc::new(Mutex::new(kv_store)),
        })
    }
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
        let _inner = request.into_inner();

        // No need to pre-create any pages - they will be created on first write
        // Just acquire the lease for the database file

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

        let db = self.db.lock().await;
        let offset = inner.offset as usize;
        let length = inner.length as usize;

        // Calculate the page key using integer division
        let page_offset = (offset / PAGE_SIZE) * PAGE_SIZE;
        let page_key = format!("{}:page:{}", inner.file_name, page_offset);

        let page_data = db
            .get(&page_key)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        if page_data.is_none() {
            println!("read page not found, returning empty data");
            return Ok(Response::new(grpsqlite::ReadResponse {
                data: vec![],
                time_millis: 0, // no time
                checksum: 0,    // no checksum
            }));
        }

        let page = page_data.unwrap();
        let offset_in_page = offset % PAGE_SIZE;

        // Check if offset is beyond page size
        if offset_in_page >= page.len() {
            println!("read offset is beyond page size");
            return Ok(Response::new(grpsqlite::ReadResponse {
                data: vec![],
                time_millis: 0, // no time
                checksum: 0,    // no checksum
            }));
        }

        // Read as much data as available from this page, up to the requested length
        let end_offset_in_page = std::cmp::min(offset_in_page + length, page.len());
        let data = page[offset_in_page..end_offset_in_page].to_vec();

        println!("read data length: {} from page {}", data.len(), page_offset);

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
        let db = self.db.lock().await;
        let offset = inner.offset as usize;

        // Calculate the page key using integer division
        let page_offset = (offset / PAGE_SIZE) * PAGE_SIZE;
        let page_key = format!("{}:page:{}", inner.file_name, page_offset);

        // Get existing page data
        let existing_page = db
            .get(&page_key)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let mut page_data = if let Some(existing) = existing_page {
            existing.to_vec()
        } else {
            Vec::new()
        };

        let offset_in_page = offset % PAGE_SIZE;

        // Resize page if needed
        if offset_in_page + inner.data.len() > page_data.len() {
            page_data.resize(offset_in_page + inner.data.len(), 0);
        }

        println!(
            "write data at page {} offset {}: {:?}",
            page_offset, offset_in_page, inner.data
        );
        page_data[offset_in_page..offset_in_page + inner.data.len()].copy_from_slice(&inner.data);

        db.put(&page_key, page_data)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
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

        let db = self.db.lock().await;

        // Group writes by page to handle them efficiently
        use std::collections::HashMap;
        let mut page_writes: HashMap<usize, Vec<_>> = HashMap::new();

        for write in inner.writes {
            let offset = write.offset as usize;
            let page_offset = (offset / PAGE_SIZE) * PAGE_SIZE;

            page_writes
                .entry(page_offset)
                .or_default()
                .push((offset, write));
        }

        // Prepare WriteBatch for atomic operation
        let mut batch = WriteBatch::new();

        // Apply writes to each affected page
        for (page_offset, writes) in page_writes {
            let page_key = format!("{}:page:{}", inner.file_name, page_offset);

            // Get existing page data
            let existing_page = db
                .get(&page_key)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;

            let mut page_data = if let Some(existing) = existing_page {
                existing.to_vec()
            } else {
                Vec::new()
            };

            // Apply all writes for this page
            for (offset, write) in writes {
                let offset_in_page = offset % PAGE_SIZE;

                log::debug!(
                    "atomic_write_batch write context={} page={} offset_in_page={} length={} checksum={}",
                    inner.context,
                    page_offset,
                    offset_in_page,
                    write.data.len(),
                    write.checksum
                );

                if offset_in_page + write.data.len() > page_data.len() {
                    page_data.resize(offset_in_page + write.data.len(), 0);
                }
                page_data[offset_in_page..offset_in_page + write.data.len()]
                    .copy_from_slice(&write.data);
            }

            // Add the page update to the batch
            batch.put(&page_key, page_data);
        }

        // Execute all page updates atomically
        db.write(batch)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

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

        let db = self.db.lock().await;

        // Find the highest page offset for this file to calculate total size
        // This is a simplified approach - in a real implementation you might want to
        // track file metadata separately for better performance
        let mut max_size = 0i64;

        // Check pages starting from 0 until we find no more
        let mut page_offset = 0;
        loop {
            let page_key = format!("{}:page:{}", inner.file_name, page_offset);
            let page_data = db
                .get(&page_key)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;

            if let Some(page) = page_data {
                max_size = page_offset as i64 + page.len() as i64;
                page_offset += PAGE_SIZE;
            } else {
                break;
            }
        }

        Ok(Response::new(grpsqlite::GetFileSizeResponse {
            size: max_size,
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

        let db = self.db.lock().await;
        let truncate_size = inner.size as usize;

        if truncate_size == 0 {
            // Delete all pages for this file
            let mut page_offset = 0;
            loop {
                let page_key = format!("{}:page:{}", inner.file_name, page_offset);
                let exists = db
                    .get(&page_key)
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?;

                if exists.is_some() {
                    db.delete(&page_key)
                        .await
                        .map_err(|e| Status::internal(e.to_string()))?;
                    page_offset += PAGE_SIZE;
                } else {
                    break;
                }
            }
        } else {
            // Calculate which page contains the truncation point
            let truncate_page_offset = (truncate_size / PAGE_SIZE) * PAGE_SIZE;
            let truncate_offset_in_page = truncate_size % PAGE_SIZE;

            // Truncate the page that contains the truncation point
            let page_key = format!("{}:page:{}", inner.file_name, truncate_page_offset);
            let page_data = db
                .get(&page_key)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;

            if let Some(page) = page_data {
                let mut page_vec = page.clone();
                if truncate_offset_in_page < page_vec.len() {
                    page_vec.truncate(truncate_offset_in_page);
                    db.put(&page_key, page_vec)
                        .await
                        .map_err(|e| Status::internal(e.to_string()))?;
                }
            }

            // Delete all pages beyond the truncation point
            let mut page_offset = truncate_page_offset + PAGE_SIZE;
            loop {
                let page_key = format!("{}:page:{}", inner.file_name, page_offset);
                let exists = db
                    .get(&page_key)
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?;

                if exists.is_some() {
                    db.delete(&page_key)
                        .await
                        .map_err(|e| Status::internal(e.to_string()))?;
                    page_offset += PAGE_SIZE;
                } else {
                    break;
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

        let db = self.db.lock().await;

        // Delete all pages for this file
        let mut page_offset = 0;
        loop {
            let page_key = format!("{}:page:{}", inner.file_name, page_offset);
            let exists = db
                .get(&page_key)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;

            if exists.is_some() {
                db.delete(&page_key)
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?;
                page_offset += PAGE_SIZE;
            } else {
                break;
            }
        }

        Ok(Response::new(()))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a grpc server and run it
    let addr = "127.0.0.1:50051".parse().unwrap();
    let memory_vfs = MemoryVfs::new().await?;
    let server = grpsqlite::grpsqlite_server::GrpsqliteServer::new(memory_vfs);
    println!("Server is running on {addr}");
    Server::builder().add_service(server).serve(addr).await?;
    Ok(())
}
