/**
 * Example server with point-in-time read support using versioned storage.
 *
 * This server maintains multiple versions of each file, allowing clients to read
 * from specific points in time. Uses im::Vector for efficient copy-on-write semantics.
 */
use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use tonic::{Request, Response, Status, transport::Server};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct VersionedFile {
    // Map timestamp -> file version
    versions: BTreeMap<u64, im::Vector<u8>>,
    // Current working version (latest)
    current_timestamp: u64,
    current_version: im::Vector<u8>,
}

impl Default for VersionedFile {
    fn default() -> Self {
        let timestamp = generate_timestamp();
        Self {
            versions: BTreeMap::new(),
            current_timestamp: timestamp,
            current_version: im::Vector::new(),
        }
    }
}

impl VersionedFile {
    fn read_at_time(&self, timestamp: Option<u64>, offset: usize, length: usize) -> (u64, Vec<u8>) {
        let (actual_timestamp, version) = match timestamp {
            // No timestamp provided - return current version
            None => (self.current_timestamp, &self.current_version),

            // Specific timestamp requested
            Some(ts) => {
                // Check if it matches current timestamp first
                if ts == self.current_timestamp {
                    (self.current_timestamp, &self.current_version)
                }
                // Exact match in historical versions
                else if let Some(version) = self.versions.get(&ts) {
                    (ts, version)
                } else {
                    // Find closest earlier version
                    if let Some((&closest_ts, version)) = self.versions.range(..=ts).next_back() {
                        (closest_ts, version)
                    } else {
                        // No version at or before timestamp - return current if it's the only one
                        (self.current_timestamp, &self.current_version)
                    }
                }
            }
        };

        // Extract the requested data range
        if offset >= version.len() {
            return (actual_timestamp, vec![]);
        }

        let end_offset = std::cmp::min(offset + length, version.len());
        let data = version
            .iter()
            .skip(offset)
            .take(end_offset - offset)
            .cloned()
            .collect();

        (actual_timestamp, data)
    }

    fn write(&mut self, offset: usize, data: &[u8]) -> u64 {
        // Archive current version if it has any content or if we have other versions
        if !self.current_version.is_empty() || !self.versions.is_empty() {
            self.versions
                .insert(self.current_timestamp, self.current_version.clone());
        }

        // Create new version via copy-on-write
        let new_timestamp = generate_timestamp();
        let mut new_version = self.current_version.clone();

        // Ensure capacity
        while new_version.len() < offset + data.len() {
            new_version.push_back(0);
        }

        // Apply write
        for (i, &byte) in data.iter().enumerate() {
            new_version.set(offset + i, byte);
        }

        self.current_version = new_version;
        self.current_timestamp = new_timestamp;

        new_timestamp
    }

    fn atomic_write_batch(&mut self, writes: &[grpsqlite::AtomicWrite]) -> u64 {
        // Archive current version
        if !self.current_version.is_empty() || !self.versions.is_empty() {
            self.versions
                .insert(self.current_timestamp, self.current_version.clone());
        }

        // Create new version
        let new_timestamp = generate_timestamp();
        let mut new_version = self.current_version.clone();

        // Apply all writes to determine final size needed
        let max_end = writes
            .iter()
            .map(|w| w.offset as usize + w.data.len())
            .max()
            .unwrap_or(0);

        while new_version.len() < max_end {
            new_version.push_back(0);
        }

        // Apply all writes
        for write in writes {
            let offset = write.offset as usize;
            for (i, &byte) in write.data.iter().enumerate() {
                new_version.set(offset + i, byte);
            }
        }

        self.current_version = new_version;
        self.current_timestamp = new_timestamp;

        new_timestamp
    }

    fn truncate(&mut self, size: usize) -> u64 {
        // Archive current version
        if !self.current_version.is_empty() || !self.versions.is_empty() {
            self.versions
                .insert(self.current_timestamp, self.current_version.clone());
        }

        let new_timestamp = generate_timestamp();
        let mut new_version = self.current_version.clone();
        new_version.truncate(size);

        self.current_version = new_version;
        self.current_timestamp = new_timestamp;

        new_timestamp
    }

    fn get_size(&self) -> i64 {
        self.current_version.len() as i64
    }
}

fn generate_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[derive(Default, Debug)]
pub struct VersionedMemoryVfs {
    files: Arc<Mutex<HashMap<String, VersionedFile>>>,
}

#[tonic::async_trait]
impl grpsqlite::grpsqlite_server::Grpsqlite for VersionedMemoryVfs {
    async fn get_capabilities(
        &self,
        _request: Request<grpsqlite::GetCapabilitiesRequest>,
    ) -> Result<Response<grpsqlite::GetCapabilitiesResponse>, Status> {
        println!("get_capabilities");
        return Ok(Response::new(grpsqlite::GetCapabilitiesResponse {
            context: Uuid::new_v4().to_string(), // not used, example only
            atomic_batch: true,
            point_in_time_reads: true, // Now supporting point-in-time reads!
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

        // Ensure the file exists in our versioned storage
        files
            .entry(inner.database)
            .or_insert_with(VersionedFile::default);

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
            "read context={} file={} offset={} length={} time_millis={} checksum={}",
            inner.context,
            inner.file_name,
            inner.offset,
            inner.length,
            inner.time_millis,
            inner.checksum
        );

        let files = self.files.lock().unwrap();
        println!(
            "DEBUG: Current file timestamp: {}, available versions: {:?}",
            files
                .get(&inner.file_name)
                .map(|f| f.current_timestamp)
                .unwrap_or(0),
            files
                .get(&inner.file_name)
                .map(|f| f.versions.keys().collect::<Vec<_>>())
                .unwrap_or_default()
        );
        let file = files
            .get(&inner.file_name)
            .ok_or(Status::not_found("File not found"))?;

        let offset = inner.offset as usize;
        let length = inner.length as usize;
        let requested_time = if inner.time_millis == 0 {
            None
        } else {
            Some(inner.time_millis as u64)
        };

        let (actual_timestamp, data) = file.read_at_time(requested_time, offset, length);

        println!(
            "read data length: {} at timestamp: {}",
            data.len(),
            actual_timestamp
        );

        Ok(Response::new(grpsqlite::ReadResponse {
            data,
            time_millis: actual_timestamp as i64,
            checksum: 0, // no checksum for now
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
        let file = files
            .entry(inner.file_name)
            .or_insert_with(VersionedFile::default);

        let offset = inner.offset as usize;
        let timestamp = file.write(offset, &inner.data);

        println!("write completed at timestamp: {}", timestamp);
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
        let file = files
            .entry(inner.file_name)
            .or_insert_with(VersionedFile::default);

        let timestamp = file.atomic_write_batch(&inner.writes);

        println!("atomic_write_batch completed at timestamp: {}", timestamp);
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

        let files = self.files.lock().unwrap();
        let size = files
            .get(&inner.file_name)
            .map(|f| f.get_size())
            .unwrap_or(0);

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
                response: "YES!!! (with versioning)".to_string(),
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
            let timestamp = file.truncate(inner.size as usize);
            println!("truncate completed at timestamp: {}", timestamp);
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
    let addr = "127.0.0.1:50051".parse().unwrap(); // Different port to avoid conflicts
    let versioned_vfs = VersionedMemoryVfs::default();
    let server = grpsqlite::grpsqlite_server::GrpsqliteServer::new(versioned_vfs);
    println!("Versioned server is running on {}", addr);
    Server::builder().add_service(server).serve(addr).await?;
    Ok(())
}
