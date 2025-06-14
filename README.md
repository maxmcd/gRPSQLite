# gRPSQLite <!-- omit in toc -->

**Turn any datastore into a SQLite backend via gRPC**

gRPSQLite lets you build **multitenant, distributed SQLite databases** backed by any storage system you want. Give every user, human or AI, their own SQLite database.

- **Real SQLite**: Works with all existing SQLite tools, extensions, and applications
- **Atomic transactions**: Dramatically faster commits via batch writes
- **Read replicas**: Unlimited read-only instances (if your backend supports point-in-time reads)
- **Any language**: Implement your storage backend as a simple gRPC server
- **Any storage**: File systems, cloud storage, databases, version control - anything

## Quick Start

```bash
# Terminal 1: Build and run container
zsh static_dev.sh && docker run -it --rm --name grpsqlite sqlite-grpsqlite-static /bin/bash

# Terminal 2: Start the memory server
docker exec -it grpsqlite cargo run --example memory_server

# Terminal 3: Use SQLite normally
docker exec -it grpsqlite sqlite3_with_grpsqlite
```

In the SQLite terminal, run these commands to see it working:
```sql
.open main.db
CREATE TABLE users(id, name);
INSERT INTO users VALUES(1, 'Alice'), (2, 'Bob');
SELECT * FROM users;
-- You'll see: 1|Alice and 2|Bob

-- Exit SQLite (Ctrl+C), then reconnect:
-- docker exec -it grpsqlite sqlite3_with_grpsqlite

.open main.db
SELECT * FROM users;
-- Data is still there! No local files - it's stored in the server!
```

## How It Works

```
┌─────────────┐    gRPC calls    ┌─────────────────┐    Your choice     ┌──────────────┐
│   SQLite    │ ───────────────► │  gRPSQLite VFS  │ ─────────────────► │   Backend    │
│     VFS     │                  │      Server     │                    │ (S3, FDB,    │
│             │ ◄─────────────── │                 │ ◄───────────────── │  Postgres,   │
└─────────────┘    SQLite API    └─────────────────┘    Database API    │  etc.)       │
                                                                        └──────────────┘
```

gRPSQLite provides a **SQLite VFS (Virtual File System)** that converts SQLite's file operations into gRPC calls. You implement a simple gRPC server that handles reads, writes, and optionally atomic commits.

> [!WARNING]
> This is very early stage software.
>
> Only primitive testing is done, and should not be used for production workloads.
>
> Documentation and examples are not comprehensive either.

## Table of Contents <!-- omit in toc -->

- [Quick Start](#quick-start)
- [How It Works](#how-it-works)
- [Example Use Cases](#example-use-cases)
- [Server Examples](#server-examples)
  - [Example: In-memory server with read-replica support](#example-in-memory-server-with-read-replica-support)
- [SQLite VFS: Dynamic vs Static](#sqlite-vfs-dynamic-vs-static)
  - [Dynamic Loading](#dynamic-loading)
  - [Statically compiling](#statically-compiling)
- [Advanced Features](#advanced-features)
  - [Atomic batch commits](#atomic-batch-commits)
  - [Read-only Replicas](#read-only-replicas)
  - [Local Page Caching (TODO)](#local-page-caching-todo)
- [Tips for Writing a gRPC server](#tips-for-writing-a-grpc-server)
  - [Handling page sizes and row keys](#handling-page-sizes-and-row-keys)
  - [Reads for missing data](#reads-for-missing-data)
  - [Leases](#leases)
- [Performance](#performance)
- [Contributing](#contributing)

## Example Use Cases

- **Multi-tenant SaaS**: Each customer, or user, gets their own SQLite database. Works great for serverless functions.
- **AI/Agent workflows**: Give each AI agent its own persistent database per-chat that users can roll back
- **Immutable Database**: Store your database in an immutable store with full history, branching, and collaboration
- **Read fanout**: Low-write, high read rate scenarios can be easily horizontally scaled

## Server Examples

The fastest way to understand gRPSQLite is to try the examples. See the [`examples/`](examples/) directory for complete server implementations in Rust:

- [Memory server (atomic batched writes)](examples/memory_server.rs) _- Start here!_
- [Memory server (non-batched writes)](examples/memory_server_no_atomic.rs)
- [Memory server (atomic + timestamped writes to support read replicas)](examples/versioned_memory_server.rs)

It's easy to implement in [any language that gRPC supports](https://grpc.io/docs/languages/).

### Example: In-memory server with read-replica support

See [`examples/versioned_memory_server_test.sql`](examples/versioned_memory_server_test.sql) and the server [`examples/versioned_memory_server.rs`](examples/versioned_memory_server.rs)

Modify the quickstart second terminal command to:

```
docker exec -it grpsqlite cargo run --example versioned_memory_server
```

As well as running 2 SQLite clients as indicated in the above linked `.sql` file (one RW, one RO).

## SQLite VFS: Dynamic vs Static

There are 2 ways of using the SQLite VFS:

1. Dynamically load the VFS
2. Statically compile the VFS into a SQLite binary

Statically compiling in is the smoothest experience, as it becomes the default VFS when you open a database file without having to `.load` anything or use a custom setup package.

### Dynamic Loading

```
cargo build --release -p sqlite_vfs --features dynamic
```

This will produce a `target/release/libsqlite_vfs.so` that you can use in SQLite:

```
.load path/to/libsqlite_vfs.so
```

This also sets it as the default VFS.

### Statically compiling

The process:
1. Building a static lib
2. Use a little C stub for initializing the VFS on start (sets the default VFS)
3. Compile SQLite with the C stub and static library

See the [`static_dev.Dockerfile`](static_dev.Dockerfile) example which does all of these steps (you'll probably want to build with `--release` though).

## Advanced Features

The provided VFS converts SQLite VFS calls to gRPC calls so that you can effectively implement a SQLite VFS via gRPC.

It manages a lot of other features for you like atomic batch commits, stable read timestamp tracking, and more.

Your gRPC server can then enable the various features based on the backing datastore you use (see `GetCapabilities` RPC).

For example, if you use a database like FoundationDB that supports both atomic transactions, as well as point-in-time reads, you get the benefits of wildly more efficient SQLite atomic commits (writes the whole transaction at once, rather than writing uncommitted rows to a journal), and support for read-only SQLite instances.

This supports a single read-write instance per database, and if your backing store supports it, unlimited read-only replicas.

### Atomic batch commits

If your server implementation supports atomic batch commits (basically any DB that can do a transaction), this results in _wildly_ faster SQLite transactions.

By default, SQLite (in wal/wal2 mode) will write uncommitted rows to the WAL file, and rely on a commit frame to determine whether the transaction actually committed.

When the server supports atomic batch commits, the SQLite VFS will instead memory-buffer writes, and on commit send a single batched write to the server (`AtomicWriteBatch`). As you can guess, this is _a lot faster_.

If your server supports this, clients should always use `PRAGMA journal_mode=memory` (which should be the default). If it doesn't, then clients should set `PRAGMA locking_mode=exclusive` and `PRAGMA journal_mode=wal` (or `wal2`).

Why `journal_mode=memory` is ideal:

1. We don't need a WAL anymore, so reads only have to hit the main database "file"
2. Because there are no WAL writes, there is no WAL checkpointing, meaning we never have to double-write committed transactions
3. Your gRPC server implementation can expect only a single database file, and reject bad clients using the wrong journal mode (if the file doesn't match the file that created the lease)

### Read-only Replicas

If your database supports point-in-time reads (letting you control the precise point in time of the transaction), then you can support read-only SQLite replicas. Without stable timestamps for reads, a read replica might see an inconsistent database state that will cause SQLite to think the database has been corrupted.

The way this works is when you first submit a read for a transaction, the response will return a stable timestamp that the SQLite VFS will remember for the duration of the transaction (until atomic rollback or xUnlock is called). Subsequent reads from the same transaction will provide this timestamp to the gRPC server, which you must use to ensure that the transaction sees a stable timestamp of the database.

**To start a read-only replica instance, simply open the DB in read-only mode.** The VFS will verify with the capabilities of the gRPC server implementation that it can support read-only replicas (if not, it will error on open with `SQLITE_CANTOPEN`), and will not attempt to acquire a lease on open.

**Some databases that CAN support read-only replicas:**

- FoundationDB
- CockroachDB (via `AS OF SYSTEM TIME`)
- RocksDB (with [User-defined Timestamps](https://github.com/facebook/rocksdb/wiki/User-defined-Timestamp))
- [BadgerDB](https://github.com/hypermodeinc/badger)
- [Git repos...](https://github.com/danthegoodman1/gRPSQLite/issues/8)


**Some databases that CANNOT support read-only replicas (at least independently):**

- Postgres
- MySQL
- Redis
- S3
- Most filesystems
- SQLite

For filesystems like S3, you could use a dedicated metadata DB for managing pages, effectively separating data and metadata. This is a preferred method for larger page sizes.

You can see an example using the SQL [`examples/versioned_memory_server_test.sql`](examples/versioned_memory_server_test.sql) and the server [`examples/versioned_memory_server.rs`](examples/versioned_memory_server.rs) of a RW instance making updates, and a non-blocking RO reading the data. This example uses an copy-on-write `Vec<u8>`, with versions timestamped every write.

### Local Page Caching ([TODO](https://github.com/danthegoodman1/gRPSQLite/issues/3))

You can enable local page caching, enabling reads for pages that haven't changed since the last time the VFS saw to be faster.

How it works:
1. When the VFS writes, it includes the checksum of the data, which you should store
2. When the VFS reads, it includes the checksum that it has locally
3. If the checksum matches what you have at the server you can respond with a blank data array, which tells the VFS to read the local copy
4. The VFS will read the page into memory, verify the checksum, and return it to SQLite

Depending on how you store data, this can dramatically speed up reads.

_Because the first page of the DB is accessed so aggressively, it's always cached in memory on the RW instance._

## Tips for Writing a gRPC server

An over-simplification: You are effectively writing a remote block device.

### Handling page sizes and row keys

Because SQLite uses stable page sizes (and predictable database/wal headers), you can key by the `offset % page_size` (sometimes called `sector_size`) interval. SQLite should always
perform writes on the `page_size` interval already, but it's a good idea to verify on write to prevent malicious clients from corrupting the DB.

This VFS has `SQLITE_IOCAP_SUBPAGE_READ` disabled, which would otherwise allow reads to occur at non-offset intervals.
However, it will still often read at special offsets and lengths for the database header. The following is an example of what operations are valled on the main db file when it is first opened:

```
read            file=main.db offset=0 length=100
get_file_size   file=main.db
read            file=main.db offset=0 length=4096
read            file=main.db offset=24 length=16
get_file_size   file=main.db
```

The simplest way to handle this is when ever you get an offset for a read, use the key `(offset % page_size) * page_size`. You should still return the data at the expected offset and length to the gRPC call.

### Reads for missing data

You need to return the requested length data, if it doesn't exist, return zeros. See examples.

### Leases

When ever a write occurrs, you MUST transactionally verify that the lease is still valid before submitting the write.

If you do not support atomic writes (e.g. backed by S3), then you will have to do either some sort of 2PC, or track metadata (pages) within an external transactional database (e.g. Postgres).


## Performance

While the network adds some overhead, gRPSQLite makes up expected dramatic performance differences through atomic batch commits and read replicas.

Atomic batch commits mean that you only send a single write operation to the gRPC server, which is likely using a backing DB that does all transaction writes at once (FDB, DynamoDB).

Additionally, if using a DB that supports read replicas, you can scale reads out for additional machines aggressively, especially when combined with tiered caches.

## Contributing

Do not submit pull requests out of nowhere, they will be closed.

If you find a bug or want to request a feature, create a discussion. Contributors will turn it into an issue if required.
