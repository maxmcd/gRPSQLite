# gRPSQLite <!-- omit in toc -->

gRPC + SQLite enabling you to easily build multitenant, metered, bottomless, infinite SQLite databases backed by anything you can dream of. Implement by making a gRPC server and using a pre-made VFS.

Give every user, human or AI, as many SQLite databases as they want.

Uses real SQLite, so it works with everything that SQLite does (packages, extensions, apps, etc.)

_It's like FUSE for SQLite. Yes, the name is a pun._

> [!WARNING]
> This is very early stage software.
>
> Only primitive testing is done, and should not be used for production workloads.
>
> Documentation and examples are not comprehensive either.

## Table of Contents <!-- omit in toc -->

- [Server Examples](#server-examples)
- [Client Examples (WIP)](#client-examples-wip)
  - [Dynamic Loading](#dynamic-loading)
  - [Statically compiling](#statically-compiling)
  - [Memory VFS with atomic commit example](#memory-vfs-with-atomic-commit-example)
- [Writing a gRPC server (WIP)](#writing-a-grpc-server-wip)
  - [Handling page sizes and row keys](#handling-page-sizes-and-row-keys)
  - [Reads for missing data](#reads-for-missing-data)
  - [Leases](#leases)
- [How it works](#how-it-works)
  - [Atomic batch commits](#atomic-batch-commits)
  - [Read-only Replicas](#read-only-replicas)
  - [Local Page Caching (TODO)](#local-page-caching-todo)
- [Performance](#performance)
- [Contributing](#contributing)


## Server Examples

See the [`examples/`](examples/) directory for example server implementations.

## Client Examples (WIP)

There are 2 ways of using the SQLite VFS:

1. Dynamically load the VFS
2. Statically compile the VFS into a SQLite binary

Statically compiling in is the smoothest experience, as it becomes the default VFS when you open a database file without having to load anything.

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

### Memory VFS with atomic commit example

You need 3 terminals.

In the first run (replace `zsh` with your shell as needed):

```
zsh static_dev.sh && docker run -it --rm --name grpsqlite sqlite-grpsqlite-static /bin/bash
```

This will build the container and run it

In the second, run:

```
docker exec -it grpsqlite cargo run --example memory_server
```

This will start the memory server example that has verbose logging


In the third terminal run:

```
docker exec -it grpsqlite sqlite3_with_grpsqlite
```

Then you can run the following SQL:

```
.log stderr
.open main.db
PRAGMA journal_mode=memory; -- 'delete' is the default
CREATE TABLE t1(a, b);
INSERT INTO t1 VALUES(1, 2);
INSERT INTO t1 VALUES(3, 4);
SELECT * FROM t1;

```

_You want to copy the newline to force execution of the last line_

Then you can ctrl-c the third terminal, `docker exec` again, and see that the data is still there despite having no local files on disk:

```
.log stderr
.open main.db
SELECT * FROM t1;

```

## Writing a gRPC server (WIP)

An over-simplification is you are effectively writing a remote block device.

### Handling page sizes and row keys

Because SQLite uses stable page sizes (and predictable database/wal headers), you can key by the `% page_size` (sometimes called `sector_size`) interval.

This VFS has `SQLITE_IOCAP_SUBPAGE_READ` (which would allow it to read at non-page offsets).

However, it will still often read at special offsets and lengths for the database header. The following is an example of what operations are valled on the main db file when it is first opened:

```
read            file=main.db offset=0 length=100
get_file_size   file=main.db
read            file=main.db offset=0 length=4096
read            file=main.db offset=24 length=16
get_file_size   file=main.db
```

The simplest way to handle this is when ever you get an offset for a read, use the key `offset % page_size`. You should still return the data at the expected offset and length to the gRPC call.

Writes will always be on `page_size` intervals, so you only need to do this trick for reads. Writes can just use the offset as the primary key, and will always contain `page_size` length data.

### Reads for missing data

You need to return the requested length data, if it doesn't exist, return zeros. See examples.

### Leases

When ever a write occurrs, you MUST transactionally verify that the lease is still valid before submitting the write.

If you do not support atomic writes (e.g. backed by S3), then you will have to do either some sort of 2PC, or track metadata (pages) within an external transactional database (e.g. Postgres).


## How it works

The provided VFS converts SQLite VFS calls to gRPC calls so that you can effectively implement a SQLite VFS via gRPC.

The provided VFS manages a lot of other features for you like atomic batch commits, stable read timestamp tracking, and more.

Your gRPC server can then enable the various features based on the backing datastore you use.

For example, if you use a database like FoundationDB that supports both atomic transactions, as well as point-in-time reads, you get the benefits of wildly more efficient SQLite atomic commits (writes the whole transaction at once, rather than writing uncommitted rows to a journal), and support for read-only SQLite instances.

This supports a single read-write instance per database, and if your backing store supports it, unlimited read-only replicas.

### Atomic batch commits

If your server supports atomic batch commits (basically any DB that can do a transaction), this results in _wildly_ faster SQLite transactions.

By default, SQLite (in wal/wal2 mode) will write uncommitted rows to the WAL file, and rely on a commit frame to determine whether the transaction actually committed.

When the server supports atomic batch commits, the SQLite VFS will instead memory-buffer writes, and on commit send a single batched write to the server (`AtomicWriteBatch`). As you can guess now, this is _a lot faster_.

If your server supports this, clients should always use `PRAGME journal_mode=memory` (which should be the default). If you don't, then they should set `pragma locking_mode=exclusive` and `pragma journal_mode=wal` (or `wal2`).

`journal_mode=memory` is ideal:

1. We don't need a WAL anymore, because we atomically commit the whole transaction (your backing store likely has its own WAL)
2. Because there are no WAL writes, there is no WAL checkpointing, meaning we never have to double-write committed transactions
3. Your gRPC server implementation can expect only a single database file, and reject bad clients using the wrong journal mode (if the file doesn't match the file that created the lease)

### Read-only Replicas

If your database supports point-in-time reads (letting you control the precise point in time of the transaction), then you can support read-only SQLite replicas. Without stable timestamps for reads, a read replica might see an inconsistent database state that will cause SQLite to think the database has been corrupted.

The way this works is when you first submit a read for a transaction, the response will return a stable timestamp that the SQLite VFS will remember for the duration of the transaction (until atomic rollback or xUnlock is called). Subsequent reads from the same transaction will provide this timestamp, which your DB should use to ensure that the transaction sees a stable timestamp of the database.

**To start a read-only replica instance, simply open the DB in read-only mode.** The VFS will verify with the capabilities of the gRPC server implementation that it can support read-only replicas (if not, it will error on open with `SQLITE_CANTOPEN`), and will not attempt to acquire a lease on open.

**Some databases that CAN support read-only replicas:**

- FoundationDB
- CockroachDB (via `AS OF SYSTEM TIME`)
- RocksDB
- Badger
- [Git repos...](https://github.com/danthegoodman1/gRPSQLite/issues/8)


**Some databases that CANNOT support read-only replicas (at least in isolation):**

- Postgres
- MySQL
- Redis
- S3
- Most filesystems
- SQLite

You can see an example using the SQL [`examples/versioned_memory_server_test.sql`](examples/versioned_memory_server_test.sql) and the server [`examples/versioned_memory_server.rs`](examples/versioned_memory_server.rs) of a RW instance making updates, and a non-blocking RO reading the data.

### Local Page Caching ([TODO](https://github.com/danthegoodman1/gRPSQLite/issues/3))

For the leased instance, you can configure the VFS to locally-cache pages. This means that reads for pages that haven't changed since the last time the VFS saw it are faster.

This is implemented by:
1. When the VFS writes, it includes the checksum of the data, which you should store
2. When the VFS reads, it includes the checksum that it has locally
3. If the checksum matches what you have at the server you can respond with a blank data array, which tells the VFS to read the data locally
4. The VFS will read the page into memory, verify the checksum, and return it to SQLite

Depending on how you store data, this can dramatically speed up reads.

By default, SQLite doesn't do page checksums 😵‍💫

Luckily, your backing DB probably does.

Local page caching tracks checksums in memory, verifying page content when reading from disk.

[Because the first page of the DB is accessed so aggressively, it's always cached in memory.](https://github.com/danthegoodman1/gRPSQLite/issues/5)

## Performance

Performance can be either worse, or better, than a traditional SQLite-on-EBS setup:

1. Read-write transactions can happen a lot faster because it can do atomic batched writes
2. Reads can be slower if the gRPC + DB latency is higher than an EBS block read
3. Read-only replica performance can be infinitely replicated if the backend supports it!

A good rule of thumb is:
1. Small transactions will be slower
2. Larger read-write transactions can be faster

## Contributing

Do not submit pull requests out of nowhere, they will be closed.

If you find a bug or want to request a feature, create a discussion. Contributors will turn it into an issue if required.
