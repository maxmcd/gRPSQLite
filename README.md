# gRPSQLite <!-- omit in toc -->

gRPC + SQLite enabling you to easily build multitenant, metered, bottomless, infinite SQLite databases backed by anything you can dream of.  Implement by making a gRPC server and using a pre-made VFS.

Give every user, human or AI, as many SQLite databases as they want.

Uses real SQLite, so it works with everything that SQLite does (packages, extensions, apps, etc.)

_Yes, the name is a pun._

> [!WARNING]
> This is very early stage software.
> Only primitive testing is done, and should not be used for production workloads.

## Table of Contents <!-- omit in toc -->

- [Examples](#examples)
  - [Memory VFS with atomic commit example](#memory-vfs-with-atomic-commit-example)
- [How it works](#how-it-works)
  - [Atomic batch commits](#atomic-batch-commits)
  - [Read-only Replicas](#read-only-replicas)
- [Contributing](#contributing)


## Examples

See the [`examples/`](examples/) directory for example server implementations.

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

Then you can run example sql commands:

```
.log stderr
.open main.db
PRAGMA journal_mode=memory;
CREATE TABLE t1(a, b);
SELECT * FROM t1;
INSERT INTO t1 VALUES(1, 2);
SELECT * FROM t1;

```

_You want to copy the newline to force execution of the last line_

Then you can ctrl-c the third terminal, `docker exec` again, and see that the data is still there despite having no local files on disk:

```
.log stderr
.open main.db
PRAGMA journal_mode=memory;
SELECT * FROM t1;

```


## How it works

The provided VFS converts SQLite VFS calls to gRPC calls so that you can effectively implement a SQLite VFS via gRPC.

The provided VFS manages a lot of other features for you like atomic batch commits, stable read timestamp tracking, and more.

Your gRPC server can then enable the various features based on the backing datastore you use.

For example, if you use a database like FoundationDB that supports both atomic transactions, as well as point-in-time reads, you get the benefits of wildly more efficient SQLite atomic commits (writes the whole transaction at once, rather than writing uncommitted rows to a journal), and support for read-only SQLite instances.

This supports a single read-write instance per database, and if your backing store supports it, unlimited read-only replicas.

### Atomic batch commits

If your server supports atomic batch commits (basically any DB that can do a transaction), this results in _wildly_ faster SQLite transactions.

By default, SQLite (in wal/wal2 mode) will write uncommitted transactions to the WAL file, and rely on a commit frame to determine whether the transaction actually committed.

When the server supports atomic batch commits, the SQLite VFS will instead memory-buffer writes, then on commit send a single batch-write to the server. As you can guess now, this is _a lot faster_.

If your server supports this, you should always use the `memory` VFS for clients. If you don't, then you should set `pragma locking_mode=exclusive` and `pragma journal_mode=wal` (or `wal2`).

The `memory` journal mode is ideal for 3 reasons:

1. We don't need a WAL anymore, because we atomically commit the whole transaction (your backing store likely has its own WAL)
2. Because there are no WAL writes, there is no WAL checkpointing, meaning we never have to double-write committed transactions
3. Your gRPC server implementation can expect only a single database file, and reject bad clients using the wrong journal mode (if the file doesn't match the file that created the lease)

### Read-only Replicas

If your database supports point-in-time reads (across transactions), then you can support read-only SQLite replicas. Without stable timestamps for reads, a read replica might see an inconsistent database state that will cause SQLite to think the database has been corrupted.

The way this works is when you first submit a read for a transaction, the response will return a stable timestamp that the SQLite VFS will remember for the duration of the transaction (until atomic rollback or xUnlock is called). Subsequent reads from the same transaction will provide this timestamp, which your DB should use to ensure that the transaction sees a stable timestamp of the database.

To start a read-only replica instance, simply open the DB is read-only mode. The VFS will verify with the capabilities of the gRPC server implementation that it can support read-only replicas (if not, it will error on open with `SQLITE_CANTOPEN`), and will not attempt to acquire a lease on open.

## Contributing

Do not submit pull requests out of nowhere, they will be closed.

If you find a bug or want to request a feature, create a discussion. Contributors will turn it into an issue if its deemed relevant.
