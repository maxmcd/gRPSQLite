# gRPSQLite

gRPC + SQLite

Multitenant, metered, bottomless, infinite SQLite databases backed by anything you can dream of. Implement by making a gRPC server and using a pre-made VFS.

Give every user, whether human or AI, as many SQLite databases as they want.

Uses real SQLite, so it works with everything that SQLite does (packages, extensions, apps, etc.)

_Yes, the name is a pun._

## Developing

### Test with memory VFS example

You need 3 terminals.

In the first run:

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

_Yes copy the newline to force execution of the last line_

Then you can ctrl-c the third terminal, execute it again, and see that the data is still there:

```
.log stderr
.open main.db
PRAGMA journal_mode=memory;
SELECT * FROM t1;

```
