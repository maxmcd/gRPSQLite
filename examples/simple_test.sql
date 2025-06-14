.log stderr

.open main.db
PRAGMA journal_mode; -- 'delete' is the default
PRAGMA locking_mode=exclusive; -- need to be in exclusive mode for wal so that xShm methods are not used (and wal still works, otherwise it will default to delete or memory if you try to set wal)
PRAGMA journal_mode=wal;

CREATE TABLE t1(a, b);
INSERT INTO t1 VALUES(1, 2);
INSERT INTO t1 VALUES(3, 4);
SELECT * FROM t1;

VACUUM;
SELECT * FROM t1;
