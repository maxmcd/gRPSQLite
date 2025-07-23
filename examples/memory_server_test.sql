-- uncomment to enable verbose logs
.log stderr

.open main.db
PRAGMA journal_mode; -- 'delete' is the default
PRAGMA journal_mode=memory;

.databases
.vfsinfo

CREATE TABLE t1(a, b);
INSERT INTO t1 VALUES(1, 2);
INSERT INTO t1 VALUES(3, 4);
SELECT * FROM t1;
pragma hello_vfs=1234;

select * from dbstat;

vacuum;
drop table t1;
vacuum;

select * from dbstat;

-- simpler example:

.log stderr
.open main.db
PRAGMA journal_mode=memory;
PRAGMA cache_size = 0;
CREATE TABLE t1(a, b);
SELECT * FROM t1;
INSERT INTO t1 VALUES(1, 2);
SELECT * FROM t1;
