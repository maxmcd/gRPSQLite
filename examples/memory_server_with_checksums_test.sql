.log stderr
.open main.db
PRAGMA journal_mode=memory;
PRAGMA cache_size = 0;
CREATE TABLE t1(a, b);
SELECT * FROM t1;
INSERT INTO t1 VALUES(1, 2);
SELECT * FROM t1;

-- LOCAL_CACHE_DIR=/tmp/grpsqlite_cache -e MAX_CACHE_BYTES=10000000

-- Insert many rows using recursive CTE
WITH RECURSIVE generate_series(x) AS (
  SELECT 1
  UNION ALL
  SELECT x+1 FROM generate_series WHERE x < 10000
)
INSERT INTO t1 SELECT x, x*2 FROM generate_series;

-- All reads should be local
select * from t1;

-- Disconnect and run
.log stderr
.open main.db
PRAGMA journal_mode=memory;
PRAGMA cache_size = 0;
select * from t1;
select * from t1;
-- the second time all reads should be local

-- Disconnect and run
.log stderr
.open main.db
PRAGMA journal_mode=memory;
PRAGMA cache_size = 0;
select * from t1;
select * from t1;
-- the second time all reads should be local

-- reduce cache size:
-- LOCAL_CACHE_DIR=/tmp/grpsqlite_cache -e MAX_CACHE_BYTES=10000

.log stderr
.open main.db
PRAGMA journal_mode=memory;
PRAGMA cache_size = 0;
select * from t1 limit 10;
select * from t1 limit 10;
-- the second time all reads should be local

-- Force reading all pages with modulo condition
select count(*) from t1 where a % 100 = 3;
-- some reads may be local, many will be remote (TinyLFU might evict them before any reads from local pages happen)
