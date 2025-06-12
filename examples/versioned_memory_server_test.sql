-- RW instance:

.log stderr
.open main.db
PRAGMA journal_mode=memory;
CREATE TABLE t1(a, b);
SELECT * FROM t1;
INSERT INTO t1 VALUES(1, 2);
INSERT INTO t1 VALUES(3, 4);
INSERT INTO t1 VALUES(5, 6);
SELECT count(*) FROM t1;

-- Read-only replica commands
.log stderr
.open --readonly main.db
SELECT count(*) FROM t1;


-- RW again:

insert into t1 values(7, 8);
select count(*) from t1;

-- RO again:

select count(*) from t1;
