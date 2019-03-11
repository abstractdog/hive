--! qt:dataset:alltypesorc
set hive.fetch.task.conversion=none;
set hive.vectorized.execution.enabled=true;

DESCRIBE FUNCTION trunc;
DESCRIBE FUNCTION EXTENDED trunc;

CREATE TABLE trunc_number_text(c DOUBLE, d INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/trunc_number.txt' INTO TABLE trunc_number_text;

CREATE TABLE trunc_number(c DOUBLE, d INT) STORED AS ORC;
INSERT INTO TABLE trunc_number SELECT * FROM trunc_number_text;

explain vectorization detail select trunc(ctimestamp1, 'MM') from alltypesorc;

select trunc(ctimestamp1, 'MM'), ctimestamp1 from alltypesorc LIMIT 10;
select trunc(ctimestamp1, 'Q'), ctimestamp1 from alltypesorc LIMIT 10;
select trunc(ctimestamp1, 'YEAR'), ctimestamp1 from alltypesorc LIMIT 10;

explain vectorization detail select trunc(CAST(ctimestamp1 AS STRING), 'MM') from alltypesorc;

select trunc(CAST(ctimestamp1 AS STRING), 'MM'), ctimestamp1 from alltypesorc LIMIT 10;
select trunc(CAST(ctimestamp1 AS STRING), 'Q'), ctimestamp1 from alltypesorc LIMIT 10;
select trunc(CAST(ctimestamp1 AS STRING), 'YEAR'), ctimestamp1 from alltypesorc LIMIT 10;

explain vectorization detail select trunc(CAST(ctimestamp1 AS DATE), 'MM') from alltypesorc;

select trunc(CAST(ctimestamp1 AS DATE), 'MM'), ctimestamp1 from alltypesorc LIMIT 10;
select trunc(CAST(ctimestamp1 AS DATE), 'Q'), ctimestamp1 from alltypesorc LIMIT 10;
select trunc(CAST(ctimestamp1 AS DATE), 'YEAR'), ctimestamp1 from alltypesorc LIMIT 10;

select trunc(ctimestamp1, 'MM'), ctimestamp1 from alltypesorc WHERE ctimestamp1 IS NULL LIMIT 10;

explain vectorization detail
select c, trunc(c,0) from trunc_number LIMIT 1;
select c, 0, trunc(c,0) from trunc_number LIMIT 1;
select c, -1, trunc(c,-1) from trunc_number LIMIT 1;
select c, 1, trunc(c,1) from trunc_number LIMIT 1;

explain vectorization detail
select c, trunc(CAST (c AS FLOAT),0) from trunc_number;
select c, 0, trunc(CAST (c AS FLOAT),0) from trunc_number LIMIT 1;
select c, -1, trunc(CAST (c AS FLOAT),-1) from trunc_number LIMIT 1;
select c, 1, trunc(CAST (c AS FLOAT),1) from trunc_number LIMIT 1;


explain vectorization detail
select c, trunc(CAST (c AS DECIMAL), 0) from trunc_number;
select c, 0, trunc(CAST (c AS DECIMAL), 0) from trunc_number LIMIT 1;
select c, -1, trunc(CAST (c AS DECIMAL), -1) from trunc_number LIMIT 1;
select c, 1, trunc(CAST (c AS DECIMAL), 1) from trunc_number LIMIT 1;

-- scale not defined -> 0
explain vectorization detail
select c, trunc(c) from trunc_number;
explain vectorization detail

drop table trunc_number_text;
drop table trunc_number;
