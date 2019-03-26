--! qt:dataset:lineitem
set hive.cli.print.header=true;
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.fetch.task.conversion=none;
set hive.vectorized.execution.enabled=true;

CREATE TABLE lineitem_test_txt (L_ORDERKEY      INT,
                                L_PARTKEY       INT,
                                L_SUPPKEY       INT,
                                L_LINENUMBER    INT,
                                L_QUANTITY      INT,
                                L_EXTENDEDPRICE DOUBLE,
                                L_DISCOUNT      DOUBLE,
                                L_TAX           DECIMAL(10,2),
                                L_RETURNFLAG    CHAR(1),
                                L_LINESTATUS    CHAR(1),
                                l_shipdate      DATE,
                                L_COMMITDATE    DATE,
                                L_RECEIPTDATE   DATE,
                                L_SHIPINSTRUCT  VARCHAR(20),
                                L_SHIPMODE      CHAR(10),
                                L_COMMENT       STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|';

LOAD DATA LOCAL INPATH '../../data/files/lineitem.txt' OVERWRITE INTO TABLE lineitem_test_txt;
CREATE TABLE lineitem_test STORED AS ORC AS SELECT * FROM lineitem_test_txt;
INSERT INTO TABLE lineitem_test VALUES (NULL,NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

select L_COMMENT from lineitem_test;

explain vectorization detail
with test as (
  select (CASE
      WHEN L_COMMENT like '%quickly%' THEN 'quickly'
      ELSE L_COMMENT
  END) AS name from lineitem_test
)
select name, count(*) as c from test group by name order by c desc limit 10;

with test as (
  select (CASE
      WHEN L_COMMENT like '%quickly%' THEN 'quickly'
      ELSE L_COMMENT
  END) AS name from lineitem_test
)
select name, count(*) as c from test group by name order by c desc limit 10;

set hive.vectorized.execution.enabled=false;

with test as (
  select (CASE
      WHEN L_COMMENT like '%quickly%' THEN 'quickly'
      ELSE L_COMMENT
  END) AS name from lineitem_test
)
select name, count(*) as c from test group by name order by c desc limit 10;
