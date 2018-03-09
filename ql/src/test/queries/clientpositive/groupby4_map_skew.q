--! qt:dataset:src
--! qt:dataset:dest1
set hive.mapred.mode=nonstrict;
set hive.map.aggr=true;
set hive.groupby.skewindata=true;
set mapred.reduce.tasks=31;

CREATE TABLE dest1(key INT) STORED AS TEXTFILE;

EXPLAIN
FROM src INSERT OVERWRITE TABLE dest1 SELECT count(1);

FROM src INSERT OVERWRITE TABLE dest1 SELECT count(1);

SELECT dest1.* FROM dest1;
