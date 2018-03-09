--! qt:dataset:src1
--! qt:dataset:dest1
set hive.mapred.mode=nonstrict;
CREATE TABLE dest1(c1 STRING, c2 INT, c3 DOUBLE) STORED AS TEXTFILE;

EXPLAIN
FROM src1 
INSERT OVERWRITE TABLE dest1 SELECT 4 + NULL, src1.key - NULL, NULL + NULL;

FROM src1 
INSERT OVERWRITE TABLE dest1 SELECT 4 + NULL, src1.key - NULL, NULL + NULL;

SELECT dest1.* FROM dest1;
