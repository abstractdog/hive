--! qt:dataset:src1
--! qt:dataset:dest1
CREATE TABLE dest1(value STRING, key INT) STORED AS TEXTFILE;

EXPLAIN
FROM src1
INSERT OVERWRITE TABLE dest1 SELECT NULL, src1.key where NULL = NULL;

FROM src1
INSERT OVERWRITE TABLE dest1 SELECT NULL, src1.key where NULL = NULL;

SELECT dest1.* FROM dest1;

