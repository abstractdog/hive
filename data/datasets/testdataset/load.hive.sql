set hive.stats.dbclass=fs;
--
-- Table src
--
DROP TABLE IF EXISTS testdataset;

CREATE TABLE testdataset (key STRING COMMENT 'default', value STRING COMMENT 'default') STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/testdataset.txt" INTO TABLE src;

ANALYZE TABLE testdataset COMPUTE STATISTICS;

ANALYZE TABLE testdataset COMPUTE STATISTICS FOR COLUMNS key,value;
