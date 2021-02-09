CREATE TABLE vector_ptf_part_simple_text(p_mfgr string, p_name string, p_date date, p_retailprice double, rowindex int)
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
        STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/vector_ptf_part_simple_all_datatypes.txt' OVERWRITE INTO TABLE vector_ptf_part_simple_text;

CREATE TABLE vector_ptf_part_simple_orc(p_mfgr string, p_name string, p_date date, p_timestamp timestamp, p_int int, p_retailprice double, p_decimal decimal(10,4), rowindex int) stored as orc;
INSERT INTO TABLE vector_ptf_part_simple_orc 
SELECT p_mfgr, p_name, p_date, CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(p_date)) as TIMESTAMP), CAST(UNIX_TIMESTAMP(p_date) as int), p_retailprice, CAST(p_retailprice as DECIMAL(10,4)), rowindex FROM vector_ptf_part_simple_text;
