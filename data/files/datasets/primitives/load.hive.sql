CREATE TABLE primitives (
  id INT COMMENT 'default',
  bool_col BOOLEAN COMMENT 'default',
  tinyint_col TINYINT COMMENT 'default',
  smallint_col SMALLINT COMMENT 'default',
  int_col INT COMMENT 'default',
  bigint_col BIGINT COMMENT 'default',
  float_col FLOAT COMMENT 'default',
  double_col DOUBLE COMMENT 'default',
  date_string_col STRING COMMENT 'default',
  string_col STRING COMMENT 'default',
  timestamp_col TIMESTAMP COMMENT 'default')
PARTITIONED BY (year INT COMMENT 'default', month INT COMMENT 'default')
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  ESCAPED BY '\\'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/types/primitives/090101.txt"
OVERWRITE INTO TABLE primitives PARTITION(year=2009, month=1);

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/types/primitives/090201.txt"
OVERWRITE INTO TABLE primitives PARTITION(year=2009, month=2);

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/types/primitives/090301.txt"
OVERWRITE INTO TABLE primitives PARTITION(year=2009, month=3);

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/types/primitives/090401.txt"
OVERWRITE INTO TABLE primitives PARTITION(year=2009, month=4);
