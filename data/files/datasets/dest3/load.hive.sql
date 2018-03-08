CREATE TABLE dest3 (key STRING COMMENT 'default', value STRING COMMENT 'default')
PARTITIONED BY (ds STRING, hr STRING)
STORED AS
INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
ALTER TABLE dest3 ADD PARTITION (ds='2008-04-08',hr='12');
