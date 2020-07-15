set hive.support.concurrency=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

CREATE TABLE default.bdaa28846 (
   cda_id             int,
   cda_run_id         varchar(255),
   cda_load_ts        timestamp,
   global_party_id    string)
PARTITIONED BY (
   cda_date           int,
   cda_job_name       varchar(12))
CLUSTERED BY (cda_id) 
INTO 2 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='true');

DESCRIBE FORMATTED default.bdaa28846;

INSERT OVERWRITE TABLE default.bdaa28846 PARTITION (cda_date = 20200601 , cda_job_name = 'core_base')
SELECT 1 as cda_id,'cda_run_id' as cda_run_id, NULL as cda_load_ts, 'global_party_id' global_party_id
UNION ALL
SELECT 2 as cda_id,'cda_run_id' as cda_run_id, NULL as cda_load_ts, 'global_party_id' global_party_id;

ALTER TABLE default.bdaa28846 ADD COLUMNS (group_id string) CASCADE ;

INSERT OVERWRITE TABLE default.bdaa28846 PARTITION (cda_date = 20200601 , cda_job_name = 'core_base')
SELECT 1 as cda_id,'cda_run_id' as cda_run_id, NULL as cda_load_ts, 'global_party_id' global_party_id, 'group_id' as group_id;

SET hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.PostExecOrcFileDump;
select * from default.bdaa28846 limit 1;
SET hive.exec.post.hooks=;

SELECT count(distinct global_party_id) FROM bdaa28846 where cda_date = 20200601 and cda_job_name = 'core_base';

