set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.exec.dynamic.partition.mode=nonstrict;

create table compaction_in_qtest(key string, val string) clustered by (val) into 2 buckets stored as ORC TBLPROPERTIES ('transactional'='true');

alter table compaction_in_qtest compact 'major' AND WAIT;
#alter table compaction_in_qtest compact 'major';

show compactions;
select reflect("java.lang.Thread", "sleep", bigint(60000));
show compactions;

drop table compaction_in_qtest;
