set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
REPL LOAD test_db from '../../data/files/repl_dump' with ('hive.exec.parallel'='false');
use test_db;
show tables;
select * from tbl1 order by fld;
select * from tbl2 order by fld;
select * from tbl3 order by fld;
select * from tbl4 order by fld;
select * from tbl5 order by fld;
select * from tbl6 order by fld;
