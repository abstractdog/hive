
CREATE EXTERNAL TABLE `udfclaim`(                 
  `systeminsertdatetime` timestamp,               
  `transactiontype` string,                       
  `claimidno` int,                                
  `udfidno` int,                                  
  `udfvaluetext` string,                          
  `udfvaluedecimal` decimal(19,4),                
  `udfvaluedate` timestamp)                       
PARTITIONED BY (                                  
  `fileloaddate` string)                          
ROW FORMAT SERDE                                  
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'     
STORED AS INPUTFORMAT                             
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT                                      
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION '${system:test.tmp.dir}/udfclaim'
TBLPROPERTIES (                                   
  'bucketing_version'='2',                        
  'discover.partitions'='true',                   
  'transient_lastDdlTime'='1654712817');


dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/udfclaim/fileloaddate=2019-02-21;
dfs -put ../../data/files/udfclaim_000000_0 ${system:test.tmp.dir}/udfclaim/fileloaddate=2019-02-21/000000_0;
msck repair table udfclaim;
show partitions udfclaim;
select claimidno, udfvaluedecimal from udfclaim;
