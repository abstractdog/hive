SET hive.vectorized.execution.enabled=false;
set hive.mapred.mode=nonstrict;
SET hive.fetch.task.conversion=none;
SET hive.cbo.enable=false;
SET hive.map.aggr=false;
-- disabling map side aggregation as that can lead to different intermediate record counts

CREATE TABLE staging_n8(t tinyint,
           si smallint,
           i int,
           b bigint,
           f float,
           d double,
           bo boolean,
           s string,
           ts timestamp,
           `dec` decimal(4,2),
           bin binary)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/over1k' OVERWRITE INTO TABLE staging_n8;
LOAD DATA LOCAL INPATH '../../data/files/over1k' INTO TABLE staging_n8;

CREATE TABLE orc_ppd_staging_n2(t tinyint,
           si smallint,
           i int,
           b bigint,
           f float,
           d double,
           bo boolean,
           s string,
           c char(50),
           v varchar(50),
           da date,
           `dec` decimal(4,2),
           bin binary)
STORED AS ORC tblproperties("orc.row.index.stride" = "1000", "orc.bloom.filter.columns"="*");

insert overwrite table orc_ppd_staging_n2 select t, si, i, b, f, d, bo, s, cast(s as char(50)) as c,
cast(s as varchar(50)) as v, cast(ts as date) as da, `dec`, bin from staging_n8 order by t, si, i, b, f, d, bo, s, c, v, da, `dec`, bin;

-- just to introduce a gap in min/max range for bloom filters. The dataset has contiguous values
-- which makes it hard to test bloom filters
insert into orc_ppd_staging_n2 select -10,-321,-65680,-4294967430,-97.94,-13.07,true,"aaa","aaa","aaa","1990-03-11",-71.54,"aaa" from staging_n8 limit 1;
insert into orc_ppd_staging_n2 select 127,331,65690,4294967440,107.94,23.07,true,"zzz","zzz","zzz","2023-03-11",71.54,"zzz" from staging_n8 limit 1;

CREATE TABLE orc_ppd_n3(t tinyint,
           si smallint,
           i int,
           b bigint,
           f float,
           d double,
           bo boolean,
           s string,
           c char(50),
           v varchar(50),
           da date,
           `dec` decimal(4,2),
           bin binary)
STORED AS ORC tblproperties("orc.row.index.stride" = "1000", "orc.bloom.filter.columns"="*");

insert overwrite table orc_ppd_n3 select t, si, i, b, f, d, bo, s, cast(s as char(50)) as c,
cast(s as varchar(50)) as v, da, `dec`, bin from orc_ppd_staging_n2 order by t, si, i, b, f, d, bo, s, c, v, da, `dec`, bin;

alter table orc_ppd_n3 add columns (boo boolean);

SET hive.optimize.index.filter=true;
-- ppd on newly added column
select count(*) from orc_ppd_n3 where si = 442 or boo is not null or boo = false;
