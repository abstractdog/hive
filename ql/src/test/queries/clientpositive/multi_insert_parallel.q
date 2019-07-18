--! qt:dataset:src

create table src_multi1 like src;
create table src_multi2 like src;

set hive.exec.parallel=true;
set hive.merge.tezfiles=true;
set tez.grouping.max-size=2000;
set tez.grouping.min-size=1000;

from src
insert overwrite table src_multi1 select * where key < 10
insert overwrite table src_multi2 select * where key > 10 and key < 20;
