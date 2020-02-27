create external table t1(age int, name string, id int) stored as orc;
create external table t6 like t1;
create external table t5 like t1;

describe formatted t1;
explain insert overwrite table t1 select a.* from t5 a full outer join t6 b on a.id=b.id and a.name=b.name and a.age=b.age;

-- this below is not supposed to put an empty file into the external table (instead only clear its contents)
insert overwrite table t1 select a.* from t5 a full outer join t6 b on a.id=b.id and a.name=b.name and a.age=b.age;
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/t1/;