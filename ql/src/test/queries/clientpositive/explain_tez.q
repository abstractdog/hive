--! qt:dataset:srcpart
--! qt:dataset:src1
--! qt:dataset:src
set hive.explain.user=false;
set hive.auto.convert.join=true;

explain tez select srcpart.key from srcpart join src on (srcpart.value=src.value) join src1 on (srcpart.key=src1.key);

