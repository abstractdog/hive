-- A columnar shuffle carries the values of its batches; the row shuffle writes them at their type. A FLOAT computed as a
-- double (CAST(double AS FLOAT) is an identity in a batch) and an INT sum that overflows hold values the row shuffle
-- rounds and wraps: the reducer must group them the same way with either shuffle.
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.optimize.groupby.unsorted.shuffle=true;
set hive.optimize.reducededuplication=false;
set hive.vectorized.execution.enabled=true;
set hive.vectorized.execution.reduce.enabled=true;

create table csn (d double, a int, b int);
insert into csn values
  (0.1, 2147483647, 1), (0.1000000001, -2147483648, 0), (0.1, 2147483647, 1), (0.2, 5, 6), (0.2000000001, 5, 6);

set hive.vectorized.execution.columnar.shuffle.enabled=false;
select cast(d as float), count(*) from csn group by cast(d as float) order by 1, 2;
select a + b, count(*) from csn group by a + b order by 1, 2;

set hive.vectorized.execution.columnar.shuffle.enabled=true;
explain vectorization only operator
select cast(d as float), count(*) from csn group by cast(d as float);
select cast(d as float), count(*) from csn group by cast(d as float) order by 1, 2;
select a + b, count(*) from csn group by a + b order by 1, 2;
