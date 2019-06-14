--! qt:dataset:src
set hive.explain.user=false;
set hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;
set hive.test.vectorizer.suppress.fatal.exceptions=false;

drop table if exists vector_and_or;
create table vector_and_or (dt1 date, dt2 date) stored as orc;

insert into table vector_and_or
  select null, null from src limit 1;
insert into table vector_and_or
  select date '1999-12-31', date '2000-01-01' from src limit 1;
insert into table vector_and_or
  select date '2001-01-01', date '2001-06-01' from src limit 1;

-- select null explicitly

explain vectorization detail select null or dt1 is not null from vector_and_or;
-- check vectorized results
select null or dt1 is not null from vector_and_or;
set hive.vectorized.execution.enabled=false;
-- check non-vectorized results
select null or dt1 is not null from vector_and_or;
set hive.vectorized.execution.enabled=true;


-- select boolean constant, already vectorized

explain vectorization detail select false or dt1 is not null from vector_and_or;
-- check vectorized results
select false or dt1 is not null from vector_and_or;
set hive.vectorized.execution.enabled=false;
-- check non-vectorized results
select false or dt1 is not null from vector_and_or;
set hive.vectorized.execution.enabled=true;

-- select dt1 = dt1 which is translated to "null or ..." after HIVE-21001

explain vectorization detail select dt1 = dt1 from vector_and_or;
-- check vectorized results
select dt1 = dt1 from vector_and_or;
set hive.vectorized.execution.enabled=false;
-- check non-vectorized results
select dt1 = dt1 from vector_and_or;
