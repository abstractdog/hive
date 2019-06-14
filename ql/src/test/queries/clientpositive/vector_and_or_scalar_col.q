--! qt:dataset:src
set hive.explain.user=false;
set hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

drop table if exists vector_and_or;
create table vector_and_or (dt1 date, dt2 date) stored as orc;

insert into table vector_and_or
  select null, null from src limit 1;
insert into table vector_and_or
  select date '1999-12-31', date '2000-01-01' from src limit 1;
insert into table vector_and_or
  select date '2001-01-01', date '2001-06-01' from src limit 1;

explain vectorization detail select null or dt1 is not null from vector_and_or;
select null or dt1 is not null from vector_and_or;

explain vectorization detail select false or dt1 is not null from vector_and_or;
select false or dt1 is not null from vector_and_or;