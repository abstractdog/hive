--! qt:dataset:alltypesorc
set hive.fetch.task.conversion=none;
set hive.vectorized.execution.enabled=true;

DESCRIBE FUNCTION trunc;
DESCRIBE FUNCTION EXTENDED trunc;

explain vectorization detail
select trunc(ctimestamp1, 'MM') from alltypesorc;

select trunc(ctimestamp1, 'MM'), ctimestamp1 from alltypesorc LIMIT 10;
select trunc(ctimestamp1, 'Q'), ctimestamp1 from alltypesorc LIMIT 10;
select trunc(ctimestamp1, 'YEAR'), ctimestamp1 from alltypesorc LIMIT 10;

select trunc(ctimestamp1, 'MM'), ctimestamp1 from alltypesorc WHERE ctimestamp1 IS NULL LIMIT 10;

