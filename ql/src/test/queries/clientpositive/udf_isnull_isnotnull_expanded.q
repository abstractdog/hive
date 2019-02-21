create table expanded_null_predicate (x int, y int, z int);
insert into expanded_null_predicate values (1, 1, 1), (2, 2, 2), (null, 3, null), (4, null, null), (null, null, null);

--set hive.vectorized.execution.enabled = false;

naselect * from expanded_null_predicate where (x, z) is null;
--select * from expanded_null_predicate where (x, z) is not null;

--set hive.vectorized.execution.enabled = true;

--select * from expanded_null_predicate where (x, z) is null;
--select * from expanded_null_predicate where (x, z) is not null;