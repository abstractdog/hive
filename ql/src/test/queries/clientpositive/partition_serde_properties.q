create table t (a string, b string) partitioned by (p string) stored as textfile;
alter table t add partition (p="p");
alter table t partition (p="p") set serdeproperties('field.delim' = ',');
describe formatted t partition (p="p");
insert into t partition (p="p") values (1, 2);
describe formatted t partition (p="p");

