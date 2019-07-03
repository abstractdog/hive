--TODO: this test is intended to be run against postgres metastore, see HIVE-21940 for details
drop table if exists my_table;
create external table my_table (col1 int, col3 int) partitioned by (col2 string) STORED AS TEXTFILE TBLPROPERTIES ("serialization.format" = "1");
insert into my_table VALUES(11, 201, "F");
describe formatted my_table;
