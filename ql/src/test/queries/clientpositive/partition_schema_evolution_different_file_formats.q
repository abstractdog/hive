create external table test_parquet (a string, b string) partitioned by (pt string) STORED AS PARQUET;
insert into test_parquet PARTITION(pt='1111') values ("aaaa","bbbb");

alter table test_parquet add columns (c string);

SELECT "describe formatted on table should return all columns, including the newly added c: a,b,c";
desc formatted test_parquet;
SELECT "describe formatted on partition should return only original columns: a,b";
desc formatted test_parquet PARTITION(pt='1111');

select * from test_parquet;

insert overwrite table test_parquet PARTITION(pt='1111') values ("aaa", "bbb", "ccc");
SELECT "select should return aaa bbb NULL 1111, as the partition schema didn't change in the absence of CASCADE keyword";
select * from test_parquet;


create external table test_orc (a string, b string) partitioned by (pt string) STORED AS ORC;
insert into test_orc PARTITION(pt='1111') values ("aaaa","bbbb");

alter table test_orc add columns (c string);

SELECT "describe formatted on table should return all columns, including the newly added c: a,b,c";
desc formatted test_orc;
SELECT "describe formatted on partition should return only original columns: a,b";
desc formatted test_orc PARTITION(pt='1111');

select * from test_orc;

insert overwrite table test_orc PARTITION(pt='1111') values ("aaa", "bbb", "ccc");
SELECT "select should return aaa bbb NULL 1111, as the partition schema didn't change in the absence of CASCADE keyword";
select * from test_orc;

create external table test_avro (a string, b string) partitioned by (pt string) STORED AS AVRO;
insert into test_avro PARTITION(pt='1111') values ("aaaa","bbbb");

alter table test_avro add columns (c string);

SELECT "describe formatted on table should return all columns, including the newly added c: a,b,c";
desc formatted test_avro;
SELECT "describe formatted on partition should return only original columns: a,b";
desc formatted test_avro PARTITION(pt='1111');

select * from test_avro;

insert overwrite table test_avro PARTITION(pt='1111') values ("aaa", "bbb", "ccc");
SELECT "select should return aaa bbb NULL 1111, as the partition schema didn't change in the absence of CASCADE keyword";
select * from test_avro;

create external table test_text (a string, b string) partitioned by (pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;
insert into test_text PARTITION(pt='1111') values ("aaaa","bbbb");

alter table test_text add columns (c string);

SELECT "describe formatted on table should return all columns, including the newly added c: a,b,c";
desc formatted test_text;
SELECT "describe formatted on partition should return only original columns: a,b";
desc formatted test_text PARTITION(pt='1111');

select * from test_text;

insert overwrite table test_text PARTITION(pt='1111') values ("aaa", "bbb", "ccc");
SELECT "select should return aaa bbb NULL 1111, as the partition schema didn't change in the absence of CASCADE keyword";
select * from test_text;
