set hive.explain.user=false;

create table sourceTable (product_id int);
create table targetTable (product_id bigint);

insert into sourceTable values(1);
insert overwrite table targetTable select UDFToLong(COALESCE(product_id,-1)) from sourceTable;

select * from targetTable;

explain select UDFToLong(COALESCE(product_id,-1)) from targetTable;