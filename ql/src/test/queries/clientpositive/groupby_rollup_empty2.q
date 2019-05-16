set hive.auto.convert.join=true;
set hive.auto.convert.sortmerge.join.to.mapjoin=true;

drop table if exists item_1;
create table item_1 (i_category_id int, i_item_sk int) stored as ORC;
insert into item_1 values (1,1), (2,2);

drop table if exists store_sales_1;
create table store_sales_1 (ss_item_sk int) partitioned by (ss_store_sk int) stored as ORC;
insert into store_sales_1 partition(ss_store_sk=1) values (1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20),(21),(22),(23),(24),(25),(26),(27),(28),(29),(30),(31),(32),(33),(34),(35),(36),(37),(38),(39),(40),(41),(42),(43),(44),(45),(46),(47),(48),(49),(50),(51),(52),(53),(54),(55),(56),(57),(58),(59),(60),(61),(62),(63),(64),(65),(66),(67),(68),(69),(70),(71),(72),(73),(74),(75),(76),(77),(78),(79),(80),(81),(82),(83),(84),(85),(86),(87),(88),(89),(90),(91),(92),(93),(94),(95),(96),(97),(98),(99),(100);

drop table if exists store_1;
create table store_1 (s_store_sk int) stored as ORC;
insert into store_1 values (1);

select i_category_id,count(1)
from item_1
join store_sales_1 on i_item_sk  = ss_item_sk
join store_1 on s_store_sk  = ss_store_sk
where i_item_sk = -1
group by rollup(i_category_id);
