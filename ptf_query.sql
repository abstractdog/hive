set hive.vectorized.ptf.max.memory.buffering.batch.count=10;
set hive.vectorized.testing.reducer.batch.size=2;

select p_mfgr, p_name, rowindex, p_date, p_retailprice,
count(*) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as cs1,
count(*) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as cs2,
count(rowindex) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as c1,
count(rowindex) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as c2,
count(p_date) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as c_order,
sum(p_retailprice) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as s1,
sum(p_retailprice) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as s2,
min(p_retailprice) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as min1,
min(p_retailprice) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as min2,
max(p_retailprice) over(partition by p_mfgr order by p_date range between 1 preceding and current row) as max1,
max(p_retailprice) over(partition by p_mfgr order by p_date range between 3 preceding and current row) as max2,
row_number() over(partition by p_mfgr) as rn,
rank() over(partition by p_mfgr) as r,
dense_rank() over(partition by p_mfgr) as dr,
rank() over(partition by p_mfgr order by p_date) as r_date,
dense_rank() over(partition by p_mfgr order by p_date) as dr_date,
first_value(p_retailprice) over(partition by p_mfgr) as fv,
last_value(p_retailprice) over(partition by p_mfgr) as lv
from vector_ptf_part_simple_orc;