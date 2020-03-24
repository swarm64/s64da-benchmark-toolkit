select sum(lo_revenue) as lo_revenue, d_year, p_brand1
from lineorder
left join date on lo_orderdate = d_datekey
left join part on lo_partkey = p_partkey
left join supplier on lo_suppkey = s_suppkey
where p_brand1 = 'MFGR#2221' and s_region = 'EUROPE'
group by d_year, p_brand1
order by d_year, p_brand1;