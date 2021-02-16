-- vim: set ft=sql:
-- EXPLAIN (FORMAT JSON)
select
    s_i_id,
    sum(s_order_cnt) as order_count
from
    stock,
    supplier,
    nation
where
    mod((s_w_id * s_i_id), 10000) = su_suppkey
    and su_nationkey = n_nationkey
    and n_name = 'Germany'
group by
    s_i_id
having
    sum(s_order_cnt) > (
        select
            sum(s_order_cnt) * .005
        from
            stock,
            supplier,
            nation
        where
            mod((s_w_id * s_i_id), 10000) = su_suppkey
            and su_nationkey = n_nationkey
            and n_name = 'Germany'
    )
order by
    order_count desc;
