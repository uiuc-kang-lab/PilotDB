select
    COUNT(DISTINCT (lineitem.ctid::text::point)[0]) n_pages_lineitem
from
    -- customer,
    lineitem,
    -- orders,
    supplier,
    nation,
    region
where
    -- c_custkey = o_custkey
    -- and l_orderkey = o_orderkey
    -- and 
    l_suppkey = s_suppkey
    -- and c_nationkey = s_nationkey
    and s_nationkey = n_nationkey
    and n_regionkey = r_regionkey
    and r_name = 'ASIA'
    -- and o_orderdate >= date '1994-01-01'
    -- and o_orderdate < date '1994-01-01' + interval '1' year
group by
    n_name;

select
    COUNT(DISTINCT (orders.ctid::text::point)[0]) n_pages_orders
from
    customer,
    orders--,
    -- lineitem,
    -- supplier,
    -- nation,
    -- region
where
    c_custkey = o_custkey
    -- and l_orderkey = o_orderkey
    -- and l_suppkey = s_suppkey
    -- and c_nationkey = s_nationkey
    -- and s_nationkey = n_nationkey
    -- and n_regionkey = r_regionkey
    -- and r_name = 'ASIA'
    and o_orderdate >= date '1994-01-01'
    and o_orderdate < date '1994-01-01' + interval '1' year;