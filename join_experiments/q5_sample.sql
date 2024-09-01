select
    n_name,
    sum(l_extendedprice * (1 - l_discount)) / 0.01 / 0.01 as revenue
from
    customer,
    orders TABLESAMPLE SYSTEM(1),
    lineitem TABLESAMPLE SYSTEM(1),
    supplier,
    nation,
    region
where
    c_custkey = o_custkey
    and l_orderkey = o_orderkey
    and l_suppkey = s_suppkey
    and c_nationkey = s_nationkey
    and s_nationkey = n_nationkey
    and n_regionkey = r_regionkey
    and r_name = 'ASIA'
    and o_orderdate >= date '1994-01-01'
    and o_orderdate < date '1994-01-01' + interval '1' year
group by
    n_name;
