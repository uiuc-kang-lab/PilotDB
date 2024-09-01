select
    sum(l_extendedprice * (1 - l_discount)) / 0.01 / 0.01 as revenue
from
    lineitem TABLESAMPLE SYSTEM(0.5),
    orders TABLESAMPLE SYSTEM(1)
where
    l_orderkey = o_orderkey
    and o_orderdate >= date '1994-01-01'
    and o_orderdate < date '1994-01-01' + interval '1' year;
