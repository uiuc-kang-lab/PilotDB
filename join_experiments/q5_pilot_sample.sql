COPY (select
    n_name,
    (lineitem.ctid::text::point)[0] l_pageid,
    (orders.ctid::text::point)[0] o_pageid,
    sum(l_extendedprice * (1 - l_discount)) as revenue
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
    n_name,
    l_pageid,
    o_pageid)
TO '/users/yuxuan18/PilotDB/join_experiments/pilot_result_1.csv' 
DELIMITER ',' CSV HEADER