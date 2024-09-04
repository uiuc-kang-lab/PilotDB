import duckdb
import time
import os

os.system("sudo sync; echo 3 | sudo tee /proc/sys/vm/drop_caches;")

conn = duckdb.connect("/mydata/tpch_1t.duckdb", read_only = True)

pilot_query = """
select
    o_year,
    sum(case
        when nation = 'BRAZIL' then volume
        else 0
    end) as sum_1,
    sum(volume) as sum_2,
    l_pageid,
    o_pageid
from
    (
        select
            extract(year from o_orderdate) as o_year,
            l_extendedprice * (1 - l_discount) as volume,
            n2.n_name as nation,
            floor(lineitem.rowid/2048) as l_pageid,
            floor(orders.rowid/2048) as o_pageid
        from
            part,
            supplier,
            lineitem TABLESAMPLE CHUNK (0.05%),
            orders,
            customer,
            nation n1,
            nation n2,
            region
        where
            p_partkey = l_partkey
            and s_suppkey = l_suppkey
            and l_orderkey = o_orderkey
            and o_custkey = c_custkey
            and c_nationkey = n1.n_nationkey
            and n1.n_regionkey = r_regionkey
            and r_name = 'AMERICA'
            and s_nationkey = n2.n_nationkey
            and o_orderdate between date '1995-01-01' and date '1996-12-31'
            and p_type = 'ECONOMY ANODIZED STEEL'
    ) as all_nations
group by
    o_year,
    l_pageid,
    o_pageid;
"""

single_table_query = """
select
    o_year,
    sum(case
        when nation = 'BRAZIL' then volume
        else 0
    end) / sum(volume) as mkt_share
from
    (
        select
            extract(year from o_orderdate) as o_year,
            l_extendedprice * (1 - l_discount) as volume,
            n2.n_name as nation
        from
            part,
            supplier,
            lineitem TABLESAMPLE CHUNK ({sample_rate_1}%),
            orders,
            customer,
            nation n1,
            nation n2,
            region
        where
            p_partkey = l_partkey
            and s_suppkey = l_suppkey
            and l_orderkey = o_orderkey
            and o_custkey = c_custkey
            and c_nationkey = n1.n_nationkey
            and n1.n_regionkey = r_regionkey
            and r_name = 'AMERICA'
            and s_nationkey = n2.n_nationkey
            and o_orderdate between date '1995-01-01' and date '1996-12-31'
            and p_type = 'ECONOMY ANODIZED STEEL'
    ) as all_nations
group by
    o_year
order by
    o_year;
"""
start = time.time()
x = conn.sql(pilot_query).fetchdf()
y = conn.sql(single_table_query.format(sample_rate_1 = 30)).fetchdf()
runtime = time.time() - start
print(runtime)
conn.close()