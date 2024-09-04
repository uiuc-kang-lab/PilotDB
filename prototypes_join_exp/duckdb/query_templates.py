gt_query_5 = """
select
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as sum_1,
    floor(lineitem.rowid/2048) as l_pageid,
    floor(orders.rowid/2048) as o_pageid,
    floor(customer.rowid/2048) as c_pageid,
    floor(supplier.rowid/2048) as s_pageid
from
    customer,
    orders,
    lineitem,
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
    o_pageid,
    c_pageid,
    s_pageid
"""


pilot_query_5 = """
select
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as sum_1,
    floor(lineitem.rowid/2048) as l_pageid,
    floor(orders.rowid/2048) as o_pageid
from
    customer,
    orders,
    lineitem TABLESAMPLE CHUNK (0.05%),
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
    o_pageid
"""

single_table_sample_query_5 = """
select
    n_name,
    sum(l_extendedprice * (1 - l_discount)) / 100 / {sample_rate_1} as revenue
from
    customer,
    orders,
    lineitem TABLESAMPLE CHUNK ({sample_rate_1}%),
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
    n_name
order by
    n_name;
"""

multi_table_sample_query_5 = """
select
    n_name,
    sum(l_extendedprice * (1 - l_discount)) * 100 / {sample_rate_1} * 100 / {sample_rate_2} as revenue
from
    customer,
    orders TABLESAMPLE CHUNK ({sample_rate_2}%),
    lineitem TABLESAMPLE CHUNK ({sample_rate_1}%),
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
    n_name
order by
    n_name;
"""

gt_query_7 = """
select
    supp_nation,
    cust_nation,
    l_year,
    sum(volume) as sum_1,
    l_pageid,
    o_pageid,
    c_pageid,
    s_pageid
from
    (
        select
            n1.n_name as supp_nation,
            n2.n_name as cust_nation,
            extract(year from l_shipdate) as l_year,
            l_extendedprice * (1 - l_discount) as volume,
            floor(lineitem.rowid/2048) as l_pageid,
            floor(orders.rowid/2048) as o_pageid,
            floor(customer.rowid/2048) as c_pageid,
            floor(supplier.rowid/2048) as s_pageid
        from
            supplier,
            lineitem,
            orders,
            customer,
            nation n1,
            nation n2
        where
            s_suppkey = l_suppkey
            and o_orderkey = l_orderkey
            and c_custkey = o_custkey
            and s_nationkey = n1.n_nationkey
            and c_nationkey = n2.n_nationkey
            and (
                (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')
                or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')
            )
            and l_shipdate between date '1995-01-01' and date '1996-12-31'
    ) as shipping
group by
    supp_nation,
    cust_nation,
    l_year,
    l_pageid,
    o_pageid,
    c_pageid,
    s_pageid
"""

pilot_query_7 = """
select
    supp_nation,
    cust_nation,
    l_year,
    sum(volume) as sum_1,
    l_pageid,
    o_pageid
from
    (
        select
            n1.n_name as supp_nation,
            n2.n_name as cust_nation,
            extract(year from l_shipdate) as l_year,
            l_extendedprice * (1 - l_discount) as volume,
            floor(lineitem.rowid/2048) as l_pageid,
            floor(orders.rowid/2048) as o_pageid
        from
            supplier,
            lineitem TABLESAMPLE CHUNK (0.05%),
            orders,
            customer,
            nation n1,
            nation n2
        where
            s_suppkey = l_suppkey
            and o_orderkey = l_orderkey
            and c_custkey = o_custkey
            and s_nationkey = n1.n_nationkey
            and c_nationkey = n2.n_nationkey
            and (
                (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')
                or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')
            )
            and l_shipdate between date '1995-01-01' and date '1996-12-31'
    ) as shipping
group by
    supp_nation,
    cust_nation,
    l_year,
    l_pageid,
    o_pageid
"""

single_table_sample_query_7 = """
select
    supp_nation,
    cust_nation,
    l_year,
    sum(volume) * 100 / {sample_rate_1} as revenue
from
    (
        select
            n1.n_name as supp_nation,
            n2.n_name as cust_nation,
            extract(year from l_shipdate) as l_year,
            l_extendedprice * (1 - l_discount) as volume
        from
            supplier,
            lineitem TABLESAMPLE CHUNK ({sample_rate_1}%),
            orders,
            customer,
            nation n1,
            nation n2
        where
            s_suppkey = l_suppkey
            and o_orderkey = l_orderkey
            and c_custkey = o_custkey
            and s_nationkey = n1.n_nationkey
            and c_nationkey = n2.n_nationkey
            and (
                (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')
                or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')
            )
            and l_shipdate between date '1995-01-01' and date '1996-12-31'
    ) as shipping
group by
    supp_nation,
    cust_nation,
    l_year
order by
    supp_nation,
    cust_nation,
    l_year;
"""

multi_table_sample_query_7 = """
select
    supp_nation,
    cust_nation,
    l_year,
    sum(volume) * 100 / {sample_rate_1} * 100 / {sample_rate_2} as revenue
from
    (
        select
            n1.n_name as supp_nation,
            n2.n_name as cust_nation,
            extract(year from l_shipdate) as l_year,
            l_extendedprice * (1 - l_discount) as volume
        from
            supplier,
            lineitem TABLESAMPLE CHUNK ({sample_rate_1}%),
            orders TABLESAMPLE CHUNK ({sample_rate_2}%),
            customer,
            nation n1,
            nation n2
        where
            s_suppkey = l_suppkey
            and o_orderkey = l_orderkey
            and c_custkey = o_custkey
            and s_nationkey = n1.n_nationkey
            and c_nationkey = n2.n_nationkey
            and (
                (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')
                or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')
            )
            and l_shipdate between date '1995-01-01' and date '1996-12-31'
    ) as shipping
group by
    supp_nation,
    cust_nation,
    l_year
order by
    supp_nation,
    cust_nation,
    l_year;
"""

gt_query_8 = """
select
    o_year,
    sum(case
        when nation = 'BRAZIL' then volume
        else 0
    end) as sum_1,
    sum(volume) as sum_2,
    l_pageid,
    o_pageid,
    c_pageid,
    s_pageid,
    p_pageid
from
    (
        select
            extract(year from o_orderdate) as o_year,
            l_extendedprice * (1 - l_discount) as volume,
            n2.n_name as nation,
            floor(lineitem.rowid/2048) as l_pageid,
            floor(orders.rowid/2048) as o_pageid,
            floor(customer.rowid/2048) as c_pageid,
            floor(supplier.rowid/2048) as s_pageid,
            floor(part.rowid/2048) as p_pageid,
        from
            part,
            supplier,
            lineitem,
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
    o_pageid,
    c_pageid,
    s_pageid,
    p_pageid;
"""

pilot_query_8 = """
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

single_table_sample_query_8 = """
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

multi_table_sample_query_8 = """
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
            orders TABLESAMPLE CHUNK ({sample_rate_2}%),
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

gt_query_9 = """
select
    nation,
    o_year,
    sum(amount) as sum_1,
    l_pageid,
    o_pageid,
    ps_pageid,
    s_pageid,
    p_pageid
from
    (
        select
            n_name as nation,
            extract(year from o_orderdate) as o_year,
            l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount,
            floor(lineitem.rowid/2048) as l_pageid,
            floor(orders.rowid/2048) as o_pageid,
            floor(partsupp.rowid/2048) as ps_pageid,
            floor(supplier.rowid/2048) as s_pageid,
            floor(part.rowid/2048) as p_pageid
        from
            part,
            supplier,
            lineitem,
            partsupp,
            orders,
            nation
        where
            s_suppkey = l_suppkey
            and ps_suppkey = l_suppkey
            and ps_partkey = l_partkey
            and p_partkey = l_partkey
            and o_orderkey = l_orderkey
            and s_nationkey = n_nationkey
            and p_name like '%green%'
    ) as profit
group by
    nation,
    o_year,
    l_pageid,
    o_pageid,
    ps_pageid,
    s_pageid,
    p_pageid
"""

pilot_query_9 = """
select
    nation,
    o_year,
    sum(amount) as sum_1,
    l_pageid,
    o_pageid
from
    (
        select
            n_name as nation,
            extract(year from o_orderdate) as o_year,
            l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount,
            floor(lineitem.rowid/2048) as l_pageid,
            floor(orders.rowid/2048) as o_pageid
        from
            part,
            supplier,
            lineitem TABLESAMPLE CHUNK (0.05%),
            partsupp,
            orders,
            nation
        where
            s_suppkey = l_suppkey
            and ps_suppkey = l_suppkey
            and ps_partkey = l_partkey
            and p_partkey = l_partkey
            and o_orderkey = l_orderkey
            and s_nationkey = n_nationkey
            and p_name like '%green%'
    ) as profit
group by
    nation,
    o_year,
    l_pageid,
    o_pageid;
"""

single_table_sample_query_9 = """
select
    nation,
    o_year,
    sum(amount) * 100 / {sample_rate_1} as sum_profit
from
    (
        select
            n_name as nation,
            extract(year from o_orderdate) as o_year,
            l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
        from
            part,
            supplier,
            lineitem TABLESAMPLE CHUNK ({sample_rate_1}%),
            partsupp,
            orders,
            nation
        where
            s_suppkey = l_suppkey
            and ps_suppkey = l_suppkey
            and ps_partkey = l_partkey
            and p_partkey = l_partkey
            and o_orderkey = l_orderkey
            and s_nationkey = n_nationkey
            and p_name like '%green%'
    ) as profit
group by
    nation,
    o_year
order by
    nation,
    o_year desc;
"""

multi_table_sample_query_9 = """
select
    nation,
    o_year,
    sum(amount) * 100 / {sample_rate_1} * 100 / {sample_rate_2} as sum_profit
from
    (
        select
            n_name as nation,
            extract(year from o_orderdate) as o_year,
            l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
        from
            part,
            supplier,
            lineitem TABLESAMPLE CHUNK ({sample_rate_1}%),
            partsupp,
            orders TABLESAMPLE CHUNK ({sample_rate_2}%),
            nation
        where
            s_suppkey = l_suppkey
            and ps_suppkey = l_suppkey
            and ps_partkey = l_partkey
            and p_partkey = l_partkey
            and o_orderkey = l_orderkey
            and s_nationkey = n_nationkey
            and p_name like '%green%'
    ) as profit
group by
    nation,
    o_year
order by
    nation,
    o_year desc;
"""

gt_query_12 = """
select
    l_shipmode,
    sum(case
        when o_orderpriority = '1-URGENT'
            or o_orderpriority = '2-HIGH'
            then 1
        else 0
    end) as sum_1,
    sum(case
        when o_orderpriority <> '1-URGENT'
            and o_orderpriority <> '2-HIGH'
            then 1
        else 0
    end) as sum_2,
    floor(lineitem.rowid/2048) as l_pageid,
    floor(orders.rowid/2048) as o_pageid
from
    orders,
    lineitem
where
    o_orderkey = l_orderkey
    and l_shipmode in ('MAIL', 'SHIP')
    and l_commitdate < l_receiptdate
    and l_shipdate < l_commitdate
    and l_receiptdate >= date '1994-01-01'
    and l_receiptdate < date '1994-01-01' + interval '1' year
group by
    l_shipmode,
    l_pageid,
    o_pageid
"""

pilot_query_12 = """
select
    l_shipmode,
    sum(case
        when o_orderpriority = '1-URGENT'
            or o_orderpriority = '2-HIGH'
            then 1
        else 0
    end) as sum_1,
    sum(case
        when o_orderpriority <> '1-URGENT'
            and o_orderpriority <> '2-HIGH'
            then 1
        else 0
    end) as sum_2,
    floor(lineitem.rowid/2048) as l_pageid,
    floor(orders.rowid/2048) as o_pageid
from
    orders,
    lineitem TABLESAMPLE CHUNK (0.05%)
where
    o_orderkey = l_orderkey
    and l_shipmode in ('MAIL', 'SHIP')
    and l_commitdate < l_receiptdate
    and l_shipdate < l_commitdate
    and l_receiptdate >= date '1994-01-01'
    and l_receiptdate < date '1994-01-01' + interval '1' year
group by
    l_shipmode,
    l_pageid,
    o_pageid;
"""

single_table_sample_query_12 = """
select
    l_shipmode,
    sum(case
        when o_orderpriority = '1-URGENT'
            or o_orderpriority = '2-HIGH'
            then 1
        else 0
    end) * 100 / {sample_rate_1} as high_line_count,
    sum(case
        when o_orderpriority <> '1-URGENT'
            and o_orderpriority <> '2-HIGH'
            then 1
        else 0
    end) * 100 / {sample_rate_1} as low_line_count
from
    orders,
    lineitem TABLESAMPLE CHUNK ({sample_rate_1}%)
where
    o_orderkey = l_orderkey
    and l_shipmode in ('MAIL', 'SHIP')
    and l_commitdate < l_receiptdate
    and l_shipdate < l_commitdate
    and l_receiptdate >= date '1994-01-01'
    and l_receiptdate < date '1994-01-01' + interval '1' year
group by
    l_shipmode
order by
    l_shipmode
"""

multi_table_sample_query_12 = """
select
    l_shipmode,
    sum(case
        when o_orderpriority = '1-URGENT'
            or o_orderpriority = '2-HIGH'
            then 1
        else 0
    end) * 100 / {sample_rate_1} * 100 / {sample_rate_2} as high_line_count,
    sum(case
        when o_orderpriority <> '1-URGENT'
            and o_orderpriority <> '2-HIGH'
            then 1
        else 0
    end) * 100 / {sample_rate_1} * 100 / {sample_rate_2} as low_line_count
from
    orders TABLESAMPLE CHUNK ({sample_rate_2}%),
    lineitem TABLESAMPLE CHUNK ({sample_rate_1}%)
where
    o_orderkey = l_orderkey
    and l_shipmode in ('MAIL', 'SHIP')
    and l_commitdate < l_receiptdate
    and l_shipdate < l_commitdate
    and l_receiptdate >= date '1994-01-01'
    and l_receiptdate < date '1994-01-01' + interval '1' year
group by
    l_shipmode
order by
    l_shipmode;
"""

gt_query_14 = """
select
    100.00 * sum(case
        when p_type like 'PROMO%'
            then l_extendedprice * (1 - l_discount)
        else 0
    end) as sum_1,
    sum(l_extendedprice * (1 - l_discount)) as sum_2,
    floor(lineitem.rowid/2048) as l_pageid,
    floor(part.rowid/2048) as p_pageid
from
    lineitem,
    part
where
    l_partkey = p_partkey
    and l_shipdate >= date '1995-09-01'
    and l_shipdate < date '1995-09-01' + interval '1' month
group by
    l_pageid,
    p_pageid
"""

pilot_query_14 = """
select
    100.00 * sum(case
        when p_type like 'PROMO%'
            then l_extendedprice * (1 - l_discount)
        else 0
    end) as sum_1,
    sum(l_extendedprice * (1 - l_discount)) as sum_2,
    floor(lineitem.rowid/2048) as l_pageid,
    floor(part.rowid/2048) as p_pageid
from
    lineitem TABLESAMPLE CHUNK (0.05%),
    part
where
    l_partkey = p_partkey
    and l_shipdate >= date '1995-09-01'
    and l_shipdate < date '1995-09-01' + interval '1' month
group by
    l_pageid,
    p_pageid;
"""

single_table_sample_query_14 = """
select
    100.00 * sum(case
        when p_type like 'PROMO%'
            then l_extendedprice * (1 - l_discount)
        else 0
    end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
    lineitem TABLESAMPLE CHUNK ({sample_rate_1}%),
    part
where
    l_partkey = p_partkey
    and l_shipdate >= date '1995-09-01'
    and l_shipdate < date '1995-09-01' + interval '1' month;
"""

multi_table_sample_query_14 = """
select
    100.00 * sum(case
        when p_type like 'PROMO%'
            then l_extendedprice * (1 - l_discount)
        else 0
    end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
    lineitem TABLESAMPLE CHUNK ({sample_rate_1}%),
    part TABLESAMPLE CHUNK ({sample_rate_2}%)
where
    l_partkey = p_partkey
    and l_shipdate >= date '1995-09-01'
    and l_shipdate < date '1995-09-01' + interval '1' month;
"""

gt_query_19 = """
select
    sum(l_extendedprice* (1 - l_discount)) as sum_1,
    floor(lineitem.rowid/2048) as l_pageid,
    floor(part.rowid/2048) as p_pageid
from
    lineitem,
    part
where
    (
        p_partkey = l_partkey
        and p_brand = 'Brand#12'
        and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        and l_quantity >= 1 and l_quantity <= 1 + 10
        and p_size between 1 and 5
        and l_shipmode in ('AIR', 'AIR REG')
        and l_shipinstruct = 'DELIVER IN PERSON'
    )
    or
    (
        p_partkey = l_partkey
        and p_brand = 'Brand#23'
        and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        and l_quantity >= 10 and l_quantity <= 10 + 10
        and p_size between 1 and 10
        and l_shipmode in ('AIR', 'AIR REG')
        and l_shipinstruct = 'DELIVER IN PERSON'
    )
    or
    (
        p_partkey = l_partkey
        and p_brand = 'Brand#34'
        and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        and l_quantity >= 20 and l_quantity <= 20 + 10
        and p_size between 1 and 15
        and l_shipmode in ('AIR', 'AIR REG')
        and l_shipinstruct = 'DELIVER IN PERSON'
    )
group by l_pageid, p_pageid
"""

pilot_query_19 = """
select
    sum(l_extendedprice* (1 - l_discount)) as sum_1,
    floor(lineitem.rowid/2048) as l_pageid,
    floor(part.rowid/2048) as p_pageid
from
    lineitem TABLESAMPLE CHUNK (0.05%),
    part
where
    (
        p_partkey = l_partkey
        and p_brand = 'Brand#12'
        and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        and l_quantity >= 1 and l_quantity <= 1 + 10
        and p_size between 1 and 5
        and l_shipmode in ('AIR', 'AIR REG')
        and l_shipinstruct = 'DELIVER IN PERSON'
    )
    or
    (
        p_partkey = l_partkey
        and p_brand = 'Brand#23'
        and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        and l_quantity >= 10 and l_quantity <= 10 + 10
        and p_size between 1 and 10
        and l_shipmode in ('AIR', 'AIR REG')
        and l_shipinstruct = 'DELIVER IN PERSON'
    )
    or
    (
        p_partkey = l_partkey
        and p_brand = 'Brand#34'
        and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        and l_quantity >= 20 and l_quantity <= 20 + 10
        and p_size between 1 and 15
        and l_shipmode in ('AIR', 'AIR REG')
        and l_shipinstruct = 'DELIVER IN PERSON'
    )
group by l_pageid, p_pageid;
"""

single_table_sample_query_19 = """
select
    sum(l_extendedprice* (1 - l_discount)) * 100 / {sample_rate_1} as revenue
from
    lineitem TABLESAMPLE CHUNK ({sample_rate_1}%),
    part
where
    (
        p_partkey = l_partkey
        and p_brand = 'Brand#12'
        and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        and l_quantity >= 1 and l_quantity <= 1 + 10
        and p_size between 1 and 5
        and l_shipmode in ('AIR', 'AIR REG')
        and l_shipinstruct = 'DELIVER IN PERSON'
    )
    or
    (
        p_partkey = l_partkey
        and p_brand = 'Brand#23'
        and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        and l_quantity >= 10 and l_quantity <= 10 + 10
        and p_size between 1 and 10
        and l_shipmode in ('AIR', 'AIR REG')
        and l_shipinstruct = 'DELIVER IN PERSON'
    )
    or
    (
        p_partkey = l_partkey
        and p_brand = 'Brand#34'
        and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        and l_quantity >= 20 and l_quantity <= 20 + 10
        and p_size between 1 and 15
        and l_shipmode in ('AIR', 'AIR REG')
        and l_shipinstruct = 'DELIVER IN PERSON'
    );
"""

multi_table_sample_query_19 = """
select
    sum(l_extendedprice* (1 - l_discount)) * 100 / {sample_rate_1} * 100 / {sample_rate_2} as revenue
from
    lineitem TABLESAMPLE CHUNK ({sample_rate_1}%),
    part TABLESAMPLE CHUNK ({sample_rate_2}%)
where
    (
        p_partkey = l_partkey
        and p_brand = 'Brand#12'
        and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        and l_quantity >= 1 and l_quantity <= 1 + 10
        and p_size between 1 and 5
        and l_shipmode in ('AIR', 'AIR REG')
        and l_shipinstruct = 'DELIVER IN PERSON'
    )
    or
    (
        p_partkey = l_partkey
        and p_brand = 'Brand#23'
        and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        and l_quantity >= 10 and l_quantity <= 10 + 10
        and p_size between 1 and 10
        and l_shipmode in ('AIR', 'AIR REG')
        and l_shipinstruct = 'DELIVER IN PERSON'
    )
    or
    (
        p_partkey = l_partkey
        and p_brand = 'Brand#34'
        and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        and l_quantity >= 20 and l_quantity <= 20 + 10
        and p_size between 1 and 15
        and l_shipmode in ('AIR', 'AIR REG')
        and l_shipinstruct = 'DELIVER IN PERSON'
    );
"""