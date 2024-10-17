pilot_query = """
select
    o_year,
    avg(case
        when nation = 'BRAZIL' then volume
        else 0
    end) as avg_1,
    stddev(case
        when nation = 'BRAZIL' then volume
        else 0
    end) as std_1,
    avg(volume) as avg_2,
    stddev(volume) as std_2,
    count(*) as sample_size
from
    (
        select
            extract(year from o_orderdate) as o_year,
            l_extendedprice * (1 - l_discount) as volume,
            n2.n_name as nation
        from
            part,
            supplier,
            lineitem {sampling_method},
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

results_mapping = [
    {
        "aggregate": "div",
        "first_element": "avg_1",
        "second_element": "avg_2",
        "size": "sample_size",
    }
]

subquery_dict = []
