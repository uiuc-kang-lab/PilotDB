pilot_query = """
select
    avg(l_extendedprice * (1 - l_discount)) as avg_1,
    avg(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as avg_2,
    avg(l_quantity) as avg_3,
    avg(l_extendedprice) as avg_4,
    avg(l_discount) as avg_5,
    stdev(l_extendedprice * (1 - l_discount)) as std_1,
    stdev(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as std_2,
    stdev(l_quantity) as std_3,
    stdev(l_extendedprice) as std_4,
    stdev(l_discount) as std_5,
    COUNT_BIG(*) as sample_size
from
    lineitem
where
    l_shipdate <= DATEADD(day, -90, '1998-12-01')
group by
    l_returnflag,
    l_linestatus
order by
    l_returnflag,
    l_linestatus;
"""

results_mapping = [
    {"agg": "sum", "mean": "avg_1", "std": "std_1", "size": "sample_size"},
    {"agg": "sum", "mean": "avg_2", "std": "std_2", "size": "sample_size"},
    {"agg": "sum", "mean": "avg_3", "std": "std_3", "size": "sample_size"},
    {"agg": "sum", "mean": "avg_4", "std": "std_4", "size": "sample_size"},
    {"agg": "avg", "avg": "avg_3", "std": "std_3"},
    {"agg": "avg", "avg": "avg_4", "std": "std_4"},
    {"agg": "avg", "avg": "avg_5", "std": "std_5"},
    {"agg": "count", "size": "sample_size"}
]