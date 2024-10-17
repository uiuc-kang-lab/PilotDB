pilot_query = """
select
    avg(l_extendedprice * (1 - l_discount)) as avg_1,
    avg(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as avg_2,
    avg(l_quantity) as avg_3,
    avg(l_extendedprice) as avg_4,
    avg(l_discount) as avg_5,
    stddev(l_extendedprice * (1 - l_discount)) as std_1,
    stddev(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as std_2,
    stddev(l_quantity) as std_3,
    stddev(l_extendedprice) as std_4,
    stddev(l_discount) as std_5,
    COUNT(*) as sample_size
from
    lineitem {sampling_method}
where
    l_shipdate <= date '1998-12-01' - interval '90 day'
group by
    l_returnflag,
    l_linestatus
order by
    l_returnflag,
    l_linestatus;
"""

results_mapping = [
    {"aggregate": "sum", "mean": "avg_1", "std": "std_1", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_2", "std": "std_2", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_3", "std": "std_3", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_4", "std": "std_4", "size": "sample_size"},
    {"aggregate": "avg", "mean": "avg_3", "std": "std_3"},
    {"aggregate": "avg", "mean": "avg_4", "std": "std_4"},
    {"aggregate": "avg", "mean": "avg_5", "std": "std_5"},
    {"aggregate": "count", "size": "sample_size"},
]

subquery_dict = []
