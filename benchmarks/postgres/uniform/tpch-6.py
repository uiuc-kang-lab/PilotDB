pilot_query = '''
select
    avg(l_extendedprice * l_discount) as avg_1,
    stddev(l_extendedprice * l_discount) as std_1,
    COUNT(*) as sample_size
from
    lineitem {sampling_method}
where
    l_shipdate >= date '1994-01-01'
    and l_shipdate < date '1994-01-01' + interval '1' year
    and l_discount between 0.06 - 0.01 and 0.06 + 0.01
    and l_quantity < 24;
'''

results_mapping = [
    {"aggregate": "sum", "mean": "avg_1", "std": "std_1", "size": "sample_size"}
]

subquery_dict = []