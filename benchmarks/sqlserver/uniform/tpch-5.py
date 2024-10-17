pilot_query = """
select
    n_name,
    avg(l_extendedprice * (1 - l_discount)) as avg_1,
    stdev(l_extendedprice * (1 - l_discount)) as std_1,
    COUNT_BIG(*) as sample_size
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
    AND o_orderdate >= '1994-01-01'
    AND o_orderdate < DATEADD(year, 1, '1994-01-01')
    AND RAND(CHECKSUM(NEWID())) < {sampling_method}
group by
    n_name;
"""

sampling_query = """
SELECT n_name,
  SUM(l_extendedprice * (1 - l_discount)) / {sample_rate} AS revenue
FROM customer,
  orders,
  lineitem,
  supplier,
  nation,
  region
WHERE c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND l_suppkey = s_suppkey
  AND c_nationkey = s_nationkey
  AND s_nationkey = n_nationkey
  AND n_regionkey = r_regionkey
  AND r_name = 'ASIA'
  AND o_orderdate >= '1994-01-01'
  AND o_orderdate < DATEADD(year, 1, '1994-01-01')
  AND RAND(CHECKSUM(NEWID())) < {sampling_method}
GROUP BY n_name
ORDER BY revenue DESC
"""
results_mapping = [
    {"aggregate": "sum", "mean": "avg_1", "std": "std_1", "size": "sample_size"}
]

subquery_dict = []
