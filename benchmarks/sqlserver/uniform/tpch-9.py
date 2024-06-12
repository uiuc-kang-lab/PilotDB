pilot_query = '''
select
    nation,
    o_year,
    avg(amount) as avg_1,
    stddev(amount) as std_1,
    COUNT(*) as sample_size
from
    (
        select
            n_name as nation,
            extract(year from o_orderdate) as o_year,
            l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
        from
            part,
            supplier,
            lineitem {sampling_method},
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
'''

# sampling_query = '''
# SELECT nation,
#   o_year,
#   SUM(amount) / {sample_rate} AS sum_profit
# FROM (
#     SELECT n_name AS nation,
#       YEAR(o_orderdate) AS o_year,
#       l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
#     FROM part,
#       supplier,
#       lineitem,
#       partsupp,
#       orders,
#       nation
#     WHERE s_suppkey = l_suppkey
#       AND ps_suppkey = l_suppkey
#       AND ps_partkey = l_partkey
#       AND p_partkey = l_partkey
#       AND o_orderkey = l_orderkey
#       AND s_nationkey = n_nationkey
#       AND p_name LIKE '%green%'
#   ) AS profit
# WHERE {sampling_method}
# GROUP BY nation,
#   o_year
# ORDER BY nation,
#   o_year DESC
# '''

sampling_query = '''
SELECT n_name AS nation,
  YEAR(o_orderdate) AS o_year,
  SUM(l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity) / {sample_rate} AS sum_profit
FROM part,
  supplier,
  lineitem,
  partsupp,
  orders,
  nation
WHERE s_suppkey = l_suppkey
  AND ps_suppkey = l_suppkey
  AND ps_partkey = l_partkey
  AND p_partkey = l_partkey
  AND o_orderkey = l_orderkey
  AND s_nationkey = n_nationkey
  AND p_name LIKE '%green%'
AND {sampling_method}
GROUP BY n_name,
  YEAR(o_orderdate)
ORDER BY n_name,
  YEAR(o_orderdate) DESC
'''

results_mapping = [
    {"aggregate": "sum", "mean": "avg_1", "std": "std_1", "size": "sample_size"}
]

subquery_dict = []