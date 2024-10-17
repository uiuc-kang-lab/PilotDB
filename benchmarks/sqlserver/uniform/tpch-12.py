pilot_query = """
select
    l_shipmode,
    avg(case
        when o_orderpriority = '1-URGENT'
            or o_orderpriority = '2-HIGH'
            then 1
        else 0
    end) as avg_1,
    avg(case
        when o_orderpriority <> '1-URGENT'
            and o_orderpriority <> '2-HIGH'
            then 1
        else 0
    end) as avg_2,
    stddev(case
        when o_orderpriority = '1-URGENT'
            or o_orderpriority = '2-HIGH'
            then 1
        else 0
    end) as std_1,
    stddev(case
        when o_orderpriority <> '1-URGENT'
            and o_orderpriority <> '2-HIGH'
            then 1
        else 0
    end) as std_2,
    COUNT(*) as sample_size
from
    orders,
    lineitem {sampling_method}
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


sampling_query = """
SELECT l_shipmode,
  SUM(
    CASE
      WHEN o_orderpriority = '1-URGENT'
      OR o_orderpriority = '2-HIGH' THEN 1
      ELSE 0
    END
  ) / {sample_rate} AS high_line_count,
  SUM(
    CASE
      WHEN o_orderpriority <> '1-URGENT'
      AND o_orderpriority <> '2-HIGH' THEN 1
      ELSE 0
    END
  ) / {sample_rate} AS low_line_count
FROM orders,
  lineitem
WHERE o_orderkey = l_orderkey
  AND l_shipmode IN ('MAIL', 'SHIP')
  AND l_commitdate < l_receiptdate
  AND l_shipdate < l_commitdate
  AND l_receiptdate >= '1994-01-01'
  AND l_receiptdate < DATEADD(YEAR, 1, '1994-01-01')
  AND {sampling_method}
GROUP BY l_shipmode
ORDER BY l_shipmode
"""
results_mapping = [
    {"aggregate": "sum", "mean": "avg_1", "std": "std_1", "size": "sample_size"},
    {"aggregate": "sum", "mean": "avg_2", "std": "std_2", "size": "sample_size"},
]

subquery_dict = []
