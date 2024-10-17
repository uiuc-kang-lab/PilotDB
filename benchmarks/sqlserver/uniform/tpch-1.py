pilot_query = """
WITH RandomSample AS (
    SELECT l_returnflag,
           l_linestatus,
           l_extendedprice,
           l_discount,
           l_quantity,
           l_tax,
           l_shipdate,
           RAND(CHECKSUM(NEWID())) AS rand_value
    FROM lineitem
    WHERE l_shipdate <= DATEADD(DAY, -90, '1998-12-01')
)
SELECT AVG(l_extendedprice * (1 - l_discount)) AS avg_1,
       AVG(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS avg_2,
       AVG(l_quantity) AS avg_3,
       AVG(l_extendedprice) AS avg_4,
       AVG(l_discount) AS avg_5,
       STDEV(l_extendedprice * (1 - l_discount)) AS std_1,
       STDEV(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS std_2,
       STDEV(l_quantity) AS std_3,
       STDEV(l_extendedprice) AS std_4,
       STDEV(l_discount) AS std_5,
       COUNT_BIG(*) AS sample_size
FROM RandomSample
WHERE rand_value < {sampling_method}
GROUP BY l_returnflag,
         l_linestatus;
"""

sampling_query = """
SELECT l_returnflag,
  l_linestatus,
  SUM(l_quantity) / {sample_rate} AS sum_qty,
  SUM(l_extendedprice) / {sample_rate} AS sum_base_price,
  SUM(l_extendedprice * (1 - l_discount)) / {sample_rate} AS sum_disc_price,
  SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) / {sample_rate} AS sum_charge,
  AVG(l_quantity) AS avg_qty,
  AVG(l_extendedprice) AS avg_price,
  AVG(l_discount) AS avg_disc,
  COUNT_BIG(*) / {sample_rate} AS count_order
FROM lineitem
WHERE l_shipdate <= DATEADD(DAY, -90, '1998-12-01') 
AND RAND(CHECKSUM(NEWID())) < {sampling_method}
GROUP BY l_returnflag,
  l_linestatus
ORDER BY l_returnflag,
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
