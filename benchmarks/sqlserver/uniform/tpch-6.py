pilot_query = """
WITH RandomSample AS (
    SELECT l_extendedprice,
           l_discount,
           RAND(CHECKSUM(NEWID())) AS rand_value
    FROM lineitem
    WHERE l_shipdate >= '1994-01-01'
      AND l_shipdate < DATEADD(YEAR, 1, '1994-01-01')
      AND l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
      AND l_quantity < 24
)
SELECT AVG(l_extendedprice * l_discount) AS avg_1,
       STDEV(l_extendedprice * l_discount) AS std_1,
       COUNT_BIG(*) AS sample_size
FROM RandomSample
WHERE rand_value < {sampling_method}; 
"""

sampling_query = """
SELECT SUM(l_extendedprice * l_discount) / {sample_rate} AS revenue
FROM lineitem
WHERE l_shipdate >= '1994-01-01'
  AND l_shipdate < DATEADD(YEAR, 1, '1994-01-01')
  AND l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
  AND l_quantity < 24
  AND RAND(CHECKSUM(NEWID())) < {sampling_method}
"""
results_mapping = [
    {"aggregate": "sum", "mean": "avg_1", "std": "std_1", "size": "sample_size"}
]

subquery_dict = []
