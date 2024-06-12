pilot_query = '''
WITH RandomSample AS (
    SELECT n1.n_name AS supp_nation,
           n2.n_name AS cust_nation,
           EXTRACT(YEAR FROM l_shipdate) AS l_year,
           l_extendedprice * (1 - l_discount) AS volume,
           RAND(CHECKSUM(NEWID())) AS rand_value
    FROM supplier,
         lineitem,
         orders,
         customer,
         nation n1,
         nation n2
    WHERE s_suppkey = l_suppkey
      AND o_orderkey = l_orderkey
      AND c_custkey = o_custkey
      AND s_nationkey = n1.n_nationkey
      AND c_nationkey = n2.n_nationkey
      AND (
          (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
          OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
      )
      AND l_shipdate BETWEEN '1995-01-01' AND '1996-12-31'
)
SELECT supp_nation,
       cust_nation,
       l_year,
       AVG(volume) AS avg_1,
       STDEV(volume) AS std_1,
       COUNT_BIG(*) AS sample_size
FROM RandomSample
WHERE rand_value < {sampling_method}
GROUP BY supp_nation,
         cust_nation,
         l_year
ORDER BY supp_nation,
         cust_nation,
         l_year;
'''

sampling_query ='''
SELECT supp_nation,
  cust_nation,
  l_year,
  SUM(volume) / {sample_rate} AS revenue
FROM (
    SELECT n1.n_name AS supp_nation,
      n2.n_name AS cust_nation,
      YEAR(l_shipdate) AS l_year,
      l_extendedprice * (1 - l_discount) AS volume
    FROM supplier,
      lineitem,
      orders,
      customer,
      nation AS n1,
      nation AS n2
    WHERE s_suppkey = l_suppkey
      AND o_orderkey = l_orderkey
      AND c_custkey = o_custkey
      AND s_nationkey = n1.n_nationkey
      AND c_nationkey = n2.n_nationkey
      AND (
        (
          n1.n_name = 'FRANCE'
          AND n2.n_name = 'GERMANY'
        )
        OR (
          n1.n_name = 'GERMANY'
          AND n2.n_name = 'FRANCE'
        )
      )
      AND l_shipdate BETWEEN '1995-01-01' AND '1996-12-31'
      AND RAND(CHECKSUM(NEWID())) < {sampling_method}
  ) AS shipping
GROUP BY supp_nation,
  cust_nation,
  l_year
ORDER BY supp_nation,
  cust_nation,
  l_year
'''
results_mapping = [
    {"aggregate": "sum", "mean": "avg_1", "std": "std_1", "size": "sample_size"}
]

subquery_dict = []