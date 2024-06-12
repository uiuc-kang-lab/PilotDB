pilot_query = '''
WITH RandomSample AS (
    SELECT EXTRACT(YEAR FROM o_orderdate) AS o_year,
           l_extendedprice * (1 - l_discount) AS volume,
           n2.n_name AS nation,
           RAND(CHECKSUM(NEWID())) AS rand_value
    FROM part,
         supplier,
         lineitem,
         orders,
         customer,
         nation n1,
         nation n2,
         region
    WHERE p_partkey = l_partkey
      AND s_suppkey = l_suppkey
      AND l_orderkey = o_orderkey
      AND o_custkey = c_custkey
      AND c_nationkey = n1.n_nationkey
      AND n1.n_regionkey = r_regionkey
      AND r_name = 'AMERICA'
      AND s_nationkey = n2.n_nationkey
      AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
      AND p_type = 'ECONOMY ANODIZED STEEL'
)
SELECT o_year,
       AVG(CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0 END) AS avg_1,
       STDEV(CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0 END) AS std_1,
       AVG(volume) AS avg_2,
       STDEV(volume) AS std_2,
       COUNT(*) AS sample_size
FROM RandomSample
WHERE rand_value < {sampling_method}
GROUP BY o_year
ORDER BY o_year;
'''

sampling_query ='''
SELECT o_year,
  SUM(
    CASE
      WHEN nation = 'BRAZIL' THEN volume
      ELSE 0
    END
  ) / SUM(volume) AS mkt_share
FROM (
    SELECT YEAR(o_orderdate) AS o_year,
      l_extendedprice * (1 - l_discount) AS volume,
      n2.n_name AS nation
    FROM part,
      supplier,
      lineitem,
      orders,
      customer,
      nation AS n1,
      nation AS n2,
      region
    WHERE p_partkey = l_partkey
      AND s_suppkey = l_suppkey
      AND l_orderkey = o_orderkey
      AND o_custkey = c_custkey
      AND c_nationkey = n1.n_nationkey
      AND n1.n_regionkey = r_regionkey
      AND r_name = 'AMERICA'
      AND s_nationkey = n2.n_nationkey
      AND o_orderdate BETWEEN '1995-01-01' AND '1996-12-31'
      AND p_type = 'ECONOMY ANODIZED STEEL'
      AND RAND(CHECKSUM(NEWID())) < {sampling_method}
  ) AS all_nations
GROUP BY o_year
ORDER BY o_year;
'''
results_mapping = [
    {"aggregate": "div", "first_element": "avg_1", "second_element": "avg_2", "size": "sample_size"}
]

subquery_dict = []