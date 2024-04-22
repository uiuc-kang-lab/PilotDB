SELECT n_name AS r0,
  SUM(l_extendedprice * (1 - l_discount)) / sampling_rate AS r1,
  'page_id_0:' || CAST(
    (CAST(CAST(lineitem.ctid AS TEXT) AS point)) [0] AS INT
  ) AS page_id_0
FROM customer,
  orders,
  lineitem AS sampling_method TABLESAMPLE SYSTEM (1 ROWS),
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
  AND o_orderdate >= CAST('1994-01-01' AS DATE)
  AND o_orderdate < CAST('1994-01-01' AS DATE) + INTERVAL '1' YEAR
GROUP BY n_name,
  page_id_0