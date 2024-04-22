SELECT l_returnflag AS r0,
  l_linestatus AS r1,
  SUM(l_quantity) / sampling_rate AS r2,
  SUM(l_extendedprice) / sampling_rate AS r3,
  SUM(l_extendedprice * (1 - l_discount)) / sampling_rate AS r4,
  SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) / sampling_rate AS r5,
  SUM(l_quantity) AS r6,
  SUM(l_extendedprice) AS r7,
  SUM(l_discount) AS r8,
  COUNT(*) / sampling_rate AS r9,
  COUNT(*) AS r10,
  'page_id_0:' || CAST(
    (CAST(CAST(None.ctid AS TEXT) AS point)) [0] AS INT
  ) AS page_id_0
FROM lineitem AS sampling_method
WHERE l_shipdate <= CAST('1998-12-01' AS DATE) - INTERVAL '90' DAY
GROUP BY l_returnflag,
  l_linestatus,
  page_id_0