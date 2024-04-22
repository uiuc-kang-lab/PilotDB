SELECT l_returnflag,
  l_linestatus,
  SUM(l_quantity) / sampling_rate AS sum_qty,
  SUM(l_extendedprice) / sampling_rate AS sum_base_price,
  SUM(l_extendedprice * (1 - l_discount)) / sampling_rate AS sum_disc_price,
  SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) / sampling_rate AS sum_charge,
  SUM(l_quantity) AS avg_qty,
  SUM(l_extendedprice) AS avg_price,
  SUM(l_discount) AS avg_disc,
  COUNT(*) / sampling_rate AS count_order,
  COUNT(*),
  'page_id_0:' || CAST(
    (CAST(CAST(lineitem.ctid AS TEXT) AS point)) [0] AS INT
  ) AS page_id_0
FROM lineitem AS sampling_method TABLESAMPLE SYSTEM (1 ROWS)
WHERE l_shipdate <= CAST('1998-12-01' AS DATE) - INTERVAL '90' DAY
GROUP BY l_returnflag,
  l_linestatus,
  page_id_0