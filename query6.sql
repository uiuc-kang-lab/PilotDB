SELECT SUM(l_extendedprice * l_discount) / sampling_rate AS revenue,
  'page_id_0:' || CAST(
    (CAST(CAST(lineitem.ctid AS TEXT) AS point)) [0] AS INT
  ) AS page_id_0
FROM lineitem AS sampling_method TABLESAMPLE SYSTEM (1 ROWS)
WHERE l_shipdate >= CAST('1994-01-01' AS DATE)
  AND l_shipdate < CAST('1994-01-01' AS DATE) + INTERVAL '1' YEAR
  AND l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
  AND l_quantity < 24
GROUP BY page_id_0