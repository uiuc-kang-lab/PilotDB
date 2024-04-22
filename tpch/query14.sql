SELECT 100.00 * SUM(
    CASE
      WHEN p_type LIKE 'PROMO%' THEN l_extendedprice * (1 - l_discount)
      ELSE 0
    END
  ) AS r0,
  SUM(l_extendedprice * (1 - l_discount)) AS r1,
  'page_id_0:' || CAST(
    (CAST(CAST(lineitem.ctid AS TEXT) AS point)) [0] AS INT
  ) AS page_id_0
FROM lineitem AS sampling_method TABLESAMPLE SYSTEM (1 ROWS),
  part
WHERE l_partkey = p_partkey
  AND l_shipdate >= CAST('1995-09-01' AS DATE)
  AND l_shipdate < CAST('1995-09-01' AS DATE) + INTERVAL '1' MONTH
GROUP BY page_id_0