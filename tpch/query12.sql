SELECT l_shipmode AS r0,
  SUM(
    CASE
      WHEN o_orderpriority = '1-URGENT'
      OR o_orderpriority = '2-HIGH' THEN 1
      ELSE 0
    END
  ) / sampling_rate AS r1,
  SUM(
    CASE
      WHEN o_orderpriority <> '1-URGENT'
      AND o_orderpriority <> '2-HIGH' THEN 1
      ELSE 0
    END
  ) / sampling_rate AS r2,
  'page_id_0:' || CAST(
    (CAST(CAST(lineitem.ctid AS TEXT) AS point)) [0] AS INT
  ) AS page_id_0
FROM orders,
  lineitem AS sampling_method TABLESAMPLE SYSTEM (1 ROWS)
WHERE o_orderkey = l_orderkey
  AND l_shipmode IN ('MAIL', 'SHIP')
  AND l_commitdate < l_receiptdate
  AND l_shipdate < l_commitdate
  AND l_receiptdate >= CAST('1994-01-01' AS DATE)
  AND l_receiptdate < CAST('1994-01-01' AS DATE) + INTERVAL '1' YEAR
GROUP BY l_shipmode,
  page_id_0