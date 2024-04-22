SELECT o_orderpriority AS r0,
  COUNT(*) / sampling_rate AS r1,
  'page_id_0:' || CAST(
    (CAST(CAST(orders.ctid AS TEXT) AS point)) [0] AS INT
  ) AS page_id_0
FROM orders AS sampling_method TABLESAMPLE SYSTEM (1 ROWS)
WHERE o_orderdate >= CAST('1993-07-01' AS DATE)
  AND o_orderdate < CAST('1993-07-01' AS DATE) + INTERVAL '3' MONTH
  AND EXISTS(
    SELECT *
    FROM lineitem
    WHERE l_orderkey = o_orderkey
      AND l_commitdate < l_receiptdate
  )
GROUP BY o_orderpriority,
  page_id_0