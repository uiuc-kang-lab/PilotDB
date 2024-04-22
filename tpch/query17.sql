SELECT SUM(l_extendedprice) / 7.0 / sampling_rate AS r0,
  'page_id_0:' || CAST(
    (CAST(CAST(lineitem.ctid AS TEXT) AS point)) [0] AS INT
  ) AS page_id_0
FROM lineitem AS sampling_method TABLESAMPLE SYSTEM (1 ROWS),
  part
WHERE p_partkey = l_partkey
  AND p_brand = 'Brand#23'
  AND p_container = 'MED BOX'
  AND l_quantity < (
    SELECT 0.2 * AVG(l_quantity)
    FROM lineitem
    WHERE l_partkey = p_partkey
  )
GROUP BY page_id_0