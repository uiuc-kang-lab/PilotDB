SELECT SUM(ws_ext_discount_amt) AS r0,
  'page_id_0:' || CAST(
    (CAST(CAST(web_sales.ctid AS TEXT) AS point)) [0] AS INT
  ) AS page_id_0
FROM web_sales TABLESAMPLE SYSTEM (1 ROWS),
  item,
  date_dim
WHERE i_manufact_id = 320
  AND i_item_sk = ws_item_sk
  AND d_date BETWEEN '2002-02-26' AND (CAST('2002-02-26' AS DATE) + 90 AS days)
  AND d_date_sk = ws_sold_date_sk
  AND ws_ext_discount_amt > (
    SELECT 1.3 * AVG(ws_ext_discount_amt)
    FROM web_sales,
      date_dim
    WHERE ws_item_sk = i_item_sk
      AND d_date BETWEEN '2002-02-26' AND (CAST('2002-02-26' AS DATE) + 90 AS days)
      AND d_date_sk = ws_sold_date_sk
  )
GROUP BY page_id_0