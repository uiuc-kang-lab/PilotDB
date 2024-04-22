SELECT i_brand_id AS brand_id,
  i_brand AS brand,
  SUM(ss_ext_sales_price) AS ext_price,
  'page_id_0:' || CAST(
    (CAST(CAST(store_sales.ctid AS TEXT) AS point)) [0] AS INT
  ) AS page_id_0
FROM date_dim,
  store_sales TABLESAMPLE SYSTEM (1 ROWS),
  item
WHERE d_date_sk = ss_sold_date_sk
  AND ss_item_sk = i_item_sk
  AND i_manager_id = 100
  AND d_moy = 12
  AND d_year = 2000
GROUP BY i_brand,
  i_brand_id,
  page_id_0