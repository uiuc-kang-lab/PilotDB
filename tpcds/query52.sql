SELECT dt.d_year AS r0,
  item.i_brand_id AS r1,
  item.i_brand AS r2,
  SUM(ss_ext_sales_price) AS r3,
  'page_id_0:' || CAST(
    (CAST(CAST(store_sales.ctid AS TEXT) AS point)) [0] AS INT
  ) AS page_id_0
FROM date_dim AS dt,
  store_sales TABLESAMPLE SYSTEM (1 ROWS),
  item
WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
  AND store_sales.ss_item_sk = item.i_item_sk
  AND item.i_manager_id = 1
  AND dt.d_moy = 12
  AND dt.d_year = 2002
GROUP BY dt.d_year,
  item.i_brand,
  item.i_brand_id,
  page_id_0