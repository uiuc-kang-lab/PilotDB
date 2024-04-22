SELECT i_brand_id AS r0,
  i_brand AS r1,
  i_manufact_id AS r2,
  i_manufact AS r3,
  SUM(ss_ext_sales_price) AS r4,
  'page_id_0:' || CAST(
    (CAST(CAST(store_sales.ctid AS TEXT) AS point)) [0] AS INT
  ) AS page_id_0
FROM date_dim,
  store_sales TABLESAMPLE SYSTEM (1 ROWS),
  item,
  customer,
  customer_address,
  store
WHERE d_date_sk = ss_sold_date_sk
  AND ss_item_sk = i_item_sk
  AND i_manager_id = 2
  AND d_moy = 12
  AND d_year = 1999
  AND ss_customer_sk = c_customer_sk
  AND c_current_addr_sk = ca_address_sk
  AND SUBSTR(ca_zip, 1, 5) <> SUBSTR(s_zip, 1, 5)
  AND ss_store_sk = s_store_sk
GROUP BY i_brand,
  i_brand_id,
  i_manufact_id,
  i_manufact,
  page_id_0