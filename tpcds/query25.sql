SELECT i_item_id AS r0,
  i_item_desc AS r1,
  s_store_id AS r2,
  s_store_name AS r3,
  SUM(ss_net_profit) AS r4,
  SUM(sr_net_loss) AS r5,
  SUM(cs_net_profit) AS r6,
  'page_id_0:' || CAST(
    (CAST(CAST(store_sales.ctid AS TEXT) AS point)) [0] AS INT
  ) AS page_id_0
FROM store_sales TABLESAMPLE SYSTEM (1 ROWS),
  store_returns,
  catalog_sales,
  date_dim AS d1,
  date_dim AS d2,
  date_dim AS d3,
  store,
  item
WHERE d1.d_moy = 4
  AND d1.d_year = 2000
  AND d1.d_date_sk = ss_sold_date_sk
  AND i_item_sk = ss_item_sk
  AND s_store_sk = ss_store_sk
  AND ss_customer_sk = sr_customer_sk
  AND ss_item_sk = sr_item_sk
  AND ss_ticket_number = sr_ticket_number
  AND sr_returned_date_sk = d2.d_date_sk
  AND d2.d_moy BETWEEN 4 AND 10
  AND d2.d_year = 2000
  AND sr_customer_sk = cs_bill_customer_sk
  AND sr_item_sk = cs_item_sk
  AND cs_sold_date_sk = d3.d_date_sk
  AND d3.d_moy BETWEEN 4 AND 10
  AND d3.d_year = 2000
GROUP BY i_item_id,
  i_item_desc,
  s_store_id,
  s_store_name,
  page_id_0