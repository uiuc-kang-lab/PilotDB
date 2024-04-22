SELECT ca_zip AS r0,
  ca_city AS r1,
  SUM(ws_sales_price) AS r2,
  'page_id_0:' || CAST(
    (CAST(CAST(web_sales.ctid AS TEXT) AS point)) [0] AS INT
  ) AS page_id_0
FROM web_sales TABLESAMPLE SYSTEM (1 ROWS),
  customer,
  customer_address,
  date_dim,
  item
WHERE ws_bill_customer_sk = c_customer_sk
  AND c_current_addr_sk = ca_address_sk
  AND ws_item_sk = i_item_sk
  AND (
    SUBSTR(ca_zip, 1, 5) IN (
      '85669',
      '86197',
      '88274',
      '83405',
      '86475',
      '85392',
      '85460',
      '80348',
      '81792'
    )
    OR i_item_id IN subquery_0
  )
  AND ws_sold_date_sk = d_date_sk
  AND d_qoy = 2
  AND d_year = 2000
GROUP BY ca_zip,
  ca_city,
  page_id_0