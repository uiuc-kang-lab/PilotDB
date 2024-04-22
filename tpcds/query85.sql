SELECT SUBSTR(r_reason_desc, 1, 20) AS r0,
  SUM(ws_quantity) AS r1,
  SUM(wr_refunded_cash) AS r2,
  SUM(wr_fee) AS r3,
  COUNT(*) AS r4,
  'page_id_0:' || CAST(
    (CAST(CAST(web_sales.ctid AS TEXT) AS point)) [0] AS INT
  ) AS page_id_0
FROM web_sales TABLESAMPLE SYSTEM (1 ROWS),
  web_returns,
  web_page,
  customer_demographics AS cd1,
  customer_demographics AS cd2,
  customer_address,
  date_dim,
  reason
WHERE ws_web_page_sk = wp_web_page_sk
  AND ws_item_sk = wr_item_sk
  AND ws_order_number = wr_order_number
  AND ws_sold_date_sk = d_date_sk
  AND d_year = 2000
  AND cd1.cd_demo_sk = wr_refunded_cdemo_sk
  AND cd2.cd_demo_sk = wr_returning_cdemo_sk
  AND ca_address_sk = wr_refunded_addr_sk
  AND r_reason_sk = wr_reason_sk
  AND (
    (
      cd1.cd_marital_status = 'M'
      AND cd1.cd_marital_status = cd2.cd_marital_status
      AND cd1.cd_education_status = '4 yr Degree'
      AND cd1.cd_education_status = cd2.cd_education_status
      AND ws_sales_price BETWEEN 100.00 AND 150.00
    )
    OR (
      cd1.cd_marital_status = 'S'
      AND cd1.cd_marital_status = cd2.cd_marital_status
      AND cd1.cd_education_status = 'Secondary'
      AND cd1.cd_education_status = cd2.cd_education_status
      AND ws_sales_price BETWEEN 50.00 AND 100.00
    )
    OR (
      cd1.cd_marital_status = 'W'
      AND cd1.cd_marital_status = cd2.cd_marital_status
      AND cd1.cd_education_status = 'Advanced Degree'
      AND cd1.cd_education_status = cd2.cd_education_status
      AND ws_sales_price BETWEEN 150.00 AND 200.00
    )
  )
  AND (
    (
      ca_country = 'United States'
      AND ca_state IN ('FL', 'TX', 'DE')
      AND ws_net_profit BETWEEN 100 AND 200
    )
    OR (
      ca_country = 'United States'
      AND ca_state IN ('IN', 'ND', 'ID')
      AND ws_net_profit BETWEEN 150 AND 300
    )
    OR (
      ca_country = 'United States'
      AND ca_state IN ('MT', 'IL', 'OH')
      AND ws_net_profit BETWEEN 50 AND 250
    )
  )
GROUP BY r_reason_desc,
  page_id_0