SELECT SUM(ss_quantity) AS r0,
  SUM(ss_ext_sales_price) AS r1,
  SUM(ss_ext_wholesale_cost) AS r2,
  SUM(ss_ext_wholesale_cost) AS r3,
  COUNT(*) AS r4,
  'page_id_0:' || CAST(
    (CAST(CAST(store_sales.ctid AS TEXT) AS point)) [0] AS INT
  ) AS page_id_0
FROM store_sales TABLESAMPLE SYSTEM (1 ROWS),
  store,
  customer_demographics,
  household_demographics,
  customer_address,
  date_dim
WHERE s_store_sk = ss_store_sk
  AND ss_sold_date_sk = d_date_sk
  AND d_year = 2001
  AND (
    (
      ss_hdemo_sk = hd_demo_sk
      AND cd_demo_sk = ss_cdemo_sk
      AND cd_marital_status = 'D'
      AND cd_education_status = 'Unknown'
      AND ss_sales_price BETWEEN 100.00 AND 150.00
      AND hd_dep_count = 3
    )
    OR (
      ss_hdemo_sk = hd_demo_sk
      AND cd_demo_sk = ss_cdemo_sk
      AND cd_marital_status = 'S'
      AND cd_education_status = 'College'
      AND ss_sales_price BETWEEN 50.00 AND 100.00
      AND hd_dep_count = 1
    )
    OR (
      ss_hdemo_sk = hd_demo_sk
      AND cd_demo_sk = ss_cdemo_sk
      AND cd_marital_status = 'M'
      AND cd_education_status = '4 yr Degree'
      AND ss_sales_price BETWEEN 150.00 AND 200.00
      AND hd_dep_count = 1
    )
  )
  AND (
    (
      ss_addr_sk = ca_address_sk
      AND ca_country = 'United States'
      AND ca_state IN ('SD', 'KS', 'MI')
      AND ss_net_profit BETWEEN 100 AND 200
    )
    OR (
      ss_addr_sk = ca_address_sk
      AND ca_country = 'United States'
      AND ca_state IN ('MO', 'ND', 'CO')
      AND ss_net_profit BETWEEN 150 AND 300
    )
    OR (
      ss_addr_sk = ca_address_sk
      AND ca_country = 'United States'
      AND ca_state IN ('NH', 'OH', 'TX')
      AND ss_net_profit BETWEEN 50 AND 250
    )
  )
GROUP BY page_id_0