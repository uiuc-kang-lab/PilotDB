SELECT channel AS r0,
  col_name AS r1,
  d_year AS r2,
  d_qoy AS r3,
  i_category AS r4,
  COUNT(*) AS r5,
  SUM(ext_sales_price) AS r6,
  page_id_0
FROM (
    SELECT 'store' AS channel,
      'ss_hdemo_sk' AS col_name,
      d_year,
      d_qoy,
      i_category,
      ss_ext_sales_price AS ext_sales_price,
      'page_id_0:' || CAST(
        (CAST(CAST(store_sales.ctid AS TEXT) AS point)) [0] AS INT
      ) AS page_id_0
    FROM store_sales TABLESAMPLE SYSTEM (1 ROWS),
      item,
      date_dim
    WHERE ss_hdemo_sk IS NULL
      AND ss_sold_date_sk = d_date_sk
      AND ss_item_sk = i_item_sk
    UNION ALL
    SELECT 'web' AS channel,
      'ws_bill_addr_sk' AS col_name,
      d_year,
      d_qoy,
      i_category,
      ws_ext_sales_price AS ext_sales_price,
      'page_id_1:' || CAST(
        (CAST(CAST(web_sales.ctid AS TEXT) AS point)) [0] AS INT
      ) AS page_id_0
    FROM web_sales TABLESAMPLE SYSTEM (1 ROWS),
      item,
      date_dim
    WHERE ws_bill_addr_sk IS NULL
      AND ws_sold_date_sk = d_date_sk
      AND ws_item_sk = i_item_sk
    UNION ALL
    SELECT 'catalog' AS channel,
      'cs_warehouse_sk' AS col_name,
      d_year,
      d_qoy,
      i_category,
      cs_ext_sales_price AS ext_sales_price,
      'page_id_2:' || CAST(
        (CAST(CAST(catalog_sales.ctid AS TEXT) AS point)) [0] AS INT
      ) AS page_id_0
    FROM catalog_sales TABLESAMPLE SYSTEM (1 ROWS),
      item,
      date_dim
    WHERE cs_warehouse_sk IS NULL
      AND cs_sold_date_sk = d_date_sk
      AND cs_item_sk = i_item_sk
  ) AS foo
GROUP BY channel,
  col_name,
  d_year,
  d_qoy,
  i_category,
  page_id_0