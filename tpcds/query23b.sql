SELECT c_last_name AS r0,
  c_first_name AS r1,
  sales AS r2,
  page_id_0
FROM (
    SELECT c_last_name,
      c_first_name,
      SUM(cs_quantity * cs_list_price) AS sales,
      'page_id_0:' || CAST(
        (CAST(CAST(catalog_sales.ctid AS TEXT) AS point)) [0] AS INT
      ) AS page_id_0
    FROM catalog_sales TABLESAMPLE SYSTEM (1 ROWS),
      customer,
      date_dim
    WHERE d_year = 2000
      AND d_moy = 5
      AND cs_sold_date_sk = d_date_sk
      AND cs_item_sk IN subquery_1
      AND cs_bill_customer_sk IN subquery_0
      AND cs_bill_customer_sk = c_customer_sk
    GROUP BY c_last_name,
      c_first_name,
      page_id_0
    UNION ALL
    SELECT c_last_name,
      c_first_name,
      SUM(ws_quantity * ws_list_price) AS sales,
      'page_id_1:' || CAST(
        (CAST(CAST(web_sales.ctid AS TEXT) AS point)) [0] AS INT
      ) AS page_id_0
    FROM web_sales TABLESAMPLE SYSTEM (1 ROWS),
      customer,
      date_dim
    WHERE d_year = 2000
      AND d_moy = 5
      AND ws_sold_date_sk = d_date_sk
      AND ws_item_sk IN subquery_1
      AND ws_bill_customer_sk IN subquery_0
      AND ws_bill_customer_sk = c_customer_sk
    GROUP BY c_last_name,
      c_first_name,
      page_id_0
  )