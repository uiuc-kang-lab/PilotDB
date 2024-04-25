SELECT channel AS r0,
  i_brand_id AS r1,
  i_class_id AS r2,
  i_category_id AS r3,
  SUM(sales) AS r4,
  SUM(number_sales) AS r5,
  page_id_0
FROM (
    SELECT 'store' AS channel,
      i_brand_id,
      i_class_id,
      i_category_id,
      SUM(ss_quantity * ss_list_price) / sampling_rate AS sales,
      COUNT(*) / sampling_rate AS number_sales,
      'page_id_0:' || CAST(
        (CAST(CAST(store_sales.ctid AS TEXT) AS point)) [0] AS INT
      ) AS page_id_0
    FROM store_sales AS sampling_method TABLESAMPLE SYSTEM (1 ROWS),
      item,
      date_dim
    WHERE ss_item_sk IN subquery_0
      AND ss_item_sk = i_item_sk
      AND ss_sold_date_sk = d_date_sk
      AND d_year = 2000 + 2
      AND d_moy = 11
    GROUP BY i_brand_id,
      i_class_id,
      i_category_id,
      page_id_0
    UNION ALL
    SELECT 'catalog' AS channel,
      i_brand_id,
      i_class_id,
      i_category_id,
      SUM(cs_quantity * cs_list_price) / sampling_rate AS sales,
      COUNT(*) / sampling_rate AS number_sales,
      'page_id_1:' || CAST(
        (CAST(CAST(catalog_sales.ctid AS TEXT) AS point)) [0] AS INT
      ) AS page_id_0
    FROM catalog_sales AS sampling_method TABLESAMPLE SYSTEM (1 ROWS),
      item,
      date_dim
    WHERE cs_item_sk IN subquery_0
      AND cs_item_sk = i_item_sk
      AND cs_sold_date_sk = d_date_sk
      AND d_year = 2000 + 2
      AND d_moy = 11
    GROUP BY i_brand_id,
      i_class_id,
      i_category_id,
      page_id_0
    UNION ALL
    SELECT 'web' AS channel,
      i_brand_id,
      i_class_id,
      i_category_id,
      SUM(ws_quantity * ws_list_price) / sampling_rate AS sales,
      COUNT(*) / sampling_rate AS number_sales,
      'page_id_2:' || CAST(
        (CAST(CAST(web_sales.ctid AS TEXT) AS point)) [0] AS INT
      ) AS page_id_0
    FROM web_sales AS sampling_method TABLESAMPLE SYSTEM (1 ROWS),
      item,
      date_dim
    WHERE ws_item_sk IN subquery_0
      AND ws_item_sk = i_item_sk
      AND ws_sold_date_sk = d_date_sk
      AND d_year = 2000 + 2
      AND d_moy = 11
    GROUP BY i_brand_id,
      i_class_id,
      i_category_id,
      page_id_0
  ) AS y
GROUP BY ROLLUP (
    channel,
    i_brand_id,
    i_class_id,
    i_category_id,
    page_id_0
  )