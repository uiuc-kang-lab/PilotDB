SELECT this_year.channel AS r0,
  this_year.i_brand_id AS r1,
  this_year.i_class_id AS r2,
  this_year.i_category_id AS r3,
  this_year.sales AS r4,
  this_year.number_sales AS r5,
  last_year.channel AS r6,
  last_year.i_brand_id AS r7,
  last_year.i_class_id AS r8,
  last_year.i_category_id AS r9,
  last_year.sales AS r10,
  last_year.number_sales AS r11,
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
    WHERE ss_item_sk IN subquery_1
      AND ss_item_sk = i_item_sk
      AND ss_sold_date_sk = d_date_sk
      AND d_week_seq = subquery_0
    GROUP BY i_brand_id,
      i_class_id,
      i_category_id,
      page_id_0
  ) AS this_year,
  (
    SELECT 'store' AS channel,
      i_brand_id,
      i_class_id,
      i_category_id,
      SUM(ss_quantity * ss_list_price) / sampling_rate AS sales,
      COUNT(*) / sampling_rate AS number_sales,
      'page_id_1:' || CAST(
        (CAST(CAST(store_sales.ctid AS TEXT) AS point)) [0] AS INT
      ) AS page_id_0
    FROM store_sales AS sampling_method TABLESAMPLE SYSTEM (1 ROWS),
      item,
      date_dim
    WHERE ss_item_sk IN subquery_1
      AND ss_item_sk = i_item_sk
      AND ss_sold_date_sk = d_date_sk
      AND d_week_seq = subquery_2
    GROUP BY i_brand_id,
      i_class_id,
      i_category_id,
      page_id_0
  ) AS last_year
WHERE this_year.i_brand_id = last_year.i_brand_id
  AND this_year.i_class_id = last_year.i_class_id
  AND this_year.i_category_id = last_year.i_category_id