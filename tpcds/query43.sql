SELECT s_store_name AS r0,
  s_store_id AS r1,
  SUM(
    CASE
      WHEN (d_day_name = 'Sunday') THEN ss_sales_price
      ELSE NULL
    END
  ) AS r2,
  SUM(
    CASE
      WHEN (d_day_name = 'Monday') THEN ss_sales_price
      ELSE NULL
    END
  ) AS r3,
  SUM(
    CASE
      WHEN (d_day_name = 'Tuesday') THEN ss_sales_price
      ELSE NULL
    END
  ) AS r4,
  SUM(
    CASE
      WHEN (d_day_name = 'Wednesday') THEN ss_sales_price
      ELSE NULL
    END
  ) AS r5,
  SUM(
    CASE
      WHEN (d_day_name = 'Thursday') THEN ss_sales_price
      ELSE NULL
    END
  ) AS r6,
  SUM(
    CASE
      WHEN (d_day_name = 'Friday') THEN ss_sales_price
      ELSE NULL
    END
  ) AS r7,
  SUM(
    CASE
      WHEN (d_day_name = 'Saturday') THEN ss_sales_price
      ELSE NULL
    END
  ) AS r8,
  'page_id_0:' || CAST(
    (CAST(CAST(store_sales.ctid AS TEXT) AS point)) [0] AS INT
  ) AS page_id_0
FROM date_dim,
  store_sales TABLESAMPLE SYSTEM (1 ROWS),
  store
WHERE d_date_sk = ss_sold_date_sk
  AND s_store_sk = ss_store_sk
  AND s_gmt_offset = -5
  AND d_year = 2000
GROUP BY s_store_name,
  s_store_id,
  page_id_0