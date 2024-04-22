WITH wscs AS (
  SELECT sold_date_sk,
    sales_price,
    page_id_0
  FROM (
      SELECT ws_sold_date_sk AS sold_date_sk,
        ws_ext_sales_price AS sales_price,
        'page_id_0:' || CAST(
          (CAST(CAST(web_sales.ctid AS TEXT) AS point)) [0] AS INT
        ) AS page_id_0
      FROM web_sales TABLESAMPLE SYSTEM (1 ROWS)
      UNION ALL
      SELECT cs_sold_date_sk AS sold_date_sk,
        cs_ext_sales_price AS sales_price,
        'page_id_1:' || CAST(
          (CAST(CAST(catalog_sales.ctid AS TEXT) AS point)) [0] AS INT
        ) AS page_id_0
      FROM catalog_sales TABLESAMPLE SYSTEM (1 ROWS)
    )
),
wswscs AS (
  SELECT d_week_seq,
    SUM(
      CASE
        WHEN (d_day_name = 'Sunday') THEN sales_price
        ELSE NULL
      END
    ) AS sun_sales,
    SUM(
      CASE
        WHEN (d_day_name = 'Monday') THEN sales_price
        ELSE NULL
      END
    ) AS mon_sales,
    SUM(
      CASE
        WHEN (d_day_name = 'Tuesday') THEN sales_price
        ELSE NULL
      END
    ) AS tue_sales,
    SUM(
      CASE
        WHEN (d_day_name = 'Wednesday') THEN sales_price
        ELSE NULL
      END
    ) AS wed_sales,
    SUM(
      CASE
        WHEN (d_day_name = 'Thursday') THEN sales_price
        ELSE NULL
      END
    ) AS thu_sales,
    SUM(
      CASE
        WHEN (d_day_name = 'Friday') THEN sales_price
        ELSE NULL
      END
    ) AS fri_sales,
    SUM(
      CASE
        WHEN (d_day_name = 'Saturday') THEN sales_price
        ELSE NULL
      END
    ) AS sat_sales,
    page_id_0
  FROM wscs,
    date_dim
  WHERE d_date_sk = sold_date_sk
  GROUP BY d_week_seq,
    page_id_0
)
SELECT d_week_seq1 AS r0,
  ROUND(sun_sales1 / sun_sales2, 2) AS r1,
  ROUND(mon_sales1 / mon_sales2, 2) AS r2,
  ROUND(tue_sales1 / tue_sales2, 2) AS r3,
  ROUND(wed_sales1 / wed_sales2, 2) AS r4,
  ROUND(thu_sales1 / thu_sales2, 2) AS r5,
  ROUND(fri_sales1 / fri_sales2, 2) AS r6,
  ROUND(sat_sales1 / sat_sales2, 2) AS r7,
  page_id_0
FROM (
    SELECT wswscs.d_week_seq AS d_week_seq1,
      sun_sales AS sun_sales1,
      mon_sales AS mon_sales1,
      tue_sales AS tue_sales1,
      wed_sales AS wed_sales1,
      thu_sales AS thu_sales1,
      fri_sales AS fri_sales1,
      sat_sales AS sat_sales1,
      page_id_0
    FROM wswscs,
      date_dim
    WHERE date_dim.d_week_seq = wswscs.d_week_seq
      AND d_year = 1998
  ) AS y,
  (
    SELECT wswscs.d_week_seq AS d_week_seq2,
      sun_sales AS sun_sales2,
      mon_sales AS mon_sales2,
      tue_sales AS tue_sales2,
      wed_sales AS wed_sales2,
      thu_sales AS thu_sales2,
      fri_sales AS fri_sales2,
      sat_sales AS sat_sales2,
      page_id_0
    FROM wswscs,
      date_dim
    WHERE date_dim.d_week_seq = wswscs.d_week_seq
      AND d_year = 1998 + 1
  ) AS z
WHERE d_week_seq1 = d_week_seq2 - 53