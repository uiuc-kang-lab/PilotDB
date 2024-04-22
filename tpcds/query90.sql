SELECT CAST(amc AS DECIMAL(15, 4)) AS r0,
  CAST(pmc AS DECIMAL(15, 4)) AS r1,
  page_id_0,
  page_id_1
FROM (
    SELECT COUNT(*) AS amc,
      'page_id_0:' || CAST(
        (CAST(CAST(web_sales.ctid AS TEXT) AS point)) [0] AS INT
      ) AS page_id_0
    FROM web_sales TABLESAMPLE SYSTEM (1 ROWS),
      household_demographics,
      time_dim,
      web_page
    WHERE ws_sold_time_sk = time_dim.t_time_sk
      AND ws_ship_hdemo_sk = household_demographics.hd_demo_sk
      AND ws_web_page_sk = web_page.wp_web_page_sk
      AND time_dim.t_hour BETWEEN 10 AND 10 + 1
      AND household_demographics.hd_dep_count = 2
      AND web_page.wp_char_count BETWEEN 5000 AND 5200
    GROUP BY page_id_0
  ) AS at,
  (
    SELECT COUNT(*) AS pmc,
      'page_id_1:' || CAST(
        (CAST(CAST(web_sales.ctid AS TEXT) AS point)) [0] AS INT
      ) AS page_id_1
    FROM web_sales TABLESAMPLE SYSTEM (1 ROWS),
      household_demographics,
      time_dim,
      web_page
    WHERE ws_sold_time_sk = time_dim.t_time_sk
      AND ws_ship_hdemo_sk = household_demographics.hd_demo_sk
      AND ws_web_page_sk = web_page.wp_web_page_sk
      AND time_dim.t_hour BETWEEN 16 AND 16 + 1
      AND household_demographics.hd_dep_count = 2
      AND web_page.wp_char_count BETWEEN 5000 AND 5200
    GROUP BY page_id_1
  ) AS pt