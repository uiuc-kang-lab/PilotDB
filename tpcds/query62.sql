SELECT SUBSTR(w_warehouse_name, 1, 20) AS r0,
  sm_type AS r1,
  web_name AS r2,
  SUM(
    CASE
      WHEN (ws_ship_date_sk - ws_sold_date_sk <= 30) THEN 1
      ELSE 0
    END
  ) AS r3,
  SUM(
    CASE
      WHEN (ws_ship_date_sk - ws_sold_date_sk > 30)
      AND (ws_ship_date_sk - ws_sold_date_sk <= 60) THEN 1
      ELSE 0
    END
  ) AS r4,
  SUM(
    CASE
      WHEN (ws_ship_date_sk - ws_sold_date_sk > 60)
      AND (ws_ship_date_sk - ws_sold_date_sk <= 90) THEN 1
      ELSE 0
    END
  ) AS r5,
  SUM(
    CASE
      WHEN (ws_ship_date_sk - ws_sold_date_sk > 90)
      AND (ws_ship_date_sk - ws_sold_date_sk <= 120) THEN 1
      ELSE 0
    END
  ) AS r6,
  SUM(
    CASE
      WHEN (ws_ship_date_sk - ws_sold_date_sk > 120) THEN 1
      ELSE 0
    END
  ) AS r7,
  'page_id_0:' || CAST(
    (CAST(CAST(web_sales.ctid AS TEXT) AS point)) [0] AS INT
  ) AS page_id_0
FROM web_sales TABLESAMPLE SYSTEM (1 ROWS),
  warehouse,
  ship_mode,
  web_site,
  date_dim
WHERE d_month_seq BETWEEN 1194 AND 1194 + 11
  AND ws_ship_date_sk = d_date_sk
  AND ws_warehouse_sk = w_warehouse_sk
  AND ws_ship_mode_sk = sm_ship_mode_sk
  AND ws_web_site_sk = web_site_sk
GROUP BY SUBSTR(w_warehouse_name, 1, 20),
  sm_type,
  web_name,
  page_id_0