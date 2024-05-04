SELECT TOP 100
  LEFT(w_warehouse_name, 20),
  sm_type,
  web_name,
  sum(
    CASE
      WHEN (ws_ship_date_sk - ws_sold_date_sk <= 30) THEN 1
      ELSE 0
    END
  ),
  sum(
    CASE
      WHEN (ws_ship_date_sk - ws_sold_date_sk > 30)
      AND (ws_ship_date_sk - ws_sold_date_sk <= 60) THEN 1
      ELSE 0
    END
  ),
  sum(
    CASE
      WHEN (ws_ship_date_sk - ws_sold_date_sk > 60)
      AND (ws_ship_date_sk - ws_sold_date_sk <= 90) THEN 1
      ELSE 0
    END
  ),
  sum(
    CASE
      WHEN (ws_ship_date_sk - ws_sold_date_sk > 90)
      AND (ws_ship_date_sk - ws_sold_date_sk <= 120) THEN 1
      ELSE 0
    END
  ),
  sum(
    CASE
      WHEN (ws_ship_date_sk - ws_sold_date_sk > 120) THEN 1
      ELSE 0
    END
  )
FROM web_sales,
  warehouse,
  ship_mode,
  web_site,
  date_dim
WHERE d_month_seq BETWEEN 1194 AND 1194 + 11
  AND ws_ship_date_sk = d_date_sk
  AND ws_warehouse_sk = w_warehouse_sk
  AND ws_ship_mode_sk = sm_ship_mode_sk
  AND ws_web_site_sk = web_site_sk
GROUP BY LEFT(w_warehouse_name, 20),
  sm_type,
  web_name
ORDER BY LEFT(w_warehouse_name, 20),
  sm_type,
  web_name;