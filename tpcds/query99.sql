SELECT SUBSTR(w_warehouse_name, 1, 20) AS r0,
  sm_type AS r1,
  cc_name AS r2,
  SUM(
    CASE
      WHEN (cs_ship_date_sk - cs_sold_date_sk <= 30) THEN 1
      ELSE 0
    END
  ) AS r3,
  SUM(
    CASE
      WHEN (cs_ship_date_sk - cs_sold_date_sk > 30)
      AND (cs_ship_date_sk - cs_sold_date_sk <= 60) THEN 1
      ELSE 0
    END
  ) AS r4,
  SUM(
    CASE
      WHEN (cs_ship_date_sk - cs_sold_date_sk > 60)
      AND (cs_ship_date_sk - cs_sold_date_sk <= 90) THEN 1
      ELSE 0
    END
  ) AS r5,
  SUM(
    CASE
      WHEN (cs_ship_date_sk - cs_sold_date_sk > 90)
      AND (cs_ship_date_sk - cs_sold_date_sk <= 120) THEN 1
      ELSE 0
    END
  ) AS r6,
  SUM(
    CASE
      WHEN (cs_ship_date_sk - cs_sold_date_sk > 120) THEN 1
      ELSE 0
    END
  ) AS r7,
  'page_id_0:' || CAST(
    (CAST(CAST(catalog_sales.ctid AS TEXT) AS point)) [0] AS INT
  ) AS page_id_0
FROM catalog_sales TABLESAMPLE SYSTEM (1 ROWS),
  warehouse,
  ship_mode,
  call_center,
  date_dim
WHERE d_month_seq BETWEEN 1224 AND 1224 + 11
  AND cs_ship_date_sk = d_date_sk
  AND cs_warehouse_sk = w_warehouse_sk
  AND cs_ship_mode_sk = sm_ship_mode_sk
  AND cs_call_center_sk = cc_call_center_sk
GROUP BY SUBSTR(w_warehouse_name, 1, 20),
  sm_type,
  cc_name,
  page_id_0