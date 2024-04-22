SELECT s_store_name AS r0,
  s_company_id AS r1,
  s_street_number AS r2,
  s_street_name AS r3,
  s_street_type AS r4,
  s_suite_number AS r5,
  s_city AS r6,
  s_county AS r7,
  s_state AS r8,
  s_zip AS r9,
  SUM(
    CASE
      WHEN (sr_returned_date_sk - ss_sold_date_sk <= 30) THEN 1
      ELSE 0
    END
  ) AS r10,
  SUM(
    CASE
      WHEN (sr_returned_date_sk - ss_sold_date_sk > 30)
      AND (sr_returned_date_sk - ss_sold_date_sk <= 60) THEN 1
      ELSE 0
    END
  ) AS r11,
  SUM(
    CASE
      WHEN (sr_returned_date_sk - ss_sold_date_sk > 60)
      AND (sr_returned_date_sk - ss_sold_date_sk <= 90) THEN 1
      ELSE 0
    END
  ) AS r12,
  SUM(
    CASE
      WHEN (sr_returned_date_sk - ss_sold_date_sk > 90)
      AND (sr_returned_date_sk - ss_sold_date_sk <= 120) THEN 1
      ELSE 0
    END
  ) AS r13,
  SUM(
    CASE
      WHEN (sr_returned_date_sk - ss_sold_date_sk > 120) THEN 1
      ELSE 0
    END
  ) AS r14,
  'page_id_0:' || CAST(
    (CAST(CAST(store_sales.ctid AS TEXT) AS point)) [0] AS INT
  ) AS page_id_0
FROM store_sales TABLESAMPLE SYSTEM (1 ROWS),
  store_returns,
  store,
  date_dim AS d1,
  date_dim AS d2
WHERE d2.d_year = 2001
  AND d2.d_moy = 8
  AND ss_ticket_number = sr_ticket_number
  AND ss_item_sk = sr_item_sk
  AND ss_sold_date_sk = d1.d_date_sk
  AND sr_returned_date_sk = d2.d_date_sk
  AND ss_customer_sk = sr_customer_sk
  AND ss_store_sk = s_store_sk
GROUP BY s_store_name,
  s_company_id,
  s_street_number,
  s_street_name,
  s_street_type,
  s_suite_number,
  s_city,
  s_county,
  s_state,
  s_zip,
  page_id_0