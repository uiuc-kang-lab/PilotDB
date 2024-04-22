SELECT cd_gender AS r0,
  cd_marital_status AS r1,
  cd_education_status AS r2,
  COUNT(*) AS r3,
  cd_purchase_estimate AS r4,
  COUNT(*) AS r5,
  cd_credit_rating AS r6,
  COUNT(*) AS r7,
  cd_dep_count AS r8,
  COUNT(*) AS r9,
  cd_dep_employed_count AS r10,
  COUNT(*) AS r11,
  cd_dep_college_count AS r12,
  COUNT(*) AS r13,
  'page_id_0:' || CAST(
    (
      CAST(
        CAST(customer_demographics.ctid AS TEXT) AS point
      )
    ) [0] AS INT
  ) AS page_id_0
FROM customer AS c,
  customer_address AS ca,
  customer_demographics TABLESAMPLE SYSTEM (1 ROWS)
WHERE c.c_current_addr_sk = ca.ca_address_sk
  AND ca_county IN (
    'Storey County',
    'Marquette County',
    'Warren County',
    'Cochran County',
    'Kandiyohi County'
  )
  AND cd_demo_sk = c.c_current_cdemo_sk
  AND EXISTS(
    SELECT *
    FROM store_sales,
      date_dim
    WHERE c.c_customer_sk = ss_customer_sk
      AND ss_sold_date_sk = d_date_sk
      AND d_year = 2001
      AND d_moy BETWEEN 1 AND 1 + 3
  )
  AND (
    EXISTS(
      SELECT *
      FROM web_sales,
        date_dim
      WHERE c.c_customer_sk = ws_bill_customer_sk
        AND ws_sold_date_sk = d_date_sk
        AND d_year = 2001
        AND d_moy BETWEEN 1 AND 1 + 3
    )
    OR EXISTS(
      SELECT *
      FROM catalog_sales,
        date_dim
      WHERE c.c_customer_sk = cs_ship_customer_sk
        AND cs_sold_date_sk = d_date_sk
        AND d_year = 2001
        AND d_moy BETWEEN 1 AND 1 + 3
    )
  )
GROUP BY cd_gender,
  cd_marital_status,
  cd_education_status,
  cd_purchase_estimate,
  cd_credit_rating,
  cd_dep_count,
  cd_dep_employed_count,
  cd_dep_college_count,
  page_id_0