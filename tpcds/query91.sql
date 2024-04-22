SELECT cc_call_center_id AS r0,
  cc_name AS r1,
  cc_manager AS r2,
  SUM(cr_net_loss) AS r3,
  'page_id_0:' || CAST(
    (
      CAST(
        CAST(customer_demographics.ctid AS TEXT) AS point
      )
    ) [0] AS INT
  ) AS page_id_0
FROM call_center,
  catalog_returns,
  date_dim,
  customer,
  customer_address,
  customer_demographics TABLESAMPLE SYSTEM (1 ROWS),
  household_demographics
WHERE cr_call_center_sk = cc_call_center_sk
  AND cr_returned_date_sk = d_date_sk
  AND cr_returning_customer_sk = c_customer_sk
  AND cd_demo_sk = c_current_cdemo_sk
  AND hd_demo_sk = c_current_hdemo_sk
  AND ca_address_sk = c_current_addr_sk
  AND d_year = 2001
  AND d_moy = 11
  AND (
    (
      cd_marital_status = 'M'
      AND cd_education_status = 'Unknown'
    )
    OR (
      cd_marital_status = 'W'
      AND cd_education_status = 'Advanced Degree'
    )
  )
  AND hd_buy_potential LIKE '1001-5000%'
  AND ca_gmt_offset = -6
GROUP BY cc_call_center_id,
  cc_name,
  cc_manager,
  cd_marital_status,
  cd_education_status,
  page_id_0