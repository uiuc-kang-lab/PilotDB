WITH ssci AS (
  SELECT ss_customer_sk AS customer_sk,
    ss_item_sk AS item_sk,
    'page_id_0:' || CAST(
      (CAST(CAST(store_sales.ctid AS TEXT) AS point)) [0] AS INT
    ) AS page_id_0
  FROM store_sales TABLESAMPLE SYSTEM (1 ROWS),
    date_dim
  WHERE ss_sold_date_sk = d_date_sk
    AND d_month_seq BETWEEN 1214 AND 1214 + 11
  GROUP BY ss_customer_sk,
    ss_item_sk
),
csci AS (
  SELECT cs_bill_customer_sk AS customer_sk,
    cs_item_sk AS item_sk
  FROM catalog_sales,
    date_dim
  WHERE cs_sold_date_sk = d_date_sk
    AND d_month_seq BETWEEN 1214 AND 1214 + 11
  GROUP BY cs_bill_customer_sk,
    cs_item_sk
)
SELECT SUM(
    CASE
      WHEN NOT ssci.customer_sk IS NULL
      AND csci.customer_sk IS NULL THEN 1
      ELSE 0
    END
  ) AS r0,
  SUM(
    CASE
      WHEN ssci.customer_sk IS NULL
      AND NOT csci.customer_sk IS NULL THEN 1
      ELSE 0
    END
  ) AS r1,
  SUM(
    CASE
      WHEN NOT ssci.customer_sk IS NULL
      AND NOT csci.customer_sk IS NULL THEN 1
      ELSE 0
    END
  ) AS r2,
  page_id_0
FROM ssci
  FULL OUTER JOIN csci ON (
    ssci.customer_sk = csci.customer_sk
    AND ssci.item_sk = csci.item_sk
  )
GROUP BY page_id_0