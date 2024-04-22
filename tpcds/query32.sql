SELECT SUM(cs_ext_discount_amt) AS "excess discount amount",
  'page_id_0:' || CAST(
    (CAST(CAST(catalog_sales.ctid AS TEXT) AS point)) [0] AS INT
  ) AS page_id_0
FROM catalog_sales TABLESAMPLE SYSTEM (1 ROWS),
  item,
  date_dim
WHERE i_manufact_id = 29
  AND i_item_sk = cs_item_sk
  AND d_date BETWEEN '1999-01-07' AND (CAST('1999-01-07' AS DATE) + 90 AS days)
  AND d_date_sk = cs_sold_date_sk
  AND cs_ext_discount_amt > (
    SELECT 1.3 * AVG(cs_ext_discount_amt)
    FROM catalog_sales,
      date_dim
    WHERE cs_item_sk = i_item_sk
      AND d_date BETWEEN '1999-01-07' AND (CAST('1999-01-07' AS DATE) + 90 AS days)
      AND d_date_sk = cs_sold_date_sk
  )
GROUP BY page_id_0