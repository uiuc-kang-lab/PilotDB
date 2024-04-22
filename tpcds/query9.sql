SELECT CASE
    WHEN (
      SELECT COUNT(*)
      FROM store_sales
      WHERE ss_quantity BETWEEN 1 AND 20
    ) > 2972190 THEN (
      SELECT SUM(ss_ext_sales_price)
      FROM store_sales
      WHERE ss_quantity BETWEEN 1 AND 20
    )
    ELSE (
      SELECT SUM(ss_net_profit)
      FROM store_sales
      WHERE ss_quantity BETWEEN 1 AND 20
    )
  END AS bucket1,
  CASE
    WHEN (
      SELECT COUNT(*)
      FROM store_sales
      WHERE ss_quantity BETWEEN 21 AND 40
    ) > 4505785 THEN (
      SELECT SUM(ss_ext_sales_price)
      FROM store_sales
      WHERE ss_quantity BETWEEN 21 AND 40
    )
    ELSE (
      SELECT SUM(ss_net_profit)
      FROM store_sales
      WHERE ss_quantity BETWEEN 21 AND 40
    )
  END AS bucket2,
  CASE
    WHEN (
      SELECT COUNT(*)
      FROM store_sales
      WHERE ss_quantity BETWEEN 41 AND 60
    ) > 1575726 THEN (
      SELECT SUM(ss_ext_sales_price)
      FROM store_sales
      WHERE ss_quantity BETWEEN 41 AND 60
    )
    ELSE (
      SELECT SUM(ss_net_profit)
      FROM store_sales
      WHERE ss_quantity BETWEEN 41 AND 60
    )
  END AS bucket3,
  CASE
    WHEN (
      SELECT COUNT(*)
      FROM store_sales
      WHERE ss_quantity BETWEEN 61 AND 80
    ) > 3188917 THEN (
      SELECT SUM(ss_ext_sales_price)
      FROM store_sales
      WHERE ss_quantity BETWEEN 61 AND 80
    )
    ELSE (
      SELECT SUM(ss_net_profit)
      FROM store_sales
      WHERE ss_quantity BETWEEN 61 AND 80
    )
  END AS bucket4,
  CASE
    WHEN (
      SELECT COUNT(*)
      FROM store_sales
      WHERE ss_quantity BETWEEN 81 AND 100
    ) > 3525216 THEN (
      SELECT SUM(ss_ext_sales_price)
      FROM store_sales
      WHERE ss_quantity BETWEEN 81 AND 100
    )
    ELSE (
      SELECT SUM(ss_net_profit)
      FROM store_sales
      WHERE ss_quantity BETWEEN 81 AND 100
    )
  END AS bucket5,
  COUNT(*),
  'page_id_0:' || CAST(
    (CAST(CAST(None.ctid AS TEXT) AS point)) [0] AS INT
  ) AS page_id_0
FROM reason
WHERE r_reason_sk = 1
GROUP BY page_id_0