SELECT CASE
            WHEN (
                  SELECT COUNT(*)
                  FROM store_sales
                  WHERE ss_quantity BETWEEN 1 AND 20
            ) > 2972190 THEN (
                  SELECT avg(ss_ext_sales_price)
                  FROM store_sales
                  WHERE ss_quantity BETWEEN 1 AND 20
            )
            ELSE (
                  SELECT avg(ss_net_profit)
                  FROM store_sales
                  WHERE ss_quantity BETWEEN 1 AND 20
            )
      END bucket1,
      CASE
            WHEN (
                  SELECT COUNT(*)
                  FROM store_sales
                  WHERE ss_quantity BETWEEN 21 AND 40
            ) > 4505785 THEN (
                  SELECT avg(ss_ext_sales_price)
                  FROM store_sales
                  WHERE ss_quantity BETWEEN 21 AND 40
            )
            ELSE (
                  SELECT avg(ss_net_profit)
                  FROM store_sales
                  WHERE ss_quantity BETWEEN 21 AND 40
            )
      END bucket2,
      CASE
            WHEN (
                  SELECT COUNT(*)
                  FROM store_sales
                  WHERE ss_quantity BETWEEN 41 AND 60
            ) > 1575726 THEN (
                  SELECT avg(ss_ext_sales_price)
                  FROM store_sales
                  WHERE ss_quantity BETWEEN 41 AND 60
            )
            ELSE (
                  SELECT avg(ss_net_profit)
                  FROM store_sales
                  WHERE ss_quantity BETWEEN 41 AND 60
            )
      END bucket3,
      CASE
            WHEN (
                  SELECT COUNT(*)
                  FROM store_sales
                  WHERE ss_quantity BETWEEN 61 AND 80
            ) > 3188917 THEN (
                  SELECT avg(ss_ext_sales_price)
                  FROM store_sales
                  WHERE ss_quantity BETWEEN 61 AND 80
            )
            ELSE (
                  SELECT avg(ss_net_profit)
                  FROM store_sales
                  WHERE ss_quantity BETWEEN 61 AND 80
            )
      END bucket4,
      CASE
            WHEN (
                  SELECT COUNT(*)
                  FROM store_sales
                  WHERE ss_quantity BETWEEN 81 AND 100
            ) > 3525216 THEN (
                  SELECT avg(ss_ext_sales_price)
                  FROM store_sales
                  WHERE ss_quantity BETWEEN 81 AND 100
            )
            ELSE (
                  SELECT avg(ss_net_profit)
                  FROM store_sales
                  WHERE ss_quantity BETWEEN 81 AND 100
            )
      END bucket5
FROM reason
WHERE r_reason_sk = 1;