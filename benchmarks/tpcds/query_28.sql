SELECT *
FROM (
    SELECT avg(ss_list_price) B1_LP,
      COUNT(ss_list_price) B1_CNT,
      COUNT(DISTINCT ss_list_price) B1_CNTD
    FROM store_sales
    WHERE ss_quantity BETWEEN 0 AND 5
      AND (
        ss_list_price BETWEEN 131 AND 131 + 10
        OR ss_coupon_amt BETWEEN 16798 AND 16798 + 1000
        OR ss_wholesale_cost BETWEEN 25 AND 25 + 20
      )
  ) B1,
  (
    SELECT avg(ss_list_price) B2_LP,
      COUNT(ss_list_price) B2_CNT,
      COUNT(DISTINCT ss_list_price) B2_CNTD
    FROM store_sales
    WHERE ss_quantity BETWEEN 6 AND 10
      AND (
        ss_list_price BETWEEN 145 AND 145 + 10
        OR ss_coupon_amt BETWEEN 14792 AND 14792 + 1000
        OR ss_wholesale_cost BETWEEN 46 AND 46 + 20
      )
  ) B2,
  (
    SELECT avg(ss_list_price) B3_LP,
      COUNT(ss_list_price) B3_CNT,
      COUNT(DISTINCT ss_list_price) B3_CNTD
    FROM store_sales
    WHERE ss_quantity BETWEEN 11 AND 15
      AND (
        ss_list_price BETWEEN 150 AND 150 + 10
        OR ss_coupon_amt BETWEEN 6600 AND 6600 + 1000
        OR ss_wholesale_cost BETWEEN 9 AND 9 + 20
      )
  ) B3,
  (
    SELECT avg(ss_list_price) B4_LP,
      COUNT(ss_list_price) B4_CNT,
      COUNT(DISTINCT ss_list_price) B4_CNTD
    FROM store_sales
    WHERE ss_quantity BETWEEN 16 AND 20
      AND (
        ss_list_price BETWEEN 91 AND 91 + 10
        OR ss_coupon_amt BETWEEN 13493 AND 13493 + 1000
        OR ss_wholesale_cost BETWEEN 36 AND 36 + 20
      )
  ) B4,
  (
    SELECT avg(ss_list_price) B5_LP,
      COUNT(ss_list_price) B5_CNT,
      COUNT(DISTINCT ss_list_price) B5_CNTD
    FROM store_sales
    WHERE ss_quantity BETWEEN 21 AND 25
      AND (
        ss_list_price BETWEEN 0 AND 0 + 10
        OR ss_coupon_amt BETWEEN 7629 AND 7629 + 1000
        OR ss_wholesale_cost BETWEEN 6 AND 6 + 20
      )
  ) B5,
  (
    SELECT avg(ss_list_price) B6_LP,
      COUNT(ss_list_price) B6_CNT,
      COUNT(DISTINCT ss_list_price) B6_CNTD
    FROM store_sales
    WHERE ss_quantity BETWEEN 26 AND 30
      AND (
        ss_list_price BETWEEN 89 AND 89 + 10
        OR ss_coupon_amt BETWEEN 15257 AND 15257 + 1000
        OR ss_wholesale_cost BETWEEN 31 AND 31 + 20
      )
  ) B6
LIMIT 100;