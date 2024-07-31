original_query = """
SELECT w_state,
   i_item_id,
   sum(
       CASE
           WHEN (
               cast(d_date AS date) < cast ('2001-04-02' AS date)
           ) THEN cs_sales_price - coalesce(cr_refunded_cash, 0)
           ELSE 0
       END
   ) AS sales_before,
   sum(
       CASE
           WHEN (
               cast(d_date AS date) >= cast ('2001-04-02' AS date)
           ) THEN cs_sales_price - coalesce(cr_refunded_cash, 0)
           ELSE 0
       END
   ) AS sales_after
FROM catalog_sales
   LEFT OUTER JOIN catalog_returns ON (
       cs_order_number = cr_order_number
       AND cs_item_sk = cr_item_sk
   ),
   warehouse,
   item,
   date_dim
WHERE i_current_price BETWEEN 0.99 AND 1.49
   AND i_item_sk = cs_item_sk
   AND cs_warehouse_sk = w_warehouse_sk
   AND cs_sold_date_sk = d_date_sk
   AND d_date BETWEEN (cast ('2001-04-02' AS date) - 30 days)
   AND (cast ('2001-04-02' AS date) + 30 days)
GROUP BY w_state,
   i_item_id
ORDER BY w_state,
   i_item_id
LIMIT 100;"""

exp_query = """
SELECT
    w_state,
    i_item_id,
    sum(page_sum_1) AS sales_before,
    sum(page_sum_2) AS sales_after,

    count(*) page_count,

    stddev_pop(page_sum_1) stddev_page_sum_1,
    avg(page_sum_1) avg_page_sum_1,
    CASE WHEN (avg(page_sum_1) = 0) THEN 0
    ELSE (3*stddev_pop(page_sum_1)/avg(page_sum_1)/0.025)^2
    END sample_page_count_1,

    stddev_pop(page_sum_2) stddev_page_sum_2,
    avg(page_sum_2) avg_page_sum_2,
    CASE WHEN (avg(page_sum_2) = 0) THEN 0
    ELSE (3*stddev_pop(page_sum_2)/avg(page_sum_2)/0.025)^2
    END sample_page_count_2,

    stddev_pop(page_size) stddev_page_size,
    avg(page_size) avg_page_size,
    CASE WHEN (avg(page_size) = 0) THEN 0
    ELSE (3*stddev_pop(page_size)/avg(page_size)/0.025)^2
    END sample_page_count_size

FROM (
SELECT w_state,
   i_item_id,
   sum(
       CASE
           WHEN (
               cast(d_date AS date) < cast ('2001-04-02' AS date)
           ) THEN cs_sales_price - coalesce(cr_refunded_cash, 0)
           ELSE 0
       END
   ) AS page_sum_1,
   sum(
       CASE
           WHEN (
               cast(d_date AS date) >= cast ('2001-04-02' AS date)
           ) THEN cs_sales_price - coalesce(cr_refunded_cash, 0)
           ELSE 0
       END
   ) AS page_sum_2,
    count(*) page_size,
    (catalog_sales.ctid::text::point)[0] page_id
FROM catalog_sales
   LEFT OUTER JOIN catalog_returns ON (
       cs_order_number = cr_order_number
       AND cs_item_sk = cr_item_sk
   ),
   warehouse,
   item,
   date_dim
WHERE i_current_price BETWEEN 0.99 AND 1.49
   AND i_item_sk = cs_item_sk
   AND cs_warehouse_sk = w_warehouse_sk
   AND cs_sold_date_sk = d_date_sk
   AND d_date BETWEEN date '2001-04-02'- interval '30 day'
   AND date '2001-04-02' + interval '30 day'
GROUP BY w_state,
   i_item_id,
   page_id)
GROUP BY w_state,
    i_item_id
ORDER BY w_state,
   i_item_id
LIMIT 100;
"""