original_query = """
SELECT sum(ws_ext_discount_amt) AS "Excess Discount Amount"
FROM web_sales,
  item,
  date_dim
WHERE i_manufact_id = 320
  AND i_item_sk = ws_item_sk
  AND d_date BETWEEN '2002-02-26' AND (cast('2002-02-26' AS date) + 90 days)
  AND d_date_sk = ws_sold_date_sk
  AND ws_ext_discount_amt > (
     SELECT 1.3 * avg(ws_ext_discount_amt)
     FROM web_sales,
        date_dim
     WHERE ws_item_sk = i_item_sk
        AND d_date BETWEEN '2002-02-26' AND (cast('2002-02-26' AS date) + 90 days)
        AND d_date_sk = ws_sold_date_sk
  )
ORDER BY sum(ws_ext_discount_amt)
LIMIT 100;
"""

exp_query = """
SELECT sum(page_sum) AS "Excess Discount Amount",
    count(*) page_count,
    stddev_pop(page_sum) stddev_page_sum,
    avg(page_sum) avg_page_sum,
    CASE WHEN (avg(page_sum) = 0) THEN 0
    ELSE (3*stddev_pop(page_sum)/avg(page_sum)/0.025)^2
    END sample_page_count,

    stddev_pop(page_size) stddev_page_size,
    avg(page_size) avg_page_size,
    CASE WHEN (avg(page_size) = 0) THEN 0
    ELSE (3*stddev_pop(page_size)/avg(page_size)/0.025)^2
    END sample_page_count_size

FROM (
SELECT sum(ws_ext_discount_amt) AS page_sum,
    (web_sales.ctid::text::point)[0] page_id,
    count(*) page_size
FROM web_sales,
  item,
  date_dim
WHERE i_manufact_id = 320
  AND i_item_sk = ws_item_sk
  AND d_date BETWEEN date '2002-02-26' AND date '2002-02-26' + interval '90 day'
  AND d_date_sk = ws_sold_date_sk
  AND ws_ext_discount_amt > (
     SELECT 1.3 * avg(ws_ext_discount_amt)
     FROM web_sales,
        date_dim
     WHERE ws_item_sk = i_item_sk
        AND d_date BETWEEN date '2002-02-26' AND date '2002-02-26' + interval '90 day'
        AND d_date_sk = ws_sold_date_sk )
GROUP BY page_id);
"""