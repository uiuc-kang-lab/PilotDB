origianl_query = """
SELECT i_item_id,
       i_item_desc,
       i_category,
       i_class,
       i_current_price,
       sum(ws_ext_sales_price) AS itemrevenue,
       sum(ws_ext_sales_price) * 100 / sum(sum(ws_ext_sales_price)) over (PARTITION by i_class) AS revenueratio
FROM web_sales,
       item,
       date_dim
WHERE ws_item_sk = i_item_sk
       AND i_category IN ('Books', 'Sports', 'Men')
       AND ws_sold_date_sk = d_date_sk
       AND d_date BETWEEN cast('1998-04-06' AS date)
       AND (cast('1998-04-06' AS date) + 30 days)
GROUP BY i_item_id,
       i_item_desc,
       i_category,
       i_class,
       i_current_price
ORDER BY i_category,
       i_class,
       i_item_id,
       i_item_desc,
       revenueratio
LIMIT 100;"""

exp_query = """
SELECT sum(page_sum) AS itemrevenue,
    sum(page_sum) * 100 / sum(sum(page_sum)) over (PARTITION by i_class) AS revenueratio,
    count(*) page_count,
    stddev_pop(page_sum) stddev_page_sum,
    avg(page_sum) avg_page_sum,
    CASE WHEN (avg(page_sum) = 0) THEN 0
    ELSE (3*stddev_pop(page_sum)/avg(page_sum)/0.025)^2
    END sample_page_count
FROM
(SELECT i_item_id,
       i_item_desc,
       i_category,
       i_class,
       i_current_price,
       sum(ws_ext_sales_price) AS page_sum,
       CAST((CAST(CAST(web_sales.ctid AS TEXT) AS point))[0] AS INT) AS page_id,
       count(*) page_size
FROM web_sales,
       item,
       date_dim
WHERE ws_item_sk = i_item_sk
       AND i_category IN ('Books', 'Sports', 'Men')
       AND ws_sold_date_sk = d_date_sk
       AND d_date BETWEEN cast('1998-04-06' AS date)
       AND (date '1998-04-06' + interval '30 day')
GROUP BY i_item_id,
       i_item_desc,
       i_category,
       i_class,
       i_current_price,
       page_id)
GROUP BY i_item_id,
    i_item_desc,
    i_category,
    i_class,
    i_current_price
ORDER BY i_category,
    i_class,
    i_item_id,
    i_item_desc,
    revenueratio
LIMIT 100
"""