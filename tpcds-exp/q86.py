original_query = """
SELECT sum(ws_net_paid) AS total_sum,
 i_category,
 i_class,
 grouping(i_category) + grouping(i_class) AS lochierarchy,
 rank() over (
   PARTITION by grouping(i_category) + grouping(i_class),
   CASE
     WHEN grouping(i_class) = 0 THEN i_category
   END
   ORDER BY sum(ws_net_paid) DESC
 ) AS rank_within_parent
FROM web_sales,
 date_dim d1,
 item
WHERE d1.d_month_seq BETWEEN 1224 AND 1224 + 11
 AND d1.d_date_sk = ws_sold_date_sk
 AND i_item_sk = ws_item_sk
GROUP BY rollup(i_category, i_class)
ORDER BY lochierarchy DESC,
 CASE
   WHEN lochierarchy = 0 THEN i_category
 END,
 rank_within_parent
LIMIT 100;"""

exp_query = """
SELECT sum(page_sum) AS result,
    i_category,
    i_class,

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
SELECT sum(ws_net_paid) AS page_sum,
 i_category,
 i_class,
    count(*) page_size,
    (web_sales.ctid::text::point)[0] page_id
FROM web_sales,
 date_dim d1,
 item
WHERE d1.d_month_seq BETWEEN 1224 AND 1224 + 11
 AND d1.d_date_sk = ws_sold_date_sk
 AND i_item_sk = ws_item_sk
GROUP BY rollup(i_category, i_class), page_id)
GROUP BY rollup(i_category, i_class);
"""