original_query = """
SELECT COUNT(*)
FROM store_sales,
   household_demographics,
   time_dim,
   store
WHERE ss_sold_time_sk = time_dim.t_time_sk
   AND ss_hdemo_sk = household_demographics.hd_demo_sk
   AND ss_store_sk = s_store_sk
   AND time_dim.t_hour = 8
   AND time_dim.t_minute >= 30
   AND household_demographics.hd_dep_count = 3
   AND store.s_store_name = 'ese'
ORDER BY COUNT(*)
LIMIT 100;"""

exp_query = """
SELECT SUM(page_size) AS result,
    count(*) page_count,
    stddev_pop(page_size) stddev_page_size,
    avg(page_size) avg_page_size,
    CASE WHEN (avg(page_size) = 0) THEN 0
    ELSE (3*stddev_pop(page_size)/avg(page_size)/0.025)^2
    END sample_page_count_size
FROM (
SELECT COUNT(*) page_size,
    (store_sales.ctid::text::point)[0] page_id
FROM store_sales,
   household_demographics,
   time_dim,
   store
WHERE ss_sold_time_sk = time_dim.t_time_sk
   AND ss_hdemo_sk = household_demographics.hd_demo_sk
   AND ss_store_sk = s_store_sk
   AND time_dim.t_hour = 8
   AND time_dim.t_minute >= 30
   AND household_demographics.hd_dep_count = 3
   AND store.s_store_name = 'ese'
GROUP BY page_id )
"""