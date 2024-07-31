original_query = """
SELECT sum(ss_net_profit) AS total_sum,
 s_state,
 s_county,
 grouping(s_state) + grouping(s_county) AS lochierarchy,
 rank() over (
   PARTITION by grouping(s_state) + grouping(s_county),
   CASE
     WHEN grouping(s_county) = 0 THEN s_state
   END
   ORDER BY sum(ss_net_profit) DESC
 ) AS rank_within_parent
FROM store_sales,
 date_dim d1,
 store
WHERE d1.d_month_seq BETWEEN 1213 AND 1213 + 11
 AND d1.d_date_sk = ss_sold_date_sk
 AND s_store_sk = ss_store_sk
 AND s_state IN (
   SELECT s_state
   FROM (
       SELECT s_state AS s_state,
         rank() over (
           PARTITION by s_state
           ORDER BY sum(ss_net_profit) DESC
         ) AS ranking
       FROM store_sales,
         store,
         date_dim
       WHERE d_month_seq BETWEEN 1213 AND 1213 + 11
         AND d_date_sk = ss_sold_date_sk
         AND s_store_sk = ss_store_sk
       GROUP BY s_state
     ) tmp1
   WHERE ranking <= 5
 )
GROUP BY rollup(s_state, s_county)
ORDER BY lochierarchy DESC,
CASE
   WHEN lochierarchy = 0 THEN s_state
 END,
 rank_within_parent
LIMIT 100;
"""

exp_query = """
SELECT sum(page_sum) AS result,
    s_state,
    s_county,
    
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
SELECT sum(ss_net_profit) AS page_sum,
    count(*) page_size,
 s_state,
 s_county,
    (store_sales.ctid::text::point)[0] page_id 
FROM store_sales,
 date_dim d1,
 store
WHERE d1.d_month_seq BETWEEN 1213 AND 1213 + 11
 AND d1.d_date_sk = ss_sold_date_sk
 AND s_store_sk = ss_store_sk
 AND s_state IN (
   SELECT s_state
   FROM (
       SELECT s_state AS s_state,
         rank() over (
           PARTITION by s_state
           ORDER BY sum(ss_net_profit) DESC
         ) AS ranking
       FROM store_sales,
         store,
         date_dim
       WHERE d_month_seq BETWEEN 1213 AND 1213 + 11
         AND d_date_sk = ss_sold_date_sk
         AND s_store_sk = ss_store_sk
       GROUP BY s_state
     ) tmp1
   WHERE ranking <= 5
 )
GROUP BY rollup(s_state, s_county), page_id)
GROUP BY rollup(s_state, s_county)
"""