origianl_query = """
SELECT a.ca_state state,
   COUNT(*) cnt
FROM customer_address a,
   customer c,
   store_sales s,
   date_dim d,
   item i
WHERE a.ca_address_sk = c.c_current_addr_sk
   AND c.c_customer_sk = s.ss_customer_sk
   AND s.ss_sold_date_sk = d.d_date_sk
   AND s.ss_item_sk = i.i_item_sk
   AND d.d_month_seq = (
       SELECT DISTINCT (d_month_seq)
       FROM date_dim
       WHERE d_year = 2002
           AND d_moy = 3
   )
   AND i.i_current_price > 1.2 * (
       SELECT avg(j.i_current_price)
       FROM item j
       WHERE j.i_category = i.i_category
   )
GROUP BY a.ca_state
HAVING COUNT(*) >= 10
ORDER BY cnt,
   a.ca_state
LIMIT 100;
"""

exp_query = """
SELECT 
    count(*) page_count,
    stddev_samp(page_size) stddev_page_sum,
    avg(page_size) avg_page_sum,
    (3*stddev_samp(page_size)/avg(page_size)/0.025)^2 sample_page_count
FROM ( 
SELECT a.ca_state state,
   COUNT(*) page_size,
    CAST((CAST(CAST(s.ctid AS TEXT) AS point))[0] AS INT) AS page_id
FROM customer_address a,
   customer c,
   store_sales s,
   date_dim d,
   item i
WHERE a.ca_address_sk = c.c_current_addr_sk
   AND c.c_customer_sk = s.ss_customer_sk
   AND s.ss_sold_date_sk = d.d_date_sk
   AND s.ss_item_sk = i.i_item_sk
   AND d.d_month_seq = (
       SELECT DISTINCT (d_month_seq)
       FROM date_dim
       WHERE d_year = 2002
           AND d_moy = 3
   )
   AND i.i_current_price > 1.2 * (
       SELECT avg(j.i_current_price)
       FROM item j
       WHERE j.i_category = i.i_category
   )
GROUP BY a.ca_state,
    page_id ) cte
GROUP BY state;
"""
