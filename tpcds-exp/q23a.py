origianl_query = """WITH frequent_ss_items AS (
 SELECT substr(i_item_desc, 1, 30) itemdesc,
   i_item_sk item_sk,
   d_date solddate,
   COUNT(*) cnt
 FROM store_sales,
   date_dim,
   item
 WHERE ss_sold_date_sk = d_date_sk
   AND ss_item_sk = i_item_sk
   AND d_year IN (2000, 2000 + 1, 2000 + 2, 2000 + 3)
 GROUP BY substr(i_item_desc, 1, 30),
   i_item_sk,
   d_date
 HAVING COUNT(*) > 4
),
max_store_sales AS (
 SELECT max(csales) tpcds_cmax
 FROM (
     SELECT c_customer_sk,
       sum(ss_quantity * ss_sales_price) csales
     FROM store_sales,
       customer,
       date_dim
     WHERE ss_customer_sk = c_customer_sk
       AND ss_sold_date_sk = d_date_sk
       AND d_year IN (2000, 2000 + 1, 2000 + 2, 2000 + 3)
     GROUP BY c_customer_sk
   )
),
best_ss_customer AS (
 SELECT c_customer_sk,
   sum(ss_quantity * ss_sales_price) ssales
 FROM store_sales,
   customer
 WHERE ss_customer_sk = c_customer_sk
 GROUP BY c_customer_sk
 HAVING sum(ss_quantity * ss_sales_price) > (95 / 100.0) * (
     SELECT *
     FROM max_store_sales
   )
)
SELECT sum(sales)
FROM (
   SELECT cs_quantity * cs_list_price sales
   FROM catalog_sales,
     date_dim
   WHERE d_year = 2000
     AND d_moy = 5
     AND cs_sold_date_sk = d_date_sk
     AND cs_item_sk IN (
       SELECT item_sk
       FROM frequent_ss_items
     )
     AND cs_bill_customer_sk IN (
       SELECT c_customer_sk
       FROM best_ss_customer
     )
   UNION ALL
   SELECT ws_quantity * ws_list_price sales
   FROM web_sales,
     date_dim
   WHERE d_year = 2000
     AND d_moy = 5
     AND ws_sold_date_sk = d_date_sk
     AND ws_item_sk IN (
       SELECT item_sk
       FROM frequent_ss_items
     )
     AND ws_bill_customer_sk IN (
       SELECT c_customer_sk
       FROM best_ss_customer
     )
 )
LIMIT 100;"""

exp_query = """WITH frequent_ss_items AS (
 SELECT substr(i_item_desc, 1, 30) itemdesc,
   i_item_sk item_sk,
   d_date solddate,
   COUNT(*) cnt
 FROM store_sales,
   date_dim,
   item
 WHERE ss_sold_date_sk = d_date_sk
   AND ss_item_sk = i_item_sk
   AND d_year IN (2000, 2000 + 1, 2000 + 2, 2000 + 3)
 GROUP BY substr(i_item_desc, 1, 30),
   i_item_sk,
   d_date
 HAVING COUNT(*) > 4
),
max_store_sales AS (
 SELECT max(csales) tpcds_cmax
 FROM (
     SELECT c_customer_sk,
       sum(ss_quantity * ss_sales_price) csales
     FROM store_sales,
       customer,
       date_dim
     WHERE ss_customer_sk = c_customer_sk
       AND ss_sold_date_sk = d_date_sk
       AND d_year IN (2000, 2000 + 1, 2000 + 2, 2000 + 3)
     GROUP BY c_customer_sk
   )
),
best_ss_customer AS (
 SELECT c_customer_sk,
   sum(ss_quantity * ss_sales_price) ssales
 FROM store_sales,
   customer
 WHERE ss_customer_sk = c_customer_sk
 GROUP BY c_customer_sk
 HAVING sum(ss_quantity * ss_sales_price) > (95 / 100.0) * (
     SELECT *
     FROM max_store_sales
   )
)

SELECT sum(page_sum) result,

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
SELECT sum(sales) page_sum,
    count(*) page_size,
    page_id
FROM (
   SELECT cs_quantity * cs_list_price sales,
   'p1_' || (catalog_sales.ctid::text::point)[0]::int AS page_id
   FROM catalog_sales,
     date_dim
   WHERE d_year = 2000
     AND d_moy = 5
     AND cs_sold_date_sk = d_date_sk
     AND cs_item_sk IN (
       SELECT item_sk
       FROM frequent_ss_items
     )
     AND cs_bill_customer_sk IN (
       SELECT c_customer_sk
       FROM best_ss_customer
     )
   UNION ALL
   SELECT ws_quantity * ws_list_price sales,
   'p2_' || (web_sales.ctid::text::point)[0]::int AS page_id
   FROM web_sales,
     date_dim
   WHERE d_year = 2000
     AND d_moy = 5
     AND ws_sold_date_sk = d_date_sk
     AND ws_item_sk IN (
       SELECT item_sk
       FROM frequent_ss_items
     )
     AND ws_bill_customer_sk IN (
       SELECT c_customer_sk
       FROM best_ss_customer
     )
 )
GROUP BY page_id
)
"""