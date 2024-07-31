original_query = """
SELECT i_brand_id brand_id,
   i_brand brand,
   sum(ss_ext_sales_price) ext_price
FROM date_dim,
   store_sales,
   item
WHERE d_date_sk = ss_sold_date_sk
   AND ss_item_sk = i_item_sk
   AND i_manager_id = 100
   AND d_moy = 12
   AND d_year = 2000
GROUP BY i_brand,
   i_brand_id
ORDER BY ext_price DESC,
   i_brand_id
LIMIT 100;"""

exp_query = """
SELECT brand_id,
    brand,
    sum(page_sum) ext_price,

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
SELECT i_brand_id brand_id,
   i_brand brand,
   sum(ss_ext_sales_price) page_sum,
   count(*) page_size,
   (store_sales.ctid::text::point)[0] page_id
FROM date_dim,
   store_sales,
   item
WHERE d_date_sk = ss_sold_date_sk
   AND ss_item_sk = i_item_sk
   AND i_manager_id = 100
   AND d_moy = 12
   AND d_year = 2000
GROUP BY i_brand,
   i_brand_id,
   page_id)
GROUP BY brand_id,
    brand
ORDER BY ext_price DESC,
   brand_id
LIMIT 100;;
"""