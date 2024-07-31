origianl_query = """
SELECT dt.d_year,
    item.i_brand_id brand_id,
    item.i_brand brand,
    sum(ss_sales_price) sum_agg
FROM date_dim dt,
    store_sales,
    item
WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
    AND store_sales.ss_item_sk = item.i_item_sk
    AND item.i_manufact_id = 816
    AND dt.d_moy = 11
GROUP BY dt.d_year,
    item.i_brand,
    item.i_brand_id
ORDER BY dt.d_year,
    sum_agg DESC,
    brand_id
LIMIT 100;
"""

exp_query = """
SELECT 
    count(*) page_count,
    stddev_samp(page_sum) stddev_page_sum,
    avg(page_sum) avg_page_sum,
    (3*stddev_samp(page_sum)/avg(page_sum)/0.025)^2 sample_page_count
FROM ( 
SELECT dt.d_year,
    item.i_brand_id brand_id,
    item.i_brand brand,
    sum(ss_sales_price) page_sum,
    CAST((CAST(CAST(store_sales.ctid AS TEXT) AS point))[0] AS INT) AS page_id
FROM date_dim dt,
    store_sales,
    item
WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
    AND store_sales.ss_item_sk = item.i_item_sk
    AND item.i_manufact_id = 816
    AND dt.d_moy = 11
GROUP BY dt.d_year,
    item.i_brand,
    item.i_brand_id,
    page_id ) q3
GROUP BY q3.d_year,
    q3.brand,
    q3.brand_id;
"""

