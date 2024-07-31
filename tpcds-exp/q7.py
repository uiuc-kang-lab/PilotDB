origianl_query = """
SELECT i_item_id,
     avg(ss_quantity) agg1,
     avg(ss_list_price) agg2,
     avg(ss_coupon_amt) agg3,
     avg(ss_sales_price) agg4
FROM store_sales,
     customer_demographics,
     date_dim,
     item,
     promotion
WHERE ss_sold_date_sk = d_date_sk
     AND ss_item_sk = i_item_sk
     AND ss_cdemo_sk = cd_demo_sk
     AND ss_promo_sk = p_promo_sk
     AND cd_gender = 'F'
     AND cd_marital_status = 'W'
     AND cd_education_status = 'College'
     AND (
           p_channel_email = 'N'
           OR p_channel_event = 'N'
     )
     AND d_year = 2001
GROUP BY i_item_id
ORDER BY i_item_id
LIMIT 100;
"""

exp_query = """
SELECT i_item_id,
    sum(page_sum_1)/sum(page_size) agg1,
    sum(page_sum_2)/sum(page_size) agg2,
    sum(page_sum_3)/sum(page_size) agg3,
    sum(page_sum_4)/sum(page_size) agg4,
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

    stddev_pop(page_sum_3) stddev_page_sum_3,
    avg(page_sum_3) avg_page_sum_3,
    CASE WHEN (avg(page_sum_3) = 0) THEN 0
    ELSE (3*stddev_pop(page_sum_3)/avg(page_sum_3)/0.025)^2
    END sample_page_count_3,

    stddev_pop(page_sum_4) stddev_page_sum_4,
    avg(page_sum_4) avg_page_sum_4,
    CASE WHEN (avg(page_sum_4) = 0) THEN 0
    ELSE (3*stddev_pop(page_sum_4)/avg(page_sum_4)/0.025)^2
    END sample_page_count_4,

    stddev_pop(page_size) stddev_page_size,
    avg(page_size) avg_page_size,
    CASE WHEN (avg(page_size) = 0) THEN 0
    ELSE (3*stddev_pop(page_size)/avg(page_size)/0.025)^2
    END sample_page_count_size

FROM
( SELECT i_item_id,
     sum(ss_quantity) page_sum_1,
     sum(ss_list_price) page_sum_2,
     sum(ss_coupon_amt) page_sum_3,
     sum(ss_sales_price) page_sum_4,
     count(*) page_size,
     CAST((CAST(CAST(store_sales.ctid AS TEXT) AS point))[0] AS INT) AS page_id
FROM store_sales,
     customer_demographics,
     date_dim,
     item,
     promotion
WHERE ss_sold_date_sk = d_date_sk
     AND ss_item_sk = i_item_sk
     AND ss_cdemo_sk = cd_demo_sk
     AND ss_promo_sk = p_promo_sk
     AND cd_gender = 'F'
     AND cd_marital_status = 'W'
     AND cd_education_status = 'College'
     AND (
           p_channel_email = 'N'
           OR p_channel_event = 'N'
     )
     AND d_year = 2001
GROUP BY i_item_id,
    page_id) cte
GROUP BY i_item_id;
"""
