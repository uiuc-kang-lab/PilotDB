origianl_query = """
SELECT i_item_id,
    ca_country,
    ca_state,
    ca_county,
    avg((cs_quantity) agg1,
    avg((cs_list_price) agg2,
    avg((cs_coupon_amt) agg3,
    avg((cs_sales_price) agg4,
    avg((cs_net_profit) agg5,
    avg((c_birth_year) agg6,
    avg((cd1.cd_dep_count) agg7
FROM catalog_sales,
    customer_demographics cd1,
    customer_demographics cd2,
    customer,
    customer_address,
    date_dim,
    item
WHERE cs_sold_date_sk = d_date_sk
    AND cs_item_sk = i_item_sk
    AND cs_bill_cdemo_sk = cd1.cd_demo_sk
    AND cs_bill_customer_sk = c_customer_sk
    AND cd1.cd_gender = 'F'
    AND cd1.cd_education_status = 'Advanced Degree'
    AND c_current_cdemo_sk = cd2.cd_demo_sk
    AND c_current_addr_sk = ca_address_sk
    AND c_birth_month IN (10, 7, 8, 4, 1, 2)
    AND d_year = 1998
    AND ca_state IN (
         'WA',
         'GA',
         'NC',
         'ME',
         'WY',
         'OK',
         'IN'
    )
GROUP BY rollup (i_item_id, ca_country, ca_state, ca_county)
ORDER BY ca_country,
    ca_state,
    ca_county,
    i_item_id
LIMIT 100;
"""

exp_query = """
SELECT i_item_id,
    ca_country,
    ca_state,
    ca_county,
    sum(page_sum_1)/sum(page_size) agg1,
    sum(page_sum_2)/sum(page_size) agg2,
    sum(page_sum_3)/sum(page_size) agg3,
    sum(page_sum_4)/sum(page_size) agg4,
    sum(page_sum_5)/sum(page_size) agg5,
    sum(page_sum_6)/sum(page_size) agg6,
    sum(page_sum_7)/sum(page_size) agg7,
    
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

    stddev_pop(page_sum_5) stddev_page_sum_5,
    avg(page_sum_5) avg_page_sum_5,
    CASE WHEN (avg(page_sum_5) = 0) THEN 0
        ELSE (3*stddev_pop(page_sum_5)/avg(page_sum_5)/0.025)^2
        END sample_page_count_5,

    stddev_pop(page_sum_6) stddev_page_sum_6,
    avg(page_sum_6) avg_page_sum_6,
    CASE WHEN (avg(page_sum_6) = 0) THEN 0
        ELSE (3*stddev_pop(page_sum_6)/avg(page_sum_6)/0.025)^2
        END sample_page_count_6,

    stddev_pop(page_sum_7) stddev_page_sum_7,
    avg(page_sum_7) avg_page_sum_7,
    CASE WHEN (avg(page_sum_7) = 0) THEN 0
        ELSE (3*stddev_pop(page_sum_7)/avg(page_sum_7)/0.025)^2
        END sample_page_count_7,

    stddev_pop(page_size) stddev_page_size,
    avg(page_size) avg_page_size,
    CASE WHEN (avg(page_size) = 0) THEN 0
        ELSE (3*stddev_pop(page_size)/avg(page_size)/0.025)^2
        END sample_page_count_size

FROM (
SELECT i_item_id,
    ca_country,
    ca_state,
    ca_county,
    sum(cs_quantity) page_sum_1,
    sum(cs_list_price) page_sum_2,
    sum(cs_coupon_amt) page_sum_3,
    sum(cs_sales_price) page_sum_4,
    sum(cs_net_profit) page_sum_5,
    sum(c_birth_year) page_sum_6,
    sum(cd1.cd_dep_count) page_sum_7,
    count(*) page_size,
    CAST((CAST(CAST(catalog_sales.ctid AS TEXT) AS point))[0] AS INT) AS page_id
FROM catalog_sales,
    customer_demographics cd1,
    customer_demographics cd2,
    customer,
    customer_address,
    date_dim,
    item
WHERE cs_sold_date_sk = d_date_sk
    AND cs_item_sk = i_item_sk
    AND cs_bill_cdemo_sk = cd1.cd_demo_sk
    AND cs_bill_customer_sk = c_customer_sk
    AND cd1.cd_gender = 'F'
    AND cd1.cd_education_status = 'Advanced Degree'
    AND c_current_cdemo_sk = cd2.cd_demo_sk
    AND c_current_addr_sk = ca_address_sk
    AND c_birth_month IN (10, 7, 8, 4, 1, 2)
    AND d_year = 1998
    AND ca_state IN (
         'WA',
         'GA',
         'NC',
         'ME',
         'WY',
         'OK',
         'IN'
    )
GROUP BY rollup (i_item_id, ca_country, ca_state, ca_county), page_id)
GROUP BY rollup (i_item_id, ca_country, ca_state, ca_county)
ORDER BY ca_country,
    ca_state,
    ca_county,
    i_item_id
LIMIT 100;
"""