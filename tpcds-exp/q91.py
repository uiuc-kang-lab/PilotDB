original_query = """
SELECT cc_call_center_id Call_Center,
       cc_name Call_Center_Name,
       cc_manager Manager,
       sum(cr_net_loss) Returns_Loss
FROM call_center,
       catalog_returns,
       date_dim,
       customer,
       customer_address,
       customer_demographics,
       household_demographics
WHERE cr_call_center_sk = cc_call_center_sk
       AND cr_returned_date_sk = d_date_sk
       AND cr_returning_customer_sk = c_customer_sk
       AND cd_demo_sk = c_current_cdemo_sk
       AND hd_demo_sk = c_current_hdemo_sk
       AND ca_address_sk = c_current_addr_sk
       AND d_year = 2001
       AND d_moy = 11
       AND (
               (
                       cd_marital_status = 'M'
                       AND cd_education_status = 'Unknown'
               )
               OR(
                       cd_marital_status = 'W'
                       AND cd_education_status = 'Advanced Degree'
               )
       )
       AND hd_buy_potential LIKE '1001-5000%'
       AND ca_gmt_offset = -6
GROUP BY cc_call_center_id,
       cc_name,
       cc_manager,
       cd_marital_status,
       cd_education_status
ORDER BY sum(cr_net_loss) DESC;
"""

exp_query = """
SELECT Call_Center,
         Call_Center_Name,
         Manager,
         sum(page_sum) Returns_Loss,

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
SELECT cc_call_center_id Call_Center,
       cc_name Call_Center_Name,
       cc_manager Manager,
       cd_marital_status,
       cd_education_status,
       sum(cr_net_loss) page_sum,
       count(*) page_size,
       (catalog_returns.ctid::text::point)[0] page_id
FROM call_center,
       catalog_returns,
       date_dim,
       customer,
       customer_address,
       customer_demographics,
       household_demographics
WHERE cr_call_center_sk = cc_call_center_sk
       AND cr_returned_date_sk = d_date_sk
       AND cr_returning_customer_sk = c_customer_sk
       AND cd_demo_sk = c_current_cdemo_sk
       AND hd_demo_sk = c_current_hdemo_sk
       AND ca_address_sk = c_current_addr_sk
       AND d_year = 2001
       AND d_moy = 11
       AND (
               (
                       cd_marital_status = 'M'
                       AND cd_education_status = 'Unknown'
               )
               OR(
                       cd_marital_status = 'W'
                       AND cd_education_status = 'Advanced Degree'
               )
       )
       AND hd_buy_potential LIKE '1001-5000%'
       AND ca_gmt_offset = -6
GROUP BY cc_call_center_id,
       cc_name,
       cc_manager,
       cd_marital_status,
       cd_education_status,
       page_id )
GROUP BY Call_Center,
       Call_Center_Name,
       Manager,
       cd_marital_status,
       cd_education_status;
"""