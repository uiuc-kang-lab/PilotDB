original_query = """WITH customer_total_return AS (
 SELECT wr_returning_customer_sk AS ctr_customer_sk,
   ca_state AS ctr_state,
   sum(wr_return_amt) AS ctr_total_return
 FROM web_returns,
   date_dim,
   customer_address
 WHERE wr_returned_date_sk = d_date_sk
   AND d_year = 2002
   AND wr_returning_addr_sk = ca_address_sk
 GROUP BY wr_returning_customer_sk,
   ca_state
)
SELECT c_customer_id,
 c_salutation,
 c_first_name,
 c_last_name,
 c_preferred_cust_flag,
 c_birth_day,
 c_birth_month,
 c_birth_year,
 c_birth_country,
 c_login,
 c_email_address,
 c_last_review_date_sk,
 ctr_total_return
FROM customer_total_return ctr1,
 customer_address,
 customer
WHERE ctr1.ctr_total_return > (
   SELECT avg(ctr_total_return) * 1.2
   FROM customer_total_return ctr2
   WHERE ctr1.ctr_state = ctr2.ctr_state
 )
 AND ca_address_sk = c_current_addr_sk
 AND ca_state = 'IN'
 AND ctr1.ctr_customer_sk = c_customer_sk
ORDER BY c_customer_id,
 c_salutation,
 c_first_name,
 c_last_name,
 c_preferred_cust_flag,
 c_birth_day,
 c_birth_month,
 c_birth_year,
 c_birth_country,
 c_login,
 c_email_address,
 c_last_review_date_sk,
 ctr_total_return
LIMIT 100;"""

exp_query = """
WITH customer_total_return AS (
SELECT ctr_customer_sk, ctr_state,
    sum(page_sum) AS ctr_total_return,

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
SELECT wr_returning_customer_sk AS ctr_customer_sk,
    ca_state AS ctr_state,
    sum(wr_return_amt) AS page_sum,
    (web_returns.ctid::text::point)[0] page_id,
    count(*) page_size
FROM web_returns,
    date_dim,
    customer_address
WHERE wr_returned_date_sk = d_date_sk
    AND d_year = 2002
    AND wr_returning_addr_sk = ca_address_sk
GROUP BY wr_returning_customer_sk,
    ca_state,
    page_id
)
GROUP BY ctr_customer_sk, ctr_state
)
SELECT c_customer_id,
 c_salutation,
 c_first_name,
 c_last_name,
 c_preferred_cust_flag,
 c_birth_day,
 c_birth_month,
 c_birth_year,
 c_birth_country,
 c_login,
 c_email_address,
 c_last_review_date_sk,
 ctr_total_return,
    page_count,
    stddev_page_sum,
    avg_page_sum,
    sample_page_count,
    stddev_page_size,
    avg_page_size,
    sample_page_count_size
FROM customer_total_return ctr1,
 customer_address,
 customer
WHERE ctr1.ctr_total_return > (
   SELECT avg(ctr_total_return) * 1.2
   FROM customer_total_return ctr2
   WHERE ctr1.ctr_state = ctr2.ctr_state
 )
 AND ca_address_sk = c_current_addr_sk
 AND ca_state = 'IN'
 AND ctr1.ctr_customer_sk = c_customer_sk
ORDER BY c_customer_id,
 c_salutation,
 c_first_name,
 c_last_name,
 c_preferred_cust_flag,
 c_birth_day,
 c_birth_month,
 c_birth_year,
 c_birth_country,
 c_login,
 c_email_address,
 c_last_review_date_sk,
 ctr_total_return
LIMIT 100;
"""