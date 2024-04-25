SELECT c_last_name, c_first_name, sales FROM (SELECT c_last_name, c_first_name, SUM(cs_quantity * cs_list_price) / sample_rate AS sales FROM catalog_sales sampling_method, customer, date_dim WHERE d_year = 2000 AND d_moy = 5 AND cs_sold_date_sk = d_date_sk AND cs_item_sk IN subquery_1 AND cs_bill_customer_sk IN subquery_0 AND cs_bill_customer_sk = c_customer_sk GROUP BY c_last_name, c_first_name UNION ALL SELECT c_last_name, c_first_name, SUM(ws_quantity * ws_list_price) / sample_rate AS sales FROM web_sales sampling_method, customer, date_dim WHERE d_year = 2000 AND d_moy = 5 AND ws_sold_date_sk = d_date_sk AND ws_item_sk IN subquery_1 AND ws_bill_customer_sk IN subquery_0 AND ws_bill_customer_sk = c_customer_sk GROUP BY c_last_name, c_first_name) ORDER BY c_last_name, c_first_name, sales LIMIT 100