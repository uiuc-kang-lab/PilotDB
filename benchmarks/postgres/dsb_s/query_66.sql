SELECT AVG(ss_wholesale_cost), SUM(ss_net_profit) FROM store_sales, store WHERE ss_store_sk = s_store_sk AND s_number_employees = 274