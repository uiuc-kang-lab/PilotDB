SELECT SUM(ss_whole_sale_cost), COUNT(ss_net_profit) FROM store_sales, store WHERE ss_store_sk = s_store_sk WHERE s_number_of_employees = 201