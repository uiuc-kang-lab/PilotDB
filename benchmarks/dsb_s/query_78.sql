SELECT SUM(ss_whole_sale_cost), SUM(ss_net_profit) FROM store_sales, store WHERE ss_store_sk = s_store_sk WHERE s_number_of_employees = 257