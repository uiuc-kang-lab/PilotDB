SELECT count(ss_sales_price), ss_store_sk store FROM store_sales WHERE 2450820 <= ss_sold_date_sk AND ss_sold_date_sk < 2452878 GROUP BY ss_store_sk