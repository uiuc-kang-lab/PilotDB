SELECT sum(ss_sales_price), ss_store_sk store FROM store_sales WHERE 2450824 <= ss_sold_date_sk AND ss_sold_date_sk < 2452882 GROUP BY store