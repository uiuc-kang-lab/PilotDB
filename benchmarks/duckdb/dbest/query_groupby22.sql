SELECT AVG(ss_sales_price) FROM store_sales WHERE 2452257 <= ss_sold_date_sk AND ss_sold_date_sk < 2452463 GROUP BY ss_store_sk