SELECT COUNT(ss_sales_price) FROM store_sales WHERE 2451433 <= ss_sold_date_sk AND ss_sold_date_sk < 2451639 GROUP BY ss_store_sk