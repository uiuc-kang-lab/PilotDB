SELECT SUM(ss_sales_price) FROM store_sales WHERE 2451021 <= ss_sold_date_sk AND ss_sold_date_sk < 2451227 GROUP BY ss_store_sk