SELECT COUNT(ss_sales_price) FROM store_sales WHERE 2450815 <= ss_sold_date_sk AND ss_sold_date_sk < 2451021 GROUP BY ss_store_sk