SELECT sum(ss_sales_price), ss_store_sk store FROM store_sales WHERE 2450819 <= ss_sold_date_sk AND ss_sold_date_sk < 2452877 GROUP BY store