WITH avg_sales AS (SELECT AVG(quantity * list_price) AS average_sales FROM (SELECT ss_quantity AS quantity, ss_list_price AS list_price FROM store_sales sampling_method, date_dim WHERE ss_sold_date_sk = d_date_sk AND d_year BETWEEN 2000 AND 2000 + 2 UNION ALL SELECT cs_quantity AS quantity, cs_list_price AS list_price FROM catalog_sales sampling_method, date_dim WHERE cs_sold_date_sk = d_date_sk AND d_year BETWEEN 2000 AND 2000 + 2 UNION ALL SELECT ws_quantity AS quantity, ws_list_price AS list_price FROM web_sales sampling_method, date_dim WHERE ws_sold_date_sk = d_date_sk AND d_year BETWEEN 2000 AND 2000 + 2) AS x) SELECT channel, i_brand_id, i_class_id, i_category_id, SUM(sales), SUM(number_sales) FROM (SELECT 'store' AS channel, i_brand_id, i_class_id, i_category_id, SUM(ss_quantity * ss_list_price) / sample_rate AS sales, COUNT(*) / sample_rate AS number_sales FROM store_sales sampling_method, item, date_dim WHERE ss_item_sk IN subquery_0 AND ss_item_sk = i_item_sk AND ss_sold_date_sk = d_date_sk AND d_year = 2000 + 2 AND d_moy = 11 GROUP BY i_brand_id, i_class_id, i_category_id HAVING SUM(ss_quantity * ss_list_price) / sample_rate > (SELECT average_sales FROM avg_sales) UNION ALL SELECT 'catalog' AS channel, i_brand_id, i_class_id, i_category_id, SUM(cs_quantity * cs_list_price) / sample_rate AS sales, COUNT(*) / sample_rate AS number_sales FROM catalog_sales sampling_method, item, date_dim WHERE cs_item_sk IN subquery_0 AND cs_item_sk = i_item_sk AND cs_sold_date_sk = d_date_sk AND d_year = 2000 + 2 AND d_moy = 11 GROUP BY i_brand_id, i_class_id, i_category_id HAVING SUM(cs_quantity * cs_list_price) / sample_rate > (SELECT average_sales FROM avg_sales) UNION ALL SELECT 'web' AS channel, i_brand_id, i_class_id, i_category_id, SUM(ws_quantity * ws_list_price) / sample_rate AS sales, COUNT(*) / sample_rate AS number_sales FROM web_sales sampling_method, item, date_dim WHERE ws_item_sk IN subquery_0 AND ws_item_sk = i_item_sk AND ws_sold_date_sk = d_date_sk AND d_year = 2000 + 2 AND d_moy = 11 GROUP BY i_brand_id, i_class_id, i_category_id HAVING SUM(ws_quantity * ws_list_price) / sample_rate > (SELECT average_sales FROM avg_sales)) AS y GROUP BY ROLLUP (channel, i_brand_id, i_class_id, i_category_id) ORDER BY channel, i_brand_id, i_class_id, i_category_id LIMIT 100