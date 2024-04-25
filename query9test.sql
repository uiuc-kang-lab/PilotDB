SELECT 
*
FROM 
(
SELECT 
    CASE 
        WHEN subquery0 > 2972190 THEN
            SUM(ss_ext_sales_price)
        ELSE 
            SUM(ss_net_profit)
    END AS revenue,
    COUNT(*) AS count,
    'page_id_0:' || CAST((CAST(CAST(store_sales.ctid AS TEXT) AS point))[0] AS INT) AS page_id_0
FROM store_sales TABLESAMPLE SYSTEM (0.0001)
WHERE ss_quantity BETWEEN 1 AND 20 
GROUP BY page_id_0 
) AS c1,
(
SELECT 
    CASE 
        WHEN subquery1 > 2972190 THEN
            SUM(ss_ext_sales_price)
        ELSE 
            SUM(ss_net_profit)
    END AS revenue,
    COUNT(*) AS count,
    'page_id_1:' || CAST((CAST(CAST(store_sales.ctid AS TEXT) AS point))[0] AS INT) AS page_id_1
FROM store_sales TABLESAMPLE SYSTEM (0.0001)
WHERE ss_quantity BETWEEN 21 AND 40  
GROUP BY page_id_1
) AS c2,
(
SELECT 
    CASE 
        WHEN subquery2 > 2972190 THEN
            SUM(ss_ext_sales_price)
        ELSE 
            SUM(ss_net_profit)
    END AS revenue,
    COUNT(*) AS count,
    'page_id_2:' || CAST((CAST(CAST(store_sales.ctid AS TEXT) AS point))[0] AS INT) AS page_id_2
FROM store_sales TABLESAMPLE SYSTEM (0.0001)
WHERE ss_quantity BETWEEN 41 AND 60  
GROUP BY page_id_2
) AS c3,
(
SELECT 
    CASE 
        WHEN subquery3 > 2972190 THEN
            SUM(ss_ext_sales_price)
        ELSE 
            SUM(ss_net_profit)
    END AS revenue,
    COUNT(*) AS count,
    'page_id_3:' || CAST((CAST(CAST(store_sales.ctid AS TEXT) AS point))[0] AS INT) AS page_id_3
FROM store_sales TABLESAMPLE SYSTEM (0.0001)
WHERE ss_quantity BETWEEN 61 AND 80  
GROUP BY page_id_3
) AS c4,
(
SELECT 
    CASE 
        WHEN subquery4 > 2972190 THEN
            SUM(ss_ext_sales_price)
        ELSE 
            SUM(ss_net_profit)
    END AS revenue,
    COUNT(*) AS count,
    'page_id_4:' || CAST((CAST(CAST(store_sales.ctid AS TEXT) AS point))[0] AS INT) AS page_id_4
FROM store_sales TABLESAMPLE SYSTEM (0.0001)
WHERE ss_quantity BETWEEN 81 AND 100  
GROUP BY page_id_4
) AS c5;
