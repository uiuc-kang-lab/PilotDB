SELECT TOP 100 
   sum(ws_ext_discount_amt) 
FROM web_sales,
   item,
   date_dim
WHERE i_manufact_id = 320
   AND i_item_sk = ws_item_sk
   AND d_date BETWEEN '2002-02-26' AND DATEADD(day, 90, CAST('2002-02-26' AS date))
   AND d_date_sk = ws_sold_date_sk
   AND ws_ext_discount_amt > (
      SELECT 1.3 * avg(ws_ext_discount_amt)
      FROM web_sales,
         date_dim
      WHERE ws_item_sk = i_item_sk
         AND d_date BETWEEN '2002-02-26' AND DATEADD(day, 90, CAST('2002-02-26' AS date))
         AND d_date_sk = ws_sold_date_sk
   )
ORDER BY sum(ws_ext_discount_amt);