SELECT i_item_id,
  i_item_desc,
  i_category,
  i_class,
  i_current_price,
  sum(cs_ext_sales_price) AS itemrevenue,
  sum(cs_ext_sales_price) * 100 / sum(sum(cs_ext_sales_price)) over (PARTITION by i_class) AS revenueratio
FROM catalog_sales,
  item,
  date_dim
WHERE cs_item_sk = i_item_sk
  AND i_category IN ('Shoes', 'Books', 'Women')
  AND cs_sold_date_sk = d_date_sk
  AND d_date BETWEEN cast('2002-01-26' AS date)
  AND (cast('2002-01-26' AS date) + 30 days)
GROUP BY i_item_id,
  i_item_desc,
  i_category,
  i_class,
  i_current_price
ORDER BY i_category,
  i_class,
  i_item_id,
  i_item_desc,
  revenueratio
LIMIT 100;