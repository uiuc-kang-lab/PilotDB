SELECT i_item_id,
      i_item_desc,
      i_current_price
FROM item,
      inventory,
      date_dim,
      catalog_sales
WHERE i_current_price BETWEEN 45 AND 45 + 30
      AND inv_item_sk = i_item_sk
      AND d_date_sk = inv_date_sk
      AND d_date BETWEEN cast('1999-02-21' AS date)
      AND (cast('1999-02-21' AS date) + 60 days)
      AND i_manufact_id IN (856, 707, 1000, 747)
      AND inv_quantity_on_hand BETWEEN 100 AND 500
      AND cs_item_sk = i_item_sk
GROUP BY i_item_id,
      i_item_desc,
      i_current_price
ORDER BY i_item_id
LIMIT 100;