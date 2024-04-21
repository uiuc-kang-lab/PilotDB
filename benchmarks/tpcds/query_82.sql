SELECT i_item_id,
      i_item_desc,
      i_current_price
FROM item,
      inventory,
      date_dim,
      store_sales
WHERE i_current_price BETWEEN 17 AND 17 + 30
      AND inv_item_sk = i_item_sk
      AND d_date_sk = inv_date_sk
      AND d_date BETWEEN cast('1999-07-09' AS date)
      AND (cast('1999-07-09' AS date) + 60 days)
      AND i_manufact_id IN (639, 169, 138, 339)
      AND inv_quantity_on_hand BETWEEN 100 AND 500
      AND ss_item_sk = i_item_sk
GROUP BY i_item_id,
      i_item_desc,
      i_current_price
ORDER BY i_item_id
LIMIT 100;