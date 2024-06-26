SELECT i_product_name,
    i_brand,
    i_class,
    i_category,
    avg(inv_quantity_on_hand) qoh
FROM inventory,
    date_dim,
    item
WHERE inv_date_sk = d_date_sk
    AND inv_item_sk = i_item_sk
    AND d_month_seq BETWEEN 1188 AND 1188 + 11
GROUP BY rollup(
        i_product_name,
        i_brand,
        i_class,
        i_category
    )
ORDER BY qoh,
    i_product_name,
    i_brand,
    i_class,
    i_category
LIMIT 100;