SELECT i_item_id,
        i_item_desc,
        i_category,
        i_class,
        i_current_price,
        sum(ss_ext_sales_price) AS itemrevenue,
        sum(ss_ext_sales_price) * 100 / sum(sum(ss_ext_sales_price)) over (PARTITION by i_class) AS revenueratio
FROM store_sales,
        item,
        date_dim
WHERE ss_item_sk = i_item_sk
        AND i_category IN ('Sports', 'Music', 'Shoes')
        AND ss_sold_date_sk = d_date_sk
        AND d_date BETWEEN cast('2002-05-20' AS date)
        AND (cast('2002-05-20' AS date) + 30 days)
GROUP BY i_item_id,
        i_item_desc,
        i_category,
        i_class,
        i_current_price
ORDER BY i_category,
        i_class,
        i_item_id,
        i_item_desc,
        revenueratio;