SELECT TOP 100
    w_state,
    i_item_id,
    sum(
        CASE
            WHEN d_date < '2001-04-02'
            THEN cs_sales_price - ISNULL(cr_refunded_cash, 0)
            ELSE 0
        END
    ) AS sales_before,
    sum(
        CASE
            WHEN d_date >= '2001-04-02'
            THEN cs_sales_price - ISNULL(cr_refunded_cash, 0)
            ELSE 0
        END
    ) AS sales_after
FROM catalog_sales
    LEFT OUTER JOIN catalog_returns ON (
        cs_order_number = cr_order_number
        AND cs_item_sk = cr_item_sk
    ),
    warehouse,
    item,
    date_dim
WHERE i_current_price BETWEEN 0.99 AND 1.49
    AND i_item_sk = cs_item_sk
    AND cs_warehouse_sk = w_warehouse_sk
    AND cs_sold_date_sk = d_date_sk
    AND d_date BETWEEN DATEADD(day, -30, '2001-04-02')
    AND DATEADD(day, 30, '2001-04-02')
GROUP BY w_state,
    i_item_id
ORDER BY w_state,
    i_item_id;