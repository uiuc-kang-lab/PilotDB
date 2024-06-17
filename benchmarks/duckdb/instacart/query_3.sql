select
    product_name,
    count(*) as order_count
from
    products,
    orders,
    order_products
where
    department_id = 4
    and products.product_id = order_products.product_id
    and order_products.order_id = orders.order_id
    and  (order_dow = 0 OR order_dow = 1)
GROUP BY product_name
ORDER BY order_count DESC
LIMIT 10

