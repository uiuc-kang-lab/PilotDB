select
    products.product_id,
    product_name,
    count_big(*) as count_orders,
from
    products,
    orders,
    order_products,
    departments
where
    orders.order_id = order_products.order_id
    and products.product_id = order_products.product_id
    and products.department_id = departments.department_id
    and order_hour_of_day >= 10
    and order_hour_of_day < 15
    and reordered = 1
group by
    products.product_id,
    product_name
order by
    count_orders desc
limit 20;
