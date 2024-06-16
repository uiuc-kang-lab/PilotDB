select
    product_name,
    count(*) as product_count
from
    orders,
    order_products,
    products,
    departments,
    aisles
where
    orders.order_id = order_products.order_id
    and order_products.order_id = products.product_id
    and products.department_id = departments.department_id
    and products.aisle_id = aisles.aisle_id
    and department = 'pantry'
group by
    product_name
order by
    product_count desc;
