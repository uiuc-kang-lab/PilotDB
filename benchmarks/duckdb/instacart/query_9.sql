select
    department,
    order_hour_of_day,
    count(*) count_orders
from
    (
        select
            department,
            order_hour_of_day,
        from
            orders,
            departments,
            products,
            order_products
        where
            orders.order_id = order_products.order_id
            and products.product_id = order_products.product_id
            and products.department_id = departments.department_id
            and product_name like '%green%'
    ) as profit
group by
    department,
    order_hour_of_day,
order by
   department,
    order_hour_of_day desc;
