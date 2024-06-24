select
    100.00 * sum(case
        when product_name like 'Organic%'
            then 1
        else 0
    end) / count_big(*) as organic_count
from
    order_products,
    orders,
    products
where
    order_products.product_id = products.product_id
    and orders.order_id = order_products.order_id
    and order_hour_of_day >= 8
    and order_hour_of_day < 12;
