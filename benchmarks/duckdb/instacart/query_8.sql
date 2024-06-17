select
    order_hour_of_day,
    sum(case
        when aisle = 'packaged meat' then 1
        else 0
    end) / count(*) as mkt_share
from
    (
        select
            order_hour_of_day,
            aisle
        from
            orders,
            aisles,
            departments,
            products,
            order_products
        where
            orders.order_id = order_products.order_id
            and products.product_id = order_products.product_id
            and products.department_id = departments.department_id
            and products.aisle_id = aisles.aisle_id
            and order_hour_of_day < 12
            and department = 'meat seafood' 
    ) as all_aisle
group by
    order_hour_of_day
order by
    order_hour_of_day;
