select
    products.product_id,
    count(*) as value
from
    products,
    order_products,
    departments
where
    products.product_id = order_products.product_id
    and products.department_id = departments.department_id
    and department = 'breakfast'
group by
    products.product_id having
        count(*) > (
            select
                count(*) * 0.1
            from
                products,
                order_products,
                departments
            where
                products.product_id = order_products.product_id
                and products.department_id = departments.department_id
                and department = 'alcohol'
        )
order by
    value desc;
