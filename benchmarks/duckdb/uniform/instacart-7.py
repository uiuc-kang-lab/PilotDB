pilot_query = """
select
    department,
    order_hour_of_day,
    count(*) as sample_size
from
    (
        select
            department,
            orders.order_hour_of_day AS order_hour_of_day
        from
            aisles,
            order_products,
            orders,
            products,
            departments
        where
            aisles.aisle_id = products.aisle_id
            and products.product_id = order_products.product_id
            and orders.order_id = order_products.order_id
            and departments.department_id = products.department_id
            and (
                department = 'international'
                or department = 'meat seafood'
            )
            and order_hour_of_day >= 15
            and order_hour_of_day <= 18
    ) as shipping
group by
    department,
    order_hour_of_day;
"""

results_mapping = [
    {"aggregate": "count", "size": "sample_size"},
]

subquery_dict = []