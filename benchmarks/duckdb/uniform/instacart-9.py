pilot_query = """
select
    department,
    order_hour_of_day,
    count(*) as sample_size
from
    (
        select
            department,
            order_hour_of_day,
        from
            orders,
            departments,
            products,
            order_products {sampling_method}
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
"""

results_mapping = [
    {"aggregate": "count", "size": "sample_size"},
]

subquery_dict = []
