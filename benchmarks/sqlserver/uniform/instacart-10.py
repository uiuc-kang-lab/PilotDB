pilot_query = """
select
    products.product_id,
    product_name,
    count(*) as sample_size,
from
    products,
    orders,
    order_products {sampling_method},
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
    product_name;
"""

results_mapping = [
    {"aggregate": "count", "size": "sample_size"},
]

subquery_dict = []