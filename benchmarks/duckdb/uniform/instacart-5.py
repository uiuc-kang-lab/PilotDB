pilot_query = """
select
    product_name,
    count(*) as sample_size
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
    product_name;
"""

results_mapping = [
    {"aggregate": "count", "size": "sample_size"},
]

subquery_dict = []