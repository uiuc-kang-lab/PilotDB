pilot_query = """
select
    product_name,
    count(*) as sample_size
from
    products,
    orders,
    order_products
where
    department_id = 4
    and products.product_id = order_products.product_id
    and order_products.order_id = orders.order_id
    and  (order_dow = 0 OR order_dow = 1)
GROUP BY product_name
"""

results_mapping = [
    {"aggregate": "count", "size": "sample_size"},
]

subquery_dict = []
