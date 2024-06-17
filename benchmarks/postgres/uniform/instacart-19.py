pilot_query = """
select
    count(*) as sample_size
from
    order_products {sampling_method},
    products
where
    (
        products.product_id = order_products.product_id
        and product_name like '%Noddle%'
        and add_to_cat_order between 1 and 3
        and reordered = 1
    )
    or
    (
        products.product_id = order_products.product_id
        and product_name like '%Rice%'
        and add_to_cat_order between 3 and 5
        and reordered = 1
    )
    or
    (
        products.product_id = order_products.product_id
        and product_name like '%Bread%'
        and add_to_cat_order between 5 and 8
        and reordered = 1
    );
"""

results_mapping = [
    {"aggregate": "count", "size": "sample_size"},
]

subquery_dict = []