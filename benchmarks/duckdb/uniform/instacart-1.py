pilot_query = """
select
    reordered,
    count(*) as sample_size
from
    order_products
where
    add_to_cat_order <= 5
group by
    reordered;
"""

results_mapping = [
    {"aggregate": "count", "size": "sample_size"},
]

subquery_dict = []