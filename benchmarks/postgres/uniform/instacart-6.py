pilot_query = """
select
    count(*) as sample_size
from
    order_products {sampling_method}
where
    reordered = 1
    and add_to_cat_order < 5;
"""

results_mapping = [
    {"aggregate": "count", "size": "sample_size"},
]

subquery_dict = []
